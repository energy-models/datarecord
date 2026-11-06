# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The tool layer: verify a record, build a model, read it back.

Notes
-----
- [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
"""

import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import pandas as pd
import pypsa
import pytest
from duckdb import DuckDBPyRelation

from datarecord import Revision, Workspace
from datarecord.schema.categories import matching
from datarecord.schema.specs import Dimension, PropertySpec
from datarecord.tools.base import Schema, UnsupportedRecordError
from datarecord.tools.pypsa import (
    CTYPE,
    PyPSA,
    PyPSACollection,
    _colliding_names,
    _registry,
    _weightings,
)
from tests.fixtures import (
    HandBuilt,
    empty_child,
    empty_root,
    export_network,
    schema,
    scratch_tables,
    unpacked,
    write_attribute,
    write_component,
)
from tests.test_roundtrip import assert_networks_equal


@pytest.fixture(scope="module")
def single_revision(tmp_path_factory, ac_dc):
    """`ac_dc` exported once for the module, for tests that only read it."""
    workspace = Workspace()
    yield export_network(ac_dc, workspace.store(str(tmp_path_factory.mktemp("ac_dc"))))
    workspace.close()


@pytest.fixture(scope="module")
def built(single_revision):
    """The network `single_revision` builds, once for the module; a test changes a copy."""
    return PyPSA.build(single_revision.record)


@pytest.fixture(scope="module")
def solved(built):
    """`built`, optimised once for the module, its solver model dropped so it copies."""
    n = built.copy()
    n.optimize(solver_name="highs")
    n.model.solver_model = None
    return n


@pytest.fixture(scope="module")
def results(single_revision, solved) -> Revision:
    """`solved`'s results, written once as a thin layer over `single_revision`."""
    return _results_over(single_revision, solved)


@pytest.fixture(scope="module")
def results_built(results):
    """The network `results` builds, once for the module; a test changes a copy."""
    return PyPSA.build(results.record)


@pytest.fixture
def exported(single_revision, workspace, tmp_path) -> HandBuilt:
    """`single_revision`'s files in a hand-built root of a new store, under its schema.

    For a test to add files or redeclare the schema before it commits.
    """
    store = workspace.store(str(tmp_path / "ac_dc"))
    layer = empty_root(store, single_revision.manifest.schema)
    source = Path(single_revision.store.layer_dir(single_revision.id))
    for kind in ("dims", "attributes"):
        shutil.copytree(source / kind, layer.dir / kind)
    return layer


def _results_over(revision, n, **labels):
    """`n`'s results, a network's or a collection's, written as a thin layer over `revision`, over its selection at `labels` where given."""
    record = revision.record
    tool = PyPSACollection if isinstance(n, pypsa.NetworkCollection) else PyPSA
    layer = tool.to_datarecord(
        n,
        record.con,
        results_only=True,
        base=record.select(**labels) if labels else record,
    )
    return revision.store.commit(layer, parent=revision)


def _over(members, dim) -> pypsa.NetworkCollection:
    """`members` as a collection whose index names `dim`, or the levels of `dim` where a list."""
    return pypsa.NetworkCollection(pd.Series(members).rename_axis(dim))


def _sub_networks(revision) -> list[str]:
    return _sub_networks_of(revision.record.dims["sub_network"])


def _sub_networks_of(axis) -> list[str]:
    return sorted(axis.df()["sub_network"])


def _prices(revision) -> pd.DataFrame:
    price = revision.record.attributes["marginal_price"].df()
    return price.pivot(index="snapshot", columns="bus", values="value")


def _flat(rel: DuckDBPyRelation) -> pd.DataFrame:
    """A connection attribute's rows with the `connection` struct spread to `entity` and `bus`."""
    return rel.project("*, connection.entity AS entity, connection.bus AS bus").df()


def _add_generator(w, name: str) -> None:
    w.add("entity", [name], ctype="Generator")
    w.add("connection", [{"entity": name, "bus": "b"}], role="attached")


_DIMS = {
    "snapshot": "datetime",
    "period": "int",
    "scenario": "str",
}


def _with_schema(layer: HandBuilt, **kwargs) -> Revision:
    """`layer` committed under a redeclared schema, keeping the attributes it already declares.

    Hand-built rather than written through `store.commit`, which would reject
    an incompatible redeclaration - here the point is to hand the tool a
    schema it must report on rather than one the writer accepted.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
    """
    was = layer.schema
    now = schema(**kwargs)
    labelled = {d: spec for d, spec in was.dimensions.items() if d not in _DIMS}
    redeclared = {d: s for d, s in now.dimensions.items() if d not in labelled}
    now = now.model_copy(update={"dimensions": {**labelled, **redeclared}})
    # Keep the old attributes, but drop any dim a redeclaration removed from
    # their address - an attribute over a dim the schema no longer declares is
    # rejected at construction, and the point here is the redeclared dims, not a
    # stale attribute reference to a gone one.
    addressable = set(now.dimensions)
    attributes = {
        name: pruned
        for name, spec in was.attributes.items()
        if (pruned := spec.model_copy(update={"dims": spec.dims & addressable})).dims
    }
    now = now.model_copy(
        update={
            "attributes": attributes,
            "properties": was.properties,
            "meta": was.meta,
        }
    )
    return replace(layer, schema=now).commit()


def _redeclared_over(store, n, attr: str, dim: str, rows: list[dict]) -> Revision:
    """`n` written as a root of `store` declaring `attr` static and over `dim` too, its rows `rows`.

    Static, since an attribute over `snapshot` is over its coordinate `period` already.
    """
    revision = export_network(n, store)
    declared = revision.manifest.schema
    spec = declared.attributes[attr]
    over = spec.model_copy(update={"dims": spec.dims - {"snapshot"} | {dim}})
    attributes = {**declared.attributes, attr: over}
    layer = empty_root(store, declared.model_copy(update={"attributes": attributes}))
    source = Path(store.layer_dir(revision.id))
    for kind in ("dims", "attributes"):
        shutil.copytree(source / kind, layer.dir / kind)
    write_attribute(layer, attr, rows)
    return layer.commit()


def _ctypes_written(revision) -> pd.Series:
    return revision.record.properties["entity"].df().set_index("entity")["ctype"]


def _north():
    """A bus `north` and a generator at it, over two snapshots."""
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
    n.add("Bus", "north")
    n.add("Generator", "gen", bus="north")
    return n


def _shaped():
    """`_north` and a bus `south`, each with onshore shapes, `country` shapes beside and a generator's site.

    `north` also has an offshore shape, and `DE` one; a custom `Bus.country`
    names each bus's country.
    """
    import shapely

    n = _north()
    n.to_crs("EPSG:3035")
    n.add("Bus", "south")
    n.c.Bus.static["country"] = ["DE", "FR"]
    n.add(
        "Shape",
        ["north onshore", "north offshore", "south onshore"],
        geometry=[
            shapely.box(0, 0, 1, 1),
            shapely.Point(2, 3),
            shapely.box(1, 1, 2, 2),
        ],
        idx=["north", "north", "south"],
        component="Bus",
        type=["onshore", "offshore", "onshore"],
    )
    n.add(
        "Shape",
        ["DE country", "FR country", "DE offshore"],
        geometry=[
            shapely.box(0, 0, 3, 3),
            shapely.box(3, 3, 4, 4),
            shapely.Point(5, 5),
        ],
        idx=["DE", "FR", "DE"],
        component="country",
        type=["country", "country", "offshore"],
    )
    n.add(
        "Shape",
        ["gen site"],
        geometry=[shapely.Point(0.5, 0.5)],
        idx="gen",
        component="Generator",
        type="site",
    )
    return n


def _add_shape(n, **attrs) -> None:
    """Add the shape `extra`, a point, with `attrs`."""
    import shapely

    n.add("Shape", ["extra"], geometry=[shapely.Point(0, 0)], **attrs)


def _with_bus_column(n, name: str, value) -> None:
    """Set the custom column `name` of every bus to `value`."""
    n.c.Bus.static[name] = value


def _shapes(n) -> pd.DataFrame:
    """`n.shapes` keyed by the label each outlines and its `type`, not by name."""
    return n.c.Shape.static.set_index(["component", "idx", "type"]).sort_index()


def _three_port_link():
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2020-01-01", periods=2, freq="h"))
    for bus in ("a", "b", "c"):
        n.add("Bus", bus)
    n.add("Link", "split", bus0="a", bus1="b", bus2="c")
    return n


def _three_port_process():
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2020-01-01", periods=2, freq="h"))
    for bus in ("a", "b", "c"):
        n.add("Bus", bus)
    n.add("Process", "dri", bus0="a", bus1="b", bus2="c")
    return n


@pytest.fixture(scope="module", params=["repeated", "distinct"])
def layout(request) -> str:
    """How `multiperiod` lays its timesteps over the periods."""
    return request.param


def _over_periods(ac_dc, layout: str):
    """`ac_dc` over two investment periods, with weightings of its own in every column of both axes.

    `repeated` runs every timestep in both periods, as `set_investment_periods`
    does; `distinct` splits the timesteps between them.
    """
    n = ac_dc.copy()
    if layout == "repeated":
        n.set_investment_periods([2030, 2040])
    else:
        periods = np.repeat([2030, 2040], len(n.snapshots) // 2)
        n.set_snapshots(
            pd.MultiIndex.from_arrays(
                [periods, n.snapshots], names=["period", "timestep"]
            )
        )
    n.investment_period_weightings = pd.DataFrame(
        {"objective": [1.0, 0.6], "years": [10.0, 5.0]}, index=n.investment_periods
    )
    steps = np.arange(len(n.snapshots), dtype=float)
    n.snapshot_weightings = pd.DataFrame(
        {"objective": 2.0 + steps, "stores": 3.0, "generators": 0.5 * steps},
        index=n.snapshots,
    )
    return n


@pytest.fixture(scope="module")
def multiperiod(ac_dc, layout):
    return _over_periods(ac_dc, layout)


@pytest.fixture(scope="module")
def multiperiod_revision(tmp_path_factory, multiperiod):
    """`multiperiod` exported once for the module."""
    workspace = Workspace()
    yield export_network(
        multiperiod, workspace.store(str(tmp_path_factory.mktemp("multiperiod")))
    )
    workspace.close()


@pytest.fixture(scope="module")
def multiperiod_built(multiperiod_revision):
    """The network `multiperiod_revision` builds."""
    return PyPSA.build(multiperiod_revision.record)


@pytest.fixture(scope="module")
def multiperiod_solved(ac_dc, layout):
    """`multiperiod`, solved over every period at once, its solver model dropped so it copies.

    Built afresh rather than copied: PyPSA's copy of a multi-period network
    loses the snapshot index's name, which its optimisation needs.
    """
    n = _over_periods(ac_dc, layout)
    n.optimize(solver_name="highs", multi_investment_periods=True)
    n.model.solver_model = None
    return n


def _myopic(n) -> dict[int, pypsa.Network]:
    """`n` split into one network per investment period, as a myopic solve writes it."""
    periods = n.snapshots.get_level_values("period")
    return {
        p: n.copy(snapshots=n.snapshots[periods == p], investment_periods=[p])
        for p in n.investment_periods
    }


def _staggered(layout: str) -> pypsa.Network:
    """Three periods of four hours over one bus, a generator built in each, the first retiring before the second period.

    `repeated` runs the same hours in every period; `distinct` moves them into
    the period's year.
    """
    periods = [2030, 2040, 2050]
    hours = pd.date_range("2030-01-01", periods=4, freq="h")
    moved = layout == "distinct"
    n = pypsa.Network()
    n.set_snapshots(
        pd.MultiIndex.from_tuples(
            [(p, t.replace(year=p) if moved else t) for p in periods for t in hours],
            names=["period", "timestep"],
        )
    )
    n.investment_periods = periods
    n.add("Bus", "bus")
    n.add("Carrier", "gas")
    n.add(
        "Generator",
        [f"gen-{p}" for p in periods],
        bus="bus",
        carrier="gas",
        p_nom_extendable=True,
        build_year=periods,
        lifetime=[10, 30, 30],
        capital_cost=[1.0, 100.0, 200.0],
        marginal_cost=10.0,
    )
    load = pd.Series(np.repeat([50.0, 50.0, 100.0], len(hours)), index=n.snapshots)
    n.add("Load", "load", bus="bus", p_set=load)
    return n


@pytest.fixture(scope="module")
def staggered_revision(tmp_path_factory, layout):
    """`_staggered` exported once for the module."""
    workspace = Workspace()
    yield export_network(
        _staggered(layout), workspace.store(str(tmp_path_factory.mktemp("staggered")))
    )
    workspace.close()


def _solve(n):
    """`n` solved, over its periods at once where it has any, its solver model dropped."""
    n.optimize(
        solver_name="highs", multi_investment_periods=bool(n.investment_periods.size)
    )
    n.model.solver_model = None
    return n


def _assert_dispatch_at(built, n, period: int) -> None:
    """`built` dispatches `n`'s generators as `n` does at its snapshots, those of `period` where `n` has no period."""
    p = built.c.Generator.dynamic["p"]
    at = p if n.investment_periods.size else p.loc[period]
    pd.testing.assert_frame_equal(
        at.loc[n.snapshots, n.c.Generator.static.index],
        n.c.Generator.dynamic["p"],
        check_names=False,
        check_index_type=False,
        check_freq=False,
        obj=f"dispatch of {period}",
    )


def _ordered(record) -> dict[str, pd.DataFrame]:
    """Every relation of a record, its rows in one order."""
    return {
        f"{kind}/{name}": rels[name].order("ALL").df()
        for kind, rels in (
            ("dims", record.dims),
            ("properties", record.properties),
            ("attributes", record.attributes),
        )
        for name in rels
    }


def _stochastic(members):
    next(iter(members.values())).set_scenarios(["x", "y"])
    return members


def _resized(members):
    members[2040].c["Generator"].static.loc["Manchester Wind", "p_nom_opt"] += 1.0
    return members


def _relabelled(members):
    return dict(zip([2040, 2030], members.values(), strict=True))


def _with_newcomer(members):
    list(members.values())[-1].add("Generator", "Newcomer", bus="Manchester")
    return members


def _with_new_snapshot(members):
    m = members[2040]
    period, last = m.snapshots[-1]
    later = pd.MultiIndex.from_tuples([(period, last + pd.Timedelta("1h"))])
    m.set_snapshots(m.snapshots.append(later))
    return members


def _unedited(n) -> None:
    pass


def _nudged(n) -> None:
    n.c.Generator.dynamic["p_max_pu"].iloc[3, 1] += 0.1


def _one_snapshot_later(n) -> None:
    later = pd.DatetimeIndex([n.snapshots[-1] + pd.Timedelta("1h")])
    n.set_snapshots(n.snapshots.append(later))


@pytest.fixture(scope="module")
def sweep(single_revision, solved) -> dict[str, pypsa.Network]:
    """`solved`, and the network it solves again with its generators' `p_max_pu` halved: two members solved apart."""
    hi = PyPSA.build(single_revision.record)
    hi.c.Generator.dynamic["p_max_pu"] *= 0.5
    hi.optimize(solver_name="highs")
    hi.model.solver_model = None
    return {"lo": solved, "hi": hi}


def _collection(sweep, store, exported, dim, results_only):
    """`sweep` over `dim`, as a full export or as results over `exported`."""
    collection = _over(sweep, dim)
    if results_only:
        return _results_over(exported.commit(), collection).record
    layer = PyPSACollection.to_datarecord(collection, store.workspace.con)
    return store.commit(layer, parent=None).record


def _assert_results_equal(got, expected, *series):
    """`got` holds `expected`'s capacities, dispatch and prices, and each of `series`."""
    pd.testing.assert_series_equal(
        got.c.Generator.static["p_nom_opt"],
        expected.c.Generator.static["p_nom_opt"],
        check_names=False,
    )
    for ctype, attr in (("Generator", "p"), ("Bus", "marginal_price"), *series):
        pd.testing.assert_frame_equal(
            got.c[ctype].dynamic[attr],
            expected.c[ctype].dynamic[attr],
            check_names=False,
            check_index_type=False,
            check_column_type=False,
            obj=attr,
        )


def _at_defaults(members):
    members["hi"].c.Generator.static["marginal_cost"] = 0.0
    return members


def _without_a_series(members):
    dynamic = members["hi"].c.Generator.dynamic
    dynamic["p_max_pu"] = dynamic["p_max_pu"].drop(columns="Norway Wind")
    return members


def _costing(lo: float, hi: float):
    def edit(members):
        members["lo"].c.Generator.static.loc["Norway Gas", "marginal_cost"] = lo
        members["hi"].c.Generator.static.loc["Norway Gas", "marginal_cost"] = hi
        return members

    return edit


def _two_levels(members):
    return dict(zip([("a", 1), ("b", 2)], members.values(), strict=True))


def _moved_bus(members):
    members["hi"].c.Bus.static.loc["London", "x"] += 1.0
    return members


def _reordered_buses(members):
    members["hi"].c.Bus.static = members["hi"].c.Bus.static.iloc[::-1]
    return members


def _without_a_snapshot(members):
    members["hi"].set_snapshots(members["hi"].snapshots[1:])
    return members


def _with_region_shapes(members):
    for n in members.values():
        _add_shape(n, idx="north", component="region", type="area")
    return members


def _with_custom_column(members):
    members["hi"].c.Generator.static["score"] = 1.0
    return members


def _with_custom_column_on_other_types(members):
    members["lo"].c.Generator.static["score"] = 1.0
    members["hi"].c.Load.static["score"] = 1.0
    return members


def test_the_record_layer_imports_no_tool():
    """`datarecord` is framework-free: reading a record never imports PyPSA.

    The call runs from the tool inward (`PyPSA.build(revision.record)`), so there is no
    registry and no name dispatch to drag a framework in.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
    """
    import subprocess
    import sys

    # A fresh interpreter, so an already-imported PyPSA from another test
    # cannot mask a real import edge.
    code = (
        "import sys; import datarecord; import datarecord.tools; "
        "assert 'pypsa' not in sys.modules, sorted(sys.modules)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


@pytest.fixture(scope="module")
def two_generators(tmp_path_factory):
    """Generators `g1` and `g2` at bus `b` over scenarios `low` and `high`, exported once."""
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
    n.add("Bus", "b")
    n.add("Generator", ["g1", "g2"], bus="b")
    n.set_scenarios({"low": 0.5, "high": 0.5})
    workspace = Workspace()
    yield export_network(n, workspace.store(str(tmp_path_factory.mktemp("two"))))
    workspace.close()


def test_schema_renames_and_computes():
    """A rename maps one source; a computed attribute maps several (`Attr.compute`)."""
    from datarecord.tools.base import Attr

    schema = Schema(
        {
            "Generator": (
                Attr(name="p_max_pu", source=("availability",)),
                Attr(
                    name="marginal_cost",
                    source=("fuel_cost", "efficiency"),
                    compute=lambda fuel, eff: fuel,
                ),
            )
        }
    )
    assert schema.sources("Generator", "p_max_pu") == ("availability",)
    assert schema.sources("Generator", "marginal_cost") == ("fuel_cost", "efficiency")
    # Anything unmapped, and any other type, stays the identity.
    assert schema.sources("Generator", "p_nom") == ("p_nom",)
    assert schema.sources("Link", "p_max_pu") == ("p_max_pu",)
    # A multi-source rename with no way to combine them is a declaration bug.
    with pytest.raises(ValueError, match="exactly one"):
        Attr(name="x", source=("a", "b"))


_SNAPSHOT_SELECTIONS = pytest.mark.parametrize(
    ("value", "hours"),
    [
        pytest.param("2015-01", list(range(10)), id="partial-scalar"),
        pytest.param(
            [pd.Timestamp("2015-01-01 05:00"), "2015-01-01 01:00"], [1, 5], id="list"
        ),
        pytest.param(slice("2015-01-01 02", "2015-01-01 04"), [2, 3, 4], id="slice"),
        pytest.param(slice(None, "2015-01-01 01"), [0, 1], id="open-start"),
        pytest.param(slice("2015-01", None), list(range(10)), id="partial-month"),
        pytest.param(slice("2015-01-01 09", "2015-01-01 09"), [9], id="one-slice"),
    ],
)


class TestPyPSA:
    """The PyPSA tool: export a network, verify and build a record, write results back."""

    def test_build_reads_the_network_source_as_its_written_record(
        self, workspace, ac_dc, built
    ):
        """`build` takes any `RecordLike`, so the tool's own source builds what its commit does.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
        """
        got = PyPSA.build(PyPSA.to_datarecord(ac_dc, workspace.con))
        pd.testing.assert_index_equal(got.snapshots, built.snapshots)
        assert_networks_equal(got, built)
        assert scratch_tables(workspace.con) == set(), (
            "the export dropped its join tables"
        )

    def test_verify_accepts_a_complete_record(self, single_revision):
        """A record written by `export_to_parquet` supplies everything PyPSA needs."""
        assert not PyPSA.verify(single_revision.record)

    def test_the_tool_writes_no_type_or_role_axis(self, single_revision):
        """The schema lists every type and role, so the record reads them from it.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
        """
        dim = "ctype"
        record = single_revision.record
        layer = Path(single_revision.store.layer_dir(single_revision.id))
        assert not (layer / "dims" / f"{dim}.parquet").exists()
        one_of = record.schema.dimensions[dim].one_of
        assert one_of is not None
        assert record.dims[dim].df()[dim].tolist() == list(one_of), (
            "every label of `one_of`, those the network leaves unused included"
        )

    def test_flags_per_role_name_the_attributes_reaching_a_connection_of_it(
        self,
        single_revision,
    ):
        """`role` categorizes `connection`, so `flags(role=...)` reads as `flags(ctype=...)` does.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
        """
        role = "output"
        record = single_revision.record
        tuples = matching(record.properties["connection"], {"role": (role,)})
        over = [
            a
            for a in record.attributes
            if "connection" in record.schema.coordinates_of(a)
        ]
        reaching = {
            a
            for a in over
            if record.attributes[a]
            .join(tuples.project("connection"), "connection")
            .limit(1)
            .fetchall()
        }
        assert set(record.flags(role=role)) == reaching

    def test_buses_and_carriers_are_labels_of_their_dims(
        self, single_revision, built, ac_dc
    ):
        """`n.buses` and `n.carriers` are the `bus` and `carrier` axes, under PyPSA's names.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        """
        record = single_revision.record
        assert not {"Bus", "Carrier"} & set(
            record.schema.dimensions["ctype"].one_of or ()
        )
        assert record.properties["bus"].columns == [
            "bus",
            "carrier",
            "type",
            "x",
            "y",
            "unit",
            "location",
            "country",
        ]
        assert record.properties["carrier"].columns == [
            "carrier",
            "color",
            "nice_name",
            "marginal_cost",
            "efficiency",
            "capital_cost",
        ]
        assert record.schema.attributes["v_nom"].dims == {"bus", "scenario"}
        assert record.schema.attributes["co2_emissions"].dims == {"carrier", "scenario"}

        for ctype in ("Bus", "Carrier"):
            original = ac_dc.c[ctype].static
            pd.testing.assert_frame_equal(
                built.c[ctype].static[original.columns],
                original,
                check_dtype=False,
                check_index_type=False,
            )

    def test_an_unlisted_carrier_is_added_and_an_empty_one_is_none(self, store):
        """A carrier named but not in `n.carriers` becomes a label; `""` names none.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        """
        n = _north()
        n.add("Carrier", "AC", color="black")
        n.add("Generator", "wind", bus="north", carrier="wind")

        record = export_network(n, store).record
        assert record.dims["carrier"].df()["carrier"].tolist() == ["AC", "wind"]
        carriers = record.properties["entity"].df()
        assert carriers.set_index("entity")["carrier"].isna().to_dict() == {
            "gen": True,
            "wind": False,
        }, "an empty carrier is stored as NULL"

        built = PyPSA.build(record)
        assert built.c.Carrier.static.index.to_list() == ["AC", "wind"]
        assert built.c.Generator.static["carrier"].to_dict() == {
            "gen": "",
            "wind": "wind",
        }

    def test_a_property_is_addressed_by_its_subject(self, single_revision):
        """`x` is a line's reactance and `bus.x` a bus's coordinate; `carrier` alone is neither.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        from datarecord.mutable import WorkingRecord

        w = WorkingRecord(single_revision.record)
        w.set("x", 0.5, entity="0")
        w.set("bus.x", 10.2, bus="London")

        x = w.attributes["x"].df().set_index("entity")["value"]
        assert x["0"] == 0.5
        assert "London" not in x.index, "`x` reaches no bus"
        buses = w.properties["bus"].df().set_index("bus")
        assert buses.at["London", "x"] == 10.2
        with pytest.raises(KeyError, match=r"`bus\.carrier` or `entity\.carrier`"):
            w.set("carrier", "AC")

    def test_a_carrier_is_a_live_label(self, single_revision):
        """`entity.carrier` names a label, and a label named is not removed.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
        """
        from datarecord.mutable import WorkingRecord

        w = WorkingRecord(single_revision.record)
        with pytest.raises(KeyError, match="coal"):
            w.set("entity.carrier", "coal", entity="Manchester Gas")
        with pytest.raises(ValueError, match="entity.carrier"):
            w.remove("carrier", ["gas"])

    @pytest.mark.slow
    def test_a_value_its_type_does_not_take_is_reported_and_left_out(self, store):
        """A broadcast reaches every entity; the build keeps it to the types taking it.

        Notes
        -----
        - [a value its type does not take](https://energy-models.github.io/datarecord/reference/tools/#a-value-its-type-does-not-take)
        """
        from datarecord.mutable import WorkingRecord

        n = _north()
        n.add("Bus", "south")
        n.add("Load", "load", bus="south")
        n.add("Line", "line", bus0="north", bus1="south", x=0.1)
        n.add("Link", "link", bus0="north", bus1="south")
        w = WorkingRecord(export_network(n, store).record)
        w.set("p_max_pu", 0.9)

        report = PyPSA.verify(w)
        assert not report, "a value its type does not take fails no build"
        assert {("Load", "p_max_pu"), ("Line", "p_max_pu")} <= report.ignored
        assert ("Link", "p_max_pu") not in report.ignored, "a link takes p_max_pu"
        n = PyPSA.build(w)
        assert (n.c.Link.static["p_max_pu"] == 0.9).all()
        assert "p_max_pu" not in n.c.Load.static
        assert "p_max_pu" not in n.c.Load.dynamic

    @pytest.mark.parametrize(
        ("layers", "built"),
        [
            (
                [
                    lambda w: (
                        w.set("p_max_pu", 0.5, entity="g1"),
                        w.set("p_max_pu", 0.6, entity="g2"),
                    ),
                    lambda w: w.set("p_max_pu", 0.9, scenario="high"),
                ],
                {
                    ("low", "g1"): 0.5,
                    ("low", "g2"): 0.6,
                    ("high", "g1"): 0.9,
                    ("high", "g2"): 0.9,
                },
            ),
            (
                [
                    lambda w: w.set("p_max_pu", 0.9),
                    lambda w: _add_generator(w, "g3"),
                ],
                {
                    ("low", "g1"): 0.9,
                    ("low", "g2"): 0.9,
                    ("low", "g3"): 0.9,
                    ("high", "g1"): 0.9,
                    ("high", "g2"): 0.9,
                    ("high", "g3"): 0.9,
                },
            ),
        ],
        ids=["scenario-crosses-entities", "added-member"],
    )
    def test_a_build_settles_rows_of_several_layers_by_the_broadcast_rule(
        self, two_generators, layers, built
    ):
        """Rows as written, a deeper layer crossing a shallower one, build each member's value.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        """
        from datarecord.mutable import WorkingRecord

        revision = two_generators
        for edit in layers:
            w = WorkingRecord(revision.record)
            edit(w)
            revision = revision.store.commit(w)

        static = PyPSA.build(revision.record).c.Generator.static
        assert static["p_max_pu"].to_dict() == built

    def test_verify_reports_an_entity_attached_to_no_bus(self, single_revision):
        """Removing a bus removes its connections; what was attached to it is unbuildable.

        Notes
        -----
        - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
        """
        from datarecord.mutable import WorkingRecord

        w = WorkingRecord(single_revision.record)
        w.remove("bus", ["Manchester"])

        missing = PyPSA.verify(w).attributes
        assert {("Generator", "bus"), ("Load", "bus")} <= missing
        assert ("Link", "bus0") not in missing, "no link is at Manchester"

    def test_requires_reports_the_records_own_types(self, single_revision):
        """`requires` is record-dependent: PyPSA's axes plus this record's types."""
        req = PyPSA.requires(single_revision.record)
        assert {"snapshot", "period", "scenario"} <= req.dims
        assert "Generator" in req.categories
        # Required attributes come from PyPSA's registry, not a list we maintain.
        assert ("Generator", "bus") in req.attributes
        assert ("Line", "x") in req.attributes
        # `name` is the member identity, never an attribute row.
        assert not [a for a in req.attributes if a[1] == "entity"]

    def test_verify_reports_a_missing_dim(self, exported):
        """A schema that declares no `scenario` dim cannot build a network.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
        """
        revision = _with_schema(exported, dims={"snapshot": "datetime"}, partial=set())

        missing = PyPSA.verify(revision.record)
        assert missing
        assert missing.dims == {"period", "scenario"}
        # A build refuses rather than failing deep inside PyPSA.
        with pytest.raises(UnsupportedRecordError, match="scenario"):
            PyPSA.build(revision.record)

    def test_verify_reports_a_type_the_tool_does_not_know(self, exported):
        """A type outside PyPSA's registry is reported by `verify`, not raised in the fold.

        The type must be one the *schema* declares - `ctype` has `one_of` and
        its list is upheld everywhere, so a record cannot hold a type its own
        schema excludes. What it can hold is a type this tool has no
        registry entry for, which reads back fine and is this tool's business rather
        than the record layer's. `Requirements.categories` is what carries it.

        Notes
        -----
        - [entity types](https://energy-models.github.io/datarecord/reference/data-model/#categories)
        - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
        """
        # Declared alongside PyPSA's own types, since the `one_of` labels pin the
        # vocabulary.
        declared = exported.schema
        assert declared.categorizing("entity") == (CTYPE,)
        dim = declared.dimensions[CTYPE]
        assert dim.one_of is not None, "PyPSA lists its types on the dim"
        layer = replace(
            exported,
            schema=declared.model_copy(
                update={
                    "dimensions": {
                        **declared.dimensions,
                        CTYPE: dim.model_copy(
                            update={"one_of": (*dim.one_of, "Widget")}
                        ),
                    }
                }
            ),
        )
        write_component(layer, "Widget", [{"entity": "w1"}])
        revision = layer.commit()

        missing = PyPSA.verify(revision.record)
        assert missing.categories == {"Widget"}
        # The types PyPSA does know are not reported.
        assert "Generator" not in missing.categories
        with pytest.raises(UnsupportedRecordError, match="Widget"):
            PyPSA.build(revision.record)

    def test_verify_reports_a_snapshot_key(self, exported):
        """`snapshot` as a key is reported, not crashed on.

        A limit of this tool's *representation*, not of the layer format: PyPSA's
        static/series split needs a component's whole series for an attribute to
        come from one layer, so a stored `snapshot = NULL` broadcast row and a
        descendant's per-snapshot row would coexist with no single container to
        put the result in. The record layer therefore permits the
        declaration - every file does carry the column - and the tool catches it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        """
        revision = _with_schema(exported, partial={"scenario", "snapshot"})

        missing = PyPSA.verify(revision.record)
        assert ("owner_map_key", "snapshot") in missing.unsupported_keys
        assert "snapshot" in missing.describe()
        with pytest.raises(UnsupportedRecordError, match="snapshot"):
            PyPSA.build(revision.record)

    def test_verify_reports_a_missing_required_attribute(self, exported, ac_dc):
        """A component type with no `bus` anywhere - no connection row supplies it.

        A port's bus is the `connection` tuple itself, declared as no attribute, so
        no default can stand in for it.
        """
        # A Generator member with no connection row supplying its bus
        # (https://energy-models.github.io/datarecord/reference/tools/#ports).
        write_component(exported, "Generator", [{"entity": "g1"}])
        # One file across every type, so the Generators' rows are dropped from it
        # rather than a per-type file being unlinked.
        path = exported.dir / "dims" / "connection.parquet"
        rows = pd.read_parquet(path)
        generators = set(ac_dc.c["Generator"].static.index) | {"g1"}
        entities = rows["connection"].map(lambda c: c["entity"])
        rows[~entities.isin(generators)].to_parquet(path, index=False)

        write_component(exported, "Line", [{"entity": "l1"}])

        missing = PyPSA.verify(exported.commit().record)
        assert ("Generator", "bus") in missing.attributes
        assert ("Line", "x") not in missing.attributes, (
            "PyPSA declares the required `x` with a default, resolvable with no row"
        )

    def test_verify_reports_a_piecewise_linear_attribute(self, exported):
        """A curve is stored correctly; it is the PyPSA translation that cannot express it.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/reference/data-model/#wide-and-long-rows)
        """
        write_attribute(
            exported,
            "marginal_cost",
            [{"entity": "Manchester Wind", "value": 20.0}],
        )
        revision = exported.commit()
        assert not PyPSA.verify(revision.record).unsupported_values, (
            "the same attribute without breakpoints is not reported"
        )

        child = empty_child(revision)
        write_attribute(
            child,
            "marginal_cost",
            [
                {
                    "entity": "Manchester Wind",
                    "breakpoint": x,
                    "value": v,
                }
                for x, v in ((0.0, 20.0), (50.0, 35.0))
            ],
        )
        revision = child.commit()

        missing = PyPSA.verify(revision.record)
        assert ("Generator", "marginal_cost") in missing.unsupported_values
        assert "unsupported values" in missing.describe()
        with pytest.raises(UnsupportedRecordError):
            PyPSA.build(revision.record)

    @pytest.mark.parametrize(
        ("spread", "dim", "attr", "values"),
        [
            pytest.param(
                lambda n: n.set_investment_periods([2030, 2040]),
                "period",
                "marginal_cost",
                {2030: 30.0, 2040: 40.0},
                id="period",
            ),
            pytest.param(
                lambda n: n.set_scenarios({"lo": 0.5, "hi": 0.5}),
                "scenario",
                "p_nom_mod",
                {"lo": 1.0, "hi": 2.0},
                id="scenario",
            ),
        ],
    )
    def test_a_value_apart_over_a_dim_pypsa_holds_it_without_is_refused(
        self, store, spread, dim, attr, values
    ):
        """A network holds one value along a dim PyPSA's registry declares the attribute without, so rows naming several of its labels are reported and refused, and a selection at one label builds its value.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        n = _north()
        spread(n)
        rows = [
            {"entity": "gen", dim: label, "value": v} for label, v in values.items()
        ]
        record = _redeclared_over(store, n, attr, dim, rows).record

        assert PyPSA.verify(record).unsupported_values == {("Generator", attr)}
        with pytest.raises(
            ValueError, match=rf"`{attr}` over \['{dim}'\].*list of one"
        ):
            PyPSA.build(record)
        views = {label: record.select(**{dim: [label]}) for label in values}
        assert not any(PyPSA.verify(view) for view in views.values())
        built = {
            label: PyPSA.build(view).c.Generator.static[attr].tolist()
            for label, view in views.items()
        }
        assert built == {label: [v] for label, v in values.items()}, (
            "each label builds its own value"
        )

    def test_a_selected_period_builds_its_row_over_the_broadcast_one(self, store):
        """At one kept period, the row naming it is the more specific than the row leaving it NULL, so the component holds one value, the period's.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        """
        n = _north()
        n.set_investment_periods([2030, 2040])
        rows: list[dict] = [
            {"entity": "gen", "period": None, "value": 10.0},
            {"entity": "gen", "period": 2030, "value": 30.0},
        ]
        record = _redeclared_over(store, n, "marginal_cost", "period", rows).record

        built = PyPSA.build(record.select(period=[2030]))

        assert built.c.Generator.static["marginal_cost"].tolist() == [30.0]

    @pytest.mark.parametrize(
        ("ctype", "dim", "branch", "standard", "changed", "extra"),
        [
            (
                "LineType",
                "line_type",
                "Line",
                "Al/St 240/40 4-bundle 380.0",
                {"x_per_length": 0.5},
                {"length": 10.0},
            ),
            (
                "TransformerType",
                "transformer_type",
                "Transformer",
                "160 MVA 380/110 kV",
                {"vsc": 20.0},
                {"s_nom": [10.0, 20.0], "phase_shift": [1.0, 2.0], "tap_side": 1},
            ),
        ],
    )
    def test_a_networks_own_type_round_trips(
        self, store, ctype, dim, branch, standard, changed, extra
    ):
        """A type the network adds to PyPSA's library is a label of its own dim, its columns properties, and the library is not written.

        The branch typed by it shares its name, and a Transformer carries its
        own `s_nom`, `phase_shift` and `tap_side` beside the type's, none of
        which clash. Each branch builds back with the same `r` and `x`, and a
        results layer over the record writes no type.

        Notes
        -----
        - [standard types](https://energy-models.github.io/datarecord/reference/tools/#standard-types)
        """
        n = pypsa.Network()
        n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
        n.add(
            "Bus",
            ["a", "b"],
            v_nom=[380.0, 110.0 if branch == "Transformer" else 380.0],
        )
        n.add(
            ctype,
            "own type",
            **{**n.c[ctype].static.loc[standard].to_dict(), **changed},
        )
        n.add(
            branch,
            ["std", "own type"],
            bus0="a",
            bus1="b",
            type=[standard, "own type"],
            **extra,
        )

        root = export_network(n, store)
        record = root.record
        assert record.dims[dim].df()[dim].tolist() == ["own type"], (
            "only the type the library lacks is written"
        )
        assert set(record.schema.properties[dim]) == set(n.c[ctype].static), (
            "every column of a type is a property of its dim"
        )
        held = record.properties[dim].df().set_index(dim).loc["own type"]
        assert held[list(changed)].to_dict() == changed
        assert set(_ctypes_written(root)) == {branch}, "a type is no entity"

        built = PyPSA.build(record)
        assert_networks_equal(built, n)
        built.calculate_dependent_values()
        n.calculate_dependent_values()
        pd.testing.assert_frame_equal(
            built.c[branch].static[["r", "x"]], n.c[branch].static[["r", "x"]]
        )

        child = _results_over(root, built)
        layer = Path(store.layer_dir(child.id)) / "dims"
        assert not (layer / "entity.parquet").exists()
        assert not (layer / f"{dim}.parquet").exists(), "a results layer writes no type"

    def test_a_network_of_library_types_writes_none(self, single_revision):
        """`ac_dc` holds PyPSA's standard types alone, so the record holds no type axis.

        Notes
        -----
        - [standard types](https://energy-models.github.io/datarecord/reference/tools/#standard-types)
        """
        held = set(single_revision.manifest.files.dims)
        assert not held & {"line_type", "transformer_type"}

    def test_to_datarecord_rejects_a_cross_type_name_collision(self, workspace):
        """PyPSA scopes names per type; a record scopes them across every entity type.

        Reported rather than repaired: renaming to `Generator:north` would hand back
        a network whose components PyPSA can no longer find by their own names, so
        the mismatch is the caller's to reconcile.

        Notes
        -----
        - [a label is unique within its dim](https://energy-models.github.io/datarecord/reference/format/#labels)
        - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
        """
        n = _north()
        n.add("Generator", "north", bus="north")
        n.add("Load", "north", bus="north")

        with pytest.raises(
            UnsupportedRecordError, match="more than one category"
        ) as raised:
            PyPSA.to_datarecord(n, workspace.con)
        assert raised.value.missing.names == {"north"}

    def test_a_bus_and_a_generator_may_share_a_name(self, store):
        """A bus is a label of `bus`, not an entity, so `north` is one of each."""
        n = _north()
        n.add("Generator", "north", bus="north")

        record = export_network(n, store).record
        assert record.dims["bus"].df()["bus"].tolist() == ["north"]
        assert "north" in record.dims["entity"].df()["entity"].tolist()
        assert_networks_equal(PyPSA.build(record), n)

    @pytest.mark.parametrize(
        ("columns", "kind"),
        [
            ({"Bus": ("country", "DE")}, "property"),
            ({"Bus": ("nom_max_onwind_2050", 5.0)}, "attribute"),
            ({"Generator": ("country", "DE")}, "property"),
            ({"Generator": ("location", "site")}, "property"),
            ({"Generator": ("e_nom", 5.0)}, "attribute"),
            ({"Generator": ("v_nom", 1.0)}, "property"),
            ({"Carrier": ("efficiency", 0.9)}, "property"),
            (
                {"Bus": ("country", "DE"), "Generator": ("country", "DE")},
                "property",
            ),
            ({"Bus": ("share", 0.5), "Generator": ("share", 0.5)}, "property"),
        ],
        ids=[
            "text-bus",
            "number-bus",
            "text-entity",
            "text-entity-named-like-a-bus-property",
            "number-entity-named-like-another-types-input",
            "number-entity-named-like-a-bus-attribute",
            "number-carrier-named-like-a-connection-attribute",
            "text-on-two-dims",
            "number-on-two-dims",
        ],
    )
    def test_a_custom_column_is_a_property_if_text_or_clashing(
        self, store, columns, kind
    ):
        """A text column describes its label, and a clashing name would be one attribute over two dims.

        Either is a property of its subject; any other custom column is an
        attribute over its type's dim. Every case is built back onto the types
        that carried it alone, a load included to receive nothing.
        """
        n = _north()
        n.add("Carrier", "AC")
        n.add("Load", "demand", bus="north")
        for ctype, (name, value) in columns.items():
            n.c[ctype].static[name] = value

        record = export_network(n, store).record
        for ctype, (name, _) in columns.items():
            dim = {"Bus": "bus", "Carrier": "carrier"}.get(ctype, "entity")
            as_property = name in record.schema.properties[dim]
            assert as_property == (kind == "property"), f"{name} is a {kind} of {dim}"
            if kind == "attribute":
                assert record.schema.attributes[name].dims == {dim, "scenario"}
        assert_networks_equal(PyPSA.build(record), n)

    def test_a_custom_property_carried_with_two_dtypes_is_refused(self, workspace):
        """One property of `entity` is one dtype, whichever types carry it."""
        n = _north()
        n.add("Load", "demand", bus="north")
        n.c.Generator.static["country"] = "DE"
        n.c.Load.static["country"] = 1.0

        with pytest.raises(ValueError, match="`country` is a property of `entity`"):
            PyPSA.to_datarecord(n, workspace.con)

    @pytest.mark.parametrize("name", ["scenario", "period", "entity"])
    def test_a_custom_column_named_like_a_key_is_refused(self, workspace, name):
        """A key of the record's rows names no property or attribute, text or not.

        Notes
        -----
        - [custom columns](https://energy-models.github.io/datarecord/reference/tools/#custom-columns)
        """
        n = _north()
        _with_bus_column(n, name, "x")
        with pytest.raises(ValueError, match="key a record's rows"):
            PyPSA.to_datarecord(n, workspace.con)

    @pytest.mark.parametrize(
        ("ctype", "values", "dtype", "kept"),
        [
            ("Bus", [True, None], "bool", [True, None]),
            ("Bus", [3, None], "int", [3, None]),
            ("Bus", [np.int64(3), np.nan], "int", [3, None]),
            ("Bus", [1, 2.5], "float", [1.0, 2.5]),
            ("Generator", [1.5, None], "float", [1.5, None]),
            ("Generator", [False, True], "bool", [False, True]),
            ("Generator", [np.bool_(True), np.nan], "bool", [True, None]),
            ("Generator", [np.int64(3), np.int64(4)], "int", [3, 4]),
            ("Generator", ["a", None], "str", ["a", None]),
            ("Generator", [True, 2], "str", ["True", "2"]),
            ("Generator", [None, None], "str", [None, None]),
        ],
    )
    def test_a_custom_object_column_is_typed_by_its_values(
        self, store, ctype, values, dtype, kept
    ):
        """A custom column of dtype `object`, as a NaN leaves a column of bools or ints, is declared as its non-null values say, and written as declared: one of mixed types as text."""
        n = _north()
        n.add("Bus", "south")
        n.add("Generator", "south gen", bus="south")
        static = n.c[ctype].static
        static["extra"] = pd.Series(values, index=static.index, dtype=object)

        record = export_network(n, store).record
        dim = "bus" if ctype == "Bus" else "entity"
        spec = record.schema.properties[dim].get("extra")
        declared = record.schema.attributes["extra"] if spec is None else spec
        assert declared.dtype == dtype
        built = PyPSA.build(record).c[ctype].static["extra"]
        held = [None if pd.isna(value) else value for value in built]
        assert held == kept, "the values round-trip as declared"

    def test_a_shape_is_a_property_of_the_label_it_outlines(self, store):
        """A shape is `{dim}.shape_{type}` as WKT, a `component` that is no PyPSA type a dim of its own, its CRS in the schema's `meta`.

        A custom `bus.country` refers to the `country` dim the shapes make, and
        a results layer writes none of it.

        Notes
        -----
        - [geometry](https://energy-models.github.io/datarecord/reference/tools/#geometry)
        - [custom columns](https://energy-models.github.io/datarecord/reference/tools/#custom-columns)
        """
        from geopandas.testing import assert_geodataframe_equal

        n = _shaped()
        revision = export_network(n, store)
        record = revision.record
        schema = record.schema
        assert schema.meta["crs"] == "EPSG:3035"
        assert "geometry" not in schema.attributes
        assert "Shape" not in set(_ctypes_written(revision)), "a shape is no entity"
        assert record.dims["country"].df()["country"].tolist() == ["DE", "FR"], (
            "a shape-made dim's labels are the shapes' `idx` in network order"
        )
        assert schema.properties["bus"]["country"].one_of == "country"
        bus = record.properties["bus"].df().set_index("bus")
        assert bus.loc["north", "shape_onshore"] == (
            "POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))"
        )
        assert set(schema.properties["country"]) == {"shape_country", "shape_offshore"}
        assert "shape_site" in schema.properties["entity"]

        built = PyPSA.build(record)
        assert built.crs == n.crs
        assert "country DE offshore" in built.c.Shape.static.index, (
            "a built shape is named `{component} {idx} {type}`"
        )
        expected = _shapes(n)
        assert_geodataframe_equal(_shapes(built), expected, check_like=True)

        child = _results_over(revision, built)
        layer = Path(store.layer_dir(child.id)) / "dims"
        assert not (layer / "country.parquet").exists(), (
            "a results layer writes no shape"
        )
        rebuilt = PyPSA.build(child.record)
        assert_geodataframe_equal(_shapes(rebuilt), expected, check_like=True)

        for network in (built, n):
            network.remove("Shape", network.c.Shape.static.index)
        assert_networks_equal(built, n)

    def test_two_components_outline_one_idx_with_one_type(self, store):
        """A `Bus` and a `country` both labelled `north` each keep their `onshore` shape, the component naming them apart.

        Notes
        -----
        - [geometry](https://energy-models.github.io/datarecord/reference/tools/#geometry)
        """
        n = _shaped()
        _add_shape(n, idx="north", component="country", type="onshore")
        built = PyPSA.build(export_network(n, store).record)
        assert {"Bus north onshore", "country north onshore"} <= set(
            built.c.Shape.static.index
        )

    @pytest.mark.parametrize(
        ("edit", "match"),
        [
            pytest.param(
                lambda n: _add_shape(n, idx="north", component="Bus", type="onshore"),
                r"share a `\(component, idx, type\)`",
                id="duplicate",
            ),
            pytest.param(
                lambda n: _add_shape(n, idx="north", type="onshore"),
                "lack a `component`, `idx` or `type`",
                id="no-component",
            ),
            pytest.param(
                lambda n: _add_shape(n, idx="north", component="Bus"),
                "lack a `component`, `idx` or `type`",
                id="no-type",
            ),
            pytest.param(
                lambda n: _add_shape(n, component="country", type="area"),
                "lack a `component`, `idx` or `type`",
                id="no-idx",
            ),
            pytest.param(
                lambda n: _add_shape(
                    n, idx="Atlantis", component="Bus", type="onshore"
                ),
                r"name \['Atlantis'\], no Bus",
                id="not-a-label",
            ),
            pytest.param(
                lambda n: _add_shape(n, idx="north", component="bus", type="onshore"),
                "a dim PyPSA's schema declares",
                id="pypsa-dim",
            ),
            pytest.param(
                lambda n: _with_bus_column(n, "shape_own", "x"),
                "no place",
                id="custom-shape-column",
            ),
            pytest.param(
                lambda n: _with_bus_column(n, "country", ["DE", "XX"]),
                r"names \['XX'\], no label of `country`",
                id="unknown-country",
            ),
        ],
    )
    def test_a_shape_naming_no_label_is_refused(self, workspace, edit, match):
        """A shape is one value of a property of one label, so `to_datarecord` refuses one that names none.

        Notes
        -----
        - [geometry](https://energy-models.github.io/datarecord/reference/tools/#geometry)
        """
        n = _shaped()
        edit(n)
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(n, workspace.con)

    def test_a_shape_made_dim_is_no_dim_of_its_own(self, store):
        """A collection over a dim of its own beside the `country` shapes make builds back as a collection over that dim alone.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        members = {2013: _shaped(), 2014: _shaped()}
        members[2014].c.Generator.static["p_nom"] = 2.0
        layer = PyPSACollection.to_datarecord(
            _over(members, "weather_year"), store.workspace.con
        )
        record = store.commit(layer, parent=None).record
        assert not PyPSACollection.verify(record), (
            "`country` is not a second dim of its own"
        )
        assert PyPSACollection.build(record).index.name == "weather_year"

    def test_a_stochastic_network_holds_its_shapes_per_scenario(self, store):
        """A stochastic network's shapes are indexed by `(scenario, name)`, as its other components are.

        Notes
        -----
        - [geometry](https://energy-models.github.io/datarecord/reference/tools/#geometry)
        """
        from geopandas.testing import assert_geodataframe_equal

        n = _shaped()
        n.set_scenarios(["a", "b"])
        built = PyPSA.build(export_network(n, store).record)
        assert built.c.Shape.static.index.names == ["scenario", "name"]
        assert_geodataframe_equal(_shapes(built), _shapes(n), check_like=True)

    def test_a_bus_selection_keeps_every_label_a_shape_made(self, store):
        """Selecting buses keeps every country shape, though `bus.country` refers to `country`, and drops the other bus's; a network squeezes no `country`.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        record = export_network(_shaped(), store).record
        built = PyPSA.build(record.select(bus=["north"]))
        outlined = _shapes(built).index.droplevel("type").unique().tolist()
        assert outlined == [
            ("Bus", "north"),
            ("Generator", "gen"),
            ("country", "DE"),
            ("country", "FR"),
        ]
        with pytest.raises(ValueError, match=r"not \['country'\]"):
            PyPSA.build(record.select(country="DE"))

    @pytest.mark.slow
    def test_a_solved_network_exports(self, store, solved):
        """A solved network's SubNetworks are labels of a dim of their own; its derived `Line.v_nom` is recomputed.

        PyPSA numbers SubNetworks `0`, `1`, ... as it numbers Lines, which the dim
        keeps apart; `optimize` leaves the derived `Line.v_nom` behind, which
        `build` recomputes.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        assert set(solved.c["SubNetwork"].static.index) & set(
            solved.c["Line"].static.index
        )

        revision = export_network(solved, store)
        record = revision.record
        names = _sub_networks(revision)
        assert names == sorted(solved.c["SubNetwork"].static.index)
        pointed = set(record.attributes["sub_network"].df()["value"])
        assert pointed <= set(names), (
            "a `sub_network` value names a `sub_network` label"
        )
        carriers = record.properties["sub_network"].df()["carrier"]
        assert set(carriers) <= set(record.dims["carrier"].df()["carrier"])

        built = PyPSA.build(record)
        assert sorted(built.c["SubNetwork"].static.index) == names
        built.determine_network_topology()
        assert sorted(built.c["SubNetwork"].static.index) == ["0", "1", "2", "3"], (
            "a built network passes PyPSA's topology code"
        )
        built.calculate_dependent_values()
        pd.testing.assert_series_equal(
            built.c.Line.static.v_nom, solved.c.Line.static.v_nom
        )

    def test_a_stochastic_network_is_not_a_false_collision(self):
        """A `(scenario, name)` index is one name per component, not one per scenario.

        Comparing the index tuples would report every scenario's copy as a distinct
        name while missing a real cross-type clash.
        """
        import pypsa

        n = pypsa.Network()
        n.set_scenarios(["a", "b"])
        n.add("Bus", "north")
        n.add("Generator", "north gen", bus="north")

        assert not _colliding_names(n)

    def test_a_solved_network_exports_its_results_long_form(
        self, single_revision, solved
    ):
        """A solved network's results are attributes, keyed by name, long-form.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
        """
        n = solved

        results = PyPSA.to_datarecord(n, single_revision.record.con).attributes
        assert "p" in results
        assert "p_nom_opt" in results

        # Relations, lazy so a tool may fetch on demand (https://energy-models.github.io/datarecord/reference/tools/).
        assert isinstance(results["p"], DuckDBPyRelation)
        p = results["p"].df()
        # The long schema's columns (https://energy-models.github.io/datarecord/reference/record/), so the write path can persist it as-is -
        # and no `ctype`, an attribute row being keyed by `name` (https://energy-models.github.io/datarecord/reference/format/#labels).
        assert {"connection", "snapshot", "scenario", "value"} <= set(p.columns)
        assert "ctype" not in p.columns
        assert "period" not in p.columns, "a series over `snapshot` reaches its period"
        assert set(p["attribute"].tolist()) == {"p"}
        # Series output: one row per (name, snapshot), for the Generator rows -
        # selected by name, since the frame no longer carries the type.
        gens = set(n.c["Generator"].static.index)
        gen = _flat(results["p"]).query("entity in @gens")
        assert len(gen) == len(n.snapshots) * len(gens)

        # A static output has no snapshot, and only the components whose value
        # differs from the default appear (some generators solve to p_nom_opt=0).
        nom = results["p_nom_opt"].df()
        nom = nom[nom["entity"].isin(gens)]
        assert "snapshot" not in nom.columns
        nonzero = n.c["Generator"].static["p_nom_opt"] != 0.0
        assert set(nom["entity"]) == set(n.c["Generator"].static.index[nonzero])

    def test_an_export_skips_results_still_at_their_default(
        self, single_revision, built
    ):
        """An unsolved network yields no `p`/`p_nom_opt` rows.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        results = PyPSA.to_datarecord(built, single_revision.record.con).attributes
        assert "p" not in results
        assert "p_nom_opt" not in results

    def test_a_solved_p_is_one_row_per_connection(self, single_revision, solved):
        """`p` holds a row per `(entity, bus)` per snapshot, each port's own sign.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/reference/tools/#results-on-connections)
        """
        n = solved

        p = _flat(PyPSA.to_datarecord(n, single_revision.record.con).attributes["p"])
        connections = unpacked(single_revision.record.dims["connection"])
        per_connection = p.groupby(["entity", "bus"]).size()
        assert set(per_connection.index) == set(
            zip(connections["entity"], connections["bus"])
        ), "every connection carries `p`, and nothing else does"
        assert (per_connection == len(n.snapshots)).all(), "one row per snapshot"

        lines = n.c["Line"]
        line = lines.static.index[0]
        for port in ("0", "1"):
            bus = lines.static.at[line, f"bus{port}"]
            rows = p[(p["entity"] == line) & (p["bus"] == bus)].set_index("snapshot")
            expected = lines.dynamic[f"p{port}"][line]
            assert np.allclose(rows["value"].reindex(expected.index), expected), (
                f"`p{port}` lands on the line's bus{port} connection"
            )

    def test_bus_results_are_keyed_by_bus(self, results, results_built, solved):
        """`marginal_price`, `v_mag_pu` and `v_ang` are over `bus`; `Bus.p` and the rest are derived.

        `build` copies a result into the network like any attribute.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/reference/tools/#results-on-connections)
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        schema = results.record.schema
        over_bus = {
            a for a, spec in _registry().schema.attributes.items() if "bus" in spec.dims
        }
        assert over_bus - _registry().inputs == {"marginal_price", "v_mag_pu", "v_ang"}
        assert not solved.c["Bus"].dynamic["p"].empty, "PyPSA did compute a `Bus.p`"
        assert "bus" not in schema.attributes["p"].dims

        expected = solved.c["Bus"].dynamic["marginal_price"]
        wide = _prices(results)
        assert np.allclose(wide[expected.columns].reindex(expected.index), expected)

        built = results_built.c["Bus"].dynamic["marginal_price"]
        pd.testing.assert_frame_equal(
            built[expected.columns],
            expected,
            check_names=False,
            check_freq=False,
            check_index_type=False,
        )

    def test_a_child_reads_its_parents_results_and_the_latest_wins(
        self, results, results_built
    ):
        """A result is an attribute: it inherits through layers, a later one standing over it.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        expected = _prices(results)
        pd.testing.assert_frame_equal(_prices(empty_child(results).commit()), expected)

        n = results_built.copy()
        n.c["Bus"].dynamic["marginal_price"] *= 2
        resolved = _results_over(results, n)
        pd.testing.assert_frame_equal(
            _prices(empty_child(resolved).commit()), expected * 2
        )

    def test_a_re_solve_leaves_no_stale_sub_network(
        self, single_revision, results, results_built, solved
    ):
        """A results layer replaces `base`'s SubNetworks rather than adding to them.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        *kept, dropped = sorted(solved.c["SubNetwork"].static.index)
        assert _sub_networks(results) == sorted([*kept, dropped])
        entities = results.record.dims["entity"].df()
        assert entities.equals(single_revision.record.dims["entity"].df()), (
            "a results layer leaves the entity axis alone"
        )

        n = results_built.copy()
        n.remove("SubNetwork", dropped)
        resolved = _results_over(results, n)
        assert _sub_networks(resolved) == sorted(kept)
        slack = resolved.record.attributes["slack_bus"].df()
        assert dropped not in set(slack["sub_network"]), "its results went with it"

    @pytest.mark.parametrize(
        "over_solved",
        [
            pytest.param(False, id="unsolved"),
            pytest.param(True, id="solved"),
        ],
    )
    def test_results_without_sub_networks_list_none(
        self, single_revision, built, results, results_built, over_solved
    ):
        """A solve without SubNetworks restates a `sub_network` axis `base` has empty, and writes none where it has none.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        base, n = (results, results_built) if over_solved else (single_revision, built)
        n = n.copy()
        n.remove("SubNetwork", n.c["SubNetwork"].static.index)
        n.c["Bus"].dynamic["marginal_price"] = pd.DataFrame(
            1.0, index=n.snapshots, columns=n.c["Bus"].static.index
        )
        revision = _results_over(base, n)
        record = revision.record
        assert _sub_networks_of(record.dims["sub_network"]) == []
        assert ("sub_network" in revision.manifest.files.dims) == over_solved, (
            "the axis is restated only where `base` has one"
        )
        assert "marginal_price" in record.attributes, "the results are written"
        slack = record.attributes.get("slack_bus")
        assert slack is None or slack.df().empty, "no stale SubNetwork keeps a result"

    @pytest.mark.slow
    def test_a_result_a_re_solve_leaves_out_reads_as_null(
        self, single_revision, results_built
    ):
        """A results layer restates `base`'s results it does not write as NULL.

        Built back, the network holds PyPSA's own default there. `mu_upper` is a
        result over a line, `marginal_price` one over a bus.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        left_out = {"Line": "mu_upper", "Bus": "marginal_price"}
        n = results_built.copy()
        for ctype, attr in left_out.items():
            n.c[ctype].dynamic[attr] = pd.DataFrame(
                1.0, index=n.snapshots, columns=n.c[ctype].static.index
            )
        base = _results_over(single_revision, n)
        for ctype, attr in left_out.items():
            n.c[ctype].dynamic[attr] = n.c[ctype].dynamic[attr].iloc[:, :0]
        resolved = _results_over(base, n)

        def read(revision, name):
            df = revision.record.attributes[name].df()
            by = [c for c in df.columns if c not in ("breakpoint", "value")]
            return df.sort_values(by).reset_index(drop=True)

        rebuilt = PyPSA.build(resolved.record)
        for ctype, attr in left_out.items():
            stale = read(base, attr)
            keys = [c for c in stale.columns if c not in ("breakpoint", "value")]
            values = stale[keys].merge(read(resolved, attr), on=keys)["value"]
            assert (stale["value"] == 1.0).all(), attr
            assert len(values) == len(stale), attr
            assert values.isna().all(), f"the stale {attr} is gone"
            c = rebuilt.c[ctype]
            assert (c.dynamic[attr] == c.defaults.at[attr, "default"]).all().all(), (
                f"PyPSA fills its own default for {attr}"
            )
        pd.testing.assert_frame_equal(read(resolved, "p_nom"), read(base, "p_nom"))

    def test_a_series_a_re_solve_leaves_out_reads_as_null(self, results, results_built):
        """A bus's series `base` holds and a re-solve writing the others leaves out is NULL at every snapshot; one `base` holds over no snapshot takes the re-solve's series.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        from datarecord.mutable import WorkingRecord

        n = results_built.copy()
        prices = n.c.Bus.dynamic["marginal_price"] + 1.0
        dropped, static = prices.columns[:2]
        w = WorkingRecord(results.record)
        w.set("marginal_price", 3.0, bus=static)
        base = results.store.commit(w)
        kept = prices.drop(columns=dropped)
        n.c.Bus.dynamic["marginal_price"] = kept
        got = _prices(_results_over(base, n))
        assert got[dropped].isna().all(), "the stale series is gone"
        assert got[got.index.isna()].isna().all().all(), "the static row is NULL"
        pd.testing.assert_frame_equal(
            got.reindex(index=kept.index, columns=kept.columns),
            kept,
            check_names=False,
            check_freq=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param([10.0, 20.0], [5.0, 5.0], id="to-shared"),
            pytest.param([5.0, 5.0], [10.0, 20.0], id="to-each"),
        ],
    )
    def test_a_re_solve_writes_a_result_shared_across_scenarios_over_each(
        self, store, first, second
    ):
        """A re-solve's result equal at every scenario, written once over `base`'s per-scenario rows, reaches each scenario, and the reverse.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        n = _north()
        n.add("GlobalConstraint", "co2", sense="<=", constant=100.0)
        n.set_scenarios(["a", "b"])
        revision = export_network(n, store)
        for mu in (first, second):
            n = PyPSA.build(revision.record)
            n.c.GlobalConstraint.static["mu"] = mu
            revision = _results_over(revision, n)
        built = PyPSA.build(revision.record)
        assert built.c.GlobalConstraint.static["mu"].tolist() == second

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "select",
        [
            pytest.param({"bus": ["London", "Norwich"]}, id="bus"),
            pytest.param({"entity": ["Manchester Wind", "0"]}, id="entity"),
        ],
    )
    def test_a_re_solve_of_a_selection_restates_only_what_it_holds(
        self, exported, solved, select
    ):
        """A selection's results replace `base`'s at the labels it holds, and leave every other label's, and the SubNetworks it numbers over itself alone, to `base`.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        base = _results_over(exported.commit(), solved)
        n = PyPSA.build(base.record.select(**select))
        n.determine_network_topology()
        n.c.Bus.dynamic["marginal_price"] += 1.0
        resolved = _results_over(base, n, **select)

        before, after = (PyPSA.build(r.record) for r in (base, resolved))
        topology = (r.c.SubNetwork.static.drop(columns="obj") for r in (after, before))
        pd.testing.assert_frame_equal(*topology)
        pd.testing.assert_series_equal(
            after.c.Line.static["sub_network"], before.c.Line.static["sub_network"]
        )
        for ctype, attr in (
            ("Bus", "marginal_price"),
            ("Generator", "p"),
            ("Line", "p0"),
        ):
            got, was = after.c[ctype].dynamic[attr], before.c[ctype].dynamic[attr]
            held = n.c[ctype].dynamic[attr]
            inside = pd.DataFrame(False, index=got.index, columns=got.columns)
            inside.loc[held.index, held.columns] = True
            pd.testing.assert_frame_equal(got.mask(inside), was.mask(inside), obj=attr)
            pd.testing.assert_frame_equal(
                got.loc[held.index, held.columns],
                held,
                check_names=False,
                check_freq=False,
                obj=attr,
            )

    @pytest.mark.slow
    def test_a_re_solve_of_some_snapshots_keeps_bases_series_elsewhere(
        self, exported, solved
    ):
        """Over a single-period `base`, a re-solve of some snapshots takes them and leaves `base`'s dispatch at the rest; one of snapshots `base` lacks is refused at the call.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        base = _results_over(exported.commit(), solved)
        n = PyPSA.build(base.record.select(snapshot=list(solved.snapshots[:2])))
        n.c.Generator.dynamic["p"] += 100.0
        built = PyPSA.build(_results_over(base, n).record).c.Generator.dynamic["p"]
        expected = solved.c.Generator.dynamic["p"].copy()
        expected.loc[n.snapshots] = n.c.Generator.dynamic["p"]
        pd.testing.assert_frame_equal(
            built[expected.columns], expected, check_names=False, check_freq=False
        )
        n.set_snapshots(n.snapshots + pd.DateOffset(years=100))
        with pytest.raises(ValueError, match="snapshot labels `base` lacks"):
            PyPSA.to_datarecord(n, base.record.con, results_only=True, base=base.record)

    @pytest.fixture
    def swept(self, exported, sweep):
        """`exported` with `sweep`'s results over the dim of its own `sensitivity`."""
        return _results_over(exported.commit(), _over(sweep, "sensitivity"))

    @pytest.mark.parametrize(
        ("base", "labels", "over", "match"),
        [
            pytest.param(
                "stochastic",
                {"scenario": "hi"},
                "record",
                "over the selection it was built from",
                id="deterministic",
            ),
            pytest.param(
                "stochastic",
                {"scenario": "hi"},
                "selection",
                "first-stage results",
                id="scenario",
            ),
            pytest.param(
                "stochastic",
                {"scenario": ["hi"]},
                "selection",
                "first-stage results",
                id="scenario-list",
            ),
            pytest.param(
                "swept",
                {"sensitivity": "lo"},
                "record",
                "rows would reach every label",
                id="own",
            ),
        ],
    )
    @pytest.mark.slow
    def test_results_of_some_labels_reaching_every_label_are_refused(
        self, request, base, labels, over, match
    ):
        """Results of a network built at some labels that would stand for every label are refused.

        A deterministic network's are NULL on `scenario`; a subset of
        scenarios decides its `p_nom_opt` for itself alone, though it is one
        value over every scenario; a member's results stand beside the other
        members'.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        revision = request.getfixturevalue(base)
        view = revision.record.select(**labels)
        n = PyPSA.build(view)
        written = view if over == "selection" else revision.record
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(n, view.con, results_only=True, base=written)

    @pytest.mark.slow
    def test_a_collection_re_solved_at_some_snapshots_restates_each_label_alone(
        self, swept, solved
    ):
        """Each member's results land at its label, and that label's own stand at the other snapshots.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        view = swept.record.select(snapshot=list(solved.snapshots[:2]))
        part = PyPSACollection.build(view)
        before = PyPSACollection.build(swept.record)
        for step, n in enumerate(part.networks, start=1):
            n.c.Generator.dynamic["p"] += 100.0 * step
        after = PyPSACollection.build(_results_over(swept, part).record)
        for label, n in zip(part.index, part.networks, strict=True):
            expected = before.networks[label].c.Generator.dynamic["p"].copy()
            expected.loc[n.snapshots] = n.c.Generator.dynamic["p"]
            pd.testing.assert_frame_equal(
                after.networks[label].c.Generator.dynamic["p"][expected.columns],
                expected,
                check_names=False,
                check_freq=False,
                obj=str(label),
            )

    @pytest.mark.slow
    @pytest.mark.parametrize("value", ["lo", ["lo"]], ids=["network", "collection"])
    def test_a_re_solve_of_some_labels_leaves_the_others(self, exported, solved, value):
        """A member's results land at its label alone, though `base` holds them once for every label.

        One label builds a network written over its selection, a list a
        collection written over its own.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        agreed = _results_over(
            exported.commit(), _over({"lo": solved, "hi": solved.copy()}, "sensitivity")
        )
        assert "sensitivity" not in agreed.record.schema.attributes["p"].dims
        view = agreed.record.select(sensitivity=value)
        if isinstance(value, str):
            n = PyPSA.build(view)
            model: pypsa.Network | pypsa.NetworkCollection = n
        else:
            model = PyPSACollection.build(view)
            (n,) = model.networks
        n.c.Generator.static["p_nom_opt"] += 1.0
        n.c.Generator.dynamic["p"] += 100.0
        record = _results_over(agreed, model, sensitivity=value).record
        built = PyPSACollection.build(record)
        _assert_results_equal(built.networks["lo"], n)
        _assert_results_equal(built.networks["hi"], solved)

    def test_results_only_takes_base(self, single_revision, solved):
        """A results layer is written over the record it was built from.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        with pytest.raises(ValueError, match="takes `base` with `results_only=True`"):
            PyPSA.to_datarecord(solved, single_revision.record.con, results_only=True)

    def test_a_collection_is_exported_over_nothing(self, single_revision, sweep):
        """A full export of a collection takes no `base`."""
        record = single_revision.record
        with pytest.raises(ValueError, match="exported whole over nothing"):
            PyPSACollection.to_datarecord(
                _over(sweep, "sensitivity"), record.con, base=record
            )

    @pytest.mark.slow
    @pytest.mark.parametrize(
        ("attr", "value"), [("p_nom_extendable", False), ("marginal_cost", 0.0)]
    )
    def test_a_full_export_over_base_writes_a_return_to_the_default(
        self, exported, attr, value
    ):
        """A value back at PyPSA's default is written over a `base` holding another, which would show through.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        revision = exported.commit()
        n = PyPSA.build(revision.record)
        assert n.c.Generator.static.at["Manchester Wind", attr] != value
        n.c.Generator.static.at["Manchester Wind", attr] = value
        layer = PyPSA.to_datarecord(n, revision.record.con, base=revision.record)
        child = PyPSA.build(revision.store.commit(layer, parent=revision).record)
        assert child.c.Generator.static.at["Manchester Wind", attr] == value

    @pytest.mark.slow
    @pytest.mark.parametrize("attr", ["p_nom_extendable", "marginal_cost", "p_nom_min"])
    @pytest.mark.parametrize(
        "entity",
        ["Manchester Wind", "Newcomer"],
        ids=["unchanged", "new-at-default"],
    )
    def test_a_full_export_over_base_leaves_out_what_it_repeats(
        self, exported, attr, entity
    ):
        """A static value equal to what `base` resolves, its row or the default, is not written."""
        revision = exported.commit()
        n = PyPSA.build(revision.record)
        n.add("Generator", "Newcomer", bus="Manchester")
        layer = PyPSA.to_datarecord(n, revision.record.con, base=revision.record)
        rows = layer.attributes[attr].df()
        assert entity not in set(rows["entity"]), "the layer stays thin"
        child = PyPSA.build(revision.store.commit(layer, parent=revision).record)
        expected = n.c.Generator.static.at[entity, attr]
        assert child.c.Generator.static.at[entity, attr] == expected

    @pytest.mark.slow
    @pytest.mark.parametrize(
        ("scenarios", "edit", "written"),
        [
            pytest.param(None, _unedited, slice(0), id="nan-repeats-null"),
            pytest.param(None, _one_snapshot_later, slice(None), id="snapshot-new"),
            pytest.param(None, _nudged, slice(1, 2), id="nudged"),
            pytest.param(["lo", "hi"], _nudged, slice(1, 2), id="nudged-scenario"),
        ],
    )
    def test_a_full_export_over_base_compares_a_series_at_each_snapshot(
        self, store, ac_dc, scenarios, edit, written
    ):
        """Over `base`, a series NaN where `base`'s is NULL repeats it, and one differing at a snapshot, or at one `base` lacks, is written whole for its entity and scenario.

        A port value or custom value repeating `base`'s is left out, and so is
        a weighting where the snapshots stay.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        n = ac_dc.copy()
        n.c.Generator.static["subsidy"] = 2.0
        if scenarios is not None:
            n.set_scenarios(scenarios)
        n.c.Generator.dynamic["p_max_pu"].iloc[0, 0] = np.nan
        revision = export_network(n, store)
        n = PyPSA.build(revision.record)
        edit(n)
        wide = n.c.Generator.dynamic["p_max_pu"]
        layer = PyPSA.to_datarecord(n, revision.record.con, base=revision.record)
        rows = layer.attributes["p_max_pu"].df()
        keys = ["scenario", "entity"] if scenarios is not None else ["entity"]
        assert set(rows.set_index(keys).index) == set(wide.columns[written])
        assert len(rows) == len(wide.columns[written]) * len(n.snapshots), (
            "a differing series is written whole"
        )
        repeated = ["efficiency", "subsidy"]
        if edit is not _one_snapshot_later:
            repeated.append("snapshot_weighting_objective")
        written_again = [a for a in repeated if not layer.attributes[a].df().empty]
        assert written_again == [], (
            "a repeated port, custom or weighting value is left out"
        )
        child = PyPSA.build(revision.store.commit(layer, parent=revision).record)
        pd.testing.assert_frame_equal(
            child.c.Generator.dynamic["p_max_pu"][wide.columns], wide, check_freq=False
        )

    @pytest.mark.parametrize(
        ("held", "edited", "written"),
        [
            pytest.param([5.0, 0.0], [5.0, 5.0], True, id="held-at-one"),
            pytest.param([5.0, 5.0], [5.0, 5.0], False, id="held-at-none"),
            pytest.param([5.0, 0.0], [0.0, 0.0], True, id="to-the-default"),
        ],
    )
    def test_a_full_export_over_base_writes_a_value_shared_across_scenarios(
        self, store, held, edited, written
    ):
        """A value equal at every scenario repeats `base` only where `base` resolves it at each, not where it holds it at one and the default at another.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        n = _north()
        n.set_scenarios(["a", "b"])
        n.c.Generator.static["p_nom"] = held
        revision = export_network(n, store)
        n = PyPSA.build(revision.record)
        n.c.Generator.static["p_nom"] = edited
        layer = PyPSA.to_datarecord(n, revision.record.con, base=revision.record)
        assert layer.attributes["p_nom"].df().empty != written
        child = PyPSA.build(revision.store.commit(layer, parent=revision).record)
        assert child.c.Generator.static["p_nom"].tolist() == edited

    def test_link_internal_power_is_p_internal(self, single_revision, solved):
        """`Link.p` is power at no bus: an entity-level `p_internal`, no `bus`.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/reference/tools/#results-on-connections)
        """
        n = solved

        results = PyPSA.to_datarecord(n, single_revision.record.con).attributes
        internal = results["p_internal"].df()
        links = list(n.c["Link"].static.index)
        assert "bus" not in internal.columns
        assert set(internal["entity"]) == set(links)
        assert len(internal) == len(n.snapshots) * len(links)
        assert "p_internal" in _registry().schema.attributes

    def test_a_three_port_record_writes_p_on_each_connection(self, workspace):
        """`p0`, `p1` and `p2` are `p` on each port's connection, though the bare registry has no `p2`.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/reference/tools/#results-on-connections)
        """
        n = _three_port_link()
        flows = {"p0": [3.0, 4.0], "p1": [-1.0, -2.0], "p2": [-1.5, -1.5]}
        for attr, values in flows.items():
            n.c["Link"].dynamic[attr] = pd.DataFrame(
                {"split": values}, index=n.snapshots
            )

        results = PyPSA.to_datarecord(n, workspace.con).attributes
        assert not set(flows) & set(results), "no positional names"
        p = _flat(results["p"]).sort_values(["bus", "snapshot"])
        assert p.groupby("bus")["value"].apply(list).to_dict() == {
            "a": flows["p0"],
            "b": flows["p1"],
            "c": flows["p2"],
        }

    def test_a_static_attribute_differing_per_scenario_survives_a_round_trip(
        self, store
    ):
        """A fixed `p_nom` is an input and may differ per scenario, as PyPSA allows.

        A per-scenario port value is placed by connection like any other.

        Notes
        -----
        - [entities and their properties](https://energy-models.github.io/datarecord/reference/tools/#entities)
        - [ports](https://energy-models.github.io/datarecord/reference/tools/#ports)
        """
        n = _three_port_link()
        n.add("Generator", "g", bus="a", p_nom=10.0, marginal_cost=1.0)
        n.add("Load", "l", bus="a", p_set=5.0)
        n.set_scenarios({"low": 0.5, "high": 0.5})
        n.c.generators.static.loc[("high", "g"), "p_nom"] = 20.0
        n.c["Link"].static["efficiency"] = 0.5
        n.c["Link"].static.loc[("low", "split"), "efficiency2"] = 0.2
        n.c["Link"].static.loc[("high", "split"), "efficiency2"] = 0.4
        n.consistency_check(strict=["scenario_invariant_attrs"])

        built = PyPSA.build(export_network(n, store).record)
        generators = built.c.generators.static[["p_nom", "marginal_cost"]]
        assert generators.to_dict("index") == {
            ("low", "g"): {"p_nom": 10.0, "marginal_cost": 1.0},
            ("high", "g"): {"p_nom": 20.0, "marginal_cost": 1.0},
        }, "p_nom keeps its scenario, a shared marginal_cost reaches both"
        links = built.c["Link"].static[["efficiency", "efficiency2"]]
        assert links.to_dict("index") == {
            ("low", "split"): {"efficiency": 0.5, "efficiency2": 0.2},
            ("high", "split"): {"efficiency": 0.5, "efficiency2": 0.4},
        }, "each scenario's port values land in their own columns"

    def test_a_custom_attribute_differing_per_scenario_survives_a_round_trip(
        self, store
    ):
        """A numeric custom column is an attribute over `scenario`, as `p_nom` is.

        Notes
        -----
        - [custom columns](https://energy-models.github.io/datarecord/reference/tools/#custom-columns)
        """
        n = _north()
        n.set_scenarios({"low": 0.5, "high": 0.5})
        n.c.Generator.static["score"] = [1.0, 2.0]

        built = PyPSA.build(export_network(n, store).record)
        assert built.c.Generator.static["score"].to_dict() == {
            ("low", "gen"): 1.0,
            ("high", "gen"): 2.0,
        }, "each scenario keeps its value"

    def test_a_custom_property_differing_per_scenario_is_refused(self, workspace):
        """A text custom column is a property, which has no `scenario` dim.

        Notes
        -----
        - [custom columns](https://energy-models.github.io/datarecord/reference/tools/#custom-columns)
        """
        n = _north()
        n.set_scenarios({"low": 0.5, "high": 0.5})
        n.c.Bus.static["country"] = ["DE", "FR"]

        with pytest.raises(ValueError, match="'north' has `country` .* by scenario"):
            PyPSA.to_datarecord(n, workspace.con)

    @pytest.mark.parametrize("attr", ["p_nom_extendable", "p_nom_opt"])
    def test_only_pypsas_scenario_invariant_attributes_lack_the_scenario_dim(
        self, attr
    ):
        """Every stored attribute may differ per scenario, but those PyPSA keeps invariant."""
        dims = _registry().schema.attributes[attr].dims
        assert "scenario" not in dims, dims

    @pytest.mark.parametrize(
        ("network", "ctype", "name", "static", "series", "attr", "default"),
        [
            (
                _three_port_link,
                "Link",
                "split",
                {"efficiency": 0.5},
                ("efficiency2", [0.2, 0.4]),
                "efficiency",
                1.0,
            ),
            (
                _three_port_process,
                "Process",
                "dri",
                {"rate0": -2.0, "rate2": 0.3},
                ("rate1", [0.8, 1.2]),
                "rate",
                None,
            ),
        ],
        ids=["link-efficiency", "process-rate"],
    )
    def test_per_port_values_survive_a_round_trip(
        self, store, network, ctype, name, static, series, attr, default
    ):
        """Each port's value comes back from its connection, a series on one port beside static ones.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/reference/tools/#ports)
        """
        series_attr, values = series
        n = network()
        n.c[ctype].static.loc[name, list(static)] = list(static.values())
        n.c[ctype].dynamic[series_attr] = pd.DataFrame(
            {name: values}, index=n.snapshots
        ).rename_axis(columns="name")

        record = export_network(n, store).record
        positional = {*static, series_attr} - {attr}
        assert not positional & set(record.attributes), "no positional names"
        assert _registry().schema.attributes[attr].default == default, (
            "rate0 and rate1 default differently"
        )
        built = PyPSA.build(record)

        c = built.c[ctype]
        assert c.static.loc[name, list(static)].to_dict() == static, (
            "each port's value lands in its own column"
        )
        assert all(c.dynamic[a].empty for a in static), "a static port stays static"
        assert c.dynamic[series_attr][name].to_list() == values
        assert_networks_equal(built, n)

    def test_per_type_defaults_round_trip_with_nothing_written(self, store):
        """A Load and a StorageUnit added without `sign` or `p_min_pu` build with their own defaults.

        A default PyPSA differs per type is declared through `default_by`.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/reference/schema/#attributes)
        """
        import pypsa

        from datarecord.mutable import WorkingRecord

        sign = _registry().schema.properties["entity"]["sign"]
        assert sign.default_by["entity.ctype"]["Load"] == -1.0, (
            "PyPSA declares a load's sign through `default_by`"
        )

        n = pypsa.Network()
        n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
        n.add("Bus", "b")
        root = export_network(n, store)

        staged = WorkingRecord(root.record)
        staged.add("entity", ["l1"], ctype="Load")
        staged.add("entity", ["su1"], ctype="StorageUnit")
        staged.add(
            "connection",
            [{"entity": e, "bus": "b"} for e in ("l1", "su1")],
            role="attached",
        )
        child = root.store.commit(staged)

        inputs = Path(store.layer_dir(child.id)) / "attributes"
        written = (
            {p.stem for p in inputs.glob("*.parquet")} if inputs.exists() else set()
        )
        assert not {"sign", "p_min_pu"} & written, "a default is never stored"

        built = PyPSA.build(child.record)
        assert built.c["Load"].static.at["l1", "sign"] == -1, "a Load's own sign"
        assert built.c["StorageUnit"].static.at["su1", "sign"] == 1
        assert built.c["StorageUnit"].static.at["su1", "p_min_pu"] == -1, (
            "a StorageUnit's own p_min_pu"
        )

    def test_delays_are_rows_keyed_by_their_connections(self, workspace, store):
        """A Process's `delay0`/`delay1` and a Link's `delay`/`delay2` are one `delay` per bus.

        A Link leaves `delay` unsuffixed at bus1, so no suffix marks it per-port.
        Never varying, it is still an attribute: long rows keyed by the connection,
        where `role` is the connection's one property.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/reference/tools/#ports)
        """
        n = _three_port_process()
        n.c["Process"].static.loc["dri", ["delay0", "delay1"]] = [1, 2]
        n.c["Process"].static.loc["dri", "cyclic_delay1"] = True
        n.add("Link", "split", bus0="a", bus1="b", bus2="c", delay=3, delay2=4)

        record = PyPSA.to_datarecord(n, workspace.con)
        members = set(record.dims["entity"].columns)
        assert not {"delay", "delay0", "delay2", "cyclic_delay1"} & members, (
            "no positional names on the members"
        )
        assert set(record.properties["connection"].columns) == {"connection", "role"}, (
            "a connection row carries its key and its property"
        )
        rows = unpacked(record.attributes["delay"])
        delay = rows.set_index(["entity", "bus"])["value"].dropna().to_dict()
        assert delay == {
            ("dri", "a"): 1,
            ("dri", "b"): 2,
            ("dri", "c"): 0,
            ("split", "b"): 3,
            ("split", "c"): 4,
        }, "bus0 of a Link has no delay"
        assert _registry().schema.attributes["cyclic_delay"].dims == {
            "connection",
            "scenario",
        }

        built = PyPSA.build(export_network(n, store).record)
        assert built.c["Link"].static.loc["split", ["delay", "delay2"]].tolist() == [
            3,
            4,
        ]
        assert built.c["Process"].static.loc["dri", "cyclic_delay1"]
        assert_networks_equal(built, n)

    def test_a_nan_in_a_series_survives_the_melt(self, workspace):
        """A NaN cell is still a row."""
        import numpy as np

        n = _three_port_link()
        n.c["Link"].dynamic["p_max_pu"] = pd.DataFrame(
            {"split": [1.0, np.nan]}, index=n.snapshots
        )

        rows = PyPSA.to_datarecord(n, workspace.con).attributes["p_max_pu"]
        split = rows.filter("entity = 'split'")
        assert len(split) == 2, "one row per snapshot, NaN included"

    def test_a_name_is_never_an_identifier_between_long_and_wide(self, store):
        """Each name is stored and comes back as its own series.

        `wind "north"` would end a quoted identifier, `snapshot` names a key
        column, and `Gen` and `gen` one case-insensitive identifier.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/reference/data-model/#wide-and-long-rows)
        """
        import pypsa

        names = ['wind "north"', "snapshot", "Gen", "gen"]
        n = pypsa.Network()
        n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
        n.add("Bus", "north")
        n.add("Generator", names, bus="north")
        series = pd.DataFrame(
            {name: [0.1 * i, 0.1 * i + 0.05] for i, name in enumerate(names, 1)},
            index=n.snapshots,
        )
        n.c["Generator"].dynamic["p_max_pu"] = series

        revision = store.commit(
            PyPSA.to_datarecord(n, store.workspace.con), parent=None
        )
        rows = revision.record.attributes["p_max_pu"]
        assert sorted(rows.project("entity, value").fetchall()) == sorted(
            (name, value) for name in names for value in series[name]
        ), "the melt stores each name's own series"
        built = PyPSA.build(revision.record).c["Generator"].dynamic["p_max_pu"]
        pd.testing.assert_frame_equal(
            built[names],
            series,
            check_names=False,
            check_freq=False,
            check_column_type=False,
        )

    def test_schema_dims_stay_generic(self, exported):
        """`Coords` carries a dim PyPSA knows nothing about; axis names live in the tool, which reports it as a dim of the record's own.

        Declared but not `partial`, so the record's files need no column for it.
        Keying it would be a different matter, reported by the tool against the
        real record - see `test_verify_reports_a_snapshot_key`.
        """
        revision = _with_schema(exported, dims={**_DIMS, "vintage": "str"})
        record = revision.record
        assert "vintage" in record.schema.dims
        assert record.dims["vintage"].fetchall() == [], "no axis rows anywhere"
        missing = PyPSA.verify(revision.record)
        assert missing.unsupported_dims == {"vintage"}, (
            "a collection's, not a network's"
        )
        assert not missing.dims, "PyPSA's own dims are satisfied"
        assert "vintage" in record.schema.long_columns

    def test_a_null_scenario_weight_reaches_every_scenario_its_own_row_does_not(self):
        rows = pd.DataFrame({"scenario": [None, "high"], "value": [0.25, 0.5]})
        con = duckdb.connect()
        index = pd.Index(["low", "mid", "high"], name="scenario")
        record = SimpleNamespace(
            schema=_registry().schema,
            dims={"scenario": con.from_df(index.to_frame(index=False))},
            attributes={"scenario_weight": con.from_df(rows)},
        )
        equal = pd.DataFrame({"weight": 1 / 3}, index=index)
        weights = _weightings(record, "scenario", equal)  # type: ignore[arg-type]
        assert weights["weight"].to_dict() == {"low": 0.25, "mid": 0.25, "high": 0.5}, (
            "a NULL-scenario row broadcasts; a scenario's own row wins"
        )

    def test_a_multi_period_snapshot_is_a_multi_dim(
        self, multiperiod, multiperiod_revision
    ):
        """A multi-period snapshot is a `(period, timestep)` tuple, its timesteps repeating or not.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        - [snapshots and weightings](https://energy-models.github.io/datarecord/reference/tools/#snapshots-and-weightings)
        """
        schema = multiperiod_revision.record.schema
        timesteps = multiperiod.snapshots.get_level_values("timestep")
        labels = multiperiod_revision.record.dims["snapshot"].df()["snapshot"]
        assert [(s["period"], s["timestep"]) for s in labels] == list(
            multiperiod.snapshots
        ), "PyPSA's index, in its order"
        assert schema.over("snapshot") == {"period": "period", "timestep": "timestep"}
        assert "snapshot" not in schema.properties
        steps = multiperiod_revision.record.dims["timestep"].df()["timestep"]
        assert steps.tolist() == list(timesteps.unique()), "each timestep once"
        assert schema.attributes["p_max_pu"].dims == {
            "entity",
            "snapshot",
            "scenario",
        }, "dims as in a single-period network"

    def test_multi_period_weightings_survive_a_round_trip(
        self, multiperiod, multiperiod_built
    ):
        pd.testing.assert_frame_equal(
            multiperiod_built.snapshot_weightings,
            multiperiod.snapshot_weightings,
            check_index_type=False,
        )
        pd.testing.assert_frame_equal(
            multiperiod_built.investment_period_weightings,
            multiperiod.investment_period_weightings,
            check_index_type=False,
        )

    def test_a_multi_period_network_survives_a_round_trip(
        self, multiperiod, multiperiod_built
    ):
        assert_networks_equal(multiperiod_built, multiperiod)

    @pytest.mark.parametrize("multiperiod", ["repeated"], indirect=True)
    def test_a_set_on_some_snapshots_keeps_the_rest_of_the_series(
        self, multiperiod, multiperiod_revision
    ):
        """A snapshot is set by its `period` and `timestep`, some of them by a frame of their tuples.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        from datarecord.mutable import WorkingRecord

        record = multiperiod_revision.record
        snapshots = record.dims["snapshot"]
        first = snapshots.limit(1).fetchall()[0][0]
        in_2040 = snapshots.filter("snapshot.period = 2040").project(
            "snapshot, 0.5 AS value"
        )
        periods = multiperiod.snapshots.get_level_values("period")
        selected = {
            "Manchester Wind": ((in_2040, {}), periods == 2040),
            "Norway Wind": ((0.5, first), np.arange(len(periods)) == 0),
        }
        staged = WorkingRecord(record)
        for entity, ((value, selector), _) in selected.items():
            staged.set("p_max_pu", value, entity=entity, **selector)

        built = PyPSA.build(multiperiod_revision.store.commit(staged).record)
        for entity, (_, changed) in selected.items():
            series = multiperiod.c["Generator"].dynamic["p_max_pu"][entity]
            pd.testing.assert_series_equal(
                built.c["Generator"].dynamic["p_max_pu"][entity],
                series.where(~changed, 0.5),
                check_index_type=False,
            )

    @pytest.mark.slow
    def test_a_myopic_solve_writes_the_results_of_one_solve(
        self, multiperiod_revision, multiperiod_solved
    ):
        """A collection of per-period networks writes the results layer of the network they split.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        whole = _results_over(multiperiod_revision, multiperiod_solved)
        members = _over(_myopic(multiperiod_solved), "period")
        myopic = _results_over(multiperiod_revision, members)
        expected = _ordered(whole.record)
        actual = _ordered(myopic.record)
        assert {
            "attributes/p",
            "attributes/p_nom_opt",
            "attributes/mu",
            "attributes/marginal_price",
            "dims/sub_network",
        } <= expected.keys(), "the solve has series and static results and SubNetworks"
        assert actual.keys() == expected.keys()
        for name, frame in expected.items():
            pd.testing.assert_frame_equal(actual[name], frame, obj=name)

    @pytest.mark.parametrize(
        ("edit", "match"),
        [
            pytest.param(_resized, "'Manchester Wind' has `p_nom_opt`", id="conflict"),
            pytest.param(
                _with_new_snapshot, "snapshot labels `base` lacks", id="snapshot"
            ),
            pytest.param(_relabelled, "has investment periods", id="period"),
            pytest.param(
                _with_newcomer,
                r"entity labels `base` lacks: \['Newcomer'\]",
                id="entity",
            ),
            pytest.param(_stochastic, "is stochastic", id="stochastic"),
        ],
    )
    def test_a_myopic_solve_must_fit_its_base(
        self, multiperiod_revision, multiperiod_solved, edit, match
    ):
        """A member holds its own period, `base`'s components, and the static results the others hold.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        record = multiperiod_revision.record
        members = _over(edit(_myopic(multiperiod_solved)), "period")
        with pytest.raises(ValueError, match=match):
            PyPSACollection.to_datarecord(
                members, record.con, results_only=True, base=record
            )

    @pytest.mark.slow
    def test_a_myopic_solve_exports_the_network_of_one_solve(
        self, store, multiperiod_solved
    ):
        """A full export of a `period` collection is that of the multi-period network it split.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        members = _over(_myopic(multiperiod_solved), "period")
        myopic = store.commit(
            PyPSACollection.to_datarecord(members, store.workspace.con), parent=None
        )
        expected = _ordered(export_network(multiperiod_solved, store).record)
        actual = _ordered(myopic.record)
        assert actual.keys() == expected.keys()
        for name, frame in expected.items():
            pd.testing.assert_frame_equal(actual[name], frame, obj=name)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        ("periods", "period", "results_only"),
        [
            pytest.param("both", 2030, True, id="both-results-first"),
            pytest.param("both", 2040, True, id="both-results-second"),
            pytest.param("one", 2030, True, id="one-results"),
            pytest.param("one", 2030, False, id="one-export"),
        ],
    )
    def test_a_network_of_one_period_writes_over_a_multi_period_base(
        self,
        store,
        multiperiod,
        multiperiod_revision,
        multiperiod_solved,
        periods,
        period,
        results_only,
    ):
        """A network carrying one of `base`'s investment periods writes its results at that period.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        member = _myopic(multiperiod_solved)[period]
        base = (
            multiperiod_revision
            if periods == "both"
            else store.commit(
                PyPSACollection.to_datarecord(
                    _over({period: _myopic(multiperiod)[period]}, "period"),
                    store.workspace.con,
                ),
                parent=None,
            )
        )
        layer = PyPSA.to_datarecord(
            member, base.record.con, results_only=results_only, base=base.record
        )
        built = PyPSA.build(
            base.store.commit(layer, parent=base).record.select(period=[period])
        )
        _assert_results_equal(built, member)

    @pytest.mark.slow
    @pytest.mark.parametrize("period", [2030, 2040])
    def test_a_period_built_as_a_list_solves_and_writes_back(
        self, multiperiod_revision, period
    ):
        """A period selected as a list keeps its investment period, so its solve writes back over `base` as is.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        base = multiperiod_revision
        view = base.record.select(period=[period])
        n = PyPSA.build(view)
        n.optimize(solver_name="highs", multi_investment_periods=True)
        n.model.solver_model = None
        layer = PyPSA.to_datarecord(n, view.con, results_only=True, base=view)
        written = base.store.commit(layer, parent=base).record
        _assert_results_equal(PyPSA.build(written.select(period=[period])), n)

    @pytest.mark.slow
    def test_results_of_selected_snapshots_land_on_them(self, multiperiod_revision):
        """A network of some snapshots of every period writes its results at `base`'s labels of them, and one of snapshots `base` lacks is refused at the call.

        Notes
        -----
        - [snapshots and weightings](https://energy-models.github.io/datarecord/reference/tools/#snapshots-and-weightings)
        """
        base = multiperiod_revision
        n = PyPSA.build(
            base.record.select(timestep=slice("2015-01-01 02:00", "2015-01-01 03:00"))
        )
        held = n.c.Generator.static.index
        p = pd.DataFrame(
            np.arange(len(n.snapshots) * len(held), dtype=float).reshape(-1, len(held))
            + 1,
            index=n.snapshots,
            columns=held,
        )
        n.c.Generator.dynamic["p"] = p
        layer = PyPSA.to_datarecord(
            n, base.record.con, results_only=True, base=base.record
        )
        built = PyPSA.build(
            base.store.commit(layer, parent=base).record
        ).c.Generator.dynamic["p"]
        pd.testing.assert_frame_equal(
            built.loc[p.index, p.columns], p, check_names=False, check_freq=False
        )
        others = built.drop(index=p.index).fillna(0)
        assert (others == 0).all().all(), "no other snapshot holds one"
        periods, timesteps = (n.snapshots.get_level_values(i) for i in range(2))
        n.set_snapshots(
            pd.MultiIndex.from_arrays([periods, timesteps + pd.DateOffset(years=100)])
        )
        with pytest.raises(ValueError, match="snapshot labels `base` lacks"):
            PyPSA.to_datarecord(n, base.record.con, results_only=True, base=base.record)

    @pytest.mark.slow
    @pytest.mark.parametrize("listed", [True, False], ids=["list", "label"])
    def test_per_period_solves_written_in_turn_keep_each_others_results(
        self, staggered_revision, listed
    ):
        """Each period built alone, solved and written over the last leaves the other periods' dispatch and the idle generators' capacities standing.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        """
        revision, solves = staggered_revision, {}
        for period in (2030, 2040, 2050):
            labels = {"period": [period] if listed else period}
            solves[period] = _solve(PyPSA.build(revision.record.select(**labels)))
            revision = _results_over(revision, solves[period], **labels)
        built = PyPSA.build(revision.record)
        for period, n in solves.items():
            _assert_dispatch_at(built, n, period)
        capacities = [n.c.Generator.static["p_nom_opt"] for n in solves.values()]
        latest = pd.concat(capacities).groupby(level=0).last()
        assert latest["gen-2030"] > 0, "the first period's solve builds its generator"
        pd.testing.assert_series_equal(
            built.c.Generator.static["p_nom_opt"],
            latest,
            check_names=False,
            check_index_type=False,
        )

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "select",
        [{"period": [2040, 2050]}, {"period": [2040]}, {"period": 2040}],
        ids=str,
    )
    def test_a_solve_of_some_periods_leaves_the_rest_to_base(
        self, staggered_revision, select
    ):
        """Re-solved at double the load, the selected periods take the re-solve's results; the other periods' dispatch and the capacities of generators idle in the selected ones are `base`'s.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/reference/tools/#writing-results)
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        whole = _solve(PyPSA.build(staggered_revision.record))
        base = _results_over(staggered_revision, whole)
        n = PyPSA.build(base.record.select(**select))
        n.c.Load.dynamic["p_set"] *= 2
        built = PyPSA.build(_results_over(base, _solve(n), **select).record)

        _assert_dispatch_at(built, n, 2040)
        periods = n.investment_periods if n.investment_periods.size else [2040]
        p = built.c.Generator.dynamic["p"]
        others = ~p.index.get_level_values("period").isin(periods)
        pd.testing.assert_frame_equal(
            p[others], whole.c.Generator.dynamic["p"][others], check_names=False
        )
        expected = whole.c.Generator.static["p_nom_opt"].copy()
        held = n.c.Generator.static["p_nom_opt"]
        expected[held.index] = held
        pd.testing.assert_series_equal(
            built.c.Generator.static["p_nom_opt"],
            expected,
            check_names=False,
            check_index_type=False,
        )

    @pytest.mark.slow
    @pytest.mark.parametrize("source", ["built", "split"])
    def test_a_period_collection_agrees_on_the_entities_active_in_each_period(
        self, staggered_revision, source
    ):
        """Members built at their period hold its active generators alone; members split off one solve hold every one, an idle generator's capacity, here zeroed, counting for no member.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        record = staggered_revision.record
        if source == "built":
            collection = PyPSACollection.build(record)
            members = dict(zip(collection.index, collection.networks, strict=True))
            for m in members.values():
                m.c.Generator.static["p_nom_extendable"] = False
                m.c.Generator.static["p_nom"] = 100.0
                _solve(m)
        else:
            members = _myopic(_solve(PyPSA.build(record)))
            for p in (2040, 2050):
                members[p].c.Generator.static.loc["gen-2030", "p_nom_opt"] = 0.0
        layer = _results_over(staggered_revision, _over(members, "period"))
        built = PyPSA.build(layer.record)

        for p, m in members.items():
            _assert_dispatch_at(built, m, p)
        active = [
            m.c.Generator.static["p_nom_opt"][m.get_active_assets("Generator", p)]
            for p, m in members.items()
        ]
        expected = pd.concat(active).groupby(level=0).first()
        assert expected["gen-2030"] > 0, "built in its own period"
        pd.testing.assert_series_equal(
            built.c.Generator.static["p_nom_opt"],
            expected,
            check_names=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize(
        ("period", "held"),
        [
            (2040, ["gen-2040"]),
            ([2040], ["gen-2040"]),
            ([2030, 2040], ["gen-2030", "gen-2040"]),
            (slice(2040, None), ["gen-2040", "gen-2050"]),
        ],
        ids=str,
    )
    def test_selected_periods_hold_the_entities_active_in_them(
        self, staggered_revision, period, held
    ):
        """A list or slice of periods drops a generator active in none of them, as one period squeezed does.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        built = PyPSA.build(staggered_revision.record.select(period=period))
        assert built.c.Generator.static.index.tolist() == held

    @pytest.mark.slow
    def test_an_asset_a_later_period_lacks_retires_there(self, store):
        """A generator the 2050 member lacks is active until 2050, not in it.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        members = _myopic(_staggered("repeated"))
        members[2050].remove("Generator", "gen-2040")
        layer = PyPSACollection.to_datarecord(
            _over(members, "period"), store.workspace.con
        )
        built = PyPSA.build(store.commit(layer, parent=None).record)
        active = {
            p: built.c.Generator.get_active_assets(p)["gen-2040"]
            for p in built.investment_periods
        }
        assert active == {2030: False, 2040: True, 2050: False}, "retired in 2050"

    def test_an_asset_held_again_after_a_gap_is_refused(self, workspace):
        members = _myopic(_staggered("repeated"))
        members[2040].remove("Generator", "gen-2040")
        with pytest.raises(ValueError, match="'gen-2040' is held in periods"):
            PyPSACollection.to_datarecord(_over(members, "period"), workspace.con)

    def test_a_child_whose_timesteps_repeat_otherwise_builds_back(
        self, staggered_revision, layout
    ):
        """A child whose timesteps repeat otherwise than its parent's restates the `snapshot` tuples.

        Notes
        -----
        - [snapshots and weightings](https://energy-models.github.io/datarecord/reference/tools/#snapshots-and-weightings)
        """
        other = _staggered("distinct" if layout == "repeated" else "repeated")
        record = staggered_revision.record
        child = staggered_revision.store.commit(
            PyPSA.to_datarecord(other, record.con, base=record),
            parent=staggered_revision,
        )
        pd.testing.assert_index_equal(
            PyPSA.build(child.record).snapshots, other.snapshots, exact=False
        )

    @pytest.mark.parametrize("multiperiod", ["repeated"], indirect=True)
    def test_a_multi_period_network_over_a_single_period_base_is_refused(
        self, single_revision, multiperiod
    ):
        record = single_revision.record
        with pytest.raises(ValueError, match="over a single-period `base`"):
            PyPSA.to_datarecord(multiperiod, record.con, results_only=True, base=record)

    @pytest.mark.parametrize(
        ("network", "results_only", "match"),
        [
            pytest.param(
                "plain", False, "with `results_only=True` only", id="no-period-export"
            ),
            pytest.param(
                "plain",
                True,
                "carries investment periods of it",
                id="no-period-results",
            ),
            pytest.param(
                "member", False, "with `results_only=True` only", id="one-period-export"
            ),
        ],
    )
    def test_a_network_over_a_multi_period_base_names_its_periods(
        self,
        multiperiod_revision,
        multiperiod_solved,
        solved,
        network,
        results_only,
        match,
    ):
        """A network not carrying all of `base`'s periods carries some, or was built at one, and is written as results, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        n = solved if network == "plain" else _myopic(multiperiod_solved)[2030]
        record = multiperiod_revision.record
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(n, record.con, results_only=results_only, base=record)

    def test_a_multi_period_record_builds_a_myopic_collection(self, staggered_revision):
        """Without a dim of its own, a multi-period record builds one multi-period network per period, of the generators active in it.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        built = PyPSACollection.build(staggered_revision.record)
        assert built.index.name == "period"
        held = {
            p: (list(n.investment_periods), n.c.Generator.static.index.tolist())
            for p, n in zip(built.index, built.networks, strict=True)
        }
        assert held == {
            2030: ([2030], ["gen-2030"]),
            2040: ([2040], ["gen-2040"]),
            2050: ([2050], ["gen-2040", "gen-2050"]),
        }

    @pytest.mark.parametrize("method", ["build", "verify"])
    def test_a_collection_is_built_over_a_dim_of_its_own_or_period(
        self, single_revision, method
    ):
        with pytest.raises(ValueError, match="with a dim of its own, one network"):
            getattr(PyPSACollection, method)(single_revision.record)

    @pytest.mark.slow
    @pytest.mark.parametrize("results_only", [False, True], ids=["export", "results"])
    def test_a_collection_over_a_dim_of_its_own_builds_back(
        self, store, exported, sweep, results_only
    ):
        """An attribute whose values differ across the members gains the dim, one they agree on is written once, and `build` returns the members.

        A full export writes the halved `p_max_pu` of one member at its label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        record = _collection(sweep, store, exported, "sensitivity", results_only)
        attributes = record.schema.attributes
        assert "sensitivity" in attributes["p_nom_opt"].dims, "solves apart differ"
        assert "sensitivity" not in attributes["x_pu"].dims, (
            "an agreed value as declared"
        )
        assert record.attributes["x_pu"].df()["entity"].is_unique, "written once"
        assert "sensitivity" in record.schema.partial
        built = PyPSACollection.build(record)
        assert built.index.name == "sensitivity"
        assert list(built.index) == ["lo", "hi"], "collection order"
        for label, member in zip(built.index, built.networks, strict=True):
            inputs = () if results_only else (("Generator", "p_max_pu"),)
            _assert_results_equal(member, sweep[label], *inputs)

    @pytest.mark.parametrize(
        ("edit", "attr", "varies"),
        [
            pytest.param(_at_defaults, "marginal_cost", True, id="defaults"),
            pytest.param(_without_a_series, "p_max_pu", True, id="row"),
            pytest.param(_costing(np.inf, 1e4), "marginal_cost", True, id="inf-finite"),
            pytest.param(
                _costing(np.inf, -np.inf), "marginal_cost", True, id="inf-sign"
            ),
            pytest.param(_costing(np.inf, np.inf), "marginal_cost", False, id="inf"),
        ],
    )
    def test_a_collection_varies_what_a_member_holds_otherwise(
        self, store, ac_dc, edit, attr, varies
    ):
        """A member holding an attribute at its defaults alone, lacking a row another holds, or an infinity of another sign or a finite value, differs.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        collection = _over(
            edit({"lo": ac_dc.copy(), "hi": ac_dc.copy()}), "sensitivity"
        )
        layer = PyPSACollection.to_datarecord(collection, store.workspace.con)
        record = store.commit(layer, parent=None).record
        assert ("sensitivity" in record.schema.attributes[attr].dims) == varies

    @pytest.mark.slow
    def test_a_re_solve_writes_once_what_agrees_over_the_dim(self, swept, solved):
        """Results over a `base` whose result varies over the collection's dim are written without it where they agree, reaching every label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        model = _over({"lo": solved, "hi": solved.copy()}, "sensitivity")
        record = _results_over(swept, model).record
        assert "sensitivity" in record.schema.attributes["p_nom_opt"].dims
        rows = record.attributes["p_nom_opt"].df()
        assert rows["sensitivity"].isna().all(), "written once, at every label"
        expected = solved.c.Generator.static["p_nom_opt"]
        pd.testing.assert_series_equal(
            rows.set_index("entity")["value"].reindex(expected.index),
            expected,
            check_names=False,
        )

    @pytest.mark.parametrize(
        ("edit", "dim", "match"),
        [
            pytest.param(_two_levels, ["case", "run"], "one index level", id="levels"),
            pytest.param(dict, "bus", "a dim the PyPSA schema", id="pypsa-dim"),
            pytest.param(dict, "scenario", "holds its scenarios itself", id="scenario"),
            pytest.param(_moved_bus, "sensitivity", "'London' has `x`", id="property"),
            pytest.param(_reordered_buses, "sensitivity", "orders `bus`", id="order"),
            pytest.param(
                _with_custom_column, "sensitivity", "declares other", id="schema"
            ),
            pytest.param(
                _with_custom_column_on_other_types,
                "sensitivity",
                "declares other",
                id="custom-columns",
            ),
            pytest.param(
                _with_region_shapes,
                "region",
                "a dim the members' shapes make",
                id="shape-made-dim",
            ),
            pytest.param(
                _with_newcomer,
                "sensitivity",
                r"holds other Generator .*'Newcomer'",
                id="entity",
            ),
            pytest.param(
                _without_a_snapshot,
                "sensitivity",
                "'hi' holds other snapshots",
                id="snapshots",
            ),
        ],
    )
    def test_a_collection_must_fit_its_dim(self, store, ac_dc, edit, dim, match):
        """A member fits its dim, and holds the snapshots, declares and orders what the others do, with their properties, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        collection = _over(edit({"lo": ac_dc.copy(), "hi": ac_dc.copy()}), dim)
        with pytest.raises(ValueError, match=match):
            PyPSACollection.to_datarecord(collection, store.workspace.con)

    @pytest.mark.parametrize("dim", ["weather_year", "period"])
    def test_a_record_has_one_dim_of_its_own(self, exported, sweep, solved, dim):
        """A collection over a `base` with a dim of its own is over that dim, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        swept = _results_over(exported.commit(), _over(sweep, "sensitivity"))
        collection = _over({"lo": solved, "hi": solved.copy()}, dim)
        with pytest.raises(ValueError, match="collections do not nest"):
            _results_over(swept, collection)

    def test_build_refuses_two_dims_of_its_own(self, exported):
        """A collection's index is one dim, so a record declaring two of its own is reported.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        was = exported.schema
        own = {
            "weather_year": Dimension(dtype="int"),
            "sensitivity": Dimension(dtype="str"),
        }
        revision = replace(
            exported,
            schema=was.model_copy(update={"dimensions": {**was.dimensions, **own}}),
        ).commit()
        missing = PyPSACollection.verify(revision.record)
        assert missing.unsupported_dims == set(own)
        with pytest.raises(UnsupportedRecordError, match="unsupported dims"):
            PyPSACollection.build(revision.record)

    @pytest.mark.parametrize(
        ("select", "expected"),
        [
            pytest.param(
                {"bus": ["London", "Manchester", "Norwich"]},
                {
                    "Bus": ["London", "Norwich", "Manchester"],
                    "Line": ["0", "1", "5"],
                    "Link": [],
                    "Generator": ["Manchester Wind", "Manchester Gas"],
                    "Load": ["London", "Norwich", "Manchester"],
                },
                id="bus",
            ),
            pytest.param(
                {"carrier": ["AC", "wind", "load"]},
                {
                    "Bus": [
                        "London",
                        "Norwich",
                        "Manchester",
                        "Bremen",
                        "Frankfurt",
                        "Norway",
                    ],
                    "Line": ["0", "1", "5", "6"],
                    "Link": [],
                    "Generator": ["Manchester Wind", "Norway Wind", "Frankfurt Wind"],
                    "Carrier": ["wind", "load", "AC"],
                },
                id="carrier",
            ),
            pytest.param(
                {"entity": ["Manchester Wind", "0"]},
                {"Line": ["0"], "Generator": ["Manchester Wind"], "Load": []},
                id="entity",
            ),
        ],
    )
    def test_a_selection_drops_every_label_referring_to_a_dropped_one(
        self, single_revision, select, expected
    ):
        """A bus or entity of another carrier goes, and an entity attached to a dropped bus with it.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        built = PyPSA.build(single_revision.record.select(**select))
        held = {ctype: built.c[ctype].static.index.tolist() for ctype in expected}
        assert held == expected
        built.consistency_check(strict=["unknown_buses", "unknown_carriers"])

    @_SNAPSHOT_SELECTIONS
    def test_selected_snapshots_are_the_networks(
        self, single_revision, ac_dc, value, hours
    ):
        """Snapshots are selected by timestamp and kept, as a list, a partial date or a slice.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        built = PyPSA.build(single_revision.record.select(snapshot=value))
        assert list(built.snapshots) == list(ac_dc.snapshots[hours])
        pd.testing.assert_frame_equal(
            built.c.Generator.dynamic["p_max_pu"],
            ac_dc.c.Generator.dynamic["p_max_pu"].iloc[hours],
            check_index_type=False,
            check_names=False,
            check_like=True,
        )

    @_SNAPSHOT_SELECTIONS
    def test_a_timestamp_selects_its_snapshot_in_every_period(
        self, multiperiod, multiperiod_revision, ac_dc, value, hours
    ):
        """A timestamp selects a multi-period snapshot through `timestep`, in each period holding it, and a period holding none goes.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        built = PyPSA.build(multiperiod_revision.record.select(timestep=value))
        timesteps = multiperiod.snapshots.get_level_values("timestep")
        expected = multiperiod.snapshots[timesteps.isin(ac_dc.snapshots[hours])]
        pd.testing.assert_index_equal(built.snapshots, expected, exact=False)

    @pytest.mark.parametrize(
        ("select", "hours"),
        [
            pytest.param({"timestep": ["2015-01-01 07:00"]}, [7], id="timestamp"),
            pytest.param(
                {"timestep": slice("2015-01-01 06", None)}, [6, 7, 8, 9], id="slice"
            ),
        ],
    )
    def test_one_period_takes_snapshots_by_timestamp(
        self, multiperiod_revision, ac_dc, select, hours
    ):
        """A snapshot selected beside one period is one of that period's, named by its timestep."""
        built = PyPSA.build(multiperiod_revision.record.select(period=2040, **select))
        assert list(built.snapshots) == list(ac_dc.snapshots[hours])

    @pytest.mark.parametrize("layout", ["distinct"], indirect=True)
    @pytest.mark.parametrize("period", [2040, [2040]], ids=["label", "list"])
    def test_a_selection_leaving_no_snapshot_is_refused(
        self, multiperiod_revision, period
    ):
        """Split between the periods, the first hour is 2030's alone."""
        view = multiperiod_revision.record.select(
            period=period, timestep=["2015-01-01 00:00"]
        )
        with pytest.raises(ValueError, match="holding no snapshot"):
            PyPSA.build(view)

    @pytest.mark.slow
    def test_dims_selected_together_combine(self, store, multiperiod):
        """One period, one scenario and two carriers: a deterministic single-period network of the AC buses and their wind generators.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        n = multiperiod.copy()
        n.set_scenarios({"lo": 0.2, "hi": 0.8})
        record = export_network(n, store).record
        view = record.select(period=2030, scenario="hi", carrier=["AC", "wind"])

        assert not PyPSA.verify(view)
        built = PyPSA.build(view)

        assert not built.has_scenarios
        assert not built.investment_periods.size
        assert set(built.c.Bus.static["carrier"]) == {"AC"}
        assert built.c.Generator.static.index.tolist() == [
            "Manchester Wind",
            "Norway Wind",
            "Frankfurt Wind",
        ]

    @pytest.mark.slow
    def test_one_period_reads_activity_per_member(self, store, multiperiod):
        """Which generators are active in 2030 depends on the member, each narrowed at its label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        later = multiperiod.copy()
        later.c.Generator.static.loc["Manchester Gas", "build_year"] = 2040
        sweep = {"lo": multiperiod.copy(), "hi": later}
        record = _collection(sweep, store, None, "sensitivity", results_only=False)

        view = record.select(period=2030)

        assert not PyPSACollection.verify(view)
        built = PyPSACollection.build(view)
        held = {
            label: "Manchester Gas" in n.c.Generator.static.index
            for label, n in zip(built.index, built.networks, strict=True)
        }
        assert held == {"lo": True, "hi": False}, "built in 2040 at `hi`"

    def test_a_null_carrier_refers_to_no_dropped_carrier(self, store, ac_dc):
        """PyPSA's `""` carrier is NULL, so a load without one stays under any carrier selected."""
        n = ac_dc.copy()
        n.c.Load.static.loc["London", "carrier"] = ""
        record = export_network(n, store).record
        built = PyPSA.build(record.select(carrier=["AC"]))
        assert built.c.Load.static.index.tolist() == ["London"]

    @pytest.mark.parametrize("period", [2030, 2040])
    @pytest.mark.slow
    def test_one_period_is_a_single_period_network_of_its_active_entities(
        self, store, ac_dc, layout, period
    ):
        """Over one period the network is indexed by its timesteps, without the period weightings, holding the generators PyPSA holds active in it.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        n = _over_periods(ac_dc, layout)
        n.c.Generator.static.loc["Manchester Gas", "build_year"] = 2040
        n.c.Generator.static.loc["Norway Gas", ["build_year", "lifetime"]] = [2000, 35]
        record = export_network(n, store).record

        built = PyPSA.build(record.select(period=period))

        in_period = n.snapshots.get_level_values("period") == period
        assert not built.investment_periods.size, "single-period"
        assert list(built.snapshots) == list(
            n.snapshots[in_period].get_level_values("timestep")
        ), "indexed by the period's timesteps"
        assert list(built.snapshot_weightings["objective"]) == list(
            n.snapshot_weightings["objective"][in_period]
        )
        active = n.c.Generator.get_active_assets(period)
        assert list(built.c.Generator.static.index) == list(active.index[active])

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (2030, {"co2_limit": np.nan, "cap_2030": np.nan}),
            (2040, {"co2_limit": np.nan, "cap_2040": np.nan}),
            ([2040], {"co2_limit": np.nan, "cap_2040": 2040.0}),
        ],
        ids=str,
    )
    @pytest.mark.slow
    def test_selected_periods_keep_the_global_constraints_applying_in_them(
        self, store, ac_dc, value, expected
    ):
        """A constraint for a period the selection drops goes; one period leaves the kept ones applying in every period, as a single-period network has no period to name.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        n = _over_periods(ac_dc, "repeated")
        n.add(
            "GlobalConstraint",
            ["cap_2030", "cap_2040"],
            investment_period=[2030, 2040],
            carrier_attribute="co2_emissions",
            sense="<=",
            constant=1.0,
        )
        record = export_network(n, store).record

        built = PyPSA.build(record.select(period=value))

        pd.testing.assert_series_equal(
            built.c.GlobalConstraint.static["investment_period"].sort_index(),
            pd.Series(expected, name="investment_period").sort_index(),
            check_index_type=False,
            check_names=False,
        )

    @pytest.mark.parametrize(
        "value", [[2040], slice(2035, None), slice(2040, 2040)], ids=str
    )
    def test_periods_selected_keep_a_multi_period_network(
        self, multiperiod, multiperiod_revision, value
    ):
        """A list or a slice of periods keeps the dim: their snapshots, weightings and every entity.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        built = PyPSA.build(multiperiod_revision.record.select(period=value))
        in_period = multiperiod.snapshots.get_level_values("period") == 2040
        pd.testing.assert_index_equal(
            built.snapshots, multiperiod.snapshots[in_period], exact=False
        )
        pd.testing.assert_frame_equal(
            built.investment_period_weightings,
            multiperiod.investment_period_weightings.loc[[2040]],
            check_index_type=False,
        )
        pd.testing.assert_index_equal(
            built.c.Generator.static.index, multiperiod.c.Generator.static.index
        )

    @pytest.fixture
    def stochastic(self, store, ac_dc):
        """`ac_dc` over three weighted scenarios, `hi` with a marginal cost of its own."""
        n = ac_dc.copy()
        n.set_scenarios({"lo": 0.2, "mid": 0.3, "hi": 0.5})
        n.c.Generator.static.loc[("hi", "Norway Gas"), "marginal_cost"] = 99.0
        return export_network(n, store)

    def test_one_scenario_is_a_deterministic_network_of_its_values(self, stochastic):
        built = PyPSA.build(stochastic.record.select(scenario="hi"))
        assert not built.has_scenarios
        assert built.c.Generator.static.loc["Norway Gas", "marginal_cost"] == 99.0

    @pytest.mark.parametrize(
        ("stored", "expected"),
        [
            pytest.param(
                {"lo": 0.2, "mid": 0.3},
                {"lo": 0.2 / 0.5, "mid": 0.3 / 0.5},
                id="stored",
            ),
            pytest.param(
                {"lo": 0.2},
                {"lo": 0.2 / (0.2 + 1 / 3), "mid": (1 / 3) / (0.2 + 1 / 3)},
                id="unstored",
            ),
        ],
    )
    def test_selected_scenarios_are_renormalised(self, stochastic, stored, expected):
        """A scenario no `scenario_weight` row reaches takes its equal share of the whole axis before the kept weights are renormalised.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        path = Path(
            stochastic.store.layer_dir(stochastic.id),
            "attributes",
            "scenario_weight.parquet",
        )
        rows = pd.read_parquet(path)
        rows["value"] = rows["scenario"].map(stored)
        rows.dropna(subset="value").to_parquet(path, index=False)

        built = PyPSA.build(stochastic.record.select(scenario=["lo", "mid"]))

        weights = built.scenario_weightings["weight"].to_dict()
        assert weights == pytest.approx(expected)

    @pytest.mark.slow
    def test_selected_scenarios_are_renormalised_per_member(self, store, ac_dc):
        """Each member of a collection renormalises the kept scenarios' weights of its own."""
        stored = {
            "a": {"lo": 0.2, "mid": 0.3, "hi": 0.5},
            "b": {"lo": 0.6, "mid": 0.2, "hi": 0.2},
        }
        members = {label: ac_dc.copy() for label in stored}
        for label, n in members.items():
            n.set_scenarios(stored[label])
        record = _collection(members, store, None, "sensitivity", results_only=False)

        built = PyPSACollection.build(record.select(scenario=["lo", "mid"]))

        weights = {
            label: n.scenario_weightings["weight"].to_dict()
            for label, n in zip(built.index, built.networks, strict=True)
        }
        assert weights == {
            "a": pytest.approx({"lo": 0.4, "mid": 0.6}),
            "b": pytest.approx({"lo": 0.75, "mid": 0.25}),
        }

    def test_selected_scenarios_weighing_nothing_are_refused(self, store, ac_dc):
        n = ac_dc.copy()
        n.set_scenarios({"lo": 0.0, "mid": 0.0, "hi": 1.0})
        record = export_network(n, store).record
        with pytest.raises(ValueError, match="weighing nothing"):
            PyPSA.build(record.select(scenario=["lo", "mid"]))

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(2013, id="scalar"),
            pytest.param([2013], id="list"),
            pytest.param(slice(2013, None), id="slice"),
        ],
    )
    @pytest.mark.slow
    def test_a_dim_of_its_own_selected_is_a_network_or_a_collection(
        self, store, sweep, value
    ):
        """One label of the record's own dim builds that member, a list or a slice of its numbers the collection of them.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        years = {2012: sweep["lo"], 2013: sweep["hi"]}
        record = _collection(years, store, None, "weather_year", results_only=False)
        view = record.select(weather_year=value)
        if isinstance(value, int):
            built = PyPSA.build(view)
        else:
            collection = PyPSACollection.build(view)
            assert list(collection.index) == [2013]
            (built,) = collection.networks
        _assert_results_equal(built, sweep["hi"], ("Generator", "p_max_pu"))

    @pytest.mark.parametrize("results_only", [False, True])
    def test_a_network_is_not_written_over_a_record_with_a_dim_of_its_own(
        self, store, sweep, results_only
    ):
        """A network over the whole of a record with a dim of its own would reach every label, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        years = {2012: sweep["lo"], 2013: sweep["hi"]}
        record = _collection(years, store, None, "weather_year", results_only=False)
        with pytest.raises(
            ValueError, match=r"with a dim of its own, \['weather_year'\]"
        ):
            PyPSA.to_datarecord(
                sweep["lo"], store.workspace.con, results_only=results_only, base=record
            )

    @pytest.mark.parametrize(("nudge", "agreed"), [(1e-9, True), (1e-3, False)])
    def test_members_agree_on_a_float_within_tolerance(self, workspace, nudge, agreed):
        """A bus's `x` the members hold within a relative 1e-5 is one value, the first member's; further apart it is refused.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        members = {2013: _north(), 2019: _north()}
        members[2013].c.Bus.static["x"] = 1.0
        members[2019].c.Bus.static["x"] = 1.0 + nudge
        collection = _over(members, "weather_year")
        if not agreed:
            with pytest.raises(ValueError, match="has `x`"):
                PyPSACollection.to_datarecord(collection, workspace.con)
            return
        record = PyPSACollection.to_datarecord(collection, workspace.con)
        assert record.properties["bus"].df()["x"].tolist() == [1.0]

    def test_a_member_holds_the_entities_its_label_keeps(self, workspace, tmp_path):
        """An entity whose property names one label of the dim of its own is no other member's, built whole or at one label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/reference/tools/#a-collection)
        """
        members = {2013: _north(), 2019: _north()}
        for n in members.values():
            n.add("Generator", "only2019", bus="north")
        swept = workspace.store(str(tmp_path / "swept")).commit(
            PyPSACollection.to_datarecord(
                _over(members, "weather_year"), workspace.con
            ),
            parent=None,
        )
        schema = swept.manifest.schema
        entity = {
            **schema.properties["entity"],
            "weather_year": PropertySpec(one_of="weather_year"),
        }
        properties = {**schema.properties, "entity": entity}
        layer = empty_root(
            workspace.store(str(tmp_path / "tagged")),
            schema.model_copy(update={"properties": properties}),
        )
        source = Path(swept.store.layer_dir(swept.id))
        for kind in ("dims", "attributes"):
            shutil.copytree(source / kind, layer.dir / kind)
        path = layer.dir / "dims" / "entity.parquet"
        frame = pd.read_parquet(path)
        frame["weather_year"] = frame["entity"].map({"only2019": 2019}).astype("Int64")
        frame.to_parquet(path, index=False)
        record = layer.commit().record

        whole = PyPSACollection.build(record)
        held = {
            label: n.c.Generator.static.index.tolist()
            for label, n in zip(whole.index, whole.networks, strict=True)
        }
        assert held == {2013: ["gen"], 2019: ["gen", "only2019"]}
        one = PyPSA.build(record.select(weather_year=2013))
        assert one.c.Generator.static.index.tolist() == held[2013]

    @pytest.mark.parametrize(
        "labels",
        [
            {"snapshot": "2015-01-01 03:00"},
            {"bus": "London"},
            {"entity": "Manchester Wind"},
            {"carrier": "AC"},
            {"ctype": "Generator"},
        ],
        ids=lambda labels: next(iter(labels)),
    )
    @pytest.mark.parametrize(
        "method", [PyPSA.build, PyPSA.verify], ids=["build", "verify"]
    )
    def test_a_selection_squeezing_a_dim_the_network_keeps_is_refused(
        self, single_revision, labels, method
    ):
        """A network is built from a selection squeezing `period`, `scenario` or a dim of the record's own alone; any other dim takes a list of one.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/reference/tools/#selecting-before-a-build)
        """
        (dim,) = labels
        with pytest.raises(ValueError, match=rf"not \['{dim}'\]"):
            method(single_revision.record.select(**labels))
