# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The tool layer: verify a record, build a model, read it back.

Notes
-----
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

import shutil
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import pandas as pd
import pypsa
import pytest
from duckdb import DuckDBPyRelation

from datarecord import Workspace
from datarecord.schema.categories import matching
from datarecord.schema.specs import Dimension
from datarecord.store.manifest import read_schema
from datarecord.tools.base import Requirements, Schema, UnsupportedRecordError
from datarecord.tools.pypsa import (
    CTYPE,
    PyPSA,
    _colliding_names,
    _registry,
    _select,
    _weightings,
)
from tests.fixtures import (
    empty_child,
    export_network,
    relation,
    schema,
    unpacked,
    write_attribute,
    write_component,
    write_schema,
)
from tests.test_roundtrip import assert_networks_equal


@pytest.fixture(scope="module")
def single_revision(tmp_path_factory, ac_dc):
    """`ac_dc` exported once for the module, for tests that only read it."""
    workspace = Workspace()
    yield export_network(ac_dc, workspace.store(str(tmp_path_factory.mktemp("ac_dc"))))
    workspace.close()


@pytest.fixture(scope="module")
def solved(single_revision):
    """The network `single_revision` builds, optimised once for the module, its solver model dropped so it copies."""
    n = PyPSA.build(single_revision.record)
    n.optimize(solver_name="highs")
    n.model.solver_model = None
    return n


@pytest.fixture
def exported(single_revision, workspace, tmp_path):
    """A copy of `single_revision`'s store, for a test to rewrite its files or schema."""
    uri = str(tmp_path / "ac_dc")
    shutil.copytree(single_revision.store.uri, uri)
    (revision,) = workspace.store(uri).revisions()
    return revision


def _results_over(revision, n, collection_dim=None):
    """`n`'s results, written as a thin layer over `revision`."""
    layer = PyPSA.to_datarecord(
        n,
        revision.record.con,
        results_only=True,
        base=revision.record,
        collection_dim=collection_dim,
    )
    return revision.child(layer)


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


def _with_schema(revision, **kwargs) -> None:
    """Redeclare the record's schema, keeping the attributes it already declares.

    Written directly rather than through `write_record`, which would reject an
    incompatible redeclaration - here the point is to hand the tool a
    schema it must report on rather than one the writer accepted.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    was = read_schema(revision.store)
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
    write_schema(now, revision.store)


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
    """`ac_dc` over two investment periods, with weightings of its own on both axes.

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
    n.snapshot_weightings = n.snapshot_weightings.assign(
        objective=np.arange(len(n.snapshots), dtype=float)
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
    collection = pypsa.NetworkCollection(sweep)
    if results_only:
        return _results_over(exported, collection, dim).record
    layer = PyPSA.to_datarecord(collection, store.workspace.con, collection_dim=dim)
    return store.root(layer).record


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
    return pd.Series(
        list(members.values()),
        index=pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["case", "run"]),
    )


def _moved_bus(members):
    members["hi"].c.Bus.static.loc["London", "x"] += 1.0
    return members


def _moved_bus_and_capacity(members):
    members["hi"].c.Generator.static.loc["Norway Gas", "p_nom"] += 1.0
    return _moved_bus(members)


def _reordered_buses(members):
    members["hi"].c.Bus.static = members["hi"].c.Bus.static.iloc[::-1]
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


def test_requirements_falsy_when_empty():
    """`bool(Requirements())` distinguishes "nothing missing" from "nothing checked"."""
    assert not Requirements()
    assert Requirements(dims=frozenset({"snapshot"}))
    assert "dims ['snapshot']" in Requirements(dims=frozenset({"snapshot"})).describe()
    assert Requirements().describe() == "nothing"


def test_the_record_layer_imports_no_tool():
    """`datarecord` is framework-free: reading a record never imports PyPSA.

    The call runs from the tool inward (`PyPSA.build(revision.record)`), so there is no
    registry and no name dispatch to drag a framework in.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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


def test_a_second_tool_needs_no_record_change():
    """A tool is a plain object taking a record; adding one changes nothing here.

    No registration and no name dispatch: conformance to `Tool` is structural,
    so a second framework's module defines its own singleton and callers import
    it.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    from datarecord.tools.base import Tool

    class FakeTool:
        name = "fake"
        schema = Schema()

        def requires(self, record):
            return Requirements(dims=frozenset({"snapshot"}))

        def verify(self, record):
            return Requirements(categories=frozenset({"Nope"}))

        def build(self, record):
            return "fake-model"

        def to_datarecord(self, model, con, *, results_only=False, base=None):
            return f"layer-of-{model}"

    assert isinstance(FakeTool(), Tool), "conformance to `Tool` is structural"


_SNAPSHOT_SELECTIONS = pytest.mark.parametrize(
    ("value", "hours"),
    [
        pytest.param("2015-01-01 03:00", [3], id="scalar"),
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

    def test_build_takes_only_a_record(self, workspace, ac_dc):
        """A `RecordLike` that is no `Record` is refused, the tool's own network source included."""
        with pytest.raises(TypeError, match="builds from a Record"):
            PyPSA.build(PyPSA.to_datarecord(ac_dc, workspace.con))

    def test_verify_accepts_a_complete_record(self, single_revision):
        """A record written by `export_to_parquet` supplies everything PyPSA needs."""
        assert not PyPSA.verify(single_revision.record)

    @pytest.mark.parametrize("dim", ["ctype", "role"])
    def test_the_tool_writes_no_type_or_role_axis(self, single_revision, dim):
        """The schema lists every type and role, so the record reads them from it.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
        """
        record = single_revision.record
        layer = Path(single_revision.store.layer_dir(single_revision.id))
        assert not (layer / "dims" / f"{dim}.parquet").exists()
        one_of = record.schema.dimensions[dim].one_of
        assert one_of is not None
        assert record.dims[dim].df()[dim].tolist() == list(one_of), (
            "every label of `one_of`, those the network leaves unused included"
        )

    @pytest.mark.parametrize("role", ["input", "output", "attached"])
    def test_flags_per_role_name_the_attributes_reaching_a_connection_of_it(
        self, single_revision, role
    ):
        """`role` categorizes `connection`, so `flags(role=...)` reads as `flags(ctype=...)` does.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
        """
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

    def test_buses_and_carriers_are_labels_of_their_dims(self, single_revision, ac_dc):
        """`n.buses` and `n.carriers` are the `bus` and `carrier` axes, under PyPSA's names.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
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
        assert record.schema.attributes["v_nom"].dims == {"bus"}
        assert record.schema.attributes["co2_emissions"].dims == {"carrier"}

        built = PyPSA.build(record)
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
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
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
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
        - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
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
        - [add / remove](https://energy-models.github.io/datarecord/design/use/editing/#add-remove)
        """
        from datarecord.mutable import WorkingRecord

        w = WorkingRecord(single_revision.record)
        with pytest.raises(KeyError, match="coal"):
            w.set("entity.carrier", "coal", entity="Manchester Gas")
        with pytest.raises(ValueError, match="entity.carrier"):
            w.remove("carrier", ["gas"])

    @pytest.mark.slow
    def test_a_value_its_type_does_not_take_is_reported_and_left_out(
        self, single_revision
    ):
        """A broadcast reaches every entity; the build keeps it to the types taking it.

        Notes
        -----
        - [a value its type does not take](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-value-its-type-does-not-take)
        """
        from datarecord.mutable import WorkingRecord

        w = WorkingRecord(single_revision.record)
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
                    lambda w: w.set("p_max_pu", 0.9, scenario="high"),
                    lambda w: w.set("p_max_pu", 0.5, entity="g1"),
                ],
                {
                    ("low", "g1"): 0.5,
                    ("low", "g2"): 1.0,
                    ("high", "g1"): 0.5,
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
        ids=["scenario-crosses-entities", "entity-crosses-a-scenario", "added-member"],
    )
    def test_a_build_settles_rows_of_several_layers_by_the_broadcast_rule(
        self, store, layers, built
    ):
        """Rows as written, a deeper layer crossing a shallower one, build each member's value.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
        """
        import pypsa

        from datarecord.mutable import WorkingRecord

        n = pypsa.Network()
        n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
        n.add("Bus", "b")
        n.add("Generator", ["g1", "g2"], bus="b")
        n.set_scenarios({"low": 0.5, "high": 0.5})
        revision = export_network(n, store)
        for edit in layers:
            w = WorkingRecord(revision.record)
            edit(w)
            revision = revision.child(w.edits)

        static = PyPSA.build(revision.record).c.Generator.static
        assert static["p_max_pu"].to_dict() == built

    def test_verify_reports_an_entity_attached_to_no_bus(self, single_revision):
        """Removing a bus removes its connections; what was attached to it is unbuildable.

        Notes
        -----
        - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
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
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        revision = exported
        _with_schema(revision, dims={"snapshot": "datetime"}, partial=set())

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
        - [entity types](https://energy-models.github.io/datarecord/design/model/properties/#categories)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        revision, store = exported, exported.store
        # Declared alongside PyPSA's own types, since the `one_of` labels pin the
        # vocabulary.
        declared = read_schema(store)
        assert declared.categorizing("entity") == (CTYPE,)
        dim = declared.dimensions[CTYPE]
        assert dim.one_of is not None, "PyPSA lists its types on the dim"
        write_schema(
            declared.model_copy(
                update={
                    "dimensions": {
                        **declared.dimensions,
                        CTYPE: dim.model_copy(
                            update={"one_of": (*dim.one_of, "Widget")}
                        ),
                    }
                }
            ),
            revision.store,
        )
        write_component(revision, "Widget", [{"entity": "w1"}])

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
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        """
        revision = exported
        _with_schema(revision, partial={"scenario", "snapshot"})

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
        revision, store = exported, exported.store
        # A Generator member with no connection row supplying its bus
        # (https://energy-models.github.io/datarecord/design/tools/pypsa/#ports).
        write_component(revision, "Generator", [{"entity": "g1"}])
        # One file across every type, so the Generators' rows are dropped from it
        # rather than a per-type file being unlinked.
        path = Path(store.layer_dir(revision.id), "dims", "connection.parquet")
        rows = pd.read_parquet(path)
        generators = set(ac_dc.c["Generator"].static.index) | {"g1"}
        entities = rows["connection"].map(lambda c: c["entity"])
        rows[~entities.isin(generators)].to_parquet(path, index=False)

        write_component(revision, "Line", [{"entity": "l1"}])

        missing = PyPSA.verify(revision.record)
        assert ("Generator", "bus") in missing.attributes
        assert ("Line", "x") not in missing.attributes, (
            "PyPSA declares the required `x` with a default, resolvable with no row"
        )

    def test_verify_reports_a_piecewise_linear_attribute(self, exported):
        """A curve is stored correctly; it is the PyPSA translation that cannot express it.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
        """
        revision, store = exported, exported.store
        write_attribute(
            store.layer_dir(revision.id),
            "marginal_cost",
            [{"entity": "Manchester Wind", "value": 20.0}],
        )
        assert not PyPSA.verify(revision.record).unsupported_values, (
            "the same attribute without breakpoints is not reported"
        )

        revision = empty_child(revision)
        write_attribute(
            store.layer_dir(revision.id),
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

        missing = PyPSA.verify(revision.record)
        assert ("Generator", "marginal_cost") in missing.unsupported_values
        assert "piecewise-linear" in missing.describe()
        with pytest.raises(UnsupportedRecordError):
            PyPSA.build(revision.record)

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
        - [standard types](https://energy-models.github.io/datarecord/design/tools/pypsa/#standard-types)
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
        - [standard types](https://energy-models.github.io/datarecord/design/tools/pypsa/#standard-types)
        """
        held = set(single_revision.record.dims)
        assert not held & {"line_type", "transformer_type"}

    def test_to_datarecord_rejects_a_cross_type_name_collision(self, workspace):
        """PyPSA scopes names per type; a record scopes them across every entity type.

        Reported rather than repaired: renaming to `Generator:north` would hand back
        a network whose components PyPSA can no longer find by their own names, so
        the mismatch is the caller's to reconcile.

        Notes
        -----
        - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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
        ("columns", "placed"),
        [
            ({"Bus": ("country", "DE")}, ("property", "bus")),
            ({"Bus": ("nom_max_onwind_2050", 5.0)}, ("attribute", "bus")),
            ({"Generator": ("country", "DE")}, ("property", "entity")),
            ({"Generator": ("location", "site")}, ("property", "entity")),
            ({"Generator": ("e_nom", 5.0)}, ("attribute", "entity")),
            ({"Generator": ("v_nom", 1.0)}, ("property", "entity")),
            ({"Carrier": ("efficiency", 0.9)}, ("property", "carrier")),
            (
                {"Bus": ("country", "DE"), "Generator": ("country", "DE")},
                ("property", "bus"),
            ),
            ({"Bus": ("share", 0.5), "Generator": ("share", 0.5)}, ("property", "bus")),
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
        self, store, columns, placed
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
        (name, _), *_ = columns.values()
        kind, dim = placed
        as_property = name in record.schema.properties[dim]
        assert as_property == (kind == "property"), f"{name} is a {kind} of {dim}"
        if kind == "attribute":
            assert record.schema.attributes[name].dims == {dim}
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
        - [custom columns](https://energy-models.github.io/datarecord/design/tools/pypsa/#custom-columns)
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
        - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
        - [custom columns](https://energy-models.github.io/datarecord/design/tools/pypsa/#custom-columns)
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
        - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
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
        - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
        """
        n = _shaped()
        edit(n)
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(n, workspace.con)

    def test_a_shape_made_dim_is_no_dim_of_its_own(self, store):
        """A collection over a dim of its own beside the `country` shapes make builds back as a collection over that dim alone.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        members = {2013: _shaped(), 2014: _shaped()}
        members[2014].c.Generator.static["p_nom"] = 2.0
        layer = PyPSA.to_datarecord(
            pypsa.NetworkCollection(members),
            store.workspace.con,
            collection_dim="weather_year",
        )
        record = store.root(layer).record
        assert not PyPSA.verify(record), "`country` is not a second dim of its own"
        built = PyPSA.build(record)
        assert isinstance(built, pypsa.NetworkCollection)
        assert built.index.name == "weather_year"

    def test_a_stochastic_network_holds_its_shapes_per_scenario(self, store):
        """A stochastic network's shapes are indexed by `(scenario, name)`, as its other components are.

        Notes
        -----
        - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
        """
        from geopandas.testing import assert_geodataframe_equal

        n = _shaped()
        n.set_scenarios(["a", "b"])
        built = PyPSA.build(export_network(n, store).record)
        assert built.c.Shape.static.index.names == ["scenario", "name"]
        assert_geodataframe_equal(_shapes(built), _shapes(n), check_like=True)

    def test_a_bus_selection_keeps_every_label_a_shape_made(self, store):
        """`country` is no dim `select` takes, so selecting buses keeps every country shape, though `bus.country` refers to it, and drops the other bus's.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        record = export_network(_shaped(), store).record
        built = PyPSA.build(record, select={"bus": ["north"]})
        outlined = _shapes(built).index.droplevel("type").unique().tolist()
        assert outlined == [
            ("Bus", "north"),
            ("Generator", "gen"),
            ("country", "DE"),
            ("country", "FR"),
        ]
        with pytest.raises(ValueError, match="takes labels of"):
            PyPSA.build(record, select={"country": "DE"})

    def test_sub_networks_are_labels_of_their_own_dim(self, store, ac_dc):
        """PyPSA numbers SubNetworks `0`, `1`, ... as it numbers Lines, which a dim of their own keeps apart.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        n = ac_dc.copy()
        n.determine_network_topology()
        assert set(n.c["SubNetwork"].static.index) & set(n.c["Line"].static.index)

        revision = export_network(n, store)
        record = revision.record
        names = _sub_networks(revision)
        assert names == [str(i) for i in range(4)]
        pointed = set(record.attributes["sub_network"].df()["value"])
        assert pointed <= set(names), (
            "a `sub_network` value names a `sub_network` label"
        )
        carriers = record.properties["sub_network"].df()["carrier"]
        assert set(carriers) <= set(record.dims["carrier"].df()["carrier"])

        built = PyPSA.build(record)
        assert sorted(built.c["SubNetwork"].static.index) == ["0", "1", "2", "3"]
        built.determine_network_topology()
        assert sorted(built.c["SubNetwork"].static.index) == ["0", "1", "2", "3"], (
            "a built network passes PyPSA's topology code"
        )

    def test_a_derived_v_nom_stays_on_the_bus(self, store, ac_dc):
        """`calculate_dependent_values` copies the bus's `v_nom` onto lines and shunts.

        The record keeps `v_nom` over `bus` alone, and `build` gets the copies
        back once PyPSA recomputes them.
        """
        n = ac_dc.copy()
        n.add("ShuntImpedance", "shunt", bus="London", b=0.1)
        n.calculate_dependent_values()

        record = export_network(n, store).record
        assert record.schema.attributes["v_nom"].dims == {"bus"}

        built = PyPSA.build(record)
        built.calculate_dependent_values()
        assert_networks_equal(built, n)

    def test_a_solved_network_exports(self, store, solved):
        """`optimize` leaves the derived `Line.v_nom` behind, which `build` recomputes."""
        built = PyPSA.build(export_network(solved, store).record)
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

    def test_pypsa_schema_is_the_identity(self, single_revision):
        """PyPSA defines the record vocabulary, so nothing is renamed.

        The seam still routes every attribute, so an entry added later takes
        effect with no change to `build`.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        assert PyPSA.schema.attrs == {}
        assert PyPSA.schema.sources("Generator", "p_max_pu") == ("p_max_pu",)
        record = single_revision.record
        identity = PyPSA.schema.resolve(
            record, "Generator", "p_max_pu", {}, record.flags()
        )
        assert identity.fetchall() == relation(single_revision, "p_max_pu").fetchall()

    def test_a_solved_network_exports_its_results_long_form(
        self, single_revision, solved
    ):
        """A solved network's results are attributes, keyed by name, long-form.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        n = solved

        results = PyPSA.to_datarecord(n, single_revision.record.con).attributes
        assert "p" in results
        assert "p_nom_opt" in results

        # Relations, lazy so a tool may fetch on demand (https://energy-models.github.io/datarecord/design/tools/).
        assert isinstance(results["p"], DuckDBPyRelation)
        p = results["p"].df()
        # The long schema's columns (https://energy-models.github.io/datarecord/design/use/reading/), so the write path can persist it as-is -
        # and no `ctype`, an attribute row being keyed by `name` (https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim).
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

    def test_an_export_skips_results_still_at_their_default(self, single_revision):
        """An unsolved network yields no `p`/`p_nom_opt` rows.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        n = PyPSA.build(single_revision.record)
        results = PyPSA.to_datarecord(n, single_revision.record.con).attributes
        assert "p" not in results
        assert "p_nom_opt" not in results

    def test_a_solved_p_is_one_row_per_connection(self, single_revision, solved):
        """`p` holds a row per `(entity, bus)` per snapshot, each port's own sign.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
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

    def test_bus_results_are_keyed_by_bus(self, exported, solved):
        """`marginal_price`, `v_mag_pu` and `v_ang` are over `bus`; `Bus.p` and the rest are derived.

        `build` copies a result into the network like any attribute.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        revision = _results_over(exported, solved)
        schema = revision.record.schema
        over_bus = {
            a for a, spec in _registry().schema.attributes.items() if "bus" in spec.dims
        }
        assert over_bus - _registry().inputs == {"marginal_price", "v_mag_pu", "v_ang"}
        assert not solved.c["Bus"].dynamic["p"].empty, "PyPSA did compute a `Bus.p`"
        assert "bus" not in schema.attributes["p"].dims

        expected = solved.c["Bus"].dynamic["marginal_price"]
        wide = _prices(revision)
        assert np.allclose(wide[expected.columns].reindex(expected.index), expected)

        built = PyPSA.build(revision.record).c["Bus"].dynamic["marginal_price"]
        pd.testing.assert_frame_equal(
            built[expected.columns],
            expected,
            check_names=False,
            check_freq=False,
            check_index_type=False,
        )

    @pytest.mark.slow
    def test_a_child_reads_its_parents_results_and_the_latest_wins(
        self, exported, solved
    ):
        """A result is an attribute: it inherits through layers, a later one standing over it.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        revision = _results_over(exported, solved)
        expected = _prices(revision)
        pd.testing.assert_frame_equal(_prices(empty_child(revision)), expected)

        n = PyPSA.build(revision.record)
        n.c["Bus"].dynamic["marginal_price"] *= 2
        resolved = _results_over(revision, n)
        pd.testing.assert_frame_equal(_prices(empty_child(resolved)), expected * 2)

    @pytest.mark.slow
    def test_a_re_solve_leaves_no_stale_sub_network(self, exported, solved):
        """A results layer replaces `base`'s SubNetworks rather than adding to them.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        revision = _results_over(exported, solved)
        *kept, dropped = sorted(solved.c["SubNetwork"].static.index)
        assert _sub_networks(revision) == sorted([*kept, dropped])
        entities = revision.record.dims["entity"].df()
        assert entities.equals(exported.record.dims["entity"].df()), (
            "a results layer leaves the entity axis alone"
        )

        n = PyPSA.build(revision.record)
        n.remove("SubNetwork", dropped)
        resolved = _results_over(revision, n)
        assert _sub_networks(resolved) == sorted(kept)
        slack = resolved.record.attributes["slack_bus"].df()
        assert dropped not in set(slack["sub_network"]), "its results went with it"

    @pytest.mark.parametrize(
        ("over_solved", "listed"),
        [
            pytest.param(False, None, id="unsolved"),
            pytest.param(True, [], id="solved", marks=pytest.mark.slow),
        ],
    )
    def test_results_without_sub_networks_list_none(
        self, exported, solved, over_solved, listed
    ):
        """A solve without SubNetworks restates a `sub_network` axis `base` has empty, and writes none where it has none.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        base = _results_over(exported, solved) if over_solved else exported
        n = PyPSA.build(base.record)
        n.remove("SubNetwork", n.c["SubNetwork"].static.index)
        n.c["Bus"].dynamic["marginal_price"] = pd.DataFrame(
            1.0, index=n.snapshots, columns=n.c["Bus"].static.index
        )
        record = _results_over(base, n).record
        axis = record.dims.get("sub_network")
        assert (None if axis is None else _sub_networks_of(axis)) == listed
        assert "marginal_price" in record.attributes, "the results are written"
        slack = record.attributes.get("slack_bus")
        assert slack is None or slack.df().empty, "no stale SubNetwork keeps a result"

    @pytest.mark.slow
    @pytest.mark.parametrize(
        ("ctype", "attr"), [("Line", "mu_upper"), ("Bus", "marginal_price")]
    )
    def test_a_result_a_re_solve_leaves_out_reads_as_null(
        self, exported, solved, ctype, attr
    ):
        """A results layer restates `base`'s results it does not write as NULL.

        Built back, the network holds PyPSA's own default there.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        n = PyPSA.build(_results_over(exported, solved).record)
        n.c[ctype].dynamic[attr] = pd.DataFrame(
            1.0, index=n.snapshots, columns=n.c[ctype].static.index
        )
        base = _results_over(exported, n)
        n = PyPSA.build(base.record)
        n.c[ctype].dynamic[attr] = n.c[ctype].dynamic[attr].iloc[:, :0]
        resolved = _results_over(base, n)

        def read(revision, name):
            df = revision.record.attributes[name].df()
            by = [c for c in df.columns if c not in ("breakpoint", "value")]
            return df.sort_values(by).reset_index(drop=True)

        stale = read(base, attr)
        keys = [c for c in stale.columns if c not in ("breakpoint", "value")]
        values = stale[keys].merge(read(resolved, attr), on=keys)["value"]
        assert (stale["value"] == 1.0).all()
        assert len(values) == len(stale)
        assert values.isna().all(), "the stale value is gone"
        c = PyPSA.build(resolved.record).c[ctype]
        assert (c.dynamic[attr] == c.defaults.at[attr, "default"]).all().all(), (
            "PyPSA fills its own default"
        )
        pd.testing.assert_frame_equal(read(resolved, "p_nom"), read(base, "p_nom"))

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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        base = _results_over(exported, solved)
        n = PyPSA.build(base.record, select=select)
        n.determine_network_topology()
        n.c.Bus.dynamic["marginal_price"] += 1.0
        resolved = _results_over(base, n)

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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        base = _results_over(exported, solved)
        n = PyPSA.build(base.record, select={"snapshot": list(solved.snapshots[:2])})
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

    @pytest.mark.slow
    def test_a_re_solve_of_a_scenario_in_a_list_keeps_the_others(self, stochastic):
        """A network of one scenario kept in a list writes its results over `scenario` at that scenario alone, and one over no `scenario` for every scenario.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        n = PyPSA.build(stochastic.record)
        buses = n.c.Bus.static.index
        n.c.Bus.dynamic["marginal_price"] = pd.DataFrame(
            1.0, index=n.snapshots, columns=buses
        )
        base = _results_over(stochastic, n)
        m = PyPSA.build(base.record, select={"scenario": ["hi"]})
        m.c.Bus.dynamic["marginal_price"] += 1.0
        m.c.Generator.static["p_nom_opt"] = 9.0
        built = PyPSA.build(_results_over(base, m).record)
        prices = built.c.Bus.dynamic["marginal_price"]
        assert (prices["hi"] == 2.0).all().all(), "the re-solved scenario's"
        assert (prices[["lo", "mid"]] == 1.0).all().all(), "the others' kept"
        assert (built.c.Generator.static["p_nom_opt"] == 9.0).all(), (
            "a first-stage result is one value across every scenario"
        )

    @pytest.fixture
    def swept(self, exported, sweep):
        """`exported` with `sweep`'s results over the dim of its own `sensitivity`."""
        return _results_over(exported, pypsa.NetworkCollection(sweep), "sensitivity")

    @pytest.mark.parametrize(
        ("base", "select", "match"),
        [
            pytest.param(
                "stochastic",
                {"scenario": "hi"},
                "with the scenario in a list",
                id="scenario",
            ),
            pytest.param(
                "swept",
                {"sensitivity": "lo"},
                "the collection `build` gives",
                id="own",
                marks=pytest.mark.slow,
            ),
        ],
    )
    def test_a_network_of_one_label_is_refused_over_the_whole_dim(
        self, request, base, select, match
    ):
        """A network built at one label of `scenario` or of a dim of its own writes results NULL on it, which would reach every label of `base`.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        revision = request.getfixturevalue(base)
        n = PyPSA.build(revision.record, select=select)
        with pytest.raises(ValueError, match=match):
            _results_over(revision, n)

    def test_results_only_takes_base(self, single_revision, solved):
        """A results layer is written over the record it was built from.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        with pytest.raises(ValueError, match="takes `base` with `results_only=True`"):
            PyPSA.to_datarecord(solved, single_revision.record.con, results_only=True)

    def test_a_collection_is_exported_over_nothing(self, single_revision, sweep):
        """A full export of a collection takes no `base`."""
        record = single_revision.record
        with pytest.raises(ValueError, match="exported whole over nothing"):
            PyPSA.to_datarecord(
                pypsa.NetworkCollection(sweep),
                record.con,
                base=record,
                collection_dim="sensitivity",
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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        n = PyPSA.build(exported.record)
        assert n.c.Generator.static.at["Manchester Wind", attr] != value
        n.c.Generator.static.at["Manchester Wind", attr] = value
        layer = PyPSA.to_datarecord(n, exported.record.con, base=exported.record)
        child = PyPSA.build(exported.child(layer).record)
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
        n = PyPSA.build(exported.record)
        n.add("Generator", "Newcomer", bus="Manchester")
        layer = PyPSA.to_datarecord(n, exported.record.con, base=exported.record)
        rows = layer.attributes[attr].df()
        assert entity not in set(rows["entity"]), "the layer stays thin"
        child = PyPSA.build(exported.child(layer).record)
        expected = n.c.Generator.static.at[entity, attr]
        assert child.c.Generator.static.at[entity, attr] == expected

    def test_link_internal_power_is_p_internal(self, single_revision, solved):
        """`Link.p` is power at no bus: an entity-level `p_internal`, no `bus`.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
        """
        n = solved

        results = PyPSA.to_datarecord(n, single_revision.record.con).attributes
        internal = results["p_internal"].df()
        links = list(n.c["Link"].static.index)
        assert "bus" not in internal.columns
        assert set(internal["entity"]) == set(links)
        assert len(internal) == len(n.snapshots) * len(links)
        assert "p_internal" in _registry().schema.attributes

    def test_a_third_port_result_lands_on_its_connection(self, workspace):
        """`p2` is `p` on the bus2 connection, declared though the bare registry has no `p2`.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
        """
        n = _three_port_link()
        n.c["Link"].dynamic["p2"] = pd.DataFrame(
            {"split": [1.0, 2.0]}, index=n.snapshots
        )

        results = PyPSA.to_datarecord(n, workspace.con).attributes
        assert "p2" not in results
        assert "p" in _registry().schema.attributes, "the bare registry declares `p`"
        p = _flat(results["p"])
        assert p["bus"].to_list() == ["c", "c"], "a third port's flow is on bus2"
        assert p["value"].to_list() == [1.0, 2.0]

    def test_a_three_port_record_writes_p_on_each_connection(self, store):
        """Writing a three-port network puts every port's flow on its connection.

        Notes
        -----
        - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
        """
        n = _three_port_link()
        flows = {"p0": [3.0, 4.0], "p1": [-1.0, -2.0], "p2": [-1.5, -1.5]}
        for attr, values in flows.items():
            n.c["Link"].dynamic[attr] = pd.DataFrame(
                {"split": values}, index=n.snapshots
            )

        revision = export_network(n, store)

        p = _flat(relation(revision, "p")).sort_values(["bus", "snapshot"])
        assert p.groupby("bus")["value"].apply(list).to_dict() == {
            "a": flows["p0"],
            "b": flows["p1"],
            "c": flows["p2"],
        }

    def test_a_stochastic_third_port_efficiency_survives_a_round_trip(self, store):
        """Per-scenario port values are placed by connection like any other.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        n = _three_port_link()
        n.set_scenarios({"low": 0.5, "high": 0.5})
        n.c["Link"].static["efficiency"] = 0.5
        n.c["Link"].static.loc[("low", "split"), "efficiency2"] = 0.2
        n.c["Link"].static.loc[("high", "split"), "efficiency2"] = 0.4

        built = PyPSA.build(export_network(n, store).record)

        static = built.c["Link"].static
        assert static[["efficiency", "efficiency2"]].to_dict("index") == {
            ("low", "split"): {"efficiency": 0.5, "efficiency2": 0.2},
            ("high", "split"): {"efficiency": 0.5, "efficiency2": 0.4},
        }, "each scenario's port values land in their own columns"

    def test_process_rate_is_one_attribute_on_its_connections(self, workspace):
        """`rate0`, `rate1`, `rate2` are one `rate`, a row per connection with its bus.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        n = _three_port_process()
        n.c["Process"].static.loc["dri", ["rate0", "rate1", "rate2"]] = [-2.0, 1.5, 0.3]

        inputs = PyPSA.to_datarecord(n, workspace.con).attributes
        assert not {"rate0", "rate1", "rate2"} & set(inputs), "no positional names"
        rate = _flat(inputs["rate"])
        assert rate.set_index("bus")["value"].to_dict() == {
            "a": -2.0,
            "b": 1.5,
            "c": 0.3,
        }
        spec = _registry().schema.attributes["rate"]
        assert "connection" in spec.dims
        assert spec.default is None, "rate0 and rate1 default differently"

    @pytest.mark.parametrize(
        ("network", "ctype", "name", "columns", "values"),
        [
            (
                _three_port_link,
                "Link",
                "split",
                ["efficiency", "efficiency2"],
                [0.5, 0.3],
            ),
            (
                _three_port_process,
                "Process",
                "dri",
                ["rate0", "rate1", "rate2"],
                [-2.0, 1.5, 0.3],
            ),
        ],
        ids=["link-efficiency", "process-rate"],
    )
    def test_per_port_values_survive_a_round_trip(
        self, store, network, ctype, name, columns, values
    ):
        """Each port's value comes back from its connection, not as PyPSA's default.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        n = network()
        n.c[ctype].static.loc[name, columns] = values

        built = PyPSA.build(export_network(n, store).record)

        assert built.c[ctype].static.loc[name, columns].tolist() == values, (
            "each port's value lands in its own column"
        )
        assert_networks_equal(built, n)

    @pytest.mark.parametrize(
        ("network", "ctype", "name", "static", "series"),
        [
            (
                _three_port_link,
                "Link",
                "split",
                ("efficiency", 0.5),
                ("efficiency2", [0.2, 0.4]),
            ),
            (
                _three_port_process,
                "Process",
                "dri",
                ("rate0", -2.0),
                ("rate1", [0.8, 1.2]),
            ),
        ],
        ids=["link-efficiency", "process-rate"],
    )
    def test_a_time_varying_port_survives_a_round_trip(
        self, store, network, ctype, name, static, series
    ):
        """A series on one port is that port's series, beside another port's static value.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        (static_attr, static_value), (series_attr, values) = static, series
        n = network()
        n.c[ctype].static.loc[name, static_attr] = static_value
        n.c[ctype].dynamic[series_attr] = pd.DataFrame(
            {name: values}, index=n.snapshots
        ).rename_axis(columns="name")

        built = PyPSA.build(export_network(n, store).record)

        assert built.c[ctype].dynamic[static_attr].empty, "the static port stays static"
        assert built.c[ctype].static.at[name, static_attr] == static_value
        assert built.c[ctype].dynamic[series_attr][name].to_list() == values
        assert_networks_equal(built, n)

    def test_per_type_defaults_round_trip_with_nothing_written(self, store):
        """A Load and a StorageUnit added without `sign` or `p_min_pu` build with their own defaults.

        A default PyPSA differs per type is declared through `default_by`.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
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
        child = root.child(staged.edits)

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
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
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
        assert _registry().schema.attributes["cyclic_delay"].dims == {"connection"}

        built = PyPSA.build(export_network(n, store).record)
        assert built.c["Link"].static.loc["split", ["delay", "delay2"]].tolist() == [
            3,
            4,
        ]
        assert built.c["Process"].static.loc["dri", "cyclic_delay1"]
        assert_networks_equal(built, n)

    def test_a_nan_in_a_series_survives_the_melt(self, workspace):
        """`UNPIVOT` drops nulls unless told otherwise; a NaN cell is still a row."""
        import numpy as np

        n = _three_port_link()
        n.c["Link"].dynamic["p_max_pu"] = pd.DataFrame(
            {"split": [1.0, np.nan]}, index=n.snapshots
        )

        rows = PyPSA.to_datarecord(n, workspace.con).attributes["p_max_pu"]
        split = rows.filter("entity = 'split'")
        assert len(split) == 2, "one row per snapshot, NaN included"

    def test_a_name_is_never_an_identifier_in_the_melt_or_pivot(self, store):
        """Each name is stored and comes back as its own series.

        `wind "north"` would end a quoted identifier, `snapshot` is the pivot's
        group-by column, and `Gen` and `gen` one case-insensitive identifier.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
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

        revision = store.root(PyPSA.to_datarecord(n, store.workspace.con))
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
        """`Coords` carries a dim PyPSA knows nothing about; axis names live in the tool.

        Declared but not `partial`, so the record's files need no column for it.
        Keying it would be a different matter, reported by the tool against the
        real record - see `test_verify_reports_a_snapshot_key`.
        """
        revision = exported
        _with_schema(revision, dims={**_DIMS, "vintage": "str"})
        record = revision.record
        assert "vintage" in record.schema.dims
        # No axis rows anywhere, so the dim is absent from the mapping rather than
        # present-and-empty (https://energy-models.github.io/datarecord/design/use/reading/#relations).
        assert "vintage" not in record.dims
        # PyPSA's own required dims are still satisfied, and the extra dim is
        # simply not something the tool looks at.
        assert not PyPSA.verify(revision.record)
        assert "vintage" in record.schema.long_columns

    def test_a_null_scenario_weight_reaches_every_scenario_its_own_row_does_not(self):
        rows = pd.DataFrame({"scenario": [None, "high"], "value": [0.25, 0.5]})
        con = duckdb.connect()
        record = SimpleNamespace(attributes={"scenario_weight": con.from_df(rows)})
        index = pd.Index(["low", "mid", "high"], name="scenario")
        equal = pd.DataFrame({"weight": 1 / 3}, index=index)
        weights = _weightings(record, "scenario", equal, con)  # type: ignore[arg-type]
        assert weights["weight"].to_dict() == {"low": 0.25, "mid": 0.25, "high": 0.5}, (
            "a NULL-scenario row broadcasts; a scenario's own row wins"
        )

    def test_snapshot_weightings_survive_a_round_trip(self, store, ac_dc):
        """Each column of `snapshot_weightings` is a `snapshot_weighting_*` attribute."""
        n = ac_dc.copy()
        steps = np.arange(len(n.snapshots), dtype=float)
        n.snapshot_weightings = pd.DataFrame(
            {"objective": 2.0 + steps, "stores": 3.0, "generators": 0.5 * steps},
            index=n.snapshots,
        )

        built = PyPSA.build(export_network(n, store).record)
        pd.testing.assert_frame_equal(
            built.snapshot_weightings,
            n.snapshot_weightings,
            check_freq=False,
            check_index_type=False,
        )

    def test_a_multi_period_snapshot_is_one_flat_axis(
        self, multiperiod, multiperiod_revision
    ):
        """A multi-period snapshot is labelled by its position, its timesteps repeating or not.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
        - [snapshots and weightings](https://energy-models.github.io/datarecord/design/tools/pypsa/#snapshots-and-weightings)
        """
        schema = multiperiod_revision.record.schema
        timesteps = multiperiod.snapshots.get_level_values("timestep")
        labels = multiperiod_revision.record.dims["snapshot"].df()["snapshot"]
        assert labels.tolist() == list(range(len(timesteps)))
        assert schema.dimensions["snapshot"].dtype == "int"
        assert set(schema.properties["snapshot"]) == {"period", "timestep"}
        assert schema.properties["snapshot"]["period"].one_of == "period"
        assert schema.properties["snapshot"]["timestep"].one_of == "timestep", (
            "a timestep is a label of the `timestep` dim"
        )
        steps = multiperiod_revision.record.dims["timestep"].df()["timestep"]
        assert steps.tolist() == list(timesteps.unique()), "each timestep once"
        assert schema.attributes["p_max_pu"].dims == {
            "entity",
            "snapshot",
            "scenario",
        }, "dims as in a single-period network"

    @pytest.mark.parametrize(
        "axis",
        [
            pytest.param(lambda n: pd.DataFrame(index=n.snapshots), id="snapshots"),
            pytest.param(
                lambda n: pd.DataFrame(index=n.investment_periods), id="periods"
            ),
            pytest.param(lambda n: n.snapshot_weightings, id="snapshot_weightings"),
            pytest.param(
                lambda n: n.investment_period_weightings, id="period_weightings"
            ),
        ],
    )
    def test_a_multi_period_axis_survives_a_round_trip(
        self, multiperiod, multiperiod_built, axis
    ):
        pd.testing.assert_frame_equal(
            axis(multiperiod_built), axis(multiperiod), check_index_type=False
        )

    def test_a_multi_period_network_survives_a_round_trip(
        self, multiperiod, multiperiod_built
    ):
        assert_networks_equal(multiperiod_built, multiperiod)

    @pytest.mark.parametrize("by", ["period", "snapshot"])
    def test_a_set_on_some_snapshots_keeps_the_rest_of_the_series(
        self, multiperiod, multiperiod_revision, by
    ):
        """`period=2040` selects that period's snapshots, a property of `snapshot`.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
        """
        from datarecord.mutable import WorkingRecord

        record = multiperiod_revision.record
        first = record.dims["snapshot"].limit(1).fetchall()[0][0]
        periods = multiperiod.snapshots.get_level_values("period")
        selector, changed = {
            "period": ({"period": 2040}, periods == 2040),
            "snapshot": ({"snapshot": first}, np.arange(len(periods)) == 0),
        }[by]
        staged = WorkingRecord(record)
        staged.set("p_max_pu", 0.5, entity="Manchester Wind", **selector)

        built = PyPSA.build(multiperiod_revision.child(staged.edits).record)
        series = multiperiod.c["Generator"].dynamic["p_max_pu"]["Manchester Wind"]
        pd.testing.assert_series_equal(
            built.c["Generator"].dynamic["p_max_pu"]["Manchester Wind"],
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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        whole = _results_over(multiperiod_revision, multiperiod_solved)
        members = pypsa.NetworkCollection(_myopic(multiperiod_solved))
        myopic = _results_over(multiperiod_revision, members, "period")
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
                _with_new_snapshot, "has snapshots `base` lacks", id="snapshot"
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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        record = multiperiod_revision.record
        members = pypsa.NetworkCollection(edit(_myopic(multiperiod_solved)))
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(
                members,
                record.con,
                results_only=True,
                base=record,
                collection_dim="period",
            )

    @pytest.mark.slow
    def test_a_myopic_solve_exports_the_network_of_one_solve(
        self, store, multiperiod_solved
    ):
        """A full export of a `period` collection is that of the multi-period network it split.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        members = pypsa.NetworkCollection(_myopic(multiperiod_solved))
        myopic = store.root(
            PyPSA.to_datarecord(members, store.workspace.con, collection_dim="period")
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
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        member = _myopic(multiperiod_solved)[period]
        base = (
            multiperiod_revision
            if periods == "both"
            else store.root(
                PyPSA.to_datarecord(
                    pypsa.NetworkCollection({period: _myopic(multiperiod)[period]}),
                    store.workspace.con,
                    collection_dim="period",
                )
            )
        )
        layer = PyPSA.to_datarecord(
            member, base.record.con, results_only=results_only, base=base.record
        )
        built = PyPSA.build(base.child(layer).record, select={"period": [period]})
        _assert_results_equal(built, member)

    @pytest.mark.slow
    @pytest.mark.parametrize("period", [2030, 2040])
    def test_a_period_built_as_a_list_solves_and_writes_back(
        self, multiperiod_revision, period
    ):
        """A period selected as a list keeps its investment period, so its solve writes back over `base` as is.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        base = multiperiod_revision
        select = {"period": [period]}
        n = PyPSA.build(base.record, select=select)
        n.optimize(solver_name="highs", multi_investment_periods=True)
        n.model.solver_model = None
        layer = PyPSA.to_datarecord(
            n, base.record.con, results_only=True, base=base.record
        )
        _assert_results_equal(PyPSA.build(base.child(layer).record, select=select), n)

    @pytest.mark.slow
    def test_results_of_selected_snapshots_land_on_them(self, multiperiod_revision):
        """A network of some snapshots of every period writes its results at `base`'s labels of them, and one of snapshots `base` lacks is refused at the call.

        Notes
        -----
        - [snapshots and weightings](https://energy-models.github.io/datarecord/design/tools/pypsa/#snapshots-and-weightings)
        """
        base = multiperiod_revision
        n = PyPSA.build(
            base.record,
            select={"snapshot": slice("2015-01-01 02:00", "2015-01-01 03:00")},
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
        built = PyPSA.build(base.child(layer).record).c.Generator.dynamic["p"]
        pd.testing.assert_frame_equal(
            built.loc[p.index, p.columns], p, check_names=False, check_freq=False
        )
        others = built.drop(index=p.index).fillna(0)
        assert (others == 0).all().all(), "no other snapshot holds one"
        periods, timesteps = (n.snapshots.get_level_values(i) for i in range(2))
        n.set_snapshots(
            pd.MultiIndex.from_arrays([periods, timesteps + pd.DateOffset(years=100)])
        )
        with pytest.raises(ValueError, match="snapshots `base` lacks"):
            PyPSA.to_datarecord(n, base.record.con, results_only=True, base=base.record)

    @pytest.mark.slow
    @pytest.mark.parametrize("listed", [True, False], ids=["list", "label"])
    def test_per_period_solves_written_in_turn_keep_each_others_results(
        self, staggered_revision, listed
    ):
        """Each period built alone, solved and written over the last leaves the other periods' dispatch and the idle generators' capacities standing.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        revision, solves = staggered_revision, {}
        for period in (2030, 2040, 2050):
            select = {"period": [period] if listed else period}
            solves[period] = _solve(PyPSA.build(revision.record, select=select))
            revision = _results_over(revision, solves[period])
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
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        whole = _solve(PyPSA.build(staggered_revision.record))
        base = _results_over(staggered_revision, whole)
        n = PyPSA.build(base.record, select=select)
        n.c.Load.dynamic["p_set"] *= 2
        built = PyPSA.build(_results_over(base, _solve(n)).record)

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
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        record = staggered_revision.record
        if source == "built":
            members = {}
            for p in (2030, 2040, 2050):
                m = PyPSA.build(record, select={"period": [p]})
                m.c.Generator.static["p_nom_extendable"] = False
                m.c.Generator.static["p_nom"] = 100.0
                members[p] = _solve(m)
        else:
            members = _myopic(_solve(PyPSA.build(record)))
            for p in (2040, 2050):
                members[p].c.Generator.static.loc["gen-2030", "p_nom_opt"] = 0.0
        layer = _results_over(
            staggered_revision, pypsa.NetworkCollection(members), "period"
        )
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
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        built = PyPSA.build(staggered_revision.record, select={"period": period})
        assert built.c.Generator.static.index.tolist() == held

    def test_a_child_whose_timesteps_repeat_otherwise_builds_back(
        self, staggered_revision, layout
    ):
        """Snapshots are positions whether timesteps repeat or not, so a child moving them restates `snapshot.timestep` alone.

        Notes
        -----
        - [snapshots and weightings](https://energy-models.github.io/datarecord/design/tools/pypsa/#snapshots-and-weightings)
        """
        other = _staggered("distinct" if layout == "repeated" else "repeated")
        record = staggered_revision.record
        child = staggered_revision.child(
            PyPSA.to_datarecord(other, record.con, base=record)
        )
        pd.testing.assert_index_equal(
            PyPSA.build(child.record).snapshots, other.snapshots, exact=False
        )

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
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        n = solved if network == "plain" else _myopic(multiperiod_solved)[2030]
        record = multiperiod_revision.record
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(n, record.con, results_only=results_only, base=record)

    @pytest.mark.parametrize("collection", [True, False], ids=["collection", "network"])
    def test_collection_dim_is_given_exactly_for_a_collection(self, ac_dc, collection):
        model = pypsa.NetworkCollection({2030: ac_dc}) if collection else ac_dc
        dim = None if collection else "period"
        with pytest.raises(ValueError, match="`collection_dim` exactly when"):
            PyPSA.to_datarecord(model, duckdb.connect(), collection_dim=dim)

    @pytest.mark.slow
    @pytest.mark.parametrize("results_only", [False, True], ids=["export", "results"])
    def test_a_collection_over_a_dim_of_its_own_builds_back(
        self, store, exported, sweep, results_only
    ):
        """An attribute whose values differ across the members gains the dim, one they agree on is written once, and `build` returns the members.

        A full export writes the halved `p_max_pu` of one member at its label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        record = _collection(sweep, store, exported, "sensitivity", results_only)
        attributes = record.schema.attributes
        assert "sensitivity" in attributes["p_nom_opt"].dims, "solves apart differ"
        assert "sensitivity" not in attributes["x_pu"].dims, (
            "an agreed value as declared"
        )
        assert record.attributes["x_pu"].df()["entity"].is_unique, "written once"
        assert "sensitivity" in record.schema.partial
        built = PyPSA.build(record)
        assert isinstance(built, pypsa.NetworkCollection)
        assert built.index.name == "sensitivity"
        assert list(built.index) == ["lo", "hi"], "collection order"
        for label, member in zip(built.index, built.networks, strict=True):
            inputs = () if results_only else (("Generator", "p_max_pu"),)
            _assert_results_equal(member, sweep[label], *inputs)

    @pytest.mark.parametrize("results_only", [False, True], ids=["export", "results"])
    def test_a_scenario_collection_differs_only_where_pypsa_varies_by_scenario(
        self, store, exported, sweep, results_only
    ):
        """Members solved apart differ in `p_nom_opt`, which PyPSA holds once across scenarios, so they are refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        with pytest.raises(
            ValueError, match="does not vary over `scenario`: name a dim"
        ):
            _collection(sweep, store, exported, "scenario", results_only)

    def test_a_scenario_collection_builds_back_stochastic(self, store, ac_dc):
        """Members differing where PyPSA varies by scenario are one stochastic network.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        members = _costing(10.0, 20.0)({"lo": ac_dc.copy(), "hi": ac_dc.copy()})
        record = _collection(members, store, None, "scenario", results_only=False)
        weights = record.attributes["scenario_weight"].df().set_index("scenario")
        assert weights["value"].to_dict() == {"lo": 0.5, "hi": 0.5}, "equal weights"
        built = PyPSA.build(record)
        assert list(built.scenarios) == ["lo", "hi"]
        for label, m in members.items():
            pd.testing.assert_series_equal(
                built.c.Generator.static["marginal_cost"].xs(label, level="scenario"),
                m.c.Generator.static["marginal_cost"],
                check_names=False,
                obj=label,
            )

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
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        collection = pypsa.NetworkCollection(
            edit({"lo": ac_dc.copy(), "hi": ac_dc.copy()})
        )
        layer = PyPSA.to_datarecord(
            collection, store.workspace.con, collection_dim="sensitivity"
        )
        record = store.root(layer).record
        assert ("sensitivity" in record.schema.attributes[attr].dims) == varies

    @pytest.mark.slow
    def test_a_re_solve_writes_once_what_agrees_over_the_dim(self, swept, solved):
        """Results over a `base` whose result varies over the collection's dim are written without it where they agree, reaching every label.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        model = pypsa.NetworkCollection({"lo": solved, "hi": solved.copy()})
        record = _results_over(swept, model, "sensitivity").record
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
            pytest.param(_two_levels, "sensitivity", "one index level", id="levels"),
            pytest.param(dict, "bus", "names a dim the PyPSA schema", id="pypsa-dim"),
            pytest.param(_stochastic, "scenario", "is stochastic", id="stochastic"),
            pytest.param(_moved_bus, "sensitivity", "'London' has `x`", id="property"),
            pytest.param(
                _moved_bus_and_capacity,
                "scenario",
                "'London' has `x`",
                id="axes-before-attributes",
            ),
            pytest.param(_reordered_buses, "sensitivity", "orders `bus`", id="order"),
            pytest.param(
                _with_custom_column, "scenario", "declares other", id="schema"
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
                "names a dim the members' shapes make",
                id="shape-made-dim",
            ),
            pytest.param(
                _with_newcomer,
                "sensitivity",
                r"holds other Generator .*'Newcomer'",
                id="entity",
            ),
        ],
    )
    def test_a_collection_must_fit_its_dim(self, store, ac_dc, edit, dim, match):
        """A member fits its dim, and declares and orders what the others do, with their properties, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        collection = pypsa.NetworkCollection(
            edit({"lo": ac_dc.copy(), "hi": ac_dc.copy()})
        )
        with pytest.raises(ValueError, match=match):
            PyPSA.to_datarecord(collection, store.workspace.con, collection_dim=dim)

    def test_a_scenario_collection_is_written_over_a_deterministic_base(
        self, store, ac_dc
    ):
        stochastic = ac_dc.copy()
        stochastic.set_scenarios(["x", "y"])
        base = export_network(stochastic, store).record
        collection = pypsa.NetworkCollection({"lo": ac_dc, "hi": ac_dc})
        with pytest.raises(ValueError, match="over a `base` without scenarios"):
            PyPSA.to_datarecord(
                collection,
                base.con,
                results_only=True,
                base=base,
                collection_dim="scenario",
            )

    @pytest.mark.parametrize(
        ("dim", "match"),
        [
            pytest.param("weather_year", "collections do not nest", id="another"),
            pytest.param("period", "builds to a `NetworkCollection`", id="period"),
        ],
    )
    def test_a_record_has_one_dim_of_its_own(self, exported, sweep, solved, dim, match):
        """A collection over a `base` with a dim of its own is over that dim or `scenario`, refused at the call.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        swept = _results_over(exported, pypsa.NetworkCollection(sweep), "sensitivity")
        collection = pypsa.NetworkCollection({"lo": solved, "hi": solved.copy()})
        with pytest.raises(ValueError, match=match):
            _results_over(swept, collection, dim)

    def test_build_refuses_two_dims_of_its_own(self, exported):
        """A collection's index is one dim, so a record declaring two of its own is reported.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        was = read_schema(exported.store)
        own = {
            "weather_year": Dimension(dtype="int"),
            "sensitivity": Dimension(dtype="str"),
        }
        write_schema(
            was.model_copy(update={"dimensions": {**was.dimensions, **own}}),
            exported.store,
        )
        missing = PyPSA.verify(exported.record)
        assert missing.unsupported_dims == set(own)
        with pytest.raises(UnsupportedRecordError, match="unsupported dims"):
            PyPSA.build(exported.record)

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
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        built = PyPSA.build(single_revision.record, select=select)
        held = {ctype: built.c[ctype].static.index.tolist() for ctype in expected}
        assert held == expected
        built.consistency_check(strict=["unknown_buses", "unknown_carriers"])

    def test_a_selection_keeps_rows_in_their_order(self, single_revision):
        """The rows a selection keeps are in their relation's order, however many DuckDB filters in parallel."""
        record = single_revision.record
        ids = np.arange(1_000_000)
        buses = np.array(["London", "Norwich", "Bremen"])[ids % 3]
        rel = record.con.from_df(pd.DataFrame({"id": ids, "bus": buses}))
        kept = _select(record, {"bus": ["London", "Bremen"]})._kept(rel)
        held = kept.project("id").fetchnumpy()["id"]
        np.testing.assert_array_equal(held, ids[buses != "Norwich"])

    @_SNAPSHOT_SELECTIONS
    def test_selected_snapshots_are_the_networks(
        self, single_revision, ac_dc, value, hours
    ):
        """A snapshot is selected by timestamp and never squeezed: one is a one-snapshot network.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        built = PyPSA.build(single_revision.record, select={"snapshot": value})
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
        """A multi-period snapshot's label is a position, and its timestamp names one in each period holding it.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        built = PyPSA.build(multiperiod_revision.record, select={"snapshot": value})
        timesteps = multiperiod.snapshots.get_level_values("timestep")
        expected = multiperiod.snapshots[timesteps.isin(ac_dc.snapshots[hours])]
        pd.testing.assert_index_equal(built.snapshots, expected, exact=False)

    @pytest.mark.parametrize(
        ("select", "hours"),
        [
            pytest.param({"snapshot": "2015-01-01 07:00"}, [7], id="timestamp"),
            pytest.param(
                {"snapshot": slice("2015-01-01 06", None)}, [6, 7, 8, 9], id="slice"
            ),
        ],
    )
    def test_one_period_takes_snapshots_by_timestamp(
        self, multiperiod_revision, ac_dc, select, hours
    ):
        """A snapshot selected beside one period is one of that period's, named by its timestep."""
        built = PyPSA.build(
            multiperiod_revision.record, select={"period": 2040, **select}
        )
        assert list(built.snapshots) == list(ac_dc.snapshots[hours])

    @pytest.mark.parametrize("layout", ["distinct"], indirect=True)
    @pytest.mark.parametrize("period", [2040, [2040]], ids=["label", "list"])
    def test_a_selection_leaving_no_snapshot_is_refused(
        self, multiperiod_revision, period
    ):
        """Split between the periods, the first hour is 2030's alone."""
        with pytest.raises(ValueError, match="leaves no snapshot"):
            PyPSA.build(
                multiperiod_revision.record,
                select={"period": period, "snapshot": "2015-01-01 00:00"},
            )

    def test_dims_selected_together_combine(self, store, multiperiod):
        """One period, one scenario and two carriers: a deterministic single-period network of the AC buses and their wind generators.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        n = multiperiod.copy()
        n.set_scenarios({"lo": 0.2, "hi": 0.8})
        record = export_network(n, store).record
        select = {"period": 2030, "scenario": "hi", "carrier": ["AC", "wind"]}

        assert not PyPSA.verify(record, select=select)
        built = PyPSA.build(record, select=select)

        assert not built.has_scenarios
        assert not built.investment_periods.size
        assert set(built.c.Bus.static["carrier"]) == {"AC"}
        assert built.c.Generator.static.index.tolist() == [
            "Manchester Wind",
            "Norway Wind",
            "Frankfurt Wind",
        ]

    def test_one_period_refuses_an_activity_varying_over_a_kept_dim(
        self, store, multiperiod
    ):
        """Which generators are active in 2030 depends on the member, which the selection keeps."""
        later = multiperiod.copy()
        later.c.Generator.static.loc["Manchester Gas", "build_year"] = 2040
        sweep = {"lo": multiperiod.copy(), "hi": later}
        record = _collection(sweep, store, None, "sensitivity", results_only=False)
        with pytest.raises(ValueError, match="`build_year` varies over"):
            PyPSA.build(record, select={"period": 2030})
        built = PyPSA.build(record, select={"period": 2030, "sensitivity": "hi"})
        assert "Manchester Gas" not in built.c.Generator.static.index

    @pytest.mark.slow
    def test_one_period_reads_activity_at_the_kept_labels_alone(
        self, store, multiperiod
    ):
        """`build_year` is set at `hi` alone, which the selection drops, so it varies over none of the labels kept."""
        sweep = {label: multiperiod.copy() for label in ("lo", "mid", "hi")}
        sweep["hi"].c.Generator.static.loc["Manchester Gas", "build_year"] = 2040
        record = _collection(sweep, store, None, "sensitivity", results_only=False)

        built = PyPSA.build(
            record, select={"period": 2030, "sensitivity": ["lo", "mid"]}
        )

        assert isinstance(built, pypsa.NetworkCollection)
        assert list(built.index) == ["lo", "mid"]
        for n in built.networks:
            assert "Manchester Gas" in n.c.Generator.static.index, "active in 2030"

    def test_a_null_carrier_refers_to_no_dropped_carrier(self, store, ac_dc):
        """PyPSA's `""` carrier is NULL, so a load without one stays under any carrier selected."""
        n = ac_dc.copy()
        n.c.Load.static.loc["London", "carrier"] = ""
        record = export_network(n, store).record
        built = PyPSA.build(record, select={"carrier": ["AC"]})
        assert built.c.Load.static.index.tolist() == ["London"]

    @pytest.mark.parametrize("period", [2030, 2040])
    def test_one_period_is_a_single_period_network_of_its_active_entities(
        self, store, ac_dc, layout, period
    ):
        """Over one period the network is indexed by its timesteps, without the period weightings, holding the generators PyPSA holds active in it.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        n = _over_periods(ac_dc, layout)
        n.c.Generator.static.loc["Manchester Gas", "build_year"] = 2040
        n.c.Generator.static.loc["Norway Gas", ["build_year", "lifetime"]] = [2000, 35]
        record = export_network(n, store).record

        built = PyPSA.build(record, select={"period": period})

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
    def test_selected_periods_keep_the_global_constraints_applying_in_them(
        self, store, ac_dc, value, expected
    ):
        """A constraint for a period the selection drops goes; one period leaves the kept ones applying in every period, as a single-period network has no period to name.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
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

        built = PyPSA.build(record, select={"period": value})

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
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        built = PyPSA.build(multiperiod_revision.record, select={"period": value})
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
        built = PyPSA.build(stochastic.record, select={"scenario": "hi"})
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
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        path = Path(
            stochastic.store.layer_dir(stochastic.id),
            "attributes",
            "scenario_weight.parquet",
        )
        rows = pd.read_parquet(path)
        rows["value"] = rows["scenario"].map(stored)
        rows.dropna(subset="value").to_parquet(path, index=False)

        built = PyPSA.build(stochastic.record, select={"scenario": ["lo", "mid"]})

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

        built = PyPSA.build(record, select={"scenario": ["lo", "mid"]})

        assert isinstance(built, pypsa.NetworkCollection)
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
            PyPSA.build(record, select={"scenario": ["lo", "mid"]})

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(2013, id="scalar", marks=pytest.mark.slow),
            pytest.param([2013], id="list"),
            pytest.param(slice(2013, None), id="slice"),
        ],
    )
    def test_a_dim_of_its_own_selected_is_a_network_or_a_collection(
        self, store, sweep, value
    ):
        """One label of the record's own dim builds that member, a list or a slice of its numbers the collection of them.

        Notes
        -----
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        years = {2012: sweep["lo"], 2013: sweep["hi"]}
        record = _collection(years, store, None, "weather_year", results_only=False)
        built = PyPSA.build(record, select={"weather_year": value})
        if not isinstance(value, int):
            assert isinstance(built, pypsa.NetworkCollection)
            assert list(built.index) == [2013]
            (built,) = built.networks
        assert isinstance(built, pypsa.Network)
        _assert_results_equal(built, sweep["hi"], ("Generator", "p_max_pu"))

    def test_a_dim_of_its_own_of_text_labels_takes_no_slice(
        self, store, exported, sweep
    ):
        record = _collection(sweep, store, exported, "sensitivity", results_only=False)
        with pytest.raises(ValueError, match="unordered"):
            PyPSA.build(record, select={"sensitivity": slice("hi", "lo")})

    @pytest.mark.parametrize(
        ("select", "match"),
        [
            pytest.param({"role": "input"}, "takes labels of", id="role"),
            *(
                pytest.param({dim: "x"}, "takes labels of", id=dim)
                for dim in ("sub_network", "line_type", "transformer_type")
            ),
            pytest.param({"ctype": "Generator"}, "takes labels of", id="ctype"),
            pytest.param({"weather_year": 2020}, "takes labels of", id="undeclared"),
            pytest.param({"bus": "Atlantis"}, "'Atlantis', a label", id="bus"),
            pytest.param(
                {"bus": ["Atlantis"]}, r"\['Atlantis'\] its axis lacks", id="list"
            ),
            pytest.param({"period": 2030}, "2030, a label", id="no-axis"),
            pytest.param({"carrier": []}, "no label of `carrier`", id="empty"),
            pytest.param(
                {"snapshot": "2016-01-01"}, "'2016-01-01', a label", id="timestamp"
            ),
            pytest.param(
                {"snapshot": slice(3, None)}, "cannot compare", id="slice-bound"
            ),
            pytest.param({"snapshot": slice(None, None, 2)}, "step", id="step"),
            *(
                pytest.param({dim: slice("A", "Z")}, "unordered", id=f"{dim}-slice")
                for dim in ("bus", "entity", "carrier")
            ),
            pytest.param(
                {"snapshot": slice("2016", None)}, "slices no label", id="slice-none"
            ),
        ],
    )
    @pytest.mark.parametrize(
        "method", [PyPSA.build, PyPSA.verify], ids=["build", "verify"]
    )
    def test_a_selection_names_labels_of_a_selectable_dim(
        self, single_revision, select, match, method
    ):
        with pytest.raises(ValueError, match=match):
            method(single_revision.record, select=select)
