# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The tool layer: verify a record, build a model, read it back.

Notes
-----
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import pandas as pd
import pytest
from duckdb import DuckDBPyRelation

from datarecord import Revision, Workspace
from datarecord.schema.categories import matching
from datarecord.tools.base import Schema, UnsupportedRecordError
from datarecord.tools.pypsa import (
    CTYPE,
    PyPSA,
    _colliding_names,
    _input_names,
    _registry_schema,
    _weightings,
)
from tests.fixtures import (
    HandBuilt,
    empty_child,
    empty_root,
    export_network,
    schema,
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
    """`built`, optimised once for the module."""
    n = built.copy()
    n.optimize(solver_name="highs")
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


def _results_over(revision, n):
    """`n`'s results, written as a thin layer over `revision`."""
    layer = PyPSA.to_datarecord(
        n, revision.record.con, results_only=True, base=revision.record
    )
    return revision.store.commit(layer, parent=revision)


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


def test_build_takes_only_a_record(workspace, ac_dc):
    """A `RecordLike` that is no `Record` is refused, the tool's own network source included."""
    with pytest.raises(TypeError, match="builds from a Record"):
        PyPSA.build(PyPSA.to_datarecord(ac_dc, workspace.con))


def test_verify_accepts_a_complete_record(single_revision):
    """A record written by `export_to_parquet` supplies everything PyPSA needs."""
    assert not PyPSA.verify(single_revision.record)


def test_the_tool_writes_no_type_or_role_axis(single_revision):
    """The schema lists every type and role, so the record reads them from it.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
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
    single_revision,
):
    """`role` categorizes `connection`, so `flags(role=...)` reads as `flags(ctype=...)` does.

    Notes
    -----
    - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
    """
    role = "output"
    record = single_revision.record
    tuples = matching(record.properties["connection"], {"role": (role,)})
    over = [
        a for a in record.attributes if "connection" in record.schema.coordinates_of(a)
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


def test_buses_and_carriers_are_labels_of_their_dims(single_revision, built, ac_dc):
    """`n.buses` and `n.carriers` are the `bus` and `carrier` axes, under PyPSA's names.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
    """
    record = single_revision.record
    assert not {"Bus", "Carrier"} & set(record.schema.dimensions["ctype"].one_of or ())
    assert record.properties["bus"].columns == [
        "bus",
        "carrier",
        "type",
        "x",
        "y",
        "unit",
        "location",
    ]
    assert record.properties["carrier"].columns == [
        "carrier",
        "color",
        "nice_name",
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


def test_an_unlisted_carrier_is_added_and_an_empty_one_is_none(store):
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
    assert built.c.Generator.static["carrier"].to_dict() == {"gen": "", "wind": "wind"}


def test_a_property_is_addressed_by_its_subject(single_revision):
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


def test_a_carrier_is_a_live_label(single_revision):
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


def test_a_value_its_type_does_not_take_is_reported_and_left_out(store):
    """A broadcast reaches every entity; the build keeps it to the types taking it.

    Notes
    -----
    - [a value its type does not take](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-value-its-type-does-not-take)
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


def _add_generator(w, name: str) -> None:
    w.add("entity", [name], ctype="Generator")
    w.add("connection", [{"entity": name, "bus": "b"}], role="attached")


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
    two_generators, layers, built
):
    """Rows as written, a deeper layer crossing a shallower one, build each member's value.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    from datarecord.mutable import WorkingRecord

    revision = two_generators
    for edit in layers:
        w = WorkingRecord(revision.record)
        edit(w)
        revision = revision.store.commit(w)

    static = PyPSA.build(revision.record).c.Generator.static
    assert static["p_max_pu"].to_dict() == built


def test_verify_reports_an_entity_attached_to_no_bus(single_revision):
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


def test_requires_reports_the_records_own_types(single_revision):
    """`requires` is record-dependent: PyPSA's axes plus this record's types."""
    req = PyPSA.requires(single_revision.record)
    assert {"snapshot", "period", "scenario"} <= req.dims
    assert "Generator" in req.categories
    # Required attributes come from PyPSA's registry, not a list we maintain.
    assert ("Generator", "bus") in req.attributes
    assert ("Line", "x") in req.attributes
    # `name` is the member identity, never an attribute row.
    assert not [a for a in req.attributes if a[1] == "entity"]


def test_verify_reports_a_missing_dim(exported):
    """A schema that declares no `scenario` dim cannot build a network.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = _with_schema(exported, dims={"snapshot": "datetime"}, partial=set())

    missing = PyPSA.verify(revision.record)
    assert missing
    assert missing.dims == {"period", "scenario"}
    # A build refuses rather than failing deep inside PyPSA.
    with pytest.raises(UnsupportedRecordError, match="scenario"):
        PyPSA.build(revision.record)


def test_verify_reports_a_type_the_tool_does_not_know(exported):
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
                    CTYPE: dim.model_copy(update={"one_of": (*dim.one_of, "Widget")}),
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
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
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


def test_verify_reports_a_snapshot_key(exported):
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
    revision = _with_schema(exported, partial={"scenario", "snapshot"})

    missing = PyPSA.verify(revision.record)
    assert ("owner_map_key", "snapshot") in missing.unsupported_keys
    assert "snapshot" in missing.describe()
    with pytest.raises(UnsupportedRecordError, match="snapshot"):
        PyPSA.build(revision.record)


def test_verify_reports_a_missing_required_attribute(exported, ac_dc):
    """A component type with no `bus` anywhere - no connection row supplies it.

    A port's bus is the `connection` tuple itself, declared as no attribute, so
    no default can stand in for it.
    """
    # A Generator member with no connection row supplying its bus
    # (https://energy-models.github.io/datarecord/design/tools/pypsa/#ports).
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


def test_verify_reports_a_piecewise_linear_attribute(exported):
    """A curve is stored correctly; it is the PyPSA translation that cannot express it.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
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
    assert "piecewise-linear" in missing.describe()
    with pytest.raises(UnsupportedRecordError):
        PyPSA.build(revision.record)


def _north():
    """A bus `north` and a generator at it, over two snapshots."""
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
    n.add("Bus", "north")
    n.add("Generator", "gen", bus="north")
    return n


def test_to_datarecord_rejects_a_cross_type_name_collision(workspace):
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


def test_a_bus_and_a_generator_may_share_a_name(store):
    """A bus is a label of `bus`, not an entity, so `north` is one of each."""
    n = _north()
    n.add("Generator", "north", bus="north")

    record = export_network(n, store).record
    assert record.dims["bus"].df()["bus"].tolist() == ["north"]
    assert "north" in record.dims["entity"].df()["entity"].tolist()
    assert_networks_equal(PyPSA.build(record), n)


@pytest.mark.parametrize(
    ("columns", "clashing"),
    [
        ({"Carrier": ("efficiency", 0.9)}, {("Carrier", "efficiency")}),
        (
            {"Bus": ("country", "DE"), "Generator": ("country", "DE")},
            {("Bus", "country"), ("Generator", "country")},
        ),
    ],
    ids=["registry-attribute", "two-dims"],
)
def test_to_datarecord_rejects_a_custom_column_declared_over_another_dim(
    workspace, columns, clashing
):
    """A custom column is an attribute over its type's dim, one name one spec.

    A carrier's own `efficiency` meets the per-port attribute; `country` on a
    bus and on a generator would be one attribute over two dims.
    """
    n = _north()
    n.add("Carrier", "AC")
    for ctype, (column, value) in columns.items():
        n.c[ctype].static[column] = value

    with pytest.raises(UnsupportedRecordError, match="another shape") as raised:
        PyPSA.to_datarecord(n, workspace.con)
    assert raised.value.missing.columns == clashing


def test_a_custom_column_round_trips_on_its_own_type(store):
    """A column PyPSA's registry lacks is an attribute over its type's dim.

    A generator's own `location` is no clash with `bus.location`, a property,
    and its own `e_nom` is built onto it though the registry gives `e_nom` to
    a store alone.
    """
    n = _north()
    n.c.Bus.static["location"] = "DE1"
    n.c.Bus.static["country"] = "site"
    n.c.Generator.static["location"] = "site"
    n.c.Generator.static["e_nom"] = 5.0

    record = export_network(n, store).record
    dims = {
        c: record.schema.attributes[c].dims for c in ("country", "location", "e_nom")
    }
    assert dims == {
        "country": {"bus", "scenario"},
        "location": {"entity", "scenario"},
        "e_nom": {"entity", "scenario"},
    }
    assert_networks_equal(PyPSA.build(record), n)


def test_shapes_round_trip_in_their_crs(store):
    """A geometry is stored as WKT, its CRS in the schema's `meta`.

    Notes
    -----
    - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
    """
    import pypsa
    import shapely
    from geopandas.testing import assert_geodataframe_equal

    n = pypsa.Network()
    n.to_crs("EPSG:3035")
    n.set_snapshots(pd.date_range("2030-01-01", periods=2, freq="h"))
    n.add("Bus", "north")
    n.add(
        "Shape",
        ["north onshore", "north offshore"],
        geometry=[shapely.box(0, 0, 1, 1), shapely.Point(2, 3)],
        idx="north",
        component="Bus",
        type=["onshore", "offshore"],
    )

    record = export_network(n, store).record
    assert record.schema.meta["crs"] == "EPSG:3035"
    stored = record.attributes["geometry"].df()
    assert sorted(stored["value"]) == [
        "POINT (2 3)",
        "POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))",
    ]
    built = PyPSA.build(record)
    assert built.crs == n.crs
    assert_geodataframe_equal(built.c.Shape.static, n.c.Shape.static)


def test_a_solved_network_exports(store, solved):
    """A solved network's SubNetworks are labels of a dim of their own; its derived `Line.v_nom` is recomputed.

    PyPSA numbers SubNetworks `0`, `1`, ... as it numbers Lines, which the dim
    keeps apart; `optimize` leaves the derived `Line.v_nom` behind, which
    `build` recomputes.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    assert set(solved.c["SubNetwork"].static.index) & set(solved.c["Line"].static.index)

    revision = export_network(solved, store)
    record = revision.record
    names = _sub_networks(revision)
    assert names == sorted(solved.c["SubNetwork"].static.index)
    pointed = set(record.attributes["sub_network"].df()["value"])
    assert pointed <= set(names), "a `sub_network` value names a `sub_network` label"
    carriers = record.properties["sub_network"].df()["carrier"]
    assert set(carriers) <= set(record.dims["carrier"].df()["carrier"])

    built = PyPSA.build(record)
    assert sorted(built.c["SubNetwork"].static.index) == names
    built.calculate_dependent_values()
    pd.testing.assert_series_equal(
        built.c.Line.static.v_nom, solved.c.Line.static.v_nom
    )


def test_a_stochastic_network_is_not_a_false_collision():
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


def test_a_solved_network_exports_its_results_long_form(single_revision, solved):
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


def test_an_export_skips_results_still_at_their_default(single_revision, built):
    """An unsolved network yields no `p`/`p_nom_opt` rows.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    results = PyPSA.to_datarecord(built, single_revision.record.con).attributes
    assert "p" not in results
    assert "p_nom_opt" not in results


def test_a_solved_p_is_one_row_per_connection(single_revision, solved):
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


def test_bus_results_are_keyed_by_bus(results, results_built, solved):
    """`marginal_price`, `v_mag_pu` and `v_ang` are over `bus`; `Bus.p` and the rest are derived.

    `build` copies a result into the network like any attribute.

    Notes
    -----
    - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    schema = results.record.schema
    over_bus = {
        a for a, spec in _registry_schema().attributes.items() if "bus" in spec.dims
    }
    assert over_bus - _input_names() == {"marginal_price", "v_mag_pu", "v_ang"}
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


def test_a_child_reads_its_parents_results_and_the_latest_wins(results, results_built):
    """A result is an attribute: it inherits through layers, a later one standing over it.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    expected = _prices(results)
    pd.testing.assert_frame_equal(_prices(empty_child(results).commit()), expected)

    n = results_built.copy()
    n.c["Bus"].dynamic["marginal_price"] *= 2
    resolved = _results_over(results, n)
    pd.testing.assert_frame_equal(_prices(empty_child(resolved).commit()), expected * 2)


def test_a_re_solve_leaves_no_stale_sub_network(
    single_revision, results, results_built, solved
):
    """A results layer replaces `base`'s SubNetworks rather than adding to them.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
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


@pytest.mark.parametrize("over_solved", [False, True], ids=["unsolved", "solved"])
def test_results_without_sub_networks_list_none(
    single_revision, built, results, results_built, over_solved
):
    """A solve without SubNetworks restates a `sub_network` axis `base` has empty, and writes none where it has none.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    base, n = (results, results_built) if over_solved else (single_revision, built)
    n = n.copy()
    n.remove("SubNetwork", n.c["SubNetwork"].static.index)
    n.c["Bus"].dynamic["marginal_price"] = pd.DataFrame(
        1.0, index=n.snapshots, columns=n.c["Bus"].static.index
    )
    record = _results_over(base, n).record
    axis = record.dims.get("sub_network")
    assert (None if axis is None else _sub_networks_of(axis)) == (
        [] if over_solved else None
    ), "the axis is restated only where `base` has one"
    assert "marginal_price" in record.attributes, "the results are written"
    slack = record.attributes.get("slack_bus")
    assert slack is None or slack.df().empty, "no stale SubNetwork keeps a result"


def test_a_result_a_re_solve_leaves_out_reads_as_null(single_revision, results_built):
    """A results layer restates `base`'s results it does not write as NULL.

    Built back, the network holds PyPSA's own default there. `mu_upper` is a
    result over a line, `marginal_price` one over a bus.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
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


def test_results_only_takes_base_exactly_with_it(single_revision, solved):
    """A results layer is written over the record it was built from, a full export over nothing.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """
    with pytest.raises(ValueError, match="takes `base` exactly when"):
        PyPSA.to_datarecord(solved, single_revision.record.con, results_only=True)


def test_link_internal_power_is_p_internal(single_revision, solved):
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
    assert "p_internal" in _registry_schema().attributes


def _three_port_link():
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2020-01-01", periods=2, freq="h"))
    for bus in ("a", "b", "c"):
        n.add("Bus", bus)
    n.add("Link", "split", bus0="a", bus1="b", bus2="c")
    return n


def test_a_three_port_record_writes_p_on_each_connection(workspace):
    """`p0`, `p1` and `p2` are `p` on each port's connection, though the bare registry has no `p2`.

    Notes
    -----
    - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
    """
    n = _three_port_link()
    flows = {"p0": [3.0, 4.0], "p1": [-1.0, -2.0], "p2": [-1.5, -1.5]}
    for attr, values in flows.items():
        n.c["Link"].dynamic[attr] = pd.DataFrame({"split": values}, index=n.snapshots)

    results = PyPSA.to_datarecord(n, workspace.con).attributes
    assert not set(flows) & set(results), "no positional names"
    p = _flat(results["p"]).sort_values(["bus", "snapshot"])
    assert p.groupby("bus")["value"].apply(list).to_dict() == {
        "a": flows["p0"],
        "b": flows["p1"],
        "c": flows["p2"],
    }


def test_a_static_attribute_differing_per_scenario_survives_a_round_trip(store):
    """A fixed `p_nom` is an input and may differ per scenario, as PyPSA allows.

    A per-scenario port value is placed by connection like any other.

    Notes
    -----
    - [entities and their properties](https://energy-models.github.io/datarecord/design/tools/pypsa/#entities-and-their-properties)
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
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


@pytest.mark.parametrize("attr", ["p_nom_extendable", "p_nom_opt"])
def test_only_pypsas_scenario_invariant_attributes_lack_the_scenario_dim(attr):
    """Every stored attribute may differ per scenario, but those PyPSA keeps invariant."""
    dims = _registry_schema().attributes[attr].dims
    assert "scenario" not in dims, dims


def _three_port_process():
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2020-01-01", periods=2, freq="h"))
    for bus in ("a", "b", "c"):
        n.add("Bus", bus)
    n.add("Process", "dri", bus0="a", bus1="b", bus2="c")
    return n


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
    store, network, ctype, name, static, series, attr, default
):
    """Each port's value comes back from its connection, a series on one port beside static ones.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
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
    assert _registry_schema().attributes[attr].default == default, (
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


def test_per_type_defaults_round_trip_with_nothing_written(store):
    """A Load and a StorageUnit added without `sign` or `p_min_pu` build with their own defaults.

    A default PyPSA differs per type is declared through `default_by`.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
    """
    import pypsa

    from datarecord.mutable import WorkingRecord

    sign = _registry_schema().properties["entity"]["sign"]
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
    written = {p.stem for p in inputs.glob("*.parquet")} if inputs.exists() else set()
    assert not {"sign", "p_min_pu"} & written, "a default is never stored"

    built = PyPSA.build(child.record)
    assert built.c["Load"].static.at["l1", "sign"] == -1, "a Load's own sign"
    assert built.c["StorageUnit"].static.at["su1", "sign"] == 1
    assert built.c["StorageUnit"].static.at["su1", "p_min_pu"] == -1, (
        "a StorageUnit's own p_min_pu"
    )


def test_delays_are_rows_keyed_by_their_connections(workspace, store):
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
    assert _registry_schema().attributes["cyclic_delay"].dims == {
        "connection",
        "scenario",
    }

    built = PyPSA.build(export_network(n, store).record)
    assert built.c["Link"].static.loc["split", ["delay", "delay2"]].tolist() == [3, 4]
    assert built.c["Process"].static.loc["dri", "cyclic_delay1"]
    assert_networks_equal(built, n)


def test_a_nan_in_a_series_survives_the_melt(workspace):
    """`UNPIVOT` drops nulls unless told otherwise; a NaN cell is still a row."""
    import numpy as np

    n = _three_port_link()
    n.c["Link"].dynamic["p_max_pu"] = pd.DataFrame(
        {"split": [1.0, np.nan]}, index=n.snapshots
    )

    rows = PyPSA.to_datarecord(n, workspace.con).attributes["p_max_pu"]
    split = rows.filter("entity = 'split'")
    assert len(split) == 2, "one row per snapshot, NaN included"


def test_a_name_is_never_an_identifier_in_the_melt_or_pivot(store):
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

    revision = store.commit(PyPSA.to_datarecord(n, store.workspace.con), parent=None)
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


def test_schema_dims_stay_generic(exported):
    """`Coords` carries a dim PyPSA knows nothing about; axis names live in the tool.

    Declared but not `partial`, so the record's files need no column for it.
    Keying it would be a different matter, reported by the tool against the
    real record - see `test_verify_reports_a_snapshot_key`.
    """
    revision = _with_schema(exported, dims={**_DIMS, "vintage": "str"})
    record = revision.record
    assert "vintage" in record.schema.dims
    # No axis rows anywhere, so the dim is absent from the mapping rather than
    # present-and-empty (https://energy-models.github.io/datarecord/design/use/reading/#relations).
    assert "vintage" not in record.dims
    # PyPSA's own required dims are still satisfied, and the extra dim is
    # simply not something the tool looks at.
    assert not PyPSA.verify(revision.record)
    assert "vintage" in record.schema.long_columns


def test_a_null_scenario_weight_reaches_every_scenario_its_own_row_does_not():
    rows = pd.DataFrame({"scenario": [None, "high"], "value": [0.25, 0.5]})
    con = duckdb.connect()
    record = SimpleNamespace(attributes={"scenario_weight": con.from_df(rows)})
    index = pd.Index(["low", "mid", "high"], name="scenario")
    equal = pd.DataFrame({"weight": 1 / 3}, index=index)
    weights = _weightings(record, "scenario", equal, con)  # type: ignore[arg-type]
    assert weights["weight"].to_dict() == {"low": 0.25, "mid": 0.25, "high": 0.5}, (
        "a NULL-scenario row broadcasts; a scenario's own row wins"
    )


@pytest.fixture(scope="module", params=["repeated", "distinct"])
def multiperiod(request, ac_dc):
    """`ac_dc` over two investment periods, with weightings of its own in every column of both axes.

    `repeated` runs every timestep in both periods, as `set_investment_periods`
    does; `distinct` splits the timesteps between them.
    """
    n = ac_dc.copy()
    if request.param == "repeated":
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


def test_a_multi_period_snapshot_is_one_flat_axis(multiperiod, multiperiod_revision):
    """A repeated timestep makes the labels positions; a distinct one is the label.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
    """
    schema = multiperiod_revision.record.schema
    timesteps = multiperiod.snapshots.get_level_values("timestep")
    repeated = not timesteps.is_unique
    labels = multiperiod_revision.record.dims["snapshot"].df()["snapshot"]
    expected = list(range(len(timesteps))) if repeated else list(timesteps)
    assert labels.tolist() == expected
    assert schema.dimensions["snapshot"].dtype == ("int" if repeated else "datetime")
    assert set(schema.properties["snapshot"]) == (
        {"period", "timestep"} if repeated else {"period"}
    ), "a timestep property only where the label is a position"
    assert schema.properties["snapshot"]["period"].one_of == "period"
    if repeated:
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


def test_multi_period_weightings_survive_a_round_trip(multiperiod, multiperiod_built):
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


def test_a_multi_period_network_survives_a_round_trip(multiperiod, multiperiod_built):
    assert_networks_equal(multiperiod_built, multiperiod)


@pytest.mark.parametrize("multiperiod", ["repeated"], indirect=True)
def test_a_set_on_some_snapshots_keeps_the_rest_of_the_series(
    multiperiod, multiperiod_revision
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
    selected = {
        "Manchester Wind": ({"period": 2040}, periods == 2040),
        "Norway Wind": ({"snapshot": first}, np.arange(len(periods)) == 0),
    }
    staged = WorkingRecord(record)
    for entity, (selector, _) in selected.items():
        staged.set("p_max_pu", 0.5, entity=entity, **selector)

    built = PyPSA.build(multiperiod_revision.store.commit(staged).record)
    for entity, (_, changed) in selected.items():
        series = multiperiod.c["Generator"].dynamic["p_max_pu"][entity]
        pd.testing.assert_series_equal(
            built.c["Generator"].dynamic["p_max_pu"][entity],
            series.where(~changed, 0.5),
            check_index_type=False,
        )
