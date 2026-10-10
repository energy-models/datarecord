# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The tool layer: verify a record, build a model, read results back.

Notes
-----
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.tools.base import Requirements, Schema, UnsupportedRecordError
from datarecord.tools.pypsa import ENTITY_GROUP, REQUIRED_DIMS, PyPSA, _colliding_names
from tests.fixtures import (
    export_network,
    relation,
    schema,
    write_entities,
    write_input,
)


@pytest.fixture
def single_revision(con, base_uri, ac_dc):
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    return revision


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


def test_verify_accepts_a_complete_record(single_revision):
    """A record written by `export_to_parquet` supplies everything PyPSA needs."""
    assert not PyPSA.verify(single_revision.record)


def test_requires_reports_the_records_own_types(single_revision):
    """`requires` is record-dependent: PyPSA's axes plus this record's types."""
    req = PyPSA.requires(single_revision.record)
    assert {"snapshot", "period", "scenario"} <= req.dims
    assert "Generator" in req.entity_types
    # Required attributes come from PyPSA's registry, not a list we maintain.
    assert ("Generator", "bus") in req.attributes
    assert ("Line", "x") in req.attributes
    # `name` is the member identity, never an attribute row.
    assert not [a for a in req.attributes if a[1] == "entity"]


def test_verify_reports_a_missing_dim(con, base_uri, ac_dc):
    """A schema that declares no `scenario` dim cannot build a network.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    _with_schema(revision, dims={"snapshot": nw.Datetime()}, partial=set())

    missing = PyPSA.verify(revision.record)
    assert missing
    assert missing.dims == {"period", "scenario"}
    # A build refuses rather than failing deep inside PyPSA.
    with pytest.raises(UnsupportedRecordError, match="scenario"):
        PyPSA.build(revision.record)


def test_verify_reports_a_type_the_tool_does_not_know(con, base_uri, ac_dc):
    """An `entity_group` outside PyPSA's registry is reported by `verify`, not raised.

    The group is an attribute the record layer knows no vocabulary for, so a
    record holds any label and reads back fine. That this tool has no registry
    entry for one is its own business. `Requirements.entity_types` is what
    carries it.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_entities(layer_dir(revision.id), [{"entity": "w1", ENTITY_GROUP: "Widget"}])

    missing = PyPSA.verify(revision.record)
    assert missing.entity_types == {"Widget"}
    # The types PyPSA does know are not reported.
    assert "Generator" not in missing.entity_types
    with pytest.raises(UnsupportedRecordError, match="Widget"):
        PyPSA.build(revision.record)


def test_the_component_type_is_an_attribute_not_an_axis(multiperiod):
    """No entity-type dim, group or trait: the type is `entity_group` on `dims/entity.parquet`.

    So it never enters a spec as an axis, and a component's constant columns
    are columns of the entity axis, NULL on the rows of a type without one.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    source = PyPSA.to_datarecord(multiperiod)
    declared = source.schema
    assert "entity_type" not in declared.dimensions
    assert declared.entity_type_dim is None
    assert not declared.traits
    assert list(source.entity_types) == []
    assert {ENTITY_GROUP, "p_nom", "s_nom", "e_nom"} <= set(
        declared.attributes_on("entity")
    )

    axis = source.dims["entity"].collect().to_native().set_index("entity")
    assert axis.loc["Manchester Wind", ENTITY_GROUP] == "Generator"
    assert pd.isna(axis.loc["Manchester Wind", "s_nom"]), "no Generator has an `s_nom`"


_DIMS = {
    "snapshot": nw.Datetime(),
    "period": nw.Int64(),
    "scenario": nw.String(),
}


def _without_default(revision, ctype: str, attribute: str) -> None:
    """Drop one attribute's declared default, leaving the rest of the schema.

    `ctype` says which type the caller means it for; the spec itself is
    declared once record-wide, so dropping the default drops it everywhere.
    """
    was = read_schema()
    assert attribute in was.attributes_for(ctype)
    spec = was.attributes[attribute]
    was.attributes[attribute] = spec.model_copy(update={"default": None})
    write_schema(was)


def _with_schema(revision, **kwargs) -> None:
    """Redeclare the record's schema, keeping the attributes it already declares.

    Written directly rather than through `write_record`, which would reject an
    incompatible redeclaration - here the point is to hand the tool a
    schema it must report on rather than one the writer accepted.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    was = read_schema()
    now = schema(typed=False, **kwargs)
    # The tool's own axes (`carrier`, ...) stay, since its groups map into them.
    kept = {
        d: spec
        for d, spec in was.dimensions.items()
        if d not in now.dimensions and d not in REQUIRED_DIMS
    }
    dimensions = {**kept, **now.dimensions}
    addressable = set(dimensions) | set(was.groups)
    now = now.model_copy(
        update={
            "dimensions": dimensions,
            "attributes": {
                a: spec
                for a, spec in was.attributes.items()
                if spec.dims <= addressable
            },
            "groups": was.groups,
            "traits": was.traits,
            "meta": was.meta,
        }
    )
    write_schema(now)


def test_verify_reports_a_snapshot_key(con, base_uri, ac_dc):
    """`snapshot` as a key is reported, not crashed on.

    A limit of this tool's *representation*, not of the layer format: PyPSA's
    static/series split needs a component's whole series for an attribute to
    come from one layer, so a stored `snapshot = NULL` broadcast row and a
    descendant's per-snapshot row would coexist with no single container to
    put the result in. The record layer therefore permits the
    declaration - every file does carry the column - and the tool catches it.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    revision = Revision.create(con)
    write_schema(PyPSA.to_datarecord(ac_dc).schema)
    export_network(ac_dc, revision, con)
    _with_schema(revision, partial={"scenario", "snapshot"})

    missing = PyPSA.verify(revision.record)
    assert ("input_key", "snapshot") in missing.unsupported_keys
    assert "snapshot" in missing.describe()
    with pytest.raises(UnsupportedRecordError, match="snapshot"):
        PyPSA.build(revision.record)


def test_verify_reports_a_missing_required_attribute(con, base_uri, ac_dc):
    """A component type with no `bus` anywhere - not in the frame, not in the catalog."""
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    # A Generator member carrying no `bus` column at all, no connection row
    # supplying one (https://energy-models.github.io/datarecord/design/record/#connections), and a schema with no default for it either.
    write_entities(
        layer_dir(revision.id), [{"entity": "g1", ENTITY_GROUP: "Generator"}]
    )
    # One file across every type, so the Generators' rows are dropped from it
    # rather than a per-type file being unlinked.
    path = Path(layer_dir(revision.id), "groups", "connection.parquet")
    rows = pd.read_parquet(path)
    generators = set(ac_dc.c["Generator"].static.index) | {"g1"}
    rows[~rows["entity"].isin(generators)].to_parquet(path, index=False)
    _without_default(revision, "Generator", "bus")

    missing = PyPSA.verify(revision.record)
    assert ("Generator", "bus") in missing.attributes


def test_verify_accepts_a_declared_default_for_a_required_attribute(
    con, base_uri, ac_dc
):
    """A declared default makes an attribute resolvable with no row anywhere.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_entities(
        layer_dir(revision.id), [{"entity": "g1", ENTITY_GROUP: "Generator"}]
    )

    # PyPSA's own registry already declares `bus` with a `""` default, which
    # is exactly the case this pins - so the record is left as written.
    assert ("Generator", "bus") not in PyPSA.verify(revision.record).attributes


def test_verify_reports_a_piecewise_linear_attribute(con, base_uri, ac_dc):
    """A curve is stored correctly; it is the PyPSA translation that cannot express it.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_input(
        layer_dir(revision.id),
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


def test_verify_accepts_a_scalar_attribute(single_revision):
    """The same attribute without breakpoints is not reported (the negative half)."""
    write_input(
        layer_dir(single_revision.id),
        "marginal_cost",
        [{"entity": "Manchester Wind", "value": 20.0}],
    )
    assert not PyPSA.verify(single_revision.record).unsupported_values


def test_to_datarecord_rejects_a_cross_type_name_collision():
    """PyPSA scopes names per type; a record scopes them across every type.

    Reported rather than repaired: renaming to `Generator:north` would hand back
    a network whose components PyPSA can no longer find by their own names, so
    the mismatch is the caller's to reconcile.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    import pypsa

    n = pypsa.Network()
    n.add("Bus", "north")
    n.add("Generator", "north", bus="north")
    n.add("Load", "north", bus="north")

    with pytest.raises(UnsupportedRecordError, match="more than one component type"):
        PyPSA.to_datarecord(n)


def test_to_datarecord_accepts_distinct_names():
    """The negative half: the same network with distinct names is buildable."""
    import pypsa

    n = pypsa.Network()
    n.add("Bus", "north")
    n.add("Generator", "north gen", bus="north")

    assert not _colliding_names(n)
    assert PyPSA.to_datarecord(n) is not None


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


def test_pypsa_schema_is_the_identity(single_revision):
    """PyPSA defines the record vocabulary today, so nothing is renamed.

    The seam still routes every attribute, so an entry added later takes
    effect with no change to `build`.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    assert PyPSA.schema.attrs == {}
    assert PyPSA.schema.sources("Generator", "p_max_pu") == ("p_max_pu",)
    identity = PyPSA.schema.resolve(single_revision.record, "Generator", "p_max_pu")
    assert identity.fetchall() == relation(single_revision, "p_max_pu").fetchall()


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


def test_results_extracts_long_form_outputs(single_revision):
    """A solved network's results come back keyed by attribute, long-form.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    n = PyPSA.build(single_revision.record)
    n.optimize(solver_name="highs")

    results = PyPSA.results(n)
    assert "p" in results
    assert "p_nom_opt" in results

    # Narwhals frames, so the seam names no one dataframe library, and lazy so a
    # tool may fetch on demand (https://energy-models.github.io/datarecord/design/tools/).
    assert isinstance(results["p"], nw.LazyFrame)
    p = results["p"].collect()
    # The long schema's columns (https://energy-models.github.io/datarecord/design/record/), so the write path can persist it as-is -
    # and no `entity_type`, an attribute row being keyed by `name` (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
    assert {"entity", "snapshot", "scenario", "period", "value"} <= set(p.columns)
    assert "entity_type" not in p.columns
    assert set(p["attribute"].to_list()) == {"p"}
    # Series output: one row per (name, snapshot), for the Generator rows -
    # selected by name, since the frame no longer carries the type.
    gens = set(n.c["Generator"].static.index)
    gen = p.filter(nw.col("entity").is_in(list(gens)))
    assert len(gen) == len(n.snapshots) * len(gens)

    # A static output has no snapshot, and only the components whose value
    # differs from the default appear (some generators solve to p_nom_opt=0).
    nom = results["p_nom_opt"].collect()
    nom = nom.filter(nw.col("entity").is_in(list(gens)))
    assert nom["snapshot"].is_null().all()
    nonzero = n.c["Generator"].static["p_nom_opt"] != 0.0
    assert set(nom["entity"].to_list()) == set(n.c["Generator"].static.index[nonzero])


def test_results_concatenate_every_type_under_one_attribute(single_revision):
    """One `p` frame holds every type's rows, matching `outputs/p.parquet`.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    n = PyPSA.build(single_revision.record)
    n.optimize(solver_name="highs")

    p = PyPSA.results(n)["p"].collect()
    names = set(p["entity"].to_list())
    # `p` is a result of several types, so the concat is what is being tested;
    # keying by `(type, attribute)` would have split these into separate frames.
    # The names identify which type each row came from, no tag column needed -
    # that being what unique names buy the union (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
    contributing = {
        c.name
        for c in n.components
        if not c.static.empty and set(c.static.index) & names
    }
    assert len(contributing) > 1
    assert not p["entity"].is_null().any()


def test_results_skips_outputs_still_at_their_default(single_revision):
    """An unsolved network yields no `p`/`p_nom_opt` rows (the outputs default rule).

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    n = PyPSA.build(single_revision.record)
    results = PyPSA.results(n)
    assert "p" not in results
    assert "p_nom_opt" not in results


def test_a_second_tool_needs_no_record_change(con, base_uri, ac_dc):
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
            return Requirements(entity_types=frozenset({"Nope"}))

        def build(self, record):
            return "fake-model"

        def results(self, model):
            return {("Thing", "x"): model}

        def to_datarecord(self, model):
            return f"layer-of-{model}"

    fake = FakeTool()
    assert isinstance(fake, Tool)

    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    assert fake.requires(revision.record).dims == {"snapshot"}
    assert fake.verify(revision.record).entity_types == {"Nope"}
    assert fake.build(revision.record) == "fake-model"
    assert fake.results("m") == {("Thing", "x"): "m"}
    assert fake.to_datarecord("m") == "layer-of-m"
    # The record itself knows nothing of either tool.
    assert not hasattr(revision, "to_model")
    assert not PyPSA.verify(revision.record)


def test_schema_dims_stay_generic(con, base_uri, ac_dc):
    """`Coords` carries a dim PyPSA knows nothing about; axis names live in the tool.

    Declared but not `partial`, so the record's files need no column for it.
    Keying it would be a different matter, reported by the tool against the
    real record - see `test_verify_reports_unsupported_keys`.
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    _with_schema(revision, dims={**_DIMS, "vintage": nw.String()})
    dims = revision.resolver.dims
    assert "vintage" in dims.schema.dims
    # No axis rows anywhere, so the dim is absent from the mapping rather than
    # present-and-empty (https://energy-models.github.io/datarecord/design/record/#frames).
    assert "vintage" not in dims.axes
    # PyPSA's own required dims are still satisfied, and the extra dim is
    # simply not something the tool looks at.
    assert not PyPSA.verify(revision.record)
    assert "vintage" in dims.schema.long_columns


# -- axes, classifications and the spec (https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool) --


@pytest.fixture
def multiperiod_revision(con, base_uri, multiperiod):
    revision = Revision.create(con)
    export_network(multiperiod, revision, con)
    return revision


def _parquet(revision, path: str) -> pd.DataFrame:
    return pd.read_parquet(Path(layer_dir(revision.id), path))


def test_buses_and_carriers_are_axes_not_entities(multiperiod_revision):
    """Bus and Carrier rows are `dims/` files; no `Bus`, `Carrier`, `Shape` or
    `GlobalConstraint` entity is written.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    entities = _parquet(multiperiod_revision, "dims/entity.parquet")
    assert not set(entities[ENTITY_GROUP]) & {
        "Bus",
        "Carrier",
        "Shape",
        "GlobalConstraint",
    }
    bus = _parquet(multiperiod_revision, "dims/bus.parquet")
    assert {"busmap_all", "busmap_5", "busmap_10", "country", "v_nom"} <= set(
        bus.columns
    )
    assert {"bus_x", "y", "bus_geometry"} <= set(bus.columns), (
        "a Bus column another type also names is prefixed"
    )
    assert "carrier" not in bus.columns, "a bus's carrier is a group"
    carrier = _parquet(multiperiod_revision, "dims/carrier.parquet")
    assert {"carrier", "co2_emissions", "color", "nice_name"} <= set(carrier.columns)


def test_a_bus_series_is_keyed_by_bus(multiperiod_revision):
    rows = _parquet(multiperiod_revision, "inputs/v_mag_pu_set.parquet")
    assert "bus" in rows.columns
    assert "entity" not in rows.columns


@pytest.mark.parametrize(
    ("group", "key", "expected"),
    [
        ("entity_carrier", "Manchester Wind", "wind"),
        ("bus_carrier", "Norway DC", "DC"),
        ("entity_technology_group", "Norway Gas", "OCGT"),
        ("technology_group_carrier", "onwind", "wind"),
    ],
)
def test_classifications_are_groups(multiperiod_revision, group, key, expected):
    rows = _parquet(multiperiod_revision, f"groups/{group}.parquet")
    by_key = dict(zip(rows.iloc[:, 0], rows.iloc[:, 1], strict=True))
    assert by_key[key] == expected


def test_a_technology_group_carries_its_resource_class(multiperiod_revision):
    axis = _parquet(multiperiod_revision, "dims/technology_group.parquet")
    classes = dict(zip(axis["technology_group"], axis["resource_class"], strict=True))
    assert classes["onwind"] == 1
    assert pd.isna(classes["OCGT"])
    members = _parquet(multiperiod_revision, "dims/entity.parquet")
    assert "resource_class" not in members.columns, "it moved to the group's axis"


def test_geometry_is_wkt_with_its_crs_in_meta(multiperiod_revision, multiperiod):
    bus = _parquet(multiperiod_revision, "dims/bus.parquet").set_index("bus")
    assert bus.loc["Manchester", "region_onshore"].startswith("POLYGON")
    assert bus.loc["Norway", "region_offshore"].startswith("POLYGON")
    assert read_schema().meta["crs"] == multiperiod.crs.to_string()


def test_a_multiperiod_snapshot_is_within_its_period(multiperiod_revision):
    schema = read_schema()
    assert schema.dimensions["snapshot"].within == {"period"}
    axis = _parquet(multiperiod_revision, "dims/snapshot.parquet")
    assert {"period", "snapshot", "snapshot_weighting_objective"} <= set(axis.columns)
    periods = _parquet(multiperiod_revision, "dims/period.parquet")
    assert periods["period_year"].tolist() == [2030, 2040, 2050]


def _with_generators(n, **columns):
    m = n.copy()
    for column, values in columns.items():
        m.c["Generator"].static[column] = values
    return m


@pytest.mark.parametrize(
    ("columns", "match"),
    [
        ({"technology_group": "all"}, "exactly one carrier"),
        (
            {
                "technology_group": ["onwind", "OCGT"] * 3,
                "resource_class": [1, None, 2, None, 1, None],
            },
            "more than one 'resource_class'",
        ),
        (
            {
                "technology_group": [None, "gas"] * 3,
                "resource_class": [1, None] * 3,
            },
            "no technology group",
        ),
    ],
)
def test_technology_groups_must_be_functions_of_their_members(
    multiperiod, columns, match
):
    n = _with_generators(multiperiod, **columns)
    with pytest.raises(ValueError, match=match):
        PyPSA.to_datarecord(n)


def test_a_shape_the_record_cannot_place_is_rejected(multiperiod):
    n = multiperiod.copy()
    import shapely

    n.add(
        "Shape",
        ["admin"],
        geometry=[shapely.Point(0, 0)],
        idx=["London"],
        component=["Bus"],
        type=["admin"],
    )
    with pytest.raises(ValueError, match="neither a bus's onshore or offshore"):
        PyPSA.to_datarecord(n)


def test_the_shipped_spec_binds_to_the_record(multiperiod):
    """The base mathspec spec loads, prints, and names only what the record has.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    import mathspec

    from datarecord.tools.pypsa import MATHSPEC

    spec = mathspec.to_spec(str(MATHSPEC))
    assert "balance" in mathspec.to_markdown(spec)
    record = PyPSA.to_datarecord(multiperiod)
    PyPSA.check_spec(spec, record)

    extra = mathspec.merge(
        [
            spec,
            {
                "dimensions": {"entity": {}},
                "parameters": {"nowhere": {"dims": ["entity"]}},
            },
        ]
    )
    with pytest.raises(ValueError, match=r"parameters \['nowhere'\]"):
        PyPSA.check_spec(extra, record)


def test_the_shipped_spec_binds_to_a_record_without_storage(ac_dc):
    """Names only absent types register, and the technology-group axis, are not missing.

    Any other name stays required.

    Notes
    -----
    - [the mathspec spec](https://energy-models.github.io/datarecord/design/tools/#the-mathspec-spec)
    """
    import mathspec

    from datarecord.tools.pypsa import MATHSPEC

    n = ac_dc.copy()
    n.set_investment_periods([2030, 2040])
    spec = mathspec.to_spec(str(MATHSPEC))
    PyPSA.check_spec(spec, PyPSA.to_datarecord(n))

    extra = mathspec.merge([spec, {"given": {"parameters": {"nowhere": {"dims": []}}}}])
    with pytest.raises(ValueError, match=r"parameters \['nowhere'\]"):
        PyPSA.check_spec(extra, PyPSA.to_datarecord(n))


@pytest.mark.parametrize(
    ("component", "column", "other"),
    [
        ("Bus", "asset_status", "Generator"),
        ("Carrier", "country", "bus"),
    ],
)
def test_a_custom_axis_column_clashing_with_another_owner_is_reported(
    multiperiod, component, column, other
):
    """A clash between custom columns is reported, never renamed by the data.

    Renaming on clash would give one column a different record name in
    another network, which a fixed spec could not bind to.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    n = multiperiod.copy()
    n.c[component].static[column] = "x"
    with pytest.raises(UnsupportedRecordError, match=rf"'{column}' on .*{other}"):
        PyPSA.to_datarecord(n)


def test_an_axis_name_does_not_depend_on_the_network(ac_dc, multiperiod):
    """`country` is `country` on the bus axis whatever the entities carry."""
    names = [
        PyPSA.to_datarecord(n).schema.meta["names"]["bus"] for n in (ac_dc, multiperiod)
    ]
    assert names[0]["country"] == names[1]["country"] == "country"
    assert names[0]["bus_x"] == "x", "a registry clash is prefixed in every network"
