# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`Record`, and the `RecordLike` protocol it is one implementation of.

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
"""

from dataclasses import dataclass

import narwhals as nw
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir, resolved_dir
from datarecord.layered.revision import Record
from datarecord.layered.sources import ParquetLayer
from datarecord.layered.write import write_record
from datarecord.mutable import WorkingRecord
from datarecord.record import Flags, Frames, RecordLike
from datarecord.schema import AttributeSpec, Schema
from tests.fixtures import (
    export_network,
    names,
    schema,
    write_attribute,
    write_entity_type,
    write_schema,
)

MEMBERS = ("dims", "relations", "attributes")


@pytest.fixture
def written(con, base_uri, ac_dc):
    """A record of one layer, so every construction below reads the same files."""
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    return revision


@pytest.fixture
def both(written, con):
    """One record, every way of constructing a `Record` over it.

    The same files reached by two source constructions: as the layer of a
    revision, and as a plain directory at a URI. They must agree on everything,
    which is what says reading a directory needs no implementation of its own -
    a fold over one source is a scan of it.

    A `WorkingRecord` with nothing staged is here for the mirrored reason: it is
    a `Record` one layer deeper, and an empty staging area must add nothing.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    node = Record(written.resolver)
    directory = Record.at(layer_dir(written.id), con)
    return (
        node,
        directory,
        WorkingRecord(node, con),
        WorkingRecord(directory, con),
    )


# -- the protocol ------------------------------------------------------------


def test_every_construction_satisfies_the_protocol(both):
    """Structural conformance, so a consumer cannot tell which it holds.

    Notes
    -----
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    """
    for record in both:
        assert isinstance(record, RecordLike)


def test_a_plain_dict_backed_record_satisfies_the_protocol(con):
    """`Frames` is the `Mapping` ABC, so eager `dict` values are a valid backing.

    The protocol asks for named lazy frames, not for a mapping that defers
    building them - so whether an implementation is lazy like `LazyFrames` or
    eager like this stays its own business.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    """
    entity_axis = nw.from_native(con.sql("SELECT 'wind' AS entity, FALSE AS deleted"))
    kinds = nw.from_native(
        con.sql("SELECT 'wind' AS entity, 'Generator' AS entity_type, FALSE AS deleted")
    )
    # `p_nom`'s own coordinates and no others: no `entity_type` in a long
    # row, and no `bus`, which is the connection relation's coordinate rather than
    # a column every attribute carries.
    long = nw.from_native(
        con.sql(
            "SELECT 'wind' AS entity, 'p_nom' AS attribute,"
            " NULL::DOUBLE AS breakpoint, 100.0 AS value,"
            " NULL::VARCHAR AS snapshot, NULL::VARCHAR AS scenario,"
            " NULL::BIGINT AS period"
        )
    )

    @dataclass(frozen=True)
    class DictRecord:
        schema: Schema
        dims: Frames
        relations: Frames
        attributes: Frames

        def flags(self, **labels) -> dict[str, Flags]:
            return {}

    record = DictRecord(
        schema(),
        {"entity": entity_axis},
        {"entity_type": kinds},
        {"p_nom": long},
    )
    assert isinstance(record, RecordLike)
    assert record.attributes["p_nom"].implementation == nw.Implementation.DUCKDB

    # And `write_record` consumes it, which is the point of widening the type:
    # it iterates and looks up, both of which a `dict` answers.
    revision = Revision.create(con)
    write_record(revision.id, record, con)
    assert "p_nom" in Record.at(layer_dir(revision.id), con).attributes


def test_revision_exposes_its_record(written):
    """`revision.record` is the entry point; `resolver` stays the DuckDB view."""
    record = written.record
    assert isinstance(record, RecordLike), "the protocol, structurally"
    assert isinstance(record, Record), "and the class this package provides"
    assert record.resolver is written.resolver


def test_constructions_agree_on_every_key_set(both):
    """One record, several ways of reading it: the keys must not depend on which."""
    node, *rest = both
    for other in rest:
        for member in MEMBERS:
            assert list(getattr(node, member)) == list(getattr(other, member)), member


def _types(record) -> list[str]:
    """The component types the record's `entity_type` relation names, sorted."""
    kinds = record.relations["entity_type"].collect("pandas").to_native()
    return sorted(kinds["entity_type"].unique())


def test_constructions_agree_on_flags(both):
    """One aggregate, whichever source list it folds over.

    Notes
    -----
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    """
    node, *rest = both
    for other in rest:
        for ctype in _types(node):
            entities = names(node, ctype)
            assert node.flags(entity=entities) == other.flags(entity=entities), ctype


def test_constructions_agree_on_rows(both):
    """A single-layer record resolves to the same rows every way.

    The rows themselves, not their count: this fixture is the whole evidence
    that reading a directory needs no implementation of its own, and two
    constructions returning the same number of different rows would satisfy a
    count.
    """
    node, *rest = both
    for other in rest:
        for attribute in node.attributes:
            left = node.attributes[attribute].collect().to_native().to_pandas()
            right = other.attributes[attribute].collect().to_native().to_pandas()
            columns = sorted(set(left.columns) & set(right.columns))
            assert set(left.columns) == set(right.columns), attribute
            left = left[columns].sort_values(columns).reset_index(drop=True)
            right = right[columns].sort_values(columns).reset_index(drop=True)
            assert left.equals(right), attribute


# -- laziness (https://energy-models.github.io/datarecord/design/record/#frames, https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) ---------------------------------------------------


def test_frames_stay_unmaterialised(both):
    """An overlay-backed frame is a DuckDB plan, not a materialised table."""
    node, *_ = both
    frame = node.attributes["p_max_pu"]
    assert isinstance(frame, nw.LazyFrame)
    assert frame.implementation == nw.Implementation.DUCKDB
    # A narwhals operation pushes into the plan rather than executing it.
    filtered = frame.filter(~nw.col("snapshot").is_null())
    assert "FILTER" in filtered.to_native().explain().upper()


def test_keys_list_without_building(both):
    """Listing a record is cheap; only a lookup does work.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    """
    for record in both:
        assert "p_max_pu" in record.attributes
        assert "nope" not in record.attributes
        assert len(list(record.attributes)) == len(record.attributes)


# -- flags, the one non-frame member (https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) ----------------------------------


def test_flags_are_per_component_type(con, base_uri):
    """One file, two types, different shapes - OR-ing across them would lose both."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, "Generator", [{"entity": "wind"}])
    write_entity_type(layer, "Link", [{"entity": "dc"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            # Generator: series only. Link: static only.
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"entity": "dc", "value": 1.0}],
    )

    record = revision.record
    generator = record.flags(entity=names(record, "Generator"))["p_max_pu"]
    link = record.flags(entity=names(record, "Link"))["p_max_pu"]
    # The Generator's rows set `snapshot`; the Link's leaves it NULL. Naming
    # the dim is what makes these two answers distinguishable at all.
    assert "snapshot" in generator.varies
    assert "snapshot" not in generator.broadcast
    assert "snapshot" in link.broadcast
    assert "snapshot" not in link.varies


def test_a_materialised_map_survives_a_dim_being_declared(con, base_uri):
    """Adding a dim is compatible, and the persisted owner map too.

    The flags live in two structs rather than a `varies_<dim>` column each, so
    a map written before the dim existed differs from the current schema by a
    struct *field*, which `UNION ALL BY NAME` fills with NULL - where a missing
    *column* would have to be reconciled. The new dim reads as unset, which is
    what it is: no row mentions it.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    narrow = {"snapshot": nw.Datetime(), "period": nw.Int64()}
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema(dims=narrow, partial=set()))
    write_entity_type(layer, "Generator", [{"entity": "wind"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ],
    )
    revision.materialise()
    # The map on disk knows nothing of `scenario` - without that this test
    # would pass whatever the flags' layout.
    uri = f"{resolved_dir(revision.id)}owner_map/attributes.parquet"
    persisted = con.sql(f"SELECT varies FROM read_parquet('{uri}')")
    assert "scenario" not in str(persisted.types[0])

    # The dim arrives after the map is on disk.
    write_schema(schema(dims={**narrow, "scenario": nw.String()}, partial=set()))
    child = revision.child()
    record = Record(child.resolver)
    flags = record.flags(entity=names(record, "Generator"))["p_max_pu"]
    assert "snapshot" in flags.varies
    assert "scenario" not in flags.varies
    assert "scenario" not in flags.broadcast


def test_flags_report_both_sets_where_components_disagree(con, base_uri):
    """A mixed type puts `snapshot` in both sets, which is the split.

    Two generators, one with a series and one with a single value. Both
    containers are needed, and both sets holding `snapshot` is what says so -
    the constant pass takes the NULL-snapshot rows, the series pass the rest.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, "Generator", [{"entity": "wind"}, {"entity": "gas"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"entity": "gas", "value": 1.0}],
    )

    record = revision.record
    combined = record.flags(entity=names(record, "Generator"))["p_max_pu"]
    assert "snapshot" in combined.varies
    assert "snapshot" in combined.broadcast


def test_flags_are_scoped_to_what_an_attribute_is_addressed_by(con, base_uri):
    """A dim an attribute has no column for is in neither set, not in `broadcast`.

    `varies | broadcast` is the test for whether an attribute touches a dim at
    all, so a dim it is not addressed by has to be absent from both - otherwise
    a consumer builds a container along an axis the attribute has no values on.

    The two are easy to conflate because one relation holds every long
    attribute's rows: `capital_cost` is stored beside `p_max_pu`, whose
    `snapshot` column is NULL for `capital_cost`'s rows. That NULL is "no such
    axis", not "every snapshot".

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(
        schema(
            attributes={
                "Generator": {
                    "capital_cost": AttributeSpec(
                        dtype=nw.Float64(), dims={"entity", "period"}
                    ),
                    "p_max_pu": AttributeSpec(
                        dtype=nw.Float64(), dims={"entity", "snapshot"}
                    ),
                }
            }
        )
    )
    write_entity_type(layer, "Generator", [{"entity": "wind"}])
    write_attribute(
        layer, "capital_cost", [{"entity": "wind", "period": 2030, "value": 1.0}]
    )
    write_attribute(layer, "p_max_pu", [{"entity": "wind", "value": 0.9}])

    record = revision.record
    flags = record.flags(entity=names(record, "Generator"))
    assert flags["capital_cost"].varies == frozenset({"entity", "period"}), (
        "`entity` is a dim like `period`, so a row naming it varies over it"
    )
    assert flags["capital_cost"].broadcast == frozenset(), (
        "a dim `capital_cost` has no column for is not one it broadcasts over"
    )
    # Addressed by `snapshot`, with a row leaving it NULL - that *is* a
    # broadcast, and the two cases must not read the same.
    assert "snapshot" in flags["p_max_pu"].broadcast


def test_flags_report_a_curve(con, base_uri):
    """`breakpoints` distinguishes a curve from a scalar, from either backing.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, "Process", [{"entity": "steel"}])
    write_attribute(
        layer,
        "marginal_cost",
        [
            {"entity": "steel", "breakpoint": x, "value": v}
            for x, v in ((0.0, 20.0), (50.0, 35.0))
        ],
    )

    record = revision.record
    assert record.flags(entity=names(record, "Process"))["marginal_cost"].breakpoints


# -- more than one layer, which is where the fold stops being a scan ---------


def test_node_record_resolves_the_overlay(con, base_uri, ac_dc):
    """One layer read alone against the same layer folded onto its parent."""
    root = Revision.create(con)
    export_network(ac_dc, root, con)
    root.materialise()

    child = root.child()
    write_attribute(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Gas", "value": 0.1}],
    )

    overlay = Record(child.resolver)
    # The single-layer view is the raw file, read through the `LayerSource`: the
    # folding resolver (`Record.at`) is the whole-tree lens, wrong for "what does
    # this one patch hold".
    layer_only = ParquetLayer(child.id, child.resolver.schema, con).attribute(
        "p_max_pu"
    )

    # The child's own layer holds one row; the resolution holds the root's too.
    assert layer_only is not None and len(layer_only) == 1
    resolved = overlay.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert len(resolved) > 1
    patched = resolved[resolved["entity"] == "Manchester Gas"]["value"].tolist()
    assert patched == [0.1]


def test_node_record_orders_members(con, base_uri, ac_dc):
    """A `Record` promises member order; for an overlay that means `order_key`.

    Notes
    -----
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    """
    root = Revision.create(con)
    export_network(ac_dc, root, con)
    root.materialise()

    child = root.child()
    write_entity_type(layer_dir(child.id), "Generator", [{"entity": "New Solar"}])

    generators = names(Record(child.resolver), "Generator")
    assert generators == [*ac_dc.c["Generator"].static.index, "New Solar"], (
        "first-introduced order: the root's members, then the child's addition"
    )


# -- one layer read at its URI, with no tree around it -----------------------


def test_a_directory_at_a_uri_reads_a_plain_record(con, base_uri, ac_dc, tmp_path):
    """No revision and no tree: any parquet directory blocks wrote is a `Record`."""
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    record = Record.at(layer_dir(revision.id), con)
    assert isinstance(record, RecordLike)
    assert names(record, "Generator"), "the network's generators are members"
    assert "p_max_pu" in record.attributes
    assert record.schema.attributes


def test_a_directory_has_no_connections_when_none_were_written(con, base_uri, tmp_path):
    """A record with no `relations/connection.parquet` has no such key, not an error.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, "Generator", [{"entity": "wind"}])

    assert "connection" not in Record.at(layer, con).relations


def test_a_directory_reads_the_ports_a_network_wrote(written, con):
    """A record a network wrote has its connections, and its ports with their roles.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    record = Record.at(layer_dir(written.id), con)
    assert {"connection", "port_entity", "port_bus"} <= set(record.relations), (
        "the network's connections and both port relations"
    )

    roles = record.attributes["role"].collect().to_native().to_pandas()
    assert set(roles["value"]) == {"input", "output", "attached"}, (
        "one file across every type, so a Generator's role sits beside a Link's"
    )


def test_missing_key_raises(both):
    """A key the record does not hold is a `KeyError`, not an empty frame."""
    for record in both:
        with pytest.raises(KeyError):
            record.attributes["not_an_attribute"]


# -- one schema per record root (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ----------------------------------------


def test_two_roots_in_one_process_read_their_own_schema(tmp_path):
    """A connection's schema comes from *its* root, not the process default.

    `connect(base_uri=...)` already scopes a connection to one record - its
    `layer_dir` macro derives from that root - so the manifest
    beside those layers is a property of the connection too. Two records on
    two roots therefore disagree about their dims without either being wrong.

    Every declared dim is a broadcast dim now, `entity`, the connection's
    `bus` and `port` included, so each tuple is the root's whole dim list rather
    than its value dims alone.
    """
    from datarecord import duck
    from datarecord.layered.resolve import write_schema as write_manifest

    roots = {}
    cases: list[tuple[str, dict[str, nw.dtypes.DType]]] = [
        ("a", {"scenario": nw.String()}),
        ("b", {"vintage": nw.Int64()}),
    ]
    for name, dims in cases:
        root = str(tmp_path / name)
        con = duck.connect(base_uri=root)
        write_manifest(schema(dims=dims, partial=set(dims)), root)
        roots[name] = (root, con, Revision.create(con))

    (_, _, revision_a), (root_b, con_b, revision_b) = roots["a"], roots["b"]
    assert revision_a.record.schema.broadcast_dims == (
        "entity",
        "bus",
        "port",
        "scenario",
        "entity_type",
    ), "root a reads its own `scenario`, beside the fixture's other dims"
    assert revision_b.record.schema.broadcast_dims == (
        "entity",
        "bus",
        "port",
        "vintage",
        "entity_type",
    ), "root b reads its own `vintage`, beside the fixture's other dims"

    # A layer read directly needs no schema supplied either: its own directory
    # carries none (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record), so the connection's root answers - which is what
    # `Record.at` used to take a `declared` argument for.
    layer = Record.at(layer_dir(revision_b.id, root_b), con_b)
    assert layer.schema.broadcast_dims == (
        "entity",
        "bus",
        "port",
        "vintage",
        "entity_type",
    ), "a layer read at its URI takes its connection's root schema"

    for _, con, _ in roots.values():
        con.close()
