# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`Record`, and the `RecordLike` protocol it is one implementation of.

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
"""

from dataclasses import dataclass

import pytest
from duckdb import DuckDBPyRelation

from datarecord.mutable import WorkingRecord
from datarecord.protocols import Flags, RecordLike, Relations
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    empty_child,
    empty_root,
    schema,
    write_attribute,
    write_component,
    write_group,
    write_schema,
)

MEMBERS = ("dims", "groups", "attributes")


@pytest.fixture
def written(store):
    """A record of one layer, so every construction below reads the same files.

    One generator with a `p_max_pu` series and an `efficiency` on its
    connection, so attributes, the entity axis and a group are all present.
    """
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(schema(), store)
    write_component(revision, "Generator", [{"entity": "wind"}])
    write_group(
        revision, "connection", [{"connection": {"entity": "wind", "bus": "north"}}]
    )
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ],
    )
    write_attribute(
        layer, "efficiency", [{"entity": "wind", "bus": "north", "value": 0.9}]
    )
    return revision


@pytest.fixture
def both(written, store):
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
    node = written.record
    directory = store.layer(written.id)
    return (
        node,
        directory,
        WorkingRecord(node),
        WorkingRecord(directory),
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


def test_a_network_source_is_a_record(store, ac_dc):
    """`to_datarecord` returns one too, which is what puts read and write on one seam."""
    assert isinstance(PyPSA.to_datarecord(ac_dc, store.workspace.con), RecordLike)


@dataclass(frozen=True)
class DictRecord:
    schema: Schema
    dims: Relations
    properties: Relations
    groups: dict[str, Relations]
    attributes: Relations

    def flags(self, **categories: object) -> dict[str, Flags]:
        return {}


def _dict_record(store, snapshot: str = "NULL::VARCHAR") -> DictRecord:
    """One `wind` entity with a `p_nom`, as eager relations, `snapshot` the SQL of its column."""
    entity_axis = store.workspace.con.sql(
        "SELECT 'wind' AS entity, 'Generator' AS ctype"
    )
    # `p_nom`'s own coordinates and no others: no `ctype` in a long
    # row, and no `bus`, which is the connection group's coordinate rather than
    # a column every attribute carries.
    long = store.workspace.con.sql(
        "SELECT 'wind' AS entity, 'p_nom' AS attribute,"
        " NULL::DOUBLE AS breakpoint, 100.0 AS value,"
        f" {snapshot} AS snapshot, NULL::VARCHAR AS scenario,"
        " NULL::BIGINT AS period"
    )
    return DictRecord(
        schema(),
        {"entity": entity_axis.project("entity")},
        {"entity": entity_axis},
        {},
        {"p_nom": long},
    )


def test_a_plain_dict_backed_record_satisfies_the_protocol(store):
    """`Relations` is the `Mapping` ABC, so eager `dict` values are a valid backing.

    The protocol asks for named relations, not for a mapping that defers
    building them - so whether an implementation is lazy like `LazyRelations`
    or eager like this stays its own business.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/record/#relations)
    """
    record = _dict_record(store)
    assert isinstance(record, RecordLike)

    # And the writer consumes it, which is the point of widening the type:
    # it iterates and looks up, both of which a `dict` answers.
    revision = store.root(record)
    assert "p_nom" in store.layer(revision.id).attributes


def test_the_writer_refuses_timestamps_with_a_time_zone(store):
    """Written as `set` takes them: a zone is refused, not converted to naive time.

    Notes
    -----
    - [the write API](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    record = _dict_record(store, "TIMESTAMPTZ '2020-01-01 00:00:00+00'")
    with pytest.raises(ValueError, match="carry timestamps with a time zone"):
        store.root(record)


def test_constructions_agree_on_every_key_set(both):
    """One record, several ways of reading it: the keys must not depend on which."""
    node, *rest = both
    for other in rest:
        for member in MEMBERS:
            assert list(getattr(node, member)) == list(getattr(other, member)), member


def test_constructions_agree_on_flags(both):
    """One aggregate, whichever source list it folds over.

    Notes
    -----
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    """
    node, *rest = both
    for other in rest:
        assert node.flags() == other.flags()


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
            left = node.attributes[attribute].df()
            right = other.attributes[attribute].df()
            columns = sorted(set(left.columns) & set(right.columns))
            assert set(left.columns) == set(right.columns), attribute
            # A group's struct column arrives as dicts, which pandas cannot sort.
            order = {"by": columns, "key": lambda s: s.astype(str)}
            left = left[columns].sort_values(**order).reset_index(drop=True)
            right = right[columns].sort_values(**order).reset_index(drop=True)
            assert left.equals(right), attribute


# -- laziness (https://energy-models.github.io/datarecord/design/record/#relations, https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) ---------------------------------------------------


@pytest.mark.parametrize("member", ["dims", "properties", "groups", "attributes"])
def test_every_value_is_a_relation(both, member):
    """Each member's values are bare DuckDB relations, whatever the backing."""
    for record in both:
        values = getattr(record, member)
        assert values, f"the fixture has {member}"
        for key in values:
            assert isinstance(values[key], DuckDBPyRelation), (member, key)


def test_relations_stay_unmaterialised(both):
    """An overlay-backed relation is a DuckDB plan, not a materialised table."""
    node, *_ = both
    filtered = node.attributes["p_max_pu"].filter("snapshot IS NOT NULL")
    assert "FILTER" in filtered.explain().upper(), "a filter pushes into the plan"


# -- flags, the one non-frame member (https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) ----------------------------------


def test_flags_report_both_sets_where_components_disagree(store):
    """A mixed type puts `snapshot` in both sets, which is the split.

    Two generators, one with a series and one with a single value. Both
    containers are needed, and both sets holding `snapshot` is what says so -
    the constant pass takes the NULL-snapshot rows, the series pass the rest.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(schema(), store)
    write_component(revision, "Generator", [{"entity": "wind"}, {"entity": "gas"}])
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
    combined = record.flags()["p_max_pu"]
    assert "snapshot" in combined.varies
    assert "snapshot" in combined.broadcast


def test_flags_are_scoped_to_what_an_attribute_is_addressed_by(store):
    """A dim an attribute has no column for is in neither set, not in `broadcast`.

    `varies | broadcast` is the test for whether an attribute touches a dim at
    all, so a dim it is not addressed by has to be absent from both - otherwise
    a consumer builds a container along an axis the attribute has no values on.

    The two are easy to conflate because one relation holds every attribute's
    rows: `p_nom` is stored beside `p_max_pu`, whose `snapshot` column is NULL
    for `p_nom`'s rows. That NULL is "no such axis", not "every snapshot".

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(
        schema(
            attributes={
                "Generator": {
                    # Addressed by the entity axis and nothing else.
                    "p_nom": AttributeSpec(dtype="float", dims={"entity"}),
                    "p_max_pu": AttributeSpec(
                        dtype="float", dims={"entity", "snapshot"}
                    ),
                }
            }
        ),
        store,
    )
    write_component(revision, "Generator", [{"entity": "wind"}])
    write_attribute(layer, "p_nom", [{"entity": "wind", "value": 100.0}])
    write_attribute(layer, "p_max_pu", [{"entity": "wind", "value": 0.9}])

    record = revision.record
    flags = record.flags()
    # Addressed by `entity` alone, so no other axis is reportable either way.
    assert flags["p_nom"].varies == {"entity"}
    assert flags["p_nom"].broadcast == frozenset(), (
        "a dim `p_nom` has no column for is not one it broadcasts over"
    )
    # Addressed by `snapshot`, with a row leaving it NULL - that *is* a
    # broadcast, and the two cases must not read the same.
    assert "snapshot" in flags["p_max_pu"].broadcast


# -- more than one layer, which is where the fold stops being a scan ---------


def test_node_record_orders_members(store):
    """A `Record` promises member order: the deepest restatement's row order.

    The order holds through a closed intermediate node that changes nothing
    about membership.

    Notes
    -----
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    root = empty_root(store)
    write_schema(schema(), store)
    write_component(
        root,
        "Generator",
        [{"entity": "wind"}, {"entity": "gas"}, {"entity": "coal"}],
    )
    root.materialise()
    middle = empty_child(root)
    middle.materialise()

    child = empty_child(middle)
    write_component(child, "Generator", [{"entity": "New Solar"}])

    axis = child.record.properties["entity"].df()
    names = list(axis[axis["ctype"] == "Generator"]["entity"])
    assert names == ["wind", "gas", "coal", "New Solar"], (
        "the child restates the root's members in order, then its addition"
    )


# -- one layer read at its URI, with no tree around it -----------------------


def test_a_directory_has_no_connections_when_none_were_written(store, tmp_path):
    """A record with no `groups/connection.parquet` has no such key, not an error.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = empty_root(store)
    write_schema(schema(), store)
    write_component(revision, "Generator", [{"entity": "wind"}])

    assert "connection" not in store.layer(revision.id).groups


def test_missing_key_raises(both):
    """A key the record does not hold is a `KeyError`, not an empty frame.

    Listing the keys is cheap and answers membership without a lookup.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/record/#relations)
    """
    for record in both:
        assert "p_max_pu" in record.attributes
        assert "nope" not in record.attributes
        assert len(list(record.attributes)) == len(record.attributes)
        with pytest.raises(KeyError):
            record.attributes["not_an_attribute"]


def test_an_unsolved_network_has_no_results(both):
    """A result still at its default is absent rather than written.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/#writing-results)
    """
    for record in both:
        assert not {"p", "marginal_price"} & set(record.attributes)


# -- one schema per record root (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ----------------------------------------


def test_two_stores_on_one_workspace_read_their_own_schema(workspace, tmp_path):
    """A store's schema comes from *its* location, and so do its layers.

    Two records on two locations disagree about their dims without either
    being wrong, while sharing one workspace.
    """
    stores = {}
    cases: list[tuple[str, dict[str, str]]] = [
        ("a", {"scenario": "str"}),
        ("b", {"vintage": "int"}),
    ]
    for name, dims in cases:
        store = workspace.store(str(tmp_path / name))
        write_schema(schema(dims=dims, partial=set(dims)), store)
        revision = empty_root(store)
        write_attribute(
            store.layer_dir(revision.id), "p_nom", [{"entity": "g", "value": 1.0}]
        )
        stores[name] = (store, revision)

    (_, revision_a), (store_b, revision_b) = stores["a"], stores["b"]
    # Beyond `entity` and `bus`, which every schema here declares, the group.
    a, b = revision_a.record.schema, revision_b.record.schema
    assert a.broadcast_dims == ("entity", "bus", "scenario", "ctype", "connection")
    assert b.broadcast_dims == ("entity", "bus", "vintage", "ctype", "connection")

    # A layer read directly needs no schema supplied either: its own directory
    # carries none (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record), so its store's answers.
    assert store_b.layer(revision_b.id).schema.broadcast_dims == (
        "entity",
        "bus",
        "vintage",
        "ctype",
        "connection",
    )

    for name, (store, revision) in stores.items():
        location = tmp_path / name
        assert (location / "layers" / str(revision.id)).is_dir(), (
            "each location's layers land under it"
        )
        assert (location / "revisions" / f"{revision.id}.json").is_file()
