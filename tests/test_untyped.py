# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record whose schema declares no relation classifying its components.

A type is an ordinary relation, so a schema that declares none is a whole record:
a component's values are rows of `attributes/` keyed by `entity`, and `add`, `set`,
`remove` and materialisation need nothing more. A tool that needs types declares
the relation in the schema it builds; the record layer does not require one.

Notes
-----
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.layered.resolve import write_schema
from datarecord.layered.write import write_record
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema
from datarecord.sources import to_sources


@pytest.fixture
def untyped_schema():
    """Two attributes over `entity`, and no relation classifying it.

    `entity` is `partial`, so a layer adds or removes one component without
    restating the rest.
    """
    return Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "timestep": Dimension(dtype=nw.Datetime()),
        },
        attributes={
            "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
            "p_max_pu": AttributeSpec(
                dtype=nw.Float64(), dims={"entity", "timestep"}, default=1.0
            ),
            "weighting": AttributeSpec(dtype=nw.Float64(), dims={"timestep"}),
        },
        partial=frozenset({"entity"}),
    )


@pytest.fixture
def root(con, base_uri, untyped_schema):
    """A committed, materialised record holding two components."""
    write_schema(untyped_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(
        "entity",
        pd.DataFrame(
            [{"entity": "a", "p_nom": 1.0}, {"entity": "b", "p_nom": 2.0}],
        ),
    )
    child = staged.commit(NewChild(revision))
    child.materialise()
    return child


def _entities(record) -> dict:
    """`p_nom` per live entity, a broadcast row expanded to every entity."""
    frame = to_sources(record, ["p_nom"])["p_nom"].collect("pandas").to_native()
    return dict(zip(frame["entity"], frame["value"], strict=True))


def test_a_component_round_trips(root):
    """`add` and commit with no type: the value reads back."""
    assert _entities(root.record) == {"a": 1.0, "b": 2.0}


def test_an_entity_type_column_is_rejected(con, base_uri, untyped_schema):
    """An `entity_type` on the entity axis is refused, not relocated.

    No attribute declares the column there, so a source handing one over
    disagrees with the schema about what the file holds.
    """
    write_schema(untyped_schema, base_uri)

    class WithEntityType:
        """A source whose entity axis carries an undeclared `entity_type`."""

        schema = untyped_schema

        def axes(self):
            return ("entity",)

        def axis(self, dim):
            if dim != "entity":
                return None
            return con.sql(
                "SELECT 'a' AS entity, 'thing' AS entity_type, "
                "FALSE AS deleted, 1.0 AS p_nom"
            )

        def relations(self):
            return ()

        def relation(self, name):
            return None

        def attributes(self):
            return ()

        def attribute(self, name):
            return None

        frozen = True

    revision = Revision.create(con)
    with pytest.raises(ValueError, match="entity_type"):
        write_record(revision.id, WithEntityType(), con)


def test_set_reaches_a_named_entity(root, con):
    """`set(..., entity=[...])` patches the named entity; the other keeps its value."""
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 5.0, entity=["a"])
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"a": 5.0, "b": 2.0}


def test_set_with_no_names_reaches_every_entity(root, con):
    """A scalar with no `entity=` is one broadcast row, which reaches every entity."""
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 9.0)
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"a": 9.0, "b": 9.0}


def test_a_child_add_joins_the_parents_entities(root, con):
    """A component added over a materialised parent sits beside its components.

    One axis, one file per layer: the child's row folds onto
    `dims/entity.parquet` with the parent's rather than replacing them.
    """
    staged = WorkingRecord(root.record, con)
    staged.add("entity", pd.DataFrame([{"entity": "c", "p_nom": 3.0}]))
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"a": 1.0, "b": 2.0, "c": 3.0}


def test_remove_drops_a_component_through_the_axis_alone(root, con):
    """`remove` writes one tombstone, on the entity axis, and the fold honours it."""
    staged = WorkingRecord(root.record, con)
    staged.remove("entity", ["a"])
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"b": 2.0}


def test_a_resolved_record_reads_the_same_as_an_unresolved_one(
    con, base_uri, untyped_schema
):
    """Materialising a node changes no value the record reports.

    A component's value read through the unmaterialised layer and through the
    materialised node cache agree - the assertion the `ResolvedLayer`
    prerequisite exists to protect.
    """
    write_schema(untyped_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(
        "entity",
        pd.DataFrame([{"entity": "a", "p_nom": 1.0}, {"entity": "b", "p_nom": 2.0}]),
    )
    child = staged.commit(NewChild(revision))

    unresolved = _entities(child.record)
    child.materialise()
    resolved = _entities(child.record)
    assert unresolved == resolved == {"a": 1.0, "b": 2.0}
