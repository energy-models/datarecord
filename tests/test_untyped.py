# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record whose components have no declared types.

A schema may omit the discriminator, and where it does there is nothing to
classify a component into: no label narrows an attribute, and a component's
constant columns live on `dims/entity.parquet` as they always do. A tool that
needs types declares the discriminator in the schema it builds; the record
layer does not require one.

Notes
-----
- [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
- [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord.layered.resolve import write_schema
from datarecord.mutable import WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema
from tests.fixtures import empty_root


@pytest.fixture
def untyped_schema():
    """Two attributes over `entity`, and no axis classifying it."""
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
        partial=frozenset(),
    )


@pytest.fixture
def root(store, untyped_schema):
    """A committed, materialised record holding two untyped components."""
    write_schema(untyped_schema, store)
    revision = empty_root(store)
    staged = WorkingRecord(revision.record)
    staged.add("entity", ["a", "b"], p_nom=[1.0, 2.0])
    child = revision.child(staged.edits)
    child.materialise()
    return child


def _entities(record) -> dict:
    """The record's `dims["entity"]` frame as an `entity -> p_nom` mapping."""
    frame = record.dims["entity"].collect().to_native().to_pandas()
    return dict(zip(frame["entity"], frame["p_nom"], strict=True))


def test_every_attribute_is_carried(untyped_schema):
    """With no discriminator there is nothing to narrow against."""
    s = untyped_schema
    assert s.labels("entity") == frozenset(), "no discriminator, so no labels"
    assert sorted(s.attributes_for("entity", "thing")) == ["p_max_pu", "p_nom"], (
        "every attribute addressing the dim"
    )
    assert s.attributes_on("entity") == ("p_nom",), (
        "the non-varying entity-addressed attribute is a column of the entity axis"
    )


def test_a_component_round_trips_through_the_entity_axis(root):
    """`add` and commit; the value reads back off the entity axis itself."""
    assert _entities(root.record) == {"a": 1.0, "b": 2.0}


def test_an_entity_type_column_is_rejected(store, untyped_schema):
    """An `entity_type` on the untyped entity axis is refused, not relocated.

    The `_validate_frame` special case that admitted the column is gone where no
    group declares the axis, so a source handing one over disagrees with the
    schema about what the file holds.
    """
    write_schema(untyped_schema, store)

    class WithEntityType:
        """A source whose entity axis carries an undeclared `entity_type`."""

        schema = untyped_schema

        def axes(self):
            return ("entity",)

        def axis(self, dim):
            if dim != "entity":
                return None
            return store.workspace.con.sql(
                "SELECT 'a' AS entity, 'thing' AS entity_type, "
                "FALSE AS deleted, 1.0 AS p_nom"
            )

        def groups(self):
            return ()

        def group(self, name):
            return None

        def attributes(self, kind="inputs"):
            return ()

        def attribute(self, name, kind="inputs"):
            return None

        frozen = True

    with pytest.raises(ValueError, match="entity_type"):
        store.root(WithEntityType())


def test_set_reaches_a_named_entity(root, store):
    """`set(..., entity=[...])` patches the named rows of the entity axis.

    `p_nom` is a column of `dims/entity.parquet`, so the edit selects labels of
    that axis and the unnamed component keeps its value.
    """
    staged = WorkingRecord(root.record)
    staged.set("p_nom", 5.0, entity=["a"])
    child = root.child(staged.edits)
    assert _entities(child.record) == {"a": 5.0, "b": 2.0}


def test_set_with_no_names_reaches_every_entity(root, store):
    """A scalar with no `entity=` broadcasts to every label the axis has."""
    staged = WorkingRecord(root.record)
    staged.set("p_nom", 9.0)
    child = root.child(staged.edits)
    assert _entities(child.record) == {"a": 9.0, "b": 9.0}


def test_a_later_add_joins_the_axis(root, store):
    """A label added in a child sits beside the parent's on `dims/entity.parquet`."""
    staged = WorkingRecord(root.record)
    staged.add("entity", ["c"], p_nom=3.0)
    child = root.child(staged.edits)
    assert _entities(child.record) == {"a": 1.0, "b": 2.0, "c": 3.0}


def test_remove_drops_a_component_through_the_axis_alone(root, store):
    """`remove` writes one tombstone, on the entity axis, and the fold honours it."""
    staged = WorkingRecord(root.record)
    staged.remove("entity", ["a"])
    child = root.child(staged.edits)
    assert _entities(child.record) == {"b": 2.0}


def test_the_resolved_axis_has_no_entity_type_column(root):
    """The resolved entity axis carries no `entity_type` where none is declared.

    Not a column of NULLs - genuinely absent, in the live relation and in the
    materialised `resolved/dims/entity.parquet` alike.
    """
    axis = root.resolver.axis("entity")
    assert axis is not None
    assert "entity_type" not in axis.columns
    assert "p_nom" in axis.columns

    persisted = pd.read_parquet(
        root.store.resolved_dir(root.id) + "dims/entity.parquet"
    )
    assert "entity_type" not in persisted.columns


def test_a_resolved_record_reads_the_same_as_an_unresolved_one(store, untyped_schema):
    """Materialising a node changes no value an untyped record reports.

    A component's value read through the unmaterialised layer and through the
    materialised node cache agree - the assertion the `ResolvedLayer`
    prerequisite exists to protect, now that the entity axis carries the value.
    """
    write_schema(untyped_schema, store)
    revision = empty_root(store)
    staged = WorkingRecord(revision.record)
    staged.add("entity", ["a", "b"], p_nom=[1.0, 2.0])
    child = revision.child(staged.edits)

    unresolved = _entities(child.record)
    child.materialise()
    resolved = _entities(child.record)
    assert unresolved == resolved == {"a": 1.0, "b": 2.0}
