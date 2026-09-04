# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record whose components have no declared types.

A schema may omit the entity-type axis, and where it does there is nothing to
classify a component into: `entity_types` is empty, a component's constant
columns live on `dims/entity.parquet` itself rather than in a per-type member
file, and no `dims/entity_type/` directory is written at all. A tool that needs
types declares the axis in the schema it builds; the record layer does not
require one.

Notes
-----
- [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
- [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
"""

from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir, resolved_dir
from datarecord.layered.resolve import write_schema
from datarecord.layered.write import write_record
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema

# The label every component here is added under. Free-form: with no enum to
# check it against, it is data the record carries rather than a declaration.
KIND = "thing"


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
def root(con, base_uri, untyped_schema):
    """A committed, materialised record holding two untyped components."""
    write_schema(untyped_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(
        KIND,
        pd.DataFrame(
            [{"entity": "a", "p_nom": 1.0}, {"entity": "b", "p_nom": 2.0}],
        ),
    )
    child = staged.commit(NewChild(revision))
    child.materialise()
    return child


def _entities(record) -> dict:
    """The record's `dims["entity"]` frame as an `entity -> p_nom` mapping."""
    frame = record.dims["entity"].collect().to_native().to_pandas()
    return dict(zip(frame["entity"], frame["p_nom"], strict=True))


def test_every_entity_addressed_attribute_is_carried(untyped_schema):
    """With no type vocabulary there is nothing to narrow against."""
    s = untyped_schema
    assert s.entity_types == frozenset(), "no axis, so no declared labels"
    assert sorted(s.attributes_for(KIND)) == ["p_max_pu", "p_nom"], (
        "entity-addressed only; `weighting` belongs to the record"
    )
    assert s.attributes_on("entity") == ("p_nom",), (
        "the non-varying entity-addressed attribute is a column of the entity axis"
    )


def test_a_component_round_trips_through_the_entity_axis(root):
    """`add` and commit work with a label the schema never declared.

    The value written under a label the schema never declared reads back off
    the entity axis itself - one file, not a per-type member file - and
    `entity_types` is empty, there being nothing to classify into.
    """
    assert list(root.record.entity_types) == [], "no declared axis, so no types"
    assert _entities(root.record) == {"a": 1.0, "b": 2.0}


def test_no_entity_type_directory_is_written(con, base_uri, untyped_schema):
    """A written untyped layer has no `dims/entity_type/` at all.

    Where today an `add` under each label wrote one member file per label, the
    constant columns are on the entity axis and no per-type directory exists.
    """
    write_schema(untyped_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(KIND, pd.DataFrame([{"entity": "a", "p_nom": 1.0}]))
    staged.add("other", pd.DataFrame([{"entity": "c", "p_nom": 3.0}]))
    child = staged.commit(NewChild(revision))

    assert not Path(layer_dir(child.id) + "dims/entity_type").exists()
    assert _entities(child.record) == {"a": 1.0, "c": 3.0}


def test_an_entity_type_column_is_rejected(con, base_uri, untyped_schema):
    """An `entity_type` on the untyped entity axis is refused, not relocated.

    The `_validate_frame` special case that admitted the column is gone where no
    group declares the axis, so a source handing one over disagrees with the
    schema about what the file holds.
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

        def entity_types(self):
            return ()

        def entity_type(self, name):
            return None

        def groups(self):
            return ()

        def group(self, name):
            return None

        def attributes(self, kind="inputs"):
            return ()

        def attribute(self, name, kind="inputs"):
            return None

        frozen = True

    revision = Revision.create(con)
    with pytest.raises(ValueError, match="entity_type"):
        write_record(revision.id, WithEntityType(), con)


def test_set_reaches_a_named_entity(root, con):
    """`set(..., entity=[...])` patches the named rows of the entity axis.

    `p_nom` is a column of `dims/entity.parquet` untyped, so the edit selects
    labels of that axis - the same names a member-file edit would - and the
    unnamed component keeps its value.
    """
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 5.0, entity=["a"])
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"a": 5.0, "b": 2.0}


def test_set_with_no_names_reaches_every_entity(root, con):
    """A scalar with no `entity=` broadcasts to every label the axis has."""
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 9.0)
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"a": 9.0, "b": 9.0}


def test_an_unknown_label_is_accepted(root, con):
    """No enum to check against, so any label is data rather than an error.

    Two labels, one axis: neither is a type, so the components sit together on
    `dims/entity.parquet` and `entity_types` stays empty.
    """
    staged = WorkingRecord(root.record, con)
    staged.add("other", pd.DataFrame([{"entity": "c", "p_nom": 3.0}]))
    child = staged.commit(NewChild(root))
    assert list(child.record.entity_types) == []
    assert _entities(child.record) == {"a": 1.0, "b": 2.0, "c": 3.0}


def test_remove_drops_a_component_through_the_axis_alone(root, con):
    """`remove` writes one tombstone, on the entity axis, and the fold honours it."""
    staged = WorkingRecord(root.record, con)
    staged.remove(KIND, ["a"])
    child = staged.commit(NewChild(root))
    assert _entities(child.record) == {"b": 2.0}


def test_the_resolved_axis_has_no_entity_type_column(root):
    """The resolved entity axis carries no `entity_type` where none is declared.

    Not a column of NULLs - genuinely absent, in the live relation and in the
    materialised `resolved/dims/entity.parquet` alike. A cache still carrying it
    is what would make `entity_types()` answer `{None}`.
    """
    axis = root.resolver.axis("entity")
    assert axis is not None
    assert "entity_type" not in axis.columns
    assert "p_nom" in axis.columns

    persisted = pd.read_parquet(resolved_dir(root.id) + "dims/entity.parquet")
    assert "entity_type" not in persisted.columns


def test_a_resolved_record_reads_the_same_as_an_unresolved_one(
    con, base_uri, untyped_schema
):
    """Materialising a node changes no value an untyped record reports.

    A component's value read through the unmaterialised layer and through the
    materialised node cache agree - the assertion the `ResolvedLayer`
    prerequisite exists to protect, now that the entity axis carries the value.
    """
    write_schema(untyped_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(
        KIND,
        pd.DataFrame([{"entity": "a", "p_nom": 1.0}, {"entity": "b", "p_nom": 2.0}]),
    )
    child = staged.commit(NewChild(revision))

    unresolved = _entities(child.record)
    child.materialise()
    resolved = _entities(child.record)
    assert unresolved == resolved == {"a": 1.0, "b": 2.0}
