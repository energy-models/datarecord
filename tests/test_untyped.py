# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record whose components have no declared types.

`entity_type` is a mapping like any other and a schema may omit it, in which
case every attribute addressed by `entity` reaches every component. A tool that
needs types declares the axis in the schema it builds; the record layer does
not require one.

Notes
-----
- [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
- [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.layered.resolve import write_schema
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


def test_every_entity_addressed_attribute_is_carried(untyped_schema):
    """With no type vocabulary there is nothing to narrow against."""
    s = untyped_schema
    assert s.entity_types == frozenset(), "no axis, so no declared labels"
    assert sorted(s.attributes_for(KIND)) == ["p_max_pu", "p_nom"], (
        "entity-addressed only; `weighting` belongs to the record"
    )


def test_a_component_round_trips_without_a_type(root):
    """`add` and commit work with a label the schema never declared."""
    assert list(root.record.entity_types) == [KIND]
    frame = root.record.entity_types[KIND].collect().to_native().to_pandas()
    assert dict(zip(frame["entity"], frame["p_nom"], strict=True)) == {
        "a": 1.0,
        "b": 2.0,
    }


def test_set_reaches_a_named_entity(root, con):
    """An edit resolves the entity's type from the record, not from the schema."""
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 5.0, entity=["a"])
    child = staged.commit(NewChild(root))

    rows = child.record.attributes["p_nom"].collect().to_native().to_pandas()
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {"a": 5.0}


def test_set_with_no_names_reaches_every_entity(root, con):
    """`names=None` falls back to the resolved components.

    `types_declaring` is empty with no declared labels, so the types come from
    what the record actually holds - otherwise a record-wide edit would
    silently reach nothing.
    """
    staged = WorkingRecord(root.record, con)
    staged.set("p_nom", 9.0)
    child = staged.commit(NewChild(root))

    rows = child.record.attributes["p_nom"].collect().to_native().to_pandas()
    assert dict(zip(rows["entity"], rows["value"], strict=True)) == {
        "a": 9.0,
        "b": 9.0,
    }


def test_an_unknown_label_is_accepted(root, con):
    """No enum to check against, so any label is data rather than an error."""
    staged = WorkingRecord(root.record, con)
    staged.add("other", pd.DataFrame([{"entity": "c", "p_nom": 3.0}]))
    child = staged.commit(NewChild(root))
    assert set(child.record.entity_types) == {KIND, "other"}
