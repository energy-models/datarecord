# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""An attribute addressed by the entity-type axis alone.

A per-type `icon` is a value per type, keyed once: rows of
`attributes/icon.parquet` keyed by `entity_type`, like any attribute. What the type axis may *not* do is key a value alongside `entity`,
where the type is determined by the entity and the row would be keyed twice
over.

Notes
-----
- [entity types](https://energy-models.github.io/datarecord/design/schema/#types)
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
"""

import narwhals as nw
import pandas as pd
import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.layered.resolve import write_schema
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Relation, Schema
from datarecord.sources import to_sources
from tests.fixtures import write_axis, write_values

TYPES = ["Bus", "Generator"]


@pytest.fixture
def typed_schema():
    """Two component types, with an `icon` addressed by the type axis alone."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "entity_type": Dimension(dtype=nw.Enum(TYPES)),
        },
        relations={"entity_type": Relation(key=["entity"], values="entity_type")},
        attributes={
            "p_nom": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
            "icon": AttributeSpec(
                dtype=nw.String(), dims={"entity_type"}, default="dot"
            ),
        },
        # `entity_type` is deliberately *not* partial: `partial` widens the owner
        # map and the resolution it keys, so it is kept to what a layer really
        # patches value by value. A layer touching one type's icon therefore owns
        # the whole type axis and restates it.
        partial=frozenset({"entity"}),
    )


@pytest.fixture
def root(con, base_uri, typed_schema):
    """A record with both types, and an icon for each."""
    write_schema(typed_schema, base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add("entity", pd.DataFrame([{"entity": "b1", "entity_type": "Bus"}]))
    staged.add(
        "entity",
        pd.DataFrame([{"entity": "g1", "entity_type": "Generator", "p_nom": 1.0}]),
    )
    child = staged.commit(NewChild(revision))
    write_axis(layer_dir(child.id), "entity_type", [{"entity_type": t} for t in TYPES])
    write_values(
        layer_dir(child.id),
        "icon",
        pd.DataFrame({"entity_type": TYPES, "icon": ["node", "turbine"]}),
    )
    return child


def _icons(record) -> dict[str, str]:
    """The icon each type resolves to, a broadcast row expanded to every type."""
    frame = to_sources(record, ["icon"])["icon"].collect("pandas").to_native()
    return {str(k): str(v) for k, v in zip(frame["entity_type"], frame["value"])}


def test_its_rows_are_keyed_by_the_type_alone(typed_schema):
    """One addressing coordinate, so one coordinate column."""
    assert typed_schema.long_columns_for("icon") == (
        "entity_type",
        "attribute",
        "breakpoint",
        "value",
    ), "the type, then the columns every long row has"


def test_it_belongs_to_no_component(typed_schema):
    """A value per type is not a value per entity."""
    assert typed_schema.coordinates_of("icon") == ("entity_type",), (
        "keyed by the type alone, with no `entity` column"
    )


def test_it_reads_back(root):
    assert _icons(root.record) == {"Bus": "node", "Generator": "turbine"}


def test_set_states_one_types_value(root, con):
    """A mapping keyed by label, with every untouched label left alone."""
    staged = WorkingRecord(root.record, con)
    staged.set("icon", {"Generator": "windmill"})
    assert _icons(staged) == {"Bus": "node", "Generator": "windmill"}


def test_a_scalar_reaches_every_type(root, con):
    staged = WorkingRecord(root.record, con)
    staged.set("icon", "square")
    assert _icons(staged) == {"Bus": "square", "Generator": "square"}


def test_a_child_layer_restates_every_types_icon(root, con):
    """`entity_type` is not `partial`, so touching one icon carries every type's.

    No exception for being the type axis: a dim outside `partial` is one a layer
    owns entirely once it touches it.
    """
    staged = WorkingRecord(root.record, con)
    staged.set("icon", {"Generator": "windmill"})

    rows = staged.resolver.sources[-1].attribute("icon")
    assert rows is not None
    patch = rows.df()
    assert sorted(str(t) for t in patch["entity_type"]) == ["Bus", "Generator"], (
        "every type's icon, not just the edited one"
    )

    child = staged.commit(NewChild(root))
    assert _icons(child.record) == {"Bus": "node", "Generator": "windmill"}, (
        "the untouched type keeps its icon because this layer carried it"
    )


def test_an_enum_label_the_dtype_does_not_declare_is_refused(root, con):
    """The staging column is the axis's `Enum`, so the insert itself rejects it.

    Wrapped rather than surfaced: DuckDB says "could not convert to UINT8",
    naming the enum's storage type instead of the dim or its vocabulary.
    """
    staged = WorkingRecord(root.record, con)
    with pytest.raises(ValueError, match="pins the vocabulary"):
        staged.set("icon", {"Nope": "x"})
    assert _icons(staged) == {"Bus": "node", "Generator": "turbine"}, "nothing staged"


def test_entity_is_refused_for_a_type_addressed_attribute(root, con):
    """An icon is keyed by its type alone, so `entity=` has nothing to scope."""
    staged = WorkingRecord(root.record, con)
    with pytest.raises(ValueError, match="does not vary over"):
        staged.set("icon", "x", entity=["g1"])


def test_naming_the_type_alongside_entity_is_refused():
    """The entity determines the type, so the row would be keyed twice over."""
    with pytest.raises(ValidationError, match="keys a row twice over"):
        Schema(
            dimensions={
                "entity": Dimension(dtype=nw.String()),
                "entity_type": Dimension(dtype=nw.Enum(TYPES)),
            },
            relations={"entity_type": Relation(key=["entity"], values="entity_type")},
            attributes={
                "p_nom": AttributeSpec(
                    dtype=nw.Float64(), dims={"entity", "entity_type"}
                )
            },
            partial=frozenset({"entity"}),
        )
