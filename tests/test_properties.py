# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Each accessor holds one section: `dims`, `properties`, `members`, per-category `flags`.

Notes
-----
- [step 2](https://energy-models.github.io/datarecord/design/proposals/properties/#step-2-the-section-decides-where-a-value-lives)
"""

import narwhals as nw
import pytest

from datarecord.layered.resolve import write_schema
from datarecord.mutable import WorkingRecord
from datarecord.schema import (
    AttributeSpec,
    Dimension,
    Group,
    PropertySpec,
    Schema,
    Trait,
)
from tests.fixtures import empty_root


@pytest.fixture
def schema():
    """`control` and `p_max_pu` are a generator's only; `sign` defaults by type."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "bus": Dimension(dtype=nw.String()),
            "snapshot": Dimension(dtype=nw.Int64()),
        },
        properties={
            "entity": {
                "entity_type": PropertySpec(dtype=nw.Enum(["Generator", "Load"])),
                "carrier": PropertySpec(dtype=nw.String(), default=""),
                "sign": PropertySpec(
                    dtype=nw.Float64(),
                    default=1.0,
                    default_by={"entity_type": {"Load": -1.0}},
                ),
                "control": PropertySpec(dtype=nw.String(), default="PQ"),
            },
            "connection": {"role": PropertySpec(dtype=nw.String())},
        },
        attributes={
            "p_max_pu": AttributeSpec(dtype=nw.Float64(), dims={"entity", "snapshot"}),
            "p_set": AttributeSpec(dtype=nw.Float64(), dims={"entity", "snapshot"}),
            "efficiency": AttributeSpec(dtype=nw.Float64(), dims={"connection"}),
        },
        groups={"connection": Group(over={"entity": "entity", "bus": "bus"})},
        traits={
            "generator": Trait(
                attributes=frozenset({"p_max_pu", "control"}),
                on={"entity_type": frozenset({"Generator"})},
            ),
        },
        partial=frozenset({"entity", "connection"}),
    )


@pytest.fixture
def revision(store, schema):
    """A generator and a load, each connected to one bus, `p_max_pu` and `p_set` set."""
    write_schema(schema, store)
    root = empty_root(store)
    staged = WorkingRecord(root.record)
    staged.add("entity", ["g1"], entity_type="Generator", carrier="wind")
    staged.add("entity", ["l1"], entity_type="Load")
    staged.add("bus", ["b0"])
    staged.add_group(
        "connection",
        nw.from_dict(
            {
                "connection": [
                    {"entity": "g1", "bus": "b0"},
                    {"entity": "l1", "bus": "b0"},
                ],
                "role": ["bus", "bus"],
            },
            backend="pandas",
        ),
    )
    staged.set("p_max_pu", 0.9)
    staged.set("p_set", 1.0, entity="l1")
    staged.set("efficiency", 0.5, entity="l1", bus="b0")
    return root.child(staged.edits)


@pytest.fixture
def record(revision):
    return revision.record


def _rows(frame: nw.LazyFrame) -> dict:
    """`frame` as `entity -> {column: value}`."""
    rows = frame.collect().to_native().to_pylist()
    return {r.pop("entity"): r for r in rows}


def test_an_axis_frame_is_its_key_only(record):
    assert record.dims["entity"].collect_schema().names() == ["entity"]


def test_properties_hold_stored_values_without_defaults(record):
    rows = _rows(record.properties["entity"])
    assert rows["g1"]["carrier"] == "wind"
    assert rows["l1"]["sign"] is None, "a stored-only read applies no default_by"


def test_a_group_property_is_in_properties(record):
    assert "role" in record.properties["connection"].collect_schema().names()
    assert "role" not in record.properties["entity"].collect_schema().names()


def test_members_project_to_the_carried_properties_with_defaults(record):
    frame = record.members("entity", entity_type="Load")
    assert "control" not in frame.collect_schema().names(), "a Generator's only"
    assert _rows(frame) == {
        "l1": {"entity_type": "Load", "carrier": "", "sign": -1.0}
    }, "sign reads its default_by with nothing stored, carrier its default"

    rows = _rows(record.members("entity"))
    assert rows["g1"]["control"] == "PQ"
    assert rows["l1"]["control"] is None, "a load does not carry control"
    assert rows["g1"]["sign"] == 1.0


def test_members_take_several_categories(record):
    frame = record.members("entity", entity_type=["Generator", "Load"])
    assert set(_rows(frame)) == {"g1", "l1"}


@pytest.mark.parametrize(
    ("read", "match"),
    [
        pytest.param(
            lambda r: r.members("entity", carrier="wind"),
            "do not categorize 'entity'",
            id="members-by-a-plain-property",
        ),
        pytest.param(
            lambda r: r.members("entity", entity_type="Store"),
            "not categories of 'entity_type'",
            id="members-by-a-stray-category",
        ),
        pytest.param(
            lambda r: r.flags(carrier="wind"),
            "categorize no dim",
            id="flags-by-a-plain-property",
        ),
    ],
)
def test_a_category_read_refuses_what_is_no_category(record, read, match):
    with pytest.raises(ValueError, match=match):
        read(record)


def test_flags_per_category_hold_what_the_category_carries(record):
    assert set(record.flags()) == {"p_max_pu", "p_set", "efficiency"}
    assert set(record.flags(entity_type="Load")) == {"p_set", "efficiency"}, (
        "p_max_pu is a Generator's only; efficiency reaches l1 through connection"
    )
    assert set(record.flags(entity_type="Generator")) == {"p_max_pu"}, (
        "p_set has no row on a generator"
    )


def test_a_scalar_property_reaches_labels_added_in_the_same_record(record):
    staged = WorkingRecord(record)
    staged.add("entity", ["g2"], entity_type="Generator")
    staged.set("carrier", "solar")
    carriers = {e: r["carrier"] for e, r in _rows(staged.properties["entity"]).items()}
    assert carriers == {"g1": "solar", "l1": "solar", "g2": "solar"}, (
        "a scalar reaches every label as staged, g2 included"
    )


def test_set_writes_a_property_on_the_labels_a_category_selects(record):
    staged = WorkingRecord(record)
    staged.set("carrier", "solar", entity_type="Generator")
    rows = _rows(staged.properties["entity"])
    assert rows["g1"]["carrier"] == "solar"
    assert rows["l1"]["carrier"] is None, "the load is not selected"


def test_a_property_keyword_matches_a_label_by_its_stored_value(record):
    staged = WorkingRecord(record)
    staged.set("p_set", 2.0, carrier="wind")
    staged.set("p_set", 3.0, control="PQ")
    rows = staged.attributes["p_set"].collect().to_native().to_pylist()
    values = {r["entity"]: r["value"] for r in rows}
    assert values == {"g1": 2.0, "l1": 1.0}, (
        "carrier is stored on g1 alone; control's default PQ is stored nowhere, "
        "so it selects no label and stages nothing"
    )


@pytest.mark.parametrize("committed", [False, True], ids=["staged", "committed"])
def test_set_of_a_category_re_categorizes_members_and_flags(revision, committed):
    staged = WorkingRecord(revision.record)
    staged.set("entity_type", "Load", entity="g1")
    record = revision.child(staged.edits).record if committed else staged
    assert set(_rows(record.members("entity", entity_type="Load"))) == {"g1", "l1"}
    assert _rows(record.members("entity", entity_type="Generator")) == {}
    assert set(record.flags(entity_type="Generator")) == set(), "no generator is left"
    assert set(record.flags(entity_type="Load")) == {"p_set", "efficiency"}, (
        "the broadcast p_max_pu reaches no load, g1 included"
    )
