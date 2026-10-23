# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A NULL on a categorized dim reaches every label, whatever its category.

Notes
-----
- [owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
- [categories](https://energy-models.github.io/datarecord/design/schema/#categories-labels-of-an-enum-dtype-dim)
"""

import pytest

from datarecord.layered.resolve import write_schema
from datarecord.mutable import WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, PropertySpec, Schema
from tests.fixtures import empty_root


@pytest.fixture
def categorized():
    """`entity` categorized by `ctype` and `carrier`."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype="str"),
            "bus": Dimension(dtype="str"),
            "snapshot": Dimension(dtype="int"),
            "ctype": Dimension(dtype={"one_of": ["Bus", "Generator", "Link", "Load"]}),
            "carrier": Dimension(dtype={"one_of": ["gas", "wind"]}),
        },
        properties={
            "entity": {
                "ctype": PropertySpec(dtype={"one_of": "ctype"}),
                "carrier": PropertySpec(dtype={"one_of": "carrier"}),
            }
        },
        attributes={
            "p_max_pu": AttributeSpec(dtype="float", dims={"entity", "snapshot"}),
        },
        partial=frozenset({"entity", "bus"}),
    )


@pytest.fixture
def root(store, categorized):
    """A root with a gas generator, a wind load, a gas load and a bus-category label."""
    write_schema(categorized, store)
    revision = empty_root(store)
    staged = WorkingRecord(revision.record)
    staged.add("ctype", ["Bus", "Generator", "Link", "Load"])
    staged.add("carrier", ["gas", "wind"])
    staged.add("entity", ["g1"], ctype="Generator", carrier="gas")
    staged.add("entity", ["w1"], ctype="Load", carrier="wind")
    staged.add("entity", ["l1"], ctype="Load", carrier="gas")
    staged.add("entity", ["b1"], ctype="Bus")
    return revision.child(staged.edits)


EVERY = {"g1", "w1", "l1", "b1"}


def _values(record, attribute: str) -> dict:
    """`record.attributes[attribute]` as `entity -> value`."""
    frame = record.attributes[attribute].df()
    assert frame["entity"].is_unique, f"one row per entity, got {frame}"
    return dict(zip(frame["entity"], frame["value"], strict=True))


def test_a_null_reaches_a_label_added_in_the_same_staged_record(root):
    staged = WorkingRecord(root.record)
    staged.set("p_max_pu", 0.9)
    staged.add("entity", ["g2"], ctype="Generator")
    assert _values(staged, "p_max_pu") == dict.fromkeys(EVERY | {"g2"}, 0.9), (
        "every label, a load and the bus included"
    )


def test_a_null_reaches_a_label_added_in_an_unmaterialised_child(root):
    staged = WorkingRecord(root.record)
    staged.set("p_max_pu", 0.9)
    layer = root.child(staged.edits)
    later = WorkingRecord(layer.record)
    later.add("entity", ["g2"], ctype="Generator", carrier="gas")
    child = layer.child(later.edits)
    assert _values(child.record, "p_max_pu") == dict.fromkeys(EVERY | {"g2"}, 0.9), (
        "a generator added below the broadcast is reached by it"
    )


@pytest.fixture
def buses(store):
    """Two AC buses and a DC one; `frequency` defaults by type."""
    write_schema(
        Schema(
            dimensions={
                "bus": Dimension(dtype="str"),
                "bus_type": Dimension(dtype={"one_of": ["AC", "DC"]}),
            },
            properties={
                "bus": {"bus_type": PropertySpec(dtype={"one_of": "bus_type"})}
            },
            attributes={
                "v_nom": AttributeSpec(dtype="float", dims={"bus"}),
                "frequency": AttributeSpec(
                    dtype="float",
                    dims={"bus"},
                    default=0.0,
                    default_by={"bus.bus_type": {"AC": 50.0}},
                ),
            },
            partial=frozenset({"bus"}),
        ),
        store,
    )
    revision = empty_root(store)
    staged = WorkingRecord(revision.record)
    staged.add("bus_type", ["AC", "DC"])
    staged.add("bus", ["ac1", "ac2"], bus_type="AC")
    staged.add("bus", ["dc1"], bus_type="DC")
    return revision.child(staged.edits)


def _by_bus(record, attribute: str) -> dict:
    """`record.attributes[attribute]` as `bus -> value`."""
    frame = record.attributes[attribute].df()
    return dict(zip(frame["bus"], frame["value"], strict=True))


def test_a_category_keyword_scopes_a_value_on_another_dim_than_entity(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus_type="AC")
    staged.set("v_nom", 380.0)
    record = buses.child(staged.edits).record

    assert _by_bus(record, "frequency") == {"ac1": 60.0, "ac2": 60.0}
    assert set(_by_bus(record, "v_nom")) == {"ac1", "ac2", "dc1"}, "a NULL reaches all"
    assert set(record.flags(bus_type="AC")) == {"frequency", "v_nom"}
    assert set(record.flags(bus_type="DC")) == {"v_nom"}, "no DC bus has a frequency"


def test_attributes_leave_a_default_by_unapplied(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus="ac1")
    record = buses.child(staged.edits).record
    assert _by_bus(record, "frequency") == {"ac1": 60.0}, (
        "ac2 has no row, so it is absent rather than its default_by 50.0"
    )
