# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A NULL on a categorized dim reaches every label, whatever its category.

Notes
-----
- [owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
- [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
"""

import pytest

from datarecord.mutable import WorkingRecord
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from tests.fixtures import broadcast, empty_root, write_schema


@pytest.fixture
def categorized():
    """`entity` categorized by `ctype` and `carrier`."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype="str"),
            "bus": Dimension(dtype="str"),
            "snapshot": Dimension(dtype="int"),
            "ctype": Dimension(one_of=["Bus", "Generator", "Link", "Load"]),
            "carrier": Dimension(one_of=["gas", "wind"]),
        },
        properties={
            "entity": {
                "ctype": PropertySpec(one_of="ctype"),
                "carrier": PropertySpec(one_of="carrier"),
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
    staged.add("entity", ["g1"], ctype="Generator", carrier="gas")
    staged.add("entity", ["w1"], ctype="Load", carrier="wind")
    staged.add("entity", ["l1"], ctype="Load", carrier="gas")
    staged.add("entity", ["b1"], ctype="Bus")
    return revision.child(staged.edits)


EVERY = {"g1", "w1", "l1", "b1"}


def _values(record, attribute: str, dim: str = "entity") -> dict:
    """`attribute` resolved per label of `dim`."""
    return {label: v for (label,), v in broadcast(record, attribute, dim).items()}


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
                "bus_type": Dimension(one_of=["AC", "DC"]),
            },
            properties={"bus": {"bus_type": PropertySpec(one_of="bus_type")}},
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
    staged.add("bus", ["ac1", "ac2"], bus_type="AC")
    staged.add("bus", ["dc1"], bus_type="DC")
    return revision.child(staged.edits)


def test_a_category_keyword_scopes_a_value_on_another_dim_than_entity(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus_type="AC")
    staged.set("v_nom", 380.0)
    record = buses.child(staged.edits).record

    assert _values(record, "frequency", "bus") == {"ac1": 60.0, "ac2": 60.0}
    assert set(_values(record, "v_nom", "bus")) == {"ac1", "ac2", "dc1"}, (
        "a NULL reaches all"
    )
    assert set(record.flags(bus_type="AC")) == {"frequency", "v_nom"}
    assert set(record.flags(bus_type="DC")) == {"v_nom"}, "no DC bus has a frequency"


def test_attributes_leave_a_default_by_unapplied(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus="ac1")
    record = buses.child(staged.edits).record
    assert _values(record, "frequency", "bus") == {"ac1": 60.0}, (
        "ac2 has no row, so it is absent rather than its default_by 50.0"
    )


@pytest.fixture
def ports(store):
    """A link `l` taking from `a` and giving to `b`, and a generator `g` attached to `b`."""
    write_schema(
        Schema(
            dimensions={
                "entity": Dimension(dtype="str"),
                "bus": Dimension(dtype="str"),
                "snapshot": Dimension(dtype="int"),
                "role": Dimension(one_of=["input", "output", "attached"]),
                "ctype": Dimension(one_of=["Generator", "Link"]),
                "connection": Dimension(over=["entity", "bus"]),
            },
            properties={
                "entity": {"ctype": PropertySpec(one_of="ctype")},
                "connection": {"role": PropertySpec(one_of="role")},
            },
            attributes={
                "efficiency": AttributeSpec(
                    dtype="float",
                    dims={"connection", "snapshot"},
                    default=1.0,
                    default_by={"connection.role": {"output": 0.9}},
                ),
                "p_nom": AttributeSpec(dtype="float", dims={"entity"}),
            },
            partial=frozenset({"entity", "bus", "connection"}),
        ),
        store,
    )
    revision = empty_root(store)
    staged = WorkingRecord(revision.record)
    staged.add("entity", ["l", "g"], ctype=["Link", "Generator"])
    staged.add("bus", ["a", "b"])
    staged.add("snapshot", [0, 1])
    tuples = [
        {"entity": "l", "bus": "a"},
        {"entity": "l", "bus": "b"},
        {"entity": "g", "bus": "b"},
    ]
    staged.add("connection", tuples, role=["input", "output", "attached"])
    return revision.child(staged.edits)


def test_a_multi_dim_is_categorized_by_its_property(ports):
    """`role=` selects connections in `set` and `flags` as `ctype=` selects entities.

    Notes
    -----
    - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
    """
    staged = WorkingRecord(ports.record)
    staged.set("efficiency", 0.5, role="output", snapshot=[0])
    staged.set("efficiency", 0.8, role=["input", "attached"], bus="b")
    staged.set("p_nom", 1.0)
    record = ports.child(staged.edits).record

    assert broadcast(record, "efficiency", "connection", "snapshot") == {
        (("l", "b"), 0): 0.5,
        (("g", "b"), 0): 0.8,
        (("g", "b"), 1): 0.8,
    }, "the role selects the tuples, a coordinate keyword narrows them"
    assert set(record.flags(role="output")) == {"efficiency"}
    assert record.flags(role="input") == {}, "the input's value was narrowed away"
    assert record.schema.categorizing("connection") == ("role",)
    assert [
        record.schema.default_for("efficiency", role=r) for r in ("input", "output")
    ] == [
        1.0,
        0.9,
    ]


def test_a_multi_dims_category_selects_only_for_an_attribute_over_it(ports):
    staged = WorkingRecord(ports.record)
    with pytest.raises(ValueError, match="'p_nom' is not over"):
        staged.set("p_nom", 1.0, role="output")


def test_a_multi_dims_category_combines_with_a_category_of_its_coordinate(ports):
    staged = WorkingRecord(ports.record)
    staged.set("efficiency", 0.5, role=["output", "attached"], ctype="Generator")
    record = ports.child(staged.edits).record
    assert broadcast(record, "efficiency", "connection", "snapshot") == {
        (("g", "b"), 0): 0.5,
        (("g", "b"), 1): 0.5,
    }, "the link's output is no generator's"


def test_a_listed_dim_widened_in_is_on_a_record_read_before(store, root, categorized):
    """A record read once keeps its fold; the listed axis comes from the schema in force."""
    assert set(root.record.dims) >= {"ctype", "carrier"}
    dimensions = {**categorized.dimensions, "sector": Dimension(one_of=["power"])}
    write_schema(categorized.model_copy(update={"dimensions": dimensions}), store)
    assert store.revision(root.id).record.dims["sector"].df()["sector"].tolist() == [
        "power"
    ]
