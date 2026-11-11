# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A NULL on a categorized dim reaches every label, whatever its category.

Notes
-----
- [owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
- [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
"""

import pandas as pd
import pytest

from datarecord.mutable import WorkingRecord
from datarecord.schema.categories import with_defaults
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from tests.fixtures import broadcast, empty_child, empty_root


@pytest.fixture(scope="module")
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
                "ctype": PropertySpec(refers="ctype"),
                "carrier": PropertySpec(refers="carrier"),
            }
        },
        attributes={
            "p_max_pu": AttributeSpec(dtype="float", dims={"entity", "snapshot"}),
        },
        partial=frozenset({"entity", "bus"}),
    )


@pytest.fixture(scope="module")
def root(module_store, categorized):
    """A root with a gas generator, a wind load, a gas load and a bus-category label."""
    revision = empty_root(module_store, categorized).commit()
    staged = WorkingRecord(revision.record)
    staged.add("entity", ["g1"], ctype="Generator", carrier="gas")
    staged.add("entity", ["w1"], ctype="Load", carrier="wind")
    staged.add("entity", ["l1"], ctype="Load", carrier="gas")
    staged.add("entity", ["b1"], ctype="Bus")
    return revision.store.commit(staged)


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
    layer = root.store.commit(staged)
    later = WorkingRecord(layer.record)
    later.add("entity", ["g2"], ctype="Generator", carrier="gas")
    child = layer.store.commit(later)
    assert _values(child.record, "p_max_pu") == dict.fromkeys(EVERY | {"g2"}, 0.9), (
        "a generator added below the broadcast is reached by it"
    )


@pytest.fixture(scope="module")
def buses(module_store):
    """Two AC buses and a DC one; `frequency` defaults by type."""
    revision = empty_root(
        module_store,
        Schema(
            dimensions={
                "bus": Dimension(dtype="str"),
                "bus_type": Dimension(one_of=["AC", "DC"]),
            },
            properties={"bus": {"bus_type": PropertySpec(refers="bus_type")}},
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
    ).commit()
    staged = WorkingRecord(revision.record)
    staged.add("bus", ["ac1", "ac2"], bus_type="AC")
    staged.add("bus", ["dc1"], bus_type="DC")
    return revision.store.commit(staged)


def test_a_category_keyword_scopes_a_value_on_another_dim_than_entity(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus_type="AC")
    staged.set("v_nom", 380.0)
    record = buses.store.commit(staged).record

    assert _values(record, "frequency", "bus") == {"ac1": 60.0, "ac2": 60.0}
    assert set(_values(record, "v_nom", "bus")) == {"ac1", "ac2", "dc1"}, (
        "a NULL reaches all"
    )
    assert set(record.flags(bus_type="AC")) == {"frequency", "v_nom"}
    assert set(record.flags(bus_type="DC")) == {"v_nom"}, "no DC bus has a frequency"


def test_attributes_leave_a_default_by_unapplied(buses):
    staged = WorkingRecord(buses.record)
    staged.set("frequency", 60.0, bus="ac1")
    record = buses.store.commit(staged).record
    assert _values(record, "frequency", "bus") == {"ac1": 60.0}, (
        "ac2 has no row, so it is absent rather than its default_by 50.0"
    )


@pytest.fixture(scope="module")
def ports(module_store):
    """A link `l` taking from `a` and giving to `b`, and a generator `g` attached to `b`."""
    revision = empty_root(
        module_store,
        Schema(
            dimensions={
                "entity": Dimension(dtype="str"),
                "bus": Dimension(dtype="str"),
                "snapshot": Dimension(dtype="int"),
                "role": Dimension(one_of=["input", "output", "attached"]),
                "ctype": Dimension(one_of=["Generator", "Link", "Store"]),
                "connection": Dimension(over=["entity", "bus"]),
            },
            properties={
                "entity": {
                    "ctype": PropertySpec(refers="ctype"),
                    "note": PropertySpec(dtype="str"),
                },
                "connection": {
                    "role": PropertySpec(refers="role"),
                    "note": PropertySpec(dtype="str"),
                },
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
    ).commit()
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
    return revision.store.commit(staged)


def test_a_multi_dim_is_categorized_by_its_property(ports):
    """`role=` selects connections in `set` and `flags` as `ctype=` selects entities.

    Notes
    -----
    - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
    """
    staged = WorkingRecord(ports.record)
    staged.set("efficiency", 0.5, role="output", snapshot=[0])
    staged.set("efficiency", 0.8, role=["input", "attached"], bus="b")
    staged.set("p_nom", 1.0)
    record = ports.store.commit(staged).record

    assert broadcast(record, "efficiency", "connection", "snapshot") == {
        (("l", "b"), 0): 0.5,
        (("g", "b"), 0): 0.8,
        (("g", "b"), 1): 0.8,
    }, "the role selects the tuples, a field keyword narrows them"
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


def test_a_multi_dims_category_combines_with_a_category_of_its_field(ports):
    staged = WorkingRecord(ports.record)
    staged.set("efficiency", 0.5, role=["output", "attached"], ctype="Generator")
    record = ports.store.commit(staged).record
    assert broadcast(record, "efficiency", "connection", "snapshot") == {
        (("g", "b"), 0): 0.5,
        (("g", "b"), 1): 0.5,
    }, "the link's output is no generator's"


def _noted(record, subject: str) -> set:
    """The labels of `subject` whose `note` is set, a tuple as `(entity, bus)`."""
    rows = record.properties[subject].df()
    noted = rows[rows["note"].notna()][subject]
    return {tuple(v.values()) if isinstance(v, dict) else v for v in noted}


@pytest.mark.parametrize(
    ("subject", "keywords", "selected"),
    [
        (
            "connection",
            {"role": ["output", "attached"], "bus": "b"},
            {("l", "b"), ("g", "b")},
        ),
        ("connection", {"ctype": "Link"}, {("l", "a"), ("l", "b")}),
    ],
    ids=["multi-dim-several", "field-category"],
)
def test_a_category_keyword_selects_what_a_dotted_property_set_reaches(
    ports, subject, keywords, selected
):
    """`role=` selects the tuples of `connection.note` as `ctype=` selects the labels of `entity.note`.

    Notes
    -----
    - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
    """
    staged = WorkingRecord(ports.record)
    staged.set(f"{subject}.note", "x", **keywords)
    assert _noted(staged, subject) == selected


def test_a_one_of_dim_a_child_declares_is_on_its_record(root, categorized):
    """A parent read once keeps its fold; the `one_of` dim's axis comes from the child's schema."""
    assert set(root.record.dimensions) >= {"ctype", "carrier"}
    dimensions = {**categorized.dimensions, "sector": Dimension(one_of=["power"])}
    child = empty_child(
        root, categorized.model_copy(update={"dimensions": dimensions})
    ).commit()
    assert child.record.dimensions["sector"].df()["sector"].tolist() == ["power"]
    assert "sector" not in root.record.dimensions, "the parent keeps its own schema"


def test_with_defaults_keeps_the_order_of_a_relation_filtered_by_subqueries(
    workspace, categorized
):
    """Two `IN (SELECT …)` filters estimate ~0 rows, so DuckDB builds the defaults join on `rel`."""
    con = workspace.con
    names = [f"e{i:02d}" for i in range(24)]
    entities = pd.DataFrame(
        {"entity": names, "ctype": "Generator", "carrier": ["gas", "wind"] * 12}
    )
    carriers = con.from_df(pd.DataFrame({"carrier": ["gas"]}))  # noqa: F841 - bound by name for the query below
    labels = con.from_df(pd.DataFrame({"entity": names}))  # noqa: F841 - bound by name for the query below
    rel = (
        con.from_df(entities)
        .project("*, row_number() OVER () AS _row")
        .filter("carrier IN (SELECT carrier FROM carriers)")
        .filter("entity IN (SELECT entity FROM labels)")
        .order("_row")
        .project("* EXCLUDE (_row)")
    )
    defaulted = with_defaults(categorized, "entity", rel, ["ctype", "carrier"], con)
    assert defaulted.df()["entity"].tolist() == names[::2], "rows keep `rel`'s order"
