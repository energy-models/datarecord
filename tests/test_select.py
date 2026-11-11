# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Selecting a view of a record at some of its labels.

Notes
-----
- [selecting](https://energy-models.github.io/datarecord/reference/selecting/)
"""

from datetime import datetime
from typing import Any

import pytest
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation
from duckdb import StarExpression as star

from datarecord import (
    AttributeSpec,
    Dimension,
    Flags,
    PropertySpec,
    RecordLike,
    Schema,
    Selection,
    WorkingRecord,
    select,
    settle,
)
from datarecord.duck import ex_all, fn
from tests.fixtures import empty_root

FEB = datetime(2013, 2, 1)
SNAPSHOTS = [
    datetime(2013, 1, 15),
    FEB,
    datetime(2013, 2, 15),
    datetime(2013, 3, 1),
    datetime(2013, 4, 1),
]


@pytest.fixture(scope="module")
def schema():
    """Buses in states in countries, entities attached to buses, scenarios staged by period.

    `bus.carrier` defaults to `AC`; `entity.sign` and `p_nom` default by
    `entity.ctype`, `p_min` and `p_max` by it and `entity.tech` in either order;
    `note` is a property of two subjects; `stage` is a multi-dim over two dims
    a test squeezes.
    """
    return Schema(
        dimensions={
            "entity": Dimension(),
            "bus": Dimension(),
            "carrier": Dimension(),
            "state": Dimension(),
            "country": Dimension(),
            "scenario": Dimension(),
            "period": Dimension(dtype="int"),
            "snapshot": Dimension(dtype="datetime"),
            "ctype": Dimension(one_of=["Generator", "Link", "Load"]),
            "tech": Dimension(one_of=["solar", "onwind"]),
            "connection": Dimension(over=["entity", "bus"]),
            "corridor": Dimension(over={"from": "bus", "to": "bus"}),
            "stage": Dimension(over=["scenario", "period"]),
        },
        properties={
            "entity": {
                "ctype": PropertySpec(refers="ctype"),
                "tech": PropertySpec(refers="tech"),
                "carrier": PropertySpec(refers="carrier"),
                "sign": PropertySpec(
                    dtype="float",
                    default=1.0,
                    default_by={"entity.ctype": {"Load": -1.0}},
                ),
                "note": PropertySpec(dtype="str"),
            },
            "bus": {
                "carrier": PropertySpec(refers="carrier", default="AC"),
                "state": PropertySpec(refers="state"),
                "note": PropertySpec(dtype="str"),
            },
            "state": {"country": PropertySpec(refers="country")},
        },
        attributes={
            "capacity": AttributeSpec(dtype="float", dims={"entity"}),
            "p_nom": AttributeSpec(
                dtype="float",
                dims={"entity", "scenario"},
                default=0.0,
                default_by={"entity.ctype": {"Load": -1.0}},
            ),
            "p_min": AttributeSpec(
                dtype="float",
                dims={"entity"},
                default=0.0,
                default_by={
                    "entity.ctype": {"Load": -1.0},
                    "entity.tech": {"solar": 0.5},
                },
            ),
            "p_max": AttributeSpec(
                dtype="float",
                dims={"entity"},
                default=1.0,
                default_by={
                    "entity.tech": {"solar": 0.5},
                    "entity.ctype": {"Load": 0.0},
                },
            ),
            "price": AttributeSpec(dtype="float", dims={"bus", "snapshot", "scenario"}),
            "efficiency": AttributeSpec(dtype="float", dims={"connection"}),
            "flow": AttributeSpec(dtype="float", dims={"corridor"}),
            "stage_cost": AttributeSpec(dtype="float", dims={"stage"}),
            "weight": AttributeSpec(dtype="float", dims={"scenario"}),
        },
        partial=frozenset(
            {
                "entity",
                "bus",
                "scenario",
                "period",
                "snapshot",
                "connection",
                "corridor",
                "stage",
            }
        ),
    )


@pytest.fixture(scope="module")
def record(module_store, schema):
    root = empty_root(module_store, schema).commit()
    with WorkingRecord(root.record) as w:
        w.add("carrier", ["AC", "DC", "wind", "gas"])
        w.add("country", ["DE", "FR"])
        w.add("state", ["BY", "NW", "IDF"], country=["DE", "DE", "FR"])
        w.add("bus", ["north", "south", "paris"], state=["NW", "BY", "IDF"])
        w.set("bus.carrier", {"north": "AC", "paris": "DC"})
        w.add("entity", ["wind1"], ctype="Generator", carrier="wind", tech="onwind")
        w.add("entity", ["gas1"], ctype="Generator", carrier="gas")
        w.add("entity", ["load1"], ctype="Load")
        w.add("entity", ["dc"], ctype="Link", carrier="DC")
        w.add(
            "connection",
            [
                {"entity": e, "bus": b}
                for e, b in [
                    ("wind1", "north"),
                    ("gas1", "south"),
                    ("load1", "paris"),
                    ("dc", "north"),
                    ("dc", "paris"),
                ]
            ],
        )
        w.add(
            "corridor",
            [
                {"from": "north", "to": "south"},
                {"from": "south", "to": "paris"},
                {"from": "paris", "to": "north"},
            ],
        )
        w.add("scenario", ["low", "high"])
        w.add("period", [2030, 2040, 2050])
        w.add("snapshot", SNAPSHOTS)
        w.add(
            "stage",
            [
                {"scenario": s, "period": p}
                for s, p in [("low", 2030), ("high", 2030), ("high", 2040)]
            ],
        )
        w.set("capacity", {"wind1": 100.0, "gas1": 50.0})
        w.set("p_nom", 1.0)
        w.set("p_nom", 2.0, entity="wind1")
        w.set("p_nom", 3.0, entity="wind1", scenario="high")
        w.set("price", 10.0)
        w.set("price", 20.0, bus="north", scenario="high")
        w.set("price", 30.0, bus="south", snapshot=FEB)
        w.set("efficiency", 0.9)
        w.set("efficiency", 0.5, entity="dc", bus="paris")
        w.set("flow", 1.0, **{"from": "north", "to": "south"})
        w.set("flow", 2.0, **{"from": "paris", "to": "north"})
        w.set("stage_cost", 4.0)
        w.set("stage_cost", 5.0, scenario="high", period=2040)
        w.set("weight", {"low": 0.25, "high": 0.75})
        return module_store.commit(w).record


def _labels(rel: DuckDBPyRelation) -> list[Any]:
    return [label for (label,) in rel.fetchall()]


def _rows(rel: DuckDBPyRelation) -> list[str]:
    """`rel`'s rows but `attribute` and `breakpoint`, sorted as text, their order being none."""
    return sorted(
        map(repr, rel.project(star(exclude=["attribute", "breakpoint"])).fetchall())
    )


def _relations(view: RecordLike) -> dict[tuple[str, str], Any]:
    """Every relation of `view`, fetched, by kind and name."""
    return {
        **{("dims", d): r.fetchall() for d, r in view.dims.items()},
        **{("properties", s): r.fetchall() for s, r in view.properties.items()},
        **{("attributes", a): _rows(r) for a, r in view.attributes.items()},
    }


def _at(record: RecordLike, labels: dict[str, Any]) -> dict[str, DuckDBPyRelation]:
    """Each dim `labels` squeezes, and each multi-dim over those alone, as a relation of its label."""
    schema = record.schema
    one = {
        d: label
        for d, label in labels.items()
        if d in schema.dimensions and not isinstance(label, list)
    }
    at = {d: record.dims[d].filter(col(d) == lit(label)) for d, label in one.items()}
    for m in schema.multi_dims:
        over = schema.over(m)
        if set(over.values()) <= one.keys():
            fields = (
                fn.struct_extract(col(m), lit(c)) == lit(one[d])
                for c, d in over.items()
            )
            at[m] = record.dims[m].filter(ex_all(fields))
    return at


VIEWS = [
    pytest.param({"period": 2030}, {"period": 2030}, id="one-label"),
    pytest.param({"period": [2040, 2030]}, {"period": [2030, 2040]}, id="list"),
    pytest.param({"period": slice(2040, None)}, {"period": [2040, 2050]}, id="slice"),
    pytest.param(
        {"snapshot": slice("2013-02", "2013-03")},
        {"snapshot": SNAPSHOTS[1:4]},
        id="date-slice",
    ),
    pytest.param({"snapshot": "2013-02"}, {"snapshot": SNAPSHOTS[1:3]}, id="month"),
    pytest.param({"snapshot": "2013-02-01"}, {"snapshot": FEB}, id="day"),
    pytest.param({"snapshot": ["2013-02-01"]}, {"snapshot": [FEB]}, id="day-listed"),
    pytest.param({"scenario": "high"}, {"scenario": "high"}, id="squeeze"),
    pytest.param(
        {"scenario": "high", "period": 2040},
        {"scenario": "high", "period": 2040},
        id="squeeze-a-multi-dim",
    ),
    pytest.param({"ctype": "Load"}, {"ctype": "Load"}, id="squeeze-a-category"),
    pytest.param({"entity": "wind1"}, {"entity": "wind1"}, id="squeeze-an-entity"),
    pytest.param(
        {"carrier": ["AC", "wind"]}, {"carrier": ["AC", "wind"]}, id="carriers"
    ),
    pytest.param({"country": ["DE"]}, {"country": ["DE"]}, id="chain"),
    pytest.param({"from": "north"}, {"from": ["north"]}, id="field"),
    pytest.param({"sign": -1}, {"sign": [-1.0]}, id="property"),
]


@pytest.mark.parametrize(("asked", "labels"), VIEWS)
def test_one_label_squeezes_and_any_other_value_keeps_the_dim(record, asked, labels):
    view = record.select(**asked)
    assert view.labels == labels, "the labels asked for, normalized"
    for name, value in labels.items():
        if name in record.schema.dimensions:
            assert (name in view.schema.dimensions) == isinstance(value, list), (
                "a list keeps its dim, one label squeezes it"
            )


@pytest.mark.parametrize(("asked", "labels"), VIEWS)
def test_reselecting_its_labels_gives_the_view(record, asked, labels):
    view = record.select(**asked)
    assert view.base is record
    assert _relations(view.base.select(**view.labels)) == _relations(view)


@pytest.mark.parametrize(("asked", "labels"), VIEWS)
def test_lifted_rows_are_the_base_rows_at_the_labels(record, asked, labels):
    view = record.select(**asked)
    at = _at(record, labels)
    for name, rel in view.attributes.items():
        columns = record.schema.long_columns_for(name)
        over = {d: r for d, r in at.items() if d in columns}
        base = record.attributes[name]
        if over:
            base = settle(base, over, Flags(frozenset(over), frozenset(over)))
        lifted = view.lift(name, rel).project(*columns)
        assert set(_rows(lifted)) <= set(_rows(base.project(*columns))), name


def test_select_is_record_select_and_a_record(record):
    view = select(record, scenario="high")
    assert isinstance(view, Selection)
    assert isinstance(view, RecordLike)
    assert view == record.select(scenario="high")
    with pytest.raises(TypeError, match="WorkingRecord"):
        WorkingRecord(view)


def test_selecting_a_selection_merges_into_its_labels(record):
    view = record.select(period=2030).select(carrier=["AC"])
    assert view.base is record, "a selection is of the record, never of a selection"
    assert view.labels == {"period": 2030, "carrier": ["AC"]}
    assert select(view, period=[2040]).labels == {"period": [2040], "carrier": ["AC"]}


ALL_BUSES = ["north", "south", "paris"]


@pytest.mark.parametrize(
    ("asked", "kept"),
    [
        pytest.param(
            {"carrier": ["AC", "wind"]},
            {
                "bus": ["north", "south"],
                "entity": ["wind1", "load1"],
                "connection": [{"entity": "wind1", "bus": "north"}],
                "corridor": [{"from": "north", "to": "south"}],
            },
            id="a-default-names-a-dropped-label-a-null-none",
        ),
        pytest.param(
            {"country": ["DE"]},
            {
                "state": ["BY", "NW"],
                "bus": ["north", "south"],
                "carrier": ["AC", "DC", "wind", "gas"],
            },
            id="through-a-chain-and-never-back",
        ),
        pytest.param(
            {"bus": ["south", "north"]},
            {
                "bus": ["north", "south"],
                "entity": ["wind1", "gas1", "load1", "dc"],
                "connection": [
                    {"entity": "wind1", "bus": "north"},
                    {"entity": "gas1", "bus": "south"},
                    {"entity": "dc", "bus": "north"},
                ],
            },
            id="tuples-go-in-axis-order",
        ),
        pytest.param(
            {"entity": ["wind1"]},
            {"connection": [{"entity": "wind1", "bus": "north"}], "bus": ALL_BUSES},
            id="labels-a-tuple-names-stay",
        ),
        pytest.param(
            {"ctype": "Load"},
            {"entity": ["load1"], "connection": [{"entity": "load1", "bus": "paris"}]},
            id="a-squeezed-category",
        ),
        pytest.param(
            {"from": "north"},
            {"corridor": [{"from": "north", "to": "south"}], "bus": ALL_BUSES},
            id="a-field-narrows-tuples",
        ),
        pytest.param(
            {"scenario": "high"},
            {"stage": [{"period": 2030}, {"period": 2040}]},
            id="a-multi-dim-loses-a-field",
        ),
    ],
)
def test_a_dropped_label_takes_what_refers_to_it(record, asked, kept):
    view = record.select(**asked)
    assert {d: _labels(view.dims[d]) for d in kept} == kept


@pytest.mark.parametrize(
    ("asked", "attribute", "rows"),
    [
        pytest.param(
            {"scenario": "high"},
            "p_nom",
            [(None, 1.0), ("wind1", 3.0)],
            id="the-named-row-wins",
        ),
        pytest.param(
            {"scenario": "low"},
            "p_nom",
            [(None, 1.0), ("wind1", 2.0)],
            id="a-null-row-reaches",
        ),
        pytest.param(
            {"scenario": "high"},
            "price",
            [(None, None, 10.0), ("north", None, 20.0), ("south", FEB, 30.0)],
            id="other-dims-as-written",
        ),
        pytest.param({"scenario": "high"}, "weight", [(0.75,)], id="no-dim-left"),
        pytest.param({"entity": "wind1"}, "capacity", [(100.0,)], id="an-entity"),
        pytest.param(
            {"scenario": "high"},
            "stage_cost",
            [(None, 4.0), ({"period": 2040}, 5.0)],
            id="a-field-leaves-its-struct",
        ),
        pytest.param(
            {"scenario": "high", "period": 2040},
            "stage_cost",
            [(5.0,)],
            id="a-multi-dim-squeezed-with-its-fields",
        ),
        pytest.param(
            {"period": 2030},
            "stage_cost",
            [(None, 4.0)],
            id="a-row-naming-a-dropped-tuple-goes",
        ),
    ],
)
def test_squeezing_settles_the_rows_along_the_dim(record, asked, attribute, rows):
    view = record.select(**asked)
    assert _rows(view.attributes[attribute]) == sorted(map(repr, rows))


def test_squeezing_takes_the_dim_out_of_the_schema_and_flags(record):
    view = record.select(scenario="high")
    assert "scenario" not in view.schema.dimensions
    assert view.schema.over("stage") == {"period": "period"}
    assert view.schema.attributes["p_nom"].dims == {"entity"}
    assert isinstance(view.schema.attributes["p_nom"].dims, frozenset)
    assert view.schema.attributes["weight"].dims == frozenset()
    assert "scenario" not in (view.schema.partial or ())
    assert all(
        "scenario" not in f.varies | f.broadcast for f in view.flags().values()
    ), "flags lose the squeezed dim"


SOLAR = {"entity.tech": {"solar": 0.5}}


@pytest.mark.parametrize(
    ("asked", "name", "default", "default_by"),
    [
        pytest.param({"ctype": "Load"}, "p_nom", -1.0, {}, id="the-category"),
        pytest.param({"ctype": "Generator"}, "p_nom", 0.0, {}, id="another-category"),
        pytest.param({"entity": "load1"}, "p_nom", -1.0, {}, id="the-subject"),
        pytest.param({"entity": "wind1"}, "p_nom", 0.0, {}, id="another-subject"),
        pytest.param({"ctype": "Load"}, "entity.sign", -1.0, {}, id="a-property"),
        pytest.param({"ctype": "Load"}, "p_min", -1.0, {}, id="later-keys-dropped"),
        pytest.param({"ctype": "Load"}, "p_max", 0.0, SOLAR, id="earlier-keys-kept"),
        pytest.param(
            {"ctype": "Generator"}, "p_min", 0.0, SOLAR, id="no-entry-passes-on"
        ),
    ],
)
def test_a_default_by_a_squeezed_category_applies_its_entry(
    record, asked, name, default, default_by
):
    view = record.select(**asked)
    resolved = view.schema.resolve(name)
    spec = resolved[1] if isinstance(resolved, tuple) else resolved
    assert (spec.default, spec.default_by) == (default, default_by)
    assert "ctype" not in view.schema.properties.get("entity", {})


@pytest.mark.parametrize(
    ("asked", "attribute", "lifted"),
    [
        pytest.param(
            {"scenario": "high"},
            "price",
            [
                (None, "high", None, 10.0),
                ("north", "high", None, 20.0),
                ("south", "high", FEB, 30.0),
            ],
            id="a-squeezed-dim-comes-back",
        ),
        pytest.param(
            {"scenario": "high"},
            "capacity",
            [("gas1", 50.0), ("wind1", 100.0)],
            id="unchanged",
        ),
        pytest.param(
            {"scenario": "high"},
            "stage_cost",
            [(None, 4.0), ({"scenario": "high", "period": 2040}, 5.0)],
            id="a-field-comes-back-into-its-struct",
        ),
        pytest.param(
            {"scenario": "high", "period": 2040},
            "stage_cost",
            [({"scenario": "high", "period": 2040}, 5.0)],
            id="a-squeezed-multi-dim-comes-back",
        ),
    ],
)
def test_lift_puts_the_squeezed_labels_back(record, asked, attribute, lifted):
    view = record.select(**asked)
    rel = view.lift(attribute, view.attributes[attribute])
    assert rel.columns == list(record.schema.long_columns_for(attribute))
    assert _rows(rel) == sorted(map(repr, lifted))


def test_a_selection_plans_on_the_base_and_reads_its_later_edits(record):
    with WorkingRecord(record) as w:
        view = w.select(scenario="high")
        rel = view.attributes["p_nom"]
        w.set("p_nom", 7.0, entity="gas1", scenario="high")
        assert "('gas1', 7.0)" in _rows(rel), "an attribute stays a plan"


@pytest.mark.parametrize(
    ("asked", "reason"),
    [
        pytest.param({"pipeline": 1}, "names no dim", id="undeclared"),
        pytest.param({"bus": "Atlantis"}, "the axis lacks", id="unknown-label"),
        pytest.param({"bus": ["north", "Atlantis"]}, "the axis lacks", id="unknown"),
        pytest.param({"snapshot": "2014"}, "names no label", id="empty-span"),
        pytest.param({"carrier": []}, "empty list", id="empty-list"),
        pytest.param({"period": slice(2030, None, 10)}, "with a step", id="step"),
        pytest.param({"bus": slice("A", "Z")}, "neither numbers", id="text"),
        pytest.param({"period": slice("2030", None)}, "cannot compare", id="end"),
        pytest.param({"snapshot": slice("2013-13", None)}, "no date", id="no-date"),
        pytest.param({"period": slice(2060, None)}, "covers no label", id="none"),
        pytest.param(
            {"connection": {"entity": "dc", "bus": "north"}},
            "multi-dim",
            id="a-multi-dim",
        ),
        pytest.param({"note": "x"}, "property of each", id="several-subjects"),
        pytest.param(
            {"entity": "gas1", "carrier": ["AC"]},
            "drops through another keyword",
            id="a-squeezed-label-dropped",
        ),
    ],
)
def test_select_refuses_naming_the_keyword(record, asked, reason):
    keyword = next(iter(asked))
    with pytest.raises(ValueError, match=reason) as refused:
        record.select(**asked)
    assert f"select({keyword}=" in str(refused.value)


def test_a_squeeze_never_defaults_a_label_null_on_its_category(record):
    with WorkingRecord(record) as w:
        w.add("entity", ["spare"])
        with pytest.raises(ValueError, match="NULL on `entity.ctype`") as refused:
            w.select(ctype="Load")
        assert "select(ctype='Load')" in str(refused.value)
        for kept in ({"ctype": ["Load"]}, {"ctype": "Generator"}):
            assert "spare" in _labels(w.select(**kept).dims["entity"]), (
                f"{kept} keeps a label NULL on its category, defaulting it as the base"
            )


def test_flags_refuse_a_category_the_view_squeezed(record):
    with pytest.raises(ValueError, match="categorize no dim"):
        record.select(ctype="Load").flags(ctype="Load")


@pytest.fixture(scope="module")
def routes(module_store):
    """Corridors and pipes between nodes, `from` a field of both, `via` a field and a property."""
    schema = Schema(
        dimensions={
            "node": Dimension(),
            "corridor": Dimension(over={"from": "node", "to": "node"}),
            "pipe": Dimension(over={"from": "node", "via": "node"}),
        },
        properties={"node": {"via": PropertySpec(dtype="str")}},
        attributes={"length": AttributeSpec(dtype="float", dims={"corridor"})},
        partial=frozenset({"node", "corridor", "pipe"}),
    )
    root = empty_root(module_store, schema).commit()
    with WorkingRecord(root.record) as w:
        w.add("node", ["a", "b", "c"], via=["b", "c", "a"])
        w.add("corridor", [{"from": "a", "to": "b"}])
        w.add("pipe", [{"from": "a", "via": "b"}, {"from": "b", "via": "c"}])
        return module_store.commit(w).record


def test_a_field_keyword_wins_over_a_property_of_its_name(routes):
    view = routes.select(via=["b"])
    assert _labels(view.dims["pipe"]) == [{"from": "a", "via": "b"}]
    assert _labels(view.dims["node"]) == ["a", "b", "c"], "no node selected by `via`"


@pytest.mark.parametrize(
    ("asked", "reason"),
    [
        pytest.param({"from": "a"}, "field of each", id="several-multi-dims"),
        pytest.param({"node": "a"}, "squeezes every dim", id="every-dim"),
    ],
)
def test_select_refuses_a_view_the_schema_cannot_hold(routes, asked, reason):
    keyword = next(iter(asked))
    with pytest.raises(ValueError, match=reason) as refused:
        routes.select(**asked)
    assert f"select({keyword}=" in str(refused.value)
