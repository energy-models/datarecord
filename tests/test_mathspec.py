# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A record's schema is derived from a mathspec spec, and its tables are what the spec reads.

Notes
-----
- [the mathspec adapter](https://energy-models.github.io/datarecord/reference/tools/#the-mathspec-adapter)
"""

import pytest
from mathspec import to_spec

from datarecord.mutable import WorkingRecord
from datarecord.tools.mathspec import schema_of, tables
from tests.fixtures import empty_root

SPEC = """
dimensions:
  entity: {}
  component: { one_of: [Generator, Load] }
  bus: {}
  region: {}
  snapshot: { dtype: int }
relations:
  connection: { dims: [entity, bus], missing: absent }
  located: { dims: [bus, region], missing: absent }
maps:
  entity:
    component: { refers_to: component }
parameters:
  sign: { dims: [entity], missing: 1 }
  cost: { dims: [entity, snapshot], missing: 0 }
  capacity: { dims: [entity] }
  limit: { dims: [entity], missing: absent }
  efficiency: { dims: [bus, entity], in: [connection], missing: 1 }
  tariff: { dims: [entity, bus, snapshot], in: [connection], missing: 0 }
variables:
  p: { dims: [entity, snapshot], bounds: { lower: 0, upper: capacity } }
  flow: { dims: [entity, bus, snapshot], in: [connection] }
constraints:
  balance:
    dims: [bus, snapshot]
    expression: sum(efficiency * flow, over=entity) == 0
objective:
  sense: minimize
  expression: sum(cost * sign * p)
"""

DEFAULT_BY: dict[str, dict[str, dict[str, float]]] = {
    "sign": {"entity.component": {"Load": -1}},
    "capacity": {"entity.component": {"Load": 0}},
    "efficiency": {"entity.component": {"Load": 0.5}},
}


@pytest.fixture(scope="module")
def spec():
    """Every default the spec states is one plain value: `sign` is `1`, whatever the component."""
    return to_spec(SPEC)


@pytest.fixture(scope="module")
def schema(spec):
    return schema_of(spec, partial={"entity", "connection"}, default_by=DEFAULT_BY)


@pytest.fixture(scope="module")
def revision(module_store, schema):
    """Two generators and two loads on two buses, with three stored attribute rows.

    `g1` has one `cost` row for every snapshot, `l2` its own `sign`, and `l1`
    its own `efficiency` at `north`.
    """
    staged = WorkingRecord(empty_root(module_store, schema).commit().record)
    staged.add("bus", ["north", "south"])
    staged.add("snapshot", [0, 1])
    staged.add(
        "entity",
        ["g1", "g2", "l1", "l2"],
        component=["Generator", "Generator", "Load", "Load"],
    )
    staged.add(
        "connection",
        [
            {"entity": "g1", "bus": "north"},
            {"entity": "l1", "bus": "north"},
            {"entity": "l2", "bus": "south"},
        ],
    )
    staged.set("cost", 3.0, entity="g1")
    staged.set("sign", -2.0, entity="l2")
    staged.set("efficiency", 0.8, entity="l1", bus="north")
    return module_store.commit(staged)


@pytest.fixture(scope="module")
def handed(revision, spec):
    return tables(revision.record, spec)


def _rows(rel) -> set[tuple]:
    return set(rel.fetchall())


def test_a_relation_is_a_multi_dim(schema):
    assert schema.over("connection") == {"entity": "entity", "bus": "bus"}


def test_a_map_is_a_property_that_refers_to_its_dim(schema):
    assert schema.properties["entity"]["component"].one_of == "component"
    assert schema.categorizing("entity") == ("component",), (
        "a map into a dim with one_of categorizes its subject"
    )


@pytest.mark.parametrize(
    ("attribute", "dims"),
    [
        pytest.param("cost", {"entity", "snapshot"}, id="no-in"),
        pytest.param("efficiency", {"connection"}, id="in-a-relation"),
        pytest.param(
            "tariff", {"connection", "snapshot"}, id="in-a-relation-and-over-a-dim"
        ),
    ],
)
def test_a_parameter_is_over_its_relations_and_the_dims_they_leave_out(
    schema, attribute, dims
):
    assert schema.attributes[attribute].dims == dims


def test_a_variable_has_no_attribute(schema, spec):
    assert sorted(spec.program.variables) == ["flow", "p"]
    assert sorted(schema.attributes) == [
        "capacity",
        "cost",
        "efficiency",
        "limit",
        "sign",
        "tariff",
    ], "the schema declares the spec's parameters, and no result of a solve"


@pytest.mark.parametrize(
    ("attribute", "default"),
    [
        pytest.param("sign", 1, id="a-value"),
        pytest.param("capacity", None, id="refused"),
        pytest.param("limit", None, id="absent"),
    ],
)
def test_a_missing_value_is_the_default(schema, attribute, default):
    assert schema.attributes[attribute].default == default


def test_a_default_by_category_is_the_schema_s_and_not_the_spec_s(schema, spec):
    assert spec.program.parameters["sign"].missing == 1
    assert schema.default_for("sign", component="Load") == -1
    assert schema.default_for("sign", component="Generator") == 1


def test_a_value_outside_the_relation_is_refused(revision):
    staged = WorkingRecord(revision.record)
    with pytest.raises(KeyError, match="which the staged axis does not hold"):
        staged.set("efficiency", 0.5, entity="g1", bus="south")


def test_a_dim_has_its_labels_and_its_maps(handed):
    assert handed["entity"].columns == ["entity", "component"]
    assert _rows(handed["entity"]) == {
        ("g1", "Generator"),
        ("g2", "Generator"),
        ("l1", "Load"),
        ("l2", "Load"),
    }, "one row per label, with the map beside it"
    assert handed["bus"].columns == ["bus"]


def test_a_relation_has_a_column_per_column_it_declares(handed):
    assert handed["connection"].columns == ["bus", "entity"], (
        "the columns are in the program's order, sorted by name"
    )
    assert _rows(handed["connection"]) == {
        ("north", "g1"),
        ("north", "l1"),
        ("south", "l2"),
    }, "one row per listed pair, the struct spread flat"


def test_a_dim_the_spec_lists_the_labels_of_has_no_table(handed):
    assert "component" not in handed


def test_a_row_for_every_label_of_a_dim_arrives_as_a_row_per_label(handed):
    assert handed["cost"].columns == ["entity", "snapshot", "value"]
    assert _rows(handed["cost"]) == {("g1", 0, 3.0), ("g1", 1, 3.0)}, (
        "one stored row for g1 reaches both snapshots, and the spec's missing: 0 "
        "gives every other entity"
    )


def test_a_default_by_category_arrives_as_rows(revision, handed):
    assert revision.record.attributes["sign"].fetchall() == [
        ("l2", "sign", None, -2.0)
    ], "the record stores the one sign that is no default"
    assert _rows(handed["sign"]) == {("l1", -1.0), ("l2", -2.0)}, (
        "a load with no row takes the default of its category, a stored row is "
        "kept, and a generator has no row because the spec's missing: 1 gives it"
    )


def test_a_default_under_missing_refused_fills_only_its_category(handed):
    assert _rows(handed["capacity"]) == {("l1", 0.0), ("l2", 0.0)}, (
        "the generators have no default, so the model refuses their missing rows"
    )


def test_a_default_in_a_relation_fills_only_the_rows_the_relation_lists(handed):
    assert handed["efficiency"].columns == ["bus", "entity", "value"], (
        "the columns are the parameter's dims in the program's order"
    )
    assert _rows(handed["efficiency"]) == {
        ("north", "l1", 0.8),
        ("south", "l2", 0.5),
    }, "l2 is filled at its one bus, and l1 keeps its stored row"


@pytest.mark.parametrize(
    ("default_by", "message"),
    [
        pytest.param(
            {"sing": {"entity.component": {"Load": -1}}},
            "declares no parameter 'sing'",
            id="no-such-parameter",
        ),
        pytest.param(
            {"limit": {"entity.component": {"Load": 0}}},
            "`missing: absent`, where a missing row is no value",
            id="a-parameter-under-absent",
        ),
    ],
)
def test_a_default_the_spec_has_no_place_for_is_refused(spec, default_by, message):
    with pytest.raises(ValueError, match=message):
        schema_of(spec, partial={"entity"}, default_by=default_by)


def test_a_parameter_in_two_relations_over_one_dim_is_refused():
    """A record would hold a value for every pair of tuples, and the spec means the tuples that agree on `bus`."""
    spec = SPEC.replace(
        "efficiency: { dims: [bus, entity], in: [connection], missing: 1 }",
        "efficiency: { dims: [bus, entity], in: [connection], missing: 1 }\n"
        "  loss: { dims: [entity, bus, region], in: [connection, located] }",
    )
    with pytest.raises(ValueError, match="both over 'bus'"):
        schema_of(to_spec(spec))


def test_a_parameter_under_absent_is_never_filled(module_store, spec, schema):
    """A record's own schema may declare a default that `schema_of` refuses to derive."""
    limit = schema.attributes["limit"].model_copy(
        update={"default_by": {"entity.component": {"Load": 0.0}}}
    )
    declared = schema.model_copy(
        update={"attributes": {**schema.attributes, "limit": limit}}
    )
    staged = WorkingRecord(empty_root(module_store, declared).commit().record)
    staged.add("entity", ["l1"], component=["Load"])
    assert tables(staged, spec)["limit"].fetchall() == [], (
        "under missing: absent a missing row is no value, whatever the schema's default"
    )


def test_defaults_by_the_categories_of_two_dims_are_refused(module_store):
    spec = SPEC.replace(
        "  region: {}\n", "  region: {}\n  kind: { one_of: [ac, dc] }\n"
    ).replace("maps:\n", "maps:\n  bus:\n    kind: { refers_to: kind }\n")
    widened = to_spec(spec)
    by_two = {"entity.component": {"Load": 0.5}, "bus.kind": {"dc": 0.9}}
    declared = schema_of(
        widened,
        partial={"entity", "bus", "connection"},
        default_by={"efficiency": by_two},
    )
    record = empty_root(module_store, declared).commit().record
    with pytest.raises(ValueError, match=r"categories of \['bus', 'entity'\]"):
        tables(record, widened)
