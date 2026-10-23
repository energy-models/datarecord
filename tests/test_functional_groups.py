# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Classifications: a property that takes its labels from a dim.

`bus.state` `one_of: state` says every bus is in at most one state: a
column of the bus axis whose values are labels of `state`, which keeps an axis
file for its order and for its properties. A group's property takes its labels
from a dim the same way.

Notes
-----
- [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
- [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
"""

from typing import Any

import duckdb
import pytest
from duckdb import sqltypes
from pydantic import ValidationError

from datarecord.mutable import WorkingRecord
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, Group, PropertySpec
from datarecord.store.store import Revision
from tests.fixtures import (
    empty_child,
    empty_root,
    write_axis,
    write_group,
    write_schema,
)


def _schema(**overrides) -> Schema:
    """A record classified twice over, bus -> state -> country, and a pair classified into a country."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "bus": Dimension(dtype="str"),
            "state": Dimension(dtype="str"),
            "country": Dimension(dtype="str"),
            "scenario": Dimension(dtype="str"),
        },
        "groups": {"bus_state": Group(over=["bus", "state"])},
        "properties": {
            "bus": {"state": PropertySpec(one_of="state")},
            "state": {"country": PropertySpec(one_of="country")},
            "bus_state": {"country": PropertySpec(one_of="country")},
        },
        "partial": frozenset(),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- the declaration --------------------------------------------------------


@pytest.mark.parametrize(
    ("one_of", "duck", "categorizing"),
    [
        pytest.param(None, sqltypes.VARCHAR, (), id="open"),
        pytest.param(
            ("lower", "upper"),
            duckdb.sqltypes.DuckDBPyType("ENUM('lower', 'upper')"),
            ("state",),
            id="listed",
        ),
    ],
)
def test_a_property_takes_its_dims_type_and_categorizes_if_the_dim_lists_labels(
    one_of, duck, categorizing
):
    """Its values are the dim's labels, typed as them; categories only where the dim lists them."""
    s = _schema(
        dimensions={**_schema().dimensions, "state": Dimension(one_of=one_of)},
        partial=frozenset({"bus"}),
    )
    assert s.column_type("state", "bus") == duck
    assert s.categorizing("bus") == categorizing, "an open dim's labels are data"
    assert Schema.model_validate_json(s.model_dump_json()) == s, "the dtype round-trips"


def test_a_group_in_dims_is_one_struct_column():
    """A group in `dims` is carried as itself, a struct over its coordinates."""
    s = _schema(
        groups={**_schema().groups, "connection": Group(over=["bus", "state"])},
        attributes={"capacity": AttributeSpec(dtype="float", dims={"connection"})},
    )
    assert s.coordinates_of("capacity") == ("connection",), "the group, not expanded"
    assert s.column_type("connection") == duckdb.struct_type(
        {"bus": sqltypes.VARCHAR, "state": sqltypes.VARCHAR}
    )
    assert s.groups_of("capacity") == ("connection",)


def test_the_over_list_form_is_sugar_for_the_dict():
    """`[bus]` is `{bus: bus}`; the dict is what a corridor needs.

    A corridor draws two coordinates from one dim.
    """
    assert Group(over=["bus"]).over == {"bus": "bus"}
    assert Group(over={"from": "bus", "to": "bus"}).key == ("from", "to")
    s = _schema(
        groups={
            **_schema().groups,
            "corridor": Group(over={"from": "bus", "to": "bus"}),
        }
    )
    assert s.group_key("corridor") == ("from", "to"), "every coordinate"


# -- what the declaration rejects -------------------------------------------


@pytest.mark.parametrize(
    ("manifest", "match"),
    [
        pytest.param(
            {"properties": {"bus": {"state": {"one_of": ["lower", "upper"]}}}},
            "Input should be a valid string",
            id="a-list-on-a-property",
        ),
        pytest.param(
            {"dimensions": {"state": {"one_of": "country"}}},
            "Input should be a valid tuple",
            id="a-dim-name-on-a-dim",
        ),
    ],
)
def test_a_misplaced_one_of_fails_to_load(manifest, match):
    """`one_of` is a list on a dim and a dim name on a property.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
    """
    dimensions = {d: {"dtype": "str"} for d in ("bus", "state", "country", "scenario")}
    manifest = {**manifest, "dimensions": dimensions | manifest.get("dimensions", {})}
    with pytest.raises(ValidationError, match=match):
        Schema.model_validate(manifest)


@pytest.mark.parametrize(
    ("groups", "match"),
    [
        pytest.param(
            {"country": Group(over=["bus", "state"])},
            "group 'country' shares its name",
            id="shares-a-dims-name",
        ),
        pytest.param(
            {"bus_state": Group(over=["nope"])},
            "over undeclared dims",
            id="over-undeclared-dim",
        ),
    ],
)
def test_a_malformed_group_is_refused(groups, match):
    """A group is a column named for it, over declared dims."""
    with pytest.raises(ValidationError, match=match):
        _schema(groups=groups, properties={})


@pytest.mark.parametrize(
    ("spec", "extra", "match"),
    [
        pytest.param({"one_of": "nope"}, {}, "an undeclared dim", id="undeclared"),
        pytest.param(
            {"one_of": "state"},
            {
                "attributes": {
                    "v": AttributeSpec(
                        dtype="float",
                        dims={"bus"},
                        default_by={"bus.state": {"lower": 1.0}},
                    )
                }
            },
            "which lists no labels, so they are data, not categories",
            id="a-default-by-on-an-open-dim",
        ),
    ],
)
def test_a_malformed_property_taking_labels_from_a_dim_is_refused(spec, extra, match):
    """A property takes its labels from a declared dim, and from one that lists none categorizes nothing."""
    with pytest.raises(ValidationError, match=match):
        _schema(
            properties={"bus": {"state": spec}},
            partial=frozenset({"bus"}),
            **extra,
        )


# -- through a real record --------------------------------------------------


def _budget_schema() -> Schema:
    """The mapping chain, with a property of `country` and an attribute over it alone."""
    return _schema(
        properties={"country": {"co2_budget": PropertySpec(dtype="float")}},
        attributes={"target": AttributeSpec(dtype="float", dims={"country"})},
        partial=frozenset({"country"}),
    )


def test_a_classified_axis_folds_as_an_ordinary_axis(store):
    """The fold learns nothing new: a dim a property takes its labels from has an axis file like any dim.

    Its own file is what gives it order and a place for `co2_budget`, which no
    bus column could hold.
    """
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(revision, "bus", [{"bus": "north"}])
    write_axis(revision, "state", [{"state": "lower"}])
    write_axis(
        revision,
        "country",
        [{"country": "NO"}, {"country": "DE"}, {"country": "FR"}],
    )

    axes = revision.record.dims
    assert axes["country"].df()["country"].tolist() == ["NO", "DE", "FR"], (
        "axis order is the file's row order, classified or not"
    )
    assert axes["bus"].df()["bus"].tolist() == ["north"]


def test_removing_a_label_of_a_dim_outside_partial_drops_its_tuples(store):
    """A group may draw on a dim `partial` does not list; removal still cascades.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    revision = empty_root(store)
    write_schema(_schema(), store)
    write_axis(revision, "bus", [{"bus": "north"}, {"bus": "south"}])
    write_axis(revision, "state", [{"state": "lower"}, {"state": "upper"}])
    write_group(
        revision,
        "bus_state",
        [
            {"bus_state": {"bus": "north", "state": "lower"}},
            {"bus_state": {"bus": "south", "state": "upper"}},
        ],
    )

    staged = WorkingRecord(revision.record)
    staged.remove("bus", ["north"])
    assert _tuples(staged) == [{"bus": "south", "state": "upper"}], (
        "the tuple naming the removed bus is dropped"
    )

    child = revision.child(staged.edits)
    assert _tuples(child.record) == [{"bus": "south", "state": "upper"}]


def _tuples(record) -> list[dict]:
    return record.groups["bus_state"].df()["bus_state"].tolist()


def test_member_order_survives_a_restate_through_a_materialised_parent(store):
    """A group's member order is the deepest restatement's row order, across a
    materialised parent: a key added at the root stays first, and a
    grandchild's addition comes after the materialised copy's members.

    No `order_key` column - member order is the file's own row order
    (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
    """
    schema = _budget_schema()
    write_schema(schema, store)

    root = empty_root(store)
    write_group(
        root,
        "bus_state",
        [{"bus_state": {"bus": "north", "state": "lower"}}],
    )

    child = empty_child(root)
    write_group(
        child,
        "bus_state",
        [{"bus_state": {"bus": "south", "state": "upper"}}],
    )
    child.materialise()

    grandchild = empty_child(child)
    write_group(
        grandchild,
        "bus_state",
        [{"bus_state": {"bus": "east", "state": "lower"}}],
    )

    rows = grandchild.record.groups["bus_state"].to_arrow_table().to_pydict()
    assert [r["bus"] for r in rows["bus_state"]] == ["north", "south", "east"], (
        "member order, seed first"
    )
    assert "order_key" not in rows


def test_a_property_of_a_named_dim_is_a_column_of_its_axis(store):
    """`co2_budget` is a property of the country, so it rides on the axis file.

    An attribute addressed by `country` alone, `target`, is long rows of
    `attributes/target.parquet` instead: the section decides, not the shape.
    """
    revision = empty_root(store)
    schema = _budget_schema()
    write_schema(schema, store)
    write_axis(
        revision,
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    axis = revision.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 40.0, "FR": 55.0}
    assert "co2_budget" not in revision.record.attributes, (
        "an axis-file column is no long frame"
    )

    staged = WorkingRecord(revision.record)
    staged.set("target", {"DE": 30.0})
    child = revision.child(staged.edits)
    assert "target" not in child.record._coords.axes["country"].columns
    rows = child.record.attributes["target"].to_arrow_table().to_pydict()
    assert rows["country"] == ["DE"]
    assert rows["value"] == [30.0]


def test_setting_one_property_keeps_its_siblings_value(store):
    """Two properties on one axis, one edited: the other must survive the fold.

    The staged row carries only the column its `set` named, and the fold is
    last-writer-wins per label over the whole row - so a source handing over
    just that column would blank the sibling. `_collapsed_axis` merging per
    column *before* the fold sees it is what makes the two calls commute, and
    this is the assertion that fails if it stops.
    """
    revision = empty_root(store)
    write_schema(
        _schema(
            properties={
                "country": {
                    "co2_budget": PropertySpec(dtype="float"),
                    "population": PropertySpec(dtype="float"),
                }
            },
            partial=frozenset({"country"}),
        ),
        store,
    )
    write_axis(
        revision,
        "country",
        [{"country": "DE", "co2_budget": 40.0, "population": 83.0}],
    )

    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})

    frame = staged.properties["country"].to_arrow_table()
    assert frame["co2_budget"].to_pylist() == [12.0], "the edited column takes the edit"
    assert frame["population"].to_pylist() == [83.0], (
        "an axis column no edit named keeps the base's value"
    )

    # And a second `set` on the sibling composes with the first rather than
    # displacing it, which is the same rule one step further.
    staged.set("country.population", {"DE": 84.0})
    frame = staged.properties["country"].to_arrow_table()
    assert frame["co2_budget"].to_pylist() == [12.0], "the earlier edit survives"
    assert frame["population"].to_pylist() == [84.0]


@pytest.mark.parametrize("partial", [frozenset({"country"}), frozenset()])
def test_a_child_layer_restates_an_axis_whole(store, partial):
    """Touching an axis means carrying every label of it, in order, whatever `partial` says.

    A layer holding the edited label alone would not leave the others stale, it
    would remove them: the axis here is what this layer says it is.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    revision = empty_root(store)
    properties = {"country": {"co2_budget": PropertySpec(dtype="float")}}
    write_schema(_schema(properties=properties, partial=partial), store)
    write_axis(
        revision,
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})

    country = staged.edits.axis("country")
    assert country is not None
    patch = country.df()
    assert patch["country"].tolist() == ["DE", "FR"], (
        "the whole axis in order, not just the edited label"
    )

    child = revision.child(staged.edits)
    axis = child.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "FR": 55.0}, (
        "FR survives because this layer carried it"
    )


def test_an_axis_resolves_over_an_unmaterialised_parent(store):
    """A node cache is an optimisation, so a fold without one answers the same.

    `ancestry_to_read` keeps an unmaterialised ancestor in the ancestry, so the
    dirs it is folded over must name that layer's raw `dims/` and not only the
    `resolved/dims/` it has never written.
    """
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(revision, "country", [{"country": "DE"}, {"country": "FR"}])

    child = empty_child(revision)
    write_axis(child, "country", [{"country": "NO"}])

    labels = child.record.dims["country"].df()["country"].tolist()
    assert labels == ["DE", "FR", "NO"], (
        "the child restates the parent's labels in their order, then its own"
    )


def test_a_property_is_set_only_on_a_label_some_layer_wrote(store):
    """A property is a column of a label's row, so `add` makes the label first."""
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(revision, "country", [{"country": "DE", "co2_budget": 40.0}])

    staged = WorkingRecord(revision.record)
    with pytest.raises(KeyError, match="`add` them first"):
        staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})

    staged.add("country", ["NO"])
    staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})
    child = revision.child(staged.edits)
    axis = child.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "NO": 3.0}


# -- a property that takes its labels from a dim holds a live label --------


@pytest.fixture
def classified(store) -> Revision:
    """North in lower, south in upper, both in DE, and the pair (north, lower) in DE."""
    revision = empty_root(store)
    write_schema(_schema(), store)
    write_axis(revision, "country", [{"country": "DE"}, {"country": "NO"}])
    write_axis(
        revision,
        "state",
        [{"state": "lower", "country": "DE"}, {"state": "upper", "country": "DE"}],
    )
    write_axis(
        revision,
        "bus",
        [{"bus": "north", "state": "lower"}, {"bus": "south", "state": "upper"}],
    )
    write_group(
        revision,
        "bus_state",
        [{"bus_state": {"bus": "north", "state": "lower"}, "country": "DE"}],
    )
    return revision


EDITS = [
    pytest.param(lambda w, v: w.set("bus.state", v, bus="north"), id="set-scalar"),
    pytest.param(lambda w, v: w.set("bus.state", {"north": v}), id="set-mapping"),
    pytest.param(lambda w, v: w.add("bus", ["east"], state=v), id="add"),
    pytest.param(lambda w, v: w.set("bus_state.country", v), id="set-on-a-group"),
]


@pytest.mark.parametrize("edit", EDITS)
def test_a_property_naming_a_dim_refuses_a_label_it_lacks(classified, edit):
    """`set` and `add` refuse a value naming no label of the dim, as staged.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    """
    staged = WorkingRecord(classified.record)
    with pytest.raises(KeyError, match=r"no '(state|country)' label \['coal'\]"):
        edit(staged, "coal")


@pytest.mark.parametrize("edit", EDITS)
@pytest.mark.parametrize("value", [None, "added"])
def test_a_property_naming_a_dim_takes_null_or_a_label_added_first(
    classified, edit, value
):
    """NULL names no label; a label staged by `add` is live."""
    staged = WorkingRecord(classified.record)
    staged.add("state", ["added"])
    staged.add("country", ["added"])
    edit(staged, value)
    classified.child(staged.edits)


def test_remove_is_refused_while_a_property_names_the_label(classified):
    """A property's row outlives the label it names, so clearing it comes first.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    staged = WorkingRecord(classified.record)
    with pytest.raises(
        ValueError, match=r"`bus.state` names them at \[\('north', 'lower'\)\]"
    ):
        staged.remove("state", ["lower"])

    staged.set("bus.state", None, bus="north")
    staged.remove("state", ["lower"])
    with pytest.raises(KeyError, match=r"no 'state' label \['lower'\]"):
        staged.set("bus.state", "lower", bus="south")
    child = classified.child(staged.edits)
    assert child.record.dims["state"].df()["state"].tolist() == ["upper"]


def test_the_writer_refuses_a_property_naming_a_removed_label(classified):
    """`add_group` checks no label, so the writer is what catches it.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    staged = WorkingRecord(classified.record)
    staged.remove("country", ["NO"])
    removed = classified.child(staged.edits)

    revived = WorkingRecord(removed.record)
    revived.add_group(
        "bus_state",
        revived.con.sql(
            "SELECT {'bus': 'south', 'state': 'upper'}"
            "::STRUCT(bus VARCHAR, state VARCHAR) AS bus_state, 'NO' AS country"
        ),
    )
    with pytest.raises(ValueError, match=r"a removed country in 'country'"):
        removed.child(revived.edits)
