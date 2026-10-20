# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Classifications: a property `into` a dim, and a group `into` one over several coordinates.

`bus.state` `into` `state` says every bus is in at most one state: a column of
the bus axis whose values are labels of `state`, which keeps an axis file for
its order and for its properties. A group needs `into` only for a tuple of two
coordinates or more.

Notes
-----
- [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
- [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
- [why `into` is the right field](https://energy-models.github.io/datarecord/design/schema/#why-into-is-the-right-field)
"""

from typing import Any

import narwhals as nw
import pytest
from pydantic import ValidationError

from datarecord.layered.revision import Revision
from datarecord.mutable import WorkingRecord
from datarecord.schema import (
    AttributeSpec,
    Dimension,
    Group,
    PropertySpec,
    Schema,
    Trait,
)
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
            "bus": Dimension(dtype=nw.String()),
            "state": Dimension(dtype=nw.String()),
            "country": Dimension(dtype=nw.String()),
            "scenario": Dimension(dtype=nw.String()),
        },
        "groups": {"bus_state": Group(over=["bus", "state"])},
        "properties": {
            "bus": {"state": PropertySpec(into="state")},
            "state": {"country": PropertySpec(into="country")},
            "bus_state": {"country": PropertySpec(into="country")},
        },
        "partial": frozenset(),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- the declaration --------------------------------------------------------


def test_the_key_is_over_without_into():
    """What the uniqueness constraint is on: each `over` tuple carries one label.

    So `groups/bus_state.parquet` is keyed by a `bus_state` struct over `bus`
    and `scenario`, `state` a plain column beside it.
    """
    s = _schema(groups={"bus_state": Group(over=["bus", "scenario"], into="state")})
    assert s.group_key("bus_state") == ("bus", "scenario")


def test_a_property_into_a_dim_takes_its_dtype_and_does_not_categorize():
    """Its values are the dim's labels, typed as them, and data rather than categories."""
    s = _schema(
        dimensions={
            **_schema().dimensions,
            "state": Dimension(dtype=nw.Enum(["lower", "upper"])),
        },
        partial=frozenset({"bus"}),
    )
    assert s.properties["bus"]["state"].dtype == nw.Enum(["lower", "upper"])
    assert s.categorizing("bus") == (), "an `Enum` dim's labels are still data"
    assert Schema.model_validate_json(s.model_dump_json()) == s, "the dtype round-trips"


def test_an_into_less_group_in_dims_is_one_struct_column():
    """A group in `dims` is carried as itself, a struct over its coordinates."""
    s = _schema(
        groups={**_schema().groups, "connection": Group(over=["bus", "state"])},
        attributes={"capacity": AttributeSpec(dtype=nw.Float64(), dims={"connection"})},
    )
    assert s.coordinates_of("capacity") == ("connection",), "the group, not expanded"
    assert s.column_type("connection") == nw.Struct(
        {"bus": nw.String(), "state": nw.String()}
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
    assert s.group_key("corridor") == ("from", "to"), "no `into`, so all of them"


# -- what the declaration rejects -------------------------------------------


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
        pytest.param(
            {"c": Group(over=["bus", "state"], into="nope")},
            "`into` undeclared dim",
            id="into-undeclared-dim",
        ),
        pytest.param(
            {"c": Group(over=["bus", "state"], into="bus")},
            "also one of its `over` coordinates",
            id="into-one-of-over",
        ),
        pytest.param(
            {"c": Group(over=["bus"], into="state")},
            "declare the property `bus.state`",
            id="into-over-one-coordinate",
        ),
    ],
)
def test_a_malformed_group_is_refused(groups, match):
    """A group is a column named for it, over declared dims, `into` another one."""
    with pytest.raises(ValidationError, match=match):
        _schema(groups=groups, properties={})


@pytest.mark.parametrize(
    ("spec", "extra", "match"),
    [
        pytest.param({"into": "nope"}, {}, "`into` undeclared dim", id="undeclared"),
        pytest.param(
            {"into": "state", "dtype": "Int64"},
            {},
            "its dtype is the dim's",
            id="dtype",
        ),
        pytest.param({}, {}, "declares its `dtype`, or the dim", id="neither"),
        pytest.param(
            {"into": "state"},
            {"traits": {"t": Trait(on={"bus.state": {"lower"}})}},
            "whose labels are data, not categories",
            id="a-trait-on-it",
        ),
    ],
)
def test_a_malformed_property_into_a_dim_is_refused(spec, extra, match):
    """`into` names a declared dim, whose dtype the property takes, and categorizes nothing."""
    with pytest.raises(ValidationError, match=match):
        _schema(
            dimensions={
                **_schema().dimensions,
                "state": Dimension(dtype=nw.Enum(["lower", "upper"])),
            },
            properties={"bus": {"state": spec}},
            partial=frozenset({"bus"}),
            **extra,
        )


# -- through a real record --------------------------------------------------


def _budget_schema() -> Schema:
    """The mapping chain, with a property of `country` and an attribute over it alone."""
    return _schema(
        properties={"country": {"co2_budget": PropertySpec(dtype=nw.Float64())}},
        attributes={"target": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"country"}),
    )


def test_a_classified_axis_folds_as_an_ordinary_axis(store):
    """The fold learns nothing new: the `into` dim has an axis file like any dim.

    Its own file is what gives it order and a place for `co2_budget`, which no
    bus column could hold.
    """
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(store.layer_dir(revision.id), "bus", [{"bus": "north"}])
    write_axis(store.layer_dir(revision.id), "state", [{"state": "lower"}])
    write_axis(
        store.layer_dir(revision.id),
        "country",
        [{"country": "NO"}, {"country": "DE"}, {"country": "FR"}],
    )

    axes = revision.resolver.dims.axes
    assert axes["country"].df()["country"].tolist() == ["NO", "DE", "FR"], (
        "axis order is the file's row order, classified or not"
    )
    assert axes["bus"].df()["bus"].tolist() == ["north"]


def test_removing_a_label_of_a_dim_outside_partial_tombstones_its_tuples(store):
    """A group may draw on a dim `partial` does not list; removal still cascades.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    revision = empty_root(store)
    write_schema(_schema(), store)
    layer = store.layer_dir(revision.id)
    write_axis(layer, "bus", [{"bus": "north"}, {"bus": "south"}])
    write_axis(layer, "state", [{"state": "lower"}, {"state": "upper"}])
    write_group(
        layer,
        "bus_state",
        [
            {"bus_state": {"bus": "north", "state": "lower"}},
            {"bus_state": {"bus": "south", "state": "upper"}},
        ],
    )

    staged = WorkingRecord(revision.record)
    staged.remove("bus", ["north"])
    assert _tuples(staged) == [{"bus": "south", "state": "upper"}], (
        "the tuple naming the removed bus is tombstoned"
    )

    child = revision.child(staged.edits)
    assert _tuples(child.record) == [{"bus": "south", "state": "upper"}]


def _tuples(record) -> list[dict]:
    return record.groups["bus_state"].collect()["bus_state"].to_list()


def test_member_order_survives_a_restate_through_a_materialised_parent(store):
    """A group's member order is the resolved file's row order, preserved across a
    materialised parent: a key introduced at the root stays first, and a
    grandchild's addition sorts after the materialised seed's members.

    No `order_key` column - member order is the fold's output order, read back
    order-preserving (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
    """
    schema = _budget_schema()
    write_schema(schema, store)

    root = empty_root(store)
    write_group(
        store.layer_dir(root.id),
        "bus_state",
        [{"bus_state": {"bus": "north", "state": "lower"}}],
    )

    child = empty_child(root)
    write_group(
        store.layer_dir(child.id),
        "bus_state",
        [{"bus_state": {"bus": "south", "state": "upper"}}],
    )
    child.materialise()

    grandchild = empty_child(child)
    write_group(
        store.layer_dir(grandchild.id),
        "bus_state",
        [{"bus_state": {"bus": "east", "state": "lower"}}],
    )

    rows = grandchild.record.groups["bus_state"].collect().to_native().to_pydict()
    assert [r["bus"] for r in rows["bus_state"]] == ["north", "south", "east"], (
        "member order, seed first"
    )
    assert "order_key" not in rows


def test_a_property_of_the_into_dim_is_a_column_of_its_axis(store):
    """`co2_budget` is a property of the country, so it rides on the axis file.

    An attribute addressed by `country` alone, `target`, is long rows of
    `inputs/target.parquet` instead: the section decides, not the shape.
    """
    revision = empty_root(store)
    schema = _budget_schema()
    write_schema(schema, store)
    write_axis(
        store.layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    axis = revision.resolver.dims.axes["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 40.0, "FR": 55.0}
    assert "co2_budget" not in revision.record.attributes, (
        "an axis-file column is no long frame"
    )

    staged = WorkingRecord(revision.record)
    staged.set("target", {"DE": 30.0})
    child = revision.child(staged.edits)
    assert "target" not in child.resolver.dims.axes["country"].columns
    rows = child.record.attributes["target"].collect().to_native().to_pydict()
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
                    "co2_budget": PropertySpec(dtype=nw.Float64()),
                    "population": PropertySpec(dtype=nw.Float64()),
                }
            },
            partial=frozenset({"country"}),
        ),
        store,
    )
    write_axis(
        store.layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0, "population": 83.0}],
    )

    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})

    frame = staged.properties["country"].collect().to_native()
    assert frame["co2_budget"].to_pylist() == [12.0], "the edited column takes the edit"
    assert frame["population"].to_pylist() == [83.0], (
        "an axis column no edit named keeps the base's value"
    )

    # And a second `set` on the sibling composes with the first rather than
    # displacing it, which is the same rule one step further.
    staged.set("country.population", {"DE": 84.0})
    frame = staged.properties["country"].collect().to_native()
    assert frame["co2_budget"].to_pylist() == [12.0], "the earlier edit survives"
    assert frame["population"].to_pylist() == [84.0]


def test_a_child_layer_holds_only_the_axis_labels_it_touched(store):
    """With the axis `partial`, a patch layer's `dims/` is the edits alone.

    The fold resolves every untouched label from the parent, which is what
    `partial` buys - and what it costs is a wider owner map, so an axis is only
    declared so where a layer really does patch label by label.
    """
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(
        store.layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    # Deliberately not materialised: a patch layer resolves over its parent's
    # raw layer, so no node cache is required for the fold to see both labels.
    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})
    country = staged.resolver.sources[-1].axis("country")
    assert country is not None
    patch = country.df()
    assert patch["country"].tolist() == ["DE"], "only the touched label"
    frame = staged.properties["country"].collect().to_native()
    got = dict(zip(frame["country"].to_pylist(), frame["co2_budget"].to_pylist()))
    assert got == {"DE": 12.0, "FR": 55.0}, (
        "a read with pending edits answers the new value; FR keeps the base's"
    )

    child = revision.child(staged.edits)
    axis = child.resolver.dims.axes["country"].df()
    resolved = dict(zip(axis["country"], axis["co2_budget"]))
    assert resolved == {"DE": 12.0, "FR": 55.0}, "last writer wins per label"
    assert axis["country"].tolist() == ["DE", "FR"], (
        "axis order follows the layer that introduced each label"
    )


def test_a_child_layer_restates_an_axis_it_owns_whole(store):
    """Outside `partial`, touching an axis means carrying every label of it.

    A layer holding the edited label alone would not leave the others stale, it
    would remove them: the fold keys by the axis key, so the axis here is what
    this layer says it is. That is the price of keeping `partial` small, and it
    is bounded by the axis rather than paid by every read.
    """
    revision = empty_root(store)
    # `partial` left empty, unlike `_budget_schema`.
    write_schema(
        _schema(
            properties={"country": {"co2_budget": PropertySpec(dtype=nw.Float64())}}
        ),
        store,
    )
    write_axis(
        store.layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})

    country = staged.resolver.sources[-1].axis("country")
    assert country is not None
    patch = country.df()
    assert sorted(patch["country"].tolist()) == ["DE", "FR"], (
        "the whole axis, not just the edited label"
    )

    child = revision.child(staged.edits)
    axis = child.resolver.dims.axes["country"].df()
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
    write_axis(
        store.layer_dir(revision.id), "country", [{"country": "DE"}, {"country": "FR"}]
    )

    child = empty_child(revision)
    write_axis(store.layer_dir(child.id), "country", [{"country": "NO"}])

    labels = child.resolver.dims.axes["country"].df()["country"].tolist()
    assert labels == ["DE", "FR", "NO"], (
        "the parent's labels survive, in the order it introduced them"
    )


def test_a_property_is_set_only_on_a_label_some_layer_wrote(store):
    """A property is a column of a label's row, so `add` makes the label first."""
    revision = empty_root(store)
    write_schema(_budget_schema(), store)
    write_axis(
        store.layer_dir(revision.id), "country", [{"country": "DE", "co2_budget": 40.0}]
    )

    staged = WorkingRecord(revision.record)
    with pytest.raises(KeyError, match="`add` them first"):
        staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})

    staged.add("country", ["NO"])
    staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})
    child = revision.child(staged.edits)
    axis = child.resolver.dims.axes["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "NO": 3.0}


# -- a property `into` a dim names a live label --------------------------------


@pytest.fixture
def classified(store) -> Revision:
    """North in lower, south in upper, both in DE, and the pair (north, lower) in DE."""
    revision = empty_root(store)
    write_schema(_schema(), store)
    layer = store.layer_dir(revision.id)
    write_axis(layer, "country", [{"country": "DE"}, {"country": "NO"}])
    write_axis(
        layer,
        "state",
        [{"state": "lower", "country": "DE"}, {"state": "upper", "country": "DE"}],
    )
    write_axis(
        layer,
        "bus",
        [{"bus": "north", "state": "lower"}, {"bus": "south", "state": "upper"}],
    )
    write_group(
        layer,
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
def test_a_property_into_a_dim_refuses_a_label_it_lacks(classified, edit):
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
def test_a_property_into_a_dim_takes_null_or_a_label_added_first(
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
    assert child.record.dims["state"].collect()["state"].to_list() == ["upper"]


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
