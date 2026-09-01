# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Functional groups: a group declaring `into`, which classifies its coordinates.

`country` over `[bus]` into `country` says every bus is in exactly one country.
The relation is a file of its own, `groups/country.parquet`, and the dim it is
`into` keeps an axis file for its order and for attributes addressed by it.

Notes
-----
- [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
- [why `into` is the right field](https://energy-models.github.io/datarecord/design/schema/#why-into-is-the-right-field)
"""

from typing import Any

import narwhals as nw
import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Group, Schema
from tests.fixtures import write_axis, write_group, write_schema


def _schema(**overrides) -> Schema:
    """A record classified twice over: bus -> state -> country."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "bus": Dimension(dtype=nw.String()),
            "state": Dimension(dtype=nw.String()),
            "country": Dimension(dtype=nw.String()),
        },
        "groups": {
            "state": Group(over=["bus"], into="state"),
            "country": Group(over=["state"], into="country"),
        },
        "partial": frozenset(),
    }
    kwargs.update(overrides)
    # A group's key coordinates address a row rather than broadcasting, so the
    # schema requires them `partial` - which is what the classification becoming
    # a relation buys, and what a column on the axis hid. Added here rather than
    # in every caller's `partial=`, which is overriding what the *record* patches
    # by value, not restating what the groups oblige.
    keys = {g.over[c] for g in kwargs["groups"].values() for c in g.key}
    kwargs["partial"] = frozenset(kwargs["partial"]) | keys
    return Schema(**kwargs)


# -- the declaration --------------------------------------------------------


def test_the_dim_a_group_is_into_is_an_ordinary_dim():
    """One namespace: the classified axis is addressable as any other is."""
    s = _schema()
    assert s.dims == ("bus", "state", "country")
    # Typed like any dim, so a column carrying its labels casts correctly.
    assert s.column_type("country") == nw.String()


def test_into_becomes_a_coordinate_of_the_group():
    """`into` is sugar: it folds into `coordinates` and nothing branches on it.

    So `groups/country.parquet` is keyed `state | country` exactly as an
    `into`-less group is keyed by its `over` alone.
    """
    s = _schema()
    assert s.group_coordinates("country") == ("state", "country")
    assert s.group_coordinates("state") == ("bus", "state")


def test_the_key_is_the_coordinates_minus_into():
    """What the uniqueness constraint is on: each `over` tuple carries one label."""
    s = _schema()
    assert s.group_key("country") == ("state",)
    assert s.group_key("state") == ("bus",)


def test_a_group_may_share_a_dims_name_and_the_dim_wins():
    """Addressing resolves the dim namespace first, so the collision is shadowing.

    `dims: [country]` is the axis - which is what a genuinely per-country value
    wants - rather than the group expanded to the states it maps from.
    """
    s = _schema(
        attributes={"co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"country"}),
    )
    assert s.coordinates_of("co2_budget") == ("country",), "the dim, not the group"
    assert s.groups_of("co2_budget") == (), "a shadowed group addresses nothing"


def test_an_into_less_group_in_dims_expands_to_its_coordinates():
    """A group no dim shadows has no other spelling, so `dims` expands it."""
    s = _schema(
        groups={"connection": Group(over=["bus", "state"])},
        attributes={"capacity": AttributeSpec(dtype=nw.Float64(), dims={"connection"})},
    )
    assert s.coordinates_of("capacity") == ("bus", "state"), "expanded, not the name"
    assert s.groups_of("capacity") == ("connection",)


def test_a_corridor_draws_two_coordinates_from_one_dim():
    """`over`'s dict form is what a relation between two of one axis needs."""
    s = _schema(groups={"corridor": Group(over={"from": "bus", "to": "bus"})})
    assert s.group_coordinates("corridor") == ("from", "to")
    assert s.group_key("corridor") == ("from", "to"), "no `into`, so all of them"


def test_the_over_list_form_is_sugar_for_the_dict():
    """`[bus]` is `{bus: bus}`; the dict is what a corridor needs."""
    assert Group(over=["bus"]).over == {"bus": "bus"}
    assert Group(over={"from": "bus", "to": "bus"}).coordinates == ("from", "to")


def test_a_functional_group_keys_no_axis():
    """`into` is not `within`: it classifies, so it does not scope a label.

    `country` labels mean the same thing everywhere, so the axis key is the
    label alone - where a nested dim's would be `(parent, label)`.
    """
    s = _schema()
    assert s.axis_key("country") == ("country",)
    assert s.axis_key("bus") == ("bus",)


# -- what the declaration rejects -------------------------------------------


def test_a_group_over_an_undeclared_dim_is_refused():
    with pytest.raises(ValidationError, match="over undeclared dims"):
        _schema(groups={"country": Group(over=["nope"], into="country")})


def test_into_must_name_a_declared_dim():
    """Rejected rather than tolerated, the failure being otherwise silent.

    A dim shadows a group of its name, so an `into` naming a dim nobody declared
    would leave `dims: [country]` quietly expanding to the coordinates instead
    of naming the axis it meant.
    """
    with pytest.raises(ValidationError, match="`into` undeclared dim"):
        _schema(groups={"c": Group(over=["bus"], into="nope")})


def test_a_group_cannot_map_a_coordinate_to_itself():
    with pytest.raises(ValidationError, match="also one of its `over` coordinates"):
        _schema(groups={"c": Group(over=["bus"], into="bus")})


# -- through a real record --------------------------------------------------


def _budget_schema() -> Schema:
    """The mapping chain, with an attribute addressed by `country` alone."""
    return _schema(
        attributes={"co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"country"}),
    )


def test_a_classified_axis_folds_as_an_ordinary_axis(con, base_uri):
    """The fold learns nothing new: the `into` dim has an axis file like any dim.

    Its own file is what gives it order and a place for `co2_budget`, which no
    bus column could hold.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(layer_dir(revision.id), "bus", [{"bus": "north"}])
    write_axis(layer_dir(revision.id), "state", [{"state": "lower"}])
    write_axis(
        layer_dir(revision.id), "country", [{"country": "DE"}, {"country": "FR"}]
    )

    axes = revision.node_cache.dims.axes
    assert sorted(axes["country"].df()["country"]) == ["DE", "FR"]
    assert axes["bus"].df()["bus"].tolist() == ["north"], (
        "no classification column: the relation is the group's own file"
    )


def test_a_chain_is_a_join_over_two_group_files(con, base_uri):
    """bus -> state -> country is one file per hop, never denormalised.

    Two files asserting bus->country would let a layer restating the states
    leave every bus's country stale, with nothing to detect it. A file per group
    gives that property for free: a layer restating a group restates one file.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_group(layer_dir(revision.id), "state", [{"bus": "north", "state": "lower"}])
    write_group(
        layer_dir(revision.id), "country", [{"state": "lower", "country": "DE"}]
    )

    groups = revision.record.groups
    assert groups["state"].collect().to_native().to_pydict() == {
        "bus": ["north"],
        "state": ["lower"],
        "order_key": [{"depth": 0, "row": 1}],
    }
    assert dict(
        zip(
            groups["country"].collect().to_native()["state"].to_pylist(),
            groups["country"].collect().to_native()["country"].to_pylist(),
        )
    ) == {"lower": "DE"}


def test_order_key_depth_survives_a_restate_through_a_materialised_parent(
    con, base_uri
):
    """A key introduced at the root and restated in a materialised child keeps
    its introducing `(depth, row)`, and the grandchild's own rows number from
    the parent's deepest `depth` rather than its position in the ancestry.

    Pins `order_key` as a struct rather than the arithmetic-across-layers
    scalar it replaces (https://energy-models.github.io/datarecord/design/proposals/staging-as-a-layer.md#what-lands-first-and-separately):
    the parent's rows pass through the anti-join with their own key intact, so
    restating a group at depth 1 must not bump `north`'s depth away from 0.
    """
    schema = _budget_schema()
    write_schema(schema)

    root = Revision.create(con)
    write_group(layer_dir(root.id), "state", [{"bus": "north", "state": "lower"}])

    child = root.child()
    write_group(layer_dir(child.id), "state", [{"bus": "south", "state": "upper"}])
    child.materialise()

    grandchild = child.child()
    write_group(layer_dir(grandchild.id), "state", [{"bus": "east", "state": "lower"}])

    rows = (
        grandchild.record.groups["state"]
        .sort("order_key")
        .collect()
        .to_native()
        .to_pydict()
    )
    assert rows["bus"] == ["north", "south", "east"]
    assert rows["order_key"] == [
        {"depth": 0, "row": 1},
        {"depth": 1, "row": 1},
        {"depth": 2, "row": 1},
    ]


def test_an_attribute_addressed_by_the_into_dim_alone_is_a_column_of_its_axis(
    con, base_uri
):
    """`co2_budget` is a property of the country, so it rides on the axis file.

    Not `inputs/co2_budget.parquet`: one addressing coordinate is a column on
    that thing's own table, and for a mapping that table is its own axis file.
    """
    revision = Revision.create(con)
    schema = _budget_schema()
    write_schema(schema)
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    assert schema.attributes_on("country") == ("co2_budget",)
    axis = revision.node_cache.dims.axes["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 40.0, "FR": 55.0}
    assert "co2_budget" not in revision.record.attributes, (
        "an axis-file column is no long frame"
    )


def test_setting_an_axis_addressed_attribute_stages_an_axis_row(con, base_uri):
    """`set` states a value for a label the axis already has.

    The staged row carries the label and the column, so a read with pending
    edits answers the new value while every untouched label keeps the base's.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})

    frame = staged.dims["country"].collect().to_native()
    got = dict(zip(frame["country"].to_pylist(), frame["co2_budget"].to_pylist()))
    assert got == {"DE": 12.0, "FR": 55.0}, (
        "FR is untouched, so it keeps the base value"
    )


def test_setting_one_axis_attribute_keeps_its_siblings_value(con, base_uri):
    """Two attributes on one axis, one edited: the other must survive the fold.

    The staged row carries only the column its `set` named, and the fold is
    last-writer-wins per label over the whole row - so a source handing over
    just that column would blank the sibling. `_collapsed_axis` merging per
    column *before* the fold sees it is what makes the two calls commute, and
    this is the assertion that fails if it stops.
    """
    revision = Revision.create(con)
    write_schema(
        _schema(
            attributes={
                "co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"}),
                "population": AttributeSpec(dtype=nw.Float64(), dims={"country"}),
            },
            partial=frozenset({"country"}),
        )
    )
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0, "population": 83.0}],
    )

    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})

    frame = staged.dims["country"].collect().to_native()
    assert frame["co2_budget"].to_pylist() == [12.0], "the edited column takes the edit"
    assert frame["population"].to_pylist() == [83.0], (
        "an axis column no edit named keeps the base's value"
    )

    # And a second `set` on the sibling composes with the first rather than
    # displacing it, which is the same rule one step further.
    staged.set("population", {"DE": 84.0})
    frame = staged.dims["country"].collect().to_native()
    assert frame["co2_budget"].to_pylist() == [12.0], "the earlier edit survives"
    assert frame["population"].to_pylist() == [84.0]


def test_a_child_layer_holds_only_the_axis_labels_it_touched(con, base_uri):
    """With the axis `partial`, a patch layer's `dims/` is the edits alone.

    The fold resolves every untouched label from the parent, which is what
    `partial` buys - and what it costs is a wider owner map, so an axis is only
    declared so where a layer really does patch label by label.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    # Deliberately not materialised: a patch layer resolves over its parent's
    # raw layer, so no node cache is required for the fold to see both labels.
    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})
    patch = staged.staged_only().dims["country"].collect().to_native()
    assert patch["country"].to_pylist() == ["DE"], "only the touched label"

    child = staged.commit(NewChild(revision))
    axis = child.node_cache.dims.axes["country"].df()
    resolved = dict(zip(axis["country"], axis["co2_budget"]))
    assert resolved == {"DE": 12.0, "FR": 55.0}, "last writer wins per label"
    assert axis["country"].tolist() == ["DE", "FR"], (
        "axis order follows the layer that introduced each label"
    )


def test_a_child_layer_restates_an_axis_it_owns_whole(con, base_uri):
    """Outside `partial`, touching an axis means carrying every label of it.

    A layer holding the edited label alone would not leave the others stale, it
    would remove them: the fold keys by the axis key, so the axis here is what
    this layer says it is. That is the price of keeping `partial` small, and it
    is bounded by the axis rather than paid by every read.
    """
    revision = Revision.create(con)
    # `partial` left empty, unlike `_budget_schema`.
    write_schema(
        _schema(
            attributes={
                "co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})
            }
        )
    )
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )

    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})

    patch = staged.staged_only().dims["country"].collect().to_native()
    assert sorted(patch["country"].to_pylist()) == ["DE", "FR"], (
        "the whole axis, not just the edited label"
    )

    child = staged.commit(NewChild(revision))
    axis = child.node_cache.dims.axes["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "FR": 55.0}, (
        "FR survives because this layer carried it"
    )


def test_an_axis_resolves_over_an_unmaterialised_parent(con, base_uri):
    """A node cache is an optimisation, so a fold without one answers the same.

    `ancestry_to_read` keeps an unmaterialised ancestor in the ancestry, so the
    dirs it is folded over must name that layer's raw `dims/` and not only the
    `resolved/dims/` it has never written.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(
        layer_dir(revision.id), "country", [{"country": "DE"}, {"country": "FR"}]
    )

    child = revision.child()
    write_axis(layer_dir(child.id), "country", [{"country": "NO"}])

    labels = child.node_cache.dims.axes["country"].df()["country"].tolist()
    assert labels == ["DE", "FR", "NO"], (
        "the parent's labels survive, in the order it introduced them"
    )


def test_set_may_name_a_label_no_layer_has_written(con, base_uri):
    """`set` introduces the label, the fold keying per label rather than whole.

    So this layer's axis file gains `NO` beside the `DE` it patches, and the
    parent's `DE` row is what the fold resolves against.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(
        layer_dir(revision.id), "country", [{"country": "DE", "co2_budget": 40.0}]
    )

    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0, "NO": 3.0})

    child = staged.commit(NewChild(revision))
    axis = child.node_cache.dims.axes["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "NO": 3.0}


def test_a_classified_axis_keeps_its_own_order(con, base_uri):
    """Axis order is the file's row order, classified or not."""
    revision = Revision.create(con)
    write_schema(_schema())
    write_axis(
        layer_dir(revision.id),
        "country",
        [{"country": "NO"}, {"country": "DE"}, {"country": "FR"}],
    )

    assert revision.node_cache.dims.axes["country"].df()["country"].tolist() == [
        "NO",
        "DE",
        "FR",
    ]
