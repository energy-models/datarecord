# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Functional relations: a relation declaring `values`, which classifies its key.

`country` keyed by `[bus]` with `values` `country` says every bus is in exactly
one country. The relation is a file of its own, `relations/country.parquet`, and
its `values` dim keeps an axis file for its order and for attributes addressed
by it.

Notes
-----
- [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
- [why `into` is the right field](https://energy-models.github.io/datarecord/design/schema/#why-into-is-the-right-field)
"""

from typing import Any

import narwhals as nw
import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Relation, Schema
from tests.fixtures import write_axis, write_relation, write_schema


def _schema(**overrides) -> Schema:
    """A record classified twice over: bus -> state -> country."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "bus": Dimension(dtype=nw.String()),
            "state": Dimension(dtype=nw.String()),
            "country": Dimension(dtype=nw.String()),
        },
        "relations": {
            "state": Relation(key=["bus"], values="state"),
            "country": Relation(key=["state"], values="country"),
        },
        "partial": frozenset({"bus", "state"}),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- the declaration --------------------------------------------------------


def test_the_values_dim_of_a_relation_is_an_ordinary_dim():
    """One namespace: the classified axis is addressable as any other is."""
    s = _schema()
    assert s.dims == ("bus", "state", "country")
    # Typed like any dim, so a column carrying its labels casts correctly.
    assert s.column_type("country") == nw.String()


def test_values_becomes_a_column_of_the_relation():
    """`values` is sugar: it folds into `columns` and nothing branches on it.

    So `relations/country.parquet` has columns `state | country` exactly as a
    `values`-less relation has its `key` alone.
    """
    s = _schema()
    assert s.relation_columns("country") == ("state", "country")
    assert s.relation_columns("state") == ("bus", "state")


def test_the_key_is_the_columns_minus_values():
    """What the uniqueness constraint is on: each `key` tuple carries one label."""
    s = _schema()
    assert s.relation_key("country") == ("state",)
    assert s.relation_key("state") == ("bus",)


def test_a_relation_may_share_a_dims_name_and_the_dim_wins():
    """A name in `dims` that is a declared dim is that dim, so the collision is shadowing.

    `dims: [country]` is the axis - which is what a genuinely per-country value
    wants - and not refused as an attribute over the relation `country`.
    """
    s = _schema(
        attributes={"co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"bus", "state", "country"}),
    )
    assert s.coordinates_of("co2_budget") == ("country",), "the dim, not the relation"


def test_a_corridor_draws_two_coordinates_from_one_dim():
    """`key`'s dict form is what a relation between two of one axis needs."""
    s = _schema(
        relations={"corridor": Relation(key={"from": "bus", "to": "bus"})},
        partial=frozenset({"bus"}),
    )
    assert s.relation_columns("corridor") == ("from", "to")
    assert s.relation_key("corridor") == ("from", "to"), "no `values`, so all of them"


def test_the_key_list_form_is_sugar_for_the_dict():
    """`[bus]` is `{bus: bus}`; the dict is what a corridor needs."""
    assert Relation(key=["bus"]).key == {"bus": "bus"}
    assert Relation(key={"from": "bus", "to": "bus"}).columns == ("from", "to")


def test_a_functional_relation_keys_no_axis():
    """`values` is not `within`: it classifies, so it does not scope a label.

    `country` labels mean the same thing everywhere, so the axis key is the
    label alone - where a nested dim's would be `(parent, label)`.
    """
    s = _schema()
    assert s.axis_key("country") == ("country",)
    assert s.axis_key("bus") == ("bus",)


# -- what the declaration rejects -------------------------------------------


def test_a_relation_keyed_by_an_undeclared_dim_is_refused():
    with pytest.raises(ValidationError, match="keyed by undeclared dims"):
        _schema(relations={"country": Relation(key=["nope"], values="country")})


def test_values_must_name_a_declared_dim():
    """Rejected rather than tolerated, the failure being otherwise silent.

    A dim shadows a relation of its name, so a `values` naming a dim nobody
    declared would leave `dims: [country]` quietly expanding to the coordinates instead
    of naming the axis it meant.
    """
    with pytest.raises(ValidationError, match="`values` in undeclared dim"):
        _schema(relations={"c": Relation(key=["bus"], values="nope")})


def test_a_relation_cannot_map_a_column_to_itself():
    with pytest.raises(ValidationError, match="also one of its `key` columns"):
        _schema(relations={"c": Relation(key=["bus"], values="bus")})


# -- through a real record --------------------------------------------------


def _budget_schema() -> Schema:
    """The mapping chain, with an attribute addressed by `country` alone."""
    return _schema(
        attributes={"co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"bus", "state", "country"}),
    )


def test_a_classified_axis_folds_as_an_ordinary_axis(con, base_uri):
    """The fold learns nothing new: the `values` dim has an axis file like any dim.

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

    axes = revision.resolver.dims.axes
    assert sorted(axes["country"].df()["country"]) == ["DE", "FR"]
    assert axes["bus"].df()["bus"].tolist() == ["north"], (
        "no classification column: the relation is a file of its own"
    )


def test_a_chain_is_a_join_over_two_relation_files(con, base_uri):
    """bus -> state -> country is one file per hop, never denormalised.

    Two files asserting bus->country would let a layer restating the states
    leave every bus's country stale, with nothing to detect it. A file per relation
    gives that property for free: a layer restating a relation restates one file.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_relation(
        layer_dir(revision.id), "state", [{"bus": "north", "state": "lower"}]
    )
    write_relation(
        layer_dir(revision.id), "country", [{"state": "lower", "country": "DE"}]
    )

    relations = revision.record.relations
    assert relations["state"].collect().to_native().to_pydict() == {
        "bus": ["north"],
        "state": ["lower"],
    }
    assert dict(
        zip(
            relations["country"].collect().to_native()["state"].to_pylist(),
            relations["country"].collect().to_native()["country"].to_pylist(),
        )
    ) == {"lower": "DE"}


def test_member_order_survives_a_restate_through_a_materialised_parent(con, base_uri):
    """A relation's member order is the resolved file's row order, preserved across a
    materialised parent: a key introduced at the root stays first, and a
    grandchild's addition sorts after the materialised seed's members.

    No `order_key` column - member order is the fold's output order, read back
    order-preserving (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
    """
    schema = _budget_schema()
    write_schema(schema)

    root = Revision.create(con)
    write_relation(layer_dir(root.id), "state", [{"bus": "north", "state": "lower"}])

    child = root.child()
    write_relation(layer_dir(child.id), "state", [{"bus": "south", "state": "upper"}])
    child.materialise()

    grandchild = child.child()
    write_relation(
        layer_dir(grandchild.id), "state", [{"bus": "east", "state": "lower"}]
    )

    rows = grandchild.record.relations["state"].collect().to_native().to_pydict()
    assert rows["bus"] == ["north", "south", "east"], "member order, seed first"
    assert "order_key" not in rows


def test_an_attribute_addressed_by_the_values_dim_alone_is_a_column_of_its_axis(
    con, base_uri
):
    """`co2_budget` is a property of the country, so it rides on the axis file.

    Not `attributes/co2_budget.parquet`: one addressing coordinate is a column on
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
    axis = revision.resolver.dims.axes["country"].df()
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
            partial=frozenset({"bus", "state", "country"}),
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
    country = staged.resolver.sources[-1].axis("country")
    assert country is not None
    patch = country.df()
    assert patch["country"].tolist() == ["DE"], "only the touched label"

    child = staged.commit(NewChild(revision))
    axis = child.resolver.dims.axes["country"].df()
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

    country = staged.resolver.sources[-1].axis("country")
    assert country is not None
    patch = country.df()
    assert sorted(patch["country"].tolist()) == ["DE", "FR"], (
        "the whole axis, not just the edited label"
    )

    child = staged.commit(NewChild(revision))
    axis = child.resolver.dims.axes["country"].df()
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

    labels = child.resolver.dims.axes["country"].df()["country"].tolist()
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
    axis = child.resolver.dims.axes["country"].df()
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

    assert revision.resolver.dims.axes["country"].df()["country"].tolist() == [
        "NO",
        "DE",
        "FR",
    ]
