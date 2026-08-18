# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Mappings: a dim that classifies another dim's labels.

`country on bus` says every bus is in exactly one country. The classification
is a column on the *classified* axis's file, and the mapping keeps an axis file
of its own for its order and for attributes addressed by it.

Notes
-----
- [mappings](https://energy-models.github.io/datarecord/design/proposals/dims-groups-traits/#mappings)
"""

import narwhals as nw
import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema
from tests.fixtures import write_axis, write_schema


def _schema(**overrides) -> Schema:
    """A record classified twice over: bus -> state -> country."""
    kwargs = {
        "dimensions": {
            "bus": Dimension(dtype=nw.String()),
            "state": Dimension(dtype=nw.String(), on={"bus"}),
            "country": Dimension(dtype=nw.String(), on={"state"}),
        },
        "partial": frozenset(),
    }
    kwargs.update(overrides)
    return Schema(**kwargs)


# -- the declaration --------------------------------------------------------


def test_a_mapping_is_a_dim():
    """One namespace: a mapping is addressable exactly as any other axis is."""
    s = _schema()
    assert s.dims == ("bus", "state", "country")
    assert s.dimensions["country"].mapping
    assert not s.dimensions["bus"].mapping
    # Typed like any dim, so a column carrying its labels casts correctly.
    assert s.column_type("country") == nw.String()


def test_the_column_lives_on_the_classified_axis():
    """`country on state` puts a `country` column on `dims/state.parquet`.

    The side where it is single-valued: one state has one country, while a
    country has many states and could not hold them in a column.
    """
    s = _schema()
    assert s.mappings_on("bus") == ("state",)
    assert s.mappings_on("state") == ("country",)
    assert s.mappings_on("country") == ()


def test_a_chain_is_not_denormalised():
    """`dims/bus.parquet` carries `state`, never `state` and `country`.

    Two files asserting bus->country would let a layer restating
    `dims/state.parquet` leave every bus's `country` stale, with nothing to
    detect it. The chain is walked, not stored.
    """
    assert _schema().mappings_on("bus") == ("state",)


def test_a_mapping_keys_no_axis():
    """`on` is not `within`: it classifies, so it does not scope a label.

    `country` labels mean the same thing everywhere, so the axis key is the
    label alone - where a nested dim's would be `(parent, label)`.
    """
    s = _schema()
    assert s.axis_key("country") == ("country",)
    assert s.axis_key("bus") == ("bus",)


# -- what the declaration rejects -------------------------------------------


def test_a_mapping_on_an_undeclared_dim_is_refused():
    with pytest.raises(ValidationError, match="`on` undeclared dims"):
        _schema(
            dimensions={
                "country": Dimension(dtype=nw.String(), on={"nope"}),
            }
        )


def test_a_mapping_cannot_classify_itself():
    with pytest.raises(ValidationError, match="`on` itself"):
        _schema(dimensions={"country": Dimension(dtype=nw.String(), on={"country"})})


def test_a_mapping_cycle_is_refused():
    """`country on state on country` classifies nothing; it is a loop."""
    with pytest.raises(ValidationError, match="`on` is cyclic"):
        _schema(
            dimensions={
                "state": Dimension(dtype=nw.String(), on={"country"}),
                "country": Dimension(dtype=nw.String(), on={"state"}),
            }
        )


# -- through a real record --------------------------------------------------


def _budget_schema() -> Schema:
    """The mapping chain, with an attribute addressed by `country` alone."""
    return _schema(
        attributes={"co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})},
        partial=frozenset({"country"}),
    )


def test_a_mapping_folds_as_an_ordinary_axis(con, base_uri):
    """The fold learns nothing new: a mapping has an axis file like any dim.

    Its own file is what gives it order and a place for `co2_budget`, which no
    bus column could hold.
    """
    revision = Revision.create(con)
    write_schema(_budget_schema())
    write_axis(layer_dir(revision.id), "bus", [{"bus": "north", "state": "lower"}])
    write_axis(layer_dir(revision.id), "state", [{"state": "lower", "country": "DE"}])
    write_axis(
        layer_dir(revision.id), "country", [{"country": "DE"}, {"country": "FR"}]
    )

    axes = revision.node_cache.dims.axes
    assert sorted(axes["country"].df()["country"]) == ["DE", "FR"]
    # The classification is readable from the axis it classifies, one hop at a
    # time: bus -> state here, state -> country next.
    assert axes["bus"].df()["state"].tolist() == ["lower"]
    assert axes["state"].df()["country"].tolist() == ["DE"]


def test_an_attribute_addressed_by_a_mapping_alone_is_a_column_of_its_axis(
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


def test_setting_a_mapping_addressed_attribute_stages_an_axis_row(con, base_uri):
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
    """A mapping introduces the label, the fold keying per label rather than whole.

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


def test_a_mapping_axis_keeps_its_own_order(con, base_uri):
    """Axis order is the file's row order, mapping or not."""
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
