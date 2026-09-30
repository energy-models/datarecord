# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Functional relations: a relation declaring `values`, which classifies its key.

`country` keyed by `[bus]` with `values` `country` says every bus is in exactly
one country. The relation is a file of its own, `relations/country.parquet`, and
its `values` dim keeps an axis file for its order and for the labels an
attribute over it is keyed by.

Notes
-----
- [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
- [why `into` is the right field](https://energy-models.github.io/datarecord/design/schema/#why-into-is-the-right-field)
"""

from typing import Any

import narwhals as nw
import pandas as pd
import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Relation, Schema
from tests.fixtures import write_axis, write_relation, write_schema, write_values


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

    Its own file is what gives it order and the labels `co2_budget` is keyed by.
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


def _budgets(record) -> dict[str, float]:
    """`co2_budget` per country, as the record resolves it."""
    frame = record.attributes["co2_budget"].collect("pandas").to_native()
    return dict(zip(frame["country"], frame["value"], strict=True))


def _budget_record(con, schema: Schema) -> Revision:
    """A root layer with two countries and a budget for each."""
    revision = Revision.create(con)
    write_schema(schema)
    write_axis(
        layer_dir(revision.id), "country", [{"country": "DE"}, {"country": "FR"}]
    )
    write_values(
        layer_dir(revision.id),
        "co2_budget",
        pd.DataFrame({"country": ["DE", "FR"], "co2_budget": [40.0, 55.0]}),
    )
    return revision


def test_set_states_one_labels_value(con, base_uri):
    """A read with pending edits answers the new value; FR keeps the base's."""
    revision = _budget_record(con, _budget_schema())
    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})
    assert _budgets(staged) == {"DE": 12.0, "FR": 55.0}, (
        "FR is untouched, so it keeps the base value"
    )


def test_a_child_layer_holds_only_the_labels_it_touched(con, base_uri):
    """With `country` `partial`, a patch layer holds the edited value alone.

    Deliberately not materialised: a patch layer resolves over its parent's raw
    layer, so no node cache is required for the fold to see both labels.
    """
    revision = _budget_record(con, _budget_schema())
    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})
    patch = staged.resolver.sources[-1].attribute("co2_budget")
    assert patch is not None
    assert patch.df()["country"].tolist() == ["DE"], "only the touched label"

    child = staged.commit(NewChild(revision))
    assert _budgets(child.record) == {"DE": 12.0, "FR": 55.0}, (
        "last writer wins per label"
    )


def test_a_child_layer_restates_a_dim_it_owns_whole(con, base_uri):
    """Outside `partial`, touching one country's budget carries every country's.

    A layer holding the edited label alone would not leave the others stale, it
    would remove them: the fold keys by `partial` dims only, so the budgets here
    are what this layer says they are.
    """
    revision = _budget_record(
        con,
        _schema(
            attributes={
                "co2_budget": AttributeSpec(dtype=nw.Float64(), dims={"country"})
            }
        ),
    )
    staged = WorkingRecord(revision.record, con)
    staged.set("co2_budget", {"DE": 12.0})
    patch = staged.resolver.sources[-1].attribute("co2_budget")
    assert patch is not None
    assert sorted(patch.df()["country"].tolist()) == ["DE", "FR"], (
        "every label, not just the edited one"
    )

    child = staged.commit(NewChild(revision))
    assert _budgets(child.record) == {"DE": 12.0, "FR": 55.0}, (
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
