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

import pytest
from pydantic import ValidationError

from datarecord import Revision
from datarecord.duck import layer_dir
from datarecord.schema import AttributeSpec, Dimension, Schema
from tests.fixtures import write_axis, write_schema


def _schema(**overrides) -> Schema:
    """A record classified twice over: bus -> state -> country."""
    kwargs = {
        "dimensions": {
            "bus": Dimension(dtype="VARCHAR"),
            "state": Dimension(dtype="VARCHAR", on={"bus"}),
            "country": Dimension(dtype="VARCHAR", on={"state"}),
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
    assert s.column_type("country") == "VARCHAR"


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
                "country": Dimension(dtype="VARCHAR", on={"nope"}),
            }
        )


def test_a_mapping_cannot_classify_itself():
    with pytest.raises(ValidationError, match="`on` itself"):
        _schema(dimensions={"country": Dimension(dtype="VARCHAR", on={"country"})})


def test_a_mapping_cycle_is_refused():
    """`country on state on country` classifies nothing; it is a loop."""
    with pytest.raises(ValidationError, match="`on` is cyclic"):
        _schema(
            dimensions={
                "state": Dimension(dtype="VARCHAR", on={"country"}),
                "country": Dimension(dtype="VARCHAR", on={"state"}),
            }
        )


def test_a_mapping_cannot_key_membership():
    """Existence cannot vary along a classification of another axis.

    Whether a component exists in Germany is already settled by its bus and
    that bus's country, so there is no freedom for it to vary independently.
    """
    with pytest.raises(ValidationError, match="cannot vary along a classification"):
        _schema(
            dimensions={
                "bus": Dimension(dtype="VARCHAR"),
                "country": Dimension(dtype="VARCHAR", on={"bus"}, keys={"component"}),
            },
            partial=frozenset({"country"}),
        )


# -- through a real record --------------------------------------------------


def test_a_mapping_folds_as_an_ordinary_axis(con, base_uri):
    """The fold learns nothing new: a mapping has an axis file like any dim.

    Its own file is what gives it order and a place for `co2_budget`, which no
    bus column could hold.
    """
    revision = Revision.create(con)
    write_schema(
        _schema(
            attributes={"co2_budget": AttributeSpec(dtype="DOUBLE", dims={"country"})},
        )
    )
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
