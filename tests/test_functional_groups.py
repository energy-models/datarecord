# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""Classifications: a property that refers to a dim.

`bus.state` `one_of: state` says every bus is in at most one state: a
column of the bus axis whose values are labels of `state`, which keeps an axis
file for its order and for its properties.

Notes
-----
- [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
- [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
"""

from typing import Any

import duckdb
import pandas as pd
import pytest
from duckdb import sqltypes
from pydantic import ValidationError

from datarecord.mutable import WorkingRecord
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from datarecord.store.store import Revision
from tests.fixtures import (
    empty_root,
    write_axis,
    write_multi_dim,
)
from tests.test_write import _Source


def _schema(**overrides) -> Schema:
    """A record classified twice over, bus -> state -> country, and a pair classified into a country."""
    kwargs: dict[str, Any] = {
        "dimensions": {
            "bus": Dimension(dtype="str"),
            "state": Dimension(dtype="str"),
            "country": Dimension(dtype="str"),
            "scenario": Dimension(dtype="str"),
            "bus_state": Dimension(over=["bus", "state"]),
        },
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


def test_a_multi_dim_in_dims_is_one_struct_column():
    """A multi-dim in `dims` is carried as itself, a struct over its fields."""
    s = _schema(
        dimensions={
            **_schema().dimensions,
            "connection": Dimension(over=["bus", "state"]),
        },
        attributes={"capacity": AttributeSpec(dtype="float", dims={"connection"})},
    )
    assert s.dims_of("capacity") == ("connection",), "the multi-dim, not expanded"
    assert s.column_type("connection") == duckdb.struct_type(
        {"bus": sqltypes.VARCHAR, "state": sqltypes.VARCHAR}
    )
    assert s.multi_dims_of("capacity") == ("connection",)


def test_the_over_list_form_is_sugar_for_the_dict():
    """`[bus]` is `{bus: bus}`; the dict is what a corridor needs.

    A corridor draws two fields from one dim.
    """
    assert Dimension(over=["bus"]).over == {"bus": "bus"}
    s = _schema(
        dimensions={
            **_schema().dimensions,
            "corridor": Dimension(over={"from": "bus", "to": "bus"}),
        }
    )
    assert list(s.over("corridor")) == ["from", "to"], "every field"
    assert s.dimensions["corridor"].dtype is None, "a multi-dim has no dtype"
    assert "dtype" not in s.model_dump()["dimensions"]["corridor"]


# -- what the declaration rejects -------------------------------------------


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
            "refers to 'state', but 'state' is not a one_of dim",
            id="a-default-by-on-a-dim-without-one-of",
        ),
    ],
)
def test_a_malformed_property_referring_to_a_dim_is_refused(spec, extra, match):
    """A property that refers to a dim without `one_of` categorizes nothing."""
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


def test_removing_a_label_of_a_dim_outside_partial_drops_its_tuples(store):
    """A multi-dim may draw on a dim `partial` does not list; removal still cascades.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    layer = empty_root(store, _schema())
    write_axis(layer, "bus", [{"bus": "north"}, {"bus": "south"}])
    write_axis(layer, "state", [{"state": "lower"}, {"state": "upper"}])
    write_multi_dim(
        layer,
        "bus_state",
        [
            {"bus_state": {"bus": "north", "state": "lower"}},
            {"bus_state": {"bus": "south", "state": "upper"}},
        ],
    )
    revision = layer.commit()

    staged = WorkingRecord(revision.record)
    staged.remove("bus", ["north"])
    assert _tuples(staged) == [{"bus": "south", "state": "upper"}], (
        "the tuple naming the removed bus is dropped"
    )

    child = revision.store.commit(staged)
    assert _tuples(child.record) == [{"bus": "south", "state": "upper"}]


def _tuples(record) -> list[dict]:
    return record.dims["bus_state"].df()["bus_state"].tolist()


def test_a_property_of_a_named_dim_is_a_column_of_its_axis(store):
    """`co2_budget` is a property of the country, so it rides on the axis file.

    An attribute addressed by `country` alone, `target`, is long rows of
    `attributes/target.parquet` instead: the section decides, not the shape.
    """
    layer = empty_root(store, _budget_schema())
    write_axis(
        layer,
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )
    revision = layer.commit()

    axis = revision.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 40.0, "FR": 55.0}
    assert "co2_budget" not in revision.record.attributes, (
        "an axis-file column is no long frame"
    )

    staged = WorkingRecord(revision.record)
    staged.set("target", {"DE": 30.0})
    child = revision.store.commit(staged)
    assert "target" not in child.record._axes["country"].columns
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
    layer = empty_root(
        store,
        _schema(
            properties={
                "country": {
                    "co2_budget": PropertySpec(dtype="float"),
                    "population": PropertySpec(dtype="float"),
                }
            },
            partial=frozenset({"country"}),
        ),
    )
    write_axis(
        layer,
        "country",
        [{"country": "DE", "co2_budget": 40.0, "population": 83.0}],
    )

    staged = WorkingRecord(layer.commit().record)
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


def test_a_child_layer_restates_an_axis_whole(store):
    """Touching an axis means carrying every label of it, in order, even where `country` is partial.

    A layer holding the edited label alone would not leave the others stale, it
    would remove them: the axis here is what this layer says it is.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
    """
    properties = {"country": {"co2_budget": PropertySpec(dtype="float")}}
    layer = empty_root(
        store, _schema(properties=properties, partial=frozenset({"country"}))
    )
    write_axis(
        layer,
        "country",
        [{"country": "DE", "co2_budget": 40.0}, {"country": "FR", "co2_budget": 55.0}],
    )
    revision = layer.commit()

    staged = WorkingRecord(revision.record)
    staged.set("country.co2_budget", {"DE": 12.0})

    country = staged.edits.axis("country")
    assert country is not None
    patch = country.df()
    assert patch["country"].tolist() == ["DE", "FR"], (
        "the whole axis in order, not just the edited label"
    )

    child = revision.store.commit(staged)
    axis = child.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "FR": 55.0}, (
        "FR survives because this layer carried it"
    )


def test_a_property_is_set_only_on_a_label_some_layer_wrote(store):
    """A property is a column of a label's row, so `add` makes the label first."""
    layer = empty_root(store, _budget_schema())
    write_axis(layer, "country", [{"country": "DE", "co2_budget": 40.0}])
    revision = layer.commit()

    staged = WorkingRecord(revision.record)
    with pytest.raises(KeyError, match="`add` them first"):
        staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})

    staged.add("country", ["NO"])
    staged.set("country.co2_budget", {"DE": 12.0, "NO": 3.0})
    child = revision.store.commit(staged)
    axis = child.record.properties["country"].df()
    assert dict(zip(axis["country"], axis["co2_budget"])) == {"DE": 12.0, "NO": 3.0}


# -- a property that refers to a dim holds a live label --------


@pytest.fixture(scope="module")
def classified(module_store) -> Revision:
    """North in lower, south in upper, both in DE, and the pair (north, lower) in DE."""
    layer = empty_root(module_store, _schema())
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
    write_multi_dim(
        layer,
        "bus_state",
        [{"bus_state": {"bus": "north", "state": "lower"}, "country": "DE"}],
    )
    return layer.commit()


EDITS = {
    "set-scalar": lambda w, v: w.set("bus.state", v, bus="north"),
    "add": lambda w, v: w.add("bus", ["east"], state=v),
    "set-on-a-multi-dim": lambda w, v: w.set("bus_state.country", v),
    "add-a-tuple": lambda w, v: w.add(
        "bus_state", [{"bus": "south", "state": "upper"}], country=v
    ),
}


@pytest.mark.parametrize("edit", EDITS.values(), ids=EDITS)
def test_a_property_naming_a_dim_refuses_a_label_it_lacks(classified, edit):
    """`set` and `add` refuse a value naming no label of the dim, as staged.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
    """
    staged = WorkingRecord(classified.record)
    with pytest.raises(KeyError, match=r"names '(state|country)' labels \['coal'\]"):
        edit(staged, "coal")


@pytest.mark.parametrize(
    ("value", "edit"),
    [
        (None, "set-scalar"),
        (None, "add-a-tuple"),
        ("added", "add"),
        ("added", "set-on-a-multi-dim"),
    ],
)
def test_a_property_naming_a_dim_takes_null_or_a_label_added_first(
    classified, value, edit
):
    """NULL names no label; a label staged by `add` is live."""
    staged = WorkingRecord(classified.record)
    staged.add("state", ["added"])
    staged.add("country", ["added"])
    EDITS[edit](staged, value)
    classified.store.commit(staged)


def test_add_refuses_a_tuple_property_label_the_dim_does_not_list(store):
    """A label outside the list cannot be added, so the message does not say to."""
    dimensions = {**_schema().dimensions, "country": Dimension(one_of=["DE", "NO"])}
    revision = empty_root(store, _schema(dimensions=dimensions)).commit()
    staged = WorkingRecord(revision.record)
    with pytest.raises(
        KeyError, match=r"its `one_of`, \['DE', 'NO'\], does not hold; use one of"
    ):
        staged.add("bus_state", [{"bus": "a", "state": "b"}], country="coal")


def test_remove_is_refused_while_a_property_names_the_label(classified):
    """A property's row outlives the label it names, so clearing it comes first.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
    """
    staged = WorkingRecord(classified.record)
    with pytest.raises(
        ValueError, match=r"`bus.state` names them at \[\('north', 'lower'\)\]"
    ):
        staged.remove("state", ["lower"])

    staged.set("bus.state", None, bus="north")
    staged.remove("state", ["lower"])
    with pytest.raises(KeyError, match=r"names 'state' labels \['lower'\]"):
        staged.set("bus.state", "lower", bus="south")
    child = classified.store.commit(staged)
    assert child.record.dims["state"].df()["state"].tolist() == ["upper"]


def test_the_writer_refuses_a_property_naming_a_removed_label(classified):
    """A source written whole is not staged, so the writer checks it.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    staged = WorkingRecord(classified.record)
    staged.remove("country", ["NO"])
    removed = classified.store.commit(staged)

    tuples = pd.DataFrame(
        {"bus_state": [{"bus": "south", "state": "upper"}], "country": ["NO"]}
    )
    source = _Source(_schema(), connections={"bus_state": tuples}, dims={})
    with pytest.raises(ValueError, match=r"a removed country in 'country'"):
        removed.store.commit(source, parent=removed)
