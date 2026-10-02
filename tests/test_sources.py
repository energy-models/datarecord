# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record as tables keyed by declared names, in and out.

Notes
-----
- [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.layered.resolve import write_schema
from datarecord.layered.write import write_record
from datarecord.schema import Dimension, Group, Schema
from datarecord.sources import from_sources, to_sources
from tests.test_declared_dims import DECLARATIONS, SNAPSHOTS, STORAGE


@pytest.fixture
def declared():
    return Schema.from_mathspec(DECLARATIONS, storage=STORAGE)


@pytest.fixture
def tables():
    """Two generators, a constant capacity, and a series with one exception."""
    return {
        "generator": pd.DataFrame({"generator": ["wind", "gas"]}),
        "bus": pd.DataFrame({"bus": ["north"]}),
        "carrier": pd.DataFrame({"carrier": ["wind", "gas"]}),
        "snapshot": pd.DataFrame({"snapshot": SNAPSHOTS}),
        "Generator_bus": pd.DataFrame(
            {"generator": ["wind", "gas"], "bus": ["north", "north"]}
        ),
        "Generator_marginal_cost": pd.DataFrame(
            {"generator": ["wind", "gas"], "value": [0.0, 50.0]}
        ),
        "Generator_p_nom": pd.DataFrame({"value": [100.0]}),
        "Generator_p_max_pu": pd.DataFrame(
            {
                "generator": [None, "wind"],
                "snapshot": [None, SNAPSHOTS[1]],
                "value": [1.0, 0.3],
            }
        ),
    }


@pytest.fixture
def record(con, base_uri, declared, tables):
    write_schema(declared, base_uri)
    revision = Revision.create(con)
    write_record(revision.id, from_sources(declared, tables), con)
    return revision.record


def _rows(frame, *key):
    df = frame.collect().to_native().to_pandas()
    return {tuple(r[k] for k in key): r["value"] for _, r in df.iterrows()}


def test_every_declared_name_comes_back(record):
    """Dimensions, the relation and each parameter written come back by name."""
    got = to_sources(record)
    assert set(got) >= {
        "generator",
        "bus",
        "carrier",
        "snapshot",
        "Generator_bus",
        "Generator_marginal_cost",
        "Generator_p_nom",
        "Generator_p_max_pu",
    }, "a table goes in and comes out under the same declared name"


def test_a_constant_left_without_coordinates_reaches_every_label(record):
    """`Generator_p_nom` was one row with no columns but `value`."""
    got = _rows(to_sources(record)["Generator_p_nom"], "generator")
    assert got == {("wind",): 100.0, ("gas",): 100.0}, (
        "a NULL generator is every generator; scenario has no labels, so it stays out"
    )


def test_a_named_row_outranks_the_broadcast_row(record):
    """The default row and the exception sit side by side in one layer."""
    got = _rows(to_sources(record)["Generator_p_max_pu"], "generator", "snapshot")
    assert got == {
        ("wind", SNAPSHOTS[0]): 1.0,
        ("wind", SNAPSHOTS[1]): 0.3,
        ("gas", SNAPSHOTS[0]): 1.0,
        ("gas", SNAPSHOTS[1]): 1.0,
    }, "one value per coordinate, the named one at (wind, second snapshot)"


def test_an_axis_attribute_is_a_table_of_its_own(record):
    """`Generator_marginal_cost` over `generator` alone round-trips as `(generator, value)`."""
    got = _rows(to_sources(record)["Generator_marginal_cost"], "generator")
    assert got == {("wind",): 0.0, ("gas",): 50.0}, (
        "stored as a column, read as a table"
    )


def test_a_name_the_schema_does_not_declare_is_refused(declared):
    with pytest.raises(KeyError, match=r"declares no \['Generator_p_min'\]"):
        from_sources(declared, {"Generator_p_min": pd.DataFrame({"value": [0.1]})})


def test_an_integer_coordinate_left_out_of_a_pandas_table_is_written_null(
    con, base_uri
):
    """Pandas has no NULL for an integer column, so the seam goes through Arrow.

    `Load_p_set` leaves out `snapshot`, an `int` dim; building its NULL column
    on the pandas frame itself raised before any row was written.
    """
    schema = Schema.from_mathspec(
        {
            "dimensions": {"snapshot": {"dtype": "int"}, "bus": {}},
            "parameters": {"Load_p_set": {"dims": ["snapshot", "bus"]}},
        },
        storage={"partial": {"bus"}},
    )
    write_schema(schema, base_uri)
    revision = Revision.create(con)
    tables = {
        "snapshot": pd.DataFrame({"snapshot": [0, 1]}),
        "bus": pd.DataFrame({"bus": ["north"]}),
        "Load_p_set": pd.DataFrame({"bus": ["north"], "value": [120.0]}),
    }
    write_record(revision.id, from_sources(schema, tables), con)
    got = _rows(to_sources(revision.record)["Load_p_set"], "snapshot", "bus")
    assert got == {(0, "north"): 120.0, (1, "north"): 120.0}, (
        "the one row reaches both snapshots"
    )


@pytest.fixture
def kind_schema():
    """A classifying group that shares its `into` dim's name, as the schema allows."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype=nw.String()),
            "kind": Dimension(dtype=nw.String()),
        },
        groups={"kind": Group(over=["entity"], into="kind")},
        partial=frozenset({"entity"}),
    )


def test_a_name_both_a_dimension_and_a_group_is_refused_on_the_way_in(kind_schema):
    """One key cannot carry both a dim's labels and a group's rows.

    `from_sources` handed the `kind` table to the dim too, and `write_record`
    then refused `dims/kind.parquet` for carrying `entity`. mathspec refuses a
    relation named after a dimension, so the seam refuses it by name first.
    """
    tables = {"kind": pd.DataFrame({"entity": ["wind"], "kind": ["Generator"]})}
    with pytest.raises(KeyError, match=r"\['kind'\] name both a dimension and a group"):
        from_sources(kind_schema, tables)


def test_a_name_both_a_dimension_and_a_group_is_refused_on_the_way_out(
    con, base_uri, kind_schema
):
    """`to_sources` refuses the colliding name, and serves every other one asked for."""
    write_schema(kind_schema, base_uri)
    revision = Revision.create(con)
    tables = {"entity": pd.DataFrame({"entity": ["wind"]})}
    write_record(revision.id, from_sources(kind_schema, tables), con)
    with pytest.raises(KeyError, match=r"\['kind'\] name both a dimension and a group"):
        to_sources(revision.record)
    assert set(to_sources(revision.record, names=["entity"])) == {"entity"}, (
        "a caller naming what it needs is served"
    )
