# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Layer keys beyond `scenario`, resolved through a real record.

What `partial` *means* as a declaration is pinned in `test_schema.py`; here it
is written into a layer and the fold is asked whether it keyed by it.

Notes
-----
- [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
"""

from pathlib import Path

import narwhals as nw
import pandas as pd

from datarecord import Revision
from datarecord.duck import layer_dir
from tests.fixtures import (
    export_network,
    relation,
    schema,
    tombstone,
    write_attribute,
    write_periods,
    write_schema,
    write_snapshots,
)


def test_partial_period_override_resolves_per_period(con, base_uri, ac_dc):
    """With `period` `partial`, a child may replace one period only."""
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(schema(partial={"scenario", "period"}))
    revision.materialise()

    child = revision.child()
    write_attribute(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "entity": "Manchester Wind",
                "period": 2030,
                "value": 0.42,
            }
        ],
    )

    df = child.resolver.inputs.df()
    wind = df[
        (df["entity"] == "Manchester Wind")
        & (df["attribute"].astype(str) == "p_max_pu")
    ]
    owners = dict(zip(wind["period"], wind["layer_uuid"], strict=False))
    assert owners.get(2030) == child.id

    rel = relation(child, "p_max_pu").df()
    wind_rows = rel[rel["entity"] == "Manchester Wind"]
    overridden = wind_rows[wind_rows["period"] == 2030]
    assert set(overridden["value"]) == {0.42}


def test_deleting_a_dim_coordinate_drops_the_attribute_rows_keyed_on_it(
    con, base_uri, ac_dc
):
    """A dim tombstone removes the attribute rows over that coordinate, not others.

    The fold anti-joins every membership's tombstones into the inputs map, dims
    included: deleting `period=2030` drops a `p_max_pu` row keyed on 2030 while
    the row over 2020 survives. Not a cascade - the attribute honours the dim's
    own tombstone because that coordinate is part of its key.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(schema(partial={"period"}))
    write_periods(layer_dir(revision.id), [{"period": 2020}, {"period": 2030}])
    write_attribute(
        layer_dir(revision.id),
        "p_max_pu",
        [
            {"entity": "Manchester Wind", "period": 2020, "value": 0.2},
            {"entity": "Manchester Wind", "period": 2030, "value": 0.3},
        ],
    )
    revision.materialise()

    child = revision.child()
    write_periods(layer_dir(child.id), [{"period": 2030, "deleted": True}])

    rel = relation(child, "p_max_pu").df()
    wind = rel[rel["entity"] == "Manchester Wind"]
    assert set(wind["period"]) == {2020}, "the deleted coordinate's row is gone"

    keys = child.resolver.inputs.df()
    keyed = keys[keys["entity"] == "Manchester Wind"]
    assert set(keyed["period"].dropna()) == {2020}, (
        "and the map no longer owns its key; an attribute not over `period` "
        "keeps a NULL there"
    )


def test_tombstone_ignores_period_even_when_period_is_partial(con, base_uri, ac_dc):
    """Deletion always acts on the whole component, never scoped to a period.

    `period` is `partial`, so it keys the *inputs* map - but the components map
    is keyed by `entity` alone, which is what makes the tombstone unscoped.
    Existence does not vary along a dim, so there is nothing to scope it by.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(schema(partial={"period"}))
    revision.materialise()

    child = revision.child()
    tombstone(layer_dir(child.id), "Generator", ["Manchester Wind"])

    axis_rel = child.resolver.dims.axes["entity"]
    assert axis_rel is not None
    assert "Manchester Wind" not in set(axis_rel.df()["entity"])


def test_the_fold_unions_maps_by_name(con, base_uri, ac_dc):
    """A child's own map is unioned with the parent's by name, not by position.

    One schema serves the whole tree, so the dim *order* is fixed - but
    the parent's map is read from a persisted parquet file whose column order
    is its own, so the union must still be `UNION ALL BY NAME`. Positional
    would swap `scenario` and `period` here, which the values below would show.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    write_schema(schema(partial={"scenario", "period"}))
    write_attribute(
        layer_dir(revision.id),
        "p_max_pu",
        [
            {
                "entity": "Manchester Wind",
                "scenario": "base",
                "period": 2020,
                "value": 0.1,
            }
        ],
    )
    revision.materialise()

    child = revision.child()
    write_attribute(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "entity": "Manchester Wind",
                "scenario": "high",
                "period": 2030,
                "value": 0.5,
            }
        ],
    )

    df = child.resolver.inputs.df()
    wind = df[(df["entity"] == "Manchester Wind") & (df["layer_uuid"] == child.id)]
    assert wind["scenario"].tolist() == ["high"]
    assert wind["period"].tolist() == [2030]


# -- nesting (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis) ----------------------------------------------------------

_NESTED_DIMS = {
    "snapshot": nw.Datetime(),
    "period": nw.Int64(),
    "scenario": nw.String(),
}
_NESTED_WITHIN = {"snapshot": {"period"}}


def test_a_nested_axis_keeps_a_label_per_parent(con, base_uri):
    """`snapshot within period` keys the axis by the pair, not the timestamp.

    Two periods holding the same timestamp are two points: `t1` alone
    names nothing once the axis is nested, so folding by the label would
    collapse them into one row.

    Notes
    -----
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    revision = Revision.create(con)
    write_schema(schema(dims=_NESTED_DIMS, within=_NESTED_WITHIN))
    write_snapshots(
        layer_dir(revision.id),
        [
            {"snapshot": "2020-01-01 00:00", "period": 2020},
            {"snapshot": "2020-01-01 01:00", "period": 2020},
            {"snapshot": "2020-01-01 00:00", "period": 2030},
            {"snapshot": "2020-01-01 01:00", "period": 2030},
        ],
    )

    axis = revision.resolver.dims.axes["snapshot"].df()
    assert len(axis) == 4
    assert sorted(axis["period"].tolist()) == [2020, 2020, 2030, 2030]


def test_a_child_overrides_one_nested_point(con, base_uri):
    """Last-writer-wins applies to `(period, snapshot)`, not to the timestamp.

    A child restating one period's hour leaves the other period's identically
    labelled hour to the parent.
    """
    revision = Revision.create(con)
    write_schema(schema(dims=_NESTED_DIMS, within=_NESTED_WITHIN))
    write_snapshots(
        layer_dir(revision.id),
        [
            {"snapshot": "2020-01-01 00:00", "period": 2020, "weight": 1.0},
            {"snapshot": "2020-01-01 00:00", "period": 2030, "weight": 1.0},
        ],
    )
    revision.materialise()

    child = revision.child()
    write_snapshots(
        layer_dir(child.id),
        [{"snapshot": "2020-01-01 00:00", "period": 2030, "weight": 7.0}],
    )

    axis = child.resolver.dims.axes["snapshot"].df()
    weights = dict(zip(axis["period"], axis["weight"], strict=True))
    assert weights == {2020: 1.0, 2030: 7.0}


def test_a_dim_names_its_own_file(con, base_uri):
    """`dims/<dim>.parquet`, whatever the dim is called.

    A dim named `bus` is the case that matters: pluralising by concatenation
    would look for `buss.parquet` and find nothing, so the axis would read as
    absent rather than wrong.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    revision = Revision.create(con)
    write_schema(schema(dims={"bus": nw.String()}, partial=set()))
    target = Path(layer_dir(revision.id), "dims")
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"bus": "north"}, {"bus": "south"}]).to_parquet(
        target / "bus.parquet", index=False
    )

    axis = revision.resolver.dims.axes["bus"].df()
    assert sorted(axis["bus"].tolist()) == ["north", "south"]


def test_the_entity_column_is_entity(con, base_uri, ac_dc):
    """`entity` names the component in every frame the protocol hands back.

    The one axis the format knows by name: the component axis, which the type
    relation and every other relation over components are keyed by.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    record = revision.record
    assert "entity" in record.dims["entity"].collect_schema().names()
    assert "entity" in record.relations["entity_type"].collect_schema().names()
    assert "entity" in record.attributes["p_max_pu"].collect_schema().names()
    # And in the owner map the fold builds over them.
    ea = revision.resolver.dims.axes["entity"]
    assert ea is not None
    assert "entity" in ea.df().columns


def test_the_entity_axis_is_where_identity_lives(con, base_uri, ac_dc):
    """`dims/entity.parquet` says which entities exist, once each.

    What type each is lives in the `entity_type` relation, keyed by the same
    names. The components map folds from the axis file alone.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    axis = con.read_parquet(layer_dir(revision.id) + "dims/entity.parquet").df()
    assert {"entity", "deleted"} <= set(axis.columns)
    assert not axis["entity"].duplicated().any()
    kinds = con.read_parquet(layer_dir(revision.id) + "relations/entity_type.parquet")
    assert "Generator" in set(kinds.df()["entity_type"])

    # And it is what the fold reads: the map's entities are the axis's.
    ea2 = revision.resolver.dims.axes["entity"]
    assert ea2 is not None
    mapped = ea2.df()
    assert set(mapped["entity"]) == set(axis.loc[~axis["deleted"], "entity"])
