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
    write_input,
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
    write_input(
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

    df = child.node_cache.inputs.df()
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

    entity_types = child.node_cache.entity_map.df()
    assert "Manchester Wind" not in set(entity_types["entity"])


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
    write_input(
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
    write_input(
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

    df = child.node_cache.inputs.df()
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

    axis = revision.node_cache.dims.axes["snapshot"].df()
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

    axis = child.node_cache.dims.axes["snapshot"].df()
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

    axis = revision.node_cache.dims.axes["bus"].df()
    assert sorted(axis["bus"].tolist()) == ["north", "south"]


def test_the_entity_column_is_entity(con, base_uri, ac_dc):
    """`entity` names the component in every frame the protocol hands back.

    The one axis the format knows by name, because it is the axis the component
    types partition: `entity_type` hangs off it and `dims/entity_type/` is
    keyed by it. Every other dim is declared.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    record = revision.record
    assert "entity" in record.entity_types["Generator"].collect_schema().names()
    assert "entity" in record.attributes["p_max_pu"].collect_schema().names()
    # And in the owner map the fold builds over them.
    assert "entity" in revision.node_cache.entity_map.df().columns


def test_the_entity_axis_is_where_identity_lives(con, base_uri, ac_dc):
    """`dims/entity.parquet` says which entities exist and what type each is.

    Derived by the writer from the per-type frames rather than handed over, so
    a record cannot disagree with itself about it. The components map folds
    from this one file, where it used to glob `dims/entity_type/` and take the
    type from the filename.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)

    axis = con.read_parquet(layer_dir(revision.id) + "dims/entity.parquet").df()
    assert {"entity", "entity_type", "deleted"} <= set(axis.columns)
    assert not axis["entity"].duplicated().any()
    assert "Generator" in set(axis["entity_type"])

    # And it is what the fold reads: the map's entities are the axis's.
    mapped = revision.node_cache.entity_map.df()
    assert set(mapped["entity"]) == set(axis.loc[~axis["deleted"], "entity"])
