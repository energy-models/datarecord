"""Layer keys beyond `scenario`, resolved through a real store (design doc §5.5).

What `partial` and `keys` *mean* as declarations is pinned in
`test_schema.py`; here they are written into a layer and the fold is asked
whether it keyed by them.
"""

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
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_schema(
        schema(partial={"scenario", "period"}, keys={"scenario": {"component"}})
    )
    record.materialise()

    child = record.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "component_type": "Generator",
                "name": "Manchester Wind",
                "period": 2030,
                "value": 0.42,
            }
        ],
    )

    df = child.node_cache.inputs.df()
    wind = df[
        (df["name"] == "Manchester Wind") & (df["attribute"].astype(str) == "p_max_pu")
    ]
    owners = dict(zip(wind["period"], wind["layer_uuid"], strict=False))
    assert owners.get(2030) == child.id

    rel = relation(child, "p_max_pu").df()
    wind_rows = rel[rel["name"] == "Manchester Wind"]
    overridden = wind_rows[wind_rows["period"] == 2030]
    assert set(overridden["value"]) == {0.42}


def test_tombstone_ignores_period_even_when_period_is_partial(con, base_uri, ac_dc):
    """Deletion always acts on the whole component, never scoped to a period (§8.3).

    `period` is `partial` but keys nothing, so it never reaches the
    components map's key - which is what makes the tombstone unscoped.
    """
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_schema(schema(partial={"period"}, keys={}))
    record.materialise()

    child = record.child()
    tombstone(layer_dir(child.id), "Generator", ["Manchester Wind"])

    components = child.node_cache.components.df()
    assert "Manchester Wind" not in set(components["name"])


def test_the_fold_unions_maps_by_name(con, base_uri, ac_dc):
    """A child's own map is unioned with the parent's by name, not by position.

    One schema serves the whole tree (§5.6), so the dim *order* is fixed - but
    the parent's map is read from a persisted parquet file whose column order
    is its own, so the union must still be `UNION ALL BY NAME`. Positional
    would swap `scenario` and `period` here, which the values below would show.
    """
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    write_schema(
        schema(partial={"scenario", "period"}, keys={"scenario": {"component"}})
    )
    write_input(
        layer_dir(record.id),
        "p_max_pu",
        [
            {
                "component_type": "Generator",
                "name": "Manchester Wind",
                "scenario": "base",
                "period": 2020,
                "value": 0.1,
            }
        ],
    )
    record.materialise()

    child = record.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "component_type": "Generator",
                "name": "Manchester Wind",
                "scenario": "high",
                "period": 2030,
                "value": 0.5,
            }
        ],
    )

    df = child.node_cache.inputs.df()
    wind = df[(df["name"] == "Manchester Wind") & (df["layer_uuid"] == child.id)]
    assert wind["scenario"].tolist() == ["high"]
    assert wind["period"].tolist() == [2030]


# -- nesting (§5.4) ----------------------------------------------------------

_NESTED_DIMS = {"snapshot": "TIMESTAMP", "period": "BIGINT", "scenario": "VARCHAR"}
_NESTED_WITHIN = {"snapshot": {"period"}}


def test_a_nested_axis_keeps_a_label_per_parent(con, base_uri):
    """`snapshot within period` keys the axis by the pair, not the timestamp.

    Two periods holding the same timestamp are two points (§5.4): `t1` alone
    names nothing once the axis is nested, so folding by the label would
    collapse them into one row.
    """
    record = Revision.create(con)
    write_schema(schema(dims=_NESTED_DIMS, within=_NESTED_WITHIN))
    write_snapshots(
        layer_dir(record.id),
        [
            {"snapshot": "2020-01-01 00:00", "period": 2020},
            {"snapshot": "2020-01-01 01:00", "period": 2020},
            {"snapshot": "2020-01-01 00:00", "period": 2030},
            {"snapshot": "2020-01-01 01:00", "period": 2030},
        ],
    )

    axis = record.node_cache.dims.axes["snapshot"].df()
    assert len(axis) == 4
    assert sorted(axis["period"].tolist()) == [2020, 2020, 2030, 2030]


def test_a_child_overrides_one_nested_point(con, base_uri):
    """Last-writer-wins applies to `(period, snapshot)`, not to the timestamp.

    A child restating one period's hour leaves the other period's identically
    labelled hour to the parent.
    """
    record = Revision.create(con)
    write_schema(schema(dims=_NESTED_DIMS, within=_NESTED_WITHIN))
    write_snapshots(
        layer_dir(record.id),
        [
            {"snapshot": "2020-01-01 00:00", "period": 2020, "weight": 1.0},
            {"snapshot": "2020-01-01 00:00", "period": 2030, "weight": 1.0},
        ],
    )
    record.materialise()

    child = record.child()
    write_snapshots(
        layer_dir(child.id),
        [{"snapshot": "2020-01-01 00:00", "period": 2030, "weight": 7.0}],
    )

    axis = child.node_cache.dims.axes["snapshot"].df()
    weights = dict(zip(axis["period"], axis["weight"], strict=True))
    assert weights == {2020: 1.0, 2030: 7.0}
