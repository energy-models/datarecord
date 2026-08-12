"""The fold, the cache and persistence (design doc §9.1, §8.2, §12)."""

import shutil
from pathlib import Path

import pytest

from datarecord import Revision
from datarecord.duck import layer_dir, resolved_dir, union_all_by_name
from tests.fixtures import export_network, tombstone, write_input


@pytest.fixture
def parent(con, base_uri, ac_dc):
    record = Revision.create(con)
    export_network(ac_dc, record, con)
    record.materialise()
    return record


def test_union_all_by_name_folds_every_relation(con):
    """Three arms, not two: the fold's union binds `u`/`rel` by name (§9.2).

    Two relations pass whatever the loop body does, since the first pairing is
    the only one. Three is what catches the failure mode the helper's variables
    invite - a scan that re-reads `rels[0]` instead of advancing would give
    `[1, 1]` here, with no error to point at it.
    """
    rels = [con.sql(f"SELECT {i} AS x") for i in (1, 2, 3)]
    got = union_all_by_name(rels, con).fetchall()
    assert sorted(v for (v,) in got) == [1, 2, 3]


def test_union_all_by_name_fills_a_missing_column_with_null(con):
    """By *name*, so an arm lacking a column reads NULL there (§3.2, §5.7).

    What lets a layer written before `bus`/`breakpoint` existed still resolve,
    and a persisted owner map survive a newly declared dim.
    """
    rels = [con.sql("SELECT 1 AS x, 'a' AS y"), con.sql("SELECT 2 AS x")]
    got = union_all_by_name(rels, con).fetchall()
    assert sorted(got, key=lambda r: r[0]) == [(1, "a"), (2, None)]


def keys(record, con):
    """The inputs map's keys: `(name, attribute)`, no type (§3.5)."""
    df = record.node_cache.inputs.df()
    return {(r["name"], str(r.attribute)) for _, r in df.iterrows()}


def component_names(record):
    return set(record.node_cache.components.df()["name"])


def test_root_map_is_its_own_layer(con, parent):
    """Every key of a root's map points at the root itself."""
    om = parent.node_cache
    assert set(om.inputs.df()["layer_uuid"]) == {parent.id}
    assert set(om.components.df()["layer_uuid"]) == {parent.id}
    assert ("Manchester Wind", "p_max_pu") in keys(parent, con)


def test_materialise_writes_the_map_under_resolved(con, parent):
    """`materialise` writes the maps under `resolved/`, not at the layer root (§8.2)."""
    assert Path(resolved_dir(parent.id), "owner_map", "inputs.parquet").exists()
    assert Path(resolved_dir(parent.id), "owner_map", "components.parquet").exists()
    # The cache shares the record's directory but stays out of the layer's own
    # namespace, so a reader that knows nothing about layering still sees a
    # plain parquet store: every glob into a layer is single-level, so nothing
    # under `resolved/` is reachable by one (§8.3).
    assert not Path(layer_dir(parent.id), "owner_map").exists()
    # The globs the fold and `DirectoryRecord` actually use must not reach a
    # cached file.
    layer = Path(layer_dir(parent.id))
    reachable = {
        p
        for pattern in (
            "*.parquet",
            "inputs/*.parquet",
            "outputs/*.parquet",
            "dims/*.parquet",
            "dims/*/*.parquet",
        )
        for p in layer.glob(pattern)
    }
    assert reachable
    assert not any("resolved" in p.parts for p in reachable)


def test_last_writer_wins_per_key(con, parent):
    """A child's key overrides the parent's, others stay with the parent."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"name": "Manchester Wind", "value": 0.42}],
    )
    df = child.node_cache.inputs.df()

    def owner_of(name, attr):
        sel = df[(df["name"] == name) & (df["attribute"].astype(str) == attr)]
        return sel["layer_uuid"].iloc[0]

    assert owner_of("Manchester Wind", "p_max_pu") == child.id
    assert owner_of("Manchester Wind", "marginal_cost") == parent.id
    assert owner_of("Norway Wind", "p_max_pu") == parent.id


def test_tombstone_removes_all_attributes(con, parent):
    """A tombstone removes every attribute and the component row of the component (§8.3)."""
    before = {k for k in keys(parent, con) if k[0] == "Norway Gas"}
    assert len(before) > 1
    assert "Norway Gas" in component_names(parent)

    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["Norway Gas"])
    assert not {k for k in keys(child, con) if k[0] == "Norway Gas"}
    assert "Norway Gas" not in component_names(child)


def test_live_fold_is_cached_per_connection(con, parent):
    """A node with no materialised cache folds once and caches the table (§14).

    Nothing invalidates it: layers are write-once (§8.2), so a fold's inputs
    cannot change under it.
    """
    child = parent.child()
    om = child.node_cache
    om.inputs.fetchall()
    om.components.fetchall()
    for table in (
        f"owner_map_inputs_{child.id.hex}",
        f"owner_map_components_{child.id.hex}",
    ):
        assert con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [table]
        ).fetchone()


def test_materialising_does_not_change_the_map(con, parent):
    """`materialise` is purely additive: same answer, fewer layers read (§8.2)."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"name": "Manchester Wind", "value": 0.42}],
    )
    live = keys(child, con)
    child.materialise()
    assert keys(child, con) == live


def test_a_removed_cache_falls_back_to_the_fold(con, parent):
    """A cache is an optimisation, so losing it costs work rather than answers (§8.2).

    The old model had to raise here: a closed node's ancestry was truncated to
    itself, so re-folding would have silently dropped every ancestor. Gating on
    the cache's presence instead means the untruncated path is still available.
    """
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_min_pu",
        [{"name": "Norway Gas", "value": 0.1}],
    )
    child.materialise()
    expected = keys(child, con)

    shutil.rmtree(Path(resolved_dir(child.id), "owner_map"))
    fresh = Revision.get(child.id, con)
    assert keys(fresh, con) == expected


def test_any_node_may_be_a_parent(con, parent):
    """A layer is write-once, so no node needs preparing to branch from (§8.2)."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"name": "Manchester Wind", "value": 0.42}],
    )
    grandchild = child.child()
    # The child was never materialised, yet its layer resolves for the
    # grandchild all the same.
    df = grandchild.node_cache.inputs.df()
    row = df[(df["name"] == "Manchester Wind") & (df["attribute"] == "p_max_pu")]
    assert set(row["layer_uuid"]) == {child.id}


def test_ancestry_is_root_first(con, parent):
    """`ancestry` returns the root->node path in resolution order (§13)."""
    child = parent.child()
    child.materialise()
    grandchild = child.child()
    assert grandchild.ancestry() == [parent.id, child.id, grandchild.id]


def test_a_record_with_no_manifest_folds(con, base_uri):
    """A store that declares no schema resolves to an empty map, not a crash (§5.6).

    `Schema()` is "no manifest yet", which is what a record reads before
    anything has been written to it. The map's flag columns are structs with a
    field per declared dim (§9.1) and DuckDB has no empty struct, so this is
    the one path where there are none to declare.
    """
    record = Revision.create(con)
    assert record.store.schema.dims == ()

    inputs = record.node_cache.inputs
    assert "varies" in inputs.columns
    assert inputs.fetchall() == []
