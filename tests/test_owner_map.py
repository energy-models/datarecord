"""The fold, the cache and persistence (design doc §9.1, §8.2, §12)."""

import shutil
from pathlib import Path

import pytest

from datarecord import DataRecord
from datarecord.duck import layer_dir, node_dir
from tests.fixtures import export_network, tombstone, write_input


@pytest.fixture
def parent(con, base_uri, ac_dc):
    record = DataRecord.create(con)
    export_network(ac_dc, record, con)
    record.materialise()
    return record


def keys(record, con):
    df = record.node_cache.inputs.df()
    return {
        (str(r.component_type), r["name"], str(r.attribute)) for _, r in df.iterrows()
    }


def component_names(record):
    return set(record.node_cache.components.df()["name"])


def test_root_map_is_its_own_layer(con, parent):
    """Every key of a root's map points at the root itself."""
    om = parent.node_cache
    assert set(om.inputs.df()["layer_uuid"]) == {parent.id}
    assert set(om.components.df()["layer_uuid"]) == {parent.id}
    assert ("Generator", "Manchester Wind", "p_max_pu") in keys(parent, con)


def test_materialise_writes_the_map_to_the_node_cache(con, parent):
    """`materialise` writes the maps beside the layer, never into it (§8.2)."""
    assert Path(node_dir(parent.id), "owner_map", "inputs.parquet").exists()
    assert Path(node_dir(parent.id), "owner_map", "components.parquet").exists()
    # The layer directory stays exactly what was written to it, so a reader
    # that knows nothing about layering still sees a plain parquet store.
    assert not Path(layer_dir(parent.id), "owner_map").exists()


def test_last_writer_wins_per_key(con, parent):
    """A child's key overrides the parent's, others stay with the parent."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"component_type": "Generator", "name": "Manchester Wind", "value": 0.42}],
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
    before = {k for k in keys(parent, con) if k[1] == "Norway Gas"}
    assert len(before) > 1
    assert "Norway Gas" in component_names(parent)

    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["Norway Gas"])
    assert not {k for k in keys(child, con) if k[1] == "Norway Gas"}
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
        [{"component_type": "Generator", "name": "Manchester Wind", "value": 0.42}],
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
        [{"component_type": "Generator", "name": "Norway Gas", "value": 0.1}],
    )
    child.materialise()
    expected = keys(child, con)

    shutil.rmtree(Path(node_dir(child.id), "owner_map"))
    fresh = DataRecord.get(child.id, con)
    assert keys(fresh, con) == expected


def test_any_node_may_be_a_parent(con, parent):
    """A layer is write-once, so no node needs preparing to branch from (§8.2)."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"component_type": "Generator", "name": "Manchester Wind", "value": 0.42}],
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
    record = DataRecord.create(con)
    assert record.store.schema.dims == ()

    inputs = record.node_cache.inputs
    assert "varies" in inputs.columns
    assert inputs.fetchall() == []
