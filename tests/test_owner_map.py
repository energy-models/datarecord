# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The fold, the cache and persistence.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
- [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

import json
from itertools import product
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from datarecord import WorkingRecord, Workspace
from datarecord.duck import union_all_by_name
from datarecord.resolve.fold import read_dim_owners
from datarecord.resolve.owner_map import _fold_map
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import Schema
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    broadcast,
    drop_entities,
    empty_child,
    empty_root,
    export_network,
    schema,
    write_attribute,
    write_axis,
    write_component,
    write_connections,
    write_scenarios,
    write_schema,
)


@pytest.fixture
def parent(store):
    """A materialised root of three generators, each with two attributes."""
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(schema(), store)
    names = ["Manchester Wind", "Norway Wind", "Norway Gas"]
    write_component(revision, "Generator", [{"entity": n} for n in names])
    for attribute, value in (("p_max_pu", 0.5), ("marginal_cost", 10.0)):
        write_attribute(
            layer, attribute, [{"entity": n, "value": value} for n in names]
        )
    revision.materialise()
    return revision


def test_union_all_by_name_folds_every_relation(store):
    """Three arms, not two: the fold's union binds `u`/`rel` by name.

    Two relations pass whatever the loop body does, since the first pairing is
    the only one. Three is what catches the failure mode the helper's variables
    invite - a scan that re-reads `rels[0]` instead of advancing would give
    `[1, 1]` here, with no error to point at it.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    """
    rels = [store.workspace.con.sql(f"SELECT {i} AS x") for i in (1, 2, 3)]
    got = union_all_by_name(rels, store.workspace.con).fetchall()
    assert sorted(v for (v,) in got) == [1, 2, 3]


def test_union_all_by_name_fills_a_missing_column_with_null(store):
    """By *name*, so an arm lacking a column reads NULL there.

    What lets a layer written before `bus`/`breakpoint` existed still resolve,
    and a persisted owner map survive a newly declared dim.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    rels = [
        store.workspace.con.sql("SELECT 1 AS x, 'a' AS y"),
        store.workspace.con.sql("SELECT 2 AS x"),
    ]
    got = union_all_by_name(rels, store.workspace.con).fetchall()
    assert sorted(got, key=lambda r: r[0]) == [(1, "a"), (2, None)]


def keys(revision):
    """The owner map's keys: `(name, attribute)`, no type.

    The map is pruned in the fold (`fold_attributes` anti-joins each keyed
    membership's removed labels), so a key whose component or multi-dim tuple was
    removed is already gone from it - the read needs no further gating.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    df = revision.record._fold.owner_map.df()
    return {(r["entity"], str(r.attribute)) for _, r in df.iterrows()}


def entity_names(revision):
    return set(revision.record.dims["entity"].df()["entity"])


def test_materialise_writes_the_map_under_resolved(store, parent):
    """`materialise` writes the maps under `resolved/`, not at the layer root.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    assert Path(store.resolved_dir(parent.id), "owner_map.parquet").exists()
    assert Path(store.dim_owners_uri(parent.id)).exists()
    assert not Path(
        store.resolved_dir(parent.id), "owner_map", "entities.parquet"
    ).exists()
    # The cache shares the record's directory but stays out of the layer's own
    # namespace, so a reader that knows nothing about layering still sees a
    # plain parquet directory: every glob into a layer is single-level, so nothing
    # under `resolved/` is reachable by one (https://energy-models.github.io/datarecord/design/layers/removal/).
    assert not Path(store.layer_dir(parent.id), "owner_map").exists()
    # The globs the fold and `Record.at` actually use must not reach a
    # cached file.
    layer = Path(store.layer_dir(parent.id))
    reachable = {
        p
        for pattern in (
            "*.parquet",
            "attributes/*.parquet",
            "dims/*.parquet",
            "dims/*/*.parquet",
        )
        for p in layer.glob(pattern)
    }
    assert reachable
    assert not any("resolved" in p.parts for p in reachable)


def _coords(store, revision):
    """`revision`'s resolved axes and dead labels, read through a workspace of its own.

    A fresh workspace holds no fold of an earlier read, so it reads whatever
    caches are on disk now.
    """
    workspace = Workspace()
    try:
        record = workspace.store(store.uri).revision(revision.id).record
        dims = record.dims
        dead = record._fold.dead
        return (
            {d: dims[d].df().to_dict("records") for d in dims},
            {d: sorted(rel.fetchall()) for d, rel in dead.items()},
        )
    finally:
        workspace.close()


@pytest.mark.parametrize("dropped", [[], ["gas"]], ids=["nothing-dead", "gas-dead"])
@pytest.mark.parametrize(
    "cached",
    [(), ("root",), ("middle",), ("root", "middle")],
    ids=["leaf-only", "root", "middle", "root-and-middle"],
)
def test_a_node_cache_points_at_the_layers_holding_its_axes(store, cached, dropped):
    """A node cache names each axis's layer and whether it has dead labels, and reads back as the full fold.

    `scenario` and `connection` are restated only in the root, so a leaf cache
    seeded from a cache above it carries their owners forward. `gas`, added in
    the middle, is dropped in the leaf where `dropped` says so.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    write_schema(schema(), store)
    root = empty_root(store)
    write_component(root, "Generator", [{"entity": "wind"}])
    write_axis(root, "bus", [{"bus": "north"}])
    write_connections(root, [{"entity": "wind", "bus": "north"}])
    write_scenarios(root, [{"scenario": "low"}, {"scenario": "high"}])
    middle = empty_child(root)
    write_component(middle, "Generator", [{"entity": "gas"}])
    leaf = empty_child(middle)
    write_axis(leaf, "bus", [{"bus": "south"}])
    if dropped:
        drop_entities(leaf, dropped)
    reader = empty_child(leaf)
    full = _coords(store, reader)

    nodes = {"root": root, "middle": middle}
    for name in cached:
        store.revision(nodes[name].id).materialise()
    store.revision(leaf.id).materialise()

    owners = read_dim_owners(leaf.id, store.workspace.con, store)
    assert {d: layer for d, layer, _ in owners} == {
        "entity": middle.id if not dropped else leaf.id,
        "bus": leaf.id,
        "connection": root.id,
        "scenario": root.id,
    }, "each axis is owned by the deepest layer restating it; `ctype` by none"
    assert {d for d, _, has_dead in owners if has_dead} == set(full[1]), (
        "`has_dead` marks exactly the dims the full fold finds dead labels for"
    )
    assert _coords(store, reader) == full, "the cache changes no answer"
    assert not list(Path(store.uri, "layers").glob("*/resolved/dims")), (
        "no axis is copied into a cache"
    )
    assert bool(list(Path(store.uri, "layers").glob("*/resolved/dead/*"))) == bool(
        dropped
    ), "dead label files are written only where a dim has dead labels"


def test_last_writer_wins_per_key(store, parent):
    """A child's key overrides the parent's, others stay with the parent.

    Every key of a root's map points at the root itself. The entity axis has no
    `layer_uuid`: it folds like an axis, its winning row the whole row in one
    file.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    """
    record = parent.record
    assert set(record._fold.owner_map.df()["layer_uuid"]) == {parent.id}
    assert "layer_uuid" not in record._coords.axes["entity"].columns
    assert ("Manchester Wind", "p_max_pu") in keys(parent)

    child = empty_child(parent)
    write_attribute(
        store.layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )
    df = child.record._fold.owner_map.df()

    def owner_of(name, attr):
        sel = df[(df["entity"] == name) & (df["attribute"].astype(str) == attr)]
        return sel["layer_uuid"].iloc[0]

    assert owner_of("Manchester Wind", "p_max_pu") == child.id
    assert owner_of("Manchester Wind", "marginal_cost") == parent.id
    assert owner_of("Norway Wind", "p_max_pu") == parent.id


def test_removing_a_component_removes_all_its_attributes(store, ac_dc):
    """Removing a component removes every attribute and the component row of it.

    Only on the branch that carries it, and through to a built model.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    parent = export_network(ac_dc, store)
    parent.materialise()
    before = {k for k in keys(parent) if k[0] == "Norway Gas"}
    assert len(before) > 1
    assert "Norway Gas" in entity_names(parent)

    child = empty_child(parent)
    drop_entities(child, ["Norway Gas"])
    sibling = empty_child(parent)
    assert not {k for k in keys(child) if k[0] == "Norway Gas"}
    assert "Norway Gas" not in entity_names(child)
    assert "Norway Gas" in entity_names(sibling), (
        "a removal only affects the branch that carries it"
    )

    n = PyPSA.build(child.record)
    assert "Norway Gas" not in n.c["Generator"].static.index
    assert "Norway Wind" in n.c["Generator"].static.index


def test_a_removed_cache_falls_back_to_the_fold(store, parent):
    """A cache is an optimisation, so losing it costs work rather than answers.

    The old model had to raise here: a closed node's ancestry was truncated to
    itself, so re-folding would have silently dropped every ancestor. Gating on
    the cache's presence instead means the untruncated path is still available.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    child = empty_child(parent)
    write_attribute(
        store.layer_dir(child.id),
        "p_min_pu",
        [{"entity": "Norway Gas", "value": 0.1}],
    )
    child.materialise()
    expected = keys(child)

    Path(store.resolved_dir(child.id), "owner_map.parquet").unlink()
    fresh = store.revision(child.id)
    assert keys(fresh) == expected


def test_any_node_may_be_a_parent(store, parent):
    """A layer is write-once, so no node needs preparing to branch from.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    """
    child = empty_child(parent)
    write_attribute(
        store.layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Wind", "value": 0.42}],
    )
    grandchild = empty_child(child)
    # The child was never materialised, yet its layer resolves for the
    # grandchild all the same.
    df = grandchild.record._fold.owner_map.df()
    row = df[(df["entity"] == "Manchester Wind") & (df["attribute"] == "p_max_pu")]
    assert set(row["layer_uuid"]) == {child.id}


def test_a_record_with_no_manifest_folds(store):
    """A record that declares no schema resolves to an empty map, not a crash.

    `Schema()` is "no manifest yet", which is what a record reads before
    anything has been written to it. The map's flag columns are structs with a
    field per declared dim and DuckDB has no empty struct, so this is
    the one path where there are none to declare.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    revision = empty_root(store)
    assert revision.record.schema.dims == ()

    owner_map = revision.record._fold.owner_map
    assert "varies" in owner_map.columns
    assert owner_map.fetchall() == []


def test_a_parquet_layer_locates_a_layers_files(store):
    """The fold names files and the source says where they are.

    `Store.layer_dir` is what a `ParquetLayer` derives from, so this pins the
    seam rather than the layout: a reader asks for `attributes/p_nom.parquet` and
    never builds the path itself.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
    """
    revision_id = uuid4()
    source = ParquetLayer(revision_id, Schema(), store)
    assert isinstance(source, LayerSource), (
        "structural, so no import is needed to be one"
    )

    assert source.uri() == store.layer_dir(revision_id), "empty is the layer root"
    assert source.uri("attributes/p_nom.parquet") == (
        store.layer_dir(revision_id) + "attributes/p_nom.parquet"
    )
    # A glob is a path like any other: the source neither parses nor validates.
    assert source.uri("attributes/*.parquet").endswith("attributes/*.parquet")


def _largest_intermediate(store, rel):
    """The most rows any operator produced while DuckDB executed `rel`."""
    profile = json.loads(
        store.workspace.con.sql("EXPLAIN (ANALYZE, FORMAT JSON) FROM rel").fetchall()[
            0
        ][1]
    )

    def largest(node):
        return max(
            [node.get("operator_cardinality") or 0]
            + [largest(child) for child in node.get("children", [])]
        )

    return largest(profile)


def test_a_read_joins_no_more_rows_than_the_layers_hold(store):
    """The owner-map join is keyed on a partial dim, not filtered on it afterwards.

    The join matched a partial dim with `raw IS NULL OR raw IS NOT DISTINCT FROM
    owned`, which DuckDB cannot hash. It joined on `entity` and `layer_uuid`
    only and filtered on `snapshot` after the join, so one layer of `T`
    snapshots per entity produced `T * T` rows per entity before the filter,
    and a read slowed with the square of the series length.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    """
    write_schema(
        Schema(
            dimensions={
                "entity": {"dtype": "str"},
                "snapshot": {"dtype": "int"},
            },
            properties={"entity": {"ctype": {"dtype": "str"}}},
            attributes={"p_max_pu": {"dtype": "float", "dims": ["entity", "snapshot"]}},
            partial=frozenset({"entity", "snapshot"}),
        ),
        store,
    )
    names, snapshots = ["wind", "solar"], 200
    base = empty_root(store)
    staged = WorkingRecord(base.record)
    staged.add("entity", names, ctype="Generator")
    staged.add("snapshot", list(range(snapshots)))
    staged.set(
        "p_max_pu",
        pd.DataFrame(
            {
                "entity": [n for n in names for _ in range(snapshots)],
                "snapshot": list(range(snapshots)) * len(names),
                "value": 0.5,
            }
        ),
    )
    revision = base.child(staged.edits)

    rel = revision.record.attributes["p_max_pu"]
    stored = len(names) * snapshots
    assert rel.count("*").fetchall() == [(stored,)], "one row per stored value"
    assert _largest_intermediate(store, rel) <= stored, (
        "no operator produces more rows than the layer holds"
    )


def test_the_fold_crosses_and_the_read_broadcasts_by_hash_join(store):
    """The fold pairs crossing rows, and the read finds its owned keys, by hash join.

    A join matching on `dim IS NULL` alone runs as a nested loop comparing
    every row with every other - even a row that sets the dim and so matches
    none. Row counts stay right, so only the plan shows it.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    write_schema(
        Schema(
            dimensions={
                "entity": {"dtype": "str"},
                "snapshot": {"dtype": "int"},
            },
            properties={"entity": {"ctype": {"dtype": "str"}}},
            attributes={
                "p_max_pu": {"dtype": "float", "dims": ["entity", "snapshot"]},
                "weight": {"dtype": "float", "dims": ["snapshot"]},
            },
            partial=frozenset({"entity", "snapshot"}),
        ),
        store,
    )
    base = empty_root(store)
    staged = WorkingRecord(base.record)
    staged.add("entity", ["wind", "solar"], ctype="Generator")
    staged.add("snapshot", [0, 1])
    staged.set("weight", 1.0)
    staged.set("p_max_pu", 0.5, entity="wind")
    root = base.child(staged.edits)
    staged = WorkingRecord(root.record)
    staged.set("weight", 2.0, snapshot=1)
    staged.set("p_max_pu", 0.7, snapshot=1)
    record = root.child(staged.edits).record

    for plan in (
        _fold_map(record.sources, record.con, record.schema).explain().upper(),
        record.attributes["weight"].explain().upper(),
    ):
        assert "HASH JOIN" in plan, "it joins at all"
        assert "NL JOIN" not in plan and "NESTED LOOP" not in plan, plan
    assert _rows(record._fold.owner_map, "attribute", "entity", "snapshot") == {
        ("weight", None, None),
        ("weight", None, 1),
        ("p_max_pu", "wind", None),
        ("p_max_pu", None, 1),
        ("p_max_pu", "wind", 1),
    }, "crossed rows stay as written beside the overlap naming both"
    assert broadcast(record, "weight", "snapshot") == {
        (0,): 1.0,
        (1,): 2.0,
    }, "the deeper, more specific row owns the key both cover"
    assert broadcast(record, "p_max_pu", "entity", "snapshot") == {
        ("wind", 0): 0.5,
        ("wind", 1): 0.7,
        ("solar", 1): 0.7,
    }


def _rows(rel, *columns):
    """`rel`'s rows over `columns`, NULL as None, as a set."""
    df = rel.project(", ".join(columns)).df()
    return {
        tuple(None if pd.isna(v) else v for v in r)
        for r in df.itertuples(index=False, name=None)
    }


@pytest.fixture
def entity_series_schema():
    """`p_nom` over `entity` and `p_max_pu` over `(entity, snapshot)`, `entity` alone partial."""
    return Schema(
        dimensions={
            "entity": {"dtype": "str"},
            "ctype": {"one_of": ["Generator", "Load"]},
            "snapshot": {"dtype": "int"},
        },
        properties={"entity": {"ctype": {"one_of": "ctype"}}},
        attributes={
            "p_nom": {"dtype": "float", "dims": ["entity"], "default": 0.0},
            "p_max_pu": {
                "dtype": "float",
                "dims": ["entity", "snapshot"],
                "default": 1.0,
            },
        },
        partial=frozenset({"entity"}),
    )


def test_a_broadcast_reaches_every_label_a_named_row_leaves(
    store, entity_series_schema
):
    """A broadcast reads as written and reaches every label no named row takes.

    `p_nom`'s broadcast reaches every entity but `gas`, which names its own
    value. `p_max_pu`'s reaches every entity and snapshot but those `wind`'s
    series names, which a consumer lays over it.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    write_schema(entity_series_schema, store)
    base = empty_root(store)
    w = WorkingRecord(base.record)
    w.add("entity", ["wind", "gas", "house"], ctype=["Generator", "Generator", "Load"])
    w.add("snapshot", [0, 1, 2, 3, 4, 5])
    w.set("p_nom", 100.0)
    w.set("p_nom", 50.0, entity="gas")
    w.set("p_max_pu", 0.9)
    w.set("p_max_pu", [0.2, 0.7, 0.4, 0.6], entity="wind", snapshot=[0, 1, 2, 3])

    for record in (w, base.child(w.edits).record):
        assert broadcast(record, "p_nom", "entity") == {
            ("wind",): 100.0,
            ("house",): 100.0,
            ("gas",): 50.0,
        }
        assert broadcast(record, "p_max_pu", "entity", "snapshot") == {
            **{(e, t): 0.9 for e in ("wind", "gas", "house") for t in range(6)},
            ("wind", 0): 0.2,
            ("wind", 1): 0.7,
            ("wind", 2): 0.4,
            ("wind", 3): 0.6,
        }
        flags = record.flags()
        assert (flags["p_nom"].varies, flags["p_nom"].broadcast) == (
            {"entity"},
            {"entity"},
        )
        assert (flags["p_max_pu"].varies, flags["p_max_pu"].broadcast) == (
            {"entity", "snapshot"},
            {"entity", "snapshot"},
        )


def test_a_deeper_broadcast_owns_its_scenario_across_named_entities(store):
    """Across layers the deeper row wins the keys it covers, named or not.

    The root names each entity with `scenario` NULL, the child names one
    scenario with `entity` NULL. The child owns `(*, high)`, the root keeps
    `(*, low)`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    write_schema(
        Schema(
            dimensions={"entity": {"dtype": "str"}, "scenario": {"dtype": "str"}},
            attributes={
                "p_nom": {
                    "dtype": "float",
                    "dims": ["entity", "scenario"],
                    "default": 0.0,
                }
            },
            partial=frozenset({"entity", "scenario"}),
        ),
        store,
    )
    base = empty_root(store)
    root = WorkingRecord(base.record)
    root.add("entity", ["wind", "gas"])
    root.add("scenario", ["low", "high"])
    root.set("p_nom", 100.0, entity="wind")
    root.set("p_nom", 50.0, entity="gas")
    staged = WorkingRecord(root)
    staged.set("p_nom", 999.0, scenario="high")
    committed = base.child(root.edits)
    child = WorkingRecord(committed.record)
    child.set("p_nom", 999.0, scenario="high")

    expected = {
        ("gas", "high"): 999.0,
        ("gas", "low"): 50.0,
        ("wind", "high"): 999.0,
        ("wind", "low"): 100.0,
    }
    for record in (staged, child, committed.child(child.edits).record):
        assert broadcast(record, "p_nom", "entity", "scenario") == expected


def test_a_deeper_entity_wins_over_a_series_it_crosses(store, entity_series_schema):
    """A deeper row crossing a parent's series on a dim outside `partial` is written along it.

    The root sets a series for every entity, `(NULL, snapshot)`; the child sets
    `wind` for every snapshot, `(wind, NULL)`. Neither names a superset of the
    other's dims, so staging writes the child's value along the series'
    snapshots too, and the root's series reaches `gas` only.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    write_schema(entity_series_schema, store)
    base = empty_root(store)
    root = WorkingRecord(base.record)
    root.add("entity", ["wind", "gas"], ctype="Generator")
    root.add("snapshot", [0, 1, 2])
    root.set("p_max_pu", [0.2, 0.5, 0.8], snapshot=[0, 1, 2])
    staged = WorkingRecord(root)
    staged.set("p_max_pu", 0.9, entity="wind")
    committed = base.child(root.edits)
    child = WorkingRecord(committed.record)
    child.set("p_max_pu", 0.9, entity="wind")

    expected = {
        ("wind", 0): 0.9,
        ("wind", 1): 0.9,
        ("wind", 2): 0.9,
        ("gas", 0): 0.2,
        ("gas", 1): 0.5,
        ("gas", 2): 0.8,
    }
    for record in (staged, child, committed.child(child.edits).record):
        assert broadcast(record, "p_max_pu", "entity", "snapshot") == expected


@pytest.mark.parametrize("materialised", [False, True])
@pytest.mark.parametrize(
    ("named", "null"), [("snapshot", "period"), ("period", "snapshot")]
)
def test_a_pointwise_set_leaving_a_dim_outside_partial_null_keeps_the_series(
    store, named, null, materialised
):
    """A child naming one of two dims outside `partial` leaves the rest of the series.

    The root sets `x` at every `(period, snapshot)` of `a`; the child sets `a`
    at one label of `named`, leaving `null` NULL.

    Notes
    -----
    - [a dim outside partial](https://energy-models.github.io/datarecord/design/layers/resolution/#a-dim-outside-partial)
    """
    write_schema(
        Schema(
            dimensions={
                "entity": {"dtype": "str"},
                "period": {"dtype": "int"},
                "snapshot": {"dtype": "int"},
            },
            attributes={
                "x": {"dtype": "float", "dims": ["entity", "period", "snapshot"]}
            },
            partial=frozenset({"entity"}),
        ),
        store,
    )
    base = empty_root(store)
    root = WorkingRecord(base.record)
    root.add("entity", ["a"])
    root.add("period", [1, 2])
    root.add("snapshot", [0, 1])
    for p, s in product([1, 2], [0, 1]):
        root.set("x", 10.0 * p + s, entity="a", period=p, snapshot=s)
    committed = base.child(root.edits)
    if materialised:
        committed.materialise()
    child = WorkingRecord(committed.record)
    child.set("x", 99.0, entity="a", **{named: 1})

    expected = {
        ("a", p, s): 99.0 if {"period": p, "snapshot": s}[named] == 1 else 10.0 * p + s
        for p, s in product([1, 2], [0, 1])
    }
    for record in (child, committed.child(child.edits).record):
        assert broadcast(record, "x", "entity", "period", "snapshot") == expected, (
            f"the set reaches every {null} at {named}=1, the root keeps the rest"
        )
