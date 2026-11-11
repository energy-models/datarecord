# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The fold, materialised revisions and persistence.

Notes
-----
- [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
- [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
"""

import json
from dataclasses import replace
from itertools import product
from pathlib import Path

import pandas as pd
import pytest

from datarecord import WorkingRecord, Workspace
from datarecord.duck import union_all_by_name
from datarecord.resolve.fold import read_dimension_owners
from datarecord.resolve.owner_map import _fold_map
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec
from datarecord.store.manifest import LayerFiles
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    broadcast,
    drop_entities,
    empty_child,
    empty_root,
    nested_tables,
    restate,
    schema,
    write_attribute,
    write_axis,
    write_component,
    write_connections,
    write_periods,
    write_scenarios,
)


@pytest.fixture
def parent(store):
    """A materialised root of three generators, each with two attributes."""
    layer = empty_root(store, schema())
    names = ["Manchester Wind", "Norway Wind", "Norway Gas"]
    write_component(layer, "Generator", [{"entity": n} for n in names])
    for attribute, value in (("p_max_pu", 0.5), ("marginal_cost", 10.0)):
        write_attribute(
            layer, attribute, [{"entity": n, "value": value} for n in names]
        )
    revision = layer.commit()
    revision.materialise()
    return revision


def test_union_all_by_name_folds_every_relation_by_name(store):
    """Every arm by *name*, so an arm lacking a column reads NULL there.

    Three arms, not two: a scan that re-reads `rels[0]` instead of advancing
    would give `1` twice, with no error to point at it. Filling by name is what
    lets a layer written before `bus`/`breakpoint` existed still resolve.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
    """
    con = store.workspace.con
    rels = [con.sql("SELECT 1 AS x, 'a' AS y"), con.sql("SELECT 2 AS x")]
    rels.append(con.sql("SELECT 'c' AS y, 3 AS x"))
    got = union_all_by_name(rels).fetchall()
    assert sorted(got, key=lambda r: r[0]) == [(1, "a"), (2, None), (3, "c")]


def keys(revision):
    """The owner map's keys: `(name, attribute)`, no type.

    The map is pruned in the fold (`fold_layer` anti-joins each keyed
    membership's removed labels), so a key whose component or multi-dim tuple was
    removed is already gone from it - the read needs no further gating.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    df = revision.record._fold.owner_map.df()
    return {(r["entity"], str(r.attribute)) for _, r in df.iterrows()}


def entity_names(revision):
    return set(revision.record.dimensions["entity"].df()["entity"])


def test_materialise_writes_the_map_under_resolved(store, parent):
    """`materialise` writes the maps under `resolved/`, not at the layer root.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    assert Path(store.resolved_dir(parent.id), "owner_map.parquet").exists()
    assert Path(store.dimension_owners_uri(parent.id)).exists()
    assert not Path(
        store.resolved_dir(parent.id), "owner_map", "entities.parquet"
    ).exists()
    # The resolved files share the record's directory but stays out of the layer's own
    # namespace, so a reader that knows nothing about layering still sees a
    # plain parquet directory: every glob into a layer is single-level, so nothing
    # under `resolved/` is reachable by one (https://energy-models.github.io/datarecord/reference/layers/#removing-labels).
    assert not Path(store.layer_dir(parent.id), "owner_map").exists()
    # The globs the fold and `Record.at` actually use must not reach a
    # resolved file.
    layer = Path(store.layer_dir(parent.id))
    reachable = {
        p
        for pattern in (
            "*.parquet",
            "attributes/*.parquet",
            "dimensions/*.parquet",
            "dimensions/*/*.parquet",
        )
        for p in layer.glob(pattern)
    }
    assert reachable
    assert not any("resolved" in p.parts for p in reachable)


def _axes(store, revision):
    """`revision`'s resolved axes and dead labels, read through a workspace of its own.

    A fresh workspace holds no fold of an earlier read, so it reads whatever
    resolved files are on disk now.
    """
    workspace = Workspace()
    try:
        record = workspace.store(store.uri).revision(revision.id).record
        dims = record.dimensions
        dead = record._fold.axes.dead
        return (
            {d: dims[d].df().to_dict("records") for d in dims},
            {d: sorted(rel.fetchall()) for d, rel in dead.items()},
        )
    finally:
        workspace.close()


@pytest.mark.parametrize(
    ("materialised", "dropped"),
    [((), []), ((), ["gas"]), (("root",), ["gas"]), (("middle",), [])],
    ids=[
        "leaf-only-nothing-dead",
        "leaf-only-gas-dead",
        "root-gas-dead",
        "middle-nothing-dead",
    ],
)
def test_a_materialised_node_points_at_the_layers_holding_its_axes(
    store, materialised, dropped
):
    """A materialised node names each axis's layer and whether it has dead labels, and reads back as the full fold.

    `scenario` and `connection` are restated only in the root, so a leaf materialised
    from a materialised node above it carries their owners forward. `gas`, added in
    the middle, is dropped in the leaf where `dropped` says so.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    root_layer = empty_root(store, schema())
    write_component(root_layer, "Generator", [{"entity": "wind"}])
    write_axis(root_layer, "bus", [{"bus": "north"}])
    write_connections(root_layer, [{"entity": "wind", "bus": "north"}])
    write_scenarios(root_layer, [{"scenario": "low"}, {"scenario": "high"}])
    root = root_layer.commit()
    middle_layer = empty_child(root)
    write_component(middle_layer, "Generator", [{"entity": "gas"}])
    middle = middle_layer.commit()
    leaf_layer = empty_child(middle)
    write_axis(leaf_layer, "bus", [{"bus": "south"}])
    if dropped:
        drop_entities(leaf_layer, dropped)
    leaf = leaf_layer.commit()
    reader = empty_child(leaf).commit()
    full = _axes(store, reader)

    nodes = {"root": root, "middle": middle}
    for name in materialised:
        store.revision(nodes[name].id).materialise()
    store.revision(leaf.id).materialise()

    owners = read_dimension_owners(leaf.id, store.workspace.con, store)
    assert {d: layer for d, layer, _ in owners} == {
        "entity": middle.id if not dropped else leaf.id,
        "bus": leaf.id,
        "connection": root.id,
        "scenario": root.id,
    }, "each axis is owned by the deepest layer restating it; `ctype` by none"
    assert {d for d, _, has_dead in owners if has_dead} == set(full[1]), (
        "`has_dead` marks exactly the dims the full fold finds dead labels for"
    )
    assert _axes(store, reader) == full, "materialising changes no answer"
    assert not list(Path(store.uri, "layers").glob("*/resolved/dims")), (
        "no axis is copied under `resolved/`"
    )
    assert bool(list(Path(store.uri, "layers").glob("*/resolved/dead/*"))) == bool(
        dropped
    ), "dead label files are written only where a dim has dead labels"


def test_removing_a_component_removes_all_its_attributes(ac_dc_root):
    """Removing a component removes every attribute and the component row of it.

    Only on the branch that carries it, and through to a built model.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    parent = ac_dc_root
    before = {k for k in keys(parent) if k[0] == "Norway Gas"}
    assert len(before) > 1
    assert "Norway Gas" in entity_names(parent)

    child_layer = empty_child(parent)
    drop_entities(child_layer, ["Norway Gas"])
    child = child_layer.commit()
    sibling = empty_child(parent).commit()
    assert not {k for k in keys(child) if k[0] == "Norway Gas"}
    assert "Norway Gas" not in entity_names(child)
    assert "Norway Gas" in entity_names(sibling), (
        "a removal only affects the branch that carries it"
    )

    n = PyPSA.build(child.record)
    assert "Norway Gas" not in n.c["Generator"].static.index
    assert "Norway Wind" in n.c["Generator"].static.index


def test_removed_resolved_files_fall_back_to_the_fold(store, parent):
    """Materialising is an optimisation, so losing the resolved files costs work rather than answers.

    The old model had to raise here: a closed node's ancestry was truncated to
    itself, so re-folding would have silently dropped every ancestor. Gating on
    the resolved files' presence instead means the untruncated path is still available.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    child_layer = empty_child(parent)
    write_attribute(child_layer, "p_min_pu", [{"entity": "Norway Gas", "value": 0.1}])
    child = child_layer.commit()
    child.materialise()
    expected = keys(child)

    Path(store.resolved_dir(child.id), "owner_map.parquet").unlink()
    fresh = store.revision(child.id)
    assert keys(fresh) == expected


def test_a_record_with_no_manifest_folds(store):
    """A record that declares no schema resolves to an empty map, not a crash.

    `Schema()` is "no manifest yet", which is what a record reads before
    anything has been written to it. The map's flag columns are structs with a
    field per declared dim and DuckDB has no empty struct, so this is
    the one path where there are none to declare.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/reference/schema/#one-schema-per-record)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    revision = empty_root(store, Schema()).commit()
    assert revision.record.schema.dimensions == {}

    owner_map = revision.record._fold.owner_map
    assert "varies" in owner_map.columns
    assert owner_map.fetchall() == []


def test_a_read_refuses_an_owner_map_naming_a_file_the_layer_lacks(store, tmp_path):
    """A materialised owner map outliving its layer's attribute file is a corrupt store.

    The manifest is rewritten without `p_nom` after the node materialised, so
    the read finds an owner with nothing to own rather than an empty attribute.
    """
    layer = empty_root(store, schema())
    write_component(layer, "Generator", [{"entity": "gas"}])
    write_attribute(layer, "p_nom", [{"entity": "gas", "value": 1.0}])
    root = layer.commit()
    root.materialise()
    manifest = root.manifest
    Path(store.manifest_uri(root.id)).unlink()
    stripped = replace(manifest, files=LayerFiles(dimensions=manifest.files.dimensions))
    stripped.write(store.files, store.manifest_uri(root.id))

    reread = Workspace().store(str(tmp_path)).revision(root.id)
    with pytest.raises(ValueError, match="holds no file of"):
        reread.record.attributes["p_nom"]


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
    - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
    """
    names, snapshots = ["wind", "solar"], 200
    base = empty_root(
        store,
        Schema(
            dimensions={
                "entity": {"dtype": "str"},
                "snapshot": {"dtype": "int"},
            },
            properties={"entity": {"ctype": {"dtype": "str"}}},
            attributes={"p_max_pu": {"dtype": "float", "dims": ["entity", "snapshot"]}},
            partial=frozenset({"entity", "snapshot"}),
        ),
    ).commit()
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
    revision = base.store.commit(staged)

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
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    base = empty_root(
        store,
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
    ).commit()
    staged = WorkingRecord(base.record)
    staged.add("entity", ["wind", "solar"], ctype="Generator")
    staged.add("snapshot", [0, 1])
    staged.set("weight", 1.0)
    staged.set("p_max_pu", 0.5, entity="wind")
    root = base.store.commit(staged)
    staged = WorkingRecord(root.record)
    staged.set("weight", 2.0, snapshot=1)
    staged.set("p_max_pu", 0.7, snapshot=1)
    record = root.store.commit(staged).record

    for plan in (
        _fold_map(record.sources).explain().upper(),
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
        properties={"entity": {"ctype": {"refers": "ctype"}}},
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
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
    """
    base = empty_root(store, entity_series_schema).commit()
    w = WorkingRecord(base.record)
    w.add("entity", ["wind", "gas", "house"], ctype=["Generator", "Generator", "Load"])
    w.add("snapshot", [0, 1, 2, 3, 4, 5])
    w.set("p_nom", 100.0)
    w.set("p_nom", 50.0, entity="gas")
    w.set("p_max_pu", 0.9)
    w.set("p_max_pu", [0.2, 0.7, 0.4, 0.6], entity="wind", snapshot=[0, 1, 2, 3])

    for record in (w, base.store.commit(w).record):
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
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
    """
    base = empty_root(
        store,
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
    ).commit()
    root = WorkingRecord(base.record)
    root.add("entity", ["wind", "gas"])
    root.add("scenario", ["low", "high"])
    root.set("p_nom", 100.0, entity="wind")
    root.set("p_nom", 50.0, entity="gas")
    staged = WorkingRecord(root)
    staged.set("p_nom", 999.0, scenario="high")
    committed = base.store.commit(root)
    child = WorkingRecord(committed.record)
    child.set("p_nom", 999.0, scenario="high")

    expected = {
        ("gas", "high"): 999.0,
        ("gas", "low"): 50.0,
        ("wind", "high"): 999.0,
        ("wind", "low"): 100.0,
    }
    for record in (staged, child, committed.store.commit(child).record):
        assert broadcast(record, "p_nom", "entity", "scenario") == expected


def test_a_deeper_entity_wins_over_a_series_it_crosses(store, entity_series_schema):
    """A deeper row crossing a parent's series on a dim outside `partial` is written along it.

    The root sets a series for every entity, `(NULL, snapshot)`; the child sets
    `wind` for every snapshot, `(wind, NULL)`. Neither names a superset of the
    other's dims, so staging writes the child's value along the series'
    snapshots too, and the root's series reaches `gas` only.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    base = empty_root(store, entity_series_schema).commit()
    root = WorkingRecord(base.record)
    root.add("entity", ["wind", "gas"], ctype="Generator")
    root.add("snapshot", [0, 1, 2])
    root.set("p_max_pu", [0.2, 0.5, 0.8], snapshot=[0, 1, 2])
    staged = WorkingRecord(root)
    staged.set("p_max_pu", 0.9, entity="wind")
    committed = base.store.commit(root)
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
    for record in (staged, child, committed.store.commit(child).record):
        assert broadcast(record, "p_max_pu", "entity", "snapshot") == expected


def test_a_set_at_one_snapshot_leaving_a_dim_outside_partial_null_keeps_the_series(
    store,
):
    """A child naming one of two dims outside `partial` leaves the rest of the series.

    The root sets `x` at every `(period, snapshot)` of `a`; the child sets `a`
    at one `snapshot`, leaving `period` NULL.

    Notes
    -----
    - [a dim outside partial](https://energy-models.github.io/datarecord/reference/resolution/#a-dim-outside-partial)
    """
    base = empty_root(
        store,
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
    ).commit()
    root = WorkingRecord(base.record)
    root.add("entity", ["a"])
    root.add("period", [1, 2])
    root.add("snapshot", [0, 1])
    keys = pd.DataFrame(product([1, 2], [0, 1]), columns=["period", "snapshot"])
    root.set("x", keys.assign(entity="a", value=10.0 * keys.period + keys.snapshot))
    committed = base.store.commit(root)
    child = WorkingRecord(committed.record)
    child.set("x", 99.0, entity="a", snapshot=1)

    expected = {
        ("a", p, s): 99.0 if s == 1 else 10.0 * p + s
        for p, s in product([1, 2], [0, 1])
    }
    for record in (child, committed.store.commit(child).record):
        assert broadcast(record, "x", "entity", "period", "snapshot") == expected, (
            "the set reaches every period at snapshot=1, the root keeps the rest"
        )


_NARROW = schema(partial={"connection"})
_WIDE = schema()


def _scenario_root(store, *, materialised):
    """A root under `_NARROW`, `scenario` outside `partial`, writing `p_max_pu` along it."""
    layer = empty_root(store, _NARROW)
    write_component(layer, "Generator", [{"entity": "gas"}, {"entity": "wind"}])
    write_scenarios(layer, [{"scenario": "low"}, {"scenario": "high"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "gas", "scenario": "low", "value": 1.0},
            {"entity": "gas", "scenario": "high", "value": 2.0},
            {"entity": "wind", "value": 5.0},
        ],
    )
    return _committed(layer, materialised=materialised)


def _p_max_pu(revision):
    return broadcast(revision.record, "p_max_pu", "entity", "scenario")


@pytest.mark.parametrize("materialised", [False, True])
def test_a_widening_child_splits_its_parents_rows_by_their_labels(store, materialised):
    """`scenario` joins `partial` in a child writing `(gas, high) 3` over a root writing it along `scenario`.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    root = _scenario_root(store, materialised=materialised)
    before = _p_max_pu(root)
    layer = empty_child(root, _WIDE)
    write_attribute(
        layer, "p_max_pu", [{"entity": "gas", "scenario": "high", "value": 3.0}]
    )
    child = layer.commit()

    assert _p_max_pu(child) == {
        ("gas", "low"): 1.0,
        ("gas", "high"): 3.0,
        ("wind", "low"): 5.0,
        ("wind", "high"): 5.0,
    }, "the root owns (gas, low), the child (gas, high)"
    assert _p_max_pu(store.revision(root.id)) == before, (
        "the root reads as it did before its child widened the schema"
    )
    fresh = Workspace()
    try:
        again = fresh.store(store.uri)
        assert _p_max_pu(again.revision(root.id)) == before
        assert _p_max_pu(again.revision(child.id)) == _p_max_pu(child)
    finally:
        fresh.close()
    folds = nested_tables(store.workspace.con, "_fold")
    folded = [child] if materialised else [root, child]
    assert sorted(s for s, tables in folds.items() if "owner_map" in tables) == sorted(
        n.id.hex for n in folded
    ), "one fold table per node folded, whatever schema reads it"


def test_a_node_reads_the_same_whichever_descendant_widens(store):
    """A middle node reads alike before and after a leaf brings `scenario` into `partial`.

    The middle crosses the root on `(entity, period)`, so it owns overlap rows,
    and the root writes `solar` both along `scenario` and NULL there.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [crossing rows](https://energy-models.github.io/datarecord/reference/resolution/#crossing-rows)
    """
    narrow = schema(partial={"period", "connection"})
    layer = empty_root(store, narrow)
    write_component(layer, "Generator", [{"entity": e} for e in ("gas", "solar")])
    write_periods(layer, [{"period": 2030}, {"period": 2040}])
    write_scenarios(layer, [{"scenario": "low"}, {"scenario": "high"}])
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "gas", "scenario": "low", "value": 1.0},
            {"entity": "gas", "scenario": "high", "value": 2.0},
            {"entity": "solar", "value": 4.0},
            {"entity": "solar", "scenario": "high", "value": 6.0},
        ],
    )
    layer = empty_child(layer.commit())
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"period": 2030, "scenario": "low", "value": 10.0},
            {"period": 2030, "scenario": "high", "value": 20.0},
        ],
    )
    middle = layer.commit()
    middle.materialise()

    def read(revision):
        return broadcast(revision.record, "p_max_pu", "entity", "period", "scenario")

    before = read(middle)
    assert before[("gas", 2040, "low")] == 1.0
    assert before[("solar", 2040, "low")] == 4.0
    assert before[("solar", 2030, "high")] == 20.0
    leaf = empty_child(middle, schema(partial={"period", "scenario", "connection"}))
    assert read(leaf.commit()) == before, (
        "a widening that writes nothing changes no value"
    )
    assert read(store.revision(middle.id)) == before


@pytest.mark.parametrize(
    ("partial", "materialised"), [(set(), False), ({"scenario"}, True)]
)
def test_an_attribute_gaining_a_dim_reads_its_rows_broadcast_along_it(
    store, partial, materialised
):
    """`q` over `entity` gains `scenario`; its row from before reads NULL there, a broadcast.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """

    def q_over(*dims):
        q = AttributeSpec(dtype="float", dims=set(dims))
        return schema(partial=partial, attributes={"Generator": {"q": q}})

    layer = empty_root(store, q_over("entity"))
    write_component(layer, "Generator", [{"entity": "gas"}])
    write_scenarios(layer, [{"scenario": "low"}, {"scenario": "high"}])
    write_attribute(layer, "q", [{"entity": "gas", "value": 1.0}])
    root = layer.commit()
    if materialised:
        root.materialise()
    child = empty_child(root, q_over("entity", "scenario")).commit()

    rows = child.record.attributes["q"].project("entity, scenario, value").fetchall()
    assert rows == [("gas", None, 1.0)], "the row from before, NULL along scenario"
    flags = child.record.flags()["q"]
    assert flags.broadcast == {"scenario"}, "it broadcasts along scenario"
    assert flags.varies == {"entity"}
    assert store.revision(root.id).record.flags()["q"].broadcast == set(), (
        "the root, under its own schema, has q over entity alone"
    )


@pytest.mark.parametrize(
    ("remover", "materialised"),
    [("parent", False), ("parent", True), ("widening child", False)],
)
def test_a_label_removed_along_a_dim_joining_partial_names_no_row(
    store, remover, materialised
):
    """`low` removed while `scenario` is outside `partial`, or by the layer bringing it in, stays gone.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    node = _scenario_root(store, materialised=False)
    if remover == "parent":
        layer = empty_child(node)
        restate(layer, "dimensions", "scenario", [], drop=["low"])
        node = layer.commit()
    if materialised:
        node.materialise()
    layer = empty_child(node, _WIDE)
    if remover == "widening child":
        restate(layer, "dimensions", "scenario", [], drop=["low"])
    child = layer.commit()

    rows = child.record.attributes["p_max_pu"].project("entity, scenario").fetchall()
    assert sorted(rows, key=str) == [("gas", "high"), ("wind", None)], (
        "no row names the removed label"
    )
    assert child.record.dimensions["scenario"].fetchall() == [("high",)]


def _committed(layer, *, materialised):
    revision = layer.commit()
    if materialised:
        revision.materialise()
    return revision


def _restated_then_widened(store):
    """A child restates `(gas, high)` along `scenario` outside `partial`; a leaf widens."""
    layer = empty_child(_scenario_root(store, materialised=False))
    write_attribute(
        layer, "p_max_pu", [{"entity": "gas", "scenario": "high", "value": 3.0}]
    )
    leaf = empty_child(layer.commit(), _WIDE).commit()
    expected = {("gas", "high"): 3.0, ("wind", "low"): 5.0, ("wind", "high"): 5.0}
    return _p_max_pu(leaf), expected


def _removed_widened_re_added(store):
    """`low` is removed outside `partial`, `scenario` joins it, then `low` is re-added."""
    layer = empty_child(_scenario_root(store, materialised=False))
    restate(layer, "dimensions", "scenario", [], drop=["low"])
    widening = empty_child(layer.commit(), _WIDE)
    layer = empty_child(widening.commit())
    restate(layer, "dimensions", "scenario", [{"scenario": "low"}])
    expected = {("gas", "high"): 2.0, ("wind", "low"): 5.0, ("wind", "high"): 5.0}
    return _p_max_pu(layer.commit()), expected


def _two_dims_joining(store):
    """`period` and `scenario` join `partial` in one child writing `(gas, 2040, high)`."""
    layer = empty_root(store, _NARROW)
    write_component(layer, "Generator", [{"entity": "gas"}])
    write_periods(layer, [{"period": 2030}, {"period": 2040}])
    write_scenarios(layer, [{"scenario": "low"}, {"scenario": "high"}])
    keys = list(product([2030, 2040], ["low", "high"]))
    write_attribute(
        layer,
        "p_max_pu",
        [
            {"entity": "gas", "period": p, "scenario": s, "value": float(i)}
            for i, (p, s) in enumerate(keys)
        ],
    )
    wide = schema(partial={"period", "scenario", "connection"})
    layer = empty_child(layer.commit(), wide)
    write_attribute(
        layer,
        "p_max_pu",
        [{"entity": "gas", "period": 2040, "scenario": "high", "value": 9.0}],
    )
    expected = {("gas", p, s): float(i) for i, (p, s) in enumerate(keys)}
    expected["gas", 2040, "high"] = 9.0
    record = layer.commit().record
    return broadcast(record, "p_max_pu", "entity", "period", "scenario"), expected


def _connection_joining(store):
    """The multi-dim `connection` joins `partial` in a child writing `(gas, south)`."""
    layer = empty_root(store, schema(partial=set()))
    write_component(layer, "Generator", [{"entity": "gas"}])
    write_axis(layer, "bus", [{"bus": "north"}, {"bus": "south"}])
    write_connections(layer, [{"entity": "gas", "bus": b} for b in ("north", "south")])
    write_attribute(
        layer,
        "efficiency",
        [
            {"entity": "gas", "bus": "north", "value": 0.4},
            {"entity": "gas", "bus": "south", "value": 0.5},
        ],
    )
    layer = empty_child(layer.commit(), _WIDE)
    write_attribute(
        layer, "efficiency", [{"entity": "gas", "bus": "south", "value": 0.9}]
    )
    expected = {(("gas", "north"),): 0.4, (("gas", "south"),): 0.9}
    return broadcast(layer.commit().record, "efficiency", "connection"), expected


@pytest.mark.parametrize(
    "build",
    [
        _restated_then_widened,
        _removed_widened_re_added,
        _two_dims_joining,
        _connection_joining,
    ],
)
def test_a_widening_of_partial_reads_what_the_narrower_layers_wrote(store, build):
    """Each shape of `partial` widening under layers written narrower reads as `build` says.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    read, expected = build(store)
    assert read == expected, build.__doc__


def test_a_layer_leaving_a_dim_outside_partial_unfilled_is_refused(store):
    """A child writing `period=2030` leaves `scenario` NULL over the root's `(gas, low)`.

    Notes
    -----
    - [a dim outside partial](https://energy-models.github.io/datarecord/reference/resolution/#a-dim-outside-partial)
    """
    layer = empty_root(store, schema(partial={"period", "connection"}))
    write_component(layer, "Generator", [{"entity": "gas"}])
    write_periods(layer, [{"period": 2030}])
    write_scenarios(layer, [{"scenario": "low"}])
    write_attribute(
        layer, "p_max_pu", [{"entity": "gas", "scenario": "low", "value": 1.0}]
    )
    layer = empty_child(layer.commit())
    write_attribute(layer, "p_max_pu", [{"period": 2030, "value": 2.0}])
    child = layer.commit()

    with pytest.raises(
        ValueError,
        match=r"leaves 'scenario' NULL for 'p_max_pu' at \{'entity': 'gas', "
        r"'period': '2030'\} where a parent row names it",
    ):
        child.record.attributes["p_max_pu"].fetchall()
