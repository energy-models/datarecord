<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 33: the fold is a fixed plan

| DEP      | 33                                                                   |
| -------- | -------------------------------------------------------------------- |
| Title    | the fold is a fixed plan                                             |
| Author   | Jonas Hörsch                                                         |
| Status   | Implemented (2026-11-03)                                             |
| Requires | [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md) |
| Created  | 2026-11-03                                                           |

The design landed in [resolution](../layers/resolution.md), [editing](../use/editing.md) and [writing](../use/writing.md), which are authoritative where they and this DEP disagree.

## Abstract

The [owner map](../layers/resolution.md#owner-map) is the table of which layer owns each key. The fold is the step that merges each layer's rows onto the owner map below it. This DEP makes the fold one query with fixed joins. It no longer loops over the sets of dims that rows name, because each row's set is now a value in a column. A `WorkingRecord` (a record with staged, uncommitted edits) then plans its relations without reading a staged row. So a relation built before an edit returns that edit. Where two staged edits cross, the later edit wins.

## Motivation

The [`partial`](../layers/tree.md#partial-the-granularity-of-an-override) dims are the dims a layer can override one label at a time. A row's named set is the set of `partial` dims on which its key has a label. Before this DEP, the fold loops over the named sets in a layer's rows and runs joins for each pair of them. So the plan of the fold depends on the data, and its joins grow with the named sets a deep ancestry accumulates. On a 16-layer PyPSA ancestry, the frozen fold runs 388 joins.

A `WorkingRecord` checks its staged rows in Python to decide which staging tables exist, which layers own an attribute and which axes are staged. So a relation built before an edit misses what the edit changes, and a view over that relation uses a stale plan.

Two staged edits cross where each names a dim that the other leaves NULL. Before this DEP, `set` refuses the second of two such edits. So the order of a user's clicks in an editor decides whether an edit succeeds.

## Specification

### Named sets as data

A row's named set is a bitmask over the `partial` dims, stored in a column of the row. The fold of one layer's owner-map rows onto the map below it runs three steps:

- To prune, the fold drops the parent rows that name a removed label, with one anti-join per `partial` dim.
- To cover, the fold copies each parent row once per subset of its named set, and tags each copy with the subset and its labels. One semi-join matches these copies to the layer's rows on attribute, subset and labels, with each layer row's own named set as its subset. One anti-join then drops every parent row that a layer row covers.
- To cross, the fold expands both sides to the named sets that the other side's row may have without either set containing the other. One equi-join keeps the pairs whose labels agree on the dims both rows name, and each pair gives an [overlap row](../layers/resolution.md#crossing-rows).

An attribute over at most one `partial` dim cannot cross, so its fold runs no crossing join. The schema alone shows which attributes these are, so the fold reads no rows to decide it.

The query computes each row's candidate masks, the named sets the row may meet, from the row's own mask and its attribute's domain. The domain is the set of `partial` dims the attribute is over. So the plan does not grow with the number of masks a schema allows.

A named set costs nothing unless some row has it. The fold expands owner-map rows only, never value rows. The same fold runs for the frozen layers and their materialised map, the staged layer, `flags()` and the write of a whole record.

Under [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md), each layer folds under its own schema. The fold first widens the map folded so far to that schema, then takes the layer's masks and candidate masks over the schema's `partial` dims. It builds these once per distinct layer schema.

### Refusals

The fold no longer refuses rows. The refusals move to three other places:

- A workspace refuses a frozen layer's unfilled rows when it first builds the fold over that layer. So a layer from any writer is still checked.
- The writer refuses a layer in which two rows cross and no row names both. A layer that another writer made fails only when a read reaches the coordinate where the two rows meet.
- `set` refuses an edit whose own rows cross each other, and rolls the edit back. `set` fills every row it stages, so it runs no check for unfilled rows.

### Contested layers

A layer is contested for an attribute where its rows name different sets of `partial` dims. Such a layer reaches some coordinates twice, and the read keeps the more specific row there. Each row of the map has a flag that is true where its layer is contested, so the read needs no Python check. The read joins the value rows of contested layers in one arm, which ranks them. It joins the value rows of every other layer in a second arm, which passes them through unranked. A frozen `Record` keeps its Python check, and takes the ranking arm only where a layer is contested.

No read is slower than before this DEP, at any depth or series length. A read may use more memory than before by a small constant. It never uses more by an amount that grows with depth or series length.

### The staged layer

The plan of a `WorkingRecord`'s relation never depends on the staged rows. It may depend on the rows of the committed layers below the staged one (the frozen prefix), which do not change while the `WorkingRecord` exists.

- Every `Record`, frozen or not, lists every dim and every attribute its schema declares. A dim or attribute with no rows gives an empty relation, where before this DEP the `Record` raises `KeyError`.
- A `WorkingRecord` creates every staging table when it is created. An axis table starts as a copy of the base's axis, and an attribute table starts empty. A `one_of` dim has no staging table.
- The staged layer owns every axis from the start, so [`dim_owners`](../layers/resolution.md#materialised-node-caches) lists the staged layer for every dim.
- The staged layer is a possible owner of every attribute, whether or not it has rows.
- The fold always runs the anti-join for the labels a staged restatement removes ([dead labels](../layers/removal.md)), even when there are none.
- The staged layer folds onto the frozen map one attribute at a time, against the parent's rows of that attribute.
- The staged layer's owner-map rows are kept in one table per attribute. Inside its transaction, each edit refreshes the tables of the attributes it touched. The fold, `flags()` and the write read these tables instead of aggregating the staged rows.
- `rollback()` empties the attribute tables and refills each axis table from the base. It drops no table.

### Crossing staged edits

When `set` stages a row that crosses a staged row of the same attribute, it looks for a staged row that names both. If there is none, `set` stages one with the new value. So the later edit wins wherever the two edits meet. The staged rows never contain a crossing that the fold would refuse.

### The nearest fold at hand

A fold starts from the deepest proper ancestor whose fold is at hand, either materialised on disk or held by the workspace. It takes that ancestor's owner map, axes and dead labels as its seed. The workspace keeps the seed as shared tables for as long as its connection lives. It then folds and checks only the layers above the seed. So a record opened on a child after a commit folds one layer onto its parent's fold.

### Writing staged edits

The write skips a staged axis that equals the base's axis, comparing rows in position order. It also skips an attribute table with no rows. So if a `remove` undoes an `add`, nothing is written.

## Rationale

With named sets as data, the plan stays fixed and the work grows only with the rows that exist. A fixed plan could instead enumerate every named set the schema allows, with one join per set. Most of those joins would run on empty inputs, in every fold of every layer. The price is that the fold copies each owner-map row once per named set it may meet, at most 2^k times for k `partial` dims. One fold also replaces the two folds there were before, which only the tests showed to agree.

On a 16-layer PyPSA ancestry with 1,000 generators over 8,760 snapshots, the frozen fold runs 171 joins instead of 388. It takes 0.68× the time and peaks at 0.45× the buffer memory. At every series length, the fold expands to at most 5,637 rows.

The staged layer must fold one attribute at a time, because a fold of all attributes at once binds every staging table into every relation. For a schema of 150 attributes, that costs seconds per relation.

Eager staging tables have a cost. Creating a `WorkingRecord` takes 0.4–0.5 s for a schema of 153 attributes, where before this DEP it takes no time. Each `WorkingRecord` also keeps a copy of every axis. In return, the staged layer owns every axis from the start. Nothing tracks which axes an edit touched, because the write compares each axis with the base.

The user of an editor expects the last writer to win. `set` already lets the last writer win where two rows name the same key.

Before this DEP, the read skips the ranking where a Python check finds no contested layer. A fixed plan cannot run that check. Ranking every value row would also keep the plan fixed. However, a single base layer with a long series then read 5× slower, with 1 GB more memory. With the contested flag in the map, only the rows that need ranking are ranked. With the staged owner rows also in tables, every read of the 16-layer ancestry is as fast as before or faster. Most are 3–10× faster. `flags()` takes 0.1 s instead of up to 2.7 s.

Each edit recomputes the owner rows of the attributes it touched, which adds a few percent to the time of a large `set`. On small data, a read of an edited record uses a few tens of MB more memory than before.

A relation stays valid across edits, but each edit still costs it a re-bind. DuckDB 2.0 re-binds every Python relation after any committed write, to any table, schema or cursor. So a consumer should hold few relations across a write.

The fixed fold cuts the joins per layer but not the number of statements. So a fold with no ancestor's fold at hand (a cold fold) stays linear in depth, at about 0.11 s per layer over 64 layers.

## Usage

A `set` that crosses a staged row succeeds:

```python
w.set("p_max_pu", 0.5, entity="wind1")
w.set("p_max_pu", 0.7, scenario="high")
```

The second `set` crosses the first, so it also stages `wind1` under `high` with 0.7. A read of `wind1` under `high` returns 0.7. Before this DEP, `set` refuses the second call.

A relation built before an edit returns the edit:

```python
rel = w.attributes["p_nom"]
w.set("p_nom", 150.0, entity="wind1")
rel.fetchall()
```

`rel` was built before the `set`, and `fetchall()` still returns `wind1` with 150.0.

## Implementation notes

1. Every `Record` lists every declared dim and attribute, with a typed empty relation for one with no rows.
2. `_dead_keys` always returns the anti-join for an unfrozen restatement.
3. `_owners` includes every unfrozen layer.
4. Eager staging tables, with `rollback()` refilling them and the write comparing axes with the base.
5. The fold with named sets as data replaces the per-pattern fold, and the checks move to `set` and to the first materialisation of a layer.
6. The contested flag as a column of the map, with the read ranking only the rows of contested layers.
7. The staged owner-map rows in per-attribute tables that every edit refreshes.
8. Last writer wins in `set`.
9. A frozen layer's unfilled rows checked once, when a workspace first folds the layer, instead of on every read.
10. Each layer's owner rows aggregated once into its checkpoint, and one union per layer instead of two binds.
11. The parts of the plan that derive from a schema built once per fold and per layer schema.
12. `materialise` reuses the frozen fold that a workspace already holds.
13. `_base_and_above` takes a workspace-held fold of an ancestor as its seed, as it takes a materialised one.

Three checks hold. The suite passes, with crossing staged edits changed to last writer wins. Owner maps equal those of the per-pattern fold on every ancestry of the equivalence runs, and on a map that the per-pattern fold materialised. After every edit, every relation built before the edit equals one built afresh.

## Rejected Ideas

- Masks from the powerset of `partial` coordinates, folding all attributes at once. In a prototype, the plan became intractable.
- Named sets enumerated from the schema per attribute. The plan is fixed and fast for one attribute, but every fold runs joins on named sets that no row has.
- Candidate masks as a constant list in the plan. At nine `partial` dims, building the plan takes 4.4 s, against 0.04 s when the query computes the masks per row.
- Candidate masks joined from a table per schema. It ties on PyPSA, but it adds joins, memory and a table to maintain.
- Ranking every value row on read, for the reason in [Rationale](#rationale).
- Refusing a `set` that crosses a staged row, because the order of a user's clicks would then decide whether an edit succeeds.
- A checkpoint every few layers instead of every layer. The checks and the crossing join each read the map below, so the plan doubles with each layer left without a checkpoint. A depth-16 fold with a checkpoint every fourth layer takes 25 s, against 3 s.
- Separate schemas or attached databases, to avoid DuckDB's re-bind after a write. DuckDB re-binds every Python relation on the connection regardless.

## Open Issues

- The checks in `set` and in the write still enumerate named sets per pattern, with fetches that depend on the data.
- `set` spreads a broadcast that is scenario-scoped or crossing into one staged row per entity and snapshot.
- When a deep ancestry should materialise its map, so that a cold fold in a fresh workspace has a bounded depth.
- Each edit runs in its own transaction, so an edit fails where the caller holds an open transaction on the same cursor.
