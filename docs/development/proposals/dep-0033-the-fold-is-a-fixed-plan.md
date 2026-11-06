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

The design landed in [resolution](../../reference/resolution.md), [editing](../../reference/editing.md) and [layers](../../reference/layers.md), which win where they and this DEP differ.

## Abstract

The fold becomes one query with fixed joins, because each row's set of named dims is a column value. A `WorkingRecord` then plans its relations without reading a staged row, so a relation built before an edit returns that edit. Where two staged edits cross, the later edit wins.

## Motivation

A row's named set is the set of `partial` dims on which its key has a label. Before this DEP, the fold runs joins for each pair of named sets in a layer's rows. Its plan then depends on the data, and on a 16-layer PyPSA ancestry the frozen fold runs 388 joins.

A `WorkingRecord` plans its relations from its staged rows, so a relation built before an edit misses it. When two staged edits cross, `set` refuses the second, so the order of a user's clicks decides whether an edit succeeds.

## Specification

### Named sets as data

The named set is a bitmask column. The fold of a layer's owner-map rows onto the map below runs three steps:

- Prune: one anti-join per `partial` dim drops the parent rows that name a removed label.
- Cover: each parent row is copied once per subset of its named set, tagged with the subset and its labels. A semi-join matches the copies to the layer's rows, and an anti-join drops each parent row a layer row covers.
- Cross: both sides expand to the named sets the other side's row may have, where neither set contains the other. One equi-join keeps the pairs whose labels agree on the dims both name, and each pair gives an overlap row.

An attribute over at most one `partial` dim cannot cross, which the schema shows, so its fold runs no crossing join. The query computes each row's candidate masks from its own mask and its attribute's `partial` dims, so the plan stays one size. The fold expands owner-map rows only, never value rows.

One fold serves the frozen layers and their materialised map, the staged layer, `flags()` and the write of a whole record. Under [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md), it first widens the map to each layer's schema, and builds the masks once per distinct layer schema.

### Refusals

The refusals leave the fold:

- A workspace refuses a frozen layer's unfilled rows when it first folds that layer.
- The writer refuses a layer in which two rows cross and no row names both. A layer from another writer fails where a read reaches the crossing.
- `set` refuses an edit whose own rows cross, and rolls it back. It fills every row it stages, so it runs no check for unfilled rows.

### Contested layers

A layer is contested for an attribute where its rows name different sets of `partial` dims. Such a layer reaches some points twice, and the read keeps the more specific row there. A flag on each map row states whether its layer is contested. The read ranks the value rows of contested layers in one arm, and passes every other layer's rows through a second arm unranked. A frozen `Record` keeps its Python check, and takes the ranking arm only where a layer is contested.

No read is slower than before, at any depth or series length. A read may use more memory by a small constant, which does not grow with either.

### The staged layer

The plan of a `WorkingRecord`'s relation never depends on the staged rows, only on the committed layers below them.

- Every `Record` lists every dim and attribute its schema declares, and one with no rows gives an empty relation, not `KeyError`.
- A `WorkingRecord` creates every staging table at once: each axis table as a copy of the base's axis, each attribute table empty. A `one_of` dim has none.
- The staged layer owns every axis, so `dim_owners` lists it for every dim, and is a possible owner of every attribute.
- The fold always runs the anti-join for the labels a staged restatement removes, and folds the staged layer one attribute at a time.
- The staged owner-map rows are in one table per attribute, which an edit refreshes inside its transaction. The fold, `flags()` and the write read them.
- `rollback()` empties the attribute tables, refills each axis table from the base and drops no table.

Where a staged row crosses a new one and no staged row names both, `set` stages one that does, with the new value. The later edit wins.

The write skips a staged axis equal to the base's, in position order, and an empty attribute table. A `remove` that undoes an `add` writes nothing.

### The nearest fold at hand

A fold seeds from the deepest proper ancestor whose fold is materialised or held by the workspace, and takes its owner map, axes and dead labels as the seed. The workspace keeps the seed while its connection lives, so a child opened after a commit folds one layer.

## Rationale

With named sets as data, the work grows only with the rows that exist. Enumerating every named set the schema allows would fix the plan with joins on mostly empty inputs. The price is a copy of each owner-map row per named set it may meet, at most 2^k for k `partial` dims. One fold also replaces two that only the tests showed to agree.

The 16-layer PyPSA ancestry has 1,000 generators over 8,760 snapshots. Its frozen fold runs 171 joins instead of 388, in 0.68× the time and 0.45× the peak buffer memory. At every series length, the fold expands to at most 5,637 rows.

Folding all attributes at once binds every staging table into every relation, which costs seconds per relation for 150 attributes. Eager staging tables make a `WorkingRecord` take 0.4–0.5 s to create for 153 attributes, and keep a copy of every axis. In return, nothing tracks which axes an edit touched.

The user of an editor expects the last writer to win, as `set` already does where two rows name one key.

Before this DEP, the read skips the ranking where a Python check finds no contested layer, which a fixed plan cannot run. Ranking every value row made one base layer with a long series read 5× slower, with 1 GB more memory. With the contested flag and the staged owner rows in tables, every read of the 16-layer ancestry is as fast or faster, most 3–10× faster. `flags()` takes 0.1 s instead of up to 2.7 s.

It costs:

- Each edit recomputes the owner rows it touched, a few percent of a large `set`. On small data, a read uses a few tens of MB more.
- DuckDB 2.0 re-binds every Python relation after any committed write, so a reader should hold few relations across an edit.
- A cold fold, with no ancestor's fold at hand, stays linear in depth, at about 0.11 s per layer over 64 layers.

## Usage

```python
w.set("p_max_pu", 0.5, entity="wind1")
rel = w.attributes["p_max_pu"]
w.set("p_max_pu", 0.7, scenario="high")
```

The second `set` crosses the first and also stages `wind1` under `high` with 0.7, which `rel` returns.

## Implementation notes

1. Every declared dim and attribute listed, and eager staging tables.
2. The fold with named sets as data, the contested flag and the staged owner tables.
3. Last writer wins in `set`, and a held fold reused as a seed and by `materialise`.
4. Each layer's owner rows aggregated once into its checkpoint.

Once built, the suite passes with crossing staged edits changed to last writer wins. Owner maps equal the per-pattern fold's on every equivalence run and on a map it materialised, and a relation built before an edit equals one built after.

## Rejected ideas

- Masks from the powerset of `partial` coordinates, folding all attributes at once: the prototype's plan became intractable.
- Named sets enumerated per attribute from the schema: joins on sets no row has.
- Candidate masks as a constant list: at nine `partial` dims, the plan takes 4.4 s to build, against 0.04 s for masks computed per row.
- Candidate masks joined from a table per schema: it ties on PyPSA, and adds joins and memory.
- Ranking every value row on read, for the reason in [Rationale](#rationale).
- Refusing a `set` that crosses a staged row: click order would decide whether an edit succeeds.
- A checkpoint every few layers: the checks and the crossing join each read the map below, so the plan doubles per layer without one. A depth-16 fold with a checkpoint every fourth layer takes 25 s, against 3 s.
- Separate schemas or attached databases, to avoid the re-bind: DuckDB re-binds every relation regardless.

## Open issues

- The checks in `set` and in the write still enumerate named sets per pattern, with fetches that depend on the data.
- `set` spreads a broadcast that is scenario-scoped or crossing into one staged row per entity and snapshot.
- When a deep ancestry should materialise its map, to bound a cold fold's depth.
- Each edit runs in its own transaction, so it fails inside a caller's open transaction on the same cursor.
