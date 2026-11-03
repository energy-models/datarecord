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

## Abstract

The fold of a layer onto the owner map below it becomes one query whose joins do not depend on which named sets its rows hold: a row's named set is a column, not a loop. A `WorkingRecord`'s relations are planned without reading a staged row, so a relation built once reads every later edit. Where two staged edits cross, the later one wins.

## Motivation

The fold today loops over the named sets the rows of a layer hold, and runs joins per pair of them. Its plan depends on the data, and its joins grow with the patterns a deep ancestry accumulates: 388 joins for the frozen fold of a 16-layer PyPSA ancestry.

A `WorkingRecord` decides from staged rows in Python whether a staging table exists, which layers own an attribute and whether an axis is staged. A relation built before an edit misses what that edit changes, and a view reads a stale plan.

Two staged edits that cross, each naming a dim the other leaves NULL, are refused today, so the order of an editor's clicks matters.

## Specification

### One fold, its patterns as data

A row's named set over the `partial` dims is a bitmask. The fold of one layer's owner-map rows onto the map below it is:

- Pruning: the parent rows naming a removed label are dropped, one anti-join per `partial` dim.
- Covering: each parent row is expanded to one row per subset of its named set, tagged with the subset and its labels on it. One semi-join on attribute, subset and labels against the layer's rows, keyed by their own named set, finds every parent row a row of the layer covers, and one anti-join drops them.
- Crossing: both sides are expanded to the named sets the other side's row may hold without either containing the other, and one equi-join keeps the pairs whose labels agree on the dims both name. Each gives an [overlap row](../layers/resolution.md#crossing-rows). An attribute over at most one `partial` dim cannot cross and runs no crossing join; the schema decides this.

The query computes each row's candidate masks from its own mask and its attribute's domain, the `partial` dims the attribute is over, so the plan does not grow with the masks a schema allows.

A named set no row holds costs nothing. Only owner-map rows are expanded, never value rows. The fold serves the frozen prefix and its materialised map, the staged step, `flags()` and the write of a whole record.

Under [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md) the masks and the candidate masks of a layer are taken over its own schema's `partial` dims, once per distinct layer schema, after the map folded so far is widened to it.

The fold no longer refuses rows. A frozen layer's unfilled rows and unsettled crossings are refused when a workspace first builds the fold over it, so a layer from any writer is still checked. A `set` whose own rows cross each other is refused, and its edit rolled back. `set` fills what it stages, so it runs no check for unfilled rows.

### The read ranks only contested rows

A layer is contested for an attribute where its rows name different sets of `partial` dims, so it reaches some coordinates twice and the read keeps the more specific row there. Whether a layer is contested is a column of the map, not a Python decision. The read joins the value rows of contested layers in one arm, which ranks them, and those of every other layer in a second arm, which passes them straight through. A frozen `Record` keeps today's check, taking the ranking arm only where a layer is contested.

No read is slower than today's, at any depth or series length. Its memory may exceed today's by a small constant, never by an amount that grows with depth or series length.

### The staged step is fixed

The plan of a relation of a `WorkingRecord` never depends on the staged rows; it may depend on the rows of the frozen prefix, which do not change while the `WorkingRecord` lives.

- Every `Record`, frozen or not, lists every dim and every attribute its schema declares. One with no rows reads an empty relation, where today it raises `KeyError`.
- A `WorkingRecord` creates every staging table when it is created. An axis table holds a copy of the base's axis, an attribute table starts empty. A `one_of` dim has none.
- The staged layer owns every axis from the start, so [`dim_owners`](../layers/resolution.md#one-fold-for-every-axis) names it for every dim.
- The staged layer is a possible owner of every attribute, whether or not it holds rows.
- The dead labels of a staged restatement are always an anti-join, empty or not.
- The staged layer folds onto the frozen map one attribute at a time, against the parent's rows of that attribute.
- The staged layer's owner-map rows are kept per attribute in a table that every edit refreshes, inside its transaction, for each attribute it touched. The fold, `flags()` and the write read these tables instead of aggregating the staged rows.
- `rollback()` empties the attribute tables and refills each axis from the base. It drops no table.

### Last writer wins in staging

A `set` whose row crosses a staged row of the same attribute, with no staged row naming both, also stages the crossing row with its own value. The later edit wins wherever the two meet, and the staged rows never hold a crossing the fold would refuse.

### A fold starts from the nearest fold at hand

A fold starts from the deepest proper ancestor whose fold is materialised on disk or held by the workspace: its owner map, axes and dead labels, kept as shared tables for as long as the workspace's connection. Only the layers above it are folded, and only they are checked. A record opened on a child after a commit folds one layer onto its parent's fold.

### Writing staged edits

A staged axis that equals the base's, as rows in position order, is not written, nor is an attribute table with no rows. An `add` undone by a `remove` writes nothing.

## Rationale

A fixed fold could enumerate every named set the schema allows, one join per pattern. Most of those joins would run on empty inputs, in every fold of every layer. Carrying the pattern as data keeps the plan fixed and the work proportional to the rows there are, at the price of copying each owner-map row once per named set it may meet, at most 2^k for k `partial` dims. One fold for every purpose replaces two whose agreement only tests could show.

On a 16-layer PyPSA ancestry with 1,000 generators over 8,760 snapshots, the frozen fold runs 171 joins instead of 388, takes 0.68× the time and peaks at 0.45× the buffer memory. Its expansion takes at most 5,637 rows at every series length.

The staged layer must fold one attribute at a time. A fold of all attributes at once binds every staging table into every relation. For a schema of 150 attributes, that costs seconds per relation.

Eager staging tables have a price. Creating a `WorkingRecord` takes 0.4–0.5 s for a schema of 153 attributes, where today it takes no time, and each one holds a copy of every axis. In exchange the staged layer owns every axis from the start. Nothing tracks which axes an edit touched, because the write compares them with the base.

Last writer wins is what the user of an editor expects. It is also what `set` already does where two rows name the same key.

A fixed plan cannot skip the ranking from a Python check, as today's read does. Ranking every value row instead kept the plan fixed but read a single base layer with a long series 5× slower and with 1 GB more memory. Carrying the check as data keeps the ranking's input to the rows that need it. With the staged owner rows kept in tables, every read of the 16-layer ancestry above is at least as fast as today's, most of them 3–10× faster, and `flags()` takes 0.1 s instead of up to 2.7 s.

Each edit recomputes the owner rows of the attributes it touched, which adds a few percent to a large `set`. The read of an edited record takes a few tens of MB more memory than today's on small data.

A relation that stays valid across edits still pays for each of them. DuckDB 2.0 re-binds every Python relation after any committed write, to any table, schema or cursor. A consumer should therefore hold few relations across a write.

The fixed fold cuts the joins per layer, not the statements, so a cold fold stays linear in depth: about 0.11 s per layer over 64 layers.

## Usage

A crossing `set` succeeds:

```python
w.set("p_max_pu", 0.5, entity="wind1")
w.set("p_max_pu", 0.7, scenario="high")  # refused today; now wind1 under high reads 0.7
```

A relation built before an edit reads the edit:

```python
rel = w.attributes["p_nom"]
w.set("p_nom", 150.0, entity="wind1")
rel.fetchall()  # holds 150.0
```

## Implementation notes

1. Every declared dim and attribute listed by every `Record`, a typed empty relation for one with no rows.
2. `_dead_keys` always returns the anti-join for an unfrozen restatement.
3. `_owners` includes every unfrozen layer.
4. Eager staging tables, `rollback()` refilling them, and the write comparing axes with the base.
5. The fold with patterns as data, replacing the per-pattern fold, and the checks moved to `set` and the first materialisation of a layer.
6. The contested flag carried as data, and the read ranking only the rows of contested layers.
7. The staged owner-map rows in per-attribute tables that every edit refreshes.
8. Last writer wins in `set`.
9. The refusal of a frozen layer checked once, when a workspace first folds it, rather than on every read.
10. Each layer's owner rows aggregated once into its checkpoint, and one union per layer in place of two binds.
11. The parts of the plan derived from a schema built once per fold and layer schema.
12. `materialise` reusing the frozen fold a workspace already holds.
13. `_base_and_above` taking a workspace-held fold of an ancestor as its seed, beside a materialised one.

The check: today's suite, with crossing staged edits turned to last-writer-wins; owner maps equal to today's on every ancestry of the equivalence runs and on a map today's code materialised; after every edit, every relation built before it equals one built afresh.

## Rejected Ideas

- Masks taken from the powerset of `partial` coordinates while folding all attributes at once. The plan became intractable in a prototype.
- Named sets enumerated from the schema per attribute. The plan is fixed and fast for one attribute, but runs joins on empty patterns in every fold.
- Candidate masks as a constant list in the plan. Building the plan takes 4.4 s at nine `partial` dims, against 0.04 s computed per row.
- Candidate masks joined from a table per schema. It ties on PyPSA, but adds joins, memory and a table to keep.
- Ranking every value row on read. See [Rationale](#rationale).
- Refusing a `set` that crosses a staged row: it makes the order of a user's clicks matter.
- A checkpoint every few layers rather than every layer. The checks and the crossing join each read the map below, so the plan doubles with each layer left unchecked: 25 s for a depth-16 fold checkpointed every fourth layer, against 3 s.
- Separate schemas or attached databases to escape DuckDB's re-bind after a write. Every Python relation on the connection is re-bound regardless.

## Open Issues

- The checks at `set` and at write still enumerate named sets per pattern, with data-driven fetches.
- `set` spreads a scenario-scoped or crossing broadcast into one staged row per entity and snapshot.
- When a deep ancestry materialises its map, so that a cold fold in a fresh workspace is bounded by depth.
- An edit runs in its own transaction, so it fails where the caller holds an open transaction on the same cursor.
