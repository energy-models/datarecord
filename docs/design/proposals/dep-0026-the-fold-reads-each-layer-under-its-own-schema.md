<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 26: the fold reads each layer under its own schema

| DEP      | 26                                                 |
| -------- | -------------------------------------------------- |
| Title    | the fold reads each layer under its own schema     |
| Author   | Jonas Hörsch                                       |
| Status   | Draft (2026-10-30)                                 |
| Requires | [DEP 25](dep-0025-a-layer-carries-its-manifest.md) |
| Created  | 2026-10-30                                         |

## Abstract

The fold reads each layer under the schema in that layer's manifest: its `partial` and its attributes' `dims`. A node's owner map then depends on its ancestry alone, so its fold table and its materialised map are valid forever, whatever a descendant declares. Where a deeper layer's schema is wider, the map folded so far is extended to it before the fold goes on, and a row varying along a dim that has joined `partial` is split by its owning layer's labels there.

## Motivation

Today every layer of an ancestry folds under one schema, the record's current one. Adding a dim to `partial` changes the [owner map's](../layers/resolution.md#owner-map) key, so every [materialised map](../layers/resolution.md#materialised-node-caches) and every fold table keyed before it stops counting, and a read refolds the whole ancestry from the root. That is the cost materialisation exists to avoid, paid at every node below the change. The workspace keeps one fold table per key for the same node.

Widening an attribute's `dims` by a dim already in `partial` keeps the key, so a map folded before the change stays in use. Its rows for that attribute hold `varies` and `broadcast` both false along the dim, and [the read's join](../layers/resolution.md#resolving-a-relation) gives such a row no stored key: the attribute reads empty ([#117](https://github.com/energy-models/datarecord/issues/117)). The extension below closes it.

With [DEP 25](dep-0025-a-layer-carries-its-manifest.md) each layer carries the full schema it was written under, and `compatible_with` holds along every ancestry, so `partial` and each attribute's `dims` only grow from a layer to its child. The fold can read each layer as it was written.

## Specification

### Each layer under its own schema

- A layer's owner-map rows are keyed by its own schema's `partial` dims, and its flags follow its own attributes' `dims`. Every source in a fold carries its own layer's schema.
- A node's owner map is keyed by that node's schema, and the node's read resolves under it.
- The fold table is `owner_map_<id>`, with no key in its name. A materialised `owner_map.parquet` always counts.
- `read_owner_map`, `materialised` and `sources_to_read` lose their schema parameter, and `LayerSource.materialised` its schema argument.

### Extending the map to a wider schema

Before a layer folds onto the map `M` of its parent, `M` is brought to the layer's schema where that schema is wider than the one `M` is keyed by. `M` arrives as folded live or as a materialised seed, and is extended the same way. Two changes need it, whether or not `d` is new to the schema:

| change in the layer's schema       | extension of `M`                                                                                                                                                  |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `d` added to an attribute's `dims` | the field `d` in both flag structs, `broadcast.d` set on the attribute's rows, which hold NULL along it, a [broadcast](../model/attributes.md#the-broadcast-rule) |
| `d` joins `partial`                | a NULL key column for rows not varying along `d`; the others split, below                                                                                         |

The first states in the flags what the rows are, so the read joins them as broadcast and [`flags()`](../use/reading.md#flags) reports them, and no map lacks a flag field its node's schema declares. Two consecutive layers with one schema extend nothing.

A row with `varies.d`, an [overlap row](../layers/resolution.md#crossing-rows) included, is split by the labels its owning layer wrote along `d` at that key. The split is one join of `M`'s varying rows, keyed as stored, NULL-safe on the old key, against the owning layers' files of the attributes concerned, and gives one row per label and, where the layer also wrote the key NULL along `d`, one row NULL there. A multi-dim's labels are its tuples. Each split row's flags are recomputed per new key by the aggregate a layer's own rows use. The split rows are then anti-joined against the [dead labels](../layers/resolution.md#a-label-outside-the-key) of `d` at the node `M` belongs to, since the read hid those rows there and, `d` being a key column from now on, no read will. The fold keeps those labels while `d` is outside `partial`: the labels each restatement removed along `d`, seeded from a materialised base's, minus the parent's axis. Usually there are none, and the anti-join is skipped. From the layer on, the fold prunes `d` by the layer's removals as it does any `partial` dim, so it tracks `d`'s restatements from the start of the fold. Where `bool_or(varies.d)` over `M` is false, as for a dim new to the schema, the projection alone is the extension.

No other compatible change reaches the fold. The refusal of a crossing along [a dim outside `partial`](../layers/resolution.md#a-dim-outside-partial) judges the layer's rows against the extended map, under the layer's schema, as it does today.

### Deleting a value along a dim outside `partial`

A layer that restates a key along a dim outside `partial` and omits a label its parent held there deletes that value, as today, and a later widening of `partial` no longer brings it back. Carrying a label's whole extent becomes what a [`WorkingRecord`](../use/editing.md#edits) edit does, by filling the untouched coordinates of the key, rather than a rule every layer must meet.

## Rationale

A layer means what it meant under the schema it was written under. Folding it under another one is why a cache has to be keyed by the schema it was folded under. Read per layer, a node's map is a function of write-once layers, the property that [makes a cache valid forever](../layers/tree.md#a-layers-data-is-write-once), with no key to check.

The split is needed because a NULL key column does not mean the same for every row. Take a root under `partial = {entity}` writing `(gas, low) 1` and `(gas, high) 2`, and a child under `{entity, scenario}` writing `(gas, high) 3`. Extended with NULL, the root's map row `(gas, NULL)` keeps `varies.scenario`, and the read joins it only to the root's rows stored NULL along `scenario`, of which there are none, so `(gas, low)` is lost. Split, the root owns `(gas, low)` and `(gas, high)`, the child's row covers the second, and the read gives `(gas, low) 1` and `(gas, high) 3`.

Recomputing the flags keeps each split row joined to the file rows its own key names. A row inheriting `varies.d` and `broadcast.d` from a parent row that held both would also join the layer's row NULL along `d`, answering its label twice.

It costs:

- A fold reads one schema per layer, which DEP 25's cache holds, so a warm read opens no file for it.
- A read under a wider `partial` pays a projection per seam, and the split join where a dim that rows vary along joins `partial`, once per node and cached with the node's fold. Today it pays a refold of the whole ancestry.
- The split reads the key columns of attribute files at fold time, for the attributes concerned.
- Joining a long axis such as `snapshot` to `partial` grows the map to a row per label wherever rows vary along it, as a refold does today.

## Usage

Nothing a user writes changes. A materialised node is read as it is after a descendant widens the schema, rather than folded again, so `rev.materialise()` before a widening is no longer wasted.

```python
rev.materialise()
child = store.commit(source, parent=rev)  # source declares scenario in partial
child.record.attributes["p_max_pu"]  # child folded onto rev's map, extended
```

## Implementation notes

1. Each source carries its layer's schema from its manifest; `_own` and `_over` key and flag by it.
2. `_fold_onto` extends the map at each seam where the schema widens: NULL columns, the field and `broadcast.d` for an attribute newly over `d`, the split with recomputed flags, the dead-label anti-join. Its `held` covers the node's `partial`, and it keeps a dim's removed labels until the dim joins `partial`. `_as_stored` and `_owners` lose their defaults for a missing flag field.
3. The fold table becomes `owner_map_<id>`; the schema parameter and the key check leave `read_owner_map`, `materialised` and `sources_to_read`.
4. Pages: [resolution](../layers/resolution.md#owner-map) gains the extension, loses the missing flag field read as broadcast in [resolving a relation](../layers/resolution.md#resolving-a-relation) and as unset in [flags](../layers/resolution.md#flags), and [materialised node caches](../layers/resolution.md#materialised-node-caches) loses the rule that a map keyed otherwise is no cache; [versioning](../model/schema.md#versioning) loses the refold on adding to `partial`; [a cursor per thread](../storage/concurrency.md#a-cursor-per-thread) names `owner_map_<id>` and loses the paragraph on reading a fold under a wider schema; [edits](../use/editing.md#edits) states the whole extent as the fill's contract; the settled open question on whether a broadcast survives materialisation loses the refold.

Checks that hold once built:

- Each node reads the same before and after a descendant widens the schema, with and without materialised caches.
- The rationale's two-layer example reads `(gas, low) 1` and `(gas, high) 3`.
- An attribute whose `dims` gained a `partial` dim reads its earlier rows, NULL along it, and `flags()` reports them as broadcast there.
- A layer that brings `d` into `partial` and removes a label of `d` leaves no row naming it.
- No workspace holds two fold tables for one node.

## Rejected Ideas

- A NULL key column for every widening: loses the rows varying along a dim that joins `partial`, as in the rationale's example.
- Refolding the ancestry under the widest schema, today's behaviour: invalidates every cache below the widening, and reads a layer under a `partial` it was not written under.
- Split rows inheriting their parent row's flags: a row that held a key both ways would answer its label twice.

## Open Issues

- Whether `WorkingRecord` gets an explicit way to delete one value along a dim outside `partial`; today only a hand-written layer can.
