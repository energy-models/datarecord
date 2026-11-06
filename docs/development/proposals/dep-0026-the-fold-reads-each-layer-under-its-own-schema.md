<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 26: the fold reads each layer under its own schema

| DEP      | 26                                                 |
| -------- | -------------------------------------------------- |
| Title    | the fold reads each layer under its own schema     |
| Author   | Jonas Hörsch                                       |
| Status   | Implemented (2026-11-03)                           |
| Requires | [DEP 25](dep-0025-a-layer-carries-its-manifest.md) |
| Created  | 2026-10-30                                         |

The design landed in [resolution](../../reference/resolution.md), [schema](../../reference/schema.md), [store](../../reference/store.md), [editing](../../reference/editing.md) and [open questions](../../about/open-questions.md). Where they and this DEP differ, follow those pages.

## Abstract

The fold reads each layer under the schema in that layer's manifest: its `partial` and its attributes' `dims`. A revision's owner map then depends on its ancestry alone, so its fold table and its materialised map stay valid whatever a descendant declares. Where a deeper layer's schema is wider, the fold first extends the map folded so far to it.

## Motivation

Before this DEP, every layer of an ancestry folds under one schema, the record's current one. Adding a dim to `partial` changes the owner map's key, so no materialised map or fold table keyed before the change stays usable. Every revision below the change then refolds its ancestry from the root, which materialisation exists to avoid. The workspace keeps one fold table per key for the same revision.

Widening an attribute's `dims` by a dim already in `partial` keeps the key, so a map folded before the change stays in use. Its rows for that attribute have `varies` and `broadcast` both false along the dim. The read's join then finds no stored key, and the attribute reads empty ([#117](https://github.com/energy-models/datarecord/issues/117)).

With [DEP 25](dep-0025-a-layer-carries-its-manifest.md), each layer has the full schema it was written under, and `compatible_with` holds along every ancestry. So `partial` and each attribute's `dims` only grow from a layer to its child, and the fold can read each layer as it was written.

## Specification

### Each layer under its own schema

- A layer's owner-map rows are keyed by its own schema's `partial` dims, and its flags follow its own attributes' `dims`. Every source in a fold has its own layer's schema.
- A revision's owner map is keyed by that revision's schema, and its read resolves under it.
- The fold table is `owner_map_<id>`, with no key in its name. A materialised `owner_map.parquet` always stays usable.
- `read_owner_map`, `materialised` and `sources_to_read` lose their schema parameter, and `LayerSource.materialised` its schema argument.

### Extending the map to a wider schema

Where a layer's schema is wider than the schema of its parent's map `M`, the fold extends `M` before folding the layer onto it. A map folded live and a materialised seed extend the same way. Two changes need an extension, whether or not `d` is new to the schema:

| Change in the layer's schema       | Extension of `M`                                                                                             |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `d` added to an attribute's `dims` | the field `d` in both flag structs, with `broadcast.d` set on the attribute's rows, which are NULL along `d` |
| `d` joins `partial`                | a NULL key column for rows not varying along `d`, and a split of the others                                  |

The first extension marks in the flags that the rows broadcast along `d`, so the read joins them as broadcast and `flags()` reports them. No map then lacks a flag field that its revision's schema declares. Two consecutive layers with one schema extend nothing.

The split divides a row with `varies.d`, an overlap row included, by the labels that its assigned layer wrote along `d` at that key:

- One join of `M`'s varying rows against the files of the layers they are assigned to gives one row per label, keyed as stored and NULL-safe on the old key. Where the layer also wrote the key NULL along `d`, it gives one more row, NULL there. A multi-dim's labels are its tuples.
- The fold recomputes each split row's flags per new key, with the aggregate a layer's own rows use.
- The fold drops each split row with a label of `d` removed at `M`'s revision. The read hid those rows there, and once `d` is a key column, no read will.
- While `d` is outside `partial`, the fold keeps the labels each restatement removed along `d`, seeded from a materialised base, minus the parent's axis. Usually there are none, and the fold skips this drop.
- From the layer on, the fold prunes `d` by the layer's removals as it does any `partial` dim.
- Where `bool_or(varies.d)` over `M` is false, as for a dim new to the schema, the projection alone is the extension.

No other compatible change affects the fold. To refuse a crossing along a dim outside `partial`, the fold judges the layer's rows against the extended map, under the layer's schema.

### Deleting a value along a dim outside `partial`

A layer that restates a key along a dim outside `partial` and omits a label its parent had there deletes that value. A later widening of `partial` no longer brings it back. A `WorkingRecord` edit fills the untouched coordinates of the key. datarecord makes each `WorkingRecord` edit carry a label's whole extent, but a layer written another way need not.

## Rationale

A layer's rows have the meaning that its own schema gives them. Only folding a layer under another schema forces datarecord to key a cache by schema. Read per layer, a revision's map is a function of write-once layers, so a cache is valid forever, with no key to check.

The split is needed because a NULL key column does not mean the same for every row. Take a root under `partial = {entity}` writing `(gas, low) 1` and `(gas, high) 2`, and a child under `{entity, scenario}` writing `(gas, high) 3`. Extended with NULL, the root's map row becomes `(gas, NULL)`, and its `varies.scenario` is still true. The read joins it only to the root's rows stored NULL along `scenario`, of which there are none, so `(gas, low)` is lost. Split, the owner map assigns `(gas, low)` and `(gas, high)` to the root, and the child's row covers the second. The read gives `(gas, low) 1` and `(gas, high) 3`.

Recomputed flags keep each split row joined to the file rows its own key matches. A split row that inherited both `varies.d` and `broadcast.d` would also join the layer's row NULL along `d`, and return its label twice.

It costs:

- A fold reads one schema per layer. DEP 25's cache contains it, so a warm read opens no file for it.
- A read under a wider `partial` adds a projection, and a split join that reads the attribute files' key columns. It runs once per revision, cached with its fold, instead of a refold of the ancestry.
- Adding a long axis such as `snapshot` to `partial` grows the map to a row per label where rows vary along it, as a refold does.

## Usage

Nothing a user writes changes. After a descendant widens the schema, datarecord reads a materialised revision as it is, so `rev.materialise()` before a widening is no longer wasted.

```python
rev.materialise()
child = store.commit(source, parent=rev)  # source declares scenario in partial
child.record.attributes["p_max_pu"]  # child folded onto rev's map, extended
```

## Implementation notes

1. Each source has its layer's schema from its manifest, and `_own` and `_over` key and flag by it.
2. `_fold_onto` extends the map wherever the schema widens, with the split and the drop of rows with removed labels. `_as_stored` and `_owners` lose their defaults for a missing flag field.
3. The fold table becomes `owner_map_<id>`, and the schema parameter and the key check leave `read_owner_map`, `materialised` and `sources_to_read`.

Once built, each revision reads the same before and after a descendant widens the schema, with and without materialised maps. The example in the rationale reads `(gas, low) 1` and `(gas, high) 3`. No workspace holds two fold tables for one revision.

## Rejected ideas

- A NULL key column for every widening: it loses the rows varying along a dim that joins `partial`, as in the rationale's example.
- Refolding the ancestry under the widest schema: it invalidates every cache below the widening, and reads a layer under a foreign `partial`.
- Split rows inheriting their parent row's flags: a row that had a key both ways would return its label twice.

## Open issues

- Whether `WorkingRecord` gets an explicit way to delete one value along a dim outside `partial`. Only a hand-written layer can.
- Whether a layer may remove a dim `d` from `partial`. It would restate, as resolved, every key of its own `partial` at which a parent map row has a label of `d`. The fold would project `d` out of the parent's map, dropping every row with a label of `d`. It would refuse a layer leaving one uncovered. `compatible_with` would accept the smaller `partial`, and a helper would build such a layer from a revision.
