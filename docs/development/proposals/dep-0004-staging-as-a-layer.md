<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 4: staging as a layer, one fold, two sources

| DEP           | 4                                                                                                                                      |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Title         | staging as a layer, one fold, two sources                                                                                              |
| Author        | Jonas Hörsch                                                                                                                           |
| Status        | Implemented (2026-09-01)                                                                                                               |
| Created       | 2026-09-01                                                                                                                             |
| Version       | v0.1.0a1                                                                                                                               |
| Superseded-By | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its per-label axis fold, and `partial` as how many labels an axis layer carries) |

The design landed in [the fold](../../reference/resolution.md) and [`WorkingRecord`](../../reference/editing.md), which win where they and this DEP differ.

## Abstract

The fold reads each layer through a `LayerSource`, so parquet layers, a plain directory and a `WorkingRecord`'s staged edits fold the same way. The staged edits become the last source. The fold is materialised up to the last frozen source and stays a relation after it. `pending` goes, and result attributes are declared in the schema.

## Motivation

The editing page says a set of pending edits is a layer, an unwritten one. The code does not deliver that. `mutable.py` reimplements the algebra of `layered/resolve.py` three times over, and each copy drifted from its twin:

- The overlay: `fold_inputs` anti-joins the parent and unions the layer, and `_overlay`, `_entity_union` and `_group_union` do the same against staged rows. Five sites, one rule.
- The flags: the fold computes `varies` and `broadcast` in its ownership `GROUP BY`, and `_flags_arm` computes them again over the staging tables.
- The collapse: `_latest_per` collapses staged rows by `_seq`, and the fold collapses layers by depth.

## Specification

- A `LayerSource` hands over one layer's own rows as the layer would be written. It has `layer_id`, `frozen`, and one member per file: `axes()`, `axis(dim)`, `entity_type(name)`, `group(name)`, `attribute(name, kind)` and `all_attributes(kind)`. A member returns `None` where the layer wrote nothing. `Kind` is `Literal["inputs", "outputs"]`.
- `entity` is an axis, read through `axis("entity")`. `all_attributes` exists only for `inputs/` and `outputs/`, because their files share one key, and returns the union unprojected. `attribute(name)` reads one file with its own columns, since a padded column is ambiguous under the broadcast rule.
- `axes()` lists a directory once, replacing a probe per declared dim.
- Everything the fold does to rows stays in the fold: padding, broadcast expansion, the ownership aggregate and `order_key`. `_seq` collapsing and tombstones stay behind the source.
- `ParquetLayer`, `DirectorySource` and `StagedSource` implement the protocol. `NodeCache` folds a list of sources, root first. A `WorkingRecord` is `sources + [StagedSource(self)]`.
- The fold is materialised up to the last frozen source, and everything after it stays a relation. Nothing needs invalidating. `NodeCache.dims` becomes a plain property that caches the frozen prefix's axes.
- A staged axis edit merges per column, so two `set` calls on one axis do not blank each other.
- As rows are staged, `_complete_owned_whole` carries the parent's rows that complete each key an edit named along the dims not in `partial`. A staged row that leaves such a dim NULL is excluded. `staged_only()` then contains what gets written.
- `WorkingRecord.pending` is removed.
- `Schema` declares result attributes beside inputs. `outputs` still lists what a layer holds, as `attributes` does. `long_columns_for` loses its fallback, and `value_hint` retires with `_value_dtype` and `_scalar_dtype`. A result may still name a component the record never declared.
- A base that is neither layered nor a directory is out of scope, and `_base_revision` refuses it.

## Rationale

A written layer's extent along every dim outside `partial` is already complete, so the fold's anti-join loses nothing. A staging area that has not completed would let one staged point evict the base's whole series. Completing as rows are staged removes the difference, so the algebra needs no second copy. The completion runs per insert, because it is scoped by the keys an edit named.

Removing `pending` is cheaper than marking carried rows in every staging table. After completion, a one-value `set` on a thousand-snapshot attribute would report a thousand pending rows. The reads state what the record is.

Stating the cache rule over `frozen`, not as a mode, means a second staged layer needs no new code. Truncating at a materialised ancestor becomes building fewer sources.

The design costs this:

- A protocol in `layered/` that `mutable.py` and `directory.py` implement structurally. `mutable.py` keeps importing from `layered/` only inside function bodies, to avoid a cycle.
- A tool must declare a result before attaching it, though PyPSA's `SubNetwork` exists only after a solve. The tool declares its result vocabulary up front or amends the schema at solve time, and `Schema.compatible_with` decides whether old layers survive.
- `set` reads the base the first time it touches an attribute with a dim outside `partial`.
- A read with pending edits re-folds the staged step every time.
- `NodeCache`'s invariant becomes "nothing up to the last `frozen` source can go stale".

The change deletes about 250 lines from `mutable.py`, and the `_collapsed_*` helpers move into `StagedSource`. It has no format change of its own.

## Implementation notes

Three changes landed first, each in its own PR:

1. `components` becomes `entity_type` across the API and on disk.
2. Axis discovery by glob in `resolve_dims` and `fold_axis`.
3. `order_key` becomes a struct `(depth, row)`. A layer's numbering no longer depends on its parent's values, which removes a subquery that would re-run on every read with a live tail. A key restated deeper keeps its first `(depth, row)`. DuckDB orders structs by field, so `ORDER BY order_key` works unchanged, and a child of a materialised parent takes `max(order_key.depth) + 1`. This one is a format change.

Steps 0 to 4 added the identity spike and extended the `both` fixture to read one record four ways. They moved the completion out of commit, declared results and added `LayerSource`. Steps 5 to 7 landed in two commits. The build added two things:

- `ResolvedLayer`, a third source for a materialised ancestor, whose axes and owner map come from `resolved/` and whose own rows still come from its layer.
- `_axis_layer` merges per column on an axis in `partial` too, because the fold is last-writer-wins per label over the whole row.

Step 8, sharing the flags aggregate with `DirectoryRecord`, was dropped. With `_flags_arm` gone, the two aggregates share three `bool_or` lines, and a helper's parameters would outweigh them.

Two things stay unfixed by choice. `attributes_of` re-executes under a live tail, bounded by the map. `commit` still needs a revision, so a `WorkingRecord` over a `DirectorySource` is readable but cannot commit.

Over both bases, the tests check that an unstaged `WorkingRecord` reads like its base and a staged one like its commit. A second `set` after a read is visible. One more test asserts that the grouping grain of the fold's flags is invisible after `attributes_of` unions them.

Measured over a 20-layer unmaterialised ancestry of the `ac_dc` network, best of five:

| Operation                           | Time   |
| ----------------------------------- | ------ |
| deep read, nothing staged           | 2.4 ms |
| deep read, one staged edit          | 61 ms  |
| `set`, first touch of an attribute  | 166 ms |
| `set`, again on the same attribute  | 16 ms  |
| `set`, again, no owned-whole dim    | 10 ms  |
| directory `flags`, scanned          | ~0 ms  |
| directory `flags`, through the fold | 12 ms  |

Of the three performance claims, two hold. A read after a `set` costs one fold step, but against a cached base, so 61 ms against 2.4 ms. A repeat `set` costs the completion about 5 ms. A `WorkingRecord` over a directory folds `flags` where a bare one scans and memoises them. A repeat `set` first measured 188 ms, because its validation rebuilt name-to-type from member frames. Reading the owner map instead brought it from 157 ms to 10 ms. Building `StagedSource`'s collapses on 50k staged rows over 500 entities and 20 periods takes about 1.4 ms to plan and 4.3 ms to run. `tests/test_scaling.py` covers none of this.

## Rejected ideas

- Marking carried rows apart from edited ones to keep `pending`: a second row class in every staging table.
- A combined read across axes or groups: each folds on its own key.
- A raw-rows member on the source: `_seq` is staging state, and the fold must not see it.
- A shared `overlay(older, newer, on)` helper: no second caller remains.
- Synthesising a source from a framework's object: it rebuilds the layer format, and `all_attributes` would materialise the whole record per read.

## Open issues

- Memoising `StagedSource`'s collapses on the highest `_seq` staged, which is local to the source.
- Whether `materialise` could run over a staging area, freezing the tail into the prefix.
- Whether the commit path collapses further. [DEP 5](dep-0005-one-record-over-a-node-cache.md) made `flattened()` equal to `self` in every field but `entity_type`. `staged_only()` stays open, because an adapter deletes about as much as it adds without a second caller.
- Whether a `DirectoryRecord` gains a map. [DEP 5](dep-0005-one-record-over-a-node-cache.md) resolves this the other way: it deletes `DirectoryRecord`, and a directory reads through the fold more cheaply.
