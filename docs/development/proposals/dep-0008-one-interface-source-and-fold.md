<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 8: one interface over two objects: a source returns its own rows, and a fold returns the resolved rows

| DEP           | 8                                                                                                   |
| ------------- | --------------------------------------------------------------------------------------------------- |
| Title         | one interface over two objects: a source returns its own rows, and a fold returns the resolved rows |
| Author        | Jonas Hörsch                                                                                        |
| Status        | Superseded (2026-10-21, implemented 2026-09-04)                                                     |
| Created       | 2026-09-02                                                                                          |
| Version       | v0.1.0a1                                                                                            |
| Superseded-By | [DEP 19](dep-0019-one-record-and-a-package-layout.md) (its fold that is `LayerData`)                |

The design landed in [the fold](../../reference/resolution.md), [the `RecordLike` protocol](../../reference/record.md), [layers](../../reference/layers.md) and [the package](../package.md). Where they and this DEP differ, follow those pages.

## Abstract

`axis(dim)` means "the rows of the object I hold" wherever it is written. A `LayerSource` returns the rows of its own layer, and a fold returns the rows of everything folded into it. `ResolvedLayer` is deleted and `NodeCache` becomes `Resolver`. A materialised node becomes a `Fold` that the fold finds among its sources. `write_record` takes `LayerData`, the interface a source and a `Resolver` share.

## Motivation

`ResolvedLayer` is a persisted `NodeCache`: `Revision.materialise()` writes the fold of a node to `resolved/`, and `ResolvedLayer` reads it back. It is typed as a `LayerSource` and sits in the fold's own input list. So a fold result appears as a fold input. The seed paths of `resolve_dims` and `_fold_map` read its folded axes and owner map, while `source_for` reads its own layer's rows through the same object.

[DEP 6](dep-0006-a-member-file-holds-values.md) needs `axis` to be layer-local on every source, and its sketch adds `resolved_axis` beside `map_uri` to get there. That hides the dual role instead of removing it.

`WorkingRecord` already keeps the two views as two objects: the `Record` over base and staged rows, and the `StagedSource`. But `staged_only()` returns a `_Written`, a third spelling of the staged layer's rows, because `write_record`'s input is spelled as neither interface.

## Specification

- `NodeCache` is renamed `Resolver`, and `stable` becomes `frozen`, true when every source is frozen. `Dims` is renamed `Coords`.
- `Fold`, in `datarecord/layered/fold.py`, has `schema`, the folded axes, groups and owner map, and the reads `attributes()`, `owners(attribute)` and `flags(entity_type=None)`. `flags` covers the whole record when `entity_type` is `None`. `Fold.read(revision_id, con)` reads `resolved/` and folds nothing.
- `LayerSource.materialised(con, schema)` returns the node's `Fold` or `None`. `_base_and_above(sources, con)` scans all but the last source, deepest first, and the first hit is the base. The last source is never the base: a node resolves from its own layer, never its own cache, so `materialise` never reads its half-written cache back.
- The base seeds each fold as depth 0, below every layer. So per key the fold still keeps the row of a layer above, and that layer's tombstones still remove a base key.
- `ResolvedLayer` is deleted. `sources` contains only plain `LayerSource`s, `source_for` returns an own-rows source and never the base, and `axis` is layer-local on every source.
- `LayerData`, in `record.py`, is `schema`, `frozen`, and the pairs `axes`/`axis`, `entity_types`/`entity_type`, `groups`/`group` and `attributes`/`attribute`. Its enumerators return `Iterable[str]`, because `Resolver.attributes` is ordered and a source's is a set. `LayerSource` and `Resolver` both satisfy it structurally.
- `layer_id`, `uri` and `all_attributes` stay source-only. `with_source`, `source_for`, `owners`, `flags`, `fold` and `revision_id` stay on `Resolver`.
- A source gains `entity_types`, `groups` and `attributes` as plain directory listings. Every source gets the schema its caller already has, and `Resolver.schema` is a required field. `declared` and the fallback to `read_schema(con)` go.
- `Resolver` renames `entity_type_frame` to `entity_type` and routes `attribute(name, kind)` to private `_relation` and `_outputs`, which return an empty relation and not `None`. `group_frame` stays as the presentational read `Record` uses.
- `write_record` takes `LayerData | RecordLike`. A `RecordLike` producer goes through `_RecordLikeAsLayerData` in `write.py`. `commit` passes `sources[-1]` for `NewChild` and the `Resolver` for `Directory`. `_Written`, `staged_only()` and the `_staged_*` frame builders are deleted, except `_staged_groups`.
- `write.py` works on DuckDB relations throughout.
- An enumerator returns the empty set exactly when `entity_type_dim is None`. A declared `String` axis still lists its member files.

## Rationale

What `axis` returns depends on the object you call it on, not on a `resolved_` prefix or a mode flag. A file-backed source then needs no branch for the configuration it reads. The name `Resolver` describes the job, where `NodeCache` described an implementation detail, and `Coords` has groups and the broadcast algebra as well as axes.

`frozen` and `materialised` mean different things. `frozen` means that a source's rows cannot change under a reader, and gates the in-memory cache per connection. `materialised` is a filesystem fact set only by `.materialise()`, and gates the base that shortens the fold. Every materialised node is frozen, and almost no frozen node is materialised.

`flags` sits on `Fold` because scoping to a type needs both the owner map and the entity axis. `LayerData` contains only the members that mean the same for a layer and a fold. A method that only one of them has shows that the two are different types.

The writer needs exactly `LayerData`: the schema, plus per kind the keys an object has and the rows of each. The read never needs the enumerators, because the fold learns keys from the schema and the owner map. The writer must list what a layer contains, where the schema declares more. The `Directory` commit writes the resolved record, which is a fold, through the same interface.

The change reshapes the source list, and `sources_to_read`, `resolve_coords` and `source_for` move together. It also moves `write_record`'s contract.

## Implementation notes

It landed as three commits on `refactor/staging-as-a-layer`: `cd2ce92`, `006c159` and `7f244a9`.

1. The read-path split: `materialised`, the base from the deepest materialised source, `ResolvedLayer` deleted, and the `Resolver` rename. `Fold` lives in its own module to break an import cycle with `sources.py`.
2. The `Fold` object. Folding a live `Fold` needs `Coords`' broadcast algebra, so `Fold.compute` is a `resolve.py` concern and `Resolver.fold` wraps `dims` and `_map("inputs")`. No cache moved. The live `Fold` has `entity_types={}`, because it is never a base.
3. `LayerData` and the write path.

`test_resolved_reads_same_as_unresolved` landed first, green before and after. It folds one node through its base and one from the root, and compares the owner map, the entity axis and every attribute. `def axis` in `sources.py` is only on `_FileLayer`, and `sources.py` changes only by deletion. On a materialised untyped node, `entity_types()` is the empty set on both the base `Fold` and the head's `ParquetLayer`.

## Rejected ideas

- `resolved_axis` and `resolved_axes` beside `map_uri`: a third folded-answer name that hides the dual role.
- A stored `base` field on `Resolver`: the base is discovered from the sources.
- `flags` on a map-only `OwnerMap`: it would need the entities relation handed in from outside.
- `all_attributes` on `LayerData`, with a folded `Resolver.all_attributes`: no caller reads it on a fold. A layer's share of the owner map needs the resolved axes to expand a NULL, so the split falls between raw rows and expand-and-aggregate.

## Open issues

- Whether `Record`'s public narwhals interface joins the shared vocabulary, or only the internal rows interface does.
- Whether a later `Store` refactor makes a `Resolver.owner_map(kind)` and a folded `all_attributes` worth having.
