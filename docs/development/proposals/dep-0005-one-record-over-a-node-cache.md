<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 5: one `Record`, over a `NodeCache`

| DEP           | 5                                                                                       |
| ------------- | --------------------------------------------------------------------------------------- |
| Title         | one `Record`, over a `NodeCache`                                                        |
| Author        | Jonas Hörsch                                                                            |
| Status        | Superseded (2026-10-21, implemented 2026-09-01)                                         |
| Requires      | [DEP 4](dep-0004-staging-as-a-layer.md)                                                 |
| Created       | 2026-09-01                                                                              |
| Version       | v0.1.0a1                                                                                |
| Superseded-By | [DEP 19](dep-0019-one-record-and-a-package-layout.md) (its `Record` over a `NodeCache`) |

The design landed in [the fold](../../reference/resolution.md) and [the `RecordLike` protocol](../../reference/record.md). Where they and this DEP differ, follow those pages.

## Abstract

`DirectoryRecord` is deleted, because a plain directory is a `DirectorySource` and folds like any layer. `LayeredRecord` becomes the one class, `Record`, a narwhals view over a `NodeCache`. The protocol becomes `RecordLike`, and `WorkingRecord` subclasses `Record`.

## Motivation

[DEP 4](dep-0004-staging-as-a-layer.md) made a staging area a source of one fold. A plain directory is a source too. The read-path page lists four ways the directory and layered implementations differ, but on one directory all four give the same answer:

| Compared                                                  | Result                        |
| --------------------------------------------------------- | ----------------------------- |
| `dims`, `entity_types`, `groups`, `attributes`, `outputs` | identical                     |
| `flags`, every declared type                              | identical                     |
| member order, every declared type                         | identical                     |
| resolved rows per attribute                               | identical                     |
| `flags`, first call                                       | 6.6 ms scanned, 2.9 ms folded |

Over one source the fold is a scan. It computes `flags` in its ownership `GROUP BY`, where `DirectoryRecord` scans twice, and `order_key` over one source is file order. `partial` is inert over one layer. The table described an implementation that predated the fold reading anything but a tree.

## Specification

- `NodeCache` does not grow. It stays the DuckDB view over a list of sources.
- `Record` is one frozen dataclass that has a `NodeCache` and adapts its relations to narwhals frames. `Record.over(*sources, con=...)` builds one, and `Record.at(uri)` reads a plain directory as its one layer.
- `Record` is the class and `RecordLike` the protocol. The 17 annotation and `isinstance` sites in reader positions take `RecordLike`.
- `WorkingRecord` subclasses `Record`. Its `NodeCache` is its base's with a `StagedSource` appended. It inherits every read member unchanged except `outputs`, because results do not overlay. `set`, `add`, `remove` and `commit` stay on `WorkingRecord` alone. Its `NodeCache` is fixed at construction: an edit changes the tables the last source reads, never which sources there are.
- `Record` caches its key sets only where `NodeCache.stable` is true, so a staged new attribute stays visible.
- `Record.at` reads a standalone directory's own `manifest.json`, and `NodeCache` has it as `declared`. A `WorkingRecord` over such a base reads under that schema.
- `directory.py` and its `flags` aggregate are deleted, and `LayeredRecord` is renamed. There is no format change.

## Rationale

`tools/` builds against narwhals frames, so keeping `NodeCache` and `Record` apart keeps that interface. A framework object still satisfies the protocol structurally, so the protocol stays and takes the longer name, `RecordLike`, for "anything shaped like a record".

Two arguments for keeping `DirectoryRecord` fail. A one-source fold leaves no owner map on the connection, because a `DirectorySource`'s id derives from its URI and nothing is created: five tables before, five after. And a directory copied out of any tree, with no `revisions` row, folds identically, because the fold needs a layer layout and not a node.

The change withdraws a design property: a directory is now read with resolution. If reading it as a one-layer record is wrong, it is wrong here. `DirectoryRecord.flags` memoised per type, so the fold is faster on the first call and the cache on the tenth.

## Implementation notes

- The cached key sets, not `frozen=True`, were the real obstacle. `NodeCache.stable` and `Record._stable_cache` fix it without an override in `WorkingRecord`.
- `over` takes `con` explicitly, because a `LayerSource` has none.
- A `LayerSource.own_schema` member was tried and dropped: a fold has at most one directory, always at the root.
- The `both` fixture compares one directory read as a directory with the same rows read as a revision's layer. A foreign directory with no tree must read through the fold. `test_tools.py` must pass unchanged. A test lists the members `WorkingRecord` defines beyond `Record`, with `outputs` the one allowed read member.
- `WorkingRecord` already has its base's `NodeCache`, resolved once in `__init__`. `NewChild()` asks the `revisions` table whether there is a node to branch from.

## Open issues

- Whether `Revision` still needs `record`, one construction away from `node_cache`.
- Whether `WorkingRecord`'s base cache becomes a slice of its own, minus the last source.
