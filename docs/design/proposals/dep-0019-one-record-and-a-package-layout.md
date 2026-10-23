<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 19: one Record and a package layout

| DEP      | 19                                                                                                                                                               |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | one Record and a package layout                                                                                                                                  |
| Author   | Jonas Hoersch                                                                                                                                                    |
| Status   | Accepted (2026-10-21)                                                                                                                                            |
| Requires | [DEP 18](dep-0018-drop-narwhals.md)                                                                                                                              |
| Created  | 2026-10-21                                                                                                                                                       |
| Version  | Unreleased                                                                                                                                                       |
| Replaces | [DEP 5](dep-0005-one-record-over-a-node-cache.md) (`Record` over a `NodeCache`), [DEP 8](dep-0008-one-interface-source-and-fold.md) (a fold that is `LayerData`) |

## Abstract

`Record` absorbs `Resolver`: one class folds a record's layers and answers `RecordLike`, and a record with staged edits folds only its staged layer per read. The package splits into three subpackages, `schema/`, `resolve/` and `store/`, under one dependency rule, which breaks the import cycle between `store.py` and `layered/revision.py`. The internal names that still say "inputs" take the owner map's vocabulary.

## Motivation

[DEP 8](dep-0008-one-interface-source-and-fold.md) split `Resolver` from `Record` so that the writer could read a `Resolver` directly as `LayerData`. Since `WorkingRecord.commit` was removed in a17bf6e, the writer no longer does: a whole record reaches it as a `Record`, through the adapter for a `RecordLike`. Each member of `Record` still forwards to one member of `Resolver` and turns its `None` into a `KeyError`, and code that needs the record has to carry both objects. `Revision` caches a `Resolver` in `rev.resolver` but builds a new `Record` around it on every access to `rev.record`, which throws away the cache of key sets that `Record` keeps. `WorkingRecord` builds its resolver with `Resolver.with_source`, which nothing else calls, and wraps that resolver in a `Record` once more.

When a working record is written as a whole, the writer has to fold its staged edits once for the duration of the write. `_pinned` does this by making a shallow copy of the `Record` and setting a `PinnedResolver` on the copy through `object.__setattr__`, since `Record` is a frozen dataclass, and `release` later reaches that `PinnedResolver` through two `isinstance` checks. Several members exist without a reader: nothing calls `Resolver.axes()` or `Resolver.groups()`, `group_frame` only calls `group`, and the writer never reads `LayerData.frozen`.

On a record with staged edits, every read of the axes, groups or dead labels folds them again from the deepest node cache on disk and opens the parquet files of every layer above it. [Reading with pending edits](../working-record.md#reading-with-pending-edits) says that the fold is materialised up to the staged layer and that only the staged step is folded per read, and the owner map already works that way.

`store.py` imports `layered/revision.py` lazily inside three methods, because `revision.py` imports `store.py` at the top. The page on [the module layout](../module-layout.md) no longer matches the tree: it lists no `fold.py` and says that `files.py` holds manifests. `schema.py` (1,554 lines) and `layered/resolve.py` (1,499 lines) each hold several parts that can stand on their own.

## Specification

### One `Record`

`Record` is a frozen dataclass in `resolve/record.py` with the four fields `Resolver` has today:

| field         | holds                                                             |
| ------------- | ----------------------------------------------------------------- |
| `revision_id` | the node whose frozen layers are folded, as for `Resolver` today  |
| `sources`     | the layers, root first, truncated at the deepest materialised one |
| `workspace`   | where the calling thread's cursor comes from                      |
| `schema`      | the record's one manifest                                         |

A `Record` is read through one API, the `RecordLike` members (`schema`, `dims`, `properties`, `groups`, `attributes`, `flags`), and its public surface adds only its four fields, `con` and `frozen`. The `Resolver` methods that read the same rows a second way, returning `None` where a mapping raises `KeyError`, are deleted: `axis`, `group`, `group_frame`, `attribute`, the enumerators `axes()`, `groups()` and `attributes()`, `with_source` and `inputs`. What stays private is the fold the mappings answer from: `_fold` holds the owner map, `_coords` holds the folded axes, groups and dead labels (the `Coords` that `Resolver.dims` returns today), and `_source_for` and `_relation` resolve one attribute. `_relation` raises `KeyError` for an attribute that no layer owns, where `attribute` returned an empty relation; only a key set read before `rollback()` and used after it reaches that case. `Record` keeps its cache of key sets, which now checks `self.frozen`.

On a record with staged edits, `_coords` folds only the layers after the frozen prefix onto that prefix's coord tables. `_frozen_coords` already holds the prefix's folded axes, groups and dead labels as shared DuckDB tables (`_coord_table`), and `resolve_coords` already takes a folded seed when it starts from a node cache on disk. It now takes those tables as its seed, as `_fold_map` takes `_frozen_table` for the owner map, so a fold on a working record reads tables already in the database, opens no file, and does what [reading with pending edits](../working-record.md#reading-with-pending-edits) describes. The key set of `groups` comes from one such fold, not from one fold per declared group.

A label or group tuple that one layer removes and a later layer adds again is now introduced where it is added again, not where it was first introduced. The prefix's coord tables and the cache of a materialised node keep a tombstoned key at most as a dead label, which has no place in member order, so a fold seeded from either cannot put a re-added key where it was first introduced, as the old rule required. Under the new rule, `fold_axis` counts only the rows of a key that are deeper than its deepest tombstone, so the seeded fold, the fold over every layer and a fold over a materialised node agree.

`WorkingRecord` is the only caller of the deleted methods, and it reads through the mappings instead:

| `WorkingRecord` today                                                         | becomes                                                                                      |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `resolver.axis(dim)`, to test for an axis or to check that labels exist       | `self.dims.get(dim)`, read once per call                                                     |
| `resolver.axis(dim)`, to match property values                                | `self.properties.get(dim)`                                                                   |
| `resolver.axis(s)` or `resolver.dims.groups.get(s)`, for a referring property | `self.properties.get(s)`, for a dim and a group alike                                        |
| `resolver.dims.groups.get(group)`                                             | `self.groups.get(group)`                                                                     |
| `_base.dims.groups.get(group)`                                                | `self._base.groups.get(group)`                                                               |
| `_base.attributes()` and `_base.attribute(name)`                              | `name in self._base.attributes` and `self._base.attributes[name]`                            |
| `_base.dims.axes.get(dim)`, the full rows                                     | `self._base.properties.get(dim)`, or `self._base.dims.get(dim)` for a dim without properties |

The base axis rows that `WorkingRecord` unions into a layer owning the dim whole now come from `properties`, which adds each declared property that no layer stored as a typed NULL column. A written axis layer already holds those columns, because the staged axis table has a column for every declared property (`_axis_columns`) and `_insert` projects a staged row onto it, so the layer reads back as before.

`PinnedRecord` subclasses `Record` and replaces `PinnedResolver`: it folds the unfrozen tail once and keeps the owner map in a table until `release()` drops it. It creates that table on the first access to `_fold`, so a write that fails before `write_layer` enters its `try` leaves no table behind. `layer_data` builds a `PinnedRecord` from the four fields of any `Record` that is not frozen, and `write_layer` calls `release()` on it after the write. `WorkingRecord` calls `Record.__init__` with the fields of its base and with `[*base.sources, StagedSource(...)]` as its sources, and keeps the base `Record` as `_base`.

`Revision` caches its `Record` instead of a `Resolver`, and `materialise` clears that cache. `Revision.resolver` is deleted, and `Workspace.at` and `Store.layer` construct a `Record` directly. A `Record` looks up `workspace.con` on each call, so one `Record` can be read from several threads, as one `Resolver` can today ([a cursor per thread](../read-path.md#a-cursor-per-thread)). A `PinnedRecord` keeps its relations on the cursor of the thread that built it, as `PinnedResolver` does.

[DEP 5](dep-0005-one-record-over-a-node-cache.md) made `Record` "the interface over one" `NodeCache`, which later became `Resolver`; with this DEP `Record` is the fold itself. [DEP 8](dep-0008-one-interface-source-and-fold.md) made "a `Resolver` that is `LayerData`"; with this DEP the writer reads no fold as `LayerData`, and `Record` does not answer `LayerData`.

### `LayerData`

`LayerData` stays separate from `RecordLike`. It loses `frozen`, which the writer never reads, and its enumerators return `set[str]` instead of `Iterable[str]`, a type that existed only so that a `Resolver` could return an ordered `list`. A `LayerSource` keeps its own `frozen`, because the fold reads it.

### The package layout

```text
datarecord/
├── __init__.py        # every public name, unchanged; where a user imports from
├── duck.py            # relation helpers
├── protocols.py       # RecordLike, LayerData, Relations, LazyRelations, EMPTY, Flags
├── schema/
│   ├── __init__.py    # re-exports Schema, Dimension, AttributeSpec, PropertySpec, Group
│   ├── dtypes.py      # DUCK_TYPES, enum_type, enum_labels, takes
│   ├── specs.py       # Dimension, AttributeSpec, PropertySpec, Group
│   ├── schema.py      # Schema, flag_type, LAYER_COLUMN, LONG_TAIL, DERIVED
│   └── categories.py  # which labels a NULL reaches on a categorized dim
├── resolve/
│   ├── __init__.py    # re-exports Record
│   ├── fold.py        # Fold
│   ├── sources.py     # LayerSource, ParquetLayer, DirectorySource
│   ├── coords.py      # resolve_coords, Coords, mortal, resolve_groups
│   ├── owner_map.py   # fold_attributes, the owner-map fold and its shared tables
│   └── record.py      # Record, PinnedRecord
├── mutable.py         # WorkingRecord, StagedSource
├── store/
│   ├── __init__.py    # re-exports Workspace, Store, Revision
│   ├── store.py       # Workspace, Store, Revision, ancestry
│   ├── files.py       # Files, LocalFiles, RemoteFiles
│   ├── cache.py       # materialised node caches
│   ├── manifest.py    # read_schema, reconcile_schema
│   └── write.py       # write_layer, layer_data
└── tools/             # unchanged
```

Runtime imports follow the order `duck` → `schema` → `protocols` → `resolve` → `mutable` → `store` → `tools`: a module imports at runtime only from modules earlier in that order, and nothing outside `tools/` imports `tools/`. An import under `TYPE_CHECKING` may point the other way, as when `resolve/` names `Workspace` in an annotation. A subpackage's `__init__.py` re-exports its user-facing names, and `datarecord/__init__.py` imports them from there. A user and the usage pages import only from `datarecord`. Code inside the package imports a name from the module that defines it, so no internal import runs through an `__init__.py`.

`Workspace`, `Store` and `Revision` share `store/store.py` because they need each other at runtime. `Revision` is a pydantic model with a field of type `Store` and calls `Workspace.process()` when it loads from its serialised form, while `Store` constructs `Revision`s, so in two modules one of them would need a lazy import.

The modules in `resolve/` keep taking a `Workspace` rather than a connection, because they look up `workspace.con` on each call to reach the cursor of the calling thread. `mutable.py` stays one module, because `StagedSource` reads eight private members of `WorkingRecord` and the two cannot be separated.

### Renames

| today                              | becomes                                      |
| ---------------------------------- | -------------------------------------------- |
| `Schema.input_key`                 | `Schema.owner_map_key`                       |
| the tool requirement `"input_key"` | `"owner_map_key"`                            |
| `Schema.input_columns`             | `Schema.owner_map_columns`                   |
| `Resolver.inputs`                  | deleted; the fold's `owner_map`              |
| `resolve.fold_inputs`              | `owner_map.fold_attributes`                  |
| `tests/fixtures.write_input`       | `write_attribute`                            |
| `resolve.write_schema`             | moves to `tests/fixtures.py`, its one caller |

`Schema.owner_map_key` is `partial_dims` plus `attribute`, the columns that identify one row of the owner map, and `Schema.owner_map_columns` is that key plus `_layer` and the flag columns. The name "inputs" is left over from the `inputs/` directory that [DEP 17](dep-0017-results-are-attributes.md) replaced with `attributes/`. The design pages keep "fold key" for `partial_dims` alone.

## Rationale

Every caller of `Resolver` already holds the `Record` or the `Revision` beside it, so the second class adds an object to pass around without separating any concern: the fold and the `RecordLike` members that answer from it are one thing.

The protocols get their own module because they can no longer sit beside their implementation, where [the module layout](../module-layout.md) puts them today. `Record` now lives in `resolve/`, and a module holding both `Flags` and `Record` would import `resolve/fold.py`, which imports `Flags` back from it.

The path helpers stay on `Store` rather than move to a separate `Layout` object. The lower layers need the `Workspace` anyway, so a `Layout` would turn every `store` argument into two arguments to save five one-line methods.

This changes every internal import path, the monkeypatches that name a module, and the `:::` paths in `docs/api/`. About 50 test lines that read `rev.resolver` or construct a `Resolver` move to `rev.record` and the new constructor. The 24 lines that test the owner map and the folded coords through `.resolver.inputs` and `.resolver.dims` read the private `_fold` and `_coords` instead, because no public member shows the owner map or an axis row before `properties` pads it. `test_functional_groups.py:337`, which checks that an attribute's value never becomes a column of the axis file, is one of them.

## Usage

A user, and every example on the usage pages, imports from `datarecord`, as today:

```python
from datarecord import (
    Record,
    RecordLike,
    Revision,
    Schema,
    Store,
    WorkingRecord,
    Workspace,
)
```

Code inside the package, and a test reaching an internal name, imports from the defining module:

```python
# before
from datarecord.layered.resolve import Resolver
from datarecord.layered.revision import Revision
from datarecord.record import Record, RecordLike

# after
from datarecord.protocols import RecordLike
from datarecord.resolve.record import Record
from datarecord.store.store import Revision
```

A caller that held the resolver holds the record:

```python
# before
rev.resolver.sources
Record(rev.resolver)
Record(Resolver(layer_id, sources, ws, schema))

# after
rev.record.sources
rev.record
Record(layer_id, sources, ws, schema)
```

## Implementation notes

1. Merge `Resolver` into `Record` and `PinnedResolver` into `PinnedRecord`. Delete `axis`, `group`, `group_frame`, `attribute`, the enumerators, `with_source`, `inputs`, `Revision.resolver` and `_pinned`, and move `WorkingRecord` onto the mappings. Seed the coord fold of a record with staged edits from the frozen prefix's coord tables. Trim `LayerData`. One commit.
2. Move the modules into the tree above, break the store–revision cycle, and apply the renames. Point the `:::` paths in `docs/api/` at the new modules, rename the "Layered" API page to "Store" in `mkdocs.yml`, and update the paths that `AGENTS.md` cites (`record.py` and `layered/resolve.py`). One commit.
3. Add to `CHANGELOG.md` under `[Unreleased]`: `Revision.resolver` is removed, `Record` is constructed from `revision_id`, `sources`, `workspace` and `schema` instead of from a `Resolver`, and the tool requirement `"input_key"` is now `"owner_map_key"`.
4. On landing, update these pages:
   - [module layout](../module-layout.md): the tree, the dependency rule, and where the protocols live.
   - [record](../record.md#layerdata): a `LayerSource` answers `LayerData`, and a `Record` reaches the writer through the adapter. The enumerators return `set[str]`.
   - [writing](../writing.md): the same correction to the writer's input.
   - [read path](../read-path.md): "one record over one fold" and "a cursor per thread" name `Record`, not `Resolver`. "The inputs key" and the `inputs` map become the owner-map key and the owner map, and `fold_inputs` becomes `fold_attributes`.
   - [layers](../layers.md): "the inputs key" becomes the owner-map key, and `fold_inputs` becomes `fold_attributes`. A `Record` takes its workspace from its fields, not from a `Resolver`.
   - [schema](../schema.md): the dtype map is `schema/dtypes.py`, and "the inputs key" becomes the owner-map key.
   - [format](../format.md): "the inputs key" becomes the owner-map key.
5. On implementation, DEP 5 and DEP 8 take `Superseded-By` this DEP, for the parts named under `Replaces`.

Checks: no module under `src/datarecord` imports `layered`, and no runtime import breaks the dependency rule. `Resolver` and `input_key` appear nowhere in `src/`, `tests/` or `docs/design/`, except in DEPs. Folding the staged layer onto the frozen prefix's coord tables gives the same axes, groups and dead labels as folding over every layer, member order and tombstones included.

## Rejected Ideas

Folding `LayerData` into `RecordLike` would remove the adapter, but a staged axis carries a `deleted` column that `RecordLike.dims` cannot hold, so the writer would have to split each axis and join it back together.

Moving `Record` next to `Resolver` without merging the two keeps the forwarding layer, the copy in `_pinned` and the duplicate members of `Revision`.

Sharing one `_coords` between the key set and the values of each mapping in Python would halve the folds per read of a working record, but each fold would still open every layer's files; seeding the fold from the tables in DuckDB removes the file reads themselves.

A `format/` subpackage would be named after [the format](../format.md), which is the layer directory, yet `write_layer` and `_FileLayer`, the code that writes and reads that directory, would not be in it. An `edit/` subpackage would hold only `mutable.py`.

`Revision` cannot import `Store` only under `TYPE_CHECKING`, because pydantic needs the type of the `store` field at runtime.

## Open Issues

None.
