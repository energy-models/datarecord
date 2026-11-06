<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 19: one Record and a package layout

| DEP           | 19                                                                                                                                                               |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title         | one Record and a package layout                                                                                                                                  |
| Author        | Jonas Hoersch                                                                                                                                                    |
| Status        | Implemented (2026-10-21)                                                                                                                                         |
| Requires      | [DEP 18](dep-0018-drop-narwhals.md)                                                                                                                              |
| Created       | 2026-10-21                                                                                                                                                       |
| Version       | Unreleased                                                                                                                                                       |
| Replaces      | [DEP 5](dep-0005-one-record-over-a-node-cache.md) (`Record` over a `NodeCache`), [DEP 8](dep-0008-one-interface-source-and-fold.md) (a fold that is `LayerData`) |
| Superseded-By | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its re-added key introduced where it is re-added)                                                          |

The design landed in [the package](../package.md), [the record](../../reference/record.md), [the fold](../../reference/resolution.md), [layers](../../reference/layers.md) and [editing](../../reference/editing.md). Where they and this DEP differ, follow those pages.

## Abstract

`Record` absorbs `Resolver`, so one class folds a record's layers and implements `RecordLike`. A record with staged edits folds only its staged layer per read. The package splits into three subpackages, `schema/`, `resolve/` and `store/`, under one dependency rule, which breaks the import cycle between `store.py` and `layered/revision.py`. The internal names that still contain "inputs" are renamed after the owner map.

## Motivation

[DEP 8](dep-0008-one-interface-source-and-fold.md) split `Resolver` from `Record` so that the writer could read a `Resolver` as `LayerData`. Since `WorkingRecord.commit` was removed in a17bf6e, the writer no longer does. Each member of `Record` still forwards to one member of `Resolver`, and code that needs the record carries both objects. `Revision` caches a `Resolver` but builds a new `Record`, with an empty cache of key sets, on every access to `rev.record`.

To write a working record whole, `_pinned` sets a `PinnedResolver` on a copy of the `Record` through `object.__setattr__`. `Resolver.axes()`, `Resolver.groups()`, `group_frame` and `LayerData.frozen` have no caller.

With edits staged, every read of the axes, groups or dead labels folds again from the deepest materialised revision, opening every layer file above it. The owner map already folds only the staged step.

`store.py` imports `layered/revision.py` lazily in three methods, because `revision.py` imports `store.py`. `schema.py` (1,554 lines) and `layered/resolve.py` (1,499 lines) each contain several separable parts.

## Specification

### One `Record`

`Record` is a frozen dataclass in `resolve/record.py` with the four fields of `Resolver`: `revision_id`, `sources`, `workspace` and `schema`. `sources` lists the layers root first, truncated at the deepest materialised one.

You read a `Record` through the `RecordLike` members, and its other public members are its four fields, `con` and `frozen`. The `Resolver` methods that read the same rows a second way are deleted: `axis`, `group`, `group_frame`, `attribute`, `axes()`, `groups()`, `attributes()`, `with_source` and `inputs`. The private `_fold` has the owner map and `_coords` the folded axes, groups and dead labels. `_relation` resolves one attribute, and raises `KeyError` where the owner map assigns it to no layer. That happens only to a key set read before `rollback()` and used after it. `Record` has its cache of key sets, which now checks `self.frozen`.

On a record with staged edits, `_coords` folds only the layers after the frozen prefix onto that prefix's coord tables. `resolve_coords` takes those shared DuckDB tables as its seed, as `_fold_map` does, so the fold opens no file. The key set of `groups` comes from one such fold, not one per group.

A label or tuple that one layer removes and a later layer adds again is introduced where it is added again. The seeded tables keep a tombstoned key at most as a dead label, which has no place in axis order. `fold_axis` therefore counts only the rows of a key that are deeper than its deepest tombstone. The seeded fold, the fold over every layer and a fold over a materialised revision then agree.

`WorkingRecord` reads through `dims`, `properties`, `groups` and `attributes` of itself and of its base where it called the deleted methods. The base axis rows that it unions into a layer that owns the dim whole come from `properties`. `properties` adds each declared property that no layer stored as a typed NULL column, so a written axis layer reads back as before.

`PinnedRecord` subclasses `Record` and replaces `PinnedResolver`. It folds the unfrozen tail once, into a table that `release()` drops. It creates the table at the first access to `_fold`, so a write that fails before `write_layer` enters its `try` leaves no table behind. `layer_data` builds one from any unfrozen `Record`, and `write_layer` releases it after the write. `WorkingRecord` calls `Record.__init__` with its base's fields and `[*base.sources, StagedSource(...)]`, and sets `_base` to the base.

`Revision` caches its `Record`, and `materialise` clears that cache. `Revision.resolver` is deleted, and `Workspace.at` and `Store.layer` construct a `Record`. A `Record` looks up `workspace.con` on each call, so several threads can read one `Record`. A `PinnedRecord` has its relations on the cursor of the thread that built it.

[DEP 5](dep-0005-one-record-over-a-node-cache.md) made `Record` the interface over a `NodeCache`, and it is now the fold itself. Against [DEP 8](dep-0008-one-interface-source-and-fold.md), the writer reads no fold as `LayerData`, and `Record` does not implement `LayerData`.

### `LayerData`

`LayerData` stays separate from `RecordLike`. It loses `frozen`, which the writer never reads, and its enumerators return `set[str]` instead of `Iterable[str]`, a type that existed only so that a `Resolver` could return an ordered `list`. A `LayerSource` has its own `frozen`, because the fold reads it.

### The package layout

```text
datarecord/
├── __init__.py        # every public name
├── duck.py            # relation helpers
├── protocols.py       # RecordLike, LayerData, Relations, LazyRelations, EMPTY, Flags
├── schema/            # dtypes.py, specs.py, schema.py, categories.py
├── resolve/           # fold.py, sources.py, coords.py, owner_map.py, record.py (Record, PinnedRecord)
├── mutable.py         # WorkingRecord, StagedSource
├── store/             # store.py (Workspace, Store, Revision), files.py, cache.py, manifest.py, write.py
└── tools/             # unchanged
```

Runtime imports follow the order `duck`, `schema`, `protocols`, `resolve`, `mutable`, `store`, `tools`. A module imports at runtime only from modules earlier in that order, and nothing outside `tools/` imports `tools/`. An import under `TYPE_CHECKING` may point the other way. A subpackage's `__init__.py` re-exports its user-facing names, and `datarecord/__init__.py` imports them from there. A user imports only from `datarecord`, and code inside the package imports a name from the module that defines it.

`Workspace`, `Store` and `Revision` share `store/store.py` because they need each other at runtime. The modules in `resolve/` take a `Workspace`, not a connection, to get the calling thread's cursor. `mutable.py` stays one module, because `StagedSource` reads eight private members of `WorkingRecord`.

### Renames

| Before                             | Under this DEP                               |
| ---------------------------------- | -------------------------------------------- |
| `Schema.input_key`                 | `Schema.owner_map_key`                       |
| the tool requirement `"input_key"` | `"owner_map_key"`                            |
| `Schema.input_columns`             | `Schema.owner_map_columns`                   |
| `Resolver.inputs`                  | deleted, in favour of the fold's `owner_map` |
| `resolve.fold_inputs`              | `owner_map.fold_attributes`                  |
| `tests/fixtures.write_input`       | `write_attribute`                            |
| `resolve.write_schema`             | moved to `tests/fixtures.py`, its one caller |

`Schema.owner_map_key` is `partial_dims` plus `attribute`, and `Schema.owner_map_columns` is that key plus `_layer` and the flag columns. "Inputs" is left over from the `inputs/` directory that [DEP 17](dep-0017-results-are-attributes.md) replaced. The pages keep "fold key" for `partial_dims` alone.

## Rationale

Every caller of `Resolver` already has the `Record` or the `Revision` beside it. Each caller needs both the fold and the members that read from it, so a second class only adds forwarding.

The protocols get their own module because `Record` now lives in `resolve/`. A module with both `Flags` and `Record` would import `resolve/fold.py`, which imports `Flags` back from it.

A `Revision` is a pydantic model with a `Store` field and calls `Workspace.process()` when it loads, while `Store` constructs `Revision`s. In two modules, one of them would need a lazy import.

What it costs: every internal import path changes, with the monkeypatches and the API pages' `:::` paths. About 50 test lines move to `rev.record`. Another 24 read the private `_fold` and `_coords`, because no public member exposes the owner map or the folded coords.

## Usage

```python
# before
from datarecord.layered.resolve import Resolver
from datarecord.layered.revision import Revision
rev.resolver.sources
Record(Resolver(layer_id, sources, ws, schema))

# after
from datarecord.resolve.record import Record
from datarecord.store.store import Revision
rev.record.sources
Record(layer_id, sources, ws, schema)
```

## Implementation notes

1. One commit merges `Resolver` into `Record`, moves `WorkingRecord` onto the mappings, seeds the staged coord fold and trims `LayerData`.
2. One commit moves the modules, breaks the import cycle and applies the renames.
3. `CHANGELOG.md`, and the pages that name `Resolver`, the inputs key or `fold_inputs`.
4. DEP 5 and DEP 8 take `Superseded-By` this DEP, for the parts named under `Replaces`.

Checks: no module imports `layered`, and no runtime import breaks the dependency rule. `Resolver` and `input_key` appear nowhere outside the DEPs. Folding the staged layer onto the frozen prefix's coord tables gives the same axes, groups and dead labels as folding over every layer, axis order and tombstones included.

## Rejected ideas

- Folding `LayerData` into `RecordLike` would remove the adapter. A staged axis has a `deleted` column that `RecordLike.dims` cannot have, so the writer would split each axis and join it back.
- Moving `Record` next to `Resolver` without merging keeps the forwarding, the copy in `_pinned` and the duplicate members of `Revision`.
- A separate `Layout` object for the path helpers on `Store`: the lower layers need the `Workspace` anyway, so it turns every `store` argument into two, to save five one-line methods.
- One `_coords` shared by each mapping's key set and values halves the folds per read. Each fold still opens every layer's files, unless it starts from a seed in DuckDB.
- A `format/` subpackage would not contain `write_layer` and `_FileLayer`, the code that writes and reads the format. An `edit/` subpackage would contain only `mutable.py`.
- `Revision` importing `Store` only under `TYPE_CHECKING` fails, because pydantic needs the `store` field's type at runtime.
