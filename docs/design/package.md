<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The package

The package is a core of subpackages that import each other in one direction at runtime, and a `tools/` subpackage beside it.
Nothing in the core names a modelling framework.
Each framework lives in one tool module under `datarecord/tools/`, which a caller imports explicitly.

```text
datarecord/
├── __init__.py        # every public name; where a user imports from
├── duck.py            # relation helpers
├── protocols.py       # RecordLike, LayerData, Relations, LazyRelations, EMPTY, Flags
├── schema/
│   ├── __init__.py    # re-exports Schema, Dimension, AttributeSpec, PropertySpec, Group
│   ├── dtypes.py      # DUCK_TYPES, enum_type, enum_labels, takes
│   ├── specs.py       # Dimension, AttributeSpec, PropertySpec, Group
│   ├── schema.py      # Schema, flag_type, LAYER_COLUMN, LONG_TAIL, DERIVED
│   └── categories.py  # selecting a categorized dim's labels, and their defaults
├── resolve/
│   ├── __init__.py    # re-exports Record
│   ├── fold.py        # Fold
│   ├── sources.py     # LayerSource, ParquetLayer, DirectorySource
│   ├── coords.py      # resolve_coords, resolve_one, Coords, mortal
│   ├── owner_map.py   # fold_attributes, the owner-map fold over keys as written, and its shared tables
│   └── record.py      # Record, PinnedRecord
├── mutable.py         # WorkingRecord, StagedSource
├── settle.py          # settle, the broadcast rule over long rows
├── store/
│   ├── __init__.py    # re-exports Workspace, Store, Revision
│   ├── store.py       # Workspace, Store, Revision, ancestry
│   ├── files.py       # Files, LocalFiles, RemoteFiles
│   ├── cache.py       # materialised node caches
│   ├── manifest.py    # read_schema, reconcile_schema
│   └── write.py       # write_layer, layer_data
└── tools/
    ├── base.py        # Tool, Schema, Attr, Requirements, UnsupportedRecordError
    └── pypsa.py       # PyPSA
```

## The import order

Runtime imports follow the order `duck` → `schema` → `protocols` → `resolve` → `mutable` → `store` → `tools`.
A module imports at runtime only from modules earlier in that order or in its own subpackage.
Nothing outside `tools/` imports `tools/`.
An import under `TYPE_CHECKING` may point the other way, as when `resolve/` names `Workspace` in an annotation.

A subpackage's `__init__.py` re-exports its user-facing names, and `datarecord/__init__.py` imports them from there, so a user imports from `datarecord` alone.
Code inside the package imports a name from the module that defines it, so no internal import runs through an `__init__.py`.

`Workspace`, `Store` and `Revision` share `store/store.py` because they need each other at runtime.
`Revision` is a pydantic model with a field of type `Store`, and it attaches to `Workspace.process()` when it loads from its serialised form.
`Store` constructs `Revision`s.
The modules in `resolve/` take a `Workspace` and look up `workspace.con` on each call, so they reach the calling thread's cursor ([a cursor per thread](storage/concurrency.md#a-cursor-per-thread)).
`store/` decides where a connection and a location come from, so `duck.py` holds no state.
`mutable.py` is one module because `StagedSource` reads the private members of `WorkingRecord`.

The protocols live in `protocols.py` because several modules consume them.
A [tool](tools/index.md) both implements and consumes `RecordLike`, and [`WorkingRecord`](use/editing.md) satisfies it.
[The writer](use/writing.md) takes a [`LayerData`](use/writing.md#layerdata), the write-side protocol a layer source answers, and wraps a `RecordLike` in an adapter where a tool hands one over.
The protocols cannot sit beside `Record` either: the fold in `resolve/` imports `Flags`, so a module that held both would import the fold back.

## Core and tools

One question decides on which side code lives: does it name a modelling framework?
Nothing in the core may.
This is how the core [depends on `duckdb`, `pydantic` and `mathspec` alone](index.md).

A tool lives under `datarecord/tools/<name>.py`, one module per modelling framework.
A caller imports it explicitly, as in `from datarecord.tools.pypsa import PyPSA`.
`datarecord.tools` itself imports no tool module, and the package's own `__init__.py` imports nothing under `tools/`.
The dependency runs strictly one way, so importing the record layer pulls in no framework, and importing one tool pulls in no other.
