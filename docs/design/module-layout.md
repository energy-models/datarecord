<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Module layout

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
│   └── categories.py  # which labels a NULL reaches on a categorized dim
├── resolve/
│   ├── __init__.py    # re-exports Record
│   ├── fold.py        # Fold
│   ├── sources.py     # LayerSource, ParquetLayer, DirectorySource
│   ├── coords.py      # resolve_coords, resolve_one, Coords, mortal
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
└── tools/             # one module per modelling framework
```

Runtime imports follow the order `duck` → `schema` → `protocols` → `resolve` → `mutable` → `store` → `tools`: a module imports at runtime only from modules earlier in that order or in its own subpackage, and nothing outside `tools/` imports `tools/`. An import under `TYPE_CHECKING` may point the other way, as when `resolve/` names `Workspace` in an annotation. A subpackage's `__init__.py` re-exports its user-facing names, and `datarecord/__init__.py` imports them from there, so a user imports only from `datarecord`. Code inside the package imports a name from the module that defines it, so no internal import runs through an `__init__.py`.

`Workspace`, `Store` and `Revision` share `store/store.py` because they need each other at runtime: `Revision` is a pydantic model with a field of type `Store` and attaches to `Workspace.process()` when it loads from its serialised form, while `Store` constructs `Revision`s. The modules in `resolve/` take a `Workspace` rather than a connection and look up `workspace.con` on each call, so they reach the cursor of the calling thread. Where a connection and a location come from is `store/`'s, so `duck.py` holds no state. `mutable.py` is one module because `StagedSource` reads the private members of `WorkingRecord`.

"[Depends on `duckdb`, `pydantic` and `mathspec`, and on nothing else](index.md#what-a-data-record-is)" is achieved at this layer — `datarecord/tools/` is where a framework-specific tool lives instead (below), and nothing under it is imported by the package's own `__init__.py`.

The protocols live in `protocols.py` rather than with any one consumer, because there are several: a [tool](tools.md) both implements and consumes `RecordLike`, [`WorkingRecord`](working-record.md) satisfies it, and [the writer](writing.md) takes a [`LayerData`](record.md#layerdata) — the write-side protocol a layer source answers — wrapping a `RecordLike` in an adapter where a tool hands one over instead. Nor can they sit beside `Record`: the fold in `resolve/` imports `Flags`, so a module holding both would import the fold back.

A tool lives outside this core, under `datarecord/tools/<name>.py` — one module per modelling framework, imported explicitly (`from datarecord.tools.pypsa import PyPSA`) rather than through `datarecord.tools` itself, which imports none of them.
What decides the side is one question: **does it name a modelling framework?** Nothing in `datarecord`'s core may.
The dependency runs strictly one way, so importing the record layer pulls in no framework, and importing one tool pulls in no other.
