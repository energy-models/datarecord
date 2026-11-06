<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The package

datarecord is a core of subpackages that import each other in one direction, and a `tools` subpackage that nothing in the core imports.

```text
datarecord/
├── __init__.py        # every public name, which a user imports from here
├── duck.py            # relation helpers, transactions and shared tables
├── protocols.py       # RecordLike, LayerData, Relations, LazyRelations, EMPTY, Flags
├── schema/
│   ├── __init__.py    # re-exports Schema, Dimension, AttributeSpec, PropertySpec
│   ├── dtypes.py      # DUCK_TYPES, enum_type, enum_labels, takes
│   ├── specs.py       # Dimension, AttributeSpec, PropertySpec
│   ├── schema.py      # Schema, flag_type, LAYER_COLUMN, LONG_TAIL, DERIVED
│   └── categories.py  # selecting a categorised dim's labels, and their defaults
├── settle.py          # settle, the broadcast rule over long rows
├── select.py          # select, Selection
├── resolve/
│   ├── __init__.py    # re-exports Record
│   ├── sources.py     # LayerSource, ParquetLayer, DirectorySource
│   ├── coords.py      # Coords, mortal: folded axes and dead labels
│   ├── owner_keys.py  # owner_keys: one layer's keys as it wrote them
│   ├── fold.py        # Fold: a revision's folded axes, owner map and reads
│   ├── owner_map.py   # fold_layer: the owner map folded one layer at a time
│   └── record.py      # Record
├── mutable.py         # WorkingRecord, StagedSource
├── store/
│   ├── __init__.py    # re-exports Workspace, Store, Claim, Revision, Manifest
│   ├── files.py       # Files, LocalFiles, RemoteFiles
│   ├── manifest.py    # Manifest, LayerFiles, ClaimFile
│   ├── cache.py       # materialise, materialised, sources_to_read
│   ├── write.py       # write_layer, layer_data
│   └── store.py       # Workspace, Store, Claim, Revision, ancestry
└── tools/
    ├── __init__.py    # imports no tool module
    ├── base.py        # Tool, Schema, Attr, Requirements, UnsupportedRecordError
    └── pypsa.py       # PyPSA
```

## The import order

Runtime imports follow the order `duck` → `schema` → `protocols` → `settle` → `select` → `resolve` → `mutable` → `store` → `tools`. A module imports at runtime only from modules earlier in that order or in its own subpackage. An import under `TYPE_CHECKING` may point the other way, as when `resolve/record.py` names `Workspace` in an annotation.

A subpackage's `__init__.py` re-exports its public names, and `datarecord/__init__.py` imports them from there, so a user imports from `datarecord` alone. Code inside the package imports a name from the module that defines it, so no internal import runs through an `__init__.py`.

Some modules are one module because their classes need each other at runtime:

- `store/store.py` contains `Workspace`, `Store`, `Claim` and `Revision`. `Claim` and `Revision` are pydantic models with a field of type `Store`, and `Store` constructs both.
- `mutable.py` contains `WorkingRecord` and `StagedSource`, because `StagedSource` reads private members of `WorkingRecord`.
- `protocols.py` contains the protocols apart from `Record`, because `resolve/fold.py` imports `Flags`, and a module that defined both would import the fold back.

`store/` sets where a connection and a location come from, so `duck.py` contains no state. The modules in `resolve/` take a `Workspace` and read `workspace.con` on each call, so each call runs on the calling thread's cursor.

## Core and tools

Code that refers to a modelling framework lives under `datarecord/tools/`, and no other code may refer to one. So the core depends only on `duckdb`, `pydantic` and `mathspec`.

A tool is one module per framework, `datarecord/tools/<name>.py`, and a caller imports it explicitly:

```python
from datarecord.tools.pypsa import PyPSA
```

Neither `datarecord/__init__.py` nor `datarecord/tools/__init__.py` imports a tool module. Importing the core pulls in no framework, and importing one tool pulls in no other. A tool's framework is an optional extra, such as `datarecord[pypsa]`.

!!! info "See also"

    [Tools](../reference/tools.md) describes what a tool reads from a record and writes back, and [contributing](contributing.md) the workflow for a change.
