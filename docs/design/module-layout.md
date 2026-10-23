<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Module layout

The target, once the split below is done:

```text
datarecord/                     # the standalone concept
├── schema.py                   # Dimension, AttributeSpec, PropertySpec, Schema
├── record.py                   # RecordLike, LayerData, Relations, LazyRelations, Flags, Record
├── store.py                    # Workspace, Store
├── files.py                    # Files: manifests and revision files, local or remote
├── mutable.py                  # WorkingRecord, the edit path
├── layered/                    # a record's resolution
│   ├── revision.py             # Revision, the node tree
│   ├── resolve.py              # owner-map fold
│   ├── categories.py           # which labels a NULL reaches on a categorized dim
│   ├── sources.py              # LayerSource: where a layer's files are
│   └── write.py                # the writer root, child and ws.write share
└── duck.py                     # relation helpers
```

`Record` lives in the top-level `record.py`, beside the protocol it is the one resolver-backed implementation of: a thin set of mappings handing a `Resolver`'s relations over bare. It names the `Resolver` for typing alone, so `layered/` imports it without a cycle. Where a connection and a location come from is `store.py`'s, so `duck.py` holds no state.

"[Depends on `duckdb`, `pydantic` and `mathspec`, and on nothing else](index.md#what-a-data-record-is)" is achieved at this layer — `datarecord/tools/` is where a framework-specific tool lives instead (below), and nothing under it is imported by the package's own `__init__.py`.

The protocols live with their implementations rather than with any one consumer, because there are several: a [tool](tools.md) both implements and consumes `RecordLike`, [`WorkingRecord`](working-record.md) satisfies it, and [the writer](writing.md) takes a [`LayerData`](record.md#layerdata) — the write-side protocol a source and a resolver share — wrapping a `RecordLike` in an adapter where a tool hands one over instead.

A tool lives outside this core, under `datarecord/tools/<name>.py` — one module per modelling framework, imported explicitly (`from datarecord.tools.pypsa import PyPSA`) rather than through `datarecord.tools` itself, which imports none of them.
What decides the side is one question: **does it name a modelling framework?** Nothing in `datarecord`'s core may.
The dependency runs strictly one way, so importing the record layer pulls in no framework, and importing one tool pulls in no other.
