<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Module layout

The package:

```text
datarecord/                     # the standalone concept
├── schema.py                   # Dimension, AttributeSpec, Schema
├── record.py                   # Record, Frames, LazyFrames, Flags
├── mutable.py                  # WorkingRecord, the edit/commit path
├── sources.py                  # from_sources, to_sources: tables by declared name
├── layered/                    # Record and its resolution
│   ├── revision.py             # Revision, the node tree
│   ├── resolve.py              # owner-map fold
│   ├── sources.py              # LayerSource: where a layer's files are
│   └── write.py                # write_record
└── duck.py                     # connection setup, path derivation
```

The protocols live with their implementations rather than with any one consumer, because there are several: [`from_sources`](sources.md) returns a `RecordLike` and `to_sources` reads one, [`WorkingRecord`](working-record.md) satisfies it, and [`write_record`](writing.md) takes a [`LayerData`](record.md#layerdata) — the write-side protocol a source and a resolver share — wrapping a `RecordLike` in an adapter where it is handed one instead.

**Nothing in `datarecord` names a modelling framework.**
A framework meets a record through [tables keyed by declared names](sources.md), and the converter that produces them lives outside the package ([framework objects](sources.md#framework-objects)).
So importing the record layer pulls in no framework.
