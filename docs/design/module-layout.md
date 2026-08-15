# Module layout

The target, once the split below is done:

```text
datarecord/                     # the standalone concept
├── schema.py                   # Dimension, AttributeSpec, Schema
├── record.py                   # Record, Frames, LazyFrames, Flags
├── directory.py                # DirectoryRecord
├── mutable.py                  # WorkingRecord, the edit/commit path
├── layered/                    # LayeredRecord and its resolution
│   ├── revision.py             # Revision, the node tree
│   ├── resolve.py              # owner-map fold
│   └── write.py                # write_record
└── duck.py                     # connection setup, path derivation
```

"[Depends on `duckdb`, `narwhals` and `pydantic`, and on nothing else](index.md#what-a-data-record-is)" is achieved at this layer — `datarecord/tools/` is where a framework-specific tool lives instead (below), and nothing under it is imported by the package's own `__init__.py`.

The protocol lives with its implementations rather than with any one consumer, because there are several: [`write_record`](writing.md) consumes a `Record`, [a tool](tools.md) both implements and consumes one, and [`WorkingRecord`](working-record.md) satisfies it.

A tool lives outside this core, under `datarecord/tools/<name>.py` — one module per modelling framework, imported explicitly (`from datarecord.tools.pypsa import PyPSA`) rather than through `datarecord.tools` itself, which imports none of them.
What decides the side is one question: **does it name a modelling framework?** Nothing in `datarecord`'s core may.
The dependency runs strictly one way, so importing the record layer pulls in no framework, and importing one tool pulls in no other.
