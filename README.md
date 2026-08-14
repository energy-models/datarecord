# datarecord

[![CI](https://img.shields.io/github/actions/workflow/status/energy-models/datarecord/ci.yml?style=flat-square&branch=main)](https://github.com/energy-models/datarecord/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/datarecord?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/datarecord)
[![pypi-version](https://img.shields.io/pypi/v/datarecord.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/datarecord)
[![python-version](https://img.shields.io/pypi/pyversions/datarecord?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/datarecord)

Dimensioned attribute data with a declared schema.

A record holds **components** (named members of a type), **connections** between components and buses, **attribute values** over both, and the **axes** those values vary along. A schema declares what may exist; the data says what does.

Records stack: a layer is a partial record on top of a parent, resolved last-writer-wins, so a scenario variant costs the rows it changes rather than a copy of everything. On disk a record is a plain parquet directory that a tool knowing nothing about this package can read.

`datarecord` depends only on `duckdb`, `narwhals` and `pydantic`. It names no modelling framework — a framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.

[`docs/design-documents/data-records.md`](docs/design-documents/data-records.md) is the authoritative design; the `§N` references below point into it.

## Installation

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
```

## The `Record` protocol

Everything a consumer codes against. It is read-only, and structural — a plain directory, a hundred-layer overlay, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds (§3).

```python
record.schema  # what may exist: the axes, the attributes (§5)
record.dims["scenario"]  # axis frames, keyed by dim
record.components["Generator"]  # wide member rows, keyed by component type
record.connections["Link"]  # component↔bus rows, keyed by component type (§3.2)
record.attributes["p_max_pu"]  # long input frames, keyed by attribute (§4.2)
record.outputs["p"]  # long result frames, keyed by attribute (§7.4)
record.flags("Generator")  # which axes each attribute actually uses (§3.6)
```

Every frame is a `narwhals.LazyFrame` — a plan, not data. Nothing is read until you `.collect()`, and listing the keys reads nothing at all (§3.5).

```python
gens = record.components["Generator"].collect().to_pandas()
```

`components`, `connections` and `dims` are **wide** — one row per thing. `attributes` and `outputs` are **long** — one row per value:

```text
name | bus | <one column per declared dim> | attribute | breakpoint | value
```

A NULL dim column means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep (§3.3). `bus` is non-NULL only for a value belonging to one connection rather than to the component (§3.2); `breakpoint` carries the abscissa of a piecewise-linear value (§3.1). A coordinate no row covers takes the attribute's `default` from the schema.

There is no `component_type` column, and none in the mapping's key either — `attributes["p_max_pu"]` holds every type's rows together. A `name` identifies one component **across every type**, so the type is something the record knows about a name rather than part of its address (§4.3). To scope to one type, join `components` on `name`.

`flags(ctype)` answers which axes an attribute's rows actually use, for a whole type in one query, so a consumer can plan its reads without opening a file:

```python
flags = record.flags("Generator")
set(flags)  # which attributes this type has at all
"timestep" in flags["p_max_pu"].varies  # some row sets it
"timestep" in flags["p_max_pu"].broadcast  # some row leaves it NULL
flags["marginal_cost"].breakpoints  # some row carries a curve
```

The two sets are **not** complements: a dim in both means this type's components disagree — some carry a per-timestep series, others a single constant row — which is the instruction to use both containers, not an ambiguity (§3.6).

## The schema

One schema per record, written down as `manifest.json` (§5).

```python
from datarecord import Schema, Dimension, AttributeSpec

schema = Schema(
    version=1,
    dimensions={
        "scenario": Dimension(dtype="VARCHAR", keys={"component", "connection"}),
        "timestep": Dimension(dtype="TIMESTAMP"),
    },
    attributes={
        "Bus": {},
        "Generator": {
            "p_nom": AttributeSpec(dtype="DOUBLE", default=0.0, unit="MW"),
            "carrier": AttributeSpec(dtype="VARCHAR"),
            "p_max_pu": AttributeSpec(
                dtype="DOUBLE", dims={"scenario", "timestep"}, default=1.0
            ),
        },
    },
    partial={"scenario"},
)
```

- **`Dimension`** declares one axis: its `dtype`, whether it `keys` the entity tables (a component exists _per scenario_, §5.3), and `within` for an axis whose labels identify a point only inside another's — multi-period time being the case (§5.4).
- **`AttributeSpec.dims`** says which axes an attribute may vary over. It is what makes a scenario-varying `p_nom` a violation rather than data, and it decides the file split: varying over nothing puts an attribute in `dims/components/`, anything else in `inputs/` (§4.1, §5.2).
- **`partial`** is the layering granularity — which dims a layer may patch value by value. `scenario` is patchable; `timestep` is not, so a patch to one hour restates that component's whole series rather than leaving a curve resolved across two layers with a hole in it (§5.5). Omit it for a record with no layers.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless (§5.8).

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible (§5.7).

## Reading a directory

A parquet directory is a record. Nothing about layers is involved:

```python
from datarecord import DirectoryRecord, connect

con = connect()
record = DirectoryRecord("s3://bucket/my-record/", con)
record.attributes["p_nom"].collect()
```

The layout is the whole format (§4):

```text
record/
├── manifest.json                   # the schema (§5)
├── dims/
│   ├── components/<Type>.parquet   # members + non-varying attribute columns
│   ├── connections/<Type>.parquet  # component↔bus connections (§3.2)
│   └── <dim>s.parquet              # one axis table per declared dim
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```

## Layers

A `Revision` is a node in a tree of layers. Each node adds one layer; what it resolves to is that layer over its ancestors', last-writer-wins (§6).

```python
from datarecord import Revision, connect

con = connect(base_uri="s3://bucket/my-record")

root = Revision.create(con)  # a new node
child = root.child()  # branch off it
record = child.record  # the resolved overlay, as a `Record`
```

A layer's data is **write-once**, so any node may be a parent and no cache ever needs invalidating (§6.1). Branching is several children sharing a parent by pointing at it, not by duplication.

`materialise()` writes a node's owner map and resolved dims under `layers/<id>/resolved/`, so descendants' reads stop there instead of walking to the root. Purely additive — a policy, not a lifecycle step, changing no answer, only how many layers a read touches (§6.2):

```python
child.materialise()
```

`Revision.get(uuid, con)` loads one by id; `revision.ancestry()` gives the root→node path.

## Editing

`WorkingRecord` is a record plus pending edits, held in memory and not yet written anywhere. It **satisfies `Record`**, so what it reads is the data with its pending edits applied — an edit can be read back, or the record handed to something that only knows how to read, without committing (§9).

```python
from datarecord import WorkingRecord, NewChild, Directory

w = WorkingRecord(root.record, con)
```

### `set`

```python
w.set("p_nom", 150.0, names=["wind1", "wind2"])  # broadcast
w.set("p_nom", [150.0, 80.0], names=["wind1", "wind2"])  # per name, positional
w.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per name, keyed
w.set("p_max_pu", frame, names=["wind1"])  # a long frame
w.set("efficiency", 0.9, names=["dc"], bus="north")  # a connection (§3.2)
w.set("p_nom", 200.0, names=["wind1"], scenario="high")  # scoped to one scenario
w.set("p_nom", nw.col("value") * 1.1, names=["wind1"])  # derived (§9.3)
w.set("p", solved_frame, kind="outputs")  # a result (§9.3.1)
```

**There is no `component_type` keyword.** A name identifies one component across every type, so the record looks the type up and validates each name against _its own_ type's `AttributeSpec` — one call may legitimately span types (§9.2).

`names=None` means every component whose type declares this attribute. `bus` names a connection; every other keyword is a dim, and its absence means "every value of that dim" by the NULL broadcast rule.

An `nw.Expr` value is a **function of the current value**: it reads the resolved value including earlier pending edits, so two such calls compose, and what gets staged is the result rather than the expression (§9.3). A named target that resolves to no row raises — the caller asked for those rows to take a new value and there is nothing to compute one from.

### `add` / `remove` / `connect` / `disconnect`

```python
import pandas as pd

w.add("Bus", pd.DataFrame({"name": ["north", "south"]}))
w.add(
    "Generator",
    pd.DataFrame(
        {
            "name": ["wind1", "wind2"],
            "carrier": ["wind", "wind"],
            "p_nom": [100.0, 80.0],
        }
    ),
)

w.remove("Generator", ["old_coal"])
w.remove("Generator", ["old_coal"], scenario="high")  # one scenario only

w.connect(
    "Link",
    pd.DataFrame(
        {
            "name": ["dc", "dc"],
            "bus": ["north", "south"],
            "role": ["bus0", "bus1"],
        }
    ),
)
w.disconnect("Link", [("dc", "south")])
```

`add` takes a wide frame and splits it by the schema: non-varying columns stay in `dims/components/`, varying ones become `inputs/` rows (§4.1). It keeps its `ctype` argument where `set` loses it — this is the call that _establishes_ what a name's type is, and where record-wide name uniqueness is enforced (§9.5). A component exists by virtue of its member row, so `add` is not a sequence of `set` calls: adding a bus with no attributes makes the point.

`remove` stages a tombstone and need not enumerate what it deletes — one row per key, and the fold applies it to every attribute (§6.3).

### Inspecting and committing

```python
w.pending  # Pending(attributes={...}, components={...},
#         connections={...}, tombstones={...})
bool(w.pending)  # whether anything is staged
w.rollback()  # discard everything staged
```

`pending` is a derived summary computed on access — a `GROUP BY` over the staging tables, not a second place rows live (§9.6). Staged rows live in DuckDB tables on the record's connection, so they vanish with it and never touch disk (§9.9).

Nothing touches the record until `commit`, which takes one of two targets (§9.7):

```python
new = w.commit(NewChild())  # a patch layer under a new child; returns it
w.commit(Directory("out/"))  # a standalone record, flattened; returns None
```

`NewChild()` writes **only the edits** and the fold resolves the rest from the parent. `Directory(uri)` writes **the resolved result** — what is staged plus what the record already reads — since there is no parent to resolve against.

The layer lands in the **child**, never in the node you branched from — layers are write-once (§6.1) — so it is the returned node that reads the edits back:

```python
new.record.attributes["p_nom"].collect()
```

`NewChild()` branches from whichever node the `WorkingRecord` was built over, which is what a caller means every time. Pass one explicitly — `NewChild(other_revision)` — only to re-parent the edits elsewhere; a `WorkingRecord` over a base that is not a node in a layer tree (a `DirectoryRecord`, a framework object) has nothing to default to and must supply one.

Staged rows are appended, never updated, so the same coordinate may be staged repeatedly; commit collapses to last-write-wins per coordinate. A `remove` after a `set` wins regardless of order — a deleted component has no attributes — and an `add` after a `remove` brings the component back (§9.7).

Neither target carries a **base's** results across: an edit changes the inputs a result was computed from. What a commit does carry is results staged into this record through `set(..., kind="outputs")`, which were computed against these pending inputs (§9.3.1).

Edit-level mistakes are caught when the edit is **staged**, not at commit, so a typo is reported at the line that typed it (§9.8).

## Writing a whole record

```python
from datarecord import write_record

write_record(revision.id, source, con)  # as a revision's layer
write_record(None, source, con, uri="out/")  # as a standalone directory
```

`source` is anything satisfying `Record` — including a framework object presenting itself as one, which is what puts read and write on a single seam. An existing layer directory is an error rather than an overwrite: frames are staged into a sibling path and renamed on success, so a source that fails validation leaves no layer rather than half of one (§8).

## Consuming a record: tools

A **tool** is one modelling framework's view of a record. Tools live under `datarecord/tools/`, are reached by importing them (no registry), and nothing in the core imports one — so importing the record layer pulls in no framework (§10, §11).

```python
from datarecord.tools.pypsa import PyPSA

missing = PyPSA.verify(record)
if missing:
    raise RuntimeError(missing.describe())

n = PyPSA.build(record)  # a pypsa.Network
n.optimize()

record_back = PyPSA.to_datarecord(n)  # the inverse; a `Record`
results = PyPSA.results(n)  # Frames, in the long schema
```

`verify` catches what the record layer cannot — a component type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks its constant-versus-varying split — and returns a falsy `Requirements` when the record is usable. `build` raises `UnsupportedRecordError` rather than producing a partial model.

`results` returns the same `Frames` type `Record.outputs` presents, so a solve's output goes straight back:

```python
w = WorkingRecord(record, con)
w.set("p_max_pu", 0.8, names=["wind1"])
n = PyPSA.build(w)  # a WorkingRecord is a Record
n.optimize()
for attr, frame in PyPSA.results(n).items():
    w.set(attr, frame, kind="outputs")
w.commit(NewChild())  # one layer, inputs and results together
```

Two checks are skipped for `kind="outputs"`: the attribute need not be schema-declared, and a result's `name` need not resolve to a declared member — a solve may produce rows for a component type it derived rather than read (§9.3.1).

## Connections

```python
from datarecord import connect

con = connect(":memory:", base_uri="s3://bucket/my-record")
```

`connect` opens a DuckDB connection carrying the `revisions` table and the path macros, loading `httpfs` and S3 credentials only for a remote `base_uri`. The connection is passed as a parameter throughout, never a module global, and is scoped to one record root — which is how the schema beside its layers is found without a separate argument. `base_uri` defaults to the `DATARECORD_BASE_URI` environment variable.

## Development

This project is managed by [pixi](https://pixi.sh):

```bash
git clone https://github.com/energy-models/datarecord
cd datarecord

pixi run test    # the test suite
pixi run lint    # ruff, prettier, taplo, typos, zizmor, reuse, mypy
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow and conventions, and [`AGENTS.md`](AGENTS.md) for how AI-assisted contributions must be marked.
