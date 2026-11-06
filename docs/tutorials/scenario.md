<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# A scenario on a branch

In this tutorial you branch a high-wind scenario off a record, override one value in it and read the result through both revisions. It takes about 15 minutes, and it starts from the record of [Your first record](first-record.md).

## The base record

Run this to build the record of the first tutorial again, committed as the revision `base`:

```python
import tempfile
from dataclasses import dataclass, field

from duckdb import DuckDBPyConnection

from datarecord import (
    AttributeSpec,
    Dimension,
    PropertySpec,
    Schema,
    WorkingRecord,
    Workspace,
)

schema = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "ctype": Dimension(one_of=["Generator", "Load"]),
        "bus": Dimension(dtype="str"),
        "carrier": Dimension(dtype="str"),
        "snapshot": Dimension(dtype="int"),
        "connection": Dimension(over=["entity", "bus"]),
    },
    properties={
        "entity": {
            "ctype": PropertySpec(one_of="ctype"),
            "carrier": PropertySpec(one_of="carrier"),
        },
    },
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}, default=0.0, unit="MW"),
        "p_max_pu": AttributeSpec(
            dtype="float", dims={"entity", "snapshot"}, default=1.0
        ),
    },
    partial={"entity", "bus", "connection"},
)


@dataclass(frozen=True)
class Empty:
    schema: Schema
    con: DuckDBPyConnection
    dims: dict = field(default_factory=dict)
    properties: dict = field(default_factory=dict)
    attributes: dict = field(default_factory=dict)

    def flags(self, **categories):
        return {}


ws = Workspace()
store = ws.store(tempfile.mkdtemp())
root = store.commit(Empty(schema, ws.con), parent=None)

with WorkingRecord(root.record) as w:
    w.add("carrier", ["wind", "gas"])
    w.add("bus", ["north", "south"])
    w.add("entity", ["wind1", "gas1"], ctype="Generator", carrier=["wind", "gas"])
    w.add(
        "connection",
        [{"entity": "wind1", "bus": "north"}, {"entity": "gas1", "bus": "south"}],
    )
    w.add("snapshot", [0, 1, 2])
    w.set("p_nom", [100.0, 50.0], entity=["wind1", "gas1"])
    w.set("p_max_pu", [0.3, 0.8, 0.5], entity="wind1", snapshot=[0, 1, 2])
    w.set("p_max_pu", 0.9, entity="gas1")
    base = store.commit(w)

print(base.record.attributes["p_nom"].df())
```

Check it:

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  100.0
1   gas1     p_nom         NaN   50.0
```

`base` has the values that the first tutorial set. The `with` block closed the [`WorkingRecord`](../reference/editing.md) once its edits were committed.

## One edit

Stage the scenario's one change, a larger wind generator, over `base`:

```python
w = WorkingRecord(base.record)
w.set("p_nom", 150.0, entity="wind1")
print(w.edits.attribute("p_nom").df())
```

Check it:

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  150.0
```

`w.edits` contains only what you staged: one row, and nothing of `gas1`.

## A child revision

Commit the edit with a `meta` entry for the scenario:

```python
high = store.commit(w, meta={"scenario": "high"})
w.close()
print(high.parent == base.id, high.meta)
```

```text
True {'scenario': 'high'}
```

`high` is a child of `base`. Committing a second `WorkingRecord` over `base` would give `high` a sibling on another branch.

## The fold

Read `p_nom` through `high`:

```python
print(high.record.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  150.0
1   gas1     p_nom         NaN   50.0
```

The step that combines a revision's layers (the [fold](../reference/resolution.md)) assigns each row to the deepest layer that wrote it. A read returns the child's `wind1` row, and `gas1` from `base`. Read `p_nom` through `base`:

```python
print(base.record.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  100.0
1   gas1     p_nom         NaN   50.0
```

`base` still has `wind1` at 100. Read the child's layer alone:

```python
print(store.layer(high.id).attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  150.0
```

The layer contains only the one row that the commit wrote.

## A materialised revision

A read of `high` folds every layer from the root down. Writing the fold's result beside the revision's layer ([materialising](../reference/resolution.md#materialised-revisions) it) lets a later read stop there:

```python
import os

high.materialise()
print(sorted(os.listdir(store.resolved_dir(high.id))))
```

```text
['dim_owners.parquet', 'owner_map.parquet']
```

The two files are the fold's result for `high`. Read the revision again:

```python
record = store.revision(high.id).record
print(record.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  150.0
1   gas1     p_nom         NaN   50.0
```

The values are the same. Materialising changes how many layers a read opens, and never what it returns.

## What you have built

You have built a scenario as a child revision that contains one row, read it through the fold beside its unchanged parent, and materialised it.

## Where to next

- [Solve a record with PyPSA](pypsa.md) writes a solve's results as a child revision.
- [Branch and materialise](../howto/branch-and-materialise.md) covers siblings, navigating the tree and claims.
- [Layers](../reference/layers.md) and [the fold](../reference/resolution.md) describe what a layer may contain and how rows of two layers combine.
