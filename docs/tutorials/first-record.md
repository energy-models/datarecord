<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Your first record

In this tutorial you build a record of two generators, commit it to a store and read it back. It takes about 25 minutes.

You need datarecord installed, as [Installation](../installation.md) shows.

## The schema

A record's [schema](../reference/schema.md) declares what the record may contain. It lists the sets of labels that values vary along (dims), the values that describe one label (properties) and the values over dims (attributes). The [data model](../reference/data-model.md) defines each. Run this:

```python
from datarecord import AttributeSpec, Dimension, PropertySpec, Schema

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
            "ctype": PropertySpec(refers="ctype"),
            "carrier": PropertySpec(refers="carrier"),
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
print(schema.flat_dims)
print(schema.multi_dims)
```

Check it:

```text
('entity', 'ctype', 'bus', 'carrier', 'snapshot')
('connection',)
```

`connection` is a dim whose labels are `(entity, bus)` tuples (a multi-dim), so the schema lists it apart from the five flat dims. `ctype` lists its labels in the schema with `one_of`, and `partial` lists the dims along which a later layer overrides one label's value.

## An empty root

A record is kept in one location (a [store](../reference/store.md)) as a tree of the data each commit adds ([layers](../reference/layers.md)). You open each layer by its id (a revision). You stage edits over a revision, so the first revision has the schema and no rows. Any object with the members of [`RecordLike`](../reference/record.md) can be committed, and this dataclass has the fewest:

```python
import tempfile
from dataclasses import dataclass, field

from duckdb import DuckDBPyConnection

from datarecord import Workspace


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
print(root.record.dims["entity"].df())
```

Check it:

```text
Empty DataFrame
Columns: [entity]
Index: []
```

The root revision's `entity` dim has no labels. The [`Workspace`](../reference/store.md) is the DuckDB database that its stores share.

## Labels

A [`WorkingRecord`](../reference/editing.md) stages edits over a revision. Add the labels:

```python
from datarecord import WorkingRecord

w = WorkingRecord(root.record)
w.add("carrier", ["wind", "gas"])
w.add("bus", ["north", "south"])
w.add("entity", ["wind1", "gas1"], ctype="Generator", carrier=["wind", "gas"])
w.add(
    "connection",
    [{"entity": "wind1", "bus": "north"}, {"entity": "gas1", "bus": "south"}],
)
w.add("snapshot", [0, 1, 2])
print(w.properties["entity"].df())
print(w.dims["connection"].df())
```

Check it:

```text
  entity      ctype carrier
0  wind1  Generator    wind
1   gas1  Generator     gas
                            connection
0  {'entity': 'wind1', 'bus': 'north'}
1   {'entity': 'gas1', 'bus': 'south'}
```

Each generator has its `ctype` and `carrier` properties, and each connection is one struct with an `entity` and a `bus` field. The carriers and buses come first, because `add` refuses a property value or a tuple field that is not yet a label of its dim.

## Values

`set` stages attribute values at the labels you name:

```python
w.set("p_nom", [100.0, 50.0], entity=["wind1", "gas1"])
w.set("p_max_pu", [0.3, 0.8, 0.5], entity="wind1", snapshot=[0, 1, 2])
w.set("p_max_pu", 0.9, entity="gas1")
print(w.attributes["p_max_pu"].df())
```

Check it:

```text
  entity  snapshot attribute  breakpoint  value
0  wind1         0  p_max_pu         NaN    0.3
1   gas1      <NA>  p_max_pu         NaN    0.9
2  wind1         1  p_max_pu         NaN    0.8
3  wind1         2  p_max_pu         NaN    0.5
```

`attributes` returns long rows, one per value, in no promised order. The `gas1` row leaves `snapshot` NULL, and a NULL means every label of its dim ([the broadcast rule](../reference/data-model.md)).

## The values, wide

[`settle`](../reference/record.md) applies that rule and gives one row per label at each coordinate, which pandas pivots wide:

```python
from datarecord import settle

labels = {"entity": w.dims["entity"], "snapshot": w.dims["snapshot"]}
settled = settle(w.attributes["p_max_pu"], labels, w.flags()["p_max_pu"])
print(settled.df().pivot(index="snapshot", columns="entity", values="value"))
```

Check it:

```text
entity    gas1  wind1
snapshot
0          0.9    0.3
1          0.9    0.8
2          0.9    0.5
```

`settle` turned the one `gas1` row into a value of 0.9 at each of the three snapshots. `properties` is already wide, with one row per label.

## A committed revision

`store.commit` writes the staged edits as a layer, a child of the revision `w` was made over:

```python
rev = store.commit(w)
print(rev.parent == root.id)
```

```text
True
```

The new revision `rev` is a child of the root. Open it by its id and read it:

```python
record = store.revision(rev.id).record
print(record.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  100.0
1   gas1     p_nom         NaN   50.0
```

The committed revision contains the values you set. The root still has no rows, because a layer is written once and a commit never changes the parent:

```python
w.close()
print(root.record.attributes["p_nom"].df())
```

```text
Empty DataFrame
Columns: [entity, attribute, breakpoint, value]
Index: []
```

## What you have built

You have built a store with two revisions. The root is empty, and its child contains two generators, their buses and connections, and values for `p_nom` and `p_max_pu`. You read the values long, as they are stored, and wide, with the broadcast rule applied.

## Where to next

- [A scenario on a branch](scenario.md) branches this record and overrides one value.
- [Solve a record with PyPSA](pypsa.md) hands a record to PyPSA.
- [Read a record](../howto/read-a-record.md) lists the other ways to read one.
