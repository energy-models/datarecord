<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Editing

A `WorkingRecord` is a record plus pending edits, held in DuckDB tables and not yet written anywhere.
It is a `Record`, so it reads as the data with its pending edits applied.
You can read an edit back, or hand the record to something that only reads, before writing it ([design](../design/use/editing.md)).

```python
from datarecord import WorkingRecord

w = WorkingRecord(root.record)
```

## `set`

```python
w.set("p_nom", 150.0, entity=["wind1", "wind2"])  # broadcast
w.set("p_nom", [150.0, 80.0], entity=["wind1", "wind2"])  # per label, positional
w.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per label, keyed
w.set(
    "p_max_pu", frame, entity=["wind1"]
)  # a long frame: pandas, polars, arrow, a relation
w.set("p_max_pu", series.rename_axis("snapshot"), entity="wind1")  # a series
w.set("p_max_pu", 0.9)  # every entity, loads included, now and later
w.set("p_max_pu", 0.9, ctype="Generator")  # every generator there is now
w.set("p_max_pu", 0.8, ctype="Generator", carrier="wind")  # the ones there are now
w.set("efficiency", 0.9, entity="dc", bus="north")  # a connection
w.set("p_nom", 200.0, entity="wind1", scenario="high")  # scoped to one scenario
w.set("entity.carrier", "solar", entity="g2")  # a property, by its subject
w.set("bus.x", 10.2, bus="north")  # a property; `x` alone names an attribute
```

Every coordinate is a keyword, a multi-dim's coordinates included.
`bus="north"` with `entity="dc"` addresses one connection, and `from=` with `to=` one corridor.
A coordinate that no keyword names is NULL, which means every label of it: every entity, loads included, and entities added later.
A multi-dim value is a whole tuple, so you give all of its coordinates or none ([design](../design/use/editing.md#set)).

A property as a keyword selects labels at call time.
A keyword is bare: it names a coordinate of the attribute first, else any property of a dim the attribute reaches.
`ctype="Generator", carrier="wind"` stages one row per label that matches both, on the record as staged.
A label added later is not reached, and a plain NULL is how a value reaches such labels.
A name that several reached dims declare a property of is refused, so set it per dim.

A multi-period snapshot carries its period as `snapshot.period`, which names a label of `period`.
It carries its timestep as `snapshot.timestep`, which names a label of the `datetime` dim `timestep` ([design](../design/model/dims.md#mapping-labels-to-another-dim)).

```python
w.add("period", [2030, 2040])
w.add("timestep", [t0, t1])
w.add(
    "snapshot", [0, 1, 2, 3], period=[2030, 2030, 2040, 2040], timestep=[t0, t1, t0, t1]
)
w.set("p_max_pu", 0.8, entity="g1", period=2030)  # snapshots 0 and 1
w.set("p_max_pu", 0.5, entity="g1", timestep=t0)  # snapshots 0 and 2
w.set("snapshot.period", {3: 2030})
w.remove("period", [2040])  # refused while a snapshot.period names it
```

A mapping's keys or a series' index are labels of the one coordinate that no keyword names, unless the series' index is named after a coordinate.
Where neither says which coordinate, the call is refused.

`set` writes properties too, a category included, each named by its subject.
`set("entity.ctype", "Load", entity="g1")` patches one column of `g1`'s axis row.
A plain name is an attribute.
`set("carrier", ...)` in a schema where only properties are called `carrier` is refused, and the error suggests `bus.carrier` and `entity.carrier`.

A multi-dim's property takes a scalar, and its coordinates as keywords select the tuples it patches, none selecting every tuple.
A category keyword selects for a property as for an attribute: `role="output"` picks the output connections, as `ctype="Generator"` picks the generators for `entity.carrier`.
A category of a coordinate selects too: `ctype="Link"` picks the connections of links.
A coordinate is part of the key, so `set("connection.bus", ...)` is refused: `remove` the tuple and `add` it anew.

```python
w.set("connection.role", "input", entity="dc", bus="north")
w.set("entity.carrier", "wind", ctype="Generator")
w.set("connection.note", "export", role="output")  # every output connection
w.set("connection.note", "link", ctype="Link", bus="north")  # links at north
w.set("connection.bus", "south", entity="dc")  # refused: a coordinate is the key
```

## `add` / `remove`

```python
import pandas as pd

w.add("carrier", ["wind"])
w.add("bus", ["north", "south"])
w.add("entity", ["wind1", "wind2"], ctype="Generator", carrier="wind")
w.add("entity", ["dc"], ctype="Link")
w.set("p_nom", [100.0, 80.0], entity=["wind1", "wind2"])

w.remove("entity", ["old_coal"])

w.add(
    "connection",
    [{"entity": "dc", "bus": "north"}, {"entity": "dc", "bus": "south"}],
    role=["input", "output"],
)
w.add("connection", pd.DataFrame({"entity": ["wind1"], "bus": ["north"]}))
w.add("connection", other.dims["connection"])  # another record's tuples, as read
w.remove("connection", [{"entity": "dc", "bus": "south"}])
```

`add(dim, labels, **columns)` works on any dim but one with `one_of`, such as `ctype`, whose labels are the schema's; `add` and `remove` refuse it ([design](../design/model/dims.md#a-dim-with-one_of)).
The keywords are the axis file's other columns, the dim's properties, bare.
Each is a scalar or a sequence aligned to `labels`.
An attribute keyword is refused, since attributes go through `set` whatever their `dims` ([design](../design/use/editing.md#add-remove)).
A label exists by virtue of its axis row, so `add` is an edit of its own: adding a bus with no properties sets nothing.

`remove(dim, labels)` takes the labels out of the staged axis.
It need not list what it deletes.
The fold applies it to every value keyed by the label, and `remove` also takes every tuple that names one of the labels out of its multi-dim, with the tuple's values ([design](../design/layers/removal.md)).
`remove("bus", ["north"])` removes every connection at `north`.

A layer restates each axis it changes whole, a multi-dim's included: every label the base has, with the edits applied, in the base's order, then the labels it adds.
An `add` of a label that `remove` took out puts it back where the base has it.
`remove` is refused while a property that refers to the dim still stores one of the labels, such as `entity.carrier` naming `gas`.
That row belongs to its subject and outlives the label, so `set` it to another label or None first ([design](../design/use/editing.md#add-remove)).

On a multi-dim, `add` and `remove` take tuples.
A frame DuckDB scans gives them in the struct column named for the multi-dim, as `dims["connection"]` reads it, or with one column per coordinate instead, but not both; it may carry a column per property, and beside it a property keyword must be a scalar.
A tuple is also a mapping from coordinate to label, and a list of them takes each property keyword as a scalar or a sequence aligned to the list.
A tuple with a coordinate missing or NULL is refused, as is one naming a label its coordinate's dim lacks, so `add` the label first ([design](../design/model/multi-dims.md)).

## Inspecting and rolling back

```python
w.attributes["p_nom"]  # the edit applied, over the base's rows
w.properties["entity"]  # additions in, removals out
w.rollback()  # discard everything staged
```

You read what you staged from the record itself, which answers with the edits applied ([design](../design/use/editing.md#reading-with-pending-edits)).
Staged rows live in DuckDB tables on the record's connection, so they vanish with it and never touch disk ([design](../design/use/editing.md#staging)).

## Writing the edits

A `WorkingRecord` does not write.
The record stays unchanged until you hand its edits to a writer ([design](../design/use/editing.md#edits)):

```python
new = root.child(w.edits)  # a patch layer under a new child of the base
ws.write(w, "out/")  # a standalone record, flattened
w.rollback()  # clear the staging area, if you are done
```

`w.edits` holds only the edits: the attribute rows set, and each axis changed, restated whole.
The fold resolves the rest from the parent.
`w` itself is the resolved result, what is staged plus what the record already reads.
A standalone directory needs that, since it has no parent to resolve against.

The edits are a delta against the revision `w` was built over, so `child` raises if you write them under another one.
Layers are write-once, so the layer lands in the new child ([design](../design/layers/tree.md#a-layers-data-is-write-once)).
The returned revision reads the edits back:

```python
new.record.attributes["p_nom"].df()
```

Writing clears nothing: a `WorkingRecord` that still holds its edits reads exactly what it did before, until `rollback()`.

An edit replaces the rows it names, so restating a coordinate overwrites it and the last write stands.
A `remove` after a `set` wins regardless of order, since a removed label has no values.
An `add` after a `remove` brings the label back with its parent's values and without the multi-dim tuples that named it.

A base's results are attributes and carry like any other.
A child written from `edits` inherits them, though an edited input may no longer support them.
The tool writes a solve's results as a layer of its own ([tools](tools.md#solving-and-writing-results-back)).

Edit-level mistakes are caught when the edit is staged, so a typo is reported at the line that typed it ([design](../design/use/editing.md#validation)).
