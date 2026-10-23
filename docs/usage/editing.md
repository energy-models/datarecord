<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Editing

`WorkingRecord` is a record plus pending edits, held in memory and not yet written anywhere. It **satisfies `Record`**, so what it reads is the data with its pending edits applied — an edit can be read back, or the record handed to something that only knows how to read, without writing it ([design](../design/working-record.md)).

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
w.set("bus.x", 10.2, bus="north")  # not `x`, an attribute of its own
```

**Every coordinate is a keyword**, a group's fields included — `bus="north"` with `entity="dc"` addresses one connection, `from=`/`to=` one corridor. A coordinate no keyword names is NULL, which means "every label of it": every entity, loads included, and ones added later. A group's fields must all be given or none, since a group value is a whole tuple ([design](../design/working-record.md#set)).

**A property as a keyword selects labels at call time**: a keyword is bare, a coordinate of the attribute first, else any property of a dim it reaches, so `ctype="Generator", carrier="wind"` stages one row per label matching both on the record as staged. A label added later is not reached; a plain NULL is how a value reaches those. A name several reached dims declare a property of is refused: set it per dim.

A parent is such a property: a dim is flat, so a multi-period snapshot carries its period as `snapshot.period`, naming a label of `period`, and its timestep as `snapshot.timestep`, naming one of the `datetime` dim `timestep` ([design](../design/schema.md#a-dim-is-flat)).

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

A mapping's keys or a series' index are labels of the one coordinate no keyword names, unless the series' index is named after a coordinate; where neither says, the call is refused rather than guessed.

`set` writes **properties** too, a category included, each named by its subject: `set("entity.ctype", "Load", entity="g1")` patches one column of `g1`'s axis row. A plain name is an attribute or result; `set("carrier", ...)` with only properties so called is refused, suggesting `bus.carrier` and `entity.carrier`.

## `add` / `remove` / `add_group` / `remove_group`

```python
import pandas as pd

w.add("carrier", ["wind"])
w.add("bus", ["north", "south"])
w.add("entity", ["wind1", "wind2"], ctype="Generator", carrier="wind")
w.add("entity", ["dc"], ctype="Link")
w.set("p_nom", [100.0, 80.0], entity=["wind1", "wind2"])

w.remove("entity", ["old_coal"])

w.add_group(
    "connection",
    pd.DataFrame(
        {
            "connection": [
                {"entity": "dc", "bus": "north"},
                {"entity": "dc", "bus": "south"},
            ],
            "role": ["bus0", "bus1"],
        }
    ),
)
w.remove_group("connection", [("dc", "south")])
```

`add(dim, labels, **columns)` works on any dim. The keywords are the axis file's other columns — the dim's properties, bare — each a scalar or a sequence aligned to `labels`. An attribute keyword is refused: attributes go through `set`, whatever their `dims` ([design](../design/working-record.md#add-remove)). A label exists by virtue of its axis row, so `add` is not a sequence of `set` calls: adding a bus with no properties makes the point.

`remove(dim, labels)` stages a tombstone per label, with no dim scope — a label [exists or it does not](../design/schema.md#existence-does-not-vary-along-a-dim). It need not enumerate what it deletes: the fold applies it to every value keyed by the label, and `remove` also tombstones every group tuple naming one ([design](../design/layers.md#deletion)). It is refused while a property naming the dim still stores one of the labels — `entity.carrier` naming `gas` — since that row is its subject's and outlives the label: `set` it to another label or None first ([design](../design/working-record.md#add-remove)).

`add_group`/`remove_group` stage a group's tuples, as one struct column named for the group with the group's properties beside it. Every group is reached the same way — `connection` has no call of its own, being one group among however many the schema declares.

## Inspecting and rolling back

```python
w.attributes["p_nom"]  # the edit applied, over the base's rows
w.properties["entity"]  # additions in, removals out
w.rollback()  # discard everything staged
```

What you staged is read back from the record itself, which satisfies `Record` and answers with the edits applied ([design](../design/working-record.md#reading-with-pending-edits)). Staged rows live in DuckDB tables on the record's connection, so they vanish with it and never touch disk ([design](../design/working-record.md#staging)).

## Writing the edits

A `WorkingRecord` does not write. Nothing touches the record until you hand its edits to a writer ([design](../design/working-record.md#edits)):

```python
new = root.child(w.edits)  # a patch layer under a new child of the base
ws.write(w, "out/")  # a standalone record, flattened
w.rollback()  # clear the staging area, if you are done
```

`w.edits` is **only the edits**, and the fold resolves the rest from the parent. `w` itself is **the resolved result** — what is staged plus what the record already reads — which is what a standalone directory needs, there being no parent to resolve against.

The edits are a delta against the revision `w` was built over, so `child` raises if you write them under another one. The layer lands in the **child**, never in the node you branched from — layers are write-once ([design](../design/layers.md#a-layers-data-is-write-once)) — so it is the returned node that reads the edits back:

```python
new.record.attributes["p_nom"].df()
```

Writing clears nothing: a `WorkingRecord` still holding its edits reads exactly what it did before, until `rollback()`.

An edit replaces the rows it names, so restating a coordinate overwrites it and the last write is what stands. A `remove` after a `set` wins regardless of order — a deleted label has no values — and an `add` after a `remove` brings the label back, without its group tuples.

A base's results are attributes and carry like any other: a child written from `edits` inherits them, though an edited input may no longer support them. A solve's results are written by the tool, not staged ([tools](tools.md#solving-and-writing-results-back)).

Edit-level mistakes are caught when the edit is **staged**, not at write, so a typo is reported at the line that typed it ([design](../design/working-record.md#validation)).
