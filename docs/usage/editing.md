<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Editing

`WorkingRecord` is a record plus pending edits, held in memory and not yet written anywhere. It **satisfies `Record`**, so what it reads is the data with its pending edits applied — an edit can be read back, or the record handed to something that only knows how to read, without committing ([design](../design/working-record.md)).

```python
from datarecord import WorkingRecord, NewChild, Directory

w = WorkingRecord(root.record, con)
```

## `set`

```python
w.set("p_nom", 150.0, entity=["wind1", "wind2"])  # broadcast
w.set("p_nom", [150.0, 80.0], entity=["wind1", "wind2"])  # per name, positional
w.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per name, keyed
w.set("p_max_pu", frame, entity=["wind1"])  # a long frame
w.set("efficiency", 0.9, port=["dc_out"])  # one port
w.set("p_max_pu", 0.5, entity=["wind1"], scenario="high")  # scoped to one scenario
w.set("p_max_pu", nw.col("value") * 1.1, entity=["wind1"])  # derived
```

**There is no `entity_type` keyword.** No attribute is narrowed to a type, so one call may span types. `set` refuses a name that is not on the entity axis — `add` it first — and an attribute that is not declared over the dims the call names ([design](../design/working-record.md#set)).

Every dim goes through `**dims` — `entity=["wind1"]`, `port=["dc_out"]`, `scenario="high"`. A dim the call does not name means every value of that dim, so `entity=None` means every entity on the axis ([design](../design/working-record.md#set)).

An `nw.Expr` value is a **function of the current value**: it reads the resolved value including earlier pending edits, so two such calls compose, and what gets staged is the result rather than the expression ([design](../design/working-record.md#an-nwexpr-value-derived-from-the-current-one)). A named target that resolves to no row raises — the caller asked for those rows to take a new value and there is nothing to compute one from.

## `add` / `remove` / `add_relation` / `remove_relation`

```python
import pandas as pd

w.add(
    "entity",
    pd.DataFrame({"entity": ["north", "south"], "entity_type": ["Bus", "Bus"]}),
)
w.add(
    "entity",
    pd.DataFrame(
        {
            "entity": ["wind1", "wind2"],
            "entity_type": ["Generator", "Generator"],
            "carrier": ["wind", "wind"],
            "p_nom": [100.0, 80.0],
        }
    ),
)

w.add(
    "port",
    pd.DataFrame(
        {
            "port": ["dc_in", "dc_out"],
            "entity": ["dc", "dc"],
            "bus": ["north", "south"],
            "role": ["input", "output"],
        }
    ),
)

w.remove("entity", ["old_coal"])

w.add_relation("port_bus", pd.DataFrame({"port": ["dc_out"], "bus": ["east"]}))
w.remove_relation("port_bus", [("dc_out",)])
```

`add(dim, frame)` takes a wide frame keyed by `dim` and splits it by the schema. For `entity`, the entity axis gets one row per entity, each attribute column becomes `attributes/` rows, and the `entity_type` column becomes rows of the `entity_type` relation, which gives each entity its type ([design](../design/working-record.md#add-remove)). A frame that carries every other column of a relation keyed by its dim adds that relation's rows the same way: the `port` frame above stages each port's label, its `role` rows, and its rows of `port_entity` and `port_bus`. A component exists by virtue of its row on the entity axis, so `add` is not a sequence of `set` calls: adding a bus with no attributes makes the point.

`remove(dim, labels)` stages a tombstone per label on that dim's axis. `dim` may be any dim declared `partial`, which includes every dim a relation is keyed by; any other dim is refused. It need not enumerate what it deletes: the fold applies it to every attribute row and every relation row keyed on the label, so a removed component takes its `entity_type` row with it ([design](../design/layers.md#deletion)).

`add_relation`/`remove_relation` take no type: a relation's rows are keyed by its columns ([design](../design/format.md#where-a-value-lives)). Every relation is reached the same way — `port_bus` has no call of its own, being one relation among however many the schema declares.

## Inspecting and rolling back

```python
w.attributes["p_max_pu"]  # the edit applied, over the base's rows
w.dims["entity"]  # additions in, removals out
w.rollback()  # discard everything staged
```

What you staged is read back from the record itself, which satisfies `Record` and answers with the edits applied ([design](../design/working-record.md#reading-with-pending-edits)). Staged rows live in DuckDB tables on the record's connection, so they vanish with it and never touch disk ([design](../design/working-record.md#staging)).

## Committing

Nothing touches the record until `commit`, which takes one of two targets ([design](../design/working-record.md#committing)):

```python
new = w.commit(NewChild())  # a patch layer under a new child; returns it
w.commit(Directory("out/"))  # a standalone record, flattened; returns None
```

`NewChild()` writes **only the edits** and the fold resolves the rest from the parent. `Directory(uri)` writes **the resolved result** — what is staged plus what the record already reads — since there is no parent to resolve against.

The layer lands in the **child**, never in the node you branched from — layers are write-once ([design](../design/layers.md#a-layers-data-is-write-once)) — so it is the returned node that reads the edits back:

```python
new.record.attributes["p_max_pu"].collect()
```

`NewChild()` branches from whichever node the `WorkingRecord` was built over, which is what a caller means every time. Pass one explicitly — `NewChild(other_revision)` — only to re-parent the edits elsewhere; a `WorkingRecord` over a base that is not a node in a layer tree — a `Record.at(uri)` over a plain directory — has nothing to default to and must supply one.

An edit replaces the rows it names, so restating a coordinate overwrites it and the last write is what stands. A `remove` after a `set` wins regardless of order — a deleted component has no attributes — and an `add` after a `remove` brings the component back.

Edit-level mistakes are caught when the edit is **staged**, not at commit, so a typo is reported at the line that typed it ([design](../design/working-record.md#validation)).
