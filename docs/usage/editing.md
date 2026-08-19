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
w.set("p_nom", 150.0, names=["wind1", "wind2"])  # broadcast
w.set("p_nom", [150.0, 80.0], names=["wind1", "wind2"])  # per name, positional
w.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per name, keyed
w.set("p_max_pu", frame, names=["wind1"])  # a long frame
w.set("efficiency", 0.9, names=["dc"], bus="north")  # a connection
w.set("p_nom", 200.0, names=["wind1"], scenario="high")  # scoped to one scenario
w.set("p_nom", nw.col("value") * 1.1, names=["wind1"])  # derived
w.set("p", solved_frame, kind="outputs")  # a result
```

**There is no `component_type` keyword.** A name identifies one component across every type, so the record looks the type up and validates each name against _its own_ type's `AttributeSpec` — one call may legitimately span types ([design](../design/working-record.md#set)).

`names=None` means every component whose type declares this attribute. `bus` names a connection; every other keyword is a dim, and its absence means "every value of that dim" by the NULL broadcast rule.

An `nw.Expr` value is a **function of the current value**: it reads the resolved value including earlier pending edits, so two such calls compose, and what gets staged is the result rather than the expression ([design](../design/working-record.md#an-nwexpr-value-derived-from-the-current-one)). A named target that resolves to no row raises — the caller asked for those rows to take a new value and there is nothing to compute one from.

## `add` / `remove` / `connect` / `disconnect`

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

`add` takes a wide frame and splits it by the schema: non-varying columns stay in `dims/components/`, varying ones become `inputs/` rows ([design](../design/format.md#where-a-value-lives)). It keeps its `ctype` argument where `set` loses it — this is the call that _establishes_ what a name's type is, and where record-wide name uniqueness is enforced ([design](../design/working-record.md#add-remove)). A component exists by virtue of its member row, so `add` is not a sequence of `set` calls: adding a bus with no attributes makes the point.

`remove` stages a tombstone and need not enumerate what it deletes — one row per key, and the fold applies it to every attribute ([design](../design/layers.md#deletion)).

## Inspecting and rolling back

```python
w.pending  # Pending(attributes={...}, components={...},
#         connections={...}, tombstones={...})
bool(w.pending)  # whether anything is staged
w.rollback()  # discard everything staged
```

`pending` is a derived summary computed on access — a `GROUP BY` over the staging tables, not a second place rows live ([design](../design/working-record.md#pending)). Staged rows live in DuckDB tables on the record's connection, so they vanish with it and never touch disk ([design](../design/working-record.md#staging)).

## Committing

Nothing touches the record until `commit`, which takes one of two targets ([design](../design/working-record.md#committing)):

```python
new = w.commit(NewChild())  # a patch layer under a new child; returns it
w.commit(Directory("out/"))  # a standalone record, flattened; returns None
```

`NewChild()` writes **only the edits** and the fold resolves the rest from the parent. `Directory(uri)` writes **the resolved result** — what is staged plus what the record already reads — since there is no parent to resolve against.

The layer lands in the **child**, never in the node you branched from — layers are write-once ([design](../design/layers.md#a-layers-data-is-write-once)) — so it is the returned node that reads the edits back:

```python
new.record.attributes["p_nom"].collect()
```

`NewChild()` branches from whichever node the `WorkingRecord` was built over, which is what a caller means every time. Pass one explicitly — `NewChild(other_revision)` — only to re-parent the edits elsewhere; a `WorkingRecord` over a base that is not a node in a layer tree (a `DirectoryRecord`, a framework object) has nothing to default to and must supply one.

Staged rows are appended, never updated, so the same coordinate may be staged repeatedly; commit collapses to last-write-wins per coordinate. A `remove` after a `set` wins regardless of order — a deleted component has no attributes — and an `add` after a `remove` brings the component back.

Neither target carries a **base's** results across: an edit changes the inputs a result was computed from. What a commit does carry is results staged into this record through `set(..., kind="outputs")`, which were computed against these pending inputs ([design](../design/working-record.md#results-through-kindoutputs)).

Edit-level mistakes are caught when the edit is **staged**, not at commit, so a typo is reported at the line that typed it ([design](../design/working-record.md#validation)).
