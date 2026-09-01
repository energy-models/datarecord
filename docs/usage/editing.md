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
w.set("efficiency", 0.9, entity=["dc"], bus="north")  # a connection
w.set("p_nom", 200.0, entity=["wind1"], scenario="high")  # scoped to one scenario
w.set("p_nom", nw.col("value") * 1.1, entity=["wind1"])  # derived
w.set("p", solved_frame, kind="outputs")  # a result
```

**There is no `entity_type` keyword.** A name identifies one component across every type, so the record looks the type up and checks that each name's type carries the attribute — one call may legitimately span types ([design](../design/working-record.md#set)).

`entity=None` means every component whose type carries this attribute. Every other coordinate goes through `**dims`, a group's included — `bus="north"` addresses one connection, `from=`/`to=` one corridor. A plain dim's absence means "every value of that dim" by the NULL broadcast rule; a group coordinate's means "every row of the group for this entity" ([design](../design/record.md#the-broadcast-rule)).

An `nw.Expr` value is a **function of the current value**: it reads the resolved value including earlier pending edits, so two such calls compose, and what gets staged is the result rather than the expression ([design](../design/working-record.md#an-nwexpr-value-derived-from-the-current-one)). A named target that resolves to no row raises — the caller asked for those rows to take a new value and there is nothing to compute one from.

## `add` / `remove` / `connect` / `disconnect`

```python
import pandas as pd

w.add("Bus", pd.DataFrame({"entity": ["north", "south"]}))
w.add(
    "Generator",
    pd.DataFrame(
        {
            "entity": ["wind1", "wind2"],
            "carrier": ["wind", "wind"],
            "p_nom": [100.0, 80.0],
        }
    ),
)

w.remove("Generator", ["old_coal"])

w.add_group(
    "connection",
    pd.DataFrame(
        {
            "entity": ["dc", "dc"],
            "bus": ["north", "south"],
            "role": ["bus0", "bus1"],
        }
    ),
)
w.remove_group("connection", [("dc", "south")])
```

`add` takes a wide frame keyed by `entity` and splits it by the schema: columns addressed by `entity` alone stay in `dims/entity_type/`, ones varying beyond it become `inputs/` rows ([design](../design/format.md#where-a-value-lives)). It keeps its `ctype` argument where `set` loses it — this is the call that _establishes_ what a name's type is, and where record-wide name uniqueness is enforced ([design](../design/working-record.md#add-remove)). A component exists by virtue of its member row, so `add` is not a sequence of `set` calls: adding a bus with no attributes makes the point.

`remove` stages a tombstone on the entity axis, with no dim scope — a component [exists or it does not](../design/schema.md#existence-does-not-vary-along-a-dim). It need not enumerate what it deletes: the fold applies it to every attribute, and to every connection of the component ([design](../design/layers.md#deletion)).

`add_group`/`remove_group` take no type at all, unlike `add`: a group's rows are keyed by its coordinates and the type is not one of them, so there is nothing for a type argument to scope ([design](../design/format.md#where-a-value-lives)). Every group is reached the same way — `connection` has no call of its own, being one group among however many the schema declares.

## Inspecting and rolling back

```python
w.attributes["p_nom"]  # the edit applied, over the base's rows
w.entity_types["Generator"]  # additions in, removals out
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
new.record.attributes["p_nom"].collect()
```

`NewChild()` branches from whichever node the `WorkingRecord` was built over, which is what a caller means every time. Pass one explicitly — `NewChild(other_revision)` — only to re-parent the edits elsewhere; a `WorkingRecord` over a base that is not a node in a layer tree (a `DirectoryRecord`, a framework object) has nothing to default to and must supply one.

Staged rows are appended, never updated, so the same coordinate may be staged repeatedly; commit collapses to last-write-wins per coordinate. A `remove` after a `set` wins regardless of order — a deleted component has no attributes — and an `add` after a `remove` brings the component back.

Neither target carries a **base's** results across: an edit changes the inputs a result was computed from. What a commit does carry is results staged into this record through `set(..., kind="outputs")`, which were computed against these pending inputs ([design](../design/working-record.md#results-through-kindoutputs)).

Edit-level mistakes are caught when the edit is **staged**, not at commit, so a typo is reported at the line that typed it ([design](../design/working-record.md#validation)).
