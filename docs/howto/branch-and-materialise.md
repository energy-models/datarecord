<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Branch and materialise

This guide shows how to branch scenarios as sibling revisions, find and materialise a revision, and write a whole record as a layer. The examples use `root`, `rev` and `store` from [Your first record](../tutorials/first-record.md). [Layers](../reference/layers.md) defines a revision and a layer, and [the fold](../reference/resolution.md) defines how a read combines them.

## Branch a scenario

Commit two `WorkingRecord`s over the same revision, and give each revision a `meta` entry to find it by:

```python
def scenario(parent, name, p_nom):
    with WorkingRecord(parent.record) as w:
        w.set("p_nom", p_nom, entity="wind1")
        return store.commit(w, meta={"scenario": name})


high = scenario(rev, "high", 150.0)
low = scenario(rev, "low", 60.0)
print(high.parent == low.parent == rev.id)
```

```text
True
```

`high` and `low` are siblings, each a layer with one row over `rev`. A `meta` value is a string, a finite number or a boolean.

## Find a revision

`store.revisions(meta=...)` returns the revisions whose `meta` contains every pair you pass. `ancestry()` returns the ids from the root down:

```python
for revision in store.revisions(meta={"scenario": "high"}):
    print(revision.meta, revision.ancestry() == [root.id, rev.id, high.id])
print(len(store.revisions()))
print(store.revision(low.id).record.attributes["p_nom"].df())
```

```text
{'scenario': 'high'} True
4
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN   60.0
1   gas1     p_nom         NaN   50.0
```

One revision matches, the store contains four, and `low` has its own `p_nom` for `wind1`. `store.revision(id)` loads one revision by its id.

## Read one layer alone

`store.layer(id)` reads a revision's layer without its ancestors:

```python
print(store.layer(low.id).attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN   60.0
```

The layer contains only the row that the commit of `low` wrote.

## Materialise a revision

`materialise()` writes a revision's fold under its layer's `resolved/` directory. A read of it or of a descendant then stops there and opens no layer above it:

```python
high.materialise()
higher = scenario(high, "higher", 200.0)
print(higher.record.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0   gas1     p_nom         NaN   50.0
1  wind1     p_nom         NaN  200.0
```

The values are the same with or without `materialise`. Materialise a revision that many reads or children start from, such as the base of a set of scenarios.

## Write a whole record as a layer

`store.commit` writes any record whole as a child of `parent`, and with `parent=None` as a new root:

```python
copy = store.commit(higher.record, parent=None, meta={"from": "higher"})
print(copy.parent, copy.ancestry() == [copy.id])
```

```text
None True
```

The new root contains the folded values of `higher`, with no history.

## Claim a revision before its data exists

`store.claim` reserves a revision id under a parent, and `claim.commit` writes the layer later, from the same or another process:

```python
claim = store.claim(parent=rev, meta={"run": "b"})
print([c.id for c in store.claims()] == [claim.id])
done = claim.commit(higher.record, meta={"status": "done"})
print(done.meta)
```

```text
True
{'run': 'b', 'status': 'done'}
```

`done` has the claim's `meta` and the commit's. The commit may add `meta` keys but not change those of the claim. A claim that is never committed stays pending, and no read opens it, until you clean it up:

```python
from datetime import timedelta

stale = store.claim(parent=rev)
print(store.cleanup(ids=[stale.id]) == [stale.id])
print(store.cleanup(older_than=timedelta(days=14)))
```

```text
True
[]
```

`cleanup` returns the ids it deleted: the stale claim, then none older than 14 days. [Store a record](store-a-record.md) shows how to hand a claim to another process.
