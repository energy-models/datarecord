<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Layers

A `Revision` is a node in a tree of layers.
Each revision adds one layer, and it reads as that layer over its ancestors' layers, a deeper layer's row winning where it crosses a shallower one ([design](../design/layers/tree.md)).

```python
from datarecord import Workspace

store = Workspace().store("/data/my-record")

root = store.root(source)  # a new revision, with `source` as its layer
child = root.child(patch)  # branch off it, with `patch` as its layer
record = child.record  # the resolved tree, as a `Record`
```

A revision exists only by writing its layer, so there is no empty revision.

Each revision is a file under `revisions/` beside `layers/`, so a location alone is enough to find the tree.
A `Revision` serialises as its id, its parent and its store's location ([design](../design/storage/store.md#the-tree-beside-the-layers)).

A layer's data is write-once, so any revision may be a parent and no cache needs invalidating ([design](../design/layers/tree.md#a-layers-data-is-write-once)).
Branching is several children that share a parent by pointing at it.

## Reading across layers

An attribute reads as its rows as written, and the more specific row wins at each coordinate.
Where a child's row crosses a parent's, the child also holds a row naming both, so that row wins there ([design](../design/layers/resolution.md#why-the-map-holds-keys-as-written)):

```python
w.set("x", 50.0, entity="gas")  # the root
w.set("x", 100.0, entity="wind")
v.set("x", 999.0, scenario="high")  # its child
child.record.attributes["x"]
# (gas, NULL) 50, (wind, NULL) 100, (NULL, high) 999, (gas, high) 999, (wind, high) 999
```

A broadcast reaches every label the record holds, including one added below a materialised revision or staged in a `WorkingRecord`:

```python
c = rev.child(w.edits)  # w.set("p_max_pu", 0.9)
c.materialise()
w = WorkingRecord(c.record)
w.add("entity", ["n"], ctype="Generator")
w.attributes["p_max_pu"]  # (NULL) 0.9, which n takes by the broadcast rule
```

A constant set over a parent's series is written out along the series' snapshots, so it wins at each of them ([design](../design/layers/resolution.md#a-dim-outside-partial)):

```python
w.set("p_max_pu", [0.2, 0.5], snapshot=[0, 1])  # the root, every entity
v.set("p_max_pu", 0.9, entity="wind")  # its child
# the child holds (wind, NULL), (wind, 0) and (wind, 1) at 0.9
```

## Materialising

`materialise()` writes a revision's owner map and resolved axes under `layers/<id>/resolved/`.
A descendant's read then stops there and walks no further towards the root.
Materialising is a policy that you apply when you choose.
It changes no answer, only how many layers a read touches ([design](../design/layers/resolution.md#materialised-node-caches)):

```python
child.materialise()
```

## Navigating

```python
revision = store.revision(uuid)  # load one by id
store.revisions()  # every revision, unordered
revision.ancestry()  # the ids from the root to this revision
store.layer(uuid)  # one layer read alone
```

## Writing a whole record

```python
root = store.root(source)  # a new root revision with this layer
child = root.child(source)  # a new child of root with this layer
record = ws.write(source, "out/")  # a standalone directory, read back
```

`source` is a `LayerData` or anything satisfying `RecordLike`, including a framework object that presents itself as a record.
Read and write therefore meet at one interface ([design](../design/use/writing.md#layerdata)).
An existing layer directory is an error.
Locally, the writer stages the layer into a sibling path and renames it on success, so a source that fails validation leaves no layer ([design](../design/use/writing.md)).

The writer casts every column the schema declares a type for, so a record's files carry the schema's types and a reader can trust them.
Each attribute file must carry exactly [its attribute's own coordinates](../design/storage/format.md#the-long-schema).
An attribute the schema does not declare is refused, and so is a column the attribute is not addressed by.
Each row must name labels and tuples that the layer's final axes hold, a multi-dim's tuples included.
