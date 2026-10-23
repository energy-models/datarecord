<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Layers

A `Revision` is a node in a tree of layers.
Each revision adds one layer, and it reads as that layer over its ancestors' layers, the deepest layer winning ([design](../design/layers/tree.md)).

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
Each row must name labels and tuples that the layer's final axes and groups hold.
