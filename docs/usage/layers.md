# Layers

A `Revision` is a node in a tree of layers. Each node adds one layer; what it resolves to is that layer over its ancestors', last-writer-wins ([design](../design/layers.md)).

```python
from datarecord import Revision, connect

con = connect(base_uri="s3://bucket/my-record")

root = Revision.create(con)  # a new node
child = root.child()  # branch off it
record = child.record  # the resolved overlay, as a `Record`
```

A layer's data is **write-once**, so any node may be a parent and no cache ever needs invalidating ([design](../design/layers.md#a-layers-data-is-write-once)). Branching is several children sharing a parent by pointing at it, not by duplication.

## Materialising

`materialise()` writes a node's owner map and resolved dims under `layers/<id>/resolved/`, so descendants' reads stop there instead of walking to the root. Purely additive — a policy, not a lifecycle step, changing no answer, only how many layers a read touches ([design](../design/layers.md#materialised-node-caches)):

```python
child.materialise()
```

## Navigating

```python
revision = Revision.get(uuid, con)  # load one by id
revision.ancestry()  # the root→node path
```
