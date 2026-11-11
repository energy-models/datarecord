<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Layers

This page describes revisions, the tree they form, `partial`, what a layer may contain and how a layer removes a label.
How datarecord reads the layers of a revision as one record is on [the resolution page](resolution.md).

## Revisions

The data one revision adds on top of its parent (a layer) is a directory under `layers/<id>/` of a [store](store.md), written once.
The id of one layer, whose manifest lists its parent, is a revision, which you hold as a `Revision`.
A revision with no parent is a root, and a revision exists from the moment the store writes its manifest.

```python
root = store.commit(source, parent=None)
w = WorkingRecord(root.record)
w.set("p_nom", 60.0, entity="gas")
child = store.commit(w)
child.parent == root.id  # True
child.ancestry()  # [root.id, child.id]
```

`ancestry()` lists the ids from the root to the revision, root first, which is the order in which the fold reads their layers.

| Member          | Is                                                                                  |
| --------------- | ----------------------------------------------------------------------------------- |
| `id`            | the layer's UUID                                                                    |
| `store`         | the `Store` that contains the layer                                                 |
| `manifest`      | the layer's [`Manifest`](format.md#manifestjson): id, parent, schema, files, `meta` |
| `parent`        | the parent's id, `None` for a root                                                  |
| `meta`          | the revision's own `meta`, which a child does not inherit                           |
| `record`        | the revision as a [`Record`](record.md): its layer folded over its ancestors'       |
| `ancestry()`    | the ids from the root to this revision                                              |
| `materialise()` | writes the revision's fold under `resolved/`                                        |

!!! info "See also"

    [The store page](store.md) describes claims, commits and serialising a revision.
    [Branch and materialise](../howto/branch-and-materialise.md) shows the calls in use.

## The tree

Revisions form a tree.
Revisions that share one parent are branches, each with its own layer over the same ancestry.
A branch never reads a sibling's layer.

Each manifest lists its parent, so datarecord resolves a revision id to its ancestry from the store's location alone.

## Write-once

A layer's data is written once.
`Claim.commit`, which `Store.commit` wraps, writes the layer's files and then its manifest, create-only.
After that, datarecord writes into the layer directory only under `resolved/`, when you materialise the revision.
A read gives the same answers with or without those files.

A read opens exactly the files the manifest lists, and ignores any other file in the directory.

!!! info "See also"

    [Layers and `partial`](../about/layers.md) argues for write-once layers.
    [The format page](format.md) lists the files of a layer.

## `partial`

`partial` is the set of dims, declared as `Schema.partial`, along which a layer overrides one value per label.
Along any other dim, a layer restates the whole extent of each map row it touches.
The owner map assigns an attribute's values to a layer per map row. Its index is the attribute's dims that are in `partial`, with the attribute's name:

```text
index(attribute) = (attribute.dims ∩ partial, attribute)
```

With `partial = {"entity"}`, `p_max_pu` over `entity` and `snapshot` has a map row per entity. The owner map assigns the snapshot series of each entity whole to one layer:

<figure class="diagram">
--8<-- "docs/static/layers-partial.svg"
</figure>

The child changes wind at snapshot 1 and so writes wind's whole series.
The read takes gas from the root, since the child writes no gas row, and wind from the child.
A [`WorkingRecord`](editing.md) stages the rest of the series for you: `set("p_max_pu", 0.9, entity="wind", snapshot=1)` also stages wind's row at snapshot 0.

- A multi-dim in `partial` is in the index as its struct column, so a layer overrides one tuple's value. With `connection` outside `partial`, a layer that changes one connection's `efficiency` restates every connection's.
- The owner map assigns a curve whole to one layer, because `breakpoint` is not in the index.
- `partial` applies to attribute values alone. A layer that changes an axis restates the whole axis, whatever `partial` contains ([removing labels](#removing-labels)).
- Only a property of a dim in `partial` can be categorising ([the data model](data-model.md)).
- The fold reads each layer under the schema in its own manifest, so a child may add a dim to `partial` ([a wider schema](resolution.md#a-wider-schema)).

Under the PyPSA schema's `partial`, a layer overrides one bus's `v_nom` and restates every carrier's `co2_emissions`, because `carrier` is not in `partial`.

!!! info "See also"

    [The schema page](schema.md) lists the `partial` key and what it accepts.
    [Layers and `partial`](../about/layers.md) argues for putting `partial` on the dim.

## What a layer may contain

The writer checks each file of a layer against the schema in the layer's manifest, as it writes the file.
A refused layer stays pending, and no read opens it.

### Types

The writer casts each column to its declared dtype, from a wider type of the same kind or from a column of NULLs alone.
It refuses any other type, naming the attribute, property or dim and its dtype.

### Attribute files

- The writer refuses an attribute that the schema does not declare.
- An attribute file has exactly a column per dim of its attribute, then `attribute`, `breakpoint` and `value` ([the long schema](format.md#attributesattrparquet)). The writer refuses a file that lacks a dim's column, or that has a column the attribute is not over.
- A multi-dim's column is a struct with exactly its fields, and the writer refuses a tuple with some fields NULL and not all.
- The writer refuses two rows of one attribute at one coordinate that set the same dims. It also refuses two rows there where each row leaves NULL a dim that the other row sets, unless a third row sets both.

### Axis files

A `dimensions/<dim>.parquet` file contains the whole axis at the new revision, in order, with each label's properties.
The writer refuses a file for an undeclared dim or a [dim with `one_of`](data-model.md), and a file with a label twice.
It refuses a column other than the dim's column and the subject's declared properties.
It also refuses a file with its parent's labels in a different order from the parent's, and a new label may sit anywhere:

```text
ValueError: dimensions/entity.parquet puts entity 'wind' out of its parent's order; a restatement keeps the order of the labels its parent holds and places a new one anywhere (https://energy-models.github.io/datarecord/reference/data-model/#dims-and-labels)
```

### Labels

The labels and tuples in every row of a layer are on the layer's final axes.
The final axis of a dim is the layer's own file, or else the parent's resolved axis.
The writer refuses an attribute row with a label or tuple outside the final axes.
It refuses a multi-dim's tuple that contains a label outside its field's final axis the same way:

```text
ValueError: attributes/p_max_pu.parquet names 'entity' labels ['coal'], which the layer's dimensions/entity.parquet does not hold; add them there or drop those rows (https://energy-models.github.io/datarecord/reference/layers/)
```

### `LayerData`

`LayerData` is the protocol the writer reads: the rows of one layer, as pairs of an enumerator and a read.

```python
class LayerData(Protocol):
    @property
    def schema(self) -> Schema: ...
    def axes(self) -> set[str]: ...
    def axis(self, dim: str) -> DuckDBPyRelation | None: ...
    def attributes(self) -> set[str]: ...
    def attribute(self, name: str) -> DuckDBPyRelation | None: ...
```

`axis` returns the rows of one axis file, and `attribute` the long rows of one attribute file.
`Store.commit`, `Claim.commit` and `Workspace.write` also take a [`RecordLike`](record.md), such as a framework object that a [tool](tools.md) presents, and read its relations through these pairs.

!!! info "See also"

    [Store a record](../howto/store-a-record.md) shows a write from start to end.

## Removing labels

To remove a label is to restate its dim's axis without it, which removes its values and the tuples that contain it.
`remove(dim, labels)` on a [`WorkingRecord`](editing.md) writes one layer that restates the axis without the labels, and every multi-dim over `dim` without the tuples that contain them:

<figure class="diagram">
--8<-- "docs/static/layers-removal.svg"
</figure>

The child writes no `p_nom` file, and the read still has no `p_nom` row for coal.
A removal applies to every attribute.
On a dim in `partial` the fold drops each parent row that has the label, and on any other dim [the read drops it](resolution.md#labels-outside-the-index).
A row that leaves the dim NULL stays, and applies to the labels that remain.

- A removed tuple takes its values. The labels in it keep their other rows, so removing `(gas, north)` leaves `gas`.
- `remove` refuses a dim with `one_of`, whose axis is the schema's list.
- A sibling branch keeps a label that one branch removes.
- A label added again after its removal has none of the values it had below the removal. Every row that leaves its dim NULL applies to it again, whichever layer wrote the row.

The writer refuses a layer that leaves a removed label in a reference.
A reference is a multi-dim's tuple, or a property that [refers to a dim](data-model.md).
The writer checks each subject whose rows contain a label the layer removes.
It also checks each property of a subject the layer restates against every label removed so far.
So where an ancestor left such a reference, the writer refuses the next layer that restates its subject.
`remove` refuses a label while a property has it as a value.

!!! info "See also"

    [Edit a record](../howto/edit-a-record.md) shows `add` and `remove`.
    [Layers and `partial`](../about/layers.md) argues for removal as restatement.
