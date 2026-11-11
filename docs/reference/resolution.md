<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Resolution

This page describes how datarecord reads the layers of a revision as one record.
It covers axes, the owner map, crossing rows, what a read returns, flags, materialised revisions and a directory.

A read returns one row at each coordinate, by two rules.
Within one layer, datarecord keeps the more specific row, by [the broadcast rule](data-model.md).
Across layers, a read returns the deeper layer's row where rows of two layers meet.

<figure class="diagram">
--8<-- "docs/static/resolution-overview.svg"
</figure>

The step that folds a revision's [layers](layers.md) one at a time, root first (the fold), builds the owner map.
The map lists, for each attribute, each key a layer wrote and the layer it assigns the key to.
A read of an attribute joins to the map the files of the layers it lists, and returns the rows as written.
A [reader](record.md) settles each coordinate by the broadcast rule.

## Axes

A layer that changes an axis restates it whole, so a revision's axis is the deepest `dims/<dim>.parquet` on its path from the root.
A layer with no file for a dim leaves its parent's axis as it is.

<figure class="diagram">
--8<-- "docs/static/resolution-axes.svg"
</figure>

The child's entity file is the resolved entity axis, and the root's scenario file is the resolved scenario axis.

- A multi-dim's axis resolves the same way, from its file keyed by its struct column.
- The labels of an axis come in the deepest file's row order, the axis order. A label added again after its removal sits where the file that adds it puts it.
- A property is a column of its subject's axis file, so its values change only when a layer restates the axis.
- A [dim with `one_of`](data-model.md) has no file, and its axis is the schema's list at every revision.

## The owner map

The owner map is the fold's result: per attribute, each key a layer wrote and the layer the map assigns it to.
A key is the attribute's dims in [`partial`](layers.md#partial) and the attribute's name.
The map contains no values, so its size does not depend on the length of a series or a curve.

| Column                   | Contains                                                                                     |
| ------------------------ | -------------------------------------------------------------------------------------------- |
| one per dim in `partial` | the key's label, or NULL where the row leaves the dim NULL. A multi-dim is one struct column |
| `attribute`              | the attribute's name                                                                         |
| `layer_uuid`             | the layer the map assigns the key to                                                         |
| `varies`                 | a struct with a boolean per dim: whether the layer's rows of this key set the dim            |
| `broadcast`              | a struct with a boolean per dim: whether the layer's rows of this key leave the dim NULL     |
| `breakpoints`            | whether the layer's rows of this key form a curve                                            |

The map has each key as its layer wrote it, a NULL included.
It has one row per layer and distinct key, and one overlap row for each pair of rows that [cross](#crossing-rows).

## Folding a layer onto its parent

The fold takes the parent's map and the layer's own keys, and returns the layer's map.
Three terms describe how the rows of two layers meet:

| Term    | Of two rows, means                                                                                                       |
| ------- | ------------------------------------------------------------------------------------------------------------------------ |
| sets    | a row sets a dim in `partial` where its key has a label, and any other dim of the attribute where its rows vary along it |
| covers  | on each dim in `partial`, the two rows have the same label, or one of them has NULL                                      |
| crosses | the two rows cover a common coordinate, and each row leaves NULL a dim that the other row sets                           |

The fold runs three steps, in this order:

1. It drops each parent row that has a label or tuple that the layer [removes](layers.md#removing-labels), on a dim in `partial`. A row that leaves the dim NULL stays.
2. It drops each parent row that a row of the layer covers, where the layer's row sets every dim that the parent row sets.
3. It adds the layer's rows, and one overlap row for each parent row that a row of the layer crosses.

Take `partial = {"entity", "scenario"}` and a root that sets `p_nom` for gas, wind and coal.
Its child removes coal, sets gas to 60 and then sets 999 for the scenario high:

<figure class="diagram">
--8<-- "docs/static/resolution-fold.svg"
</figure>

The fold drops the root's coal row, which has the removed label, and the root's gas row, which the child's gas row covers.
It keeps the root's wind row and adds the overlap row `(wind, high)`, which the owner map assigns to the child.
The child's own `(gas, high)` is a row that its [`WorkingRecord`](editing.md) staged, because the second `set` crosses the first.

A parent row that sets fewer dims than a covering row of the layer stays as written.
A child that writes only `(gas, high) = 70` leaves the root's `(gas, NULL)` in the map. By the broadcast rule, a read takes `(gas, high)` from the child.

## Crossing rows

Two rows of two layers cross where they cover a common coordinate and each row leaves NULL a dim that the other row sets.
At such a coordinate neither row is more specific, so the broadcast rule alone does not settle it.
The fold therefore adds an overlap row, which the owner map assigns to the deeper layer, with every label that either row sets.

<figure class="diagram">
--8<-- "docs/static/resolution-crossing.svg"
</figure>

The child's `(NULL, high)` crosses both root rows, so the map gains `(gas, high)` and `(wind, high)`, which it assigns to the child.
At `(wind, high)` the overlap row sets both dims, so by the broadcast rule the value there is 999.
The crossed parent rows stay as written, and `(wind, low)` is 100.

A later fold treats an overlap row as any other row, which it may drop, cross or remove.

## Where a row applies

A NULL on a dim in `partial` means every label of that dim's axis as resolved at the revision read.
A row therefore applies to a label added below the revision that wrote it, or one staged in a `WorkingRecord`.
In the example above, a child that adds the scenario `mid` has `(gas, mid)` at 50.

A row applies at every coordinate that no more specific row covers, whichever layer wrote it.
So a read returns a deeper layer's value only at the coordinates its rows apply to.
With `p_max_pu` over `entity`, `scenario` and `snapshot`, a root row `(gas, NULL, 1) = 1.0` and a child row `(NULL, high, 0) = 2.0` leave `(gas, high, 1)` at 1.0, from the root.

## Reading an attribute

`record.attributes[name]` returns the attribute's rows from the layers that the map assigns a key to, as written.
A key left NULL stays NULL.
Where the map has an overlap row, the read also returns a value at the overlap row's labels. That value comes from the layer the map assigns the overlap row to.
The read expands no NULL, settles no coordinate, and returns rows in no fixed order.
An attribute that no layer wrote is an empty relation, typed as declared, and the reader applies the schema's `default`.

A layer may contain rows of one attribute that set different dims in `partial`.
The child above is one: its `(gas, NULL)`, `(NULL, high)` and `(gas, high)` all apply at `(gas, high)`.
At such a coordinate the read keeps the layer's row that leaves no dim NULL that another row there sets.
Where no row does, the read fails, naming the attribute and the coordinate.
A curve is kept or dropped whole.

!!! info "See also"

    [The record page](record.md) describes `RecordLike` and settling rows.
    [Why the owner map contains keys as written](../about/layers.md#why-the-owner-map-contains-keys-as-written) gives the reasons for the map's shape.

## Labels outside the key

A dim outside `partial` is in no key, so the fold reads none of its labels.
The read drops each row that has a removed label on such a dim, or a removed tuple of such a multi-dim.
A removed label is one that a layer's restatement dropped and the revision's axis lacks.

Take `partial = {"entity"}`, a root with `p_max_pu` for wind at snapshots 0, 1 and 2, and a child that removes snapshot 2:

<figure class="diagram">
--8<-- "docs/static/resolution-labels-outside.svg"
</figure>

The child writes no `p_max_pu` file, yet the read drops the root's row at snapshot 2 and returns its other two rows.

With `connection` outside `partial`, a connection attribute's key is the attribute's name alone.
Removing a tuple then removes its values at read, as it does for a multi-dim in `partial` at the fold.

## A dim outside `partial`

A crossing along a dim outside `partial` has no key column for an overlap row to set.
A `WorkingRecord` settles such a crossing as it stages.
Where a staged row leaves the dim NULL over a parent row that sets it, the commit also writes the value along the parent's labels.

<figure class="diagram">
--8<-- "docs/static/resolution-outside.svg"
</figure>

With `partial = {"entity"}`, the child's rows at wind set every dim that the root's rows set.
Nothing crosses, and wind is 0.9 at each snapshot.
The fold refuses a layer written another way that leaves such a crossing.
It checks the layer when a workspace first folds it:

```text
ValueError: layer 0f74b479-422a-4364-b29f-36758ea3105c leaves 'snapshot' NULL for 'p_max_pu' at {'entity': 'wind'} where a parent row names it; write it along the parent's 'snapshot' labels there, as `WorkingRecord.set` does
```

## A wider schema

The fold reads each layer under the schema in its own manifest.
A child's schema may be wider than its parent's where the change passes [`compatible_with`](schema.md).
The fold then extends the parent's map to the child's schema before it folds the child.

- Where a dim is added to an attribute's `dims`, the parent's rows of that attribute broadcast along it.
- Where a dim is added to `partial`, the fold splits each parent row that varies along it by the labels that its layer wrote there.

<figure class="diagram">
--8<-- "docs/static/resolution-widen.svg"
</figure>

Under `partial = {"entity"}`, the owner map assigns gas's scenarios whole to the root.
Under the child's `partial = {"entity", "scenario"}`, the fold splits the root's row into `(gas, low)` and `(gas, high)`, and the child's `(gas, high)` covers the second.
The read gives `(gas, low) 1` and `(gas, high) 3`.

## Flags

The fold computes `varies`, `broadcast` and `breakpoints` for each row of the map from the layer's rows of that key.
An overlap row takes the flags of the layer row it comes from.
`record.flags()` unions them per attribute:

<figure class="diagram">
--8<-- "docs/static/resolution-flags.svg"
</figure>

`p_max_pu` has `snapshot` in both sets, because wind's rows set it and gas's row leaves it NULL.
`p_nom` broadcasts along `entity` and `scenario`, the two dims it is over.

- `flags(**categories)` unions the flags of the rows that apply to the labels of those categories. A row NULL on the dim applies to every label.
- Each struct has a field for every dim the schema declares. A dim that a multi-dim draws on has its own field, so `flags()` reports `bus` as broadcast for a `marginal_price` constant over buses.
- The flags describe the rows in the map. So a NULL row that deeper rows cover at every label still puts its dim in `broadcast`.

!!! info "See also"

    [The record page](record.md) describes what a reader does with `flags`.

## Materialised revisions

To materialise a revision is to write its owner map, pointers to the files of its axes and its removed labels under `layers/<id>/resolved/`, with `Revision.materialise()`.
A read of a descendant then stops at that revision and reads no layer above it.

A fold starts from the deepest proper ancestor whose fold is at hand, either materialised on disk or kept as tables by the workspace.

<figure class="diagram">
--8<-- "docs/static/resolution-nearest.svg"
</figure>

For a record opened on `child`, the fold starts from `c`, the deepest fold at hand, and folds `d` and then `child` onto it.
It reads nothing of `root`, `a` or `b`.

- A read gives the same answers after you materialise, and opens fewer layers.
- datarecord never starts a revision's fold from that revision's own cache.
- A materialised map has keys as written, so a NULL in it applies to a label that a descendant adds.
- A read under a wider schema extends a materialised map as it extends one folded live.

!!! info "See also"

    [The format page](format.md#resolved) lists the files under `resolved/`.
    [Branch and materialise](../howto/branch-and-materialise.md) shows when to call `materialise()`.

## A directory

`Workspace.at(uri)` reads a standalone directory as a fold over one layer, whose layout is a layer's.

<figure class="diagram">
--8<-- "docs/static/resolution-directory.svg"
</figure>

The owner map assigns every key to the one layer, the axis order is each file's row order, and `partial` has no effect.
One `Record` reads both a directory and a revision, so a reader cannot tell them apart.
`store.layer(id)` reads one layer of a store alone in the same way.

!!! info "See also"

    [One interface over a directory and a tree](../about/one-record.md#one-interface-over-a-directory-and-a-tree) gives the reasons for one `Record`.
    [The store page](store.md) describes `Workspace.at` and `Workspace.write`.
