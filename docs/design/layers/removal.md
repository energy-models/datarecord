<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Removing labels

A layer removes a label by restating the axis without it.
Removing a label also removes its values on every attribute and the tuples of every [multi-dim](../model/multi-dims.md) that name it.
A property that still holds the label blocks its removal.
The writer checks every layer against its final axes, a multi-dim's tuples included, so every value and tuple names a label that is there.
Sibling branches keep a label that one branch removes.

## Restating an axis

A layer that changes an axis writes `dims/<dim>.parquet` whole: every label the axis has at its revision, with their properties, in order.
A layer removes a label when its axis file omits a label that its parent's axis holds.

A layer removes a multi-dim's tuple the same way, by restating `dims/<multi-dim>.parquet` without it.
An axis loses a label or tuple only where its own file omits it.

A hand-written layer whose axis file holds only a few labels is a restatement too, so it removes every label it omits.

## What a removal reaches

Removing the entity `e1` removes its `p_nom`, its `p_max_pu` at every snapshot and scenario, and its value in every other attribute.

The fold removes the values.
[`fold_attributes`](resolution.md#owner-map) drops the parent's owner-map rows that name a label or tuple the layer removes.
A key whose label or tuple was removed is then absent from the resolved map.
A row that leaves the coordinate NULL names no label and stays.
A dim outside `partial`, multi or not, is in no key, so its rows are [dropped at read](resolution.md#a-label-outside-the-key) against that coordinate's dead labels.

A removed tuple takes its values with it.
The labels it names keep their other rows, so removing a connection leaves the entity it attached.

Removing a label is one cascade, the same for every dim but one with `one_of`, whose labels are the schema's and never removed ([a dim with `one_of`](../model/dims.md#a-dim-with-one_of)).
[`remove(dim, labels)`](../use/editing.md#add-remove) restates the axis without the labels, and every multi-dim with a coordinate over `dim` without the tuples that name one, in one layer.
The tuples' values go with them.
Re-adding a label or tuple in `partial` brings back the label and no value that named it below the removal.
Every broadcast covering it, written before or after the removal, [reaches it](resolution.md#what-a-row-reaches) again.
Re-adding one outside `partial` brings back its values, since the read only hid them.

## What the writer checks

The writer refuses a layer that adds a tuple naming a dead label.
It refuses a layer that stores a dead label in a [property that refers to](../model/properties.md#properties) a dim.
It refuses a layer that removes a label which a tuple or such a property still names.
A property that holds a label must be cleared before the label goes, and [`remove`](../use/editing.md#add-remove) refuses the label until then.

The writer checks two kinds of subject, each whole and in its final state:

- each subject whose rows name a label the layer removes;
- the properties of each subject the layer restates.

It checks them against the dead labels at the layer's revision.
The labels a layer removes are its parent's axis minus its restatement.
A restated multi-dim's tuples are held to the layer's final axes, which hold no dead label.
A reference that an ancestor left dangling therefore fails the next write that touches its subject.
That write restates the subject whole, so it can drop or repair the row.

A layer's attribute rows name only labels of its final axes and tuples of its final multi-dims.
Its multi-dims' tuples name only labels of its final axes.
[The writer](../use/writing.md) checks both, so a value or a tuple names only a label that some layer added and this layer keeps.

## A layer from elsewhere

A layer may be a parquet directory that this package did not write.
Each of its `dims/` files must restate its axis whole and hold each key once.
Each such file carries its key and its subject's declared properties, and no other column.
