<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Dims and labels

A dim is a set of unique labels, declared with a dtype from mathspec. The row order of a dim's file is its order. The axis holds the labels that layers have added, except for a dim with `one_of`, whose axis is its list ([a dim with `one_of`](#a-dim-with-one_of)). A property may map each label to a label of another dim, such as a snapshot to its period ([mapping labels to another dim](#mapping-labels-to-another-dim)).

Every dim is declared in the schema, so a record with `region`, `technology` or `vintage` needs no code change. A `Dimension` declares the axis's type and, optionally, its `one_of`, `unit` and `description` ([the schema](schema.md)). A dim declaring `over` instead is a [multi-dim](multi-dims.md), whose labels are tuples of other dims' labels. Which dims an attribute varies over is [declared per attribute](attributes.md#attributespec). The patch granularity is [`partial`](../layers/tree.md#partial-the-granularity-of-an-override). What describes or classifies a label is one of the dim's [properties](properties.md#properties).

## Axis order

An axis's order is the row order of its frame in `dims`. The labels of an axis, a multi-dim's tuples among them, come in the row order of the deepest layer file for that axis ([one fold for every axis](../layers/resolution.md#one-fold-for-every-axis)). A label or tuple that one layer removes and a later layer adds again sits where the later layer's file puts it.

Writers need to ensure data is not inadvertently reordered from its parent's labels, since the order may carry meaning, such as port numbering, which follows from the `connection` order.

## A dim with `one_of`

A dim with `one_of: [labels]` has exactly those labels, as a mathspec dimension does. Its `dtype` is `str`, the default. A `one_of` on any other dtype is refused at load. So is an empty list, a label listed twice, a label that is no string, and a dim name in place of the list. The dim's column is an `ENUM` of the labels ([dtypes](schema.md#dtypes)).

The axis is the list, in list order. Its labels are the schema's, not data:

- **No layer file.** No `dims/<dim>.parquet` is written or read for it, nor a `resolved/dims/` cache. Every read derives the axis from the schema, so `record.dims["ctype"]` is the list. The writer refuses a layer that hands over such a file, and the read refuses a layer that holds one ([axis files](../storage/format.md#axis-files)).
- **No properties.** With no file there is no column to hold one, so a property declared on the dim is refused at load.
- **No edits.** [`add` and `remove`](../use/editing.md#add-remove) on it are refused; a new label is a schema change.
- **Otherwise a dim.** An attribute may range over it, a multi-dim's coordinate may draw on it, and a property may refer to it. A value naming a label the list does not hold is refused where a dangling label is. The dim has no dead labels, so nothing cascades from it ([deletion](../layers/removal.md)).

A category is checked at load wherever it is named ([categories](properties.md#categories)), and an axis that is the list makes every checked category a label the record holds.

## Mapping labels to another dim

A dim's labels may map to labels of another dim, such as each snapshot to its period. A property that refers to the other dim holds that map, with at most one label per label of its subject. mathspec declares the same map as a mathspec relation, `{key: snapshot, values: period}`. PyPSA's multi-period `(period, timestep)` snapshot is a dim `snapshot` with two such properties, `snapshot.period` and `snapshot.timestep`. A snapshot label is unique across the dim, so a source that repeats its timesteps in every period gives each snapshot a label of its own ([PyPSA](../tools/pypsa.md)). A level above the period is a property of `period`, such as `period.horizon` ([a chain is a join](properties.md#referring-to-a-dim)). A selector keyword such as `period=2030` picks the snapshots of that period ([`set`](../use/editing.md#set)).

```python
properties = {
    "snapshot": {
        "period": PropertySpec(one_of="period"),
        "timestep": PropertySpec(one_of="timestep"),
    }
}
```
