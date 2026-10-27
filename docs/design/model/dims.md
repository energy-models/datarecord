<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Dims and labels

A dim is a set of unique labels, declared with a dtype from mathspec. The row order of a dim's file is its order. A dim with `one_of` allows only the labels its `one_of` names. The axis holds the labels that layers have added. A property may map each label to a label of another dim, such as a snapshot to its period ([mapping labels to another dim](#mapping-labels-to-another-dim)).

Every dim is declared in the schema, so a record with `region`, `technology` or `vintage` needs no code change. A `Dimension` declares the axis's type and, optionally, its `one_of`, `unit` and `description` ([the schema](schema.md)). Which dims an attribute varies over is [declared per attribute](attributes.md#attributespec). The patch granularity is [`partial`](../layers/tree.md#partial-the-granularity-of-an-override). What describes or classifies a label is one of the dim's [properties](properties.md#properties).

## Axis order

An axis's order is the row order of its frame in `dims`. The labels of an axis and the rows of a group come in the row order of the deepest layer file for that axis or group ([one fold for every axis](../layers/resolution.md#one-fold-for-every-axis)). A label or tuple that one layer removes and a later layer adds again sits where the later layer's file puts it.

Writers need to ensure data is not inadvertently reordered from its parent's labels, since the order may carry meaning, such as port numbering, which follows from the `connection` order.

## A dim with `one_of`

A dim with `one_of: [labels]` allows only those labels, as a mathspec dimension does. Its `dtype` is `str`, the default. A `one_of` on any other dtype is refused at load. So is an empty list, a label listed twice, a label that is no string, and a dim name in place of the list. The dim's column is an `ENUM` of the labels ([dtypes](schema.md#dtypes)).

The list bounds the axis. [`add`](../use/editing.md#add-remove) refuses a label outside the list. The axis file holds the labels that layers have added and not removed. [`remove`](../use/editing.md#add-remove) works as on any dim. A property that refers to the dim holds a live label like any other. A tool may write the whole list, as [PyPSA's](../tools/index.md) does for `ctype` and `role`. That is the tool's convention.

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
