<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The record format

A record on disk is a directory with `manifest.json`, `dims/` and `attributes/`, and any parquet reader can open it.
An axis file holds its key, then its properties, in order.
An attribute file holds the attribute's own coordinates, then `attribute`, `breakpoint` and `value`.
The section in which a name is declared decides the file its values go in.

[A write](../use/writing.md) produces the directory, and `Workspace.at(uri)` reads it.
A record that is never written has no directory and answers [the protocol](../use/reading.md) all the same.

```text
record/
├── manifest.json                   # the schema, and the record's id
├── dims/<dim>.parquet              # one axis per declared dim, multi-dims included: labels and properties
└── attributes/<attr>.parquet       # one attribute per file, results included
```

Each file is named for what it holds: `dims/scenario.parquet` holds the `scenario` axis, and `attributes/p_nom.parquet` holds `p_nom`.
A file takes the declared name unchanged, so the dim `bus` lives in `dims/bus.parquet`.

## Axis files

`dims/<dim>.parquet` holds the labels of the dim that exist at the layer's node, in order, with the properties that describe each.

```text
<dim> | <property> ...
```

In [the running example](../index.md#the-running-example) `dims/entity.parquet` is `entity | ctype | carrier | sign | type`.
A `Record` hands an axis file over as `dims[dim]`, and [the writer](../use/writing.md) writes it.

A label exists at a node when the deepest `dims/<dim>.parquet` on its path has a row for it.
That file's row order is the axis order.
A layer removes a label by writing the file without it ([removal](../layers/removal.md)).
Each property is one column, NULL where no layer wrote a value, so a category's frame is a filter on its property column.

## Where a value lives

The section that declares a name decides its file.
One name is one spec in one file.
A property belongs to its subject and is a column of that subject's file alone, so `bus.x` and an attribute `x` live in two places.

| declared as             | lands in                                        |
| ----------------------- | ----------------------------------------------- |
| property of a dim       | `dims/<dim>.parquet`, a column of the axis file |
| property of a multi-dim | `dims/<multi-dim>.parquet`, likewise            |
| attribute, any `dims`   | `attributes/<attr>.parquet`, long rows          |

`attributes/p_nom.parquet` holds `entity | attribute | breakpoint | value`, one row per entity.
Widening `p_nom` over `scenario` adds a `scenario` column to the same file.
That column is NULL in every row written before, and [the broadcast rule](../model/attributes.md#the-broadcast-rule) reads those rows unchanged.
A scenario `weight` over `scenario` alone is long rows too.

An axis file therefore carries its key and its subject's properties, and no attribute values.
A framework's constant frame per category joins the category's properties with the coordinate-only rows of its attributes, and the [tool](../tools/index.md) builds it.

A [multi-dim](../model/multi-dims.md)'s tuples live in `dims/<multi-dim>.parquet`: every tuple the multi-dim has at the layer's node, in order.
The file is keyed by [one struct column](../model/multi-dims.md#one-struct-column) named for the multi-dim, as a plain dim's file is keyed by the column named for it.
For the `connection` multi-dim over `(entity, bus)`, `dims/connection.parquet` is `connection STRUCT(entity, bus) | role`, where `role` is a property of `connection`.
An attribute row carries the multi-dim in the same struct column, so no read or write converts between two shapes.
A record with no file for a multi-dim has no tuples of it.

A multi-dim's property that refers to a dim is a plain column of its file beside the key: `dims/bus_country.parquet` is `bus_country STRUCT(bus, scenario) | country`.
A classification of one coordinate is a [property](../model/properties.md#properties) of the dim, so `bus.country` is a column of `dims/bus.parquet`.
A property that refers to a dim is stored like any other property, and nothing in the layout depends on it.

## The long schema

Every `attributes/` file carries its attribute's own coordinates, then the columns every row has:

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are what the attribute's [`dims`](../model/attributes.md#attributespec) declare, multi-dims first.
A [multi-dim](../model/multi-dims.md) is one struct column named for it, with a field per coordinate.
`attributes/efficiency.parquet` over the `connection` multi-dim carries `connection STRUCT(entity, bus)`.
`attributes/p_max_pu.parquet` over `entity` and `snapshot` carries `entity | snapshot` and no `bus`.
The writer refuses a multi-dim's column that is not a struct of exactly its coordinates.
DuckDB casts a struct by field name, so a missing field would turn NULL, an extra field would be dropped, and every row would silently change its key.

Each attribute file has its own column set.
A fixed prefix such as `entity | bus` would put an all-NULL `entity` on a record-level weighting, which has no entity.
`UNION ALL BY NAME` supplies NULL for a column a file does not carry, so the differing shapes cost nothing.
The same rule reads a file written before a dim was declared ([resolving a relation](../layers/resolution.md#resolving-a-relation)).
The fold's key is uniform across files: [`partial_dims`](../layers/tree.md#partial-the-granularity-of-an-override) plus `attribute`, one fixed tuple over every attribute.
A coordinate that an attribute does not carry reads as NULL in that key.
The [owner map](../layers/resolution.md#owner-map) holds each key as its layer wrote it, so a NULL there means every label, as it does in the file.

Each file holds one attribute, so `value` carries that attribute's dtype.

## A label is unique within its dim

An axis has one row per label, and a label's properties are columns of that row.
In the example, two entities both called `north` collide, whatever their `ctype`.
A bus called `north` is a label of another dim and collides with neither.
`add` of a label already there restates its row ([add / remove](../use/editing.md#add-remove)).

An `attributes/` row addresses a label by the label alone, so `set("p_nom", 150.0, entity="wind1")` names no `ctype` ([set](../use/editing.md#set)).
A consumer that wants the `p_max_pu` of one `ctype` joins the resolved attribute to the resolved `entity` axis on `entity`.

A modelling framework that scopes names per class reconciles them before it writes, and its [tool's `verify`](../tools/pypsa.md#what-verify-reports) reports a collision.
The record layer never renames a label.
A label is the framework's own name, and a renamed label would come back as one the framework cannot find.
