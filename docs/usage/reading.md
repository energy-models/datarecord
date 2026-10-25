<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reading a record

## The `Record` protocol

A consumer codes against the read-only `RecordLike` protocol.
A plain directory, a hundred-layer tree, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds ([design](../design/use/reading.md)).

```python
record.schema  # what may exist: the axes, the groups, the properties, the attributes
record.dims["entity"]  # axis labels, keyed by dim
record.groups["connection"]  # group rows, keyed by group, one relation each
record.properties["entity"]  # stored property values, keyed by dim or group
record.attributes["p_max_pu"]  # long relations, results included, keyed by attribute
record.flags(ctype="Generator")  # which axes each attribute actually uses
```

Every value is a `duckdb.DuckDBPyRelation` on `record.con`, which is a query plan.
Nothing is read until you fetch it with `.df()`, `.pl()` or `.to_arrow_table()`, and listing the keys reads nothing ([design](../design/use/reading.md#relations)).

```python
entities = record.properties["entity"]
gens = entities.filter("ctype = 'Generator'").df()
p_nom = record.attributes["p_nom"].filter("value > 0").df()
```

## Labels and properties

`dims[dim]` is the axis's label column alone, in axis order.
A multi-period snapshot's period is a property of the snapshot ([design](../design/model/dims.md#mapping-labels-to-another-dim)):

```python
record.dims["snapshot"]  # snapshot
record.properties["snapshot"]  # snapshot | period | timestep
```

`properties[subject]` is a dim's or a group's key with every property declared for it, as stored.
A NULL there is a value no layer wrote.

A categorizing property is a property of a `partial` dim that refers to a dim with `one_of`.
One category's labels are `properties[dim]` filtered on that property, with every property of the dim.
`Schema.default_for("entity.sign", **categories)` gives an unset property's default, `default_by` per category included.
It names the property by its subject and the categories by their plain names, as the frame's columns are ([design](../design/use/reading.md#properties-and-categories)).

## Wide and long

`dims`, `groups` and `properties` are wide, with one row per label or tuple.
`attributes` are long, with one row per value:

```text
<coordinate> ... | attribute | breakpoint | value
```

`attributes` holds every attribute in long rows, whatever its `dims`, so `p_nom` over `entity` alone has the same layout as `p_max_pu`.
The coordinates are the attribute's own ([design](../design/storage/format.md#the-long-schema)).
`p_nom` has `entity`, a connection attribute such as `efficiency` has one `connection` struct column, and an attribute addressed by another axis alone has no entity column.

A NULL in a coordinate column means every label of that dim ([design](../design/model/attributes.md#the-broadcast-rule)).
A constant `p_max_pu` is one row with `timestep = NULL`, and a varying one is a row per timestep.
One `p_max_pu` row with `entity = NULL` reaches every entity, loads included.
A group column broadcasts the same way: a NULL `connection` means every connection.
`breakpoint` carries the abscissa of a piecewise-linear value.

`attributes` shows stored rows only, in no promised order.
Two rows may cover one coordinate, of one layer or of two, and the row naming every dim the others there name gives its value.
A tool settles them with `settle` ([design](../design/tools/index.md#settling-rows)).
A coordinate that no row covers takes the attribute's default, which `Schema.default_for(name, **categories)` gives per category.

An attribute row holds no property, so `attributes["p_max_pu"]` holds every entity's rows together.
To scope it to one category, join `properties["entity"]`, filtered on the category, on `entity`.

## `flags`

`flags()` says which axes each attribute's rows use, across the record in one query, so a consumer can plan its reads before opening a file.
`flags(**categories)` answers for one category of one dim, and counts only rows that reach a matching label:

```python
flags = record.flags(ctype="Generator")
set(flags)  # which attributes generators have at all
"timestep" in flags["p_max_pu"].varies  # some row sets it
"timestep" in flags["p_max_pu"].broadcast  # some row leaves it NULL
flags["marginal_cost"].breakpoints  # some row carries a curve
```

A dim can be in both sets.
Then some labels carry a per-timestep series and others a single constant row, and the consumer uses both containers ([design](../design/use/reading.md#flags)).

## Reading a directory

A parquet directory is a record, and reading one involves no layers:

```python
from datarecord import Workspace

ws = Workspace()
record = ws.at("s3://bucket/my-record/")
record.attributes["p_nom"].df()
```

The layout is the whole format ([design](../design/storage/format.md)):

```text
record/
├── manifest.json            # the schema, and the record's id
├── dims/<dim>.parquet       # one axis per declared dim: labels and their properties
├── groups/<group>.parquet   # which tuples of the group exist, and their properties
└── attributes/<attr>.parquet  # one attribute per file, whatever its dims, results included
```
