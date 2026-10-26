<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reading a record

## The `Record` protocol

A consumer codes against the read-only `RecordLike` protocol.
A plain directory, a hundred-layer tree, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds ([design](../design/use/reading.md)).

```python
record.schema  # what may exist: the axes, the properties, the attributes
record.dims["entity"]  # axis labels, keyed by dim
record.dims["connection"]  # a multi-dim's tuples, as one struct column
record.properties["entity"]  # stored property values, keyed by dim
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

A multi-dim's labels are its tuples, so `dims["connection"]` is the one struct column `connection STRUCT(entity, bus)`, in its own order ([design](../design/model/multi-dims.md#one-struct-column)):

```python
record.dims["connection"]  # connection
record.properties["connection"]  # connection | role
```

`properties[subject]` is a dim's key with every property declared for it, as stored.
A NULL there is a value no layer wrote.

A categorizing property is a property of a `partial` dim, a multi-dim among them, that refers to a dim with `one_of`.
One category's labels are `properties[dim]` filtered on that property, with every property of the dim.
`Schema.default_for("entity.sign", **categories)` gives an unset property's default, `default_by` per category included.
It names the property by its subject and the categories by their plain names, as the frame's columns are ([design](../design/use/reading.md#properties-and-categories)).

## Wide and long

`dims` and `properties` are wide, with one row per label or tuple.
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
A multi-dim's struct column broadcasts the same way: a NULL `connection` means every connection.
`breakpoint` carries the abscissa of a piecewise-linear value.

`attributes` shows stored rows only, in no promised order.
Two rows may cover one coordinate, of one layer or of two, and the row naming every dim the others there name gives its value.
A consumer settles them with `settle`, as [settling rows](#settling-rows) shows.
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

## Settling rows

`attributes` gives rows as written, so a consumer applies the broadcast rule itself.
`settle(rel, labels, flags)` does it in one query, and `flags` tells it how little it needs to do ([design](../design/use/reading.md#settling-rows)):

```python
from datarecord import settle

flags = record.flags(ctype="Generator")
gens = record.properties["entity"].filter("ctype = 'Generator'").select("entity")
p_max_pu = settle(record.attributes["p_max_pu"], {"entity": gens}, flags["p_max_pu"])
```

`labels` names the coordinates to settle, each with a relation of its labels in a column of the coordinate's name.
A NULL there becomes one row per label, a row naming a label not passed is dropped, and at each coordinate the most specific row is kept.
So the rows of other categories need no filter first.
A coordinate left out of `labels` stays as written, so a constant `p_max_pu` stays one row with `snapshot` NULL rather than one row per snapshot.
Leave out every coordinate your container can hold a broadcast for.

`flags` decides, per coordinate in `labels`, how much work the query does:

| the coordinate is in     | the rows                         | `settle`                                  |
| ------------------------ | -------------------------------- | ----------------------------------------- |
| `varies` only            | all name it                      | keeps the rows naming a passed label      |
| `broadcast` only         | all leave it NULL                | expands each row over the labels          |
| `varies` and `broadcast` | some name it, some leave it NULL | expands, then picks the most specific row |

The pick is a window over every coordinate and the costly step, so it runs only where a coordinate is in both sets.
`flags(**categories)` counts only the rows reaching that category, which often takes a coordinate out of one set and spares the pick.
Flags of a wider scope overstate the rows and give the same answer, only more slowly.

`flags` also chooses the container before any value is read:

```python
f = flags["p_max_pu"]
if "snapshot" not in f.varies:
    ...  # one value per generator
elif "snapshot" not in f.broadcast:
    ...  # one series per generator
else:
    ...  # constants for some generators, series for others
```

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
├── dims/<dim>.parquet       # one axis per declared dim, multi-dims included: labels and their properties
└── attributes/<attr>.parquet  # one attribute per file, whatever its dims, results included
```
