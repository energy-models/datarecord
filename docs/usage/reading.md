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
The keys are every name the schema declares, and one that no layer holds reads an empty relation with its declared columns.

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
record.properties["connection"]  # connection | role | note
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

## Closing

A record holds the tables its fold is kept in until you `close()` it, and a `with` block closes it on exit.
Then the workspace drops them, unless another open record, a [registration](../design/storage/views.md) or a record collected without closing holds them.
The next record over the same revision folds its ancestry again, so a process reading one revision often keeps one record open rather than one per read.
A closed record raises `ValueError` on any read, and a relation taken from it before fails with DuckDB's missing-table error, so fetch what you need first.
`revision.record` hands out a new record once the one it cached is closed.

```python
with store.revision(rev_id).record as record:
    p_nom = record.attributes["p_nom"].df()
```

A record you never close keeps its tables for the workspace's life, so a chained read stays safe ([design](../design/storage/store.md#the-databases-schemas)).

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
├── manifest.json            # the record's id, its schema and the files it holds
├── dims/<dim>.parquet       # one axis per declared dim, multi-dims included: labels and their properties
└── attributes/<attr>.parquet  # one attribute per file, whatever its dims, results included
```

## Reading from SQL

`register` names a record's relations as views in the workspace's database, so a client that speaks only SQL reads it, over a DuckDB server too ([design](../design/storage/views.md)).
Over a `WorkingRecord` the views follow every edit:

```python
from datarecord import WorkingRecord

w = WorkingRecord(store.revision(rev_id).record)
w.register("edit_3f2a")
```

```sql
SELECT entity, value FROM edit_3f2a.attributes.p_nom;
SELECT * FROM edit_3f2a.properties.bus WHERE carrier = 'AC';
```

```python
w.set("p_nom", 150.0, entity="wind1")  # the next SELECT reads 150.0
```

The views are raw, as `attributes` is: a NULL coordinate means every label, and a client [settles rows](#settling-rows) itself.

A commit leaves the views reading `w`.
To go on editing the new revision, drop them and register a fresh `WorkingRecord`:

```python
child = store.commit(w)
ws.unregister("edit_3f2a")
WorkingRecord(child.record).register("edit_8c01")
ws.registered()  # name | revision | working | created
```

`registered()` reads the table `registered`, which a SQL client queries directly.
One registration's views are listed by DuckDB's catalog, where `duckdb_views().schema_name` holds only the last part of a nested schema's path, so join the schemas for the rest:

```sql
SELECT s.parent_schema, s.schema_name, v.view_name
FROM duckdb_views() v JOIN duckdb_schemas() s ON v.schema_oid = s.oid
WHERE s.parent_schema = 'edit_8c01';
```
