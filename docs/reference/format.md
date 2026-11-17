<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The record format

A record on disk is a directory of parquet files and JSON manifests. Any parquet library, such as pandas or polars, can open the parquet files.
This page describes the files of a layer, top down, then the views that a registered record adds to the database.

## The layout

A [layer](layers.md) of a store lies under `layers/<id>/` of the store's location.
A standalone directory has the same layout at its own URI, with no `claim.json` and no `resolved/`.

```text
<location>/layers/<id>/
├── claim.json                  # written when the store hands out the id
├── manifest.json               # written last: the revision exists from here
├── dimensions/<dim>.parquet          # one axis per dimension the layer restates
├── attributes/<attr>.parquet   # one attribute per file, results included
├── owner_map_rows.parquet      # owner-map rows of the shapes varying outside partial
└── resolved/                   # written by materialise()
    ├── dimension_owners.parquet
    ├── dead/<dim>.parquet
    └── owner_map.parquet
```

Each file takes the declared name unchanged, so the dimension `bus` is `dimensions/bus.parquet` and the attribute `p_nom` is `attributes/p_nom.parquet`.
A read opens the files that the manifest lists, and no other.

## `claim.json`

`claim.json` lists the id, the parent, the claimed `meta` and when the [store](store.md) handed out the id.
The store writes it create-only and keeps it after the commit, so an id is claimed once.
The manifest, not `claim.json`, is authoritative for `parent` and `meta`: a claim rebuilt from its serialised form commits what that form says ([the claim](store.md#claim)).

```json
{ "id": "4731e41b-…", "parent": "9716e2ef-…", "meta": { "run": "a" }, "created": "2026-11-05T17:37:14.912772Z" }
```

## `manifest.json`

A `Manifest` is a layer's `manifest.json`, which lists its id, parent, full schema, files and meta.
The writer writes it last and create-only, and the revision exists from that moment.

| Key      | Contains                                                                                                                                                                              |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`     | the layer's UUID                                                                                                                                                                      |
| `parent` | the parent's UUID, `null` for a root and for a standalone directory                                                                                                                   |
| `schema` | the full [schema](schema.md) the layer was written under                                                                                                                              |
| `files`  | `{"dimensions": [...], "attributes": [...], "owner_map_rows": [...]}`: the declared names of the files the layer wrote, and the `_shape` of each shape `owner_map_rows.parquet` holds |
| `meta`   | the revision's own `meta`: a flat object of strings, integers, finite floats and booleans                                                                                             |

```json
{"id": "4731e41b-…", "parent": "9716e2ef-…", "schema": {…}, "files": {"dimensions": ["entity"], "attributes": ["p_nom"], "owner_map_rows": []}, "meta": {"run": "a"}}
```

A layer that restates only the entity axis and writes only `p_nom` lists those two files.
`meta` describes one revision, such as a run's id, and a child does not inherit it.
`schema.meta` describes the record, and a child inherits it with the rest of the schema.

## `dimensions/<dim>.parquet`

An axis file contains every label of one dimension at the layer's revision, in axis order, one row per label.
Its first column is the dimension's column, named for it, and each further column is one [property](data-model.md) of the dimension.

```text
dimensions/entity.parquet       entity | ctype | carrier | sign | type
dimensions/connection.parquet   connection STRUCT(entity, bus) | role
```

- A property is NULL in a row where no layer wrote a value for that label.
- A [multi-dimension](data-model.md)'s file has one struct column named for it, with its fields, as its first column. Its rows are every tuple of the multi-dimension at the layer's revision.
- A [dimension with `one_of`](data-model.md) has no file. The writer refuses a layer that has one.
- A layer removes a label or tuple by writing the file without it ([removing labels](layers.md#removing-labels)).

## `attributes/<attr>.parquet`

An attribute file contains the long rows of one attribute: a column per dimension of the attribute, then the columns every row has.

```text
<dim> ... | attribute | breakpoint | value
```

The dimensions are those the attribute's [`dims`](schema.md) declare, in the order the schema declares them, a multi-dimension's fields packed in one struct column.

| Attribute                               | Columns                                                        |
| --------------------------------------- | -------------------------------------------------------------- |
| `p_nom` over `entity`                   | `entity, attribute, breakpoint, value`                         |
| `p_max_pu` over `entity` and `snapshot` | `entity, snapshot, attribute, breakpoint, value`               |
| `efficiency` over `connection`          | `connection STRUCT(entity, bus), attribute, breakpoint, value` |

A NULL in a dimension's column means every label of its dimension, by [the broadcast rule](data-model.md).
`breakpoint` is NULL except on the rows of a curve.
`value` has the attribute's dtype, since each file contains one attribute.

Widening `p_nom` over `scenario` adds a `scenario` column to the files written from then on.
A file written before has no `scenario` column, and a read takes its rows as NULL there.

### Where a value lives

The section of the schema that declares a name sets the file its values go in.

| Declared as                   | Is stored in                                 |
| ----------------------------- | -------------------------------------------- |
| property of a dimension       | a column of `dimensions/<dim>.parquet`       |
| property of a multi-dimension | a column of `dimensions/<multi-dim>.parquet` |
| attribute, any `dims`         | the long rows of `attributes/<attr>.parquet` |

A property is named by its subject, so `bus.x` is a column of `dimensions/bus.parquet`, and an attribute `x` is `attributes/x.parquet`.
An axis file has no attribute values.

## `owner_map_rows.parquet`

`owner_map_rows.parquet` holds the layer's own rows of the [owner map](resolution.md#the-owner-map), which a fold would otherwise aggregate from its attribute files.
A shape is the set of dimensions an attribute is over.
The writer keeps every owner-map row of a shape where a row of the layer names a dimension of the shape outside [`partial`](layers.md#partial), and no row of any other shape.
The other shapes have an owner-map row per attribute row, so a fold aggregates them from the attribute files at no extra cost.
The manifest lists the `_shape` of each shape the file holds in `files.owner_map_rows`, in ascending order, so a read knows them without opening the file.
A layer that keeps no shape has no `owner_map_rows.parquet`, and `files.owner_map_rows` is empty.

The file has the owner map's columns but `layer_uuid`, which a read sets to the layer it reads, and the `BIGINT` column `_shape`.
Bit `i` of `_shape` is set where the row's attribute is over the `i`-th dimension of the layer's schema.
The rows are grouped by `_shape`, in ascending order, and the distinct values of `_shape` are the list in the manifest.
A manifest listing a `_shape` that is not the shape of an attribute file it lists, or listing them out of order, is refused when read.

## Labels

An axis file has one row per label, so two entities named `north` collide whatever their `ctype`.
A bus named `north` is a label of another dimension and collides with neither.

An attribute row has the label alone, so a `p_nom` row has an `entity` column and no `ctype` column.
datarecord never renames a label: a [tool](tools.md) reports a collision in a framework's names.

## `resolved/`

`Revision.materialise()` writes a revision's fold under `resolved/`.
No manifest lists these files, a read opens each by name, and a layer's data stays as it was.

| File                       | Contains                                                                                                              |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `dimension_owners.parquet` | a row `(dimension, layer_uuid, has_dead)` per axis: the layer whose `dimensions/<dim>.parquet` is the revision's axis |
| `dead/<dim>.parquet`       | the removed labels of a dimension, written only where `has_dead` is true                                              |
| `owner_map.parquet`        | the [owner map](resolution.md#the-owner-map), written last                                                            |

A revision is materialised once `owner_map.parquet` exists.
Beside the owner map's columns, `owner_map.parquet` has the `BIGINT` column `_shape`.
Bit `i` of `_shape` is set where the row's attribute is over the `i`-th dimension of the revision's schema.
The rows are grouped by `_shape`, in ascending order, so a fold that reads one shape skips the row groups of the others.
`dimension_owners.parquet` points into ancestor layers by UUID rather than copying their files.

## Registered views

`record.register(name)` creates the DuckDB schema `name` in [the workspace's database](store.md#the-database), `datarecord` by default, with a view per dimension, property subject and attribute, and a view of the owner map.
A SQL client of that database reads the record through these views.

| View                               | One per                                       | Reads as                                   |
| ---------------------------------- | --------------------------------------------- | ------------------------------------------ |
| `datarecord.name.dimensions.<dim>` | dimension, a dimension with `one_of` included | `record.dimensions[dim]`                   |
| `datarecord.name.properties.<dim>` | dimension that declares properties            | `record.properties[dim]`                   |
| `datarecord.name.attributes.<a>`   | attribute the schema declares                 | `record.attributes[a]`, empty with no rows |
| `datarecord.name.owner_map`        | record                                        | the [owner map](#the-owner_map-view)       |

The views are the record's relations unsettled: a NULL in a dimension's column means every label, two rows may cover one coordinate and no default applies.
Over a `WorkingRecord`, a view returns each edit once `set`, `add` or `remove` returns.
A view reads the tables of its own database under any name a client attaches it as: a client that attaches the workspace's file as `r` reads `r.name.dimensions.<dim>`, and never a table of its own with the same name.

DuckDB applies a filter on a dimension in [`partial`](layers.md#partial) of an attribute view, such as `WHERE entity = 'gas'`, in its scan of the layer files.
It then reads only the row groups whose statistics admit the label, and those that hold a NULL in that dimension or in one the view takes ahead of it.
The view takes the dimensions in order of how many more of the attribute's map rows set them than leave them NULL, a tie in declaration order.
Over a `WorkingRecord` it counts the frozen layers' map rows, and takes the declaration order where they do not hold the attribute.
The order changes how many row groups a filter reads, never the rows a view returns.
Parquet has no statistics for a whole struct column, so a filter on a multi-dimension skips row groups only where it tests a field.
`connection.entity = 'gas'` skips them, and `connection = {'entity': 'gas', 'bus': 'north'}` reads every row group that holds a tuple.
Where `snapshot` is a multi-dimension over `period` and `timestep`, filter on `snapshot.period IS NULL` rather than `snapshot IS NULL` for the rows that leave it NULL.
Both return the same rows, since datarecord refuses a tuple with a NULL field, and only the first skips row groups.

### The `owner_map` view

`datarecord.name.owner_map` is the record's [owner map](resolution.md#the-owner-map), with the columns and types that the map has.
It lists each map row of each attribute, the layer the map assigns it to, and whether the layer's rows set each dimension (`varies`) or leave it NULL (`broadcast`).
It contains no values.
So a SQL client finds which attributes have rows for a label, and computes an attribute's [flags](record.md#flags), without reading a value:

```sql
SELECT attribute, bool_or(varies.snapshot), bool_or(broadcast.snapshot)
FROM datarecord.name.owner_map
GROUP BY attribute
```

The query returns, per attribute, whether `snapshot` is in the `varies` and the `broadcast` of `record.flags()[attribute]`.

Over a `WorkingRecord`, the view returns each edit once `set`, `add` or `remove` returns.
A map row that the edits stage has the `WorkingRecord`'s own layer id as its `layer_uuid`, the same for every edit until the record closes.
`store.commit` writes the edits as a new layer with an id of its own, so a committed map row has a `layer_uuid` that differs from the staged one.

DuckDB applies a filter on `attribute` or on a dimension's column of the view, such as `WHERE attribute = 'p_nom'` or `WHERE entity = 'gas'`, in its scan of the map.
Over a `WorkingRecord`, a filter on `attribute` restricts the read to the map rows and edits of that attribute.
A filter on a dimension there reaches only some of the scans of the base's map, as the fold reads the whole map to find the rows that an edit covers or [crosses](resolution.md#crossing-rows).

Run `USE datarecord` once to write short names, `name.dimensions.bus` and `registered`.
`USE` holds only for the connection or cursor that runs it, and a client that attached the file under another name runs `USE` with that name.
datarecord never runs `USE` on its own cursors.

`register` raises if `name` exists in the workspace's database, or if another cursor registers it at the same time.
The database is always named, so `name` may be that of another attached database.
`Workspace.unregister(name)` drops the schema, its views and its row in the `registered` table, and releases the record's folds and staging tables that the registration kept, which go once nothing else keeps them.
The views go at once: a statement already running reads on to its end, and any later one, such as a Python relation over the views run again, finds no schema.

### The `registered` table

Every registration is a row of the workspace's `registered` table, `datarecord.main.registered`, which `Workspace.registered()` returns.

| Column     | Contains                                                                 |
| ---------- | ------------------------------------------------------------------------ |
| `name`     | the schema                                                               |
| `revision` | the revision the record reads, or the base revision of a `WorkingRecord` |
| `working`  | whether the record is a `WorkingRecord`                                  |
| `held`     | the [database schemas](store.md#the-database) the workspace keeps for it |
| `created`  | when the record was registered                                           |

!!! info "See also"

    [The storage page](../about/storage.md) argues for parquet, the manifest written last and the views.
    [Read a record](../howto/read-a-record.md) shows a read from SQL.
