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
├── dims/<dim>.parquet          # one axis per dim the layer restates
├── attributes/<attr>.parquet   # one attribute per file, results included
└── resolved/                   # written by materialise()
    ├── dim_owners.parquet
    ├── dead/<dim>.parquet
    └── owner_map.parquet
```

Each file takes the declared name unchanged, so the dim `bus` is `dims/bus.parquet` and the attribute `p_nom` is `attributes/p_nom.parquet`.
A read opens the files that the manifest lists, and no other.

## `claim.json`

`claim.json` lists the id, the parent, the claimed `meta` and when the [store](store.md) handed out the id.
The store writes it create-only and keeps it after the commit, so an id is claimed once.

```json
{ "id": "4731e41b-…", "parent": "9716e2ef-…", "meta": { "run": "a" }, "created": "2026-11-05T17:37:14.912772Z" }
```

## `manifest.json`

A `Manifest` is a layer's `manifest.json`, which lists its id, parent, full schema, files and meta.
The writer writes it last and create-only, and the revision exists from that moment.

| Key      | Contains                                                                                  |
| -------- | ----------------------------------------------------------------------------------------- |
| `id`     | the layer's UUID                                                                          |
| `parent` | the parent's UUID, `null` for a root and for a standalone directory                       |
| `schema` | the full [schema](schema.md) the layer was written under                                  |
| `files`  | `{"dims": [...], "attributes": [...]}`: the declared names of the files the layer wrote   |
| `meta`   | the revision's own `meta`: a flat object of strings, integers, finite floats and booleans |

```json
{"id": "4731e41b-…", "parent": "9716e2ef-…", "schema": {…}, "files": {"dims": ["entity"], "attributes": ["p_nom"]}, "meta": {"run": "a"}}
```

A layer that restates only the entity axis and writes only `p_nom` lists those two files.
`meta` describes one revision, such as a run's id, and a child does not inherit it.
`schema.meta` describes the record, and a child inherits it with the rest of the schema.

## `dims/<dim>.parquet`

An axis file contains every label of one dim at the layer's revision, in axis order, one row per label.
Its first column is the key, named for the dim, and each further column is one [property](data-model.md) of the dim.

```text
dims/entity.parquet       entity | ctype | carrier | sign | type
dims/connection.parquet   connection STRUCT(entity, port) | bus
```

- A property is NULL in a row where no layer wrote a value for that label.
- A [multi-dim](data-model.md)'s file is keyed by one struct column named for it, with a field per coordinate. Its rows are every tuple of the multi-dim at the layer's revision.
- A [dim with `one_of`](data-model.md) has no file. The writer refuses a layer that has one.
- A layer removes a label or tuple by writing the file without it ([removing labels](layers.md#removing-labels)).

## `attributes/<attr>.parquet`

An attribute file contains the long rows of one attribute: its own coordinates, then the columns every row has.

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are the dims that the attribute's [`dims`](schema.md) declare, multi-dims first, each multi-dim as one struct column.

| Attribute                               | Columns                                                         |
| --------------------------------------- | --------------------------------------------------------------- |
| `p_nom` over `entity`                   | `entity, attribute, breakpoint, value`                          |
| `p_max_pu` over `entity` and `snapshot` | `entity, snapshot, attribute, breakpoint, value`                |
| `efficiency` over `connection`          | `connection STRUCT(entity, port), attribute, breakpoint, value` |

A NULL coordinate means every label of its dim, by [the broadcast rule](data-model.md).
`breakpoint` is NULL except on the rows of a curve.
`value` has the attribute's dtype, since each file contains one attribute.

Widening `p_nom` over `scenario` adds a `scenario` column to the files written from then on.
A file written before has no `scenario` column, and a read takes its rows as NULL there.

### Where a value lives

The section of the schema that declares a name sets the file its values go in.

| Declared as             | Is stored in                                 |
| ----------------------- | -------------------------------------------- |
| property of a dim       | a column of `dims/<dim>.parquet`             |
| property of a multi-dim | a column of `dims/<multi-dim>.parquet`       |
| attribute, any `dims`   | the long rows of `attributes/<attr>.parquet` |

A property is named by its subject, so `bus.x` is a column of `dims/bus.parquet`, and an attribute `x` is `attributes/x.parquet`.
An axis file has no attribute values.

## Labels

An axis file has one row per label, so two entities named `north` collide whatever their `ctype`.
A bus named `north` is a label of another dim and collides with neither.

An attribute row has the label alone, so a `p_nom` row has an `entity` column and no `ctype` column.
datarecord never renames a label: a [tool](tools.md) reports a collision in a framework's names.

## `resolved/`

`Revision.materialise()` writes a revision's fold under `resolved/`.
No manifest lists these files, a read opens each by name, and a layer's data stays as it was.

| File                 | Contains                                                                                                  |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| `dim_owners.parquet` | a row `(dim, layer_uuid, has_dead)` per axis: the layer whose `dims/<dim>.parquet` is the revision's axis |
| `dead/<dim>.parquet` | the removed labels of a dim, written only where `has_dead` is true                                        |
| `owner_map.parquet`  | the [owner map](resolution.md#the-owner-map), written last                                                |

A revision is materialised once `owner_map.parquet` exists.
`dim_owners.parquet` points into ancestor layers by UUID rather than copying their files.

## Registered views

`record.register(name)` creates the DuckDB schema `name` in [the workspace's database](store.md#the-database), `datarecord` by default, with a view per dim, property subject and attribute.
A SQL client of that database reads the record through these views.

| View                               | One per                           | Reads as                                   |
| ---------------------------------- | --------------------------------- | ------------------------------------------ |
| `datarecord.name.dims.<dim>`       | dim, a dim with `one_of` included | `record.dims[dim]`                         |
| `datarecord.name.properties.<dim>` | dim that declares properties      | `record.properties[dim]`                   |
| `datarecord.name.attributes.<a>`   | attribute the schema declares     | `record.attributes[a]`, empty with no rows |

The views are the record's relations unsettled: a NULL coordinate means every label, two rows may cover one point and no default applies.
Over a `WorkingRecord`, a view returns each edit once `set`, `add` or `remove` returns.
A view reads the tables of its own database under any name a client attaches it as: a client that attaches the workspace's file as `r` reads `r.name.dims.<dim>`, and never a table of its own with the same name.

Run `USE datarecord` once to write short names, `name.dims.bus` and `registered`.
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
