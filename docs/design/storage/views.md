<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Registered views

`record.register(name)` gives a record's relations names in the workspace's database.
Any client of that database reads the record through them in SQL, a client over a DuckDB server included, with no Python on its side.
Over a `WorkingRecord` the views are live: every edit shows in them once it commits.

## The layout

`name` is a schema in the workspace's own database, with three nested schemas and a view per relation:

| view                  | one per                                                                 | reads as                                           |
| --------------------- | ----------------------------------------------------------------------- | -------------------------------------------------- |
| `name.dims.<dim>`     | dim, a [dim with `one_of`](../model/dims.md#a-dim-with-one_of) included | `record.dims[dim]`                                 |
| `name.properties.<d>` | dim that declares properties                                            | `record.properties[d]`                             |
| `name.attributes.<a>` | attribute the schema declares                                           | `record.attributes[a]`, empty where it has no rows |

Every record lists every dim and attribute its schema declares, so two records under one schema register the same views.
`name` is a top-level schema, so its views are visible to every cursor and every connection to the database, and they read the workspace's tables without naming a catalog.
Nested schemas keep each kind's names apart without a naming convention such as `dim_` or `attr_`.

The views are raw, as the relations are.
A NULL coordinate means every label ([the broadcast rule](../model/attributes.md#the-broadcast-rule)), two rows can cover one coordinate, and no property default is applied.
[Settling](../use/reading.md#settling-rows) is the client's.
Settled views would have to materialise every broadcast, and raw ones keep the record's lazy model.

## The `registered` table

Every registration is a row of the workspace's `registered` table:

| column     | holds                                                                           |
| ---------- | ------------------------------------------------------------------------------- |
| `name`     | the schema                                                                      |
| `revision` | the node the record reads; for a `WorkingRecord`, the revision it was opened on |
| `working`  | whether the record is a `WorkingRecord`                                         |
| `created`  | when it was registered                                                          |

`Workspace.registered()` reads it, and a SQL client reads the table itself.
A table tells a registration from any other schema in the database, so a server that crashed leaves names that can be found again.
DuckDB has no comments on schemas to mark them by, and a marker view in each schema would be found only by a reserved name.

`register` creates the schemas, the views and the row in one transaction, and raises if `name` exists.
`Workspace.unregister(name)` drops the schema with its views and its row, in one transaction.
DuckDB nests no transaction, so both refuse a cursor that already holds one open, as [an edit](../use/editing.md#edits) does.

## Live over a `WorkingRecord`

A view keeps the plan its relation had when the view was created.
The plan of a relation of a `WorkingRecord` [never depends on the staged rows](../use/editing.md#reading-with-pending-edits).
It may depend on the rows of the frozen layers, which do not change while the `WorkingRecord` lives.
So a view created once reads every later edit, and is never replaced.

That keeps a client's query valid through any edit, with no message between server and client.
Replacing a view whenever an edit makes something new would tie the record to its registrations, and a client's query could fail between two edits.

`rollback()` empties the [staging tables](../use/editing.md#staging) rather than dropping them, so the views stay valid and read the base again.
`store.commit(w)` leaves the views on `w` as they are, still reading `w`.
A server that goes on editing drops them and registers a `WorkingRecord` on the new revision.

## Consistent states

A reader on another cursor sees only states that some sequence of whole edits leaves.
Each edit runs in [one transaction](../use/editing.md#edits), so the reader sees it whole or not at all.
Within it the statements are ordered too: `remove` drops a multi-dim's tuples before the label they name.
The tables the frozen layers fold to are built when the `WorkingRecord` is opened, so no edit's transaction creates one.

## Building the views

A view's SQL is its relation rendered with its literals inline, so it reads no Python object by name and outlives the relation that made it.
`DuckDBPyRelation.sql_query()` gives SQL that reads such objects, which are gone by the next read, and breaks on a relation with parameters.
The rendering is DuckDB's own, the one `DuckDBPyRelation.create_view` makes, but that method takes a single identifier and no nested path.
So `register` calls the private `_render` and `_create_view` with the path `(name, kind, view)`, DuckDB is pinned to an exact version, and a test reads a view from another thread's cursor after its relation is collected.

`register` builds every relation before it creates any view.
Each view changes the catalog, and DuckDB re-binds a relation built before that change, node by node, the next time something derives from it.
Building first keeps a record of a hundred and more views to seconds.
