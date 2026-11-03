<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 34: a record registers its relations as views

| DEP      | 34                                                                                                                                                                       |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Title    | a record registers its relations as views                                                                                                                                |
| Author   | Jonas Hörsch                                                                                                                                                             |
| Status   | Implemented (2026-11-03)                                                                                                                                                 |
| Requires | [DEP 25](dep-0025-a-layer-carries-its-manifest.md), [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md), [DEP 33](dep-0033-the-fold-is-a-fixed-plan.md) |
| Created  | 2026-11-02                                                                                                                                                               |
| Version  | Unreleased                                                                                                                                                               |

## Abstract

`record.register(name)` creates a DuckDB schema `name` in the workspace's database, with one view per dim, per property subject and per declared attribute, in the nested schemas `name.dims`, `name.properties` and `name.attributes`. Any client of that database, a SQL client over a DuckDB server included, reads the record without Python. Over a `WorkingRecord` the views are created once and stay live: every edit shows in them as soon as it commits, since [DEP 33](dep-0033-the-fold-is-a-fixed-plan.md) makes each relation a fixed plan.

## Motivation

A frontend reads a revision and edits it through a server. The server holds a `Workspace`, opens a revision, wraps it in a `WorkingRecord`, and hands the frontend a name it can query over a DuckDB server. The frontend then sends edits, reads them back, and finally asks for a commit.

Each server builds these views itself today. A prototype wraps each relation's `sql_query()` in `CREATE VIEW`, one catalog per revision with three schemas inside. It works for a frozen record and fails for an edited one:

- A view holds the plan its relation had when it was created. Today that plan depends on the staged rows, so an edit that changes the plan never reaches the view. DEP 33 removes this.
- The SQL of a staged relation reads Python objects by name, which are gone when the view is next read.

## Specification

### Layout

| view                  | one per                                                            | columns                                               |
| --------------------- | ------------------------------------------------------------------ | ----------------------------------------------------- |
| `name.dims.<dim>`     | dim, a [`one_of` dim](../model/dims.md#a-dim-with-one_of) included | the axis, as `record.dims[dim]`                       |
| `name.properties.<d>` | dim that declares properties                                       | as `record.properties[d]`                             |
| `name.attributes.<a>` | attribute the schema declares                                      | as `record.attributes[a]`, empty where it has no rows |

Two records under one schema register the same views, since every record lists every declared dim and attribute (DEP 33).

`name` is a top-level schema in the workspace's database, so the views are visible to every cursor and every connection to it. The views are raw: a NULL coordinate means every label ([the broadcast rule](../model/attributes.md#the-broadcast-rule)), two rows can cover one coordinate, and no property default is applied. [Settling](../use/reading.md#settling-rows) is the client's.

### API

- `Record.register(name: str) -> None` creates `name` and its views, and adds a row for `name` to the workspace's `registered` table, in one transaction. It raises if `name` exists.
- `Workspace.unregister(name: str) -> None` drops `name` with its views and its row, in one transaction.
- `Workspace.registered() -> DuckDBPyRelation` reads the `registered` table: `name`, `revision`, `working` and `created`. For a `WorkingRecord`, `revision` is the revision it was opened on. A SQL client reads the table itself.
- `WorkingRecord.rollback()` leaves the views valid, since it refills the staging tables rather than dropping them (DEP 33).
- `store.commit(w)` leaves the views on `w` as they are. A server drops them and registers a `WorkingRecord` on the new revision.

### Consistent states

Every state a reader on another cursor can observe is a state some sequence of whole edits could leave. An edit that writes several statements orders them so that each committed step is such a state, or commits them as one. A removal drops a multi-dim's tuples before the label they name.

### Schema changes

A `WorkingRecord` has no method that changes its schema; the schema changes only at commit. A method that changes it is to refuse a change that alters a column or the fold of an existing view: widening an attribute's `dims`, adding a property, or adding a dim to `partial`. Adding an attribute or a dim is to add its view.

## Rationale

Views created once and never replaced keep a client's queries valid through any edit, with no notification between server and client. The alternative is replacing a view whenever an edit creates something new, which couples the record to its registrations and lets a client's query fail between two edits.

Raw views keep the record's lazy, NULL-means-every-label model. Settled views would have to materialise every broadcast.

## Usage

```python
w = WorkingRecord(store.revision(layer_uuid).record)
w.register("edit_3f2a")
```

```sql
SELECT entity, value FROM edit_3f2a.attributes.p_nom;
SELECT * FROM edit_3f2a.properties.bus WHERE carrier = 'AC';
```

```python
w.set("p_nom", 150.0, entity="wind1")  # the next SELECT reads 150.0
child = store.commit(w)
ws.unregister("edit_3f2a")
WorkingRecord(child.record).register("edit_8c01")
```

## Implementation notes

1. Edits leave only consistent states: one transaction per edit, with the frozen fold tables built before the first edit.
2. `register` and `unregister`. A view is created by the connection's `_create_view` with the path `(name, kind, view)` and the SQL of `_render(rel, literals=True, wrap_text=True)`, the call `DuckDBPyRelation.create_view` makes with a plain name, which keeps alive the Python objects a relation reads. The public method takes one identifier only, so DuckDB is pinned and a test reads a view from another cursor after the relation is collected.
3. `register` builds every relation before it creates any view. Each view it creates changes the catalog, and DuckDB re-binds a relation built before that change, node by node, the next time it derives from it. The pieces every relation of a record shares, its coords, the fold of its frozen layers and the staged layer's base, are built once per record, and a union of relations is `rel.union_by_name` rather than `con.sql` over the captured relations, which binds the whole captured text again. In a prototype on a PyPSA record with 168 views, this took registering from 20–25 s to about 9.5 s; a single transaction around all creates showed no clear gain.

The check: views registered on a fresh `WorkingRecord`, then `set`, `add` and `remove` on attributes, properties, plain dims and multi-dims, and after each step every view equals the relation built afresh. On the 168-view PyPSA record of note 3, `register` takes at most 10 s. A second thread reading after every committed statement observes no inconsistent state.

## Rejected Ideas

- One catalog per revision, attached as `:memory:`, with three schemas inside. DuckDB 2.0 nests schemas, so the workspace's database suffices, and the views read its tables without qualifying the catalog.
- One flat schema with prefixed names, `dim_bus` and `attr_p_nom`. Nested schemas keep each kind's names apart without inventing a convention.
- `CREATE VIEW … AS rel.sql_query()`. The text reads Python objects that are released with the relation, and breaks on a relation with parameters.
- A two-step registration through a helper view in `main`. It works with public API only, and clutters `main` with one view per relation.
- Replacing views on structural edits. See [Rationale](#rationale).
- Marking a registered schema instead of listing it in a table. DuckDB 2.0 does not implement comments on schemas. A comment on every view adds a catalog change per view. A marker view inside each schema is found only by a reserved view name, which a table spares.
- Leaving discovery to the server. A crashed server leaves schemas that no one can tell from the workspace's own.
- `MERGE INTO` in place of a transaction per edit. Every edit also writes the staged owner tables, so none fits in one statement: `set` takes two or three, `rollback` nine. Without a transaction, a reader saw a half-written edit in about half its reads; inside one, `MERGE` was 5–30% slower than `DELETE` and `INSERT` and no more consistent.

## Open Issues

- Schema changes that alter an existing view: refused for now, or replacing the views they reach.
- Settling in the client: [`settle`](../use/reading.md#settling-rows) ported to the frontend, and a page on the raw format for SQL clients.
- Cleanup: who unregisters an abandoned schema, and by what rule. The `registered` table lists them; DuckDB 2.0 has no comments on schemas to mark them by.
- A public `create_view` taking a qualified name, to request upstream.
- DuckDB 2.0 lets a reader on another cursor see part of a committed transaction once it writes about 300,000 rows or more, in 0.1–1% of reads. No edit can rule it out until DuckDB does, and it is to be reported upstream.
- The cold fold of a deep revision is paid by `register` or the first read of a view, once per lineage in a workspace, and stays linear in depth until a revision's map is materialised.
- The fold of every revision a server opens is held for the life of its connection: who drops it, and when.
