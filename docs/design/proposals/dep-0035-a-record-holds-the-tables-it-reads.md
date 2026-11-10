<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 35: a record holds the tables it reads

| DEP      | 35                                                                                                |
| -------- | ------------------------------------------------------------------------------------------------- |
| Title    | a record holds the tables it reads                                                                |
| Author   | Jonas Hörsch                                                                                      |
| Status   | Draft (2026-11-04)                                                                                |
| Requires | [DEP 33](dep-0033-the-fold-is-a-fixed-plan.md), [DEP 34](dep-0034-a-record-registers-as-views.md) |
| Created  | 2026-11-04                                                                                        |

## Abstract

The tables a workspace keeps move out of `main` into two schemas: `_fold` for a revision's fold, `_staged` for a `WorkingRecord`'s staging tables. A call's intermediates become temporary tables of its cursor. A revision's fold and a record's staging tables each get a schema of their own inside, which is held by its holders: the records reading it and the registrations whose views read it. The workspace drops a schema when its last holder releases it. A record releases its holds on `close()`, or once it is collected unclosed. Every relation a record hands out keeps the record alive, so its tables live while the record or any relation taken from it lives.

## Motivation

A long-lived workspace, such as a server that opens a revision per request, grows without bound today. Two kinds of table are created and never dropped:

| tables                                                                         | created by                                                           | read by                                                                                                                               |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `owner_map_<rid>`, `dims_<dim>_<rid>`, `dead_<name>_<rid>`, `dim_owners_<rid>` | the first cursor to read revision `rid` while it is not materialised | every record of `rid` on any thread, every `WorkingRecord` over `rid`, the cold fold of a descendant, views registered on any of them |
| `staged_*_<layer>`, about 310 for a PyPSA record                               | `WorkingRecord(...)`                                                 | that `WorkingRecord`, views registered on it                                                                                          |

Every other table, a fold's checkpoints, an edit's snapshot, the PyPSA exporter's joins, lives within one call and is dropped by it.

DuckDB does not help. It drops a table a view reads without complaint, and the view fails only when next queried. A relation names its tables and keeps no reference to the record it came from.

## Specification

### Layout

| schema            | holds                                                                       | created by                     |
| ----------------- | --------------------------------------------------------------------------- | ------------------------------ |
| `main`            | `schemas`, `revisions`, `registered`                                        | the workspace                  |
| `_fold.<rid>`     | `owner_map`, `dims_<dim>`, `dead_<name>` and `dim_owners` of revision `rid` | the first cursor to fold `rid` |
| `_staged.<layer>` | a `WorkingRecord`'s staging tables                                          | `WorkingRecord(...)`           |
| `<name>`          | the views of a registration (DEP 34)                                        | `register(name)`               |

`_fold` and `_staged` are created with the workspace, so `register` refuses them as names it finds taken. The id in a nested schema's name keys its tables, which no longer carry it.

A table that lives within one call, a fold's checkpoints, an edit's snapshot or the PyPSA exporter's joins, is a `TEMP` table of the calling cursor, in no schema of the database. The call creates, reads and drops it on that cursor.

### Holders

| holder            | holds                                                  | releases on                      |
| ----------------- | ------------------------------------------------------ | -------------------------------- |
| a `Record`        | `_fold.<id>` of each layer it folds                    | `close()`, or garbage collection |
| a `WorkingRecord` | as a `Record` over its base, and its `_staged.<layer>` | `close()`, or garbage collection |
| a registration    | every schema the registering record holds              | `Workspace.unregister(name)`     |

A record holds its schemas from construction, created or not. A `Record` over revision `rid` folds `rid`'s layer and the ancestors down to the deepest materialised one, and reads `_fold.<rid>` and, where it holds them, an ancestor's `_fold.<id>`. When the last holder of a schema releases it, the workspace drops it with every table in it. A registration takes its hold inside the transaction that creates its views, so no `unregister` sees it unheld.

A registration's row in `registered` lists the schemas it holds in a `held` column, and a workspace opened on a file holds them again for it: its views read on, and unregistering one drops them.

### API

- `Record.close() -> None` releases the record's holds. A second call does nothing.
- `Record` is a context manager, closing on exit.
- A record is closed when it is garbage collected, if it was not before.
- Every relation a record hands out, through `dims`, `properties` and `attributes`, keeps the record alive while it lives: a `weakref.finalize` on the relation holds the record as its argument.
- A relation datarecord builds by SQL over another relation, a Python variable its SQL scans, keeps that relation alive the same way.
- A closed record refuses every read and edit with an error naming it closed.
- `Revision.record` builds a new record once the one it caches is closed.
- `WorkingRecord(...)` is refused while its cursor holds an open transaction: the workspace's own cursor could not see the staging tables to drop them.

### Lifetime of a relation

A relation taken from a record, `record.attributes["p_nom"]` among them, keeps the record, and so its tables, alive. A relation derived from it by its methods, `filter`, `join` or `aggregate`, keeps its inputs, so keeps the record too. A record's tables therefore live while the record or any relation taken from it lives, unless it is closed: reading a relation after its record is closed fails with DuckDB's missing-table error once no other holder keeps its tables.

SQL run over such a relation by a replacement scan, `con.sql("FROM rel")`, binds it at once and keeps neither the relation nor the record. Wherever datarecord's own code does so, in the writer or the PyPSA exporter, it anchors the result to the relation it reads, so the chain from the result back to the record holds. Only a caller's own `con.sql` over a record's relation breaks it, and the caller mends it the same way: by keeping the relation it reads, or the record, or a `with` block, alive until it has fetched. A registered view stays valid until it is unregistered, whatever happens to the record.

## Rationale

`close()` frees a workspace at a point the caller chooses, as a server answering a request needs. The finaliser covers the record a caller forgets, which is what leaks today. One `weakref.finalize` serves both: `close()` detaches it and releases, so the release runs at most once.

`store.revision(rid).record.attributes["p_nom"].df()` keeps no name for the record, which a bare finaliser would collect before `.df()` runs: the release would drop the fold `.df()` reads, and the read would fail at random with the collector's timing. So the relation keeps the record. DuckDB's relations are slotted objects that take no attribute, but they take a weak reference, and a `weakref.finalize` on one holds its arguments until the relation is collected; passing the record as an argument to a finaliser that does nothing keeps it exactly that long. A relation derived from another keeps its inputs, so the chain `attributes["p_nom"].filter(...).aggregate(...)` keeps the record from its first link. A finaliser costs about 0.6 µs per relation handed out.

A first draft had a `Record` collected unclosed abandon its folds to the workspace, which kept them for its life. It pinned every revision ever read by an unclosed record, and `store.revision(rid).record.register(name)` never freed the fold on `unregister`, since the collected record had passed its hold to the workspace. Tying the record's life to its relations makes a collected `Record` release as a `WorkingRecord` does. A one-liner read then folds its revision again each time, since nothing keeps the fold between reads; a process reading one revision often keeps one record open instead.

Only what a record hands out is anchored. A relation the record caches for itself would keep it alive for good.

A `Record` holds the fold of each layer it folds rather than of its revision only. Its relations read an ancestor's fold where the workspace has one ([the nearest fold at hand](../layers/resolution.md#a-fold-starts-from-the-nearest-fold-at-hand)), and that fold would otherwise drop with the ancestor's last record.

A schema per revision and per record makes it the unit of holding: a `WorkingRecord`'s about 310 staging tables are dropped by one `DROP SCHEMA … CASCADE`, and the holds name a handful of schemas, not hundreds of tables. Nesting them keeps the top level to `main`, the two schemas and the registrations, which is what a SQL client browsing the database sees.

A call's intermediate table is created, read and dropped by one call on one cursor, so no other cursor needs to see it and no holder needs to keep it: a `TEMP` table, visible to that cursor alone, fits. It leaves nothing behind in a file database when the process dies mid-call, where a table of a shared schema would stay until something swept it. A transaction that writes to the workspace's database can still create one, since DuckDB does not count the temporary catalog as a second database written. Past DuckDB's `memory_limit` it spills to the `temp_directory`, `.tmp` by default, as a table of an in-memory database does. The tables other cursors read, the fold's and the staging tables, stay in the database.

A registration holds the schemas of its record rather than the tables its rendered SQL names. The two sets cover the same tables, and taking the record's spares parsing the views.

The holds live in a Python mapping on the `Workspace`, behind one lock. A record cannot outlive the process holding the mapping, and a registration, the one holder that can, is a row of `registered`, from which a reopened workspace lists it.

A finaliser can run in any thread at any allocation, possibly while that thread's cursor holds a transaction. The workspace therefore drops schemas on a cursor of its own, under the lock. The same lock orders a drop against a lookup on another thread, which would otherwise find a schema between its last release and its drop.

It costs a contract: a relation lives no longer than its closed record, and a caller's own SQL over it keeps nothing. The relation that SQL reads, or the record, must be kept, or the result fetched while it is held.

## Usage

```python
with Record.at(uri) as r:
    df = r.attributes["p_nom"].df()
```

```python
df = (
    store.revision(rid).record.attributes["p_nom"].filter("value > 0").df()
)  # the relation keeps the record

rel = store.revision(rid).record.attributes["p_nom"]
ws.con.sql("FROM rel WHERE value > 0").df()  # `rel` keeps the record while the SQL runs
```

```python
store.revision(rid).record.register("plain")
ws.unregister("plain")  # the fold goes once the collected record released it too
```

```python
w = WorkingRecord(store.revision(layer_uuid).record)
w.register("edit_3f2a")
w.close()  # the views still read; their tables are held by the registration
ws.unregister("edit_3f2a")  # the staging tables are dropped
```

## Implementation notes

1. The holder registry on `Workspace`: hold, release, and drop on the last release, on the workspace's own cursor under one lock.
2. `WorkingRecord`: its staging tables, `close()`, the finaliser, the refusal after close, and the registration's holds.
3. The layout: `_fold` and `_staged`, every shared table moved into them, the holds taken on schemas, and a call's intermediates made `TEMP` tables of its cursor.
4. `Record`: `_fold.<id>` of each layer it folds, held from construction; `close()`, the finaliser, the refusal after close, and `Revision.record` replacing a closed record. A `WorkingRecord` takes its base's, and reads a `Record` base through a copy open while it is, so closing the caller's base leaves its edits working.
5. Anchoring: each relation of `dims`, `properties` and `attributes` keeps its record through a `weakref.finalize`, and a collected `Record` releases.

The checks: a workspace that opens and closes records, edits and registrations in a loop ends with only the tables of `main` and the two schemas, empty, and no temporary table on its cursor. A view registered on a closed record reads until it is unregistered. A read after `close()` raises the closed error. A relation, or one derived from it, of a record collected unclosed reads after a collection, and the record's fold and staging tables go once the relation does. A registration of a collected record frees its fold on `unregister`.

## Rejected Ideas

- Only a finaliser. The caller cannot free a workspace at a point it chooses, and a record caught in a reference cycle waits for the cycle collector.
- A `Record`'s finaliser releasing with nothing keeping it. A chained read would fail whenever the collector runs between taking the relation and fetching it.
- A `Record` collected unclosed abandoning its folds to the workspace. It pins every revision an unclosed record read for the workspace's life, and a registration of such a record never frees its fold.
- Wrapping each relation in a proxy holding the record. A proxy is no `DuckDBPyRelation`, which every consumer takes, and a relation derived from it would drop the proxy.
- Only `close()`. Every forgotten record leaks, as all of them do today.
- Holds kept in a DuckDB table. The Python holders would not survive a reopen, and the one that does, a registration, is already a row of `registered`.
- A `_scratch` schema of the database for a call's intermediates. Each call drops its own, but a process that dies mid-call leaves them in a file database, and they are named in a catalog every cursor sees for tables only one reads.
- A top-level schema per revision and per record. The top level fills with ids as `main` does today.
- A table prefix per kind in `main`, as today. A record's tables can then be dropped only one by one, and a SQL client sees them all beside the bookkeeping.
- Evicting shared tables by age or size. It breaks a live record's relations without telling it.
- Dropping a revision's shared tables once it is materialised. A live record's relations still name them.
- Relying on DuckDB to refuse a drop. It tracks no view's dependencies.

## Open Issues

- Who unregisters a forgotten registration, and by what rule. Unregistering now frees its tables, so the rule decides how long they stay.
- A record's cached `dims`, `properties` and `attributes` name the record, a reference cycle, so a forgotten record releases at the cycle collector's pace, not at its last reference.
- A caller's own SQL over a record's relation keeps nothing. DuckDB binds such SQL at once and offers no hook to keep a Python object with the result, and `anchored`, which datarecord's own code uses for this, is not public.
- A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which its registrations may read and no record holds. Those no `held` column lists could be swept, and nothing does yet.
