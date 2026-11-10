<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 35: the workspace keeps the tables a record reads

| DEP      | 35                                                                                                |
| -------- | ------------------------------------------------------------------------------------------------- |
| Title    | the workspace keeps the tables a record reads                                                     |
| Author   | Jonas Hörsch                                                                                      |
| Status   | Draft (2026-11-04)                                                                                |
| Requires | [DEP 33](dep-0033-the-fold-is-a-fixed-plan.md), [DEP 34](dep-0034-a-record-registers-as-views.md) |
| Created  | 2026-11-04                                                                                        |

## Abstract

The tables a workspace keeps move out of `main` into two schemas. `_fold` contains each revision's fold, and `_staged` each `WorkingRecord`'s staging tables. A call's intermediates become temporary tables of its cursor. A record or a registration that reads a nested schema takes a hold on it. The workspace drops the schema when its last hold is released, at the record's `close()` or the registration's `unregister`. A record collected unclosed releases its holds too. Every relation a record hands out keeps the record alive, so its tables live while the record or any relation taken from it lives.

## Motivation

Before this DEP, a long-lived workspace, such as a server that opens a revision per request, grows without bound. Two kinds of table are never dropped:

- The fold tables of each revision, `owner_map_<rid>`, `dims_<dim>_<rid>`, `dead_<name>_<rid>` and `dim_owners_<rid>`. The first cursor to read an unmaterialised revision creates them. Every record and `WorkingRecord` of the revision, a descendant's cold fold and their registered views read them.
- The staging tables `staged_*_<layer>` of each `WorkingRecord`, about 310 for a PyPSA record, which it and the views registered on it read.

Every other table lives within one call, which drops it. DuckDB drops a table that a view reads without complaint, and the view fails only when next queried. A relation has no reference to the record it came from.

## Specification

### Layout

| Schema            | Contains                                                                        | Created by                     |
| ----------------- | ------------------------------------------------------------------------------- | ------------------------------ |
| `main`            | `schemas`, `revisions`, `registered`                                            | the workspace                  |
| `_fold.<rid>`     | `owner_map`, `dims_<dim>`, `dead_<name>` and `dim_owners` of revision `rid`     | the first cursor to fold `rid` |
| `_staged.<layer>` | a `WorkingRecord`'s staging tables                                              | `WorkingRecord(...)`           |
| `<name>`          | the views of a registration ([DEP 34](dep-0034-a-record-registers-as-views.md)) | `register(name)`               |

The workspace creates `_fold` and `_staged` with itself, so `register` refuses them as taken names. The id in a nested schema's name keys its tables, which no longer carry it.

A table that lives within one call, a fold's checkpoints, an edit's snapshot or the PyPSA exporter's joins, is a `TEMP` table of the calling cursor, in no schema of the database. The call creates, reads and drops it on that cursor.

### Holders

| Holder            | Takes a hold on                                        | Releases on                      |
| ----------------- | ------------------------------------------------------ | -------------------------------- |
| a `Record`        | `_fold.<id>` of each layer it folds                    | `close()`, or garbage collection |
| a `WorkingRecord` | as a `Record` over its base, and its `_staged.<layer>` | `close()`, or garbage collection |
| a registration    | every schema the registering record has a hold on      | `Workspace.unregister(name)`     |

A record takes its holds at construction, whether the schemas exist yet or not. A `Record` over `rid` folds the layers down to the deepest materialised ancestor, and reads `_fold.<rid>` and any ancestor's `_fold.<id>` it has a hold on. When the last hold on a schema is released, the workspace drops the schema with every table in it. A registration takes its hold inside the transaction that creates its views, so no `unregister` finds the schema without a hold.

A registration's row in `registered` lists its schemas in a `held` column. A workspace opened on a file takes those holds again for it, so its views still read, and unregistering one drops them.

### API

- `Record.close() -> None` releases the record's holds. A second call does nothing.
- `Record` is a context manager, which closes it on exit.
- Garbage collection closes a record that is still open.
- Every relation a record hands out, through `dims`, `properties` and `attributes`, keeps the record alive while it lives: a `weakref.finalize` on the relation holds the record as its argument.
- A relation datarecord builds by SQL over another relation, a Python variable its SQL scans, keeps that relation alive the same way.
- A closed record refuses every read and edit with an error that states it is closed.
- `Revision.record` builds a new record once the one it caches is closed.
- `WorkingRecord(...)` is refused while its cursor has an open transaction, because the workspace's own cursor could not see the staging tables to drop them.

### Lifetime of a relation

A relation taken from a record, such as `record.attributes["p_nom"]`, keeps the record, and so its tables, alive. A relation derived from it by its methods, `filter`, `join` or `aggregate`, keeps its inputs, so it keeps the record too. A record's tables therefore live while the record or any relation taken from it lives, unless it is closed. Once the record is closed and no other hold covers its tables, reading the relation fails with DuckDB's missing-table error.

SQL run over such a relation by a replacement scan, `con.sql("FROM rel")`, binds it at once and keeps neither the relation nor the record. Wherever datarecord's own code does so, in the writer or the PyPSA exporter, it anchors the result to the relation it reads, so the chain from the result back to the record holds. Only a caller's own `con.sql` over a record's relation breaks it, and the caller mends it the same way: by keeping the relation it reads, or the record, or a `with` block, alive until it has fetched. A registered view stays valid until it is unregistered, whatever happens to the record.

## Rationale

`close()` frees a workspace when the caller chooses, as a server answering a request needs. The finaliser covers a forgotten record. One `weakref.finalize` serves both: `close()` detaches it and releases, so the release runs at most once.

In `store.revision(rid).record.attributes["p_nom"].df()`, nothing keeps a name for the record, so with a bare finaliser the collector may take it before `.df()` runs. The release would drop the fold that `.df()` reads, and the read would fail at random with the collector's timing. So the relation keeps the record. DuckDB's relations are slotted objects that take no attribute, but they take a weak reference, and a `weakref.finalize` on one holds its arguments until the relation is collected. Passing the record as an argument to a finaliser that does nothing keeps it exactly that long. A relation derived from another keeps its inputs, so the chain `attributes["p_nom"].filter(...).aggregate(...)` keeps the record from its first link. A finaliser costs about 0.6 µs per relation handed out.

A first draft had a `Record` collected unclosed pass its folds to the workspace, which kept them for its life. It pinned every revision ever read by an unclosed record, and `store.revision(rid).record.register(name)` never freed the fold on `unregister`, since the collected record had passed its hold to the workspace. Tying the record's life to its relations makes a collected `Record` release as a `WorkingRecord` does. A one-liner read then folds its revision again each time, since nothing keeps the fold between reads, so a process that reads one revision often keeps one record open instead.

Only what a record hands out is anchored. A relation the record caches for itself would keep it alive for good.

A `Record` takes a hold on every layer's fold it reads. Its relations read an ancestor's fold where the workspace has one ([the nearest fold at hand](../../reference/resolution.md#materialised-revisions)), and the workspace would otherwise drop that fold with the ancestor's last record.

A schema per revision and per record is the unit of a hold. One `DROP SCHEMA … CASCADE` drops a `WorkingRecord`'s staging tables, and the holds list a handful of schemas, not hundreds of tables. Nesting keeps the top level that a SQL client sees to `main`, the two schemas and the registrations.

A call's intermediate table is created, read and dropped by one call on one cursor, so no other cursor needs to see it and no holder needs to keep it: a `TEMP` table, visible to that cursor alone, fits. It leaves nothing behind in a file database when the process dies mid-call, where a table of a shared schema would stay until something swept it. A transaction that writes to the workspace's database can still create one, since DuckDB does not count the temporary catalog as a second database written. Past DuckDB's `memory_limit` it spills to the `temp_directory`, `.tmp` by default, as a table of an in-memory database does. The tables other cursors read, the fold's and the staging tables, stay in the database.

A registration takes holds on the schemas of its record rather than on the tables its SQL reads. The two sets cover the same tables, and by taking the record's set, datarecord need not parse the views.

The holds are in a Python mapping on the `Workspace`, behind one lock. A record cannot outlive the process, and a registration, the one holder that can, is a row of `registered`, which a reopened workspace lists.

A finaliser can run in any thread at any allocation, even inside a transaction on that thread's cursor. The workspace therefore drops schemas on a cursor of its own, under the lock. The lock also stops a lookup on another thread finding a schema between its last release and its drop.

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
w.close()  # the views still read: the workspace keeps their tables for the registration
ws.unregister("edit_3f2a")  # the staging tables are dropped
```

## Implementation notes

1. The holder registry on `Workspace`, under one lock.
2. `WorkingRecord`: `close()`, the finaliser, the refusal after close, and the registration's holds.
3. The layout, with every shared table moved into `_fold` and `_staged`, and a call's intermediates made `TEMP` tables of its cursor.
4. `Record`: its holds, `close()`, the finaliser, the refusal after close, and `Revision.record` replacing a closed record. A `WorkingRecord` reads a `Record` base through its own open copy, so closing the caller's base leaves its edits working.
5. Anchoring: each relation of `dims`, `properties` and `attributes` keeps its record through a `weakref.finalize`, and a collected `Record` releases.

Once built, a workspace that opens and closes records, edits and registrations in a loop ends with only `main`'s tables and two empty schemas, and no temporary table on its cursor. A view registered on a closed record reads until it is unregistered. A read after `close()` raises the closed error. A relation, or one derived from it, of a record collected unclosed reads after a collection, and the record's fold and staging tables go once the relation does. A registration of a collected record frees its fold on `unregister`.

## Rejected ideas

- Only a finaliser: the caller cannot choose when to free a workspace, and a record in a reference cycle waits for the cycle collector.
- A `Record`'s finaliser releasing with nothing keeping it: a chained read would fail whenever the collector runs between taking the relation and fetching it.
- A `Record` collected unclosed passing its folds to the workspace: it pins every revision an unclosed record read for the workspace's life, and a registration of such a record never frees its fold.
- Wrapping each relation in a proxy holding the record: a proxy is no `DuckDBPyRelation`, which every consumer takes, and a relation derived from it would drop the proxy.
- Only `close()`: every forgotten record leaks.
- Holds kept in a DuckDB table: the Python holders do not survive a reopen, and a registration, which does, is a row of `registered`.
- A `_scratch` schema of the database for a call's intermediates: each call drops its own, but a process that dies mid-call leaves them in a file database, and they are named in a catalog every cursor sees for tables only one reads.
- A top-level schema per revision and per record: the top level fills with ids.
- A table prefix per kind in `main`: the workspace drops a record's tables one by one, and a SQL client sees them all.
- Evicting shared tables by age or size: it breaks a live record's relations silently.
- Dropping a revision's shared tables once it is materialised: a live record's relations still read them.
- Relying on DuckDB to refuse a drop: it tracks no view's dependencies.

## Open issues

- Who unregisters a forgotten registration, and by what rule. Unregistering frees its tables, so the rule sets how long they stay.
- A record's cached `dims`, `properties` and `attributes` name the record, a reference cycle, so a forgotten record releases at the cycle collector's pace, not at its last reference.
- A caller's own SQL over a record's relation keeps nothing. DuckDB binds such SQL at once and offers no hook to keep a Python object with the result, and `anchored`, which datarecord's own code uses for this, is not public.
- A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which its registrations may read and no record has a hold on. Nothing sweeps those that no `held` column lists.
