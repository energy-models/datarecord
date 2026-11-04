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

The tables a workspace creates move out of `main` into three schemas: `_fold` for a revision's fold, `_staged` for a `WorkingRecord`'s staging tables, `_scratch` for a call's intermediates. A revision's fold and a record's staging tables each get a schema of their own inside, which is held by its holders: the records reading it and the registrations whose views read it. The workspace drops a schema when its last holder releases it. A record releases its holds on `close()`, or when it is garbage collected.

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

| schema            | holds                                                                       | created by                            |
| ----------------- | --------------------------------------------------------------------------- | ------------------------------------- |
| `main`            | `schemas`, `revisions`, `registered`                                        | the workspace                         |
| `_fold.<rid>`     | `owner_map`, `dims_<dim>`, `dead_<name>` and `dim_owners` of revision `rid` | the first cursor to fold `rid`        |
| `_staged.<layer>` | a `WorkingRecord`'s staging tables, and an edit's snapshot of one           | `WorkingRecord(...)`                  |
| `_scratch`        | a fold's checkpoints, the PyPSA exporter's joins                            | the call using them, which drops them |
| `<name>`          | the views of a registration (DEP 34)                                        | `register(name)`                      |

`_fold`, `_staged` and `_scratch` are created with the workspace, so `register` refuses them as names it finds taken. The id in a nested schema's name keys its tables, which no longer carry it.

### Holders

| holder            | holds                                                  | releases on                      |
| ----------------- | ------------------------------------------------------ | -------------------------------- |
| a `Record`        | `_fold.<rid>` of the revision its relations read       | `close()`, or garbage collection |
| a `WorkingRecord` | as a `Record` over its base, and its `_staged.<layer>` | `close()`, or garbage collection |
| a registration    | every schema the registering record holds              | `Workspace.unregister(name)`     |

A schema is held from when it is created or first found for a holder. When its last holder releases it, the workspace drops it with every table in it. A registration takes its hold inside the transaction that creates its views, so no `unregister` sees it unheld.

A workspace opened on a file lists every registration in `registered` as a holder of nothing: unregistering one drops its views.

### API

- `Record.close() -> None` releases the record's holds. A second call does nothing.
- `Record` is a context manager, closing on exit.
- A record is closed when it is garbage collected, if it was not before.
- A closed record refuses every read and edit with an error naming it closed.
- `WorkingRecord(...)` is refused while its cursor holds an open transaction: the workspace's own cursor could not see the staging tables to drop them.

### Lifetime of a relation

A relation taken from a record, `record.attributes["p_nom"]` among them, is valid while the record is open. Reading it after the record is closed or collected fails with DuckDB's missing-table error. A registered view stays valid until it is unregistered, whatever happens to the record.

## Rationale

`close()` frees a workspace at a point the caller chooses, as a server answering a request needs. The finaliser covers the record a caller forgets, which is what leaks today. One `weakref.finalize` serves both: `close()` calls it, and it runs at most once.

A schema per revision and per record makes it the unit of holding: a `WorkingRecord`'s about 310 staging tables are dropped by one `DROP SCHEMA … CASCADE`, and the holds name a handful of schemas, not hundreds of tables. Nesting them keeps the top level to `main`, the three schemas and the registrations, which is what a SQL client browsing the database sees.

A registration holds the schemas of its record rather than the tables its rendered SQL names. The two sets cover the same tables, and taking the record's spares parsing the views.

The holds live in a Python mapping on the `Workspace`, behind one lock. A record cannot outlive the process holding the mapping, and a registration, the one holder that can, is a row of `registered`, from which a reopened workspace lists it.

A finaliser can run in any thread at any allocation, possibly while that thread's cursor holds a transaction. The workspace therefore drops schemas on a cursor of its own, under the lock. The same lock orders a drop against a lookup on another thread, which would otherwise find a schema between its last release and its drop.

It costs a contract: a relation lives no longer than its record. `Record.at(uri).attributes["p_nom"]`, with no name kept for the record, can fail at the next garbage collection. The record must be kept, or the relation fetched while it is held.

## Usage

```python
with Record.at(uri) as r:
    df = r.attributes["p_nom"].df()
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
3. The layout: `_fold`, `_staged` and `_scratch`, every table moved into them, and the holds taken on schemas.
4. `Record`: `_fold.<rid>` of its revision, taken where `_shared` and `_held` create or find it. A `WorkingRecord` takes its base's.

The checks: a workspace that opens and closes records, edits and registrations in a loop ends with only the tables of `main` and the three schemas, empty. A view registered on a closed `WorkingRecord` reads until it is unregistered. A read after `close()` raises the closed error.

## Rejected Ideas

- Only a finaliser. The caller cannot free a workspace at a point it chooses, and a record caught in a reference cycle waits for the cycle collector.
- Only `close()`. Every forgotten record leaks, as all of them do today.
- Holds kept in a DuckDB table. The Python holders would not survive a reopen, and the one that does, a registration, is already a row of `registered`.
- A top-level schema per revision and per record. The top level fills with ids as `main` does today.
- A table prefix per kind in `main`, as today. A record's tables can then be dropped only one by one, and a SQL client sees them all beside the bookkeeping.
- Evicting shared tables by age or size. It breaks a live record's relations without telling it.
- Dropping a revision's shared tables once it is materialised. A live record's relations still name them.
- Relying on DuckDB to refuse a drop. It tracks no view's dependencies.

## Open Issues

- Who unregisters an abandoned registration, and by what rule. Unregistering now frees its tables, so the rule decides how long they stay.
- Whether a record's planned relations form a reference cycle with it, which would delay its finaliser to the cycle collector.
- A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which its registrations may read and no record holds. Sweeping those no registration reads needs a registration to list its schemas in `registered`.
