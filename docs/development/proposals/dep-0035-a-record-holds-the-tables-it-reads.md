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

The tables a workspace creates move out of `main` into three schemas. `_fold` contains each revision's fold, `_staged` each `WorkingRecord`'s staging tables, and `_scratch` a call's intermediates. A record or a registration that reads a nested schema takes a hold on it. The workspace drops the schema when its last hold is released, at the record's `close()` or the registration's `unregister`. A `WorkingRecord` collected unclosed releases its holds, and a `Record` collected unclosed passes them to the workspace.

## Motivation

Before this DEP, a long-lived workspace, such as a server that opens a revision per request, grows without bound. Two kinds of table are never dropped:

- The fold tables of each revision, `owner_map_<rid>`, `dims_<dim>_<rid>`, `dead_<name>_<rid>` and `dim_owners_<rid>`. The first cursor to read an unmaterialised revision creates them. Every record and `WorkingRecord` of the revision, a descendant's cold fold and their registered views read them.
- The staging tables `staged_*_<layer>` of each `WorkingRecord`, about 310 for a PyPSA record, which it and the views registered on it read.

Every other table lives within one call, which drops it. DuckDB drops a table that a view reads without complaint, and the view fails only when next queried. A relation has no reference to the record it came from.

## Specification

### Layout

| Schema            | Contains                                                                        | Created by                            |
| ----------------- | ------------------------------------------------------------------------------- | ------------------------------------- |
| `main`            | `schemas`, `revisions`, `registered`                                            | the workspace                         |
| `_fold.<rid>`     | `owner_map`, `dims_<dim>`, `dead_<name>` and `dim_owners` of revision `rid`     | the first cursor to fold `rid`        |
| `_staged.<layer>` | a `WorkingRecord`'s staging tables                                              | `WorkingRecord(...)`                  |
| `_scratch`        | a fold's checkpoints, an edit's snapshot, the PyPSA exporter's joins            | the call using them, which drops them |
| `<name>`          | the views of a registration ([DEP 34](dep-0034-a-record-registers-as-views.md)) | `register(name)`                      |

The workspace creates `_fold`, `_staged` and `_scratch` with itself, so `register` refuses them as taken names. The id in a nested schema's name keys its tables, which no longer carry it.

### Holders

| Holder            | Takes a hold on                                        | Releases on                      |
| ----------------- | ------------------------------------------------------ | -------------------------------- |
| a `Record`        | `_fold.<id>` of each layer it folds                    | `close()`                        |
| a `WorkingRecord` | as a `Record` over its base, and its `_staged.<layer>` | `close()`, or garbage collection |
| a registration    | every schema the registering record has a hold on      | `Workspace.unregister(name)`     |
| the workspace     | every schema of a `Record` collected unclosed          | never                            |

A record takes its holds at construction, whether the schemas exist yet or not. A `Record` over `rid` folds the layers down to the deepest materialised ancestor, and reads `_fold.<rid>` and any ancestor's `_fold.<id>` it has a hold on. When the last hold on a schema is released, the workspace drops the schema with every table in it. A registration takes its hold inside the transaction that creates its views, so no `unregister` finds the schema without a hold.

A registration's row in `registered` lists its schemas in a `held` column. A workspace opened on a file takes those holds again for it, so its views still read, and unregistering one drops them.

### API

- `Record.close() -> None` releases the record's holds. A second call does nothing.
- `Record` is a context manager, which closes it on exit.
- Garbage collection closes a `WorkingRecord` that is still open. A `Record` collected unclosed passes its holds to the workspace.
- A closed record refuses every read and edit with an error that states it is closed.
- `Revision.record` builds a new record once the one it caches is closed.
- `WorkingRecord(...)` is refused while its cursor has an open transaction, because the workspace's own cursor could not see the staging tables to drop them.

### Lifetime of a relation

A relation taken from a record, such as `record.attributes["p_nom"]`, is valid while the record is open. Once the record is closed and no other hold covers its tables, reading the relation fails with DuckDB's missing-table error. A `WorkingRecord`'s relation fails the same way once it is collected. A relation of a `Record` collected unclosed stays valid. A registered view stays valid until it is unregistered, whatever happens to the record.

## Rationale

`close()` frees a workspace when the caller chooses, as a server answering a request needs. The finaliser covers a forgotten `WorkingRecord`. One `weakref.finalize` serves both: `close()` detaches it and releases, so the release runs at most once.

A `Record`'s finaliser passes its holds on rather than releasing them. In `store.revision(rid).record.attributes["p_nom"].df()`, nothing keeps a name for the record, so the collector may take it before `.df()` runs. A release there would drop the fold that `.df()` reads, and the read would fail at random. The workspace keeps a forgotten `Record`'s tables as it did before this DEP. A closed `Record` has no holds left. Nobody reads a `WorkingRecord` that way, and its staging tables are worth dropping.

A `Record` takes a hold on every layer's fold it reads, because the workspace would otherwise drop an ancestor's fold with the ancestor's last record.

A schema per revision and per record is the unit of a hold. One `DROP SCHEMA … CASCADE` drops a `WorkingRecord`'s staging tables, and the holds list a handful of schemas, not hundreds of tables. Nesting keeps the top level that a SQL client sees to `main`, the three schemas and the registrations.

A registration takes holds on the schemas of its record rather than on the tables its SQL reads. The two sets cover the same tables, and by taking the record's set, datarecord need not parse the views.

The holds are in a Python mapping on the `Workspace`, behind one lock. A record cannot outlive the process, and a registration, the one holder that can, is a row of `registered`, which a reopened workspace lists.

A finaliser can run in any thread at any allocation, even inside a transaction on that thread's cursor. The workspace therefore drops schemas on a cursor of its own, under the lock. The lock also stops a lookup on another thread finding a schema between its last release and its drop.

It costs a contract: a relation is valid only while its record is open, or while its `WorkingRecord` is uncollected.

## Usage

```python
with Record.at(uri) as r:
    df = r.attributes["p_nom"].df()
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
3. The layout, with every table moved into `_fold`, `_staged` and `_scratch`.
4. `Record`: its holds, `close()`, the finaliser that passes holds on, and the refusal after close. A `WorkingRecord` reads a `Record` base through its own open copy, so closing the caller's base leaves its edits working.

Once built, a workspace that opens and closes records, edits and registrations in a loop ends with only `main`'s tables and three empty schemas. A view registered on a closed record reads until it is unregistered. A read after `close()` raises the closed error, and a relation of a `Record` collected unclosed still reads.

## Rejected ideas

- Only a finaliser: the caller cannot choose when to free a workspace, and a record in a reference cycle waits for the cycle collector.
- A `Record`'s finaliser releasing, as a `WorkingRecord`'s does: a chained read would fail whenever the collector runs between taking the relation and fetching it.
- Only `close()`: every forgotten record leaks.
- Holds kept in a DuckDB table: the Python holders do not survive a reopen, and a registration, which does, is a row of `registered`.
- A top-level schema per revision and per record: the top level fills with ids.
- A table prefix per kind in `main`: the workspace drops a record's tables one by one, and a SQL client sees them all.
- Evicting shared tables by age or size: it breaks a live record's relations silently.
- Dropping a revision's shared tables once it is materialised: a live record's relations still read them.
- Relying on DuckDB to refuse a drop: it tracks no view's dependencies.

## Open issues

- Who unregisters an abandoned registration, and by what rule. Unregistering frees its tables, so the rule sets how long they stay.
- Whether a record's planned relations form a reference cycle with it, which would delay its finaliser to the cycle collector.
- A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which its registrations may read and no record has a hold on. Nothing sweeps those that no `held` column lists.
