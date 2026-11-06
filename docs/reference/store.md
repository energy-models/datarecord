<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Workspace and store

This page describes `Workspace`, `Store`, `Claim` and `Revision`, in the order you use them.
A workspace is the DuckDB database and the file access that its stores share, so records from several stores join.
A store is one location, local or on S3, that contains the layers of a tree of revisions.

```python
from datarecord import Workspace

ws = Workspace()  # ":memory:" by default
store = ws.store("/data/my-record")
root = store.commit(source, parent=None)
record = ws.write(source, "out/")
```

`root` is a [revision](layers.md#revisions) of the store, and `record` is a standalone directory read back.

## `Workspace`

| Member                             | Does                                                                   |
| ---------------------------------- | ---------------------------------------------------------------------- |
| `Workspace(database=":memory:")`   | opens the DuckDB database, in memory or in a file                      |
| `con`                              | returns the calling thread's cursor on the database                    |
| `store(uri)`                       | returns the workspace's one `Store` for the normalised location        |
| `write(source, uri, *, meta=None)` | writes `source` as a standalone directory and returns it as a `Record` |
| `at(uri)`                          | reads the standalone directory at `uri` as a `Record`                  |
| `registered()`                     | returns the [`registered` table](format.md#the-registered-table)       |
| `unregister(name)`                 | drops a [registration](format.md#registered-views)                     |
| `Workspace.process()`              | returns the process's default workspace, created on first use          |
| `close()`                          | closes the database and every thread's cursor                          |

A location is a local path or an `s3://` URI, and any other scheme raises `ValueError`.
`store("")` raises, and a local path is resolved when you call `store`, `write` or `at`, so a later `chdir` does not change the location.

The record's data lies at the location, and the database contains only caches and staged edits.
Deleting a workspace's database file loses at most the edits not yet committed.
In a file-backed database, staged edits survive a crash and the manifest cache lasts across runs.

### A standalone directory

A standalone directory is one layer at its own URI, with a manifest that lists no parent.
`ws.write` writes the data and then the manifest, under a fresh id, and refuses a URI where anything exists:

```text
FileExistsError: /data/out/ already exists; a standalone record is written to a URI that does not exist yet
```

Locally, an empty directory counts as existing, and remotely any key under the URI does.
A write that fails leaves its data files at the URI and no manifest, so a second write there raises until the files are deleted.

`ws.at` raises `FileNotFoundError` for a URI with no `manifest.json`, and `ValueError` for a manifest that lists a parent.
`store.layer(id)` reads one layer of a store alone.

### The database

The tables that a workspace creates lie in DuckDB schemas of their own:

| Schema         | Contains                                | Created by                      |
| -------------- | --------------------------------------- | ------------------------------- |
| `main`         | `schemas`, `revisions` and `registered` | the workspace                   |
| `_fold.<id>`   | the tables of one revision's fold       | the first cursor to fold `<id>` |
| `_staged.<id>` | a `WorkingRecord`'s staging tables      | `WorkingRecord(...)`            |
| `_scratch`     | a call's intermediate tables            | the call, which drops them      |
| `<name>`       | the views of a registration             | `record.register(name)`         |

`<id>` is the 32-digit hex of the revision's or layer's UUID.
`revisions` and `schemas` cache each manifest whole, and gain rows only, so the cache can be incomplete and never stale.

The workspace keeps the `_fold` schemas of the layers a `Record` folds until `record.close()`, and a `WorkingRecord`'s `_staged` schema until it closes.
It keeps the schemas of a registered record until `unregister`.
The workspace drops a schema once no open record or registration reads it.
A closed record raises `ValueError` on every read, and a `Record` collected unclosed leaves its schemas to the workspace until the workspace closes.

### Files, local and remote

Manifests, claims, listing `layers/` and deleting a pending layer go through the workspace's `Files`, which datarecord picks by the URI's scheme.
DuckDB reads and writes the parquet.

```python
class Files(Protocol):
    def read_text(self, uri: str) -> str | None: ...
    def create(self, uri: str, text: str) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def exists(self, uri: str) -> bool: ...
    def delete(self, prefix: str) -> None: ...
```

`create` writes only where nothing is, and raises `PreconditionFailedError` otherwise.
Locally it writes under a temporary name and hard-links the file into place, so a read finds a whole file or none.

An `s3://` URI needs the optional extra `datarecord[remote]`, which installs the boto3 package.
Remotely, `create` is a `put_object` with `IfNoneMatch="*"`, which the server must honour, as AWS S3 does and SeaweedFS does from version 4.18 on.
Credentials and the endpoint come from the AWS environment and config files.
Where `AWS_ENDPOINT_URL` is set, DuckDB's S3 secret takes its endpoint with path-style URLs, for a self-hosted server.

### Threads

`ws.con` returns the calling thread's cursor, so every thread can share one workspace, one store and one `Record`.
Tables, registered views and fold results are shared between cursors.
A `DuckDBPyRelation` and a `WorkingRecord` belong to the thread that built them.
A write or an edit refuses a relation from another thread or connection:

```text
ValueError: this relation was built on another connection or thread; build it on `record.con` in this thread, or hand over a pandas, polars or arrow frame to land
```

When two threads fold one revision at once, both fold and datarecord keeps one of the two tables, so each reads the same answer.

!!! info "See also"

    [Store a record](../howto/store-a-record.md) shows a store, a directory, S3 and threads in use.

## `Store`

| Member                                  | Does                                                                          |
| --------------------------------------- | ----------------------------------------------------------------------------- |
| `uri`                                   | the absolute location                                                         |
| `claim(*, parent, meta=None)`           | hands out a new layer id under `parent` and returns a `Claim`                 |
| `commit(source, *, parent, meta=None)`  | claims and commits in one call, and returns the `Revision`                    |
| `commit(w, *, meta=None)`               | writes a `WorkingRecord`'s edits as a child of the revision it was built over |
| `revision(id)`                          | returns one revision, and raises `KeyError` for an id with no manifest        |
| `revisions(*, meta=None)`               | returns every revision, unordered, or those whose `meta` has every pair given |
| `claims()`                              | returns the pending layers, oldest first                                      |
| `cleanup(*, ids=None, older_than=None)` | deletes pending layers and returns their ids                                  |
| `layer(id)`                             | reads one layer alone as a `Record`                                           |

`parent` is a required keyword, so a new root is `parent=None`.
`revisions(meta={"run": 1})` matches a value of the same type only, so `1` matches neither `1.0` nor `True`.

### The steps of a commit { #a-revision-exists-only-by-its-manifest }

A write to a store runs in three steps:

| Step   | File                   | Write         | Contains                                  |
| ------ | ---------------------- | ------------- | ----------------------------------------- |
| claim  | `claim.json`           | create-only   | `id`, `parent`, `meta`, when it was taken |
| data   | `dims/`, `attributes/` | unconditional | the layer's parquet                       |
| commit | `manifest.json`        | create-only   | the [manifest](format.md#manifestjson)    |

A layer with a claim and no manifest is pending, and no read, listing or `ancestry()` finds it.
A commit that fails leaves its layer pending with the files it wrote, and a retried commit writes them again.

Before it writes data, a commit checks three things and refuses the layer otherwise:

- The layer's schema passes [`compatible_with`](schema.md) against the parent's manifest's schema.
- Its `meta` has every key of the claim's `meta` with the same value, and may add keys.
- Its parent is a revision of this store.

`store.commit(w)` takes `w`'s base as the parent and raises `TypeError` for a `parent` passed beside it.
Over a base that is no revision of this store, such as a standalone directory, it refuses `w` and names `ws.write(w, uri)`, which writes `w` whole.

### `Claim`

A `Claim` is a revision id that a store reserves before the layer is written.
It has `id`, `store`, `parent`, `meta` and `created`, and `claim.commit(source, *, meta=None)` writes the layer and returns the `Revision`.

A claim serialises as its store's location and its id, and pickles the same way.
A job in another process can then commit it, days later if need be.
A claim has one committer at a time.
A commit raises `ValueError` once a cleanup deleted its claim.

### Cleanup

Only `cleanup` deletes, and only a pending layer:

```python
store.cleanup(ids=[claim.id])
store.cleanup(older_than=timedelta(days=14))
```

`cleanup` takes exactly one of `ids` and `older_than`, and raises `ValueError` before it deletes anything if an id is not pending.
It checks each layer for a manifest again right before deleting it, without a lock.
A cleanup that races a commit can still delete a layer that is committing, so pass ids or an age that match no running job.

### Concurrent writers

Each writer claims a fresh id and writes into its own layer directory, so two writers never write the same file.
Two writers that widen one parent's schema at once both commit, each child under its own schema.
`materialise()` writes the one kind of file that is not create-only, and two writers that materialise one revision write the same content.

## `Revision`

A `Revision` is the id of one layer in a store.
[The layers page](layers.md#revisions) lists its members.

A revision serialises as its id and its store's location, and pickles the same way:

```python
payload = rev.model_dump_json()  # {"id": "4731e41b-…", "store": "/data/my-record"}
rev = Revision.model_validate_json(payload)  # in another process
```

The second process loads the revision onto `Workspace.process()` and reads its manifest and each parent's, so `rev.ancestry()` returns there too.

`rev.record` builds the record once and returns the same one until you close it, then builds another.

!!! info "See also"

    [The storage page](../about/storage.md) argues for the manifest written last and for writers that share no file.
    [The API reference](api/store.md) lists every signature.
