<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Workspaces and stores

A `Workspace` holds the DuckDB database and the means to read and write files.
A `Store` holds one location and its layers, and each layer carries the manifest that makes it a revision.
The location holds the record, and the database holds caches.
Every claim and manifest is written once, create-only, so two writers never take or commit one id.
Files go through a small `Files` protocol with create-only writes, backed by the local filesystem or by S3.

## The store

Several stores share one workspace, so records from them join.
A standalone directory needs the workspace alone.

```python
ws = Workspace()  # ":memory:" by default
store = ws.store("/data/a")  # one Store per normalised location
rev = store.revision(rid)  # look one up
record = ws.at("out/")  # a standalone directory, read under its own manifest
layer = store.layer(rid)  # one layer read alone, under its own manifest
```

A call takes the workspace explicitly only where nothing in hand carries one: `ws.store`, `ws.at`, and the `con` that a tool receives when it converts a framework object.
Everything else takes its workspace from what built it.
A revision and a claim take it from their store, a `Record` from the revision or workspace that builds it, and a `WorkingRecord` from its base record.

The location is required and absolute.
`ws.store("")` raises.
A local path is resolved on the call, so a later `chdir` or a process with another working directory reaches the same location.
`layer_dir`, `resolved_dir`, `manifest_uri` and `claim_uri` derive from the location and the id alone.

The location is authoritative, and the database is disposable.
Deleting a workspace's `database` loses at most uncommitted edits.
A file-backed database keeps staged edits across a crash and the `revisions` cache across runs, and it gives DuckDB room to spill.
Every cache keyed by id assumes that an id always names the same content.
A fresh UUID, claimed once, over write-once files guarantees this, and a location wiped and written again breaks it.

### The database's schemas

The tables a workspace creates sit in schemas of their own, so `main` holds only its bookkeeping:

| schema            | holds                                                                                        | created by                            |
| ----------------- | -------------------------------------------------------------------------------------------- | ------------------------------------- |
| `main`            | `schemas`, `revisions`, `registered`                                                         | the workspace                         |
| `_fold.<rid>`     | the [shared tables](concurrency.md#a-cursor-per-thread) of revision `rid`'s fold             | the first cursor to fold `rid`        |
| `_staged.<layer>` | a `WorkingRecord`'s [staging tables](../use/editing.md#staging)                              | `WorkingRecord(...)`                  |
| `_scratch`        | a call's intermediate tables, a fold's checkpoints and the PyPSA exporter's joins among them | the call using them, which drops them |
| `<name>`          | the [views of a registration](views.md)                                                      | `register(name)`                      |

`_fold`, `_staged` and `_scratch` are created with the workspace, so `register` refuses them as taken names.
The id in a nested schema's name keys its tables, which do not carry it, and is its 32-digit hex, without dashes.
A `WorkingRecord` holds its schema until it is closed or collected, and a registration of it holds the same schema until `Workspace.unregister(name)`; the workspace drops the schema, with every table in it, once neither does.
A workspace reopened on a file lists every registration in `registered`, so `unregister` drops its views there too.

### The layers

```text
<location>/
└── layers/<id>/
    ├── claim.json          # written when the id is handed out, and kept
    ├── manifest.json       # written last: makes the revision exist
    ├── dims/<dim>.parquet
    ├── attributes/<attr>.parquet
    └── resolved/           # node caches
```

A layer directory holds everything about its revision, so the tree has no file of its own and listing it is one listing of `layers/`.
A [standalone directory](concurrency.md#a-standalone-directory) is one layer at its own URI: `manifest.json` with no parent, beside `dims/` and `attributes/`.

The manifest is the layer's description and its commit:

```python
class Manifest:
    id: UUID
    parent: UUID | None
    schema: Schema  # full, not a delta
    files: LayerFiles  # {"dims": [...], "attributes": [...]}
    meta: dict[str, str | int | float | bool] = {}
```

`schema` is the full [schema](../model/schema.md#one-schema-per-record) the layer was written under, so one file answers a revision's schema without walking its ancestry.
`files` lists what the layer wrote, by declared name.
A read opens exactly these files, globs nothing, and ignores any file in the layer the list does not name, such as one a failed attempt left behind.
`meta` is the revision's own: flat, set at its write, and not inherited by a child.
Each value is a string, an integer, a finite float or a boolean, since JSON has no NaN or infinity, and any other value is refused before anything is written.
It says how one layer was made, such as a run's id.
`schema.meta` says something about the record, such as a framework's network attributes, and a child inherits it with the rest of the schema.

`claim.json` holds the id, the parent, the claimed `meta` and when the id was handed out.

The workspace caches each committed manifest whole, since none ever changes:

```sql
revisions(location VARCHAR, id UUID, parent UUID, meta JSON,
          files STRUCT(dims VARCHAR[], attributes VARCHAR[]), schema VARCHAR,
          PRIMARY KEY (location, id))
schemas(hash VARCHAR PRIMARY KEY, schema JSON)
```

A revision's `schema` is the SHA-256 of its schema's JSON, and `schemas` holds each distinct schema once, so a chain that repeats its parent's schema stores it once.
The workspace also keeps one parsed `Schema` per hash, which every revision sharing it hands out, so a manifest read from its file or the table validates its schema only where the hash is new to the workspace.
`meta` is JSON, so its values keep their types.
On a warm cache, building a `Record` reads no file.
The cache can be incomplete but never stale, and rows are only ever added.
A revision's row goes in only after its parent's, so a revision the cache holds has its whole ancestry there.

`revisions()` lists `layers/` once and reads the manifest of each layer the table lacks.
`revision(id)` on a miss reads that manifest and each parent's the table lacks, and raises `KeyError` where the first is absent, a pending layer included.
`revisions(meta={...})` returns the revisions whose `meta` holds every pair given, as one query over the table.
A value matches only a value of its own type, so `1` matches neither `1.0` nor `True`.
`claims()` reads the claim files on each call and caches nothing, since a pending layer may commit at any time.

Two cursors that add the same row at once conflict.
The row is the same whoever wins, so the loser moves on.
A conflict before the winner's row is visible is retried, for ten seconds at most.

### Files, local and remote

Tables go through DuckDB.
Manifests, claims, listing `layers/` and deleting a pending layer go through the workspace's `Files`, which the URI's scheme selects.
A URI without `://` is local, an `s3://` URI is remote, and any other scheme raises.

```python
class Files(Protocol):
    def read_text(self, uri: str) -> str | None: ...  # None on a miss
    def create(self, uri: str, text: str) -> None: ...  # only where nothing is
    def list(self, prefix: str) -> list[str]: ...  # names directly under prefix
    def exists(self, uri: str) -> bool: ...
    def delete(self, prefix: str) -> None: ...  # everything under prefix
```

`create` writes only where nothing is yet, and raises `PreconditionFailedError` otherwise.
Every claim and manifest is written by it.

Locally `create` writes under a temporary name and hard-links the file into place, which the OS refuses over an existing file, so a reader meets a whole file or none.

Remotely, `Files` runs on [boto3](https://boto3.amazonaws.com/), behind the optional extra `datarecord[remote]`.
`create` is `put_object` with the conditional header `IfNoneMatch="*"`.
A refused write (`412`, or `409` for a racing one) raises `PreconditionFailedError`.
DuckDB's `httpfs` writes a layer's parquet unconditionally, into a layer whose id was claimed once.
The condition holds only as far as the server honours it.
SeaweedFS checks it atomically from 4.18, and AWS S3 since November 2024.

The configuration is the AWS environment and config files, and datarecord adds no settings of its own.
boto3 reads them itself.
DuckDB gets `CREATE SECRET (TYPE s3, PROVIDER credential_chain)`.
When `AWS_ENDPOINT_URL` is set, the secret adds its `ENDPOINT`, `URL_STYLE 'path'`, `USE_SSL` per its scheme and `CHAIN 'env;config'`, because a self-hosted server has no bucket-subdomain DNS and no instance-metadata service to probe.
The workspace sets both up on the first `s3://` URI it receives, so a purely local workspace pays for neither.

## A revision exists only by its file

That file is its manifest.
A write to a store runs in three steps:

| step   | file                   | write         | holds                                     |
| ------ | ---------------------- | ------------- | ----------------------------------------- |
| claim  | `claim.json`           | create-only   | `id`, `parent`, `meta`, when it was taken |
| data   | `dims/`, `attributes/` | unconditional | the layer's parquet                       |
| commit | `manifest.json`        | create-only   | the layer manifest                        |

`store.claim(parent=..., meta=...)` hands out a fresh id by its claim, and `claim.commit(source, meta=...)` writes the data and then the manifest ([writing](../use/writing.md)).
A claim is create-only and kept after the commit, so a second claim of one id fails at any time and an id names one revision forever.
A layer with a claim and no manifest is pending.
`revision()`, `revisions()`, `ancestry()`, `store.layer` and `ws.at` reach a layer only through its manifest, so no read reaches a pending one.
A claim may stay pending for days, as when a job is submitted with it and commits at its end.

The commit checks its parent, `meta` and schema before it writes anything.
Its schema must pass `compatible_with` against its parent's manifest's ([versioning](../model/schema.md#versioning)), which cannot change, so there is no race to settle.
Its `meta` keeps every key of the claim's unchanged and may add keys, such as an outcome the job learns at its end.
Its parent is the claim's, which `store.claim` takes only from its own store, refusing another store's revision and naming both locations.

The edits of a `WorkingRecord` `w` are a delta against the revision `w` was built over, so they commit only as its child.
`store.commit(w)` takes that revision as the parent and refuses a `parent` passed beside it, and `claim.commit(w)` refuses a claim whose parent is another.
Over no revision of the store, a standalone directory or another store's revision, `store.commit(w)` refuses `w`, naming `ws.write(w, uri)`, which writes `w` whole.

Data goes straight into `layers/<id>/`, locally as remotely, and nothing is staged and renamed.
A commit that fails leaves its layer pending, with whatever data files it wrote.
No read reaches them, a retried commit writes them again, and the manifest lists only what the committing attempt wrote.
A [cleanup](#cleanup) deletes them on request.
A claim has one committer at a time, since two commits of one claim at once write their data files under the same names.

A standalone directory has no claim and orders its write by its manifest alone ([a standalone directory](concurrency.md#a-standalone-directory)).

## Cleanup

Only an explicit call deletes, and only a pending layer:

```python
store.claims()  # the pending layers, oldest first, with their meta
store.cleanup(ids=[...])  # these pending layers
store.cleanup(older_than=timedelta(days=14))  # pending, claimed at least this long ago
```

`cleanup` takes exactly one of `ids` and `older_than`, deletes each layer directory through `Files.delete`, and returns the ids it deleted.
A layer with a manifest is not deleted, and each is checked for one again right before its delete.
An id that is not pending is refused, naming it, before anything is deleted.
A layer directory without a `claim.json`, a claim still being written or what a delete left behind, is no claim: `claims()` does not list it and `older_than` does not age it.

A job may run for days, so no timeout is both safe and useful, and nothing deletes on its own.
A commit refuses a claim whose `claim.json` is gone, checking before it writes its data and again before its manifest.
Neither check is a lock, so a cleanup that deletes a layer between that last check and the manifest still leaves a revision listing files that are gone, and a layer that commits between its own check and its delete is still deleted.
The caller therefore picks the ids, or an age that no running job reaches.
