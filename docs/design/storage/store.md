<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Workspaces and stores

A `Workspace` holds the DuckDB database and the means to read and write files.
A `Store` holds one location with its manifest, revision tree and layers.
The location holds the record, and the database holds caches.
Revision files sit beside the layers, one per revision, and are written create-only.
Files go through a small `Files` protocol with conditional writes, backed by the local filesystem or by S3.

## The store

Several stores share one workspace, so records from them join.
A standalone directory needs the workspace alone.

```python
ws = Workspace()  # ":memory:" by default
store = ws.store("/data/a")  # one Store per normalised location
rev = store.revision(rid)  # look one up
record = ws.at("out/")  # a standalone directory, read under its own manifest
layer = store.layer(rid)  # one layer read alone, under the store's manifest
```

A call takes the workspace explicitly only where nothing in hand carries one: `ws.store`, `ws.at`, and the `con` that a tool receives when it converts a framework object.
Everything else takes its workspace from what built it.
A revision takes it from its store, a `Record` from the revision or workspace that builds it, and a `WorkingRecord` from its base record.

The location is required and absolute.
`ws.store("")` raises.
A local path is resolved on the call, so a later `chdir` or a process with another working directory reaches the same location.
`layer_dir`, `resolved_dir` and the manifest's location derive from the location alone.

The location is authoritative, and the database is disposable.
Deleting a workspace's `database` loses at most uncommitted edits.
A file-backed database keeps staged edits across a crash and the `revisions` cache across runs, and it gives DuckDB room to spill.
A location is written once.
Every cache keyed by id assumes that an id always names the same content.
A fresh UUID over write-once files guarantees this, and a location wiped and written again breaks it.

### The tree beside the layers

```text
<location>/
├── manifest.json
├── revisions/<id>.json        # {"id": ..., "parent": ...}
└── layers/<id>/
```

The revision files sit beside `layers/`, so a layer directory holds exactly what the layer wrote, and listing the tree is one listing over small files.
Each revision has its own file, named by a fresh UUID, so the file is write-once like a layer.
Two processes that create revisions in one location never write the same object.
The file is written create-only through [files](#files-local-and-remote), which enforces this.

The workspace's `revisions` table caches those files for every store on the workspace, keyed by `(location, id)`.
A revision's parent never changes, so the cache can be incomplete but never stale, and rows are only ever added.
`revisions()` lists `revisions/` once and reads only the files the table lacks.
`revision(id)` on a miss reads that file and each parent the table lacks, and raises `KeyError` when the first file is absent.
Two cursors that add the same row at once conflict.
The row is the same whoever wins, so the loser moves on.

### Files, local and remote

Tables go through DuckDB.
The manifest, the revision files and their listing go through the workspace's `Files`, which the URI's scheme selects.
A URI without `://` is local, an `s3://` URI is remote, and any other scheme raises.

```python
class Files(Protocol):
    def read_text(self, uri: str) -> tuple[str, Version] | None: ...  # None on a miss
    def write_text(
        self, uri: str, text: str, *, expect: Version | Absent | None
    ) -> Version: ...
    def list(self, prefix: str) -> list[str]: ...  # names directly under prefix
    def exists(self, uri: str) -> bool: ...
```

`expect` is the precondition of a write.
`Absent` writes only if nothing is there.
A `Version` writes only if the object is unchanged since it was read.
`None` writes unconditionally.

Locally every write lands under a temporary name and moves into place, so a reader meets a whole file or none.
`Absent` moves the file with a hard link, which the OS refuses over an existing file.
A local `Version` is the content's SHA-256.
The writer checks it and replaces the file while it holds a lock on a sibling `<name>.lock`, which makes the check and the write one step.

Remotely, `Files` runs on [boto3](https://boto3.amazonaws.com/), behind the optional extra `datarecord[remote]`.
`expect` maps to the conditional headers of `put_object`: `IfNoneMatch="*"` for `Absent`, and `IfMatch` for a `Version`, which is the object's ETag.
A refused write (`412`, or `409` for a racing one) raises `PreconditionFailedError`.
The conditions guard the revision files, which the server then keeps create-only, and the [manifest's compare-and-swap](concurrency.md#the-manifests-compare-and-swap).
DuckDB's `httpfs` writes a layer's parquet unconditionally, under a fresh UUID.
The conditions hold only as far as the server honours them.
SeaweedFS checks them atomically from 4.18, and AWS S3 since November 2024.

The configuration is the AWS environment and config files, and datarecord adds no settings of its own.
boto3 reads them itself.
DuckDB gets `CREATE SECRET (TYPE s3, PROVIDER credential_chain)`.
When `AWS_ENDPOINT_URL` is set, the secret adds its `ENDPOINT`, `URL_STYLE 'path'`, `USE_SSL` per its scheme and `CHAIN 'env;config'`, because a self-hosted server has no bucket-subdomain DNS and no instance-metadata service to probe.
The workspace sets both up on the first `s3://` URI it receives, so a purely local workspace pays for neither.

## A revision exists only by its file

A tree write runs in three steps: the layer, then the [manifest](../model/schema.md#one-schema-per-record), then [`revisions/<id>.json`](#the-tree-beside-the-layers).
The revision file comes last, and its existence makes the layer real.
`revision()`, `revisions()` and `ancestry()` reach a layer only through its revision file.

Locally a failed write leaves nothing, since the layer is staged and renamed.
Remotely there is no rename, so a failed write can leave part of a layer behind.
That part is unreachable, so every reader sees a remote write as atomic.
Removing such leftovers is a separate cleanup task.

The manifest check comes after the layer.
A layer whose schema is incompatible, or whose write fails, leaves the manifest untouched and the layer unreachable.
The manifest widens only for a layer that exists.

A standalone directory has no revision file and orders its write by the manifest ([a standalone directory](concurrency.md#a-standalone-directory)).
