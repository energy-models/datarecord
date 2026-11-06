<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 9: a `Workspace` holds the connection, a `Store` holds a location

| DEP     | 9                                                              |
| ------- | -------------------------------------------------------------- |
| Title   | a `Workspace` holds the connection, a `Store` holds a location |
| Author  | Jonas Hörsch                                                   |
| Status  | Implemented (2026-10-10)                                       |
| Created | 2026-10-09                                                     |
| Version | Unreleased                                                     |

The design landed in [`Workspace` and `Store`](../../reference/store.md), [layers](../../reference/layers.md), [`WorkingRecord`](../../reference/editing.md), [tools](../../reference/tools.md) and [the package](../package.md), which win where they and this DEP differ.

## Abstract

`duck`'s module globals split into two objects. A `Workspace` has the DuckDB connection and file access, local or remote, which is all a standalone directory needs. A `Store` has one location, with its manifest, revision tree and layers, and works through a workspace that several stores share. A revision crosses a process boundary as its store's location.

## Motivation

Checked against `49b7268`:

- `default_connection()` is a process singleton on `:memory:`, and the `revisions` table lives only there. A revision deserialised in a new process has an empty ancestry.
- Only the schema reads `base_uri_of(con)`. Layers and caches use `DEFAULT_BASE_URI`, so `connect(base_uri=X)` writes its manifest under `X` and its layers elsewhere. An empty default writes `layers/<id>/` into the working directory.
- `PyPSA.results(n)` builds relations on DuckDB's default connection, so the next union fails.
- Revisions cannot be listed.
- A remote manifest is unreadable and unwritable, and a failed remote write leaves part of a layer.
- Of eight threads on one DuckDB 1.5.5 connection, seven fail.
- `materialise()` writes the `inputs` owner map first, so a reader can fold from half a cache.

One object holding location and connection would fix these, but a standalone directory would name a location it never uses.

## Specification

```python
class Workspace:
    con: DuckDBPyConnection
    def __init__(self, database: str = ":memory:") -> None: ...
    @classmethod
    def process(cls) -> Workspace: ...
    def store(self, uri: str) -> Store: ...
    def at(self, uri: str) -> Record: ...
    def write(self, source: LayerData | RecordLike, uri: str) -> Record: ...
    def close(self) -> None: ...

class Store:
    uri: str
    workspace: Workspace
    def root(self, source: LayerData | RecordLike) -> Revision: ...
    def revision(self, id: UUID) -> Revision: ...
    def revisions(self) -> list[Revision]: ...
    def layer(self, id: UUID) -> Record: ...
    def layer_dir(self, id: UUID) -> str: ...
    def resolved_dir(self, id: UUID) -> str: ...

class Revision:
    def child(self, source: LayerData | RecordLike) -> Revision: ...
```

- Only `ws.store`, `ws.at`, `ws.write` and a tool's `con` name the workspace. Everything else takes it from its source.
- `ws.store(uri)` returns one store per normalised location, filled with `dict.setdefault`. `ws.store("")` raises `ValueError`, and a local path becomes absolute. A store has no connection and no `close`.
- `ws.at(uri)` requires the directory's own `manifest.json`. A tree layer reads through `store.layer(id)`.
- There is no empty revision. `store.root` and `rev.child` write the layer, then widen the manifest, then write `revisions/<id>.json`, create-only. The revision file makes the layer real.
- `ws.write` writes the manifest last. A remote `ws.write` refuses a URI under which `Files.list` finds anything.
- `materialise()` writes `owner_map/inputs.parquet` last, and each local file goes to a temporary name and is renamed.
- `WorkingRecord(record)` takes no `con` and has no `commit`. `w.edits` is its staged `LayerData`. `rev.child` raises when a staged source's base is another revision.
- The tree is `<location>/revisions/<id>.json`, `{"id", "parent"}`, with a UUID drawn in Python. The workspace's `revisions` table, `(location, id, parent)`, caches those files and is only added to. A concurrent duplicate insert's `TransactionException` is caught.
- Parquet goes through DuckDB. The manifest, revision files, listings and existence checks go through `Files`, with `read_text`, `write_text(..., expect=Version | Absent | None)`, `list` and `exists`.
- `s3://` is remote and a URI without `://` is local. Any other scheme raises. Local `Files` uses `pathlib`, `open(..., "x")` for `Absent`, and a SHA-256 `Version` checked under a sibling `.lock`. Remote `Files` uses boto3 behind `datarecord[remote] = ["boto3>=1.35.69"]`, with `IfNoneMatch="*"` and `IfMatch=<etag>`.
- The manifest is widened by compare-and-swap, and a writer that loses re-reads and checks again.
- Remote access is configured by the AWS environment alone. DuckDB gets `CREATE SECRET (TYPE s3, PROVIDER credential_chain, ...)`, plus `ENDPOINT`, `URL_STYLE 'path'` and `CHAIN 'env;config'` when `AWS_ENDPOINT_URL` is set. `httpfs` loads on the first `://` URI.
- `ws.con` is a cursor per thread, from a `threading.local` subclass. Only a relation keeps a cursor. Every other object looks up `workspace.con` per call. A relation, a frame and a `WorkingRecord` belong to the thread that built them.
- A frozen node's folded axes and groups become shared tables, `dims_{dim}_{id}` and `group_{name}_{id}`, read through `cast_declared`. A miss that loses the `create` race reads the winner's table.
- `as_relation` refuses a relation from another connection or cursor with a `ValueError` that states the fix.
- `Revision` serialises as `{"id", "parent", "store"}` and validates back through `Workspace.process().store(uri).revision(id)`, the one default left. `Store.__reduce__` does the same.
- A database file is a workspace, never the record. A location is not reused.
- `Record` moves to `layered/record.py`, and `datarecord/record.py` keeps `RecordLike` and `LayerData`.
- `Tool.to_datarecord(model, con)` and `Tool.results(model, con)` require `con`, and `on_connection` goes.
- Deleted: `DEFAULT_BASE_URI`, `DATARECORD_BASE_URI`, `_BASE_URIS`, `base_uri_of`, `default_connection`, `connect`, the `layer_dir` macro, every `base_uri=None`, `read_json`, `Revision.create`, `Revision.get`, `Record.at`, `Record.over`, public `write_record`, `commit`, `NewChild`, `Directory`, `Target` and `Resolver._cached_dims`. `Workspace` and `Store` join `__all__`.

## Rationale

A required location replaces `base_uri=None`, which silently meant the process default.

`child` sits on the parent, because a parent is always its own store's. One call per destination fits destinations that need and return different things.

`commit` did three jobs, and the caller now does each: picks `w.edits` or `w`, names the parent, and calls `rollback()`. Nothing guards `rev.child(w)`, which writes the whole record as a correct but larger layer.

A revision file per UUID is write-once like a layer, so two processes never write one object. One listing per thousand revisions replaces a read per revision. The `revisions` cache cannot go stale, because a parent never changes.

boto3 is what Manta already runs. Its conditional writes guard the revision files and the manifest, but not the layer, whose parquet DuckDB writes. A fresh UUID and an unreachable failed write cover that.

The guarantees hold only as far as a server honours the headers:

| SeaweedFS      | `If-None-Match` and `If-Match` on `PutObject`                                   |
| -------------- | ------------------------------------------------------------------------------- |
| before 3.97    | ignored                                                                         |
| 3.97 to 4.17   | checked before upload, so two racing writers can both pass (#7154)              |
| 4.18 and later | checked under the write lock, so exactly one of two racing writers wins (#8802) |

Manta's pin moves from 4.05 to 4.18 or later. AWS S3 honours both headers since late 2024.

The tree cannot live only in a DuckDB file, which takes one writer and cannot be remote. A file still earns its place: staged edits survive a crash, the `revisions` cache persists, and DuckDB can spill.

## Implementation notes

1. `Workspace` and `Store`, the local `Files`, the tree, cursors, shared fold tables and the deleted globals, in one commit. A half-honoured base is a bug either side.
2. Writing: `root`, `child`, `ws.write`, and `edits`.
3. Remote `Files` and conditional manifest writes.
4. Tools take `con`.

On DuckDB 1.5.5, eight threads on their own cursors all succeed. So do sixteen threads taking a first cursor at once, fifty times over.

The remote tests run against SeaweedFS 4.18 or later in a container, because a mock would ignore the headers.

The build departed in the remote `Files`. boto3 uses path-style addressing whenever `AWS_ENDPOINT_URL` is set. A failure setting up `httpfs` raises. A `403` on a read or `exists` counts as a miss, and a `409` counts as a refused write beside `412`.

An older location has no `revisions/` and reads as an empty tree, fixed by exporting the `revisions` table as one JSON file per row.

## Rejected ideas

- One object for location and connection: two locations could not join.
- `store.write(source, parent=...)` repeats the store, `rev.write` reads as changing `rev`, and a target union needs overloads.
- Revision files in a layer, one `revisions.parquet` or a shared DuckDB file: not create-only.
- `gen_random_uuid()`: the layer directory needs the id before any row exists.
- `fsspec` or `s3fs` has no create-only write, and obstore is a second S3 client.
- A `remote=` override: it would not reach `Workspace.process()` in a deserialising task.
- A secret from `KEY_ID` and `SECRET`: loses tokens and roles.
- A dict keyed by `threading.get_ident()`, whose ids are reused, or a `ContextVar`, which a task runner copies into workers.
- A thread check: it misses calls a frame makes on its own cursor.
- Re-issuing a relation on another cursor: impossible over a Python object.
- Copying a foreign relation: an unasked-for full materialisation.

## Open issues

- Sweeping orphaned layers of a failed remote write.
- `ws.new(schema)`, a `WorkingRecord` with no base.
- A handle to resume staged edits from a database file.
- A `created` field on a revision, and reading over `https://`.
