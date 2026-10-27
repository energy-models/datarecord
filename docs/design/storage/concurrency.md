<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Threads, processes and concurrent writers

Each thread reads through its own DuckDB cursor, and relations are the only objects bound to one.
Fold results are shared tables keyed by the id of the layer they fold, so a fold that one thread builds serves every thread.
A revision serialises as its location and id, so it resolves again in a fresh process.
Two writers to one store meet only at the manifest.
A bounded compare-and-swap settles that race and keeps both widenings.

## A cursor per thread

A DuckDB connection is unsafe to use from two threads at once, and DuckDB's answer is a cursor per thread.
The workspace's `con` is the calling thread's cursor on the workspace's one database.
A `threading.local` subclass makes it on first use in each thread.
Its `__init__` reruns in each new thread, so the lookup needs no existence check and no lock, and a cursor goes away with its thread.
A dict keyed by thread id would reuse ids, and a `ContextVar` would be copied into a task runner's workers, which would then share the parent's cursor.

Tables are shared between cursors: the `revisions` cache, the staging tables and the folded caches.
A `DuckDBPyRelation` is bound to the cursor that built it.
The line between thread-bound and shared objects runs around relations alone.

A store, a revision, a `Record` and a layer source hold their workspace and look up `workspace.con` on each call.
Every relation they build lands on the calling thread's cursor, so one `Record` serves any thread.
Anything else they cache is plain data that every thread computes the same way.
A relation and a `WorkingRecord` belong to the thread that built them.
When one of them reaches another thread's write path, it is rejected as [a relation from another connection](#a-relation-from-another-connection-is-rejected) is.

A fold caches its results as shared tables.
The frozen prefix's folded axes are the tables `dims_<dim>_<id>`, a [multi-dim](../model/multi-dims.md)'s like any other.
Its [dead labels](../layers/resolution.md#a-label-outside-the-key) are the tables `dead_<dim>_<id>`, one per coordinate that has any.
A last table, `coords_<id>`, lists both kinds and is created after them, so a reader that finds it finds them all.
The [owner map](../layers/resolution.md#owner-map) is `owner_map_<key>_<id>` beside them, where `<key>` is the owner map's key columns joined by commas.
A dim's name is spelled as declared, since the schema [holds every name to a plain identifier](../model/schema.md#names).
`<id>` is the node whose frozen layers were folded: a revision's id, the base's id for a `WorkingRecord`, or the [id in the manifest](../model/schema.md#one-schema-per-record) for a standalone directory.

The schema is no part of the key beyond `partial`, so a fold made before the manifest widened is read as it is under the wider schema, while one made before a dim joined or left `partial` is folded again.
A [compatible widening](../model/schema.md#versioning) only adds, and changes no column a table already has.
The one addition a table can lack is the new dim's field in the [flag structs](../layers/resolution.md#owner-map).
Each reader of a flag takes a missing field as unset, and decides this from the struct's type when it builds the query.
A materialised owner map is cast to the declared types.
These tables are left as the fold produced them, and each holds what the same fold returns uncached.

A fold that one thread builds serves every thread.
When two threads miss at once, both fold and both create the table.
DuckDB fails the second with a write-write conflict, and the miss path catches it and reads the winner's table.
The duplicate fold wastes work and gives the same answer.

### A relation from another connection is rejected

`as_relation` is the boundary that every frame crosses on its way to a write or an edit.
It binds a relation to the target cursor first, which costs no read.
The bind fails for a relation from another connection or from another thread's cursor.
`as_relation` re-raises that failure as a `ValueError` that names the fix: build the frame on `record.con` in this thread, or hand over a pandas, polars or arrow frame to land.
Copying the relation onto the target connection would collect it in full, which the caller never asked for, and fails anyway once that connection is closed.

## Crossing a process boundary

`Revision` serialises its store as its location: `{"id": …, "parent": …, "store": "/data/a"}`.
It validates back through `Workspace.process().store(uri).revision(id)`, which reads the revision file and each parent the process lacks, so `ancestry()` answers in a fresh process.
`Store` pickles the same way.

`Workspace.process()` is the one default workspace, and only deserialisation reaches it implicitly.
It is created on first use, on `:memory:`.
Code that wants its own records to join with deserialised ones names it.

## The manifest's compare-and-swap

Two writers race on `manifest.json` when they create a location's first layers or widen its schema at once.
The manifest is therefore written through a [conditional write](store.md#files-local-and-remote).
A writer reads the manifest with its version, or `Absent` where there is none, checks compatibility, and writes only if the manifest is still at that version.
A writer that loses re-reads and checks again.
Two writers that widen at once end with both widenings, the later schema including the earlier, or with the later writer's incompatibility reported.
No widening is silently lost.

The retry is bounded.
A writer that loses ten times, with a jittered backoff in between, gives up with an error.
A credential that can write `manifest.json` but cannot read it meets the same error, because a remote read answered 403 counts as missing and every create-only write then fails.

`materialise()` follows the same rule.
`owner_map.parquet` [marks a node as materialised](../layers/resolution.md#materialised-node-caches), so it is written last, after the resolved dims.
Locally each file is written under a temporary name and renamed.
A parquet file appears at its final path before it is complete, and a reader that meets it mid-write fails.
Two writers that materialise one node write the same content, so that race is harmless.

## A standalone directory

A standalone directory has no revision file, so its manifest is written last.
`ws.at` requires a manifest, so a directory whose write failed reads as no record.

`ws.write` writes to a URI that does not exist yet and refuses one that does: locally the path, even an empty directory, and remotely any key under it.
A whole-record write refuses an existing layer in the same way.
The caller picks this URI, unlike a layer's UUID, and a write over an existing record would mix the two records under the old manifest.
The check and the write are two steps, so two writers racing to one new URI can still mix.
A standalone directory has one writer.

Each write gives the record a fresh [id](../model/schema.md#one-schema-per-record) in its manifest.
A record deleted and written again at one URI is therefore a new layer to every workspace, and no cached fold of the old record applies to it.
`ws.write` and `ws.at` resolve a local path as `ws.store` does.
