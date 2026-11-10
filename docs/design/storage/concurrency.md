<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Threads, processes and concurrent writers

Each thread reads through its own DuckDB cursor, and relations are the only objects bound to one.
Fold results are shared tables keyed by the id of the layer they fold, so a fold that one thread builds serves every thread.
A revision or a claim serialises as its location and id, so it resolves again in a fresh process.
Two writers to one store never write the same file, and each checks its schema against its parent's manifest, which never changes, so they have no race to settle.

## A cursor per thread

A DuckDB connection is unsafe to use from two threads at once, and DuckDB's answer is a cursor per thread.
The workspace's `con` is the calling thread's cursor on the connection that [the workspace's database](store.md#the-workspaces-database) is attached to.
A `threading.local` subclass makes it on first use in each thread.
Its `__init__` reruns in each new thread, so the lookup needs no existence check and no lock, and a cursor goes away with its thread.
A dict keyed by thread id would reuse ids, and a `ContextVar` would be copied into a task runner's workers, which would then share the parent's cursor.

Tables in the workspace's database are shared between cursors: the `revisions` cache, the staging tables and the folded caches.
So are a record's [registered views](views.md), which every cursor reads and which show an edit once its transaction commits.
A `DuckDBPyRelation` is bound to the cursor that built it.
The line between thread-bound and shared objects runs around relations alone.

A store, a revision, a `Record` and a layer source hold their workspace and look up `workspace.con` on each call.
Every relation they build lands on the calling thread's cursor, so one `Record` serves any thread.
Anything else they cache is plain data that every thread computes the same way.
A relation and a `WorkingRecord` belong to the thread that built them.
When one of them reaches another thread's write path, it is rejected as [a relation from another connection](#a-relation-from-another-connection-is-rejected) is.

A fold caches its results as shared tables.
The frozen layers' folded axes are the tables `dims_<dim>_<id>`, a [multi-dim](../model/multi-dims.md)'s like any other.
Its [dead labels](../layers/resolution.md#a-label-outside-the-key) are the tables `dead_<dim>_<id>`, one per coordinate that has any.
A last table, `dim_owners_<id>`, has the shape of a node cache's [`dim_owners.parquet`](../layers/resolution.md#materialised-node-caches), a row `(dim, layer_uuid, has_dead)` per folded axis.
It is created after them, so a reader that finds it finds them all.
A reader opens `dead_<dim>_<id>` only where `has_dead` is true.
The [owner map](../layers/resolution.md#owner-map) is `owner_map_<id>` beside them.
A dim's name is spelled as declared, since the schema [holds every name to a plain identifier](../model/schema.md#names).
`<id>` is the node whose frozen layers were folded: a revision's id, the base's id for a `WorkingRecord`, or the [id in its manifest](store.md#the-layers) for a standalone directory.

The fold reads each layer under [its own schema](../layers/resolution.md#extending-the-map-to-a-wider-schema), so what it folds for a node depends on the node's ancestry alone, and one table per node serves every reader.
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

`Revision` serialises its store as its location: `{"id": …, "store": "/data/a"}`.
It validates back through `Workspace.process().store(uri).revision(id)`, which reads its manifest and each parent's the process lacks, so `ancestry()` answers in a fresh process.
`Store` pickles the same way.

A `Claim` serialises alike, as its location and id, and validates or unpickles back by reading its claim file, so its parent and `meta` come from the store and nothing cached travels.
A job handed a claim commits it in its own process, days later if it runs that long.

`Workspace.process()` is the one default workspace, and only deserialisation reaches it implicitly.
It is created on first use, on `:memory:`.
Code that wants its own records to join with deserialised ones names it.

## Writers never share a file

Each writer claims a fresh id, writes its data into its own layer directory and commits by its own manifest, all create-only but the parquet, which no other writer names.
A layer's schema is checked against its parent's manifest, which no later write changes.
Two writers that widen one parent at once therefore both land, each child under its own schema, and neither sees the other's widening.
A widening reaches only the revisions whose ancestry declares it, and two sibling branches may declare one name differently.

`materialise()` writes the one kind of file that is not create-only.
`owner_map.parquet` [marks a node as materialised](../layers/resolution.md#materialised-node-caches), so it is written last, after the dim owners and dead labels.
Locally each file is written under a temporary name and renamed.
A remote object appears whole or not at all, so it needs neither.
Two writers that materialise one node write the same content, so that race is harmless.

## A standalone directory

A standalone directory has no claim, so its manifest, written last and create-only, is all that orders its write.
`ws.at` requires a manifest, so a directory whose write failed reads as no record.

`ws.write` writes to a URI that does not exist yet and refuses one that does: locally the path, even an empty directory, and remotely any key under it.
The caller picks this URI, unlike a layer's id, and a write over an existing record would replace its data files before its create-only manifest refused the write.
The check and the write are two steps, so two writers racing to one new URI can still mix their data files, though only one manifest lands.
A standalone directory has one writer.

Each write gives the record a fresh [id](store.md#the-layers) in its manifest.
A record deleted and written again at one URI is therefore a new layer to every workspace, and no cached fold of the old record applies to it.
`ws.write` and `ws.at` resolve a local path as `ws.store` does.
