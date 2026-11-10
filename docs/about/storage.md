<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Storage

A record is a set of plain parquet files read through DuckDB. A store creates each file once and never overwrites it, so the files record the order of each write.

## Why parquet and DuckDB

A record on disk is a directory of parquet files and one `manifest.json` ([the format](../reference/format.md)). A layer's data files contain no schema and no layer ids. So you can open `attributes/p_nom.parquet` as an ordinary table with any parquet library, such as pandas or polars, without datarecord installed. The same layout works on a local disk and on S3.

The rejected alternative is a DuckDB database file as the store. A DuckDB file takes one writer at a time, so parallel jobs would queue on its lock. DuckDB also cannot write such a file to an S3 bucket. A store keeps the record at its location instead. The object that opens stores (a workspace) has a DuckDB database. It contains only what can be rebuilt or lost: cached folds and the staging tables of `WorkingRecord`s.

DuckDB is the engine because it reads parquet locally and remotely, plans lazily, and pushes a filter into the scan, a struct field's included. One file per attribute suits that, since a read of three attributes opens three files.

The cost is that a parquet file is never changed in place, so every edit writes new files. That matches layers written once. To query a record in SQL, a client runs DuckDB, directly or through registered views.

## Why a manifest is written last

A layer becomes a revision when its manifest is written, and not before. A write to a store runs in three steps: a create-only claim of a fresh id, the data files, then a create-only `manifest.json` ([the store](../reference/store.md)).

The reason is S3, which has no rename. A local write could stage its files and rename them into place, but a remote write that fails can leave part of a layer behind. Every read opens a layer through its manifest and reads only the files the manifest lists, so the leftovers of a failed attempt are unreachable. A remote write is then atomic for every read.

The claim exists for jobs that run for days. It makes a pending layer visible to a listing and to `cleanup`, which must not delete a job still running. The claim stays after the commit, so a second claim of one id fails at any time and an id identifies one revision for good. datarecord never deletes a pending layer on its own, because no timeout is both safe and useful for a job of unknown length.

The rejected alternatives each break on S3 or on a long job. Claiming an id by creating its directory is atomic only on a local file system. Dropping the claim after the commit would let a reused id fail only at its manifest, days later. A heartbeat would need a file a job rewrites for days, for a timeout no long job can meet.

Each manifest contains the full schema rather than a delta from its parent, so reading one file gives a revision's schema without walking its ancestry. A schema is small beside a layer, so the repetition costs little.

The cost is a clean-up you ask for. A failed write leaves files under its id until you call `store.cleanup` ([store a record](../howto/store-a-record.md)).

## Why relations are registered views

`Record.register(name)` creates a DuckDB view per dim, property subject and attribute, under a schema of that name in the workspace's database ([the format](../reference/format.md)). Any client of that database then reads the record in SQL, a client with no Python included.

Over a `WorkingRecord`, each view is created once and never replaced. A relation's plan does not depend on the staged rows, so a view created once returns every later edit. A client's query stays valid through any edit, with no message between the server and the client. The rejected alternative replaces a view whenever an edit adds something new. That ties the record to its registrations, and a client's query can fail between two edits.

The views are raw, as the relations are. A settled view would expand every broadcast and give up the lazy model, so a client applies the broadcast rule itself. Nested schemas keep the names of dims, properties and attributes apart, where one flat schema would need prefixed names such as `attr_p_nom`.

The workspace attaches its database under a fixed name, `datarecord` unless given, and never as a connection's default. A server's connection keeps its own default database, which datarecord's tables do not fill, and every name datarecord writes names its database, so one it forgot to qualify fails in the tests rather than resolving by luck. One database also lets `register` write its schemas, views and row in one transaction, since a DuckDB transaction writes to one attached database. The rejected alternative registers into a separate target database: the row and the views could not commit together, and a view there would have to name the workspace's database, whose name each client chooses.

The cost is a private DuckDB call. DuckDB's public method for a view takes one identifier and no nested path, so `register` uses its private API. datarecord therefore caps DuckDB at the newest release its tests passed on.

## Why writers never share a file

Each writer claims a fresh id, writes into its own layer directory and commits by its own manifest. Every file a write coordinates through is created once, so two writers never write one file.

The rejected alternative is one schema per store, in a shared manifest that each widening write updates. Every widening write would update that one file. Each write would race on it, a writer could lose the race after writing its layer, and one branch's new attribute would widen its siblings.

With a manifest per layer, a commit checks its schema against its parent's manifest, which no later write changes. No write has a shared file to race on or retry, so two writers that widen one parent at once both commit.

The cost is that a widening applies only to the revisions whose ancestry declares it. Two sibling branches may declare one name differently, which matters when you read them side by side. `materialise`, which caches a revision's fold under `resolved/`, writes the one kind of file that is not create-only. Two writers that materialise one revision write the same content. A standalone directory has no claim, so it takes one writer, and `ws.write` refuses a URI that already exists.

## What you can now judge

You can now judge where to put a store, whether to keep a DuckDB file between runs, and how many jobs may write at once.
