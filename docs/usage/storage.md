<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Storage

## Workspaces and stores

A `Workspace` holds the DuckDB database and reaches files.
A `Store` is one location with its [schema](../design/model/schema.md#one-schema-per-record), its revisions and its layers.
Several stores share one workspace, so their records join ([design](../design/storage/store.md#the-store)).

```python
from datarecord import Workspace

ws = Workspace()  # ":memory:" by default
store = ws.store("/data/my-record")  # absolute; never empty
record = ws.at("out/")  # a standalone directory
```

A revision's file is written after its layer and the manifest, so a write that fails leaves no revision behind ([design](../design/storage/store.md#a-revision-exists-only-by-its-file)).

## A standalone directory

```python
record = ws.write(source, "out/")  # a new directory, read back
record = ws.at("out/")  # read it again later
```

`ws.write` refuses a URI where anything exists, even an empty local directory.
It writes the manifest last, so a write that fails leaves no record behind ([design](../design/storage/concurrency.md#a-standalone-directory)).

## On S3

```python
store = ws.store("s3://bucket/my-record")
record = ws.at("s3://bucket/out/")
```

An `s3://` URI needs the optional extra `datarecord[remote]`.
Credentials and the endpoint come from the AWS environment and config files.
Set `AWS_ENDPOINT_URL` to reach a self-hosted server.
The server has to honour conditional writes, as AWS S3 does and SeaweedFS does from 4.18 ([design](../design/storage/store.md#files-local-and-remote)).

## Threads and processes

`ws.con` is the calling thread's DuckDB cursor, so one workspace serves every thread.
A relation belongs to the thread that built it ([design](../design/storage/concurrency.md#a-cursor-per-thread)).

A `Revision` serialises as its id, its parent and its store's location, and pickles the same way.
In a fresh process it loads back onto `Workspace.process()` ([design](../design/storage/concurrency.md#crossing-a-process-boundary)):

```python
payload = rev.model_dump_json()  # in one process
rev = Revision.model_validate_json(payload)  # in another
rev.ancestry()
```
