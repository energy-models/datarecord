<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Store a record

This guide shows how to set up a [workspace and its stores](../reference/store.md), write a standalone directory, connect to S3, and read from several threads or processes. The examples use `record`, `rev` and `ws` from [Your first record](../tutorials/first-record.md).

## Set up a workspace and a store

A `Workspace` is a DuckDB database, in memory unless you pass a file. `ws.store(uri)` returns the one store at that location:

```python
import os

from datarecord import Workspace

work = Workspace(os.path.join(tempfile.mkdtemp(), "work.duckdb"))
data = tempfile.mkdtemp()
print(work.store(data) is work.store(data))
```

```text
True
```

A second call returns the same store, and [the database](../reference/store.md#the-database) contains no record's data. Records of stores on one workspace join, because their relations share its database. `work.write` refuses a relation from another workspace:

```python
work.write(record, os.path.join(tempfile.mkdtemp(), "elsewhere"))
```

```text
ValueError: this relation was built on another connection or thread; build it on `record.con` in this thread, or hand over a pandas, polars or arrow frame to land
```

The relations of `record` are planned on `ws`, so `work` cannot read them. To combine records, open their stores on one workspace.

The workspace attaches its database as `datarecord` and never makes it the connection's default, so SQL names its tables and views in full, `datarecord.main.revisions`. `Workspace(con=server)` attaches it on a connection of yours, such as a server's, and leaves that connection's own database as it was; `ws.close()` detaches it and closes only a connection the workspace opened. The prefix `_dr_` is datarecord's: create no table or view whose name starts with it in the default database or temp schema of that connection.

## Write a standalone directory

`ws.write(source, uri)` writes any record as one directory with no parent, and `ws.at(uri)` reads it back. The URI must not exist yet, not even as an empty directory:

```python
uri = os.path.join(tempfile.mkdtemp(), "plants")
written = ws.write(record, uri)
print(sorted(os.listdir(uri)))
ws.write(record, uri)
```

```text
['attributes', 'dimensions', 'manifest.json']
FileExistsError: /private/var/folders/.../plants/ already exists; a standalone record is written to a URI that does not exist yet
```

The second write raises, because `uri` exists. The manifest is written last, so a write that fails leaves no record. Delete the failed write's data files before you write there again. The [format](../reference/format.md) page lists each file.

## Open a record on S3

Install the `remote` extra, which adds `boto3`:

```bash
pip install datarecord[remote]
```

Then pass an `s3://` URI wherever a path goes:

```python
store = ws.store("s3://bucket/my-record")
record = ws.at("s3://bucket/out/")
```

The credentials and the endpoint come from the AWS environment variables and config files. Set `AWS_ENDPOINT_URL` to connect to a self-hosted server, which must honour conditional writes.

## Read from several threads

`ws.con` is the calling thread's DuckDB cursor, so every thread can read one record. Build each relation in the thread that fetches it:

```python
from concurrent.futures import ThreadPoolExecutor


def p_nom(entity):
    return (
        record.attributes["p_nom"].filter(f"entity = '{entity}'").df()["value"].item()
    )


with ThreadPoolExecutor() as pool:
    print(list(pool.map(p_nom, ["wind1", "gas1"])))
```

```text
[100.0, 50.0]
```

Each thread built its own relation and fetched one generator's `p_nom`. A relation or a `WorkingRecord` belongs to the thread that built it, and another thread's write path refuses it.

## Hand a revision to another process

A `Revision` or a `Claim` serialises as its id and its store's location, as JSON or as a pickle:

```python
import pickle

from datarecord import Revision

payload = rev.model_dump_json()
print(payload)
again = Revision.model_validate_json(payload)
print(again.ancestry() == rev.ancestry())
print(pickle.loads(pickle.dumps(rev)).id == rev.id)
```

```text
{"id":"702b30e4-66fd-43b3-96ea-4fdddbca7232","store":"/private/var/folders/.../tmpl4cn2v4a"}
True
True
```

The JSON has the id and the store's location, and the loaded revision has the same ancestry. The loaded revision attaches to `Workspace.process()`, the one workspace a process creates on first use, and datarecord reads its manifests from the store. Build a source for `claim.commit` on that workspace, since a relation from another workspace is refused. Writers never share a file, so several processes can commit to one store at once ([the store](../reference/store.md)).
