<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Usage

How to use the package. [Design](../design/index.md) is what it is and why;

## Installation

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
```

Or with conda/mamba, from conda-forge:

```bash
conda install -c conda-forge datarecord
```

## Connections

Every entry point takes a DuckDB connection. It is passed as a parameter
throughout, never a module global, and is scoped to one record root — which is
how [the schema beside its layers](../design/schema.md#one-schema-per-record)
is found without a separate argument.

```python
from datarecord import connect

con = connect(":memory:", base_uri="s3://bucket/my-record")
```

`connect` opens a connection carrying the `revisions` table and the path macros,
loading `httpfs` and S3 credentials only for a remote `base_uri`. `base_uri`
defaults to the `DATARECORD_BASE_URI` environment variable.

## The pages

| page                           | what it covers                                |
| ------------------------------ | --------------------------------------------- |
| [Reading a record](reading.md) | the `Record` protocol, frames, `flags`        |
| [The schema](schema.md)        | declaring dims and attributes                 |
| [Layers](layers.md)            | revisions, branching, materialising           |
| [Editing](editing.md)          | `WorkingRecord`: `set`, `add`, `commit`       |
| [Writing](writing.md)          | `write_record`                                |
| [Tools](tools.md)              | consuming a record from a modelling framework |
