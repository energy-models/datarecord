<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# datarecord

[![docs](https://img.shields.io/badge/docs-energy--models.github.io-blue?style=flat-square&logo=materialformkdocs&logoColor=white)](https://energy-models.github.io/datarecord/)
[![CI](https://img.shields.io/github/actions/workflow/status/energy-models/datarecord/ci.yml?style=flat-square&branch=main)](https://github.com/energy-models/datarecord/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/datarecord?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/datarecord)
[![pypi-version](https://img.shields.io/pypi/v/datarecord.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/datarecord)
[![python-version](https://img.shields.io/pypi/pyversions/datarecord?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/datarecord)

datarecord keeps a base dataset and its scenarios in one store, and writes only the rows that each scenario changes.

datarecord stores each scenario as a layer, which contains only the rows that differ from its parent. So to see what a scenario changes, you read its layer. When you read the scenario, datarecord combines its layers from the base down.

Each record has a schema, which lists the dims (the labels that values vary along) and the properties of each label. It also lists the attributes, with their dtypes and units. datarecord checks every edit against the schema and refuses it before it writes anything.

On disk, each layer is a directory of parquet files. You can open these files with any parquet library, such as pandas or polars, without datarecord installed.

datarecord depends only on `duckdb`, `pydantic` and `mathspec`, and not on any modelling framework. A framework such as PyPSA reads a record, a workflow engine writes one, and neither needs to know how the other works.

## Documentation

Full documentation is at **<https://energy-models.github.io/datarecord/>**:

- [Tutorials](https://energy-models.github.io/datarecord/tutorials/): your first record, a scenario on a branch, a record solved with PyPSA
- [How-to guides](https://energy-models.github.io/datarecord/howto/): declaring a schema, reading, selecting, editing, branching and storing a record
- [Reference](https://energy-models.github.io/datarecord/reference/): the data model, the schema, layers, resolution, the format and the [Python API](https://energy-models.github.io/datarecord/reference/api/)
- [About](https://energy-models.github.io/datarecord/about/): why a record is built the way it is

## Installation

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
pip install datarecord[remote]   # with records on S3
```

The package does not yet declare `mathspec`, which is not on PyPI: install it alongside from [GitHub](https://github.com/energy-models/mathspec), at the commit `pixi.toml` pins.

## Example

```python
from datarecord import Workspace

ws = Workspace()
record = ws.at("s3://bucket/my-record/")

record.properties["entity"].df()  # each entity's stored properties
record.attributes["p_max_pu"].df()  # long value rows, one per value
record.flags(ctype="Generator")  # which axes each attribute uses
```

Every read returns a DuckDB relation (a query that has not run yet). DuckDB reads the files only when you call `.df()`, `.pl()` or `.to_arrow_table()`.

To branch a scenario, you stage edits on a `WorkingRecord`, and `store.commit` writes them as one child layer:

```python
from datarecord import WorkingRecord

w = WorkingRecord(revision.record)
w.set("p_nom", 150.0, entity=["wind1", "wind2"])
child = store.commit(w)  # a child of `revision`, with the edits
```

See the [how-to guides](https://energy-models.github.io/datarecord/howto/) for the rest.

## Development

This project is managed by [pixi](https://pixi.sh):

<!-- --8<-- [start:docs-install-dev] -->

```bash
git clone https://github.com/energy-models/datarecord
cd datarecord

pixi run test    # the test suite
pixi run lint    # ruff, prettier, taplo, typos, zizmor, reuse, mypy
```

<!-- --8<-- [end:docs-install-dev] -->

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow and conventions, and [`AGENTS.md`](AGENTS.md) for how AI-assisted contributions must be marked.
