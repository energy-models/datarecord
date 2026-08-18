<!--
SPDX-FileCopyrightText: datarecord contributors
SPDX-License-Identifier: CC-BY-4.0
-->

<!--- --8<-- [start:docs-preamble] -->

# datarecord

[![docs](https://img.shields.io/badge/docs-energy--models.github.io-blue?style=flat-square&logo=materialformkdocs&logoColor=white)](https://energy-models.github.io/datarecord/)
[![CI](https://img.shields.io/github/actions/workflow/status/energy-models/datarecord/ci.yml?style=flat-square&branch=main)](https://github.com/energy-models/datarecord/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/datarecord?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/datarecord)
[![pypi-version](https://img.shields.io/pypi/v/datarecord.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/datarecord)
[![python-version](https://img.shields.io/pypi/pyversions/datarecord?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/datarecord)
[![Documentation build status](https://readthedocs.org/projects/datarecord/badge/?version=latest)](https://datarecord.readthedocs.io)

Dimensioned attribute data with a declared schema.

A record holds **components** (named members of a type), **connections** between components and buses, **attribute values** over both, and the **axes** those values vary along. A schema declares what may exist; the data says what does.

Records stack: a layer is a partial record on top of a parent, resolved last-writer-wins, so a scenario variant costs the rows it changes rather than a copy of everything. On disk a record is a plain parquet directory that a tool knowing nothing about this package can read.

`datarecord` depends only on `duckdb`, `narwhals` and `pydantic`. It names no modelling framework — a framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.

## Installation

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
```

## A taste

```python
from datarecord import DirectoryRecord, connect

con = connect()
record = DirectoryRecord("s3://bucket/my-record/", con)

record.components["Generator"].collect()  # wide member rows
record.attributes["p_max_pu"].collect()  # long value rows, one per value
record.flags("Generator")  # which axes each attribute uses
```

Every frame is a `narwhals.LazyFrame` — a plan, not data. Nothing is read until you `.collect()`.

Records stack, and a `WorkingRecord` accumulates edits that become one layer at commit:

```python
from datarecord import WorkingRecord, NewChild

w = WorkingRecord(revision.record, con)
w.set("p_nom", 150.0, names=["wind1", "wind2"])
child = w.commit(NewChild())
```

See [Usage](https://energy-models.github.io/datarecord/usage/) for the rest.

<!--- --8<-- [end:docs-preamble] -->
<!--- --8<-- [start:docs-postamble] -->

## Development

This project is managed by [pixi](https://pixi.prefix.dev/):

<!--- --8<-- [start:docs-install-dev] -->

```bash
git clone https://github.com/energy-models/datarecord
cd datarecord

pixi run test    # the test suite
pixi run lint    # ruff, prettier, taplo, typos, zizmor, reuse, mypy
pixi run docs-serve    # serve this site locally
```

<!--- --8<-- [end:docs-install-dev] -->

See the [contributing guide](./contributing.md) for the workflow and conventions,
and [`AGENTS.md`](./AGENTS.md) for how AI-assisted contributions must be marked.

<!--- --8<-- [end:docs-postamble] -->

## Documentation

Full documentation is at **<https://energy-models.github.io/datarecord/>**:

- [Usage](https://energy-models.github.io/datarecord/usage/) — reading, editing, layering and writing a record
- [Design](https://energy-models.github.io/datarecord/design/) — what a record is and why, the authoritative design
- [API Reference](https://energy-models.github.io/datarecord/api/) — every public symbol
