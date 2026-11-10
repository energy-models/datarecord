<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# datarecord

[![docs](https://img.shields.io/badge/docs-energy--models.github.io-blue?style=flat-square&logo=materialformkdocs&logoColor=white)](https://energy-models.github.io/datarecord/)
[![CI](https://img.shields.io/github/actions/workflow/status/energy-models/datarecord/ci.yml?style=flat-square&branch=main)](https://github.com/energy-models/datarecord/actions/workflows/ci.yml)
[![pypi-version](https://img.shields.io/pypi/v/datarecord.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/datarecord)
[![python-version](https://img.shields.io/pypi/pyversions/datarecord?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/datarecord)

<!-- --8<-- [start:docs-pitch] -->

datarecord is a Python package that stores a model's input data, and every scenario built on it, as one versioned dataset. It is for modellers who run a framework such as PyPSA over many variants of one dataset. With datarecord you keep every variant, see how each differs from its base, and read any of them back as one dataset.

<!-- --8<-- [end:docs-pitch] -->

<!-- --8<-- [start:docs-intro] -->

A modelling study often ends with dozens of variants of one dataset. One adds wind capacity, another sets a carbon price, a third swaps the demand series. Kept as full copies, the variants drift apart, and you cannot tell which rows differ without comparing every file. datarecord stores each variant as a layer that contains only the rows that differ from its parent, so a scenario costs the rows it changes. To see what a scenario changes, you read its layer.

## What you get

You build a scenario by editing a parent revision (one saved version in the store) and committing the edits. `store.commit` writes the edits as a new layer under that parent. When you read the scenario, datarecord combines its layers from the base down. Where two layers set the same row, the value from the layer nearer the scenario is the one you read. If many reads start from one revision, you can [materialise](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions) it, so that later reads do not combine the layers below it again.

Each record has a schema, which lists the dims (the labels that values vary along) and the properties of each label. It also lists the attributes, with their dtypes and units. datarecord checks every edit against the schema and refuses it before it writes anything. For example, if the schema declares `p_nom` over `entity`, datarecord refuses a value of `p_nom` per `snapshot`. An attribute that the schema does not list raises `KeyError`. So every record you read from disk has the shape that its schema declares.

On disk, each layer is a directory of parquet files (a common columnar file format). You can open these files with any parquet library, such as pandas or polars, without datarecord installed. However, to read a scenario combined across its layers, you need datarecord. In Python, every read returns a DuckDB relation (a query that has not run yet). DuckDB reads the files only when you call `.df()`, `.pl()` or `.to_arrow_table()`. The calls are the same whether the store is on a local disk or on S3.

datarecord does not depend on any modelling framework. It needs only `duckdb`, `pydantic` and `mathspec`. A framework such as PyPSA reads a record through the [`RecordLike`](https://energy-models.github.io/datarecord/reference/record/) protocol, and a workflow engine writes one. So neither needs to know how the other works. The [PyPSA tool](https://energy-models.github.io/datarecord/howto/use-the-pypsa-tool/), which translates between a record and a PyPSA network, installs as an extra.

<!-- --8<-- [end:docs-intro] -->

## Documentation

Full documentation is at **<https://energy-models.github.io/datarecord/>**:

- [Tutorials](https://energy-models.github.io/datarecord/tutorials/): your first record, a scenario on a branch, a record solved with PyPSA
- [How-to guides](https://energy-models.github.io/datarecord/howto/): declaring a schema, reading, selecting, editing, branching and storing a record
- [Reference](https://energy-models.github.io/datarecord/reference/): the data model, the schema, layers, resolution, the format and the [Python API](https://energy-models.github.io/datarecord/reference/api/)
- [About](https://energy-models.github.io/datarecord/about/): why a record is built the way it is

## Installation

<!-- --8<-- [start:docs-install] -->

```bash
pip install "datarecord @ git+https://github.com/energy-models/datarecord"          # core
pip install "datarecord[pypsa] @ git+https://github.com/energy-models/datarecord"   # with the PyPSA tool
pip install "datarecord[remote] @ git+https://github.com/energy-models/datarecord"  # with records on S3
```

The package on PyPI, `0.1.0a1`, predates the design these pages describe, so install from GitHub until the next release. The install brings `mathspec` with it, at the commit that `pyproject.toml` pins.

<!-- --8<-- [end:docs-install] -->

[Installation](https://energy-models.github.io/datarecord/installation/) lists other package managers.

## Example

<!-- --8<-- [start:docs-example] -->

This example branches a scenario `high` off a committed revision `base`, raises the capacity of `wind1` from 100 to 150 and reads both back:

```python
from datarecord import WorkingRecord

w = WorkingRecord(base.record)
w.set("p_nom", 150.0, entity="wind1")
high = store.commit(w, meta={"scenario": "high"})

high.record.attributes["p_nom"].df()
#   entity attribute  breakpoint  value
# 0  wind1     p_nom         NaN  150.0
# 1   gas1     p_nom         NaN   50.0

store.layer(high.id).attributes["p_nom"].df()
#   entity attribute  breakpoint  value
# 0  wind1     p_nom         NaN  150.0
```

`high` reads as the whole record, with `wind1` at 150 and `gas1` unchanged. Its layer on disk contains one row. `base` still reads `wind1` at 100.

<!-- --8<-- [end:docs-example] -->

The [scenario tutorial](https://energy-models.github.io/datarecord/tutorials/scenario/) builds this from an empty store in about ten minutes.

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
