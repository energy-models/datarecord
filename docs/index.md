<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# datarecord

datarecord keeps a base dataset and its scenarios in one store, and writes only the rows that each scenario changes.

A modelling study often ends with dozens of variants of one dataset. One adds wind capacity, another sets a carbon price, a third swaps the demand series. If you keep each variant as a full copy, the copies drift apart, and you cannot tell which rows differ without comparing every file. datarecord stores each variant as a layer, which contains only the rows that differ from its parent. So to see what a scenario changes, you read its layer.

## What you get

You build a scenario by editing a parent revision (one saved version in the store) and committing the edits. `store.commit` writes the edits as a new layer under that parent. When you read the scenario, datarecord combines its layers from the base down. Where two layers set the same row, the value from the layer nearer the scenario is the one you read. If many reads start from one revision, you can [materialise](reference/resolution.md#materialised-revisions) it, so that later reads do not combine the layers below it again.

Each record has a schema, which lists the dims (the labels that values vary along) and the properties of each label. It also lists the attributes, with their dtypes and units. datarecord checks every edit against the schema and refuses it before it writes anything. For example, if the schema declares `p_nom` over `entity`, datarecord refuses a value of `p_nom` per `snapshot`. An attribute that the schema does not list raises `KeyError`. So every record you read from disk has the shape that its schema declares.

On disk, each layer is a directory of parquet files (a common columnar file format). You can open these files with any parquet library, such as pandas or polars, without datarecord installed. However, to read a scenario combined across its layers, you need datarecord. In Python, every read returns a DuckDB relation (a query that has not run yet). DuckDB reads the files only when you call `.df()`, `.pl()` or `.to_arrow_table()`. The calls are the same whether the store is on a local disk or on S3.

datarecord does not depend on any modelling framework. It needs only `duckdb`, `pydantic` and `mathspec`. A framework such as PyPSA reads a record through the [`RecordLike`](reference/record.md) protocol, and a workflow engine writes one. So neither needs to know how the other works. The [PyPSA tool](howto/use-the-pypsa-tool.md), which translates between a record and a PyPSA network, installs as an extra.

## Example

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

`high` reads as the whole record, with `wind1` at 150 and `gas1` unchanged. Its layer on disk contains one row. `base` still reads `wind1` at 100. The [scenario tutorial](tutorials/scenario.md) builds this from an empty store in about ten minutes.

## Install

```bash
pip install datarecord              # core
pip install "datarecord[pypsa]"     # with the PyPSA tool
pip install "datarecord[remote]"    # with records on S3
```

datarecord depends on mathspec at a pinned git commit, which PyPI does not accept, so a wheel from PyPI needs mathspec installed beside it. [Installation](installation.md) shows how, and lists other package managers.

## Where to go

<div class="grid cards" markdown>

- :material-school: **[Tutorials](tutorials/index.md)**

  Build a first record, branch a scenario and solve a record with PyPSA, one
  step at a time.

- :material-format-list-checks: **[How-to guides](howto/index.md)**

  Finish one task: declare a schema, read, select, edit, branch, store, or
  hand a record to PyPSA.

- :material-book-open-variant: **[Reference](reference/index.md)**

  What datarecord accepts, returns and refuses, and the format on disk.

- :material-lightbulb-on-outline: **[About](about/index.md)**

  Why the design is the way it is, what it rejected, and what it leaves open.

</div>

To change datarecord itself, start at [Development](development/index.md).
