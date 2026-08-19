<!--
SPDX-FileCopyrightText: datarecord contributors
SPDX-License-Identifier: CC-BY-4.0
-->

<!--
By default, the index will be a copy of your repository README preamble.
You can replace this cross-reference or append/prepend to it by updating this page.
-->

--8<-- "README.md:docs"

Dimensioned attribute data with a declared schema.

A record holds **components** (named members of a type), **connections** between components and buses, **attribute values** over both, and the **axes** those values vary along. A schema declares what may exist; the data says what does.

Records stack: a layer is a partial record on top of a parent, resolved last-writer-wins, so a scenario variant costs the rows it changes rather than a copy of everything. On disk a record is a plain parquet directory that a tool knowing nothing about this package can read.

`datarecord` depends only on `duckdb`, `narwhals` and `pydantic`. It names no modelling framework — a framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
```

```python
from datarecord import DirectoryRecord, connect

con = connect()
record = DirectoryRecord("s3://bucket/my-record/", con)

record.components["Generator"].collect()  # wide member rows
record.attributes["p_max_pu"].collect()  # long value rows
record.flags("Generator")  # which axes each attribute uses
```

## Where to go

<div class="grid cards" markdown>

- :material-book-open-variant: **[Usage](usage/index.md)**

  How to read, edit, layer and write a record, and how a modelling framework
  consumes one.

- :material-drawing: **[Design](design/index.md)**

  What a record is and why it is that way — the authoritative design. The
  docstrings cite these pages rather than restating the argument.

</div>

## Contributing

See the [contributing guide](./contributing.md) for the workflow and conventions,
and [`AGENTS.md`](./AGENTS.md) for how AI-assisted contributions must be marked.

```bash
pixi run test    # the test suite
pixi run lint    # ruff, prettier, taplo, typos, zizmor, reuse, mypy
pixi run docs-serve    # serve this site locally
```
