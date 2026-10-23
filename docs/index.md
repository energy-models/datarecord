<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# datarecord

A record holds labeled data with a declared schema. Its labels sit on axes called dims. Groups list which tuples of labels exist, properties describe single labels, and attributes hold values at coordinates. The schema declares what may exist, and the data says what does.

A record can be one parquet directory or a tree of layers, and a reader reads both through one interface. A layer adds a partial record on top of its parent, so a scenario variant stores only the rows it changes. Any parquet reader can open a record's directory.

`datarecord` depends on `duckdb`, `pydantic` and `mathspec`, and names no modelling framework. A framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.

```bash
pip install datarecord           # core
pip install datarecord[pypsa]    # with the PyPSA tool
```

The package does not yet declare `mathspec`, which is not on PyPI. Install it alongside from [GitHub](https://github.com/energy-models/mathspec), at the commit `pixi.toml` pins ([installation](installation.md)).

```python
from datarecord import Workspace

ws = Workspace()
record = ws.at("s3://bucket/my-record/")

record.properties["entity"].df()  # one row per entity, its properties
record.attributes["p_max_pu"].df()  # long value rows
record.flags(ctype="Generator")  # which axes each attribute uses
```

## Where to go

<div class="grid cards" markdown>

- :material-book-open-variant: **[Usage](usage/index.md)**

  How to read, edit, layer and write a record, and how a modelling framework
  consumes one.

- :material-drawing: **[Design](design/index.md)**

  What a record is and why it is that way. The design is authoritative, and
  the docstrings cite it.

- :material-api: **[API Reference](api/index.md)**

  Generated signatures and docstrings for every public symbol.

</div>

## Contributing

See [`CONTRIBUTING.md`](https://github.com/energy-models/datarecord/blob/main/CONTRIBUTING.md)
for the workflow and conventions, and
[`AGENTS.md`](https://github.com/energy-models/datarecord/blob/main/AGENTS.md)
for how AI-assisted contributions must be marked.

```bash
pixi run test    # the test suite
pixi run lint    # ruff, prettier, taplo, typos, zizmor, reuse, mypy
pixi run -e docs docs    # serve this site locally
```
