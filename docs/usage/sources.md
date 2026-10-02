<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tables in and out

This page shows how to fill a record from a framework's data and solve it with specsolve. A record meets both through tables keyed by the names its schema declares ([design](../design/sources.md), [API](../api/sources.md)).

| declared as | its table                             |
| ----------- | ------------------------------------- |
| a dimension | its labels, one column named after it |
| a relation  | its rows, one column per coordinate   |
| a parameter | its coordinates and `value`           |

A schema built with `Schema.from_mathspec` carries the names of a mathspec declarations file, so these are the names and the shapes that specsolve's `solve(spec, sources)` takes.

## Import

A converter outside datarecord produces the tables from a framework's own object. For PyPSA, `sources(n)` in specsolve's `differential/pypsa/prep.py` produces them for mathspec's `pypsa.yaml`. `from_sources` reads them as a record, and `write_record` writes it as a layer:

```python
from datarecord import Revision, write_record
from datarecord.sources import from_sources

revision = Revision.create(con)
write_record(revision.id, from_sources(schema, tables), con)
```

A parameter table may leave out a dimension it broadcasts over: a constant over every snapshot has no `snapshot` column. A name the schema does not declare raises `KeyError`.

## Solve

```python
import specsolve
from datarecord.sources import to_sources

sources = {name: frame.collect() for name, frame in to_sources(record).items()}
result = specsolve.solve(spec, sources)
```

`to_sources` returns lazy frames, and `collect` makes each one a table that specsolve reads. A value stored once for every label comes out as one row per label, and a coordinate with no value has no row. A parameter that no layer wrote has no table, and specsolve refuses a spec that reads it.

`record` may be a `WorkingRecord`, so a solve reads pending edits before they are committed.

## Answers

A solve's answers are stored as a record of their own, whose schema the producer defines. That record is linked to the input record by the revision id of the input node ([design](../design/sources.md#answers)).

## Framework objects

datarecord does not build a framework's own object, such as a `pypsa.Network`, from a record. A converter outside datarecord does that from `to_sources(record)`.
