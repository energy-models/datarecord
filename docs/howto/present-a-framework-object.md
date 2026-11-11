<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Present a framework object as a record

This guide shows how to wrap a framework's own object so that any reader of a record reads it, and how to commit it. It is for a tool author, who knows the framework. [`RecordLike`](../reference/record.md) is the protocol the wrapper satisfies, and the [data model](../reference/data-model.md) defines the shapes below.

## Implement the six members

`RecordLike` is a structural protocol, so a class with its six members satisfies it without importing anything from datarecord. The members are `schema`, `con`, `dimensions`, `properties`, `attributes` and `flags`, and [the record](../reference/record.md) states what each returns.

A mapping may be a plain `dict`. A dimension with `one_of` needs no entry in `dimensions`, because its axis is the schema's list.

## Write the wrapper

This framework's object has a table of units and a table of hourly availability. The wrapper declares a schema for them and renames their columns to the record's:

```python
from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd
from duckdb import DuckDBPyConnection

from datarecord import AttributeSpec, Dimension, Flags, PropertySpec, Relations, Schema
from datarecord.resolve.fold import Fold

SCHEMA = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "ctype": Dimension(one_of=["Generator", "Load"]),
        "snapshot": Dimension(dtype="int"),
    },
    properties={"entity": {"ctype": PropertySpec(refers="ctype")}},
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}, default=0.0),
        "p_max_pu": AttributeSpec(
            dtype="float", dims={"entity", "snapshot"}, default=1.0
        ),
    },
    partial={"entity"},
)


class Fleet:
    def __init__(self) -> None:
        self.units = pd.DataFrame(
            {
                "name": ["wind1", "gas1"],
                "kind": ["Generator", "Generator"],
                "capacity": [100.0, 50.0],
            }
        )
        self.availability = pd.DataFrame(
            {"name": ["wind1"] * 3, "hour": [0, 1, 2], "pu": [0.3, 0.8, 0.5]}
        )


@dataclass(frozen=True)
class FleetRecord:
    fleet: Fleet
    con: DuckDBPyConnection
    schema: Schema = SCHEMA

    @property
    def dimensions(self) -> Relations:
        units = self.con.from_df(self.fleet.units)
        hours = self.con.from_df(self.fleet.availability)
        return {
            "entity": units.project("name AS entity"),
            "snapshot": hours.project("hour::BIGINT AS snapshot")
            .distinct()
            .order("snapshot"),
        }

    @property
    def properties(self) -> Relations:
        units = self.con.from_df(self.fleet.units)
        return {"entity": units.project("name AS entity, kind AS ctype")}

    @property
    def attributes(self) -> Relations:
        units = self.con.from_df(self.fleet.units)
        hours = self.con.from_df(self.fleet.availability)
        return {
            "p_nom": units.project(
                "name AS entity, 'p_nom' AS attribute, NULL::DOUBLE AS breakpoint, capacity AS value"
            ),
            "p_max_pu": hours.project(
                "name AS entity, hour::BIGINT AS snapshot, 'p_max_pu' AS attribute,"
                " NULL::DOUBLE AS breakpoint, pu AS value"
            ),
        }

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        return Fold.as_one_layer(self).flags(**categories)
```

`Fold.as_one_layer` reads the wrapper's rows as one layer and answers `flags` as a record written from them would. The PyPSA tool answers `flags` the same way.

## Check the wrapper reads as a record

```python
from datarecord import RecordLike, Workspace, select

ws = Workspace()
record = FleetRecord(Fleet(), ws.con)
print(isinstance(record, RecordLike))
print(record.flags(ctype="Generator")["p_max_pu"])
print(select(record, snapshot=1).attributes["p_max_pu"].df())
```

```text
True
Flags(varies=frozenset({'entity', 'snapshot'}), broadcast=frozenset(), breakpoints=False)
  entity attribute  breakpoint  value
0  wind1  p_max_pu         NaN    0.8
```

`p_max_pu` varies along both dimensions, and the selection at snapshot 1 returns `wind1`'s 0.8. `isinstance` checks only that the members exist. The `select` call is the test that the relations have the shapes the reference states.

## Commit the wrapper

`store.commit` and `ws.write` take the wrapper as they take any record, and validate every row as they write it:

```python
import tempfile

store = ws.store(tempfile.mkdtemp())
root = store.commit(record, parent=None)
print(root.record.properties["entity"].df())
```

```text
  entity      ctype
0  wind1  Generator
1   gas1  Generator
```

The committed root has both units as generators. A `WorkingRecord` stages edits over a committed `Record` alone, so commit the wrapper before you edit it:

```python
from datarecord import WorkingRecord

WorkingRecord(record)
```

```text
TypeError: a `WorkingRecord` folds its staged rows over a `Record`'s layers, and a FleetRecord has none: write it first, `WorkingRecord(ws.write(source, uri))`
```

`WorkingRecord(root.record)` stages edits over the committed wrapper.

## Build relations on demand

Where building a relation costs a read, as for a file on a remote store, return a `LazyRelations` instead of a `dict`. It lists its keys at once and builds a relation when a reader looks one up:

```python
from datarecord import LazyRelations

lazy = LazyRelations(("p_nom", "p_max_pu"), lambda name: record.attributes[name])
print(list(lazy), "p_nom" in lazy)
```

```text
['p_nom', 'p_max_pu'] True
```

Listing the keys and testing for one builds no relation.
