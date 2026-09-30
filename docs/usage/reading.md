<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reading a record

## The `Record` protocol

Everything a consumer codes against. It is read-only, and structural — a plain directory, a hundred-layer overlay, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds ([design](../design/record.md)).

```python
record.schema  # what may exist: the axes, the attributes
record.dims["scenario"]  # axis frames, keyed by dim
record.relations["port_bus"]  # relation rows, keyed by relation — one frame each
record.attributes["p_max_pu"]  # long frames, keyed by attribute
record.flags(["wind1", "wind2"])  # which axes each attribute uses, over these entities
```

Every frame is a `narwhals.LazyFrame` — a plan, not data. Nothing is read until you `.collect()`, and listing the keys reads nothing at all ([design](../design/record.md#frames)).

```python
entities = record.dims["entity"].collect().to_pandas()
```

## Wide and long

`dims` and `relations` are **wide** — one row per thing. `attributes` is **long** — one row per value:

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are the attribute's own, from its declared `dims` — `entity | scenario | timestep` for `p_max_pu`, `port | timestep` for `efficiency`, and no entity column at all for an attribute that is not over `entity` ([design](../design/format.md#the-long-schema)).

A NULL dim column means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep ([design](../design/record.md#the-broadcast-rule)). Every dim an attribute is over broadcasts this way, `entity` and `port` included. `breakpoint` carries the abscissa of a piecewise-linear value. A coordinate no row covers takes the attribute's `default` from the schema.

There is no `entity_type` column in an attribute's key or in a relation's — `attributes["p_max_pu"]` and `relations["port_entity"]` each hold the rows of every type together. An entity's type is a row of the `entity_type` relation, so the type is something the record knows about a name rather than part of its address ([design](../design/schema.md#types)). To scope to one type, join `relations["entity_type"]` on `entity`:

```python
import narwhals as nw

types = record.relations["entity_type"]
generators = record.dims["entity"].join(
    types.filter(nw.col("entity_type") == "Generator"), on="entity"
)
names = generators.collect()["entity"].to_list()
```

## `flags`

`flags(entities)` answers which axes an attribute's rows actually use, over the named entities in one query, so a consumer can plan its reads without opening a file. `flags()` answers for every entity. It takes a sequence of names; a bare string raises `TypeError`. For one type, pass that type's names:

```python
flags = record.flags(names)  # the generators, from above
set(flags)  # which attributes these entities have at all
"timestep" in flags["p_max_pu"].varies  # some row sets it
"timestep" in flags["p_max_pu"].broadcast  # some row leaves it NULL
flags["marginal_cost"].breakpoints  # some row carries a curve
```

The two sets are **not** complements: a dim in both means the named entities disagree — some carry a per-timestep series, others a single constant row — which is the instruction to use both containers, not an ambiguity ([design](../design/record.md#flags)).

## Reading a directory

A parquet directory is a record. Nothing about layers is involved:

```python
from datarecord import Record, connect

con = connect()
record = Record.at("s3://bucket/my-record/", con)
record.attributes["p_max_pu"].collect()
```

The layout is the whole format ([design](../design/format.md)):

```text
record/
├── manifest.json                   # the schema
├── dims/
│   ├── entity.parquet              # which entities exist
│   └── <dim>.parquet               # one axis table per declared dim
├── relations/<relation>.parquet    # which tuples of the relation exist
└── attributes/<attr>.parquet       # one attribute per file
```
