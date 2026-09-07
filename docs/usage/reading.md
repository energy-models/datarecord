# Reading a record

## The `Record` protocol

Everything a consumer codes against. It is read-only, and structural — a plain directory, a hundred-layer overlay, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds ([design](../design/record.md)).

```python
record.schema  # what may exist: the axes, the attributes
record.dims["scenario"]  # axis frames, keyed by dim
record.components["Generator"]  # wide member rows, keyed by component type
record.groups["connection"]["Link"]  # group rows, keyed by group then type
record.attributes["p_max_pu"]  # long input frames, keyed by attribute
record.outputs["p"]  # long result frames, keyed by attribute
record.flags("Generator")  # which axes each attribute actually uses
```

Every frame is a `narwhals.LazyFrame` — a plan, not data. Nothing is read until you `.collect()`, and listing the keys reads nothing at all ([design](../design/record.md#frames)).

```python
gens = record.components["Generator"].collect().to_pandas()
```

## Wide and long

`components`, `groups` and `dims` are **wide** — one row per thing. `attributes` and `outputs` are **long** — one row per value:

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are the attribute's own, from its declared `dims` — `entity` for `p_max_pu`, `entity | bus` for a connection attribute like `efficiency`, and no entity column at all for one addressed by an axis alone ([design](../design/format.md#the-long-schema)).

A NULL dim column means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep ([design](../design/record.md#the-broadcast-rule)). Two coordinates are the exception and never broadcast — `entity`, and a group's coordinate such as `bus`, where a NULL means "every connection of this entity" rather than every bus. `breakpoint` carries the abscissa of a piecewise-linear value. A coordinate no row covers takes the attribute's `default` from the schema.

There is no `component_type` column, and none in the mapping's key either — `attributes["p_max_pu"]` holds every type's rows together. An `entity` identifies one component **across every type**, so the type is something the record knows about a name rather than part of its address ([design](../design/format.md#entity-is-unique-across-types)). To scope to one type, join `components` on `entity`.

## `flags`

`flags(ctype)` answers which axes an attribute's rows actually use, for a whole type in one query, so a consumer can plan its reads without opening a file:

```python
flags = record.flags("Generator")
set(flags)  # which attributes this type has at all
"timestep" in flags["p_max_pu"].varies  # some row sets it
"timestep" in flags["p_max_pu"].broadcast  # some row leaves it NULL
flags["marginal_cost"].breakpoints  # some row carries a curve
```

The two sets are **not** complements: a dim in both means this type's components disagree — some carry a per-timestep series, others a single constant row — which is the instruction to use both containers, not an ambiguity ([design](../design/record.md#flags)).

## Reading a directory

A parquet directory is a record. Nothing about layers is involved:

```python
from datarecord import DirectoryRecord, connect

con = connect()
record = DirectoryRecord("s3://bucket/my-record/", con)
record.attributes["p_nom"].collect()
```

The layout is the whole format ([design](../design/format.md)):

```text
record/
├── manifest.json                   # the schema
├── dims/
│   ├── components/<Type>.parquet   # members + non-varying attribute columns
│   ├── <group>/<Type>.parquet      # which tuples of the group exist
│   └── <dim>.parquet               # one axis table per declared dim
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```
