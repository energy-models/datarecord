<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reading a record

## The `Record` protocol

Everything a consumer codes against. It is read-only, and structural — a plain directory, a hundred-layer overlay, a record with pending edits and a framework's own object all satisfy it, and a consumer cannot tell which it holds ([design](../design/record.md)).

```python
record.schema  # what may exist: the axes, the properties, the attributes
record.dims["entity"]  # axis labels, keyed by dim
record.properties["entity"]  # stored property values, keyed by dim or group
record.groups["connection"]  # group rows, keyed by group — one relation each
record.attributes["p_max_pu"]  # long relations, results included, keyed by attribute
record.flags(ctype="Generator")  # which axes each attribute actually uses
```

Every value is a `duckdb.DuckDBPyRelation` on `record.con` — a plan, not data. Nothing is read until you fetch it, with `.df()`, `.pl()` or `.to_arrow_table()`, and listing the keys reads nothing at all ([design](../design/record.md#relations)).

```python
entities = record.properties["entity"]
gens = entities.filter("ctype = 'Generator'").df()
p_nom = record.attributes["p_nom"].filter("value > 0").df()
```

## Labels and properties

`dims[dim]` is the axis's label column only, in axis order. A dim is flat, so a multi-period snapshot's period is a property beside it ([design](../design/schema.md#a-dim-is-flat)):

```python
record.dims["snapshot"]  # snapshot
record.properties["snapshot"]  # snapshot | period | timestep
```

`properties[subject]` is a dim's or a group's key with every property declared for it, **as stored**: a NULL is a value no layer wrote.

One category's labels are `properties[dim]` filtered on a categorizing property — a property of a `partial` dim naming a closed dim. They carry every property of the dim, and an unset one's default, `default_by` per category included, is `Schema.default_for("entity.sign", **categories)` — a property named by its subject, the categories by their plain names, as the frame's columns are ([design](../design/record.md#properties-and-categories)).

## Wide and long

`dims`, `properties` and `groups` are **wide** — one row per label or tuple. `attributes` are **long** — one row per value:

```text
<coordinate> ... | attribute | breakpoint | value
```

`attributes` holds **every** attribute, whatever its `dims` — `p_nom` over `entity` alone is long rows just like `p_max_pu`. The coordinates are the attribute's own — `entity` for `p_nom`, one `connection` struct column for a connection attribute like `efficiency`, and no entity column at all for one addressed by another axis alone ([design](../design/format.md#the-long-schema)).

A NULL dim column means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep ([design](../design/record.md#the-broadcast-rule)). `entity` broadcasts like any dim; on a categorized dim a NULL reaches only the labels whose categories carry the attribute, so one `p_max_pu` row with `entity = NULL` reaches every generator and no load. A group column broadcasts the same way, a NULL `connection` meaning every connection. `breakpoint` carries the abscissa of a piecewise-linear value.

`attributes` shows stored rows only: a coordinate no row covers takes the attribute's default, which `Schema.default_for(name, **categories)` answers per category.

No property is a column of an attribute row — `attributes["p_max_pu"]` holds every entity's rows together. To scope to one category, join `properties["entity"]`, filtered on the category, on `entity`.

## `flags`

`flags()` answers which axes each attribute's rows actually use, across the record in one query, so a consumer can plan its reads without opening a file. `flags(**categories)` answers for one category of one dim, counting only rows reaching a matching label and only attributes those labels carry:

```python
flags = record.flags(ctype="Generator")
set(flags)  # which attributes generators have at all
"timestep" in flags["p_max_pu"].varies  # some row sets it
"timestep" in flags["p_max_pu"].broadcast  # some row leaves it NULL
flags["marginal_cost"].breakpoints  # some row carries a curve
```

The two sets are **not** complements: a dim in both means the labels disagree — some carry a per-timestep series, others a single constant row — which is the instruction to use both containers, not an ambiguity ([design](../design/record.md#flags)).

## Reading a directory

A parquet directory is a record. Nothing about layers is involved:

```python
from datarecord import Workspace

ws = Workspace()
record = ws.at("s3://bucket/my-record/")
record.attributes["p_nom"].df()
```

The layout is the whole format ([design](../design/format.md)):

```text
record/
├── manifest.json            # the schema, and the record's id
├── dims/<dim>.parquet       # one axis per declared dim: labels and their properties
├── groups/<group>.parquet   # which tuples of the group exist, and their properties
└── attributes/<attr>.parquet  # one attribute per file, whatever its dims, results included
```
