<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Read a record

This guide shows how to read a record's labels, properties and attributes, settle its rows, and read it from a directory or from SQL. You need a record: the examples use `record`, `store`, `rev` and `ws` from [Your first record](../tutorials/first-record.md). Every read goes through [`RecordLike`](../reference/record.md), so the same calls work on a revision, a directory, a `WorkingRecord`, a selection or a framework's own object.

Every value is a DuckDB relation, a query plan that DuckDB runs only when you call `.df()`, `.pl()` or `.to_arrow_table()`.

## Read labels and properties

`dims[dim]` returns a dim's axis in axis order, and `properties[dim]` returns its labels with every property, one row per label. To read one category, filter on the property:

```python
print(record.dims["snapshot"].df())
print(record.properties["entity"].filter("ctype = 'Generator'").df())
```

```text
   snapshot
0         0
1         1
2         2
  entity      ctype carrier
0  wind1  Generator    wind
1   gas1  Generator     gas
```

The axis lists the three snapshots in order, and the filter keeps both generators. A multi-dim's axis is one struct column, such as `connection`, with a field per coordinate.

## Read attributes long

`attributes[name]` returns the stored rows, one per value, in no promised order. A row has the attribute's own coordinates, then `attribute`, `breakpoint` and `value`:

```python
print(record.attributes["p_max_pu"].filter("entity = 'wind1'").df())
```

```text
  entity  snapshot attribute  breakpoint  value
0  wind1         2  p_max_pu         NaN    0.5
1  wind1         1  p_max_pu         NaN    0.8
2  wind1         0  p_max_pu         NaN    0.3
```

Each row is one value of `wind1`'s series. A NULL coordinate means every label of its dim ([the broadcast rule](../reference/data-model.md)). An attribute row has no property column, so join `properties` on the key to filter by category.

## Plan a read with `flags`

`flags(**categories)` returns, per attribute, the dims some row sets (`varies`) and the dims some row leaves NULL (`broadcast`), counting only rows for the category:

```python
flags = record.flags(ctype="Generator")
print(flags["p_max_pu"])
```

```text
Flags(varies=frozenset({'entity', 'snapshot'}), broadcast=frozenset({'snapshot'}), breakpoints=False)
```

`snapshot` is in both sets, so some generators have a series and others a constant. A dim in `varies` alone has a label in every row.

## Apply the broadcast rule

To get one row per label at each point, pass `settle` the rows, the labels of each coordinate to expand, and the attribute's flags:

```python
from datarecord import settle

gens = record.properties["entity"].filter("ctype = 'Generator'").select("entity")
labels = {"entity": gens, "snapshot": record.dims["snapshot"]}
print(settle(record.attributes["p_max_pu"], labels, flags["p_max_pu"]).df())
```

```text
  entity  snapshot attribute  breakpoint  value
0   gas1         2  p_max_pu         NaN    0.9
1   gas1         1  p_max_pu         NaN    0.9
2   gas1         0  p_max_pu         NaN    0.9
3  wind1         1  p_max_pu         NaN    0.8
4  wind1         0  p_max_pu         NaN    0.3
5  wind1         2  p_max_pu         NaN    0.5
```

`settle` expands `gas1`'s NULL `snapshot` over the three snapshots and drops rows of labels you did not pass. A coordinate you leave out of `labels` stays as written. [The record reference](../reference/record.md) lists what each flag costs.

A point that no row covers takes the attribute's default. `Schema.default_for` gives it per category:

```python
print(record.schema.default_for("p_max_pu", ctype="Generator"))
```

```text
1.0
```

A generator with no `p_max_pu` row has a value of 1.0.

## Read a directory

`ws.write` writes any record as a standalone directory, and `ws.at` reads one:

```python
import os

uri = os.path.join(tempfile.mkdtemp(), "copy")
ws.write(record, uri)
print(sorted(os.listdir(uri)))
print(ws.at(uri).attributes["p_nom"].df())
```

```text
['attributes', 'dims', 'manifest.json']
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  100.0
1   gas1     p_nom         NaN   50.0
```

The directory contains a manifest and a folder per kind of file, and `ws.at` returns the same values. It takes an `s3://` URI too ([Store a record](store-a-record.md)). The [format](../reference/format.md) page lists each file.

## Read from SQL

`register` creates a DuckDB schema with a view per dim, property subject and attribute, so a client that speaks only SQL reads the record:

```python
record.register("base")
print(ws.con.sql("SELECT entity, value FROM base.attributes.p_nom").df())
print(ws.registered().project("name, working").df())
ws.unregister("base")
```

```text
  entity  value
0   gas1   50.0
1  wind1  100.0
   name  working
0  base    False
```

The query returns the stored `p_nom` rows, and `registered()` lists the registration. The views contain the rows as stored, so a SQL client applies the broadcast rule itself. A view over a `WorkingRecord` returns every edit as you stage it.

## Close a record

datarecord keeps a record's fold in workspace tables until you close the record. Use a `with` block, and fetch what you need inside it:

```python
with store.revision(rev.id).record as record:
    p_nom = record.attributes["p_nom"].df()
record.attributes["p_nom"]
```

```text
ValueError: this `Record` is closed and its folds released; read the revision's `record` again (https://energy-models.github.io/datarecord/reference/store/#the-database)
```

`p_nom` contains the values, and any later read of the closed record raises `ValueError`.
