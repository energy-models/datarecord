<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The record

`RecordLike` is the read-only protocol that every record satisfies: a schema, a DuckDB connection, three mappings of relations and `flags`.
A **reader** is code that reads a record through `RecordLike`, such as a module that translates a record for PyPSA (a [tool](tools.md)), or your script.
`Record`, `WorkingRecord`, `Selection` and a framework's own object all satisfy the protocol, and a reader codes against it alone.
The reasons for one interface over a directory and a tree are on the [about page](../about/one-record.md).

`Record` is the class datarecord returns from `Revision.record` and `Workspace.at(uri)`.
The examples read the record of the running example that the [data model](data-model.md) describes.

## `schema`

`schema` is the record's [schema](schema.md).
A `Record` returns the schema of the deepest revision's data (its last [layer](layers.md)), and a `Selection` returns its base's schema without the dims it squeezed.

## `con`

`con` is the DuckDB connection that every relation of the record is planned on.
A `Record` returns the calling thread's cursor of its workspace, so a relation belongs to the thread that built it ([store](store.md)).
A reader plans its own frames and SQL on `con` beside the record's relations.

## `dims`

`dims` maps every dim the schema declares to a relation of its axis: the label column alone, one row per label, in axis order.
A multi-dim's relation is its one struct column, such as `connection STRUCT(entity, bus)`.
A dim with `one_of` returns its list, and a dim that no layer has labels for returns an empty relation with its declared column.

## `properties`

`properties` maps every subject that declares properties to a relation of the subject's key and one column per property, named by the property's plain name.
The values are as stored, with no default applied, so NULL means that no layer wrote a value.
`Schema.default_for("entity.carrier", **categories)` returns a property's default.

## `attributes`

`attributes` maps every attribute the schema declares to a relation of its long rows: the attribute's coordinates, then `attribute`, `breakpoint` and `value`.
The rows are as written, in no promised order:

- A NULL coordinate stays NULL, and a reader applies [the broadcast rule](data-model.md#the-broadcast-rule) itself, with [`settle`](#settle) or its own SQL.
- Two rows may cover one point, of one layer or of two. Where they do, the record has one row there that leaves no dim NULL that another row there sets.
- A point that no row covers has no row, and `Schema.default_for` gives its default.

An attribute that no layer has rows for returns an empty relation with its declared columns.

## `flags`

**Flags** are what `record.flags(**categories)` returns: per attribute, which dims its rows set and which they leave NULL.

| Field         | Value                                        |
| ------------- | -------------------------------------------- |
| `varies`      | the dims that some row of the attribute sets |
| `broadcast`   | the dims that some row leaves NULL           |
| `breakpoints` | whether any row has a breakpoint             |

`flags()` answers for the whole record in one query.
Only attributes with rows have an entry, so `set(record.flags())` lists the attributes the record has rows for.
Each set has only dims that the attribute is addressed by, multi-dims included, and a dim outside its `dims` is in neither set.

`flags(**categories)` answers for the labels of one category.
Each keyword is a [categorising property](data-model.md#categories) of one dim, by its plain name, with one category or a list.
Only rows that apply to a matching label count: a row that has the label, and a row that leaves its dim NULL.

```python
record.flags(ctype="Generator")["p_max_pu"]
```

```text
Flags(varies=frozenset({'snapshot', 'entity'}), broadcast=frozenset({'snapshot', 'entity'}), breakpoints=False)
```

`snapshot` is in both sets, because `wind` has a row per snapshot and `gas` takes its value at every snapshot from the NULL row.
The flags describe the rows as written, so a NULL row that more specific rows cover at every label still puts its dim in `broadcast`.

`flags` raises `ValueError` when the keywords categorise several dims, or none:

```text
['carrier'] categorize no dim; a categorizing property is a property of a `partial` dim that refers to a dim with `one_of`
```

## Relations

`Relations` is `Mapping[str, DuckDBPyRelation]`, the type of `dims`, `properties` and `attributes`.
A `Record` returns `LazyRelations`, which builds each relation when a reader first looks it up.
Listing the keys reads no file, and building a relation reads the file's footer, which is one round trip on a remote store.

A relation is a query plan, and nothing is read until a reader fetches it with `.fetchall()`, `.df()`, `.pl()` or `.to_arrow_table()`.

## Beyond the protocol

`Record` has members that `RecordLike` does not require.

### `select`

`record.select(**labels)` returns a [selection](selecting.md) of the record, as `select(record, **labels)` does for any `RecordLike`.

### `close`

`close()` releases the tables in which the record's fold is kept, and a `with` block calls it on exit.
The workspace drops a table once no open record and no registration reads it.
Calling `close()` again does nothing, and `closed` is `True` from the first call on.

After `close()`, a read raises `ValueError`, and a relation taken before fails on fetch with DuckDB's missing-table error.
`revision.record` returns a new record once the one it cached is closed.
A record you never close leaves its tables in place for the workspace's life, so a chained read such as `store.revision(rid).record.attributes["p_nom"].df()` succeeds.

### `register`

`record.register(name)` creates a DuckDB view per dim, property subject and attribute, for a SQL client ([format](format.md)).

## `settle`

**To settle rows** is to apply the broadcast rule to long rows with `settle(rel, labels, flags)`, giving one row per label passed at each point.
`settle` expands each NULL on a coordinate in `labels` over the labels passed, and drops a row with a label outside them.
At each point it keeps the row that leaves no coordinate in `labels` NULL that another row there sets.

A coordinate left out of `labels` stays as written, and its NULL counts as one more label:

```python
from datarecord import settle

flags = record.flags(ctype="Generator")
gens = record.properties["entity"].filter("ctype = 'Generator'").select("entity")
rows = settle(record.attributes["p_max_pu"], {"entity": gens}, flags["p_max_pu"])
rows.order("entity, snapshot").fetchall()
```

```text
[('gas', None, 'p_max_pu', None, 1.0), ('wind', 0, 'p_max_pu', None, 0.3),
 ('wind', 1, 'p_max_pu', None, 0.8), ('wind', 2, 'p_max_pu', None, 0.5),
 ('wind', None, 'p_max_pu', None, 1.0)]
```

`dc` is no generator, so its rows are gone.
`snapshot` is not in `labels`, so `gas` keeps one constant row, and `wind` keeps its series beside a row at `snapshot` NULL.
Passing `"snapshot": record.dims["snapshot"]` as well gives one row per generator and snapshot.

`labels` maps each coordinate to a relation of its labels, or of a multi-dim's tuples, in a column of the coordinate's name.
`flags` sets how much work the query does, and flags that overstate the rows give the same result more slowly:

| The coordinate is in     | `settle`                                  |
| ------------------------ | ----------------------------------------- |
| `varies` only            | keeps the rows with a label passed        |
| `broadcast` only         | expands each row over the labels passed   |
| `varies` and `broadcast` | expands, then keeps the most specific row |

`filter=False` keeps a row with a label outside `labels` as written, for a reader whose own join on the labels drops it.
Where every row at a point leaves NULL a coordinate that another row there sets, the query raises `duckdb.Error` on fetch, naming the point.

!!! info "See also"

    - [The data model](data-model.md) defines dims, properties, attributes and the broadcast rule.
    - [Selecting](selecting.md) describes `select` and `Selection`.
    - [Editing](editing.md) describes `WorkingRecord`.
    - [Read a record](../howto/read-a-record.md) shows the steps.
    - The [record API](api/record.md) lists every signature.
