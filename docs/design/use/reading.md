<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Reading

`RecordLike` is the protocol that a reader codes against, and a framework object can satisfy it structurally.
`Record` is the class that implements it.
A record hands back DuckDB relations for dims, groups, properties and attributes, built lazily so that listing them costs no I/O.
`flags` reports which axes an attribute's rows set or leave NULL, so a consumer can plan containers before fetching anything.
DuckDB is the one engine from fold to consumer.

```python
@runtime_checkable
class RecordLike(Protocol):
    """Dimensioned attribute data with a declared schema."""

    @property
    def schema(self) -> Schema: ...  # dims, properties, attributes, partial

    @property
    def dims(self) -> Relations: ...  # axis key columns, keyed by dim
    @property
    def properties(self) -> Relations: ...  # stored property values, keyed by subject
    @property
    def groups(self) -> Relations: ...  # each group's rows, keyed by group
    @property
    def attributes(self) -> Relations: ...  # long relations, keyed by attribute

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]: ...
```

A signature annotates against `RecordLike`, and a framework object satisfies it without depending on this package.
`Record` is the interface over [one fold](../layers/resolution.md#one-record-over-one-fold), and `Revision.record` and `Workspace.at(uri)` both return one.
The class has the short name because a caller constructs and holds it.
The protocol is read-only.
[Writing](writing.md) goes through `store.root(source)`, `rev.child(source)` or `ws.write(source, uri)`, and [the record format](../storage/format.md) is how a record is stored.

Each accessor holds one section of [the schema](../model/schema.md): `dims` the labels, `properties` the metadata of those labels, `groups` the tuples that exist, and `attributes` the data.
What a name is declared as decides where its values are read.

## Properties and categories

A [property](../model/properties.md#properties) holds one value per label of its subject, a dim or a group, as a column of the subject's file.

`properties[subject]` is the subject's key with every property declared for it, as stored.
It is a record-wide view, NULL wherever no layer wrote a value.
It has a key for each subject that declares properties, and its columns are the properties' plain names, since the relation already names the subject.

A property of a dim in [`partial`](../layers/tree.md#partial-the-granularity-of-an-override) that refers to a dim with `one_of` [categorizes](../model/properties.md#categories) its dim.
One category's labels are `properties[dim]` filtered on it, with every property of the dim.
`Schema.default_for` gives a property's [default](../model/properties.md#default_by-a-default-per-category): `default_by` per category, else `default`.
A framework's static frame, such as PyPSA's `n.generators`, is that frame with defaults applied, joined with the entity-only rows of its attributes.
A per-category read that applies the defaults itself waits until a caller needs one.

A category selects labels.
A label whose category changes keeps what it held, and a per-category read judges it by its current categories.

## Relations

```python
Relations = Mapping[str, DuckDBPyRelation]
```

A `Record` hands back bare DuckDB relations, each a plan on the record's connection.
Every record is DuckDB underneath, so a value handed over as itself costs no wrapping per read, and a caller who wants SQL needs no unwrapping.
A consumer filters, joins and aggregates with relation calls or SQL, and fetches with `.df()`, `.pl()` or `.to_arrow_table()`.

An attribute's relation holds its rows as written, in no promised order.
A NULL coordinate stays NULL, and two rows may cover one coordinate, of one layer or of two.
The consumer settles each coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule): the row naming every dim the others there name wins, as [`settle`](#settling-rows) does.
The record guarantees such a row wherever two rows cross ([resolution](../layers/resolution.md#crossing-rows)), so a consumer needs no knowledge of layers or of `partial`.

`LazyRelations` builds each relation on first access.
Building a relation is itself I/O: `read_parquet` opens the file to read the footer that binds the relation's columns.
Locally that is a page-cache hit.
Against a remote record it is a round trip per attribute, so a consumer that wants three of forty attributes pays for three, and listing the keys pays for none.

Laziness saves I/O.
An unmaterialised relation is a query plan, so holding every attribute's relation at once costs little memory either way.
Fetching is the expense, and the consumer decides when to fetch.

## `Flags`

`Flags` say which axes an attribute's rows use, so a consumer can plan its reads before opening a file.

```python
@dataclass(frozen=True)
class Flags:
    varies: frozenset[str]  # dims some row of this attribute sets
    broadcast: frozenset[str]  # dims some row leaves NULL, i.e. "all values"
    breakpoints: bool  # any row carries a breakpoint
```

`flags()` answers for the whole record in one query, keyed by attribute.
Only attributes with rows are present, so `set(record.flags())` also says which attributes the record has.

`flags(**categories)` answers for one category.
The keywords are categorizing properties of one dim by their plain names, such as `ctype="Generator"`, each with one category or a list ([categories](#properties-and-categories)).
Only rows that reach a matching label count: a row naming it through the dim's own column or a group's single field over the dim, and a row leaving that column NULL, which reaches every label or tuple.
An attribute is present where such a row exists.
Keywords that categorize several dims are refused, and so is a group that reaches the dim through two fields, since a row would then count towards no single label.

The sets name dims, so a consumer asks about a named axis: `"timestep" in flags["p_max_pu"].varies`.
`breakpoints` is a boolean, since a breakpoint is an abscissa within one row's value and no axis ([wide and long rows](../model/attributes.md#wide-and-long-rows)).

`varies` and `broadcast` can overlap.
An attribute may have per-timestep rows for one entity and a single NULL-timestep row for another, so `timestep` is in both.
That tells the consumer to use both containers.
`timestep in broadcast` selects the NULL-timestep rows into a constant frame, and `timestep in varies` selects the rest into a series frame.
For one label the two sets would be complements, and the union over many labels is what makes the pair informative.
For the same reason a framework asks per category: a Generator's per-timestep rows and a Link's single row, unioned, describe neither.

`varies | broadcast` tests whether an attribute touches a dim at all.
Both sets empty for a dim means the attribute has no values along it, so the consumer builds no container there.

Both sets cover only the dims and groups an attribute is addressed by.
A dim outside its [`dims`](../model/attributes.md#attributespec) is in neither set.
The source the flags aggregate, such as the [owner map](../layers/resolution.md#owner-map), a scan of `attributes/` or a staging table, is one relation over every attribute.
A dim one attribute uses reads NULL in the rows of another attribute that lacks it, and that NULL means the axis does not exist for that attribute.
Counting it as broadcast would mislead every consumer into building a constant container along an axis with no values.

`p_nom` set per entity puts `entity` in `varies`, and a single `entity = NULL` row puts it in `broadcast`.
The flags describe the rows the record holds, so a broadcast that more specific rows shadow at every label still puts its dim in `broadcast`.

`flags` covers attributes alone.
Properties are columns of their subject's file, read from `properties`.

## Settling rows

`settle(rel, labels, flags)`, exported as `from datarecord import settle`, settles an attribute's rows as written by [the broadcast rule](../model/attributes.md#the-broadcast-rule), one row per coordinate.
A row NULL on a coordinate in `labels` takes each label there, and a row naming a label outside `labels`, or reaching none, is dropped.
At each coordinate the row naming every coordinate in `labels` that the other rows there name is kept, and where none does the query fails, naming the coordinate.
A coordinate outside `labels` is kept as written, a NULL there being one more label, so a constant `p_max_pu` stays one row with `snapshot` NULL.
`filter=False` keeps a row naming a label outside `labels` as written, for a caller whose own joins on the labels drop it anyway.

`flags` spares the work a record does not need, and flags overstating the rows change no value.
Only a coordinate in `broadcast` is expanded, and rows are picked only where one is in `varies` as well.
Elsewhere every row names the same coordinates, one row per key, and `settle` keeps those naming a passed label.

The rule needs no knowledge of layers, so `settle` lives in core and reads a plain directory as it reads a tree.

## One engine

The protocol names DuckDB: a `RecordLike` answers `DuckDBPyRelation`s.
A tool's record builds them on the caller's connection, as the PyPSA tool scans its pandas frames with `con.from_df`.
A `RecordLike` therefore needs DuckDB, which is the cost of one representation from fold to consumer.

The resolution engine has no abstraction layer either.
The [owner-map fold](../layers/resolution.md#owner-map) is relational algebra of real complexity, and an engine interface with one implementation behind it costs effort for a second engine that does not exist.
