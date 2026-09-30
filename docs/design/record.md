<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The `Record` protocol

The definition sketched in [what a data record is](index.md#what-a-data-record-is), in full. This is the contract a consumer codes against; [the record format](format.md) is how it is stored.
It is read-only: writing is [`write_record(revision_id, source, con)`](writing.md).

Two names, one shape. **`RecordLike`** is the protocol below — what a signature annotates against, and what a framework object satisfies structurally without depending on this package. **`Record`** is the class this package provides: the narwhals interface over [one fold](read-path.md#one-record-over-one-fold), which is what `Revision.record` and `Record.at(uri)` both give you. The concrete thing gets the short name because it is what a caller constructs and holds.

```python
@runtime_checkable
class RecordLike(Protocol):
    """Dimensioned attribute data with a declared schema."""

    @property
    def schema(self) -> Schema: ...  # dims, attributes, partial

    @property
    def dims(self) -> Frames: ...  # axis frames, keyed by dim
    @property
    def relations(self) -> Frames: ...  # each relation's rows, keyed by relation
    @property
    def attributes(self) -> Frames: ...  # long frames, keyed by attribute

    def flags(self, entities: Sequence[str] | None = None) -> dict[str, Flags]: ...
```

## `LayerData`

`RecordLike` is what a caller reads; `write_record` writes a different, narrower protocol — **`LayerData`**, "the rows of one thing, enumerated and read":

```python
@runtime_checkable
class LayerData(Protocol):
    @property
    def schema(self) -> Schema: ...
    @property
    def frozen(self) -> bool: ...

    def axes(self) -> Iterable[str]: ...
    def axis(self, dim: str) -> DuckDBPyRelation | None: ...
    def relations(self) -> Iterable[str]: ...
    def relation(self, name: str) -> DuckDBPyRelation | None: ...
    def attributes(self) -> Iterable[str]: ...
    def attribute(self, name: str) -> DuckDBPyRelation | None: ...
```

Each pair is an enumerator — the keys of that kind — and a read for one key.
The enumerators return `Iterable[str]` rather than `set[str]` so a `Resolver`, whose keys carry a stable order a `Record` over it relies on, can return a `list` where a source returns a `set`; the meaning is the same set of keys either way.

One object, two meanings: a [`LayerSource`](read-path.md#owner-map) answers `axis`/`relation`/`attribute` for its own layer's rows; a `Resolver` answers the same names for everything folded into it.
`write_record` cannot tell which it holds and does not need to — a staged layer's own rows (`NewChild`) and a resolved whole record (`Directory`) are both a `LayerData`, so [committing](working-record.md#committing) writes either without a third shape adapting one to the other.

`schema` governs which of the other pairs are populated, rather than standing beside them as a peer: `relations`/`relation` answer only for the relations it declares.
An enumerator answers the **empty set**, never a phantom key, where the schema declares nothing of that kind.

Rows are raw `DuckDBPyRelation`s, not narwhals frames — the write path stays one engine throughout.
A `RecordLike` over narwhals `Frames` instead, such as the one [`from_sources`](sources.md) returns, is not itself a `LayerData`; `write_record` wraps it in a thin adapter that reads its `Frames` mappings through the same enumerate-and-read pairs, so a source stays narwhals-facing without `write_record` growing a second code path.

## Wide and long rows

`schema` is [the declaration](schema.md): which axes and relations exist, which attributes exist, and over which axes each may vary.
The rest is data, and comes in two shapes.

`dims` and `relations` are **wide** — one row per thing, keyed by the dim or the relation:

```text
dims["scenario"]                       scenario | ...   one row per axis label, in axis order
dims["entity"]                         entity | <attributes over entity alone>
dims["port"]                           port | <attributes over port alone>
relations["connection"]                entity | bus
relations["port_bus"]                  port | bus
relations["entity_type"]               entity | entity_type
```

`relations` is keyed by the relation alone, one frame each: a relation's rows are keyed by its columns and the component type is not one of them, so `relations/connection.parquet` holds every type's attachments ([where the rows live](format.md#where-a-value-lives)).

`attributes` is **long** — one row per value, keyed by the attribute's name:

```text
attributes["p_max_pu"]     entity | <one column per coordinate> | attribute | breakpoint | value
attributes["efficiency"]   port | <...> | attribute | breakpoint | value
```

A row names what the value belongs to, the coordinate it sits at, and the value there.

**The columns are the attribute's own**, not a fixed set every file carries: an attribute's coordinates are the dims its [`dims`](schema.md#attributespec) name.
So `efficiency` over `port` and `timestep` carries `port | timestep`, and `objective_weighting` over `snapshot` alone carries no entity column at all — an all-NULL `entity` would be a column claiming a component the value has none of.
`union_by_name` is what lets the fold union files of differing shape, supplying NULL for a coordinate a given file does not carry.

There is **no `entity_type` column** in that row, and none in the mapping's key either: `attributes["p_max_pu"]` holds every type's `p_max_pu` together, since an `entity` already identifies a component on its own ([what a data record is](index.md#what-a-data-record-is)).
A consumer that wants one type's rows joins `relations["entity_type"]` on `entity`, and a frame per type is `dims["entity"]` joined to it the same way.

**`breakpoint`** is NULL for the ordinary case. It carries the abscissa of a piecewise-linear value: a curve is one row per breakpoint, `value` the ordinate at each. Convexity is never checked or recorded — that is a framework's judgement.

## Connections

A component attaches to buses, and some attributes are per attachment rather than per component.
Each attachment is one label of a dim of its own, `port` in the PyPSA-shaped schema, and two functional [relations](schema.md#relations) tie it to its component and its bus: `port_entity` keyed by `port` with values `entity`, and `port_bus` keyed by `port` with values `bus`.
`role`, which end of the component a port is, is an attribute over `port` alone, so it is a column of `dims["port"]`. `efficiency` is over `port` and `timestep`, so it is long rows like any other attribute, and decodes by the same rules with no special case ([data on a relation's rows](schema.md#data-on-a-relations-rows)).

A port is identified by **its own label**, never by position. A patch layer that adds a port adds a label, and no other port is renumbered.

A schema may also declare `connection`, keyed by `(entity, bus)`, for the topology alone. `relations["connection"]` then lists the attachments, one row per `(entity, bus)` across every component type, and carries no attribute.
A record declaring none of these has no connections. Nothing about the mechanism is particular to buses — `corridor` over `(from, to)` is the same machinery, which is why `bus` is a dim name here rather than a word the read path knows.

## The broadcast rule

A row's `value` applies to every combination of its NULL dim columns, enumerated from the axis frames in `dims`.
A NULL dim means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep.

**A NULL expands only for an attribute over that dim.** Every row of the fold carries every dim, so a row of `efficiency`, which is not over `scenario`, reads NULL there too. That NULL means "not over this dim", and it expands nothing.

**Every dim an attribute is over broadcasts**, `entity` and `port` included: a NULL `entity` on `p_max_pu` means every entity, and a NULL `port` on `efficiency` means every port.
The `values` dim of a functional relation is an ordinary axis too: `entity_type` and `country` broadcast like any other.

One layer may hold a default and its exceptions side by side: an `efficiency` row with `port = NULL` beside one that names `dc_out`.
Where two rows of the owning layer cover one coordinate, the row that names more of the attribute's dims wins there.
Two rows that cover one coordinate where neither names a superset of the other's dims are a tie, and `write_record` refuses them: state the value at that coordinate in a row of its own.
Two rows that leave the same dims NULL at one coordinate are a duplicate, and `write_record` refuses them too: keep one row there.
A curve is one row per distinct breakpoint at its coordinate, so its rows are no duplicate; a scalar row beside a curve there is, and the write keeps either the scalar or the curve.
A coordinate no row covers — including an attribute with no rows at all — takes that attribute's `default` from [the schema](schema.md#attributespec).

Broadcast form is preserved: a value held once is answered once, so a consumer can reconstruct the constant-versus-varying split from the shape it gets back.

## Axis order

An axis's order is the row order of its frame in `dims`.

Components and a relation's rows are ordered too, in the order they were introduced ([the owner map](read-path.md#owner-map)).
Order is never a stored column, there as here: a file's row order is the input, and `order_key` is what the fold derives from it to answer "first introduced" across layers.

The distinction is **whether a table gains rows across layers.** A relation does, so its map keeps `order_key`; an axis does not, since an axis is [not partial](schema.md#partial-the-granularity-of-an-override) and a layer restating it restates it whole, leaving file order intact.

## `Frames`

```python
Frames = Mapping[str, nw.LazyFrame]
```

narwhals is the boundary type because it is a _protocol over dataframes_ rather than a dataframe: a record may hand back a DuckDB relation, a polars plan or a pandas frame and the consumer's code is the same.

`LazyFrames` is the lazily-building implementation, used where constructing a frame is itself I/O: `read_parquet` reads the parquet footer to bind the schema, so it opens the file.
Locally that is a page-cache hit; against a remote record it is a round trip per attribute, so a consumer wanting three of forty attributes pays three rather than forty, and listing the keys pays none.

Laziness here saves I/O, not memory: an unmaterialised relation is a query plan, so holding every attribute's frame at once costs little.
The expense is `collect`, which is the consumer's call either way.

## `Flags`

Which axes an attribute's rows actually use, over a set of entities — so a consumer can plan its reads without opening a file.

```python
@dataclass(frozen=True)
class Flags:
    varies: frozenset[str]  # dims some row of this attribute sets
    broadcast: frozenset[str]  # dims some row leaves NULL, i.e. "all values"
    breakpoints: bool  # any row carries a breakpoint
```

`flags(entities)` answers for the named entities in one query, keyed by attribute, and `flags()` answers for every entity.
It takes a sequence of names; a bare string raises `TypeError`.
Only attributes with rows are present, so `set(record.flags(names))` also answers which attributes these entities have at all.

The sets name dims, so a consumer asks about a **named** axis: `"timestep" in flags["p_max_pu"].varies`.
`breakpoints` is a boolean rather than a set because a breakpoint is not a dim ([wide and long rows](#wide-and-long-rows)) — it is an abscissa within one row's value, not an axis the value is indexed by.

The flags describe the rows a read returns: a row that another row of its layer outranks at a coordinate ([the broadcast rule](#the-broadcast-rule)) counts for nothing there.
So `flags(port=["dc_out"])` for an `efficiency` default beside a `dc_out` row puts `port` in `varies` and not in `broadcast`.

**The two sets are not complements.** An attribute may have per-timestep rows for one component and a single NULL-timestep row for another, so `timestep` lands in both.
That is an instruction to use both containers: `timestep in broadcast` selects the NULL-timestep rows into a constant frame, `timestep in varies` selects the rest into a series frame.
Per component they would be complements; the aggregation over several entities is what makes the pair carry information.

**`varies | broadcast`** is the test for whether an attribute touches a dim at all.
Both sets empty for a dim means the attribute has no values along it, so the consumer builds no container there.

**Both sets are scoped to the dims the attribute is over.** A dim outside its [`dims`](schema.md#attributespec) is in neither, never in `broadcast`.
The two are easy to conflate because whatever the flags are aggregated from — the [owner map](read-path.md#owner-map), a scan of `attributes/`, a staging table — is one DuckDB relation over every attribute, so a dim one attribute uses reads NULL for the rows of one that does not; but that NULL means "no such axis", not "every value of it".
Reporting it as broadcast would answer the question above wrongly for every attribute in the record: a consumer would build a constant container along an axis the attribute has no values on.

So an attribute over `entity` alone reports both sets empty, and that is not the same as having no rows — an attribute with no rows at all is [absent from the mapping](#flags) entirely.

A consumer asks per type, because one file holds every type's rows: unioning across types would report a Generator's per-timestep rows and a Link's single row as one shape, which describes neither.
So it passes the names of one type, read from `relations["entity_type"]`, and `flags` filters the [owner map](read-path.md#owner-map) on `entity`.

## The protocol names no engine

The protocol names no engine — `nw.LazyFrame` is all a consumer sees, so a pure-Python, polars or Ibis-backed record would satisfy it without change.

The implementations provided are DuckDB-backed, both of them, and the resolution engine is not abstracted behind an interface.
The [owner-map fold](read-path.md#owner-map) is relational algebra of real complexity, and an engine abstraction with one implementation behind it is a cost paid for a second that does not exist.
Adding one later means writing a second `Record`, which the protocol already permits.
