# The `Record` protocol

The definition sketched in [what a data record is](index.md#what-a-data-record-is), in full. This is the contract a consumer codes against; [the record format](format.md) is how it is stored.
It is read-only: writing is [`write_record(revision_id, record, con)`](writing.md).

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
    def components(self) -> Frames: ...  # members, keyed by component type
    @property
    def groups(self) -> Frames: ...  # each group's rows, keyed by group
    @property
    def attributes(self) -> Frames: ...  # long input frames, keyed by attribute

    @property
    def outputs(self) -> Frames: ...  # long result frames, keyed by attribute

    def flags(self, ctype: str) -> dict[str, Flags]: ...
```

## Wide and long rows

`schema` is [the declaration](schema.md): which axes exist, which attributes each component type may carry, and over which axes each may vary.
The rest is data, and comes in two shapes.

`dims`, `components` and `groups` are **wide** — one row per thing, keyed by the dim, the component type, or the group:

```text
dims["scenario"]                       scenario | ...   one row per axis label, in axis order
components["Generator"]                entity | <non-varying attribute columns>
groups["connection"]                   entity | bus | <attributes over the group>
```

`groups` is keyed by the group alone, one frame each: a group's rows are keyed by its coordinates and the component type is not one of them, so `groups/connection.parquet` holds every type's attachments ([where the rows live](format.md#where-a-value-lives)).

`attributes` and `outputs` are **long** — one row per value, keyed by the attribute's name:

```text
attributes["p_max_pu"]     entity | <one column per coordinate> | attribute | breakpoint | value
attributes["efficiency"]   entity | bus | <...> | attribute | breakpoint | value
```

A row names what the value belongs to, the coordinate it sits at, and the value there.

**The columns are the attribute's own**, not a fixed set every file carries: an attribute's coordinates are what its [`dims`](schema.md#attributespec) declare, with a [group](schema.md#groups) expanding to its coordinate names.
So `efficiency` over the `connection` group carries `entity | bus`, `flow` over a `corridor` carries `from | to`, and `objective_weighting` over `snapshot` alone carries no entity column at all — an all-NULL `entity` would be a column claiming a component the value has none of.
`union_by_name` is what lets the fold union files of differing shape, supplying NULL for a coordinate a given file does not carry.

There is **no `entity_type` column** in that row, and none in the mapping's key either: `attributes["p_max_pu"]` holds every type's `p_max_pu` together, since an `entity` already identifies a component on its own ([what a data record is](index.md#what-a-data-record-is)).
A consumer wanting one type's rows joins `components` on `entity` — the entity frames are what say which type an entity is.

**`breakpoint`** is NULL for the ordinary case. It carries the abscissa of a piecewise-linear value: a curve is one row per breakpoint, `value` the ordinate at each. Convexity is never checked or recorded — that is a framework's judgement.

## Connections

Some attributes belong not to a component but to one of its connections to a bus.

A connection is one row of the **`connection` [group](schema.md#groups)** — `Group(over={"entity": "entity", "bus": "bus"})` — rather than a structural category of its own.
`groups["connection"]` lists the attachments themselves, one row per `(entity, bus)`, across every component type; `role` — which end of the component it is — describes the connection and identifies nothing, and is an ordinary attribute a tool declares over the group ([PyPSA does](tools.md)) rather than a column the format fixes.

A connection is identified by **the bus it attaches to**, never by position.
An attribute is a connection attribute because its `dims` name the group, so a per-connection value is otherwise an ordinary long row: `efficiency` may vary by timestep and scenario like any other attribute, and decodes by the same rules with no special case.

A record declaring no such group has no connections, and one declaring it with no rows answers `groups["connection"]` empty.
Nothing about the mechanism is particular to buses — `corridor` over `(from, to)` is the same machinery, which is why `bus` is a coordinate name here rather than a word the read path knows.

## The broadcast rule

A row's `value` applies to every combination of its NULL dim columns, enumerated from the axis frames in `dims`.
A NULL dim means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep.

**Two kinds of coordinate do not broadcast**, and for the same reason — neither has an axis to expand against:

- **`entity`.** A NULL there is a value belonging to no component rather than to every component. It is the one dim the format knows by name, being [the axis the component types partition](format.md#the-entity-axis).
- **A [group](schema.md#groups)'s coordinate.** A NULL `bus` on a connection attribute means "every connection of _this_ entity", which is the group's rows — a sparse subset only the group's table knows, not the bus axis.

Both are compared NULL-safely and are what the schema requires to be [`partial`](schema.md#partial-the-granularity-of-an-override): a coordinate addressed individually is one a layer patches value by value.

Rows never overlap, so at most one covers any coordinate.
A coordinate no row covers — including an attribute with no rows at all — takes that attribute's `default` from [the schema](schema.md#attributespec).

Broadcast form is preserved: a value held once is answered once, so a consumer can reconstruct the constant-versus-varying split from the shape it gets back.

## Axis order

An axis's order is the row order of its frame in `dims`.

Components and a group's rows are ordered too, in the order they were introduced ([the owner map](read-path.md#owner-map)).
Order is never a stored column, there as here: a file's row order is the input, and `order_key` is what the fold derives from it to answer "first introduced" across layers.

The distinction is **whether a table gains rows across layers.** A group does, so its map keeps `order_key`; an axis does not, since an axis is [not partial](schema.md#partial-the-granularity-of-an-override) and a layer restating it restates it whole, leaving file order intact.

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

Which axes an attribute's rows actually use, for one component type — so a consumer can plan its reads without opening a file.

```python
@dataclass(frozen=True)
class Flags:
    varies: frozenset[str]  # dims some row of this attribute sets
    broadcast: frozenset[str]  # dims some row leaves NULL, i.e. "all values"
    breakpoints: bool  # any row carries a breakpoint
```

`flags(ctype)` answers for a whole type in one query, keyed by attribute.
Only attributes with rows are present, so `set(record.flags(ctype))` also answers which attributes this type has at all.

The sets name dims, so a consumer asks about a **named** axis: `"timestep" in flags["p_max_pu"].varies`.
`breakpoints` is a boolean rather than a set because a breakpoint is not a dim ([wide and long rows](#wide-and-long-rows)) — it is an abscissa within one row's value, not an axis the value is indexed by.

**The two sets are not complements.** An attribute may have per-timestep rows for one component and a single NULL-timestep row for another, so `timestep` lands in both.
That is an instruction to use both containers: `timestep in broadcast` selects the NULL-timestep rows into a constant frame, `timestep in varies` selects the rest into a series frame.
Per component they would be complements; the aggregation over a type is what makes the pair carry information.

**`varies | broadcast`** is the test for whether an attribute touches a dim at all.
Both sets empty for a dim means the attribute has no values along it, so the consumer builds no container there.

**Both sets are scoped to what the attribute is addressed by.** A dim outside its [`dims`](schema.md#attributespec) is in neither, never in `broadcast`.
The two are easy to conflate because whatever the flags are aggregated from — the [owner map](read-path.md#owner-map), a scan of `inputs/`, a staging table — is one relation over every attribute, so a dim one attribute uses reads NULL for the rows of one that does not; but that NULL means "no such axis", not "every value of it".
Reporting it as broadcast would answer the question above wrongly for every attribute in the record: a consumer would build a constant container along an axis the attribute has no values on.

So an attribute addressed by `entity` alone reports both sets empty, and that is not the same as having no rows — an attribute with no rows at all is [absent from the mapping](#flags) entirely.

Per component type, because one file holds every type's rows: unioning across types would report a Generator's per-timestep rows and a Link's single row as one shape, which describes neither.
Scoping is a join to the components map on `entity`, not a filter on the attribute rows ([entity is unique across types](format.md#entity-is-unique-across-types)).

## The protocol names no engine

The protocol names no engine — `nw.LazyFrame` is all a consumer sees, so a pure-Python, polars or Ibis-backed record would satisfy it without change.

The implementations provided are DuckDB-backed, both of them, and the resolution engine is not abstracted behind an interface.
The [owner-map fold](read-path.md#owner-map) is relational algebra of real complexity, and an engine abstraction with one implementation behind it is a cost paid for a second that does not exist.
Adding one later means writing a second `Record`, which the protocol already permits.
