# The `Record` protocol

The definition sketched in [what a data record is](index.md#what-a-data-record-is), in full. This is the contract a consumer codes against; [the record format](format.md) is how it is stored.
It is read-only: writing is [`write_record(revision_id, record, con)`](writing.md).

```python
@runtime_checkable
class Record(Protocol):
    """Dimensioned attribute data with a declared schema."""

    @property
    def schema(self) -> Schema: ...  # dims, attributes, partial

    @property
    def dims(self) -> Frames: ...  # axis frames, keyed by dim
    @property
    def components(self) -> Frames: ...  # members, keyed by component type
    @property
    def connections(self) -> Frames: ...  # component↔bus rows, keyed by type
    @property
    def attributes(self) -> Frames: ...  # long input frames, keyed by attribute

    @property
    def outputs(self) -> Frames: ...  # long result frames, keyed by attribute

    def flags(self, ctype: str) -> dict[str, Flags]: ...
```

## Wide and long rows

`schema` is [the declaration](schema.md): which axes exist, which attributes each component type may carry, and over which axes each may vary.
The rest is data, and comes in two shapes.

`dims`, `components` and `connections` are **wide** — one row per thing, keyed by the dim or the component type:

```text
dims["scenario"]         scenario | ...           one row per axis label, in axis order
components["Generator"]  entity | <non-varying attribute columns>
connections["Link"]      entity | bus | role | ...  one row per component↔bus attachment
```

`attributes` and `outputs` are **long** — one row per value, keyed by the attribute's name:

```text
attributes["p_max_pu"]   entity | bus | <one column per declared dim> | attribute | breakpoint | value
```

A row names the component it belongs to, the coordinate it sits at, and the value there.

There is **no `component_type` column** in that row, and none in the mapping's key either: `attributes["p_max_pu"]` holds every type's `p_max_pu` together, since an `entity` already identifies a component on its own ([what a data record is](index.md#what-a-data-record-is)).
A consumer wanting one type's rows joins `components` on `entity` — the entity frames are what say which type an entity is.

Two of the long columns are NULL for the ordinary case, a component-level scalar:

- **`bus`** names one of the component's [connections](#connections), where the value belongs to that attachment rather than to the component — a `Link`'s `efficiency` at one end.
- **`breakpoint`** carries the abscissa of a piecewise-linear value: a curve is one row per breakpoint, `value` the ordinate at each. Convexity is never checked or recorded — that is a framework's judgement.

## Connections

Some attributes belong not to a component but to one of its connections to a bus.

A connection is identified by **the bus it attaches to**, never by position.
`connections[ctype]` lists the attachments themselves, one row per `(entity, bus)`; `role` — which end of the component it is — describes the connection and identifies nothing.

A per-connection value is otherwise an ordinary long row: `efficiency` on one connection may vary by timestep and scenario like any other attribute, and decodes by the same rules with no special case.
A record whose components have no connections answers `connections` empty.

## The broadcast rule

A row's `value` applies to every combination of its NULL dim columns, enumerated from the axis frames in `dims`.
A NULL dim means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep.

Rows never overlap, so at most one covers any coordinate.
A coordinate no row covers — including an attribute with no rows at all — takes that attribute's `default` from [the schema](schema.md#attributespec).

Broadcast form is preserved: a value held once is answered once, so a consumer can reconstruct the constant-versus-varying split from the shape it gets back.

## Axis order

An axis's order is the row order of its frame in `dims`.

Components and connections are ordered too, in the order they were introduced ([the owner map](read-path.md#owner-map)).
Order is never a stored column, there as here: a file's row order is the input, and `order_key` is what the fold derives from it to answer "first introduced" across layers.

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
Both sets empty for a dim means no row mentions it, so the consumer skips the attribute.

Per component type, because one file holds every type's rows: unioning across types would report a Generator's per-timestep rows and a Link's single row as one shape, which describes neither.
Scoping is a join to the components map on `entity`, not a filter on the attribute rows ([entity is unique across types](format.md#entity-is-unique-across-types)).

## The protocol names no engine

The protocol names no engine — `nw.LazyFrame` is all a consumer sees, so a pure-Python, polars or Ibis-backed record would satisfy it without change.

The implementations provided are DuckDB-backed, both of them, and the resolution engine is not abstracted behind an interface.
The [owner-map fold](read-path.md#owner-map) is relational algebra of real complexity, and an engine abstraction with one implementation behind it is a cost paid for a second that does not exist.
Adding one later means writing a second `Record`, which the protocol already permits.
