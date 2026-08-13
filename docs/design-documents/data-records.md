# Design: Data Records

Status: Draft · Owner: Jonas Hörsch · Date: 2026-08-05

## 1. What a data record is

Dimensioned attribute data with a declared schema.

A record holds **components** (named members of a type), **connections** between components and buses, **attribute values** over both, and the **axes** those values vary along.
A schema declares what may exist; the data says what does.

A component's `name` identifies it **across every type**: names are unique record-wide, not per type (§3.5).
So an attribute row names a component and nothing more, and a component's type is something the record knows about it rather than part of its address.

Neither the concept nor this package names a modelling framework.
A framework consumes a record, a workflow engine produces one, and neither needs to know how the other works.
`datarecord` depends on `duckdb`, `narwhals` and `pydantic`, and on nothing else.

Two implementations of one interface:

- **`DirectoryRecord`** — one parquet directory.
  What the files hold is what it presents.
- **`LayeredRecord`** — a tree of layers, each adding a partial record on top of its parent, resolved last-writer-wins.

A consumer cannot tell which it holds.
That is what lets a framework read a hundred-layer overlay through the same call it would use for a single directory.

## 2. Scope

- **In scope:** the record format; the schema; the `Record` protocol and `WorkingRecord`; overlay resolution and its owner map; the write path; how the two implementations differ.
- **Out of scope:** a non-DuckDB implementation (the protocol permits one, §4.4, but only DuckDB-backed ones are provided); concurrent writers to one record; unmaterialised/meta layers.

## 3. The record format

A record is a parquet directory:

```
record/
├── manifest.json                   # the schema (§5)
├── dims/
│   ├── components/<Type>.parquet   # members + non-varying attribute columns
│   ├── connections/<Type>.parquet  # component↔bus connections (§6)
│   └── <dim>s.parquet              # one axis table per declared dim
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```

### 3.1 Where a value lives

Decided by the attribute's declared `dims` (§5.2), not by a particular value:

- **`dims/components/<Type>.parquet`** — attributes that vary over nothing (`dims = {}`): one column per attribute, indexed by `name`.
  A component's membership is its row here, and the file it is in is what gives it its type (§3.5).
- **`inputs/<attr>.parquet`** — every attribute that may vary, even where a given component's value happens to be constant.
  That component is then a row with the varying dim NULL.

So a component type's constant frame is assembled from both: the non-varying columns, and the dim-NULL rows of the varying files.

### 3.2 The long schema

Every `inputs/` and `outputs/` file carries:

```
name | bus | attribute | breakpoint | value | <dim> ...
```

with one column per declared dim.
`bus` addresses an attribute of one of the component's connections rather than of the component itself (§6); `breakpoint` carries the abscissa of a piecewise-linear value (§7).
Both are part of the schema rather than optional extensions, and both are NULL for the ordinary case — a component-level scalar — so a file whose every row is one carries two all-NULL columns.

That uniformity is the point: one column set means one `UNION ALL BY NAME` and one join shape for every kind of attribute row (§9.2).
A conditional column set would need the absent case handled everywhere regardless.
It also means a file written without one of these columns still reads back correctly, since `union_by_name` supplies NULL — the right reading of a record with no connections and no curves.

One attribute per file, so `value` carries that attribute's dtype.
There is **no `component_type` column**: `name` is unique across every type (§3.5), so `inputs/p_max_pu.parquet` holds every type's `p_max_pu` keyed by name alone, and a reader wanting one type's rows joins `dims/components/` (§3.5).

A connection's `role` is not in the long schema: it lives on the connection row and identifies nothing in `inputs/` (§6).

### 3.3 The decode rule

A row's `value` applies to every combination of its NULL dimension columns, enumerated from the axis tables under `dims/`.
A NULL dim means "all values of that dim", not that the attribute lacks the axis: a constant `p_max_pu` is one row with `timestep = NULL`, a varying one is a row per timestep.

Rows never overlap within a record.
A coordinate no row covers, including an attribute with no file, takes the attribute's `default` from the schema.

Broadcast form is preserved rather than incidental: a writer that holds a value once stores it once, and a reader sees the same shape back.
That is what lets a consumer reconstruct a constant-versus-varying split from the stored form.

### 3.4 Axis order

An axis is ordered, by the row order of its `<dim>s.parquet`.
Nothing declares this and nothing needs to: the row order _is_ the order.

Under resolution the same rule extends across layers by first-introduced position — a descendant appending a new period lands after the parent's.
Components and connections get the same semantics from the owner map's `order_key` (§9.1).

### 3.5 `name` is unique, and the entity tables say what a name is

A `name` identifies one component across the whole record.
Two types may not share one: a `Bus` and a `Generator` both called `north` are a collision, not two components.

That is what removes `component_type` from every attribute key.
An `inputs/` row addresses `(name, bus, …, attribute)`, and the type it belongs to is recoverable but not part of the address.
The alternative — carrying the type in the key — makes it a _component's_ identity in one place and a _row's_ in another, and every join then has to agree about which.

**The entity tables are the mapping.** `dims/components/<Type>.parquet` already partitions membership by type, one file per type, so `name -> component_type` is the file a name's row is in.
Nothing new is stored to answer it: no separate entity table, because the component tables _are_ one.
Where the union of those files needs the type as data — the owner map (§9.1), a glob across types — `component_type` is a column of the **entity** rows, never of the attribute rows.

So a consumer wanting one type's `p_max_pu` joins the resolved attribute frame to the components map on `name`.
That join is what the `component_type` filter used to be, and it is against a relation the read path already builds.

Two things follow, and they are the reason to want this.
An attribute row is addressed the way a component is, so `set("p_nom", 150.0, names=["wind1"])` needs no type: the name determines it (§11.2).
And the inputs key loses a column, so the fold's key is `(name, bus, *owned_per dims, attribute)` — one less column to compare NULL-safely in every join in §9.

**Enforced, not assumed.** `write_record` rejects a record whose component tables share a name, and `add` rejects a name the record already resolves under another type (§11.5).
A collision cannot be left to be discovered: it would silently merge two components' attribute rows, since the rows themselves no longer record which type they meant.

A modelling framework that scopes names per type must therefore reconcile before it writes.
Its tool's `verify` is where that is reported (§12), rather than something the record layer mangles a name to paper over: a record's `name` is the framework's own name, and a record that renamed them would hand back components a framework cannot find.

## 4. `Record`

```python
@runtime_checkable
class Record(Protocol):
    """Dimensioned attribute data with a declared schema."""

    @property
    def schema(self) -> Schema: ...  # dims, attributes, partial (§5)

    @property
    def dims(self) -> Frames: ...  # axis frames, keyed by dim
    @property
    def components(self) -> Frames: ...  # members, keyed by component type
    @property
    def connections(self) -> Frames: ...  # component↔bus rows, keyed by type (§6)
    @property
    def attributes(self) -> Frames: ...  # long input frames, keyed by attribute

    @property
    def outputs(self) -> Frames: ...  # long result frames, keyed by attribute

    def flags(self, ctype: str) -> dict[str, Flags]: ...
```

`attributes` is keyed by attribute name, matching the file layout: one `inputs/p_max_pu.parquet` holds every component type's rows, and a reader wanting one type joins `components` on `name` (§3.5).

`outputs` is an ordinary member, and its **emptiness is the existence answer** — the same idiom §4.3 uses for `flags`, where an attribute with no rows is absent from the mapping rather than present with empty sets.
An unsolved record answers with an empty mapping, and nothing half-writes results, so there is no state in which empty is ambiguous.
`write_record` writes `outputs/` only for a source whose mapping is non-empty, so a record with no results produces a layer without the directory rather than an empty one.

Because `Record` is structural, a source may omit the member entirely, and `write_record` reads an absent `outputs` as "no results" rather than raising — the same answer as defining it empty.

One thing does set it apart: results do not overlay (§9.4), so a layered record's `outputs` reads its own layer where every other member reads the resolution.
That is a documented property of the member rather than a reason to split the protocol.
A separate `Solved` would not have changed those semantics, only gated access to them, and it made every consumer test for a second type to reach a member most records simply have none of.

Read-only.
Writing is `write_record(revision_id, record, con)` — a function over a record rather than a method on one (§10).

Called `Record` rather than `Layer` because a layer is one node's own contribution and a resolved overlay is not one, but both are records in the sense §3 uses.

### 4.1 A protocol, not a base class

A `Record` is a _view_, and the two implementations share no state: one resolves a fold, the other reads files.
Structural typing also lets a consumer satisfy it without depending on this package at all — which is how a framework object can present itself as a record, and how a framework could implement `n.import_from_store` against nothing but the protocol.

### 4.2 `Frames`

```python
Frames = Mapping[str, nw.LazyFrame]
```

The `Mapping` ABC, deliberately.
What the protocol requires is that the **values** be unmaterialised; whether the mapping builds them up front or on lookup is an implementation's own business, so a plain `dict` satisfies it as fully as a lazily-building mapping does.

narwhals is the boundary type because it is a _protocol over dataframes_ rather than a dataframe: a record may hand back a DuckDB relation, a polars plan or a pandas frame and the consumer's code is the same.
A native representation is reached only where parquet is written (§10).

`LazyFrames` is the lazily-building implementation, used where constructing a frame is itself I/O: `read_parquet` reads the parquet footer to bind the schema, so it opens the file.
Locally that is a page-cache hit; against a remote record it is a round trip per attribute, so a consumer wanting three of forty attributes pays three rather than forty, and listing the keys pays none.

It does **not** bound memory, and the interface should not be defended on that ground: an unmaterialised relation is a query plan, so holding every attribute's frame at once costs little.
The expense is `collect`, which is the consumer's call either way.

### 4.3 `Flags`

Which axes an attribute's rows actually use, for one component type.
This is what lets a consumer plan its reads — which attributes exist, and which container each one's values belong in — before opening a single `inputs/` file.

```python
@dataclass(frozen=True)
class Flags:
    varies: frozenset[str]  # dims some row of this attribute sets
    broadcast: frozenset[str]  # dims some row leaves NULL, i.e. "all values"
    breakpoints: bool  # any row carries a breakpoint (§7)
```

Keyed by attribute, so `flags(ctype)` answers for a whole type in one query rather than one call per attribute.
Its **key set is the existence answer**: an attribute with no rows for this type is absent from the mapping rather than present with empty sets, so `set(record.flags(ctype))` is "which attributes does this type actually have".
Emptiness cannot mean absence, because an attribute declared over no dims at all (§5.2) has rows but nothing in either set.

Named for the dims themselves rather than for what a framework calls their absence.
Whether a value is "static" is a statement about one axis being NULL, and which axis that is depends on the schema — so `"timestep" in flags["p_max_pu"].varies` says what a `has_series` flag can only imply, and stays meaningful for a `region`- or `vintage`-varying attribute that no time axis describes.
Consumers ask about a **named** dim, never "does anything vary": a framework splitting on time cares about `timestep` and not about `scenario`, so a single "has varying dims" boolean would answer no one's question.

`breakpoints` stays a boolean because it is not a dim (§7): a breakpoint is an abscissa within one row's value, not an axis the value is indexed by.
It is what tells a consumer whose target takes only scalars that an attribute is unbuildable, rather than discovering it mid-translation.

Per component type, because one file holds every type's rows: unioning across them would report a Generator's per-timestep rows and a Link's single row as one shape, which describes neither.
Scoping to a type is a join to the components map on `name` rather than a filter on the attribute rows (§3.5), which changes what the query does and not what it answers.

#### The two sets are independent

Not complements.
An attribute may have per-timestep rows for one component and a single NULL-timestep row for another, so both `varies` and `broadcast` contain `timestep`.

That is not an ambiguity the consumer has to resolve — it is an instruction to use both containers.
A framework splitting constant from time-varying data reads the two sets in two passes: `timestep in broadcast` selects the NULL-timestep rows into the constant frame, `timestep in varies` selects the rest into the series frame, and a type whose components disagree goes down both paths with each row landing in the right one.
Collapsing to one boolean would force a choice that has no correct answer.

Per component the two _would_ be complements, which is why the aggregation is what makes the pair carry information rather than what costs it.

#### `varies | broadcast` — on this axis at all

The union is the test for whether an attribute touches a dim in any form.
Both sets empty for a dim means no rows mention it, so neither container applies and the consumer skips the attribute entirely.

Worth naming because it reads as an odd idiom otherwise: it is not "varying or not varying" but "present on this axis", the question that comes before which container to use.

### 4.4 Backend-agnostic protocol, DuckDB implementations

The protocol names no engine — `nw.LazyFrame` is all a consumer sees, so a pure-Python, polars or Ibis-backed record would satisfy it without change.

The implementations provided are DuckDB-backed, both of them, and the resolution engine is not abstracted behind an interface.
The owner-map fold is relational algebra of real complexity, and an engine abstraction with one implementation behind it is a cost paid for a second that does not exist.
Adding one later means writing a second `Record`, which the protocol already permits.

## 5. The schema

One schema per record, and `manifest.json` is how it is written down — the two words name the same thing, the file and the object.

```python
class Dimension(BaseModel):
    """One axis attribute data may vary over."""

    dtype: str  # the axis labels' type
    within: frozenset[str] = frozenset()  # labels unique only within these dims (§5.4)
    keys: frozenset[KeyKind] = ...  # entity tables this dim keys (§5.3)
    unit: str | None = None  # what the labels measure, if anything (§5.8)
    description: str | None = None  # what the axis is, in prose (§5.8)


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take."""

    dtype: str  # value column type
    dims: frozenset[str] = frozenset()  # dims it may vary over; subset of declared
    default: Any | None = None
    breakpoints: bool = False  # may carry a piecewise-linear curve (§7)
    bus: BusRelation = "component"  # "component" | "connection"
    unit: str | None = None  # what the values measure (§5.8)
    description: str | None = None  # what the attribute is, in prose (§5.8)


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations (§5.7)

    dimensions: dict[str, Dimension]
    attributes: dict[
        str, dict[str, AttributeSpec]
    ]  # component type -> attribute -> spec

    # Which dims a layer may patch value by value; absent for a record with no
    # layers, since nothing overrides anything (§5.5).
    partial: frozenset[str] | None = None

    meta: dict[str, Any] = {}  # opaque; the package never interprets it
```

`partial` is the only layering-specific part, and so the only optional one.
Everything else describes the data and is always present.

`component_type`, `name` and `attribute` are `VARCHAR`: those vocabularies belong to a modelling framework, and this package knows none.
A type no tool recognises reads back fine and is reported by the tool that cannot build it, not rejected inside the fold.

`meta` is where a framework's own top-level data goes — network attributes, coordinate reference system, free-form metadata.
It is stored and never interpreted, since none of it describes the dimensioned data.

### 5.1 Dimensions

Every dim is declared: a record with `region`, `technology` or `vintage` needs no code change, and `dtype` is the axis's own property.

A `Dimension` declares the axis's shape — its type, its nesting (§5.4), which entity tables it keys (§5.3).
It does not declare which dims an _attribute_ varies over (that is per attribute, §5.2), nor the patch granularity (§5.5), nor order (§3.4).

### 5.2 `AttributeSpec`

What one attribute may do over those axes:

```python
"Generator": {
    # a capacity to build: one decision, evaluated against every scenario
    "p_nom":         AttributeSpec(dtype="float64", dims=frozenset()),
    # an availability profile: varies over time, and per scenario
    "p_max_pu":      AttributeSpec(dtype="float64", dims={"scenario", "timestep"}),
    "marginal_cost": AttributeSpec(dtype="float64", dims={"scenario"}, breakpoints=True),
    "carrier":       AttributeSpec(dtype="str", dims=frozenset()),
}
```

`dims` is what makes a scenario-varying `p_nom` a schema violation: a capacity is a first-stage decision, one value taken before the scenario is known, which is the point of stochastic scenarios differing only in dispatch.

`p_nom` and `carrier` both have `dims = {}` and are not the same kind of thing — one is a label, the other a number an optimiser decides.
`dims` says only over which axes a value may differ.
Varying over nothing is also what puts both in `dims/components/` rather than `inputs/` (§3.1), so the schema decides the file split rather than a writer guessing it.

The rest answers what a bare column set cannot:

- _May it carry breakpoints?_ — `breakpoints`, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later (§7).
- _Is it bus-relative?_ — `bus`, so `efficiency` is known to be a connection attribute and `p_max_pu` a component one, rather than inferred from whether a `bus` value happens to be present.

### 5.3 `keys` — which entity tables a dim keys

Whether a component or a connection exists _per value_ of a dim:

```python
dimensions = {
    "scenario": Dimension(dtype="str", keys={"component", "connection"}),
    "timestep": Dimension(dtype="datetime64[us]"),
}
```

| `KeyKind`    | keys                              | consequence                                            |
| ------------ | --------------------------------- | ------------------------------------------------------ |
| `component`  | `dims/components/<Type>.parquet`  | a component exists per value, and is deleted per value |
| `connection` | `dims/connections/<Type>.parquet` | a connection exists per value                          |

On `Dimension` rather than `AttributeSpec` because existence is not an attribute's property: a component exists in scenario `high` or it does not, and `p_max_pu` gets no vote.

Also not layering-specific, which is why it sits beside `dtype` rather than with `partial`.
It puts the dim in the entity table's own key, so it decides that table's shape — one row per `(name, scenario)` rather than per `name`.
A single directory with no ancestry can therefore hold a generator present in `high` and absent from `low`, and answer which scenarios it exists in.
Tombstone scoping (§8.3) is a consequence of that key rather than its purpose.

A dim in `keys` must be in `partial` where that section exists, since keying membership per value of an axis that is only ever owned whole has no meaning.

### 5.4 `within` — an axis inside an axis

A dim whose labels identify a point only _within_ another dim's value.
Multi-period time is the case: the axis is a `(period, timestep)` pair, so `t1` alone names nothing and two periods may hold different timesteps.

```python
dimensions = {
    "period": Dimension(dtype="int64"),
    "timestep": Dimension(dtype="datetime64[us]", within={"period"}),
}
```

`within` makes `timesteps.parquet` carry a `period` column, and the axis key `(period, timestep)` rather than `timestep`.
It is on `Dimension` because nesting is structural — true of the data however stored, so a directory record needs it exactly as much as a layered one.

A **set**, because two different things could each be one parent:

- _Chained_ — `timestep` in `period` in `horizon`.
  Each dim names its immediate parent and the chain is walked, giving `(horizon, period, timestep)`.
- _Several direct parents_ — `timestep` identified only within a `(period, stage)` pair, where neither contains the other.
  This is what a multi-stage stochastic program with investment periods looks like.

So the axis key is `(*parents, dim)`, parents in declaration order.
Every name in `within` must be a declared dim, and the nesting graph must be acyclic.

Distinct from `AttributeSpec.dims` despite the similar shape: `dims` names _independent coordinates_ — a value exists at each combination and the set never chains — whereas `within` _qualifies a label_ and is transitive, so naming `period` pulls in `period`'s own parents.

The inner dim is named for the thing it indexes (`timestep`) rather than for the pair (`snapshot`), because once nesting exists the pair needs its own name: a framework consuming the record calls `(period, timestep)` a snapshot.

### 5.5 `partial` — the granularity of an override

Everything is overridable; a layer exists in order to override.
The remaining question is at what granularity along each axis, and it splits from §5.2's question because the two are properties of different things:

- _Which dims may this attribute vary over at all?_ — per **attribute**.
  `p_max_pu` varies over scenario and timestep; `p_nom` over neither.
  `AttributeSpec.dims`.
- _May a layer patch individual values along this axis, or must it restate the axis whole?_ — per **dimension**.
  `scenario` is patchable value by value; `timestep` is not, for any attribute.
  `schema.partial`.

```python
partial = {"scenario"}  # timestep absent, so a patch restates the series
```

A dim outside `partial` is one a layer owns entirely once it touches it: overriding one timestep of `p_max_pu` means carrying that component's _entire_ series, because a partial series would resolve across two layers and produce a curve with a hole.
The reason is a consumer's rather than the format's — a framework that splits constant from varying data cannot receive half a series — which is why it belongs to the axis: it is true of every attribute varying over it.

The dims a layer owns an attribute per follow from the two declarations:

```
owned_per(attribute) = attribute.dims ∩ schema.partial
```

So `p_max_pu` is owned per scenario — `timestep` is not partial, so a patch to one hour restates that scenario's whole series; `marginal_cost` per scenario; `p_nom` and `carrier` once, across everything.

Two things this buys.
The schema can distinguish `p_max_pu` from `p_nom`, which a dim-level flag cannot: that would say every attribute is owned per scenario, including those a scenario must not change.
And a `p_nom` row carrying a non-NULL `scenario` becomes a write-time violation rather than something the NULL-broadcast rule absorbs — a first-stage decision quietly turned into a per-scenario one is the error worth catching.

What the fold does with this is unchanged: the inputs key is one fixed tuple over all attributes, and an attribute not varying over a dim writes NULL there.
So the declarations constrain and validate; they do not make the key vary per row.

### 5.6 One schema per record

Not one per layer.
A directory record's schema is `manifest.json` in the directory; a layered record's lives **beside** the layers, not inside any of them:

```
record-root/
├── manifest.json               # the schema — one, for the whole tree
└── layers/<uuid>/              # a layer: dims/, inputs/, outputs/ — no manifest
    └── resolved/               # caches (owner map, resolved dims) — §8.2
```

A schema is not layered data.
Folding it would let a layer change what `p_nom` _means_ — its dtype, which dims it varies over — which is not a patch to data but a redefinition of the thing being patched, and it makes the schema unknowable without walking the ancestry.
One schema makes it a property of the record, validatable before anything is read and stated once for a hundred-layer tree.

The cost is that adding an attribute amends the root schema rather than shipping inside the layer that introduces it.
That is the right trade: a new attribute is a schema change, and one buried several layers deep is exactly what should be visible.

A layer directory therefore holds only data, which is what keeps it a plain parquet directory readable by a tool that knows nothing about layering.

### 5.7 Versioning

One schema outlives many layers (§5.6), so a change to it meets data written under the previous one.
`version` records which schema a record's layers were written against, and what matters is which changes existing layers survive.

**Compatible** — old layers stay readable, `version` bumps and nothing else happens:

- adding an attribute, or a component type
- adding a dim no existing attribute varies over
- widening an `AttributeSpec.dims`: rows that set fewer dims still decode, since an unset dim is NULL and NULL means "all values" (§3.3)
- adding to `partial`: ownership becomes finer, and an existing layer's rows are simply owned at the coarser granularity they were written with
- changing a `unit` or `description` (§5.8), which describe the data without deciding how any row decodes

**Incompatible** — existing rows would decode differently, or not at all:

- narrowing `dims`, since a row setting a now-undeclared dim has no valid reading
- changing a `dtype`
- removing from `partial`: a layer that patched one value along that axis is now a partial override of an axis owned whole, which is exactly the hole §5.5 forbids
- changing `within`, since the axis key changes shape
- adding to a dim's `keys`, since an entity table gains a key column its existing rows do not carry

The compatible changes are those where NULL already means what the new schema needs it to mean, so the decode rule (§3.3) absorbs them without touching a row.

An incompatible change therefore needs the layers rewritten rather than the schema edited, which for a layered record means flattening to a `Directory` (§11.7) under the new schema.
A reader encountering a `version` it was not written for should refuse rather than guess, since every failure above is silent.

### 5.8 `unit` and `description`

Both a `Dimension` and an `AttributeSpec` may carry a `unit` and a `description`.
Neither is interpreted: no conversion, no dimensional analysis, no validation that `MW` and `kW` are not being added.
They are stored, read back, and handed to whatever displays or documents the record.

They belong in the schema rather than in `meta` because they describe the _dimensioned data_ — which is exactly the line `meta` is on the other side of (§5).
A `unit` is a property of an attribute in the same way its `dtype` is, and a consumer asking "what is `p_nom` and what is it measured in" should not have to know a framework's own metadata layout to find out.

`None` means undeclared, not dimensionless.
A quantity that genuinely has no unit is `""` — the distinction matters to a renderer choosing between showing nothing and showing an empty unit, and to a later pass that wants to find what is still undocumented.

A dimension's `unit` describes what its _labels_ measure, which is only sometimes meaningful: a `vintage` axis labelled in years or a `distance` axis in km has one, while `scenario` and `timestep` do not — a timestamp is not a quantity.
`description` applies to any axis.

Neither field changes how a row decodes, so adding or editing one is a compatible change (§5.7).

## 6. Connections

Some attributes belong not to a component but to one of its connections to a bus.

A connection is identified by **the bus it attaches to**.
Position is a framework detail: keying the overlay by position would mean a patch layer had to know a connection's current index, so an ancestor inserting a connection earlier would silently redirect that patch to a different bus.

So connections are rows in `dims/connections/<Type>.parquet`, keyed by `(name, bus, *connection key dims)`, carrying their own tombstones.
`role` — which end of the component it is — is an ordinary described column, not part of the key.

`bus` is also part of the **inputs** key, `(name, bus, *owned_per dims, attribute)`, NULL for a component-level attribute and NULL-safe-compared so that case is unaffected.
That is what makes a per-connection attribute owned _per connection_: without it, a patch changing one connection's `efficiency` would own — and so have to restate — every connection's.

A per-connection attribute is otherwise an ordinary long-schema row: `efficiency` on one connection may vary by timestep and scenario like any other attribute, and resolves by the same rules with no special case.

A record with no `dims/connections/` resolves as one whose components have no connections.

## 7. Piecewise-linear values

Values that are curves rather than scalars — costs on a component, efficiencies on a connection — are one row per breakpoint, distinguished by `breakpoint`: the abscissa at which `value` is the ordinate.

`breakpoint` is deliberately **not** part of the overlay key.
A layer owns a whole curve, the same rule a non-`partial` dim follows (§5.5), so a parent's breakpoints and a descendant's can never resolve into one curve with a hole.

Convexity is never checked or recorded: that is a framework's judgement.

## 8. Layered resolution

A `LayeredRecord` resolves a tree of layers.
Each node adds one layer; a node's data is its layer resolved over its ancestors', last-writer-wins.

```python
class Revision(BaseModel):
    id: UUID
    parent: UUID | None  # None for a root
```

Each record has one layer at `layers/<id>/`; the path derives from the UUID rather than being stored.
Caches derived from the ancestry rather than from layer data live in a `resolved/` subdirectory of it, so the layer's own top level stays exactly what that layer wrote.

Nodes form a tree: branching is several children sharing a parent, and they share the parent's layer and all further ancestry by pointing at it, not by duplication.
Resolution order along a root→node path is ancestry order; a node's own layer is last and wins.

The node metadata — `(id, parent)` — is persisted in the `revisions` table, so a revision id resolves to its ancestry, and thus its layer set, through it.

### 8.1 A layer's data is write-once

A node has no mutable state.

A layer's data is created by one act — `write_record` (§10), whether called directly or by a commit (§11.7) — which refuses an existing directory and stages into a sibling path, so a layer is complete when it first becomes visible and never changes afterwards.
Editing needs no mutable layer: edits accumulate in the staging area (§11.9) and become a layer at commit.

The one thing written into a layer directory after that act is its `resolved/` cache (§8.2), which is why the invariant is stated over the layer's _data_ rather than over the directory.
That is not a loosening that costs anything: a cache is derived from immutable layers, so writing one cannot change an answer, and nothing downstream reads it as data.

Two properties follow. **Any node may be a parent**, since an immutable base cannot shift under its descendants.
And **a cache never needs invalidating**, since one derived from immutable layers cannot go stale.

### 8.2 Materialised node caches

A node's owner map and resolved dims may be materialised under `layers/<id>/resolved/`.
Not the schema: there is one for the whole record (§5.6), so there is nothing per node to resolve.

Where a materialised map exists, a read stops there: the map is already folded over everything above it, so a read walks the ancestry only back to the nearest materialised node rather than to the root.
That truncation is what keeps a deep chain cheap.
The same rule decides where resolved dims come from — a node's own `resolved/dims/` where materialised, its raw `dims/` otherwise — answered by the cache's presence rather than by any recorded state.
The two are distinct paths within one directory, so a record read as an ancestor and the same record read as itself never alias.

Materialising is a policy: every N layers, at a branch point, on demand.
It is purely additive, writing files under `resolved/` and changing no answer, only how many layers a read touches to reach it.

### 8.3 Deletion

A `deleted = true` row in `dims/components/<Type>.parquet` tombstones a component from every attribute, scoped by whichever dims key `component` (§5.3).
A `deleted = true` row in `dims/connections/<Type>.parquet` tombstones one bus's connection — its connection row and its `inputs/` rows — leaving the component and its other connections intact.

When the owner map is folded, a tombstone removes that key's entries from the map, so a deleted component is absent from the resolved map rather than filtered at read time.
A tombstone only affects the branch that carries it; sibling branches keep the component.

The fold treats an absent `deleted` column as "tombstones nothing", so a layer may be any standard parquet directory, not only one this package wrote.
Every derived cache lives under `resolved/` for the same reason: every glob the read path issues into a layer is single-level — `inputs/*.parquet`, `dims/*.parquet`, `dims/*/*.parquet` — so nothing under `resolved/` is reachable by one.
A reader pointed at a layer directory therefore sees exactly what that layer wrote, and materialising a node's caches never changes what it sees.

## 9. The DuckDB read path

### 9.1 Owner map

The owner map answers, for a node, which layer owns each key.
Three maps, not one:

Key columns first, then what each map carries over them:

```
# inputs                            # components               # connections
name                                name                       name
bus  -- NULL for component-level    <component key dims>       bus  -- never NULL
<owned_per dims>                    --                         <connection key dims>
attribute                           component_type             --
--                                  layer_uuid                 component_type
layer_uuid                          order_key                  layer_uuid
varies      STRUCT(<dim>: BOOLEAN, ...)                        order_key
broadcast   STRUCT(<dim>: BOOLEAN, ...)
breakpoints BOOLEAN
```

All three map to the owning `layer_uuid`, with deletions already applied.
None carries `value`, a varying dim's value, or `breakpoint`, so all stay small regardless of the series data or the size of a curve.

Splitting them keeps each row shape honest — `attribute` and the flags are meaningless for a component or connection row — and lets each persist as its own file.

`component_type` is on the **entity** maps only, never on `inputs`: an attribute row is addressed by `name` alone (§3.5), and the components map is what says which type a name is.
So that map is the entity mapping every type-scoped question goes through — `flags(ctype)` joins it (§4.3), as does a consumer wanting one type's frame.

Where it is present it is a **column, not part of the key.**
Every one of the three maps is keyed on `name` (plus `bus` and the dims that apply); the components map carries `component_type` because it is the table that answers "what type is this name", and that answer is functionally determined by the key rather than keying alongside it.
The fold therefore aggregates the type over the group-by instead of grouping on it.

The distinction is load-bearing rather than pedantic.
Keying on the type would mean a name could resolve to two rows — one per type — which is exactly the collision §3.5 forbids, silently admitted at read time instead of rejected at write time.
The same holds in the staging area: `remove` under one type followed by `add` under another must collapse to the later edit (§11.7), and a type-partitioned key keeps both.

`order_key` is monotonic across the fold history, giving first-introduced order across layers (§3.4).
It is assigned pre-union, per layer, because the fold's own output has no order of its own — a bare `row_number()` over what `UNION ALL` returns would scramble which row counts as first.

`order_key` is on the components and connections maps only; the axes need none, since an axis row's order comes from its file (§3.4).
The two carry it for different reasons, and only one is a correctness requirement.

For **connections** it is load-bearing.
A framework wanting positional ports numbers them by this order, so a patch layer adding a third connection appends rather than renumbering.
Without it the port index would follow whatever order the fold happened to emit, and adding a connection could silently move an existing one's attributes to a different port — the positional-keying failure §6 exists to prevent, reappearing at the point where position is reconstructed.

For **components** it is a stability guarantee rather than a correctness one: nothing resolves differently, since a component's rows are keyed the same way whatever order they come back in.
What it buys is that member order is deterministic across reads and recognisable — a record round-tripped through the write path comes back in the order it was authored, with additions appended, rather than reshuffled.

Each map is built by folding along the root→node path: parent map minus deletions and overrides, union the layer's own keys.
A node whose maps are materialised (§8.2) persists all three, so a read needs only the ancestry **back to the nearest materialised node** — the key scalability property.
Elsewhere the fold runs live over that node's persisted maps, cached per connection; since layers are write-once (§8.1), such a cache never needs invalidating.

The flags (§4.3) are folded in alongside the ownership group-by, so they cost nothing beyond it.
They are computed **per key**, so per component: whether _this_ component's `p_max_pu` sets `timestep` is a different question from whether any does.

Two **structs** rather than a `varies_<dim>` column per dim, because which dims exist is declared (§5.1) and a flat layout would make the map's _column set_ depend on the schema. §5.7 calls adding a dim compatible; that has to hold for a map already persisted at a materialised node (§8.2), not only for the layers.
With a struct the difference is a missing _field_, which `UNION ALL BY NAME` fills with NULL exactly as it would a missing column, and the new dim reads as unset — which it is, since no row mentions it.
The map's columns are then fixed, and only the fields move.

`breakpoints` stays outside both structs, being no dim (§7).
That also means the dim namespace lives entirely inside `varies`/`broadcast`, so a dim named `breakpoints` would collide with nothing.

`Record.flags(ctype)` unions them over the names of one type, which is the granularity every consumer works at (§4.3).
The union is not a loss of the per-key answer so much as the question being asked of a type: a framework assigns containers per type, so a type whose components disagree must be told so, and a dim landing in both sets is exactly that message.
The union stops at the type boundary, since across types it would describe neither.

### 9.2 Resolving a relation

A resolved relation semi-joins the owning layers' files to the `inputs` map, keeping only owned rows:

```sql
SELECT u.name, u.bus, u.timestep,
       COALESCE(u.scenario, o.scenario) AS scenario,   -- one per owned_per dim
       u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/inputs/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN inputs o
  ON o.name        = u.name
 AND o.bus         IS NOT DISTINCT FROM u.bus   -- NULL-safe: component-level keys NULL against NULL
 AND o.attribute   = u.attribute
 AND o.layer_uuid  = u.layer_uuid
 AND (u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario)
```

`name` joins on equality rather than NULL-safely: it is required and unique (§3.5), so there is no NULL to be safe about — the one column the old key needed a second equality for is simply gone.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
There is no per-read `MAX`/group-by and no tombstone filter — deletions are already absent from the map.

Each owned-per dim's arm is **NULL-aware**: a stored NULL means "all values", and the map may own it for only some of them, so the row joins every entry naming its layer and takes that value in the output.

`bus` is joined **NULL-safely** rather than NULL-aware: it is part of the key but a required column rather than a broadcast dim, so NULL means "this attribute is the component's, not a connection's" and never "every bus".
It is the `connections` map that decides which connections exist at all; a row whose connection was tombstoned is gone because that tombstone removed its `inputs` keys from the map, not by a filter here.

`breakpoint` is projected but not joined on, being no part of the key: a curve is owned whole (§7), so every breakpoint of a key comes from the winning layer.

Non-key dims pass through unchanged, because within one key-dim combination the rows come from one layer.

An attribute no layer wrote is absent from the map; its relation is empty, and the consumer applies the schema's `default`.

### 9.3 What differs between the implementations

|                  | `DirectoryRecord`              | `LayeredRecord`                             |
| ---------------- | ------------------------------ | ------------------------------------------- |
| resolution       | none — one record              | owner-map fold along ancestry               |
| `flags`          | `GROUP BY` scan over `inputs/` | free, folded with ownership                 |
| member order     | file order                     | `order_key`, first-introduced across layers |
| `schema.partial` | absent                         | the granularity of every patch              |

`flags` from a directory needs a real aggregate: parquet's footer statistics are per row group, not per component type, so a file mixing one type's series rows with another's constant says nothing about either.

### 9.4 Outputs

`outputs/<attr>.parquet` does not overlay.
An output relation reads the node's own layer only: if that layer has no `outputs/`, the record has no results, and an ancestor's are not inherited.

## 10. Writing a whole record

```python
def write_record(
    revision_id: UUID, source: Record, con: DuckDBPyConnection
) -> None: ...
```

Writes `source` as a new layer.
An existing layer directory is an error rather than an overwrite or a merge, so a whole-record write can never half-replace what a record holds.

`outputs/` is written only for a source whose `outputs` mapping is non-empty (§4), so a record with no results produces a layer with no `outputs/` rather than an empty directory.
A source may omit the member altogether, which reads the same way.

Keys are looked up one at a time and each file written before the next is built, so a lazily-building source does one read per file written rather than one per key up front.
Frames are staged into a sibling directory and renamed on success, so a frame the fold could not resolve leaves no layer rather than half of one.

Every column the schema declares a type for is cast to it on the way out, so a record's files carry the schema's types and a reader can trust them.
Without that a source may hand over an all-NULL column its dataframe library typed as float, and every reader would re-cast defensively instead.

Validation is structural: a long frame carries the §3.2 columns, and an entity frame carries every dim it is keyed by.
Which component types and attribute names are valid belongs to the schema's vocabulary.

Because a `Record` is the input, anything satisfying the protocol can be written — including a framework object presenting itself as one, which is what puts read and write on a single seam.

## 11. `WorkingRecord`

`Record` is read-only, and `write_record` writes a whole record from a source that already knows everything it will contain.
Neither covers editing: adding components, removing them, setting an attribute on a group.

```python
class WorkingRecord:
    """A `Record` that accepts edits and materialises them on commit."""

    def __init__(self, base: Record, con: DuckDBPyConnection) -> None: ...

    def set(
        self,
        attribute: str,
        value: Any,  # scalar | sequence | mapping | frame | nw.Expr
        *,
        names: Sequence[str] | None = None,
        bus: str | None = None,
        kind: Literal["inputs", "outputs"] = "inputs",
        **dims: Any,
    ) -> None: ...

    def add(self, ctype: str, frame: IntoFrame) -> None: ...
    def remove(self, ctype: str, names: Sequence[str], **dims: Any) -> None: ...

    def connect(self, ctype: str, frame: IntoFrame) -> None: ...
    def disconnect(
        self, ctype: str, pairs: Sequence[tuple[str, str]], **dims: Any
    ) -> None: ...

    @property
    def pending(self) -> Pending: ...
    def commit(self, target: Target) -> Any: ...  # the new child, for NewChild
    def rollback(self) -> None: ...
```

Built over a base `Record` and a DuckDB connection: `WorkingRecord(revision.record, con)`.

A **class, not a protocol**.
`Record` is a protocol because several things satisfy it — two backings, a framework object presenting itself as one (§4.1), the two readings commit writes — and structural typing is what lets a consumer satisfy it without depending on this package.
There is one way to edit a record, so a second name for it would be an interface over its only implementation.
Where the staged rows live is this class's own business (§11.9), which is why the name says what it is rather than how.

It **satisfies** `Record`, which is the load-bearing decision: a mutable record reads as a record, and what it reads is the data _with its pending edits applied_.
So an edit can be read back, or the record handed to something that only knows `Record`, without committing.
Structurally, not by inheritance — the read members are implemented here over base-plus-staged (§11.10).

Two properties follow from accumulate-then-commit, and both are the point:

- An edit costs a row in a staging table, not a rewrite.
  A hundred edits to one attribute are a hundred rows, collapsed once at commit.
- Nothing touches the record until `commit()`.
  A caller that fails halfway leaves no layer; one that changes its mind calls `rollback()`.

### 11.1 The shape of an edit

Each edit maps onto exactly one part of the format:

| edit                        | writes                                                              | key it targets                            |
| --------------------------- | ------------------------------------------------------------------- | ----------------------------------------- |
| set an attribute on a group | `inputs/<attr>.parquet` rows                                        | `(name, bus, *owned_per dims, attribute)` |
| add components              | `dims/components/` rows, plus `inputs/` rows for varying attributes | `(name, *component key dims)`             |
| remove components           | a `deleted = true` tombstone                                        | `(name, *component key dims)`             |
| connect / disconnect        | `dims/connections/` rows and tombstones                             | `(name, bus, *connection key dims)`       |

Every key is `name`-based, because `name` is what identifies a component (§3.5).
An **entity** edit still _names_ a type — `add("Generator", frame)` — because it creates the thing that has one, and the row it writes records it; but the type is a column of that row rather than part of the key it targets.
That is what makes `remove("Generator", ["x"])` followed by `add("Bus", frame)` collapse to the later edit: one name has one answer, where a type-partitioned key would keep both and commit a record whose two types share a name.

The crucial property: **an edit is expressed in the format's own terms.** Setting `p_nom` on twenty components _is_ twenty `inputs/p_nom.parquet` rows, which is what a patch layer would hold anyway.
So a staged edit is already the row it will be written as, and `commit()` is a concatenation rather than a translation.

### 11.2 `set`

```python
record.set("p_nom", 150.0, names=["wind1", "wind2"])  # broadcast
record.set("p_nom", [150.0, 80.0], names=["wind1", "wind2"])  # per name
record.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per name, keyed
record.set("p_max_pu", frame, names=["wind1"])  # long frame
record.set("efficiency", 0.9, names=["dc"], bus="north")  # a connection
record.set("p_nom", 200.0, names=["wind1"], scenario="high")  # scoped
record.set("p_nom", nw.col("value") * 1.1, names=["wind1"])  # derived (§11.3)
record.set("p", solved, kind="outputs")  # a result (§9.4)
```

**There is no `component_type` keyword.** A name identifies one component across every type (§3.5), so the type is a property of the name rather than something the caller supplies: the record looks it up in the resolved components map, which is the same read `names` is already checked against (§11.8).
That removes the parameter that had to be either given or inferred in every earlier spelling, and with it the class of error where a name was staged under the wrong type.

One call may therefore span types, since the names decide: `set("p_nom", {"wind1": 150.0, "link_dc": 80.0})` validates `wind1` against `Generator.p_nom` and `link_dc` against `Link.p_nom`, and stages both.
Each name is validated against **its own** type's `AttributeSpec` (§11.8), so an attribute one type declares and another does not is an error naming the name that caused it.

`names=None` means every component the record resolves that the schema declares this attribute for — the types declaring `attribute`, not every type.
`set("p_max_pu", 0.9)` is "every component with a `p_max_pu`", which is the only reading left once the type keyword is gone, and the useful one.

`bus` names a connection rather than the component (§6); every other keyword is a dim, so `scenario="high"` scopes the edit and its absence means "every scenario" by the NULL broadcast rule.

`kind` names the destination in the format's own terms — §11.1's table is a mapping from edit to destination, and this makes that destination the parameter it was always implicitly carrying.
`"outputs"` stages into `outputs/` instead of `inputs/`, which is how a tool hands results back to a record before it is committed (§11.2.1).

`value` takes five forms, because assigning one value to a group and assigning a different value to each member are equally ordinary and neither should require building a frame:

| `value`   | meaning                         | `names`                                       |
| --------- | ------------------------------- | --------------------------------------------- |
| scalar    | broadcast to every name         | required unless `None` means all              |
| sequence  | aligned positionally to `names` | required, same length                         |
| mapping   | keys are names                  | ignored if given, else the keys are the names |
| frame     | supplies its own keys           | redundant                                     |
| `nw.Expr` | a function of the current value | selects what to derive from (§11.3)           |

A frame "supplies its own keys" now means its `name` column alone: a `component_type` column is neither required nor read, since the name determines the type (§3.5).
A frame carrying one is rejected rather than ignored — it says the writer believes the type is part of the key, and silently dropping the column would let a genuine disagreement through.

The first three normalise to a long frame before staging, so there is one staging path.
A length mismatch between a sequence and `names` is an error at the call, not a silently truncated edit.

Every form is checked against the components the record resolves (§11.8), the frame form included: "supplies its own keys" decides where the names come from, not whether they have to exist.

A one-dimensional labelled series is genuinely ambiguous: its index may hold names or axis labels.
Index dtype does not settle it, since an axis label may be a string like a name, so the tie is broken by membership — an index whose labels are all resolved axis values is a series, otherwise a mapping over names — and an index matching both is rejected rather than guessed.

`names=None` means every component of that type the record currently resolves, which is a read, so it includes earlier pending edits.

### 11.3 An `nw.Expr` value — derived from the current one

```python
record.set("p_nom", nw.col("value") * 1.1)  # scale up every p_nom
record.set("p_max_pu", nw.col("value").clip(upper=0.9), names=["wind1"])
```

A fifth `value` form rather than a second method.
Nothing else a caller passes is an `nw.Expr`, so the dispatch is unambiguous — unlike the series-versus-mapping tie §11.2 has to break by membership.

What it does differently is read before it stages:

- What it derives from is the resolved value **including earlier pending edits** (§11.10), so two such calls compose.
- Where the other forms stage without touching parent data, this one must resolve the keys it targets first.
  On a layered record that is a fold, so a broad derived edit is the one edit whose cost scales with the ancestry rather than with the rows written.
- What is staged is the _result_, not the expression.
  So a committed layer holds ordinary rows, and nothing in the format records that a value was derived — replaying an edit sequence is not a thing the record supports.

The expression is evaluated by narwhals against the resolved long frame, so it names `value` rather than the attribute: the frame is long, and one attribute per call means the column is always `value`.

**A named target must resolve to a row.**
If the caller names `names`, a `bus` or any dim scope, every one of those targets must produce a row to derive from, or the call raises.
The caller asked for those rows to take a new value and there is nothing to compute one from, which is a failed change rather than a no-op — the same class of error as naming a component no layer declares (§11.8), and it was silently staging zero rows before.

With `names=None` and no scope the instruction is "whatever resolves", so an empty result is an answer rather than a failure.
That asymmetry is the whole of the rule: a broad derived edit over a type where only some members carry the attribute is ordinary, while a targeted one that hits nothing is a typo.

### 11.3.1 Results through `kind="outputs"`

A tool solves against a record and hands back what it computed:

```python
record = WorkingRecord(record, con)
record.set("p_max_pu", 0.8, names=["wind1"])
model = PyPSA.build(record)  # solve the edited record
model.optimize()
for attr, frame in PyPSA.results(model).items():
    record.set(attr, frame, kind="outputs")
record.commit(NewChild(record))  # one layer, inputs and results together
```

In memory only: the results live in the staging area beside the input edits and become part of the same layer at commit, so a solve produces one new record rather than a record plus a separate results record.
Nothing on disk is mutated, and §8.1 stands unchanged.

Two things differ from an input edit, both following from §9.4:

- **No schema check on the attribute name.**
  A result attribute is not schema-declared — `Tool.results` derives which attributes count as results from the framework's own registry (§12), and `write_record` persists `outputs/` without consulting the schema.
  So an unknown attribute name is an error for an input and simply unknowable for a result.
  The dim vocabulary is still checked for both.
- **No membership check on `name`.**
  An input value for a name no layer declares is rejected, because it would resolve to nothing (§11.8).
  A result may legitimately name a component the record never declared: PyPSA's `SubNetwork` exists only after a solve, so rejecting it would refuse a real result.
  This is also what makes a result's name need no resolvable type: an input's type comes from looking the name up (§11.2), and a result that declares no member has nothing to look up.
- **No `_restated` completion at commit.**
  Results are complete as produced rather than a partial override of a parent's (§5.5), so there is nothing to carry forward from the base.

Keeping results coherent with the inputs they were computed from is the caller's business.
Editing an input after attaching results leaves results describing a record that no longer exists, and nothing here silently discards them — a record that dropped them on the next `set` would be guessing at which of the two the caller meant to keep.

### 11.4 Accessors — **not implemented**

`set` is the whole of the edit API.
This section is the intended spelling for an accessor over it, not something the package provides.

```python
record["Generator"]["p_nom"] = 150.0  # every generator
record["Generator"]["p_nom", ["wind1", "wind2"]] = [150.0, 80.0]
record["Generator"]["p_max_pu", "wind1"] = series
record["Link", "north"]["efficiency", "dc"] = 0.9  # a connection
record["Generator", {"scenario": "high"}]["p_nom", "wind1"] = 200.0
```

The component type in the subscript is a **scope**, not part of the key it writes: it selects which members `names` resolves against and which `AttributeSpec` a bare attribute means, then `set` addresses the names it produced (§3.5).
So `record["Generator"]["p_nom"] = 150.0` is "every Generator", which `set("p_nom", 150.0)` alone cannot say — that being the one thing an accessor would add now that the keyword is gone, and the reason this spelling survives the change.

Sugar with **no added capability** otherwise: `__setitem__` normalises its key into `(attribute, names)` and its extra arguments into `bus=`/dims, then calls `set`.
Keeping the method as the protocol member and any accessor on top is deliberate — `set` is what an implementation provides and other code calls, so a spelling over it can change, or not exist, without touching an implementation.

It reads as well as writes, since a `WorkingRecord` is a `Record`: `record["Generator"]["p_nom"]` returns that type's resolved frame, so getter and setter are symmetric and the accessor is a component-type view rather than a write-only handle.
The read must be scoped by both the component type and the names — an accessor whose getter ignores either is not the view this describes.

It deliberately does not reproduce a dataframe library's full indexing grammar — no boolean masks, no slices — because a record is not a dataframe and a partial imitation invites the assumption that the rest works.
Omitting `names` is how "all" is spelled.

### 11.5 `add` / `remove`

```python
record.add("Generator", frame)  # wide, in dims/components/ shape
record.remove("Generator", ["old_coal"])
record.remove("Generator", ["old_coal"], scenario="high")  # one scenario only
```

`add` takes a wide frame and splits it: attributes varying over nothing stay in `dims/components/`, varying ones become `inputs/` rows, per §3.1.
Which is which comes from the schema, so `add` needs no framework registry.
A column the schema does not name is written to `dims/components/` unchanged.

`add` keeps its `ctype` argument where `set` loses it: this is the call that _establishes_ what a name's type is, so there is nothing yet to look it up in (§3.5).
It is also where uniqueness is enforced — a name the record already resolves, under this type or any other, is rejected here rather than at commit, so the collision is reported at the line that introduces it (§11.8).

It is **not** a sequence of `set` calls, even though the varying columns it stages take the same path a `set` would.
`set` writes `inputs/` rows only, and a component exists by virtue of its `dims/components/` row: staging attribute values for a name no layer declares is precisely what §11.8 rejects.
Adding a bus with no attributes makes the point — nothing to `set`, yet the bus must exist.
Membership is not reducible to attribute values.

`remove` stages a tombstone, scoped by whichever component key dims the keywords name.
It need not enumerate what it deletes: one row per key, and the fold applies it to every attribute.

### 11.6 `pending`

```python
@dataclass(frozen=True)
class Pending:
    attributes: Mapping[str, int]  # attribute -> staged row count
    components: Mapping[str, int]
    connections: Mapping[str, int]
    tombstones: Mapping[str, int]

    def __bool__(self) -> bool: ...
```

A **derived summary, not a second place rows live**: the counts are a `GROUP BY` over the staging tables (§11.9), computed on access and discarded.
There is one staging layer and it is in DuckDB, so a hundred-thousand-row edit yields a `Pending` of a few integers.

### 11.7 Committing

```python
Target = NewChild | Directory
```

- **`NewChild(record)`** — create a child of `record` and write the staged rows as its layer.
  The patch-layer path: read a parent, edit, commit a child.
  Any node may be a parent (§8.1), so this needs no preparation of the one being branched from.
- **`Directory(uri)`** — write a standalone record.
  What is staged _plus what the record already reads_, flattened into one layer.

The two write different things.
A `NewChild` writes **only the edits** — that is what a patch layer is, and the fold resolves the rest from the parent.
A `Directory` writes **the resolved result**, since there is no parent to resolve against.
Both go through `write_record`, which is possible because each reading is presented as a `Record` — the one place the protocol's several implementations earn it twice over.

Neither carries the **base's** results across.
An edit changes the inputs a result was computed from, so a parent's `outputs/` says nothing about the child — results belong to the node that was solved, and a node with different inputs is a different node.

What a commit does carry is results **staged into this record** through `set(..., kind="outputs")` (§11.3.1).
Those were computed against these pending inputs, so they describe exactly the layer being written, and both readings write them: a `NewChild` layer holds its edits and the results computed from them together.

Staged rows are appended, never updated, so the same coordinate may be staged repeatedly.
Commit collapses to last-write-wins per coordinate, which is what `ROW_NUMBER() OVER (PARTITION BY <coordinate> ORDER BY _seq DESC) = 1` gives when every staged row carries a monotonic `_seq`.

Per **coordinate**, not per ownership key: the ownership key excludes the dims an attribute is not owned per (§5.5), so partitioning on it would collapse a whole staged series into one row — two edits at different snapshots are two coordinates, not two writes to the same place.
The same distinction governs the read overlay (§11.10) and the restate below, and it is the one thing easy to get wrong here.

Three interactions need stating, because each is where a naive append is wrong:

- **`remove` after `set`** on the same component: the tombstone wins regardless of sequence, since a deleted component has no attributes.
  Commit drops staged attribute rows for tombstoned keys.
- **`add` after `remove`** of the same name: the component exists again.
  Commit must not write both a member row and a tombstone — the later operation wins.
- **`set` on a component this record also added**: correct as-is, since the two live in different files.

The non-`partial` rule (§5.5) is the subtle one.
Overwriting one value along a non-partial axis means the layer must carry that component's _whole_ extent along it, so such a `set` must at commit read the resolved series for that key and write it out complete.
That is the one commit-time read of parent data.

### 11.8 Validation

`write_record` validates structurally, so commit inherits that.
What editing adds is edit-level: an `add` whose frame lacks `name`, an `add` whose name collides with one the record already resolves (§3.5), a `set` naming a component the record does not resolve, a dim keyword the schema does not declare.
These are caught when the edit is **staged**, not at commit — a caller should learn about a typo'd attribute at the line that typed it, not fifty edits later.

A `set` resolves each name to its type before checking anything else, so "no member row for `wind9`" and "`Generator` declares no `p_nom_maxx`" are both reported against the name that produced them.
The membership read this needs is the one §11.2 already performs, so deriving the type costs nothing beyond it.

### 11.9 Staging

Staged rows live in DuckDB tables on the record's own connection:

```sql
CREATE TABLE staged_inputs_<id>      (<long schema>, _seq BIGINT);          -- no component_type
CREATE TABLE staged_components_<id>  (<component columns>, deleted BOOLEAN, _seq BIGINT);
CREATE TABLE staged_connections_<id> (<connection columns>, deleted BOOLEAN, _seq BIGINT);
```

The staged rows are the format's own rows (§11.1), so `staged_inputs` loses `component_type` exactly as `inputs/` does, and the entity tables keep it.

These tables are the **only** place a staged row exists: `pending` counts them and the reads of §11.10 fold them, neither holding a copy.

DuckDB rather than in-memory objects, for three reasons that all matter: the reads are already a fold, so staging elsewhere would mean marshalling every edit into a relation on every read; a large edit is a bulk insert rather than ten thousand Python objects; and commit becomes one window-function query whose result `write_record` consumes unmaterialised.

Connection-scoped, like the owner-map cache, so they vanish with the connection and never appear on disk.
A record whose edits must survive a process boundary should commit.

`_seq` is assigned per edit call, not per row, so an edit's rows collapse together and edit order is what last-write-wins means.

### 11.10 Reading with pending edits

The inherited `Record` members must reflect the edits; otherwise `set` then read gives the old value, which no caller would expect.

A set of pending edits **is** a layer — an unwritten one.
So the reads compose the same way: the staged rows are the last layer, resolved over whatever the record was reading before.

```
resolved = fold(parent layers..., staged rows)
```

For a layered mutable record this is exactly one more fold step over the same owner-map machinery, with the staging tables standing in for a layer directory.
It costs what one more layer costs.
`flags` follows: a staged edge setting a dim adds it to `varies`, one leaving it NULL adds it to `broadcast`, and a staged curve sets `breakpoints` — unioned with the underlying answer.

## 12. Consuming a record

A **tool** is the framework-specific translation target built from a record.
The call runs from the tool inward:

```python
class Tool(Protocol):
    name: str
    schema: Schema  # attribute mapping

    def requires(self, record: Record) -> Requirements: ...
    def verify(self, record: Record) -> Requirements: ...  # falsy when usable
    def build(self, record: Record) -> Any: ...
    def to_datarecord(self, model: Any) -> Record: ...  # inverse of build
    def results(self, model: Any) -> Frames: ...
```

`results` returns `Frames` — the same type `Record.outputs` presents, keyed by attribute, each frame in the long schema (§3.2).
So a tool's results go straight to `write_record`, or one at a time to `set(attr, frame, kind="outputs")` with no key to unpack (§11.3.1).

A framework holds its results per component type, so reaching this shape means concatenating each attribute's types into one frame.
That is free: the frames are lazy, so the union is a plan rather than a copy, and nothing materialises until a caller collects.
The concatenation needs no `component_type` column to distinguish the arms, since `name` is unique across them (§3.5) — which is what makes the union a plain one rather than a tagged one.

Lazy is what the protocol asks for rather than what any implementation must do.
A tool reshaping a solved model's in-memory containers has nothing to defer and wraps its eager frames with `.lazy()`; one that could fetch a result attribute from a solver on demand is free to, and a caller wanting three of forty then pays for three.
That is §4.2's argument, on the write side.

A record is the input to a translation, not the owner of one, so there is no registry and no name dispatch: a tool is a module-level singleton reached by importing it, `build` returns the framework's own type, and nothing in the record layer imports a tool.

`build` takes a `Record` rather than a record, so a tool builds from a directory as readily as from an overlay and has no reason to know layering exists.

A tool's `verify` catches what the record layer cannot: a component type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks the framework's constant-versus-varying split.
It is also where a framework scoping names **per type** meets a record scoping them record-wide (§3.5).
PyPSA permits a `Bus` and a `Generator` both called `north`, so `to_datarecord` reports such a network as unbuildable rather than writing a record whose two components share one key.
Reported rather than repaired: renaming to `Generator:north` would hand back a network whose components the framework can no longer find by their own names, and the record layer does not own a framework's vocabulary.
PyPSA is itself moving to record-wide unique names, so this is a constraint that resolves rather than one to design around.
It is also where bus-keyed connections are collapsed back to a framework's positional encoding, ordered by `order_key`, and where a curve is either translated or reported unbuildable.

The tool's own `Schema` reconciles vocabularies: per component type, which record attribute a tool's attribute is renamed from, or which several it is computed from.
Since §5's schema makes a record's attribute names _declared_ rather than conventional, this maps one declared vocabulary to another.

Frames are built and handed over one component type at a time, so peak memory is one type's frames rather than the whole model.

## 13. Module layout

The target, once the split below is done:

```
datarecord/                     # the standalone concept
├── schema.py                   # Dimension, AttributeSpec, Schema
├── record.py                   # Record, Frames, LazyFrames, Flags
├── directory.py                # DirectoryRecord
├── mutable.py                  # WorkingRecord, the edit/commit path
├── layered/                    # LayeredRecord and its resolution
│   ├── revision.py             # Revision, the node tree
│   ├── resolve.py              # owner-map fold
│   └── write.py                # write_record
└── duck.py                     # connection setup, path derivation
```

§1's "depends on `duckdb`, `narwhals` and `pydantic`, and on nothing else" is achieved at this layer — `datarecord/tools/` is where a framework-specific tool lives instead (below), and nothing under it is imported by the package's own `__init__.py`.

The protocol lives with its implementations rather than with any one consumer, because there are several: `write_record` consumes a `Record`, a tool both implements and consumes one, and `WorkingRecord` satisfies it.

A tool lives outside this core, under `datarecord/tools/<name>.py` — one module per modelling framework, imported explicitly (`from datarecord.tools.pypsa import PyPSA`) rather than through `datarecord.tools` itself, which imports none of them.
What decides the side is one question: **does it name a modelling framework?** Nothing in `datarecord`'s core may.
The dependency runs strictly one way, so importing the record layer pulls in no framework, and importing one tool pulls in no other.

## 14. Open questions

- **What a component tombstone means for a connection keyed by fewer dims.** When `component` keys `scenario` and `connection` does not, deleting a component in one scenario removes a connection that is not scenario-scoped, even though the component survives elsewhere.
  The conservative reading — project the tombstone down to the shared dims — is implemented; deciding it properly needs the folded components map, which the connections fold cannot reach.
  Low priority while no framework allows scenario-varying connections.

- **Whether `within` should subsume `bus`.** `bus` and a nested dim express the same relation.
  A `timestep` label identifies a point only within a `period`; a `bus` label identifies a connection only within a component — `"north"` alone names nothing, since every component may attach to `north`, while `(link_dc, north)` names one connection.
  Written as a dim that would be `Dimension(dtype="str", within={"name"})`, and `bus` would stop being a hardcoded key column.
  §3.5 strengthens the analogy rather than weakening it: `name` is now a single global axis rather than one qualified by `component_type`, so `within={"name"}` names something well-defined where `within={"component_type", "name"}` would have been the awkward spelling.

  What blocks it is that `bus` inverts the rule NULL follows for a dim.
  A NULL declared dim means "all values", and the fold expands it against the axis (§9.2); a NULL `bus` means "this attribute belongs to the component rather than to any connection", and is compared NULL-safely, never expanded.
  So `bus` would be a dim carrying an explicit exception to the one behaviour that makes a dim a dim.
  With one instance of each relation in hand there is nothing to generalise against, and unifying them would touch every key and every NULL comparison.

- **Whether `partial` should ever be per attribute.** §5.5 puts it on the axis because it is true of every attribute varying over that axis.
  A counter-example would be an attribute whose series a consumer _can_ accept in pieces while others cannot — none known, and permitting it would make the fold's key vary per attribute, which the fixed inputs key assumes it does not.

- **Whether staged results should be invalidated by a later input edit.** Results attached through `set(..., kind="outputs")` (§11.3.1) were computed from the inputs pending at that moment, so editing an input afterwards leaves them describing a record that no longer exists.
  Dropping them on the next input edit was considered and rejected: it silently discards work the caller may have wanted, and a record that guesses which of the two the caller meant to keep is worse than one that keeps both and says so.
  Coherence is the caller's business, and a commit writes whatever is staged.
  If this bites in practice, a `pending`-level warning is the cheap next step rather than a silent truncation.

- **Whether a `WorkingRecord` over an open record stages against a snapshot.** Writing into an open record invalidates its owner-map cache.
  A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
  The second is simpler and arguably more correct — an edit sequence should not see another writer's changes mid-flight — but it means a record can go stale.

- **Registering a record's relations as named views.** A frontend issuing ad-hoc SQL needs names in a catalog rather than Python objects, which `CREATE VIEW` against a file-backed catalog provides — each view's definition being the resolved overlay, materialising nothing.
  Creating a view binds its schema, so registering N attributes costs N footer reads; and catalog reopen cost is linear in view count, which argues for one catalog per record rather than one shared.
