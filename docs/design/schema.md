# The schema

One schema per record, and `manifest.json` is how it is written down — the two words name the same thing, the file and the object.

```python
class Dimension(BaseModel):
    """One axis attribute data may vary over."""

    dtype: str  # the axis labels' type
    within: frozenset[str] = frozenset()  # labels unique only within these dims
    keys: frozenset[KeyKind] = ...  # entity tables this dim keys
    unit: str | None = None  # what the labels measure, if anything
    description: str | None = None  # what the axis is, in prose


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take."""

    dtype: str  # value column type
    dims: frozenset[str] = frozenset()  # dims it may vary over; subset of declared
    default: Any | None = None
    breakpoints: bool = False  # may carry a piecewise-linear curve
    bus: BusRelation = "component"  # "component" | "connection"
    unit: str | None = None  # what the values measure
    description: str | None = None  # what the attribute is, in prose


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations

    dimensions: dict[str, Dimension]
    attributes: dict[
        str, dict[str, AttributeSpec]
    ]  # component type -> attribute -> spec

    # Which dims a layer may patch value by value; absent for a record with no
    # layers, since nothing overrides anything.
    partial: frozenset[str] | None = None

    meta: dict[str, Any] = {}  # opaque; the package never interprets it
```

`partial` is the only layering-specific part, and so the only optional one.
Everything else describes the data and is always present.

`component_type`, `name` and `attribute` are `VARCHAR`: those vocabularies belong to a modelling framework, and this package knows none.
A type no tool recognises reads back fine and is reported by the tool that cannot build it, not rejected inside the fold.

`meta` is where a framework's own top-level data goes — network attributes, coordinate reference system, free-form metadata.
It is stored and never interpreted, since none of it describes the dimensioned data.

## Dimensions

Every dim is declared: a record with `region`, `technology` or `vintage` needs no code change, and `dtype` is the axis's own property.

A `Dimension` declares the axis's shape — its type, its [nesting](#within-an-axis-inside-an-axis), [which entity tables it keys](#keys-which-entity-tables-a-dim-keys).
It does not declare which dims an _attribute_ varies over (that is [per attribute](#attributespec)), nor [the patch granularity](#partial-the-granularity-of-an-override), nor [order](record.md#axis-order).

## `AttributeSpec`

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
Varying over nothing is also what puts both in `dims/components/` rather than `inputs/` ([where a value lives](format.md#where-a-value-lives)), so the schema decides the file split rather than a writer guessing it.

The rest answers what a bare column set cannot:

- _May it carry breakpoints?_ — `breakpoints`, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later ([wide and long rows](record.md#wide-and-long-rows)).
- _Is it bus-relative?_ — `bus`, so `efficiency` is known to be a [connection](record.md#connections) attribute and `p_max_pu` a component one, rather than inferred from whether a `bus` value happens to be present.

## `keys` — which entity tables a dim keys

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
[Tombstone scoping](layers.md#deletion) is a consequence of that key rather than its purpose.

A dim in `keys` must be in `partial` where that section exists, since keying membership per value of an axis that is only ever owned whole has no meaning.

## `within` — an axis inside an axis

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

## `partial` — the granularity of an override

Everything is overridable; a layer exists in order to override.
The remaining question is at what granularity along each axis, and it splits from [`AttributeSpec`](#attributespec)'s question because the two are properties of different things:

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

```text
owned_per(attribute) = attribute.dims ∩ schema.partial
```

So `p_max_pu` is owned per scenario — `timestep` is not partial, so a patch to one hour restates that scenario's whole series; `marginal_cost` per scenario; `p_nom` and `carrier` once, across everything.

Two things this buys.
The schema can distinguish `p_max_pu` from `p_nom`, which a dim-level flag cannot: that would say every attribute is owned per scenario, including those a scenario must not change.
And a `p_nom` row carrying a non-NULL `scenario` becomes a write-time violation rather than something the NULL-broadcast rule absorbs — a first-stage decision quietly turned into a per-scenario one is the error worth catching.

What the fold does with this is unchanged: the inputs key is one fixed tuple over all attributes, and an attribute not varying over a dim writes NULL there.
So the declarations constrain and validate; they do not make the key vary per row.

## One schema per record

Not one per layer.
A directory record's schema is `manifest.json` in the directory; a layered record's lives **beside** the layers, not inside any of them:

```text
record-root/
├── manifest.json               # the schema — one, for the whole tree
└── layers/<uuid>/              # a layer: dims/, inputs/, outputs/ — no manifest
    └── resolved/               # caches (owner map, resolved dims)
```

A schema is not layered data.
Folding it would let a layer change what `p_nom` _means_ — its dtype, which dims it varies over — which is not a patch to data but a redefinition of the thing being patched, and it makes the schema unknowable without walking the ancestry.
One schema makes it a property of the record, validatable before anything is read and stated once for a hundred-layer tree.

The cost is that adding an attribute amends the root schema rather than shipping inside the layer that introduces it.
That is the right trade: a new attribute is a schema change, and one buried several layers deep is exactly what should be visible.

A layer directory therefore holds only data, which is what keeps it a plain parquet directory readable by a tool that knows nothing about layering.

## Versioning

One schema outlives many layers ([above](#one-schema-per-record)), so a change to it meets data written under the previous one.
`version` records which schema a record's layers were written against, and what matters is which changes existing layers survive.

**Compatible** — old layers stay readable, `version` bumps and nothing else happens:

- adding an attribute, or a component type
- adding a dim no existing attribute varies over
- widening an `AttributeSpec.dims`: rows that set fewer dims still decode, since an unset dim is NULL and NULL means "all values" ([the broadcast rule](record.md#the-broadcast-rule))
- adding to `partial`: ownership becomes finer, and an existing layer's rows are simply owned at the coarser granularity they were written with
- changing a [`unit` or `description`](#unit-and-description), which describe the data without deciding how any row decodes

**Incompatible** — existing rows would decode differently, or not at all:

- narrowing `dims`, since a row setting a now-undeclared dim has no valid reading
- changing a `dtype`
- removing from `partial`: a layer that patched one value along that axis is now a partial override of an axis owned whole, which is exactly the hole [`partial`](#partial-the-granularity-of-an-override) forbids
- changing `within`, since the axis key changes shape
- adding to a dim's `keys`, since an entity table gains a key column its existing rows do not carry

The compatible changes are those where NULL already means what the new schema needs it to mean, so [the broadcast rule](record.md#the-broadcast-rule) absorbs them without touching a row.

An incompatible change therefore needs the layers rewritten rather than the schema edited, which for a layered record means flattening to a [`Directory`](working-record.md#committing) under the new schema.
A reader encountering a `version` it was not written for should refuse rather than guess, since every failure above is silent.

## `unit` and `description`

Both a `Dimension` and an `AttributeSpec` may carry a `unit` and a `description`.
Neither is interpreted: no conversion, no dimensional analysis, no validation that `MW` and `kW` are not being added.
They are stored, read back, and handed to whatever displays or documents the record.

They belong in the schema rather than in `meta` because they describe the _dimensioned data_ — which is exactly the line `meta` is on the other side of.
A `unit` is a property of an attribute in the same way its `dtype` is, and a consumer asking "what is `p_nom` and what is it measured in" should not have to know a framework's own metadata layout to find out.

`None` means undeclared, not dimensionless.
A quantity that genuinely has no unit is `""` — the distinction matters to a renderer choosing between showing nothing and showing an empty unit, and to a later pass that wants to find what is still undocumented.

A dimension's `unit` describes what its _labels_ measure, which is only sometimes meaningful: a `vintage` axis labelled in years or a `distance` axis in km has one, while `scenario` and `timestep` do not — a timestamp is not a quantity.
`description` applies to any axis.

Neither field changes how a row decodes, so adding or editing one is a [compatible change](#versioning).
