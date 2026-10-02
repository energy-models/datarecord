<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

One schema per record, and `manifest.json` is how it is written down — the two words name the same thing, the file and the object.

```python
class Dimension(BaseModel):
    """One axis attribute data may vary over."""

    dtype: str  # the axis labels' type
    within: frozenset[str] = frozenset()  # labels unique only within these dims
    unit: str | None = None  # what the labels measure, if anything
    description: str | None = None  # what the axis is, in prose


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take."""

    dtype: str  # value column type
    dims: frozenset[str] = frozenset()  # the dims it is over
    default: Any | None = None
    breakpoints: bool = False  # may carry a piecewise-linear curve
    unit: str | None = None  # what the values measure
    description: str | None = None  # what the attribute is, in prose


class Relation(BaseModel):
    """Which tuples over several dims exist: a sparse subset of a dim product."""

    key: dict[str, str]  # coordinate name -> the dim it draws labels from
    values: str | None = None  # each `key` tuple carries exactly one label of it
    description: str | None = None


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations

    dimensions: dict[str, Dimension]
    attributes: dict[str, AttributeSpec]  # flat: one attribute, one spec
    relations: dict[str, Relation]

    # Which dims a layer may patch value by value; absent for a record with no
    # layers, since nothing overrides anything.
    partial: frozenset[str] | None = None

    meta: dict[str, Any] = {}  # opaque; the package never interprets it
```

`partial` is the only layering-specific part, and so the only optional one.
Everything else describes the data and is always present.

`entity` and `attribute` are `VARCHAR`: those vocabularies belong to a modelling framework, and this package knows none.
A [type](#types) is a label of a declared dim, typed as the schema declares it. So a type _the schema declares_ but no model uses reads back fine; which types a model uses is the spec's to say ([requirements](sources.md#requirements)).

`meta` is where a framework's own top-level data goes — network attributes, coordinate reference system, free-form metadata.
It is stored and never interpreted, since none of it describes the dimensioned data.

## Dimensions

Every dim is declared: a record with `region`, `technology` or `vintage` needs no code change, and `dtype` is the axis's own property.

A `Dimension` declares the axis's shape — its type and its [nesting](#within-an-axis-inside-an-axis).
It does not declare which dims an _attribute_ varies over (that is [per attribute](#attributespec)), nor [the patch granularity](#partial-the-granularity-of-an-override), nor [order](record.md#axis-order), nor what classifies it — [a relation with it as `values`](#values-a-relation-that-classifies) says that, from the relation's side.

## `AttributeSpec`

What one attribute may do over those axes:

```python
attributes = {
    # a capacity to build: one decision, evaluated against every scenario
    "p_nom": AttributeSpec(dtype="float64", dims={"entity"}),
    # an availability profile: varies over time, and per scenario
    "p_max_pu": AttributeSpec(dtype="float64", dims={"entity", "scenario", "timestep"}),
    "marginal_cost": AttributeSpec(
        dtype="float64", dims={"entity", "scenario"}, breakpoints=True
    ),
    "carrier": AttributeSpec(dtype="str", dims={"entity"}),
    # over a dim of its own: one port per attachment of a component to a bus
    "efficiency": AttributeSpec(dtype="float64", dims={"port", "timestep"}),
    # over an axis alone: a weighting belongs to no component
    "objective_weighting": AttributeSpec(dtype="float64", dims={"snapshot"}),
}
```

**Flat, one spec per attribute.** An attribute belongs to no [type](#types): an attribute over `entity` can be set on any entity.
`attributes/p_max_pu.parquet` holds the rows of every entity in one file with one `value` dtype, so one attribute is one spec, one file and one dtype.

**`dims` names dims only**, as the `dims` of a mathspec parameter do. The schema refuses a [relation](#relations) there.
Every coordinate of an attribute is a dim, the entity axis included, and a NULL in any of them [broadcasts](record.md#the-broadcast-rule).
Data on a relation's rows goes over a dim of its own: `efficiency` is per port because its `dims` name `port`, a dim with one label per attachment of a component to a bus ([data on a relation's rows](#data-on-a-relations-rows)).

`dims` is also what makes a scenario-varying `p_nom` a schema violation: a capacity is a first-stage decision, one value taken before the scenario is known, which is the point of stochastic scenarios differing only in dispatch.

`p_nom` and `carrier` have the same `dims` and are not the same kind of thing — one is a label, the other a number an optimiser decides.
`dims` says only which dims a value is over.

`breakpoints` answers what a bare column set cannot: whether it may carry a piecewise-linear curve, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later ([wide and long rows](record.md#wide-and-long-rows)).

An attribute over exactly one dim is a column of that dim's own file, and anything more is long rows in `attributes/` — [where a value lives](format.md#where-a-value-lives) is the rule, and it is the schema that decides the file split rather than a writer guessing it.

## Types

A type is the [`values`](#values-a-relation-that-classifies) of a relation keyed by `entity`. A PyPSA-shaped schema declares the relation `entity_type` keyed by `entity`, with the dim `entity_type` as its values:

```python
dimensions = {
    "entity": Dimension(dtype="str"),
    "entity_type": Dimension(dtype=Enum(["Bus", "Generator", "Link"])),
}
relations = {
    "entity_type": Relation(key=["entity"], values="entity_type"),
}
```

`values` makes the relation functional, so each entity has one type. The rows `(entity, entity_type)` live in `relations/entity_type.parquet`, like the rows of any relation ([where a value lives](format.md#where-a-value-lives)).
An `Enum` dtype pins the vocabulary and makes an unknown type a write-time error; a plain `str` leaves the labels as data, which is the right declaration for a record whose types are not known up front.

**No relation is special.** Several functional relations keyed by `entity` may exist side by side, a type and a carrier for example, and the record treats each the same way. The name `entity_type` is the convention of the schema that declares it, not a word the record layer reads.

**A type does not narrow an attribute.** An attribute over `entity` can be set on any entity, whatever its type. Which attributes a type uses is the business of the spec that reads the record ([requirements](sources.md#requirements)).

**An attribute may not be over both the entity and its type.** The schema rejects an attribute that names both `entity` and `entity_type` in its `dims`: `values` says the type follows from the entity, so the row would be keyed twice over and the two could disagree.
This is the general rule for [a functional relation and its key](#values-a-relation-that-classifies); `country` keyed by `bus` is rejected the same way.

**A value per type is ordinary.** A per-type `icon` is an attribute over `entity_type` alone, so it is a column of `dims/entity_type.parquet`, like any [attribute over one dim alone](format.md#where-a-value-lives).
`entity_type` is an ordinary broadcast dim. Outside [`partial`](#partial-the-granularity-of-an-override), a layer that touches one type's icon restates the whole axis.

**Entirely optional.** A schema that declares no type relation has entities with no types.
A schema that needs types declares the relation.

## Relations

A relation declares **which tuples over several dims exist**: a sparse subset of a dim product, with its own order. Its rows are unique per `key`, and a relation with [`values`](#values-a-relation-that-classifies) also names one column those rows determine.

```python
relations = {
    "connection": Relation(key={"entity": "entity", "bus": "bus"}),
    "corridor": Relation(key={"from": "bus", "to": "bus"}),
    # functional: one country per bus
    "country": Relation(key=["bus"], values="country"),
}
```

Not a dim. A dim declares an axis of labels and a NULL in its column means "every value of it"; a relation declares which _combinations_ are there, which no axis can say because the product is sparse — a component attaches to two buses out of a thousand.

`key` maps **coordinate name → dim** rather than naming a bare set of dims, because two coordinates may draw on the same axis: a corridor between two nodes is `(from, to)`, which a set could not spell.
A list is sugar for the dict with identical keys and values, so `key=["bus"]` is `key={"bus": "bus"}`.

**A relation's file holds the relation's columns and its tombstones, and nothing else.** `relations/connection.parquet` is `entity | bus`, and `relations/corridor.parquet` is `from | to` ([where a value lives](format.md#where-a-value-lives)).
No attribute is over a relation, so no attribute is a column of one. Data on a relation's rows goes over a dim of its own ([data on a relation's rows](#data-on-a-relations-rows)).

**`partial` names every dim a relation is keyed by.** A layer adds or removes one relation row at a time, so the fold keys by those dims, and the schema refuses a `partial` that leaves one out ([`partial`](#partial-the-granularity-of-an-override)).
The `values` dim of a functional relation is not in its key, so the rule does not reach it: `country` is an ordinary axis whose NULL means "every country" like any other dim's.

**Connections are one instance**, not a structural category: `Relation(key={"entity": "entity", "bus": "bus"})`. `bus` is one coordinate of one relation rather than a column the format fixes, and neither word appears in the record layer. `connection` is whatever a schema calls it.

### `values` — a relation that classifies

`values` names the dim a relation is **functional into**: each tuple of `key` carries exactly one of its labels. `country` keyed by `[bus]` with `values="country"` says every bus is in one country.

It is a declaration a relation without `values` cannot make. Such a relation can only _happen_ to be single-valued, which leaves a duplicate row a data error the schema has no name for; `values` names it, so the constraint is declared and checkable on write. No existing system declares it — GAMS's `map(b,c)` is a set over a tuple of sets with single-valuedness left to convention, and a duplicated `b` silently double-counts — which is the argument for the field rather than against it, a schema whose purpose is making shape checkable having no reason to inherit that gap.

**`values` must name a declared dim.** That is what keeps the axis file, and the axis file is the whole of what a functional relation has over a bare tuple set: `dims/country.parquet` gives `country` its [order](record.md#axis-order) and somewhere for a per-country CO2 budget to live.

**`values` is sugar, resolved once at parse.** It folds into the relation's columns, so `relations/country.parquet` has the columns `bus | country` exactly as `connection` has `entity | bus`, and no read path, file layout or fold key branches on whether a relation has one. The field stays for the three things that still need it: the uniqueness constraint (the key being the columns minus `values`), a consumer's aggregation, and round-tripping the manifest — writing back `key: [bus, country]` where the author wrote `values:` would silently rewrite their schema.

**An attribute may not be over both the key and the `values` dim.** `dims={"bus", "country"}` is rejected: `values` says the country follows from the bus, so the row would be keyed twice over and the two free to disagree.

**Nothing assumes one coordinate.** `key: [bus, scenario]` with `values: country` — a bus whose country varies per scenario — is allowed, the constraint being per-tuple already. It costs nothing in storage because [every relation is a file](format.md#where-a-value-lives).

### Data on a relation's rows

An attribute is over dims only, as a mathspec parameter is. To hold data on the rows of a relation, give each row a dim of its own, relate that dim to the relation's columns, and declare the attribute over the dim.

In a PyPSA-shaped schema the rows are the attachments of components to buses. Each attachment is one label of a `port` dim, and two functional relations tie a port to its component and to its bus:

```python
dimensions = {
    "entity": Dimension(dtype="str"),
    "bus": Dimension(dtype="str"),
    "port": Dimension(dtype="str"),  # one label per attachment of a component to a bus
    "timestep": Dimension(dtype="datetime64[us]"),
}
relations = {
    "port_entity": Relation(key=["port"], values="entity"),
    "port_bus": Relation(key=["port"], values="bus"),
}
attributes = {
    "role": AttributeSpec(dtype="str", dims={"port"}),  # dims/port.parquet
    "efficiency": AttributeSpec(dtype="float64", dims={"port", "timestep"}),
}
partial = {"port"}  # both relations are keyed by it
```

mathspec's [PyPSA example](https://github.com/energy-models/mathspec/blob/main/examples/pypsa.yaml) declares the same shape: `link_output` is the dim, `Link_output_link` and `Link_output_bus` are the relations, and `Link_efficiency` is over `link_output`.

- **A port is an ordinary label.** `role` is over `port` alone, so it is a column of `dims/port.parquet`. `efficiency` is over `port` and `timestep`, so it is long rows in `attributes/efficiency.parquet`, keyed by `port | timestep` ([where a value lives](format.md#where-a-value-lives)).
- **A NULL `port` broadcasts** like a NULL in any other dim: the row covers every port ([the broadcast rule](record.md#the-broadcast-rule)).
- **A port is keyed by its own label, never by position.** A patch layer that adds a port adds a label, so no other port changes its name ([connections](record.md#connections)).
- **A `connection` relation holds topology alone.** A relation keyed by `(entity, bus)` says which components attach to which buses. No attribute is over it.
- **A relation may share a dim's name.** A name in `dims` is always the dim, so `dims={"country"}` is over the `country` dim whether or not a relation of that name exists. The `entity_type` relation and dim in [types](#types) are the common case.

The schema refuses an attribute over a relation, and the message names the rewrite:

```python
"efficiency": AttributeSpec(dtype="float64", dims={"connection", "timestep"})
```

```text
attribute 'efficiency' is over the relation 'connection'; an attribute is over dims only - give each row of 'connection' a dim of its own, relate that dim to 'connection''s columns, and declare 'efficiency' over it
```

## Existence does not vary along a dim

A component exists or it does not. There is no declaration making membership vary per value of an axis — no generator present in scenario `high` and absent from `low` — so `dims/entity.parquet` holds one row per entity and a tombstone removes it whole.

That is a deliberate narrowing. An earlier `Dimension.keys` declared exactly this, putting the dim in an entity table's key so a component existed per scenario; it is gone, with nothing in its place, because [what it should mean is unsettled](open-questions.md) once connections are one relation among several rather than a fixed second entity table.

What remains is the distinction that was doing the useful work: a **value** may vary along an axis where the **thing** may not. A stochastic network holding a different `capital_cost` per scenario is expressing exactly that, and it is an attribute with `dims = {"entity", "scenario"}` — long rows in `attributes/`, not a component that exists twice. [Where a value lives](format.md#where-a-value-lives) already places it.

## `within` — an axis inside an axis

A dim whose labels identify a point only _within_ another dim's value.
Multi-period time is the case: the axis is a `(period, timestep)` pair, so `t1` alone names nothing and two periods may hold different timesteps.

```python
dimensions = {
    "period": Dimension(dtype="int64"),
    "timestep": Dimension(dtype="datetime64[us]", within={"period"}),
}
```

`within` makes `timestep.parquet` carry a `period` column, and the axis key `(period, timestep)` rather than `timestep`.
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

### `within` is not `values`

Both are acyclic and both relate one dim to another. They mean opposite things:

- **`within`** names a dim's _parents_: its label set is **scoped per parent**, so `t1` in 2015 and `t1` in 2020 are different points and the axis key is `(period, timestep)`.
- **`values`** names the dim a _relation_ classifies its key by: one flat label set, each `key` tuple picking one of its labels. `country` is not scoped by `bus`; it is a partition of buses, and its axis key is `country` alone.

Nesting versus classification. `within` cannot express `country`, and a functional relation cannot express `timestep`.
`within` stays on `Dimension` because nesting is a property of the axis itself; `values` is on `Relation` because a classification is a relation, with rows.

A functional relation is **single-valued by declaration** rather than by construction, which is the whole of what `values` adds — a many-to-many classification is a relation without `values`, and expressible as one.

**A chain is not denormalised.** bus→state→country is two relation files, never a `country` column on `dims/bus.parquet`. Two files asserting bus→country would let a layer restating the states leave every bus's country stale with nothing to detect it, so the chain is a join over two files — which a file per relation gives for free, a layer restating a relation restating exactly one file.

**The record does not resolve across levels.** An attribute over `country` is handed back keyed by country; projecting it down to buses is a join through the relation, and that is the consumer's work. The fold learns no new operation.

Membership could not vary along a classification either, if it varied along anything: whether a component exists in Germany is already settled by its bus and that bus's country, so there would be no freedom for it to vary independently ([existence does not vary](#existence-does-not-vary-along-a-dim)).

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

The dims a layer owns an attribute per follow from the declarations:

```text
owned_per(attribute) = attribute.dims ∩ partial_dims
partial_dims         = schema.partial, in declaration order
```

`partial_dims` is the fold key: the dims declared `partial`, which include every dim a [relation](#relations) is keyed by — `entity` for the `entity_type` relation, `port` for `port_bus`.
So `p_max_pu` is owned per entity and per scenario — `timestep` is not partial, so a patch to one hour restates that entity-scenario's whole series; `marginal_cost` per entity and scenario; `p_nom` and `carrier` per entity, once across everything else.

Two things this buys.
The schema can distinguish `p_max_pu` from `p_nom`, which a dim-level flag cannot: that would say every attribute is owned per scenario, including those a scenario must not change.
And a `p_nom` row carrying a non-NULL `scenario` becomes a write-time violation rather than something the NULL-broadcast rule absorbs — a first-stage decision quietly turned into a per-scenario one is the error worth catching.

What the fold does with this is unchanged: the key of `attributes/` is one fixed tuple over all attributes, and an attribute not varying over a dim writes NULL there.
So the declarations constrain and validate; they do not make the key vary per row.

**An axis file is owned the same way**, and the [attributes it carries](format.md#where-a-value-lives) do not argue for adding it.
Outside `partial`, a layer touching an axis restates it whole — every label, with the static attributes attached to them — because a layer holding one label is not saying the others are unchanged but that they are not there: the fold keys by the axis key, so what this layer carries is what the axis has here.
That is the same "no half-owned extent" rule a series obeys, applied to a set of labels rather than a curve.

**Keep it small.** Every `partial` value dim widens the fold key, and the key is paid for by every read of every attribute.
The cost of leaving a value dim out is paid once per edit and bounded by the axis; the cost of putting it in is paid by every read forever.
So `partial` names the dims a relation is keyed by, and beside them only the value dims a layer patches value by value — `scenario`, not `timestep`.

## One schema per record

Not one per layer.
A directory record's schema is `manifest.json` in the directory; a layered record's lives **beside** the layers, not inside any of them:

```text
record-root/
├── manifest.json               # the schema — one, for the whole tree
└── layers/<uuid>/              # a layer: dims/, relations/, attributes/ — no manifest
    └── resolved/               # caches (owner map, resolved dims)
```

A schema is not layered data.
Folding it would let a layer change what `p_nom` _means_ — its dtype, which dims it varies over — which is not a patch to data but a redefinition of the thing being patched, and it makes the schema unknowable without walking the ancestry.
One schema makes it a property of the record, validatable before anything is read and stated once for a hundred-layer tree.

The cost is that adding an attribute amends the root schema rather than shipping inside the layer that introduces it.
That is the right trade: a new attribute is a schema change, and one buried several layers deep is exactly what should be visible.

A layer directory therefore holds only data, which is what keeps it a plain parquet directory readable by a tool that knows nothing about layering.

Which of the two a directory is decides what it is read under. A **standalone** record carries its own `manifest.json`, so `Record.at(uri)` reads that file and the record answers the same through any connection — it is one whole record, and may well be opened from somewhere that knows nothing about it. A single **layer** directory carries none, so it is read under the connection's root, which is the tree it belongs to.

An edit changes neither. A [`WorkingRecord`](working-record.md) is one more layer over its base, and a layer declares nothing — so it reads under whatever its base's schema is, standalone or not. A staging area that could redeclare would be the layered-schema case above, arriving by a different door.

## Versioning

One schema outlives many layers ([above](#one-schema-per-record)), so a change to it meets data written under the previous one.
`version` records which schema a record's layers were written against, and what matters is which changes existing layers survive.

**Compatible** — old layers stay readable, `version` bumps and nothing else happens:

- adding an attribute or a relation
- adding a dim no existing attribute varies over
- widening an `AttributeSpec.dims`: rows that set fewer dims still decode, since an unset dim is NULL and NULL means "all values" ([the broadcast rule](record.md#the-broadcast-rule))
- adding to `partial`: ownership becomes finer, and an existing layer's rows are simply owned at the coarser granularity they were written with
- changing a [`unit` or `description`](#unit-and-description), which describe the data without deciding how any row decodes

**Incompatible** — existing rows would decode differently, or not at all:

- narrowing `dims`, since a row setting a now-undeclared dim has no valid reading
- changing a `dtype`
- removing from `partial`: a layer that patched one value along that axis is now a partial override of an axis owned whole, which is exactly the hole [`partial`](#partial-the-granularity-of-an-override) forbids
- changing `within`, since the axis key changes shape

Adding a functional relation is compatible in the same sense adding any relation is: the record gains a file, and until some layer writes it every coordinate reads as unclassified — no row, which is what "no country assigned" means anyway.

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
