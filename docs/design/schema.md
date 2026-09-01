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
    dims: frozenset[str] = frozenset()  # what addresses it: dims and groups
    default: Any | None = None
    breakpoints: bool = False  # may carry a piecewise-linear curve
    unit: str | None = None  # what the values measure
    description: str | None = None  # what the attribute is, in prose


class Group(BaseModel):
    """Which tuples over several dims exist: a sparse subset of a dim product."""

    over: dict[str, str]  # coordinate name -> the dim it draws labels from
    into: str | None = None  # each `over` tuple carries exactly one label of it
    description: str | None = None


class Trait(BaseModel):
    """A bundle of attributes, and which entity types carry it."""

    attributes: frozenset[str] = frozenset()
    on: dict[str, frozenset[str]] = {}  # entity-type axis -> the labels it applies to
    description: str | None = None


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations

    dimensions: dict[str, Dimension]
    attributes: dict[str, AttributeSpec]  # flat: one attribute, one spec
    groups: dict[str, Group]
    traits: dict[str, Trait]  # the only thing that narrows an attribute to some types

    # Which dims a layer may patch value by value; absent for a record with no
    # layers, since nothing overrides anything.
    partial: frozenset[str] | None = None

    meta: dict[str, Any] = {}  # opaque; the package never interprets it
```

`partial` is the only layering-specific part, and so the only optional one.
Everything else describes the data and is always present.

`entity` and `attribute` are `VARCHAR`: those vocabularies belong to a modelling framework, and this package knows none.
The [entity-type axis](#entity_type-the-axis-of-kinds) is typed as the schema declares it, which is a record's own statement rather than a framework's — so a type _the schema declares_ but no tool recognises reads back fine and is reported by the tool that cannot build it, not rejected inside the fold.

`meta` is where a framework's own top-level data goes — network attributes, coordinate reference system, free-form metadata.
It is stored and never interpreted, since none of it describes the dimensioned data.

## Dimensions

Every dim is declared: a record with `region`, `technology` or `vintage` needs no code change, and `dtype` is the axis's own property.

A `Dimension` declares the axis's shape — its type and its [nesting](#within-an-axis-inside-an-axis).
It does not declare which dims an _attribute_ varies over (that is [per attribute](#attributespec)), nor [the patch granularity](#partial-the-granularity-of-an-override), nor [order](record.md#axis-order), nor what classifies it — [a group `into` it](#into-a-group-that-classifies) says that, from the relation's side.

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
    # addressed by a group rather than by the entity axis
    "efficiency": AttributeSpec(dtype="float64", dims={"connection", "timestep"}),
    # addressed by an axis alone: a weighting belongs to no component
    "objective_weighting": AttributeSpec(dtype="float64", dims={"snapshot"}),
}
```

**Flat, one spec per attribute**, with [component types subscribing](#traits) rather than owning.
The nesting this replaces said an attribute _belongs to_ a component type, which is false in both directions: `objective_weighting` has no type, and `p_max_pu` was three identical specs under Generator, Link and StorageUnit.

The storage already disagreed with the nesting. `inputs/p_max_pu.parquet` holds every type's rows in one file with one `value` dtype, so two types declaring one attribute with **different dtypes** was expressible in the schema and unrepresentable on disk — a silent wrong read that nothing rejected.
Flat declaration makes it unrepresentable instead, which is the stronger form of the same guarantee: one attribute, one spec, one file, one dtype.

`dims` is the **only addressing mechanism**, and it names dims and [groups](#groups) alike, resolved by [one rule](#addressing-dims-x): a name is the dim of that name if one is declared, and otherwise the group of that name expanded to its coordinates.
Whether a coordinate is the entity axis, a group or a plain axis changes where the labels come from, not how the attribute is declared or stored.
There is no `bus` field: an attribute is a [connection](record.md#connections) attribute because its `dims` name the `connection` group, which is what lets a second group exist without a second field.

`dims` is also what makes a scenario-varying `p_nom` a schema violation: a capacity is a first-stage decision, one value taken before the scenario is known, which is the point of stochastic scenarios differing only in dispatch.

`p_nom` and `carrier` have the same `dims` and are not the same kind of thing — one is a label, the other a number an optimiser decides.
`dims` says only which coordinates address a value.

`breakpoints` answers what a bare column set cannot: whether it may carry a piecewise-linear curve, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later ([wide and long rows](record.md#wide-and-long-rows)).

An attribute naming exactly one addressing coordinate is a column on that thing's own table, and anything more is long rows in `inputs/` — [where a value lives](format.md#where-a-value-lives) is the rule, and it is the schema that decides the file split rather than a writer guessing it.

## `entity_type` — the axis of kinds

What kind of thing a component is, declared like any other classification: a [group `into`](#into-a-group-that-classifies) it, over `entity` alone.

```python
dimensions = {
    "entity": Dimension(dtype="str"),
    "entity_type": Dimension(dtype=Enum(["Bus", "Generator", "Link"])),
}
groups = {
    "entity_type": Group(over=["entity"], into="entity_type"),
}
```

`into` is what says every component carries exactly one type, and being over `entity` alone is what makes this axis _the_ entity-type axis rather than one classification among several. At most one group may be that; a second has no resolved answer for what a component carries.
An `Enum` dtype pins the vocabulary and makes an unknown type a write-time error; a plain `str` leaves the labels as data, which is the right declaration for a record whose types are not known up front.

**Its rows are the entity axis file**, not a `groups/` file of its own — the one exception to [a file per group](format.md#where-a-value-lives). `dims/entity.parquet` carries `entity_type`, which is [where the format already put it](format.md#entity-is-unique-across-types), and the writer derives it from the per-type member files rather than taking it from a `Record`, so nothing can disagree with itself about which type a component is.

**It may not address a value alongside the entity.** An attribute naming both `entity` and the type in its `dims` is rejected: `into` declares the type to follow from the entity, so the row is keyed twice over and the two are free to disagree.
That is [why no attribute row carries the type](format.md#entity-is-unique-across-types), and it is the general rule for [a functional group and what it maps from](#into-a-group-that-classifies) rather than anything particular to types — `country` over `bus` is rejected the same way.

**Addressed by the type alone is ordinary.** A per-type `icon` is a value per type, keyed once, and it lands where any [attribute addressed by one dim alone](format.md#where-a-value-lives) does: a column of `dims/entity_type.parquet`.
Its axis file is owned like any other's: outside [`partial`](#partial-the-granularity-of-an-override) a layer touching one type's icon restates the type axis whole, which is what a dim owned entirely means everywhere else.
Being classified buys it no exemption, and carrying an attribute is no reason to declare it `partial` — that would widen the fold's key with a column no `inputs/` row can carry.

**Entirely optional.** A schema declaring no such axis has components with no types, and everything addressed by `entity` reaches all of them.
A tool that needs types requires the axis in the schema it builds — [PyPSA does](tools.md) — which is where that requirement belongs, not here.

**At most one.** A second dim `on` `entity` is rejected: a component has one type, and two vocabularies over one axis leave `attributes_for` with no resolved answer for what it carries.

## Traits

A trait is a named bundle of attributes, and which entity types carry them:

```python
traits = {
    "investable": Trait(
        attributes={"capital_cost", "build_year", "lifetime", "capacity"},
        on={"entity_type": {"Generator", "Line", "Link", "Store"}},
    ),
    "dispatchable": Trait(
        attributes={"p_min_pu", "p_max_pu", "p_set"},
        on={"entity_type": {"Generator", "Link"}},
    ),
}
```

**A trait narrows; it does not grant.** An attribute the schema declares is carried by every entity type it can address, and a trait is the only thing that cuts that down.
Writing `entity` in an attribute's `dims` is what says it is per component; declining to bundle it says it is so for every type — the same thing `dims={"scenario"}` already means along the scenario axis, where no subscription mechanism exists and nobody finds it surprising.

That direction is why [an attribute belonging to no type](proposals/dims-groups-traits.md#what-starts-it) has somewhere to live at all.
Under the previous shape a type _subscribed_ and an attribute reached nothing until one did, which is what forced `attributes` to be nested under types and left snapshot weightings homeless.

`Schema.attributes_for(ctype)` — the untraited attributes addressed by `entity`, plus what the traits naming `ctype` bundle — is what [`flags`](record.md#flags) and the [`add` routing](working-record.md#add-remove) read, so everything downstream asks one question and gets a resolved answer.
An attribute addressed by an axis alone is carried by no type however few traits mention it: a snapshot weighting belongs to the record.

A trait rather than a bare list of attribute names, for two reasons.
The deduplication is real: the boundaries are measured from a framework's registry rather than invented, and `investable` covers six types.
And a trait is **queryable** — a consumer dispatching on "everything investable" asks the schema rather than enumerating types, which is what makes the vocabulary worth declaring at all.

**Declared, not inferred.** No framework ships a trait registry to read, so the mapping from its component registry to traits is authored and maintained. That cost is the price of the vocabulary being useful to something other than this schema.

**Only an entity-type axis may scope a trait.** `on` is keyed by a dim declared `on={"entity"}` and the schema rejects any other, because a trait scoped to, say, `country` would make an attribute's vocabulary depend on data — which attributes a component carries would follow from what its bus maps to, a per-entity lookup every caller of `attributes_for` treats as answerable from the schema alone.

A trait with an empty `on` narrows nothing: it is a bundle for a consumer to dispatch on, and its attributes stay carried by every type.

A trait may only name an attribute the schema declares: it says which attributes apply, never what they are, so a name with no spec is a typo rather than a shorthand declaration.
Two traits bundling one attribute is fine — they resolve to a set — since [one attribute has one spec](#attributespec) and there is nothing left to conflict.

## Groups

A group declares **which tuples over several dims exist**: a sparse subset of a dim product, with its own order.

```python
groups = {
    "connection": Group(over={"entity": "entity", "bus": "bus"}),
    "corridor": Group(over={"from": "bus", "to": "bus"}),
    "country": Group(over=["bus"], into="country"),  # functional: one country per bus
}
```

Not a dim. A dim declares an axis of labels and a NULL in its column means "every value of it"; a group declares which _combinations_ are there, which no axis can say because the product is sparse — a component attaches to two buses out of a thousand.

`over` maps **coordinate name → dim** rather than naming a bare set of dims, because two coordinates may draw on the same axis: a corridor between two nodes is `(from, to)`, which a set could not spell.
A list is sugar for the dict with identical keys and values, so `over=["bus"]` is `over={"bus": "bus"}`.

An attribute over a group carries the group's _coordinate_ names as columns, never the group's own name:

```text
inputs/efficiency.parquet   entity | bus | timestep | attribute | breakpoint | value
inputs/flow.parquet         from | to | timestep | attribute | breakpoint | value
```

The group name appears only in the schema. A reader goes attribute → group → coordinates, never the reverse, so two groups may share a coordinate set without ambiguity — the attribute names which group constrains it.

**A group in `dims` expands to its coordinates** where no dim shadows it, so `dims={"connection", "timestep"}` gives the columns `entity | bus | timestep` — [addressing](#addressing-dims-x) states the full rule.
The fold's key therefore does not vary per attribute: [`partial_dims`](#partial-the-granularity-of-an-override) is one fixed tuple, now the union of plain dims and group coordinate names.

A group's table columns are **not declared here.** They are the attributes whose `dims` name exactly this group ([where a value lives](format.md#where-a-value-lives)) — `role` on a connection is `AttributeSpec(dtype="VARCHAR", dims={"connection"})`.
Declaring them a second time on the `Group` would be two ways to say one thing, disagreeing eventually.

**A group's key coordinate never broadcasts.** A NULL `bus` on a connection attribute means "every connection of this entity", which is [the group's rows](record.md#the-broadcast-rule) rather than the whole bus axis — there is no axis to expand against, only a sparse subset the group's table knows.
That is why a key coordinate lands in the fold's key and must be declared `partial`, alongside `entity` and for the same reason.
A functional group's `into` dim is not one of these: it is an ordinary axis whose NULL means "every country" like any other dim's.

**Connections are one instance**, not a structural category: `Group(over={"entity": "entity", "bus": "bus"})`, with `role` an ordinary attribute over it. `bus` is accordingly one coordinate of one group rather than a column the format fixes.

### `into` — a group that classifies

`into` names the dim a group is **functional into**: each tuple of `over` carries exactly one of its labels. `country` over `[bus]` into `country` says every bus is in one country.

It is a declaration a bare group cannot make. A group can only _happen_ to be single-valued, which leaves a duplicate row a data error the schema has no name for; `into` names it, so the constraint is declared and checkable on write. No existing system declares it — GAMS's `map(b,c)` is a set over a tuple of sets with single-valuedness left to convention, and a duplicated `b` silently double-counts — which is the argument for the field rather than against it, a schema whose purpose is making shape checkable having no reason to inherit that gap.

**`into` must name a declared dim.** That is what keeps the axis file, and the axis file is the whole of what a functional group has over a bare tuple set: `dims/country.parquet` gives `country` its [order](record.md#axis-order) and somewhere for a per-country CO2 budget to live.

**`into` is sugar, resolved once at parse.** It folds into the group's coordinates, so `groups/country.parquet` is keyed `bus | country` exactly as `connection` is keyed `entity | bus`, and no read path, file layout or fold key branches on whether a group has one. The field is retained for the three things that still need it: the uniqueness constraint (the key being the coordinates minus `into`), a consumer's aggregation, and round-tripping the manifest — writing back `over: [bus, country]` where the author wrote `into:` would silently rewrite their schema.

**It may not key an attribute alongside what it maps from.** `dims={"bus", "country"}` is rejected: `into` says the country follows from the bus, so the row would be keyed twice over and the two free to disagree.

**Nothing assumes one coordinate.** `over: [bus, scenario]` with `into: country` — a bus whose country varies per scenario — is allowed, the constraint being per-tuple already. It costs nothing in storage because [every group is a file](format.md#where-a-value-lives).

### Addressing: `dims: [X]`

An attribute names a group in its `dims` exactly as it names a dim, and one rule resolves both:

**`X` is the dim `X` if one is declared, and otherwise the group `X` expanded to its `over` coordinates.**

```python
"efficiency": AttributeSpec(dims={"connection", "timestep"}),  # entity | bus | timestep
"co2_budget": AttributeSpec(dims={"country"}),  # country: the dim, not the group
```

A group with no dim of its name has no other spelling, so expanding it is the only way to declare an attribute over it, and the expansion is what keeps [the fold's key](#partial-the-granularity-of-an-override) one fixed tuple.

**A group may share a dim's name**, and there the dim wins. For a functional group that is the natural spelling: the dim is the axis of labels, the group is the relation between it and `over`, and shadowing is what should happen — a value that is genuinely per-country is `dims: [country]`, the dim, which is what the axis file exists for. Expanding instead would give `dims: [bus]` written so the reader has to look up the group's `over` to see it.

Nothing then justifies prohibiting the collision in the `into`-less case either: the dim namespace resolving first means a dim and a group both called `connection` is not ambiguous, and one rule covers every collision instead of a rule plus a guard. What it gives up is a schema error — a shadowed `into`-less group loses its only spelling in `dims` and no longer says so at load time. That is not worth a second rule: the collision has to be authored deliberately, both entries are visible in one file, and [a lint would recover it](open-questions.md) without also rejecting the harmless case.

## Existence does not vary along a dim

A component exists or it does not. There is no declaration making membership vary per value of an axis — no generator present in scenario `high` and absent from `low` — so `dims/entity.parquet` holds one row per entity and a tombstone removes it whole.

That is a deliberate narrowing. An earlier `Dimension.keys` declared exactly this, putting the dim in an entity table's key so a component existed per scenario; it is gone, with nothing in its place, because [what it should mean is unsettled](open-questions.md) once connections are one group among several rather than a fixed second entity table.

What remains is the distinction that was doing the useful work: a **value** may vary along an axis where the **thing** may not. A stochastic network holding a different `capital_cost` per scenario is expressing exactly that, and it is an attribute with `dims = {"entity", "scenario"}` — long rows in `inputs/`, not a component that exists twice. [Where a value lives](format.md#where-a-value-lives) already places it.

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

### `within` is not `into`

Both are acyclic and both relate one dim to another. They mean opposite things:

- **`within`** names a dim's _parents_: its label set is **scoped per parent**, so `t1` in 2015 and `t1` in 2020 are different points and the axis key is `(period, timestep)`.
- **`into`** names the dim a _group_ classifies its coordinates by: one flat label set, each `over` tuple picking one of its labels. `country` is not scoped by `bus`; it is a partition of buses, and its axis key is `country` alone.

Nesting versus classification. `within` cannot express `country`, and a functional group cannot express `timestep`.
`within` stays on `Dimension` because nesting is a property of the axis itself; `into` is on `Group` because a classification is a relation, with rows.

A functional group is **single-valued by declaration** rather than by construction, which is the whole of what `into` adds — a many-to-many classification is an ordinary group, and expressible as one.

**A chain is not denormalised.** bus→state→country is two group files, never a `country` column on `dims/bus.parquet`. Two files asserting bus→country would let a layer restating the states leave every bus's country stale with nothing to detect it, so the chain is a join over two files — which a file per group gives for free, a layer restating a group restating exactly one file.

**The record does not resolve across levels.** An attribute over `country` is handed back keyed by country; projecting it down to buses is a join through the group, and that is the consumer's work. The fold learns no new operation.

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

**An axis file is owned the same way**, and the [attributes it carries](format.md#where-a-value-lives) do not argue for adding it.
Outside `partial`, a layer touching an axis restates it whole — every label, with the static attributes attached to them — because a layer holding one label is not saying the others are unchanged but that they are not there: the fold keys by the axis key, so what this layer carries is what the axis has here.
That is the same "no half-owned extent" rule a series obeys, applied to a set of labels rather than a curve.

**Keep it small.** `partial` is the fold's key, so every entry widens the owner map and the resolution it keys, for every read of every attribute.
The cost of leaving an axis out is paid once per edit and bounded by the axis; the cost of putting it in is paid by every read forever.
So it stays what it is for `entity` and a group's coordinate — the dims a layer genuinely patches value by value — rather than growing to cover whatever an axis happens to hold.

The [entity-type axis](#entity_type-the-axis-of-kinds) is the case that tests this: it carries an attribute and is exempt from `partial`, and it still restates whole rather than earning an exception.

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

- adding an attribute, a component type, a trait, or a group
- adding a dim no existing attribute varies over
- subscribing a type to a further attribute, whether directly or through a trait
- widening an `AttributeSpec.dims`: rows that set fewer dims still decode, since an unset dim is NULL and NULL means "all values" ([the broadcast rule](record.md#the-broadcast-rule))
- adding to `partial`: ownership becomes finer, and an existing layer's rows are simply owned at the coarser granularity they were written with
- changing a [`unit` or `description`](#unit-and-description), which describe the data without deciding how any row decodes

**Incompatible** — existing rows would decode differently, or not at all:

- narrowing `dims`, since a row setting a now-undeclared dim has no valid reading
- changing a `dtype`
- removing from `partial`: a layer that patched one value along that axis is now a partial override of an axis owned whole, which is exactly the hole [`partial`](#partial-the-granularity-of-an-override) forbids
- changing `within`, since the axis key changes shape
- a type ceasing to carry an attribute, whether by dropping it or by unsubscribing the trait that bundled it: its rows are still in the file, now with no valid reading for that type
- adding a dim that does not broadcast, since the fold's ownership key changes shape

Adding a functional group is compatible in the same sense adding any group is: the record gains a file, and until some layer writes it every coordinate reads as unclassified — no row, which is what "no country assigned" means anyway.

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
