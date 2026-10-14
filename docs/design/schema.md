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
    """A bundle of attributes, and which entity types and entities carry it."""

    attributes: frozenset[str] = frozenset()
    on: dict[str, frozenset[str]] = {}  # entity-type axis -> the labels it applies to
    switch: str | None = None  # attribute deciding it per entity; joins `attributes`
    description: str | None = None


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations

    dimensions: dict[str, Dimension]
    attributes: dict[str, AttributeSpec]  # flat: one attribute, one spec
    results: dict[str, AttributeSpec]  # what a solve computes, governed apart
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
    # addressed by an axis alone: a weighting belongs to no entity
    "objective_weighting": AttributeSpec(dtype="float64", dims={"snapshot"}),
}
```

**Flat, one spec per attribute**, with [entity types subscribing](#traits) rather than owning.
The nesting this replaces said an attribute _belongs to_ an entity type, which is false in both directions: `objective_weighting` has no type, and `p_max_pu` was three identical specs under Generator, Link and StorageUnit.

The storage already disagreed with the nesting. `inputs/p_max_pu.parquet` holds every type's rows in one file with one `value` dtype, so two types declaring one attribute with **different dtypes** was expressible in the schema and unrepresentable on disk — a silent wrong read that nothing rejected.
Flat declaration makes it unrepresentable instead, which is the stronger form of the same guarantee: one attribute, one spec, one file, one dtype.

`dims` is the **only addressing mechanism**, and it names dims and [groups](#groups) alike, resolved by [one rule](#addressing-dims-x): a name is the dim of that name if one is declared, and otherwise the group of that name, carried as one column.
Whether a coordinate is the entity axis, a group or a plain axis changes where the labels come from, not how the attribute is declared or stored.
There is no `bus` field: an attribute is a [connection](record.md#connections) attribute because its `dims` name the `connection` group, which is what lets a second group exist without a second field.

`dims` is also what makes a scenario-varying `p_nom` a schema violation: a capacity is a first-stage decision, one value taken before the scenario is known, which is the point of stochastic scenarios differing only in dispatch.

`p_nom` and `carrier` have the same `dims` and are not the same kind of thing — one is a label, the other a number an optimiser decides.
`dims` says only which coordinates address a value.

`breakpoints` answers what a bare column set cannot: whether it may carry a piecewise-linear curve, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later ([wide and long rows](record.md#wide-and-long-rows)).

An attribute naming exactly one addressing coordinate is a column on that thing's own table, and anything more is long rows in `inputs/` — [where a value lives](format.md#where-a-value-lives) is the rule, and it is the schema that decides the file split rather than a writer guessing it.

## Results

`results` is a second mapping of `AttributeSpec`, keyed and shaped exactly like `attributes`.
What a solve computes is declared, not discovered: a result has a dtype, coordinates and a description like any other attribute, and reading one back needs its dtype as much as an input does — a string-valued result cast as a number is lost to `TRY_CAST` with nothing raised.

It is a **separate mapping rather than a flag** because the two are governed differently at every point a caller touches them.
A result is written to `outputs/<attr>.parquet`, never [overlays](read-path.md#outputs) a parent's, and may name an entity the record does not declare.
An input meets [`attributes_for`](#traits), which is the per-type vocabulary that validates a `set` and splits `add`'s wide frame; a result meets none of that, and keeping it out of `attributes` is what makes that structural rather than a condition repeated at each site.
A name in both is rejected: one name is one file with one `value` column, so it is an input or a result and not both.

The questions the long schema asks of a _stored_ attribute — its dtype, its coordinates — span both, since `outputs/` and `inputs/` share a layout.
Those go through one lookup that consults each in turn.

**A tool declares its results from the same registry it declares its inputs from.**
PyPSA marks them with `status` starting `"Output"`, so the vocabulary is read rather than kept as a list, and an upgrade that adds a result is picked up.
That is what keeps declaration from becoming a maintenance burden on the tool: the alternative — a tool attaching whatever its registry produced, unchecked — buys nothing a caller can rely on, since a consumer reading `outputs/` still needs a dtype to read it at.

Results version like inputs ([versioning](#versioning)): removing one, changing its dtype, or narrowing its `dims` makes existing layers unreadable for the same reasons.

## `entity_type` — the axis of kinds

What kind of thing an entity is, declared like any other classification: a [group `into`](#into-a-group-that-classifies) it, over `entity` alone.

```python
dimensions = {
    "entity": Dimension(dtype="str"),
    "entity_type": Dimension(dtype=Enum(["Bus", "Generator", "Link"])),
}
groups = {
    "entity_type": Group(over=["entity"], into="entity_type"),
}
```

`into` is what says every entity carries exactly one type, and being over `entity` alone is what makes this axis _the_ entity-type axis rather than one classification among several. At most one group may be that; a second has no resolved answer for what an entity carries.
An `Enum` dtype pins the vocabulary and makes an unknown type a write-time error; a plain `str` leaves the labels as data, which is the right declaration for a record whose types are not known up front.

**Its rows are the entity axis file**, not a `groups/` file of its own — the one exception to [a file per group](format.md#where-a-value-lives). `dims/entity.parquet` carries `entity_type`, which is [where the format already put it](format.md#entity-is-unique-across-types): the axis file a source hands over carries the column, so `entity -> entity_type` is one fact in one place and nothing can disagree with itself about which type an entity is.

**It may not address a value alongside the entity.** An attribute naming both `entity` and the type in its `dims` is rejected: `into` declares the type to follow from the entity, so the row is keyed twice over and the two are free to disagree.
That is [why no attribute row carries the type](format.md#entity-is-unique-across-types), and it is the general rule for [a functional group and what it maps from](#into-a-group-that-classifies) rather than anything particular to types — `country` over `bus` is rejected the same way.

**Addressed by the type alone is ordinary.** A per-type `icon` is a value per type, keyed once, and it lands where any [attribute addressed by one dim alone](format.md#where-a-value-lives) does: a column of `dims/entity_type.parquet`.
Its axis file is owned like any other's: outside [`partial`](#partial-the-granularity-of-an-override) a layer touching one type's icon restates the type axis whole, which is what a dim owned entirely means everywhere else.
Being classified buys it no exemption, and carrying an attribute is no reason to declare it `partial` — that would widen the fold's key with a column no `inputs/` row can carry.

**Entirely optional.** A schema declaring no such axis has entities with no types, and everything addressed by `entity` reaches all of them.
There is then no `entity_type` column on the entity axis and no `dims/entity_type/` at all: an entity's non-varying values are columns of `dims/entity.parquet` itself ([where a value lives](format.md#where-a-value-lives)), which is the honest shape of "these entities have no kinds" — one file, not one per label an `add` happened to use. [`Record.entity_types`](record.md) is empty, matching `schema.entity_types`.
A tool that needs types requires the axis in the schema it builds — [PyPSA does](tools.md) — which is where that requirement belongs, not here.

**At most one.** A second dim `on` `entity` is rejected: an entity has one type, and two vocabularies over one axis leave `attributes_for` with no resolved answer for what it carries.

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
Writing `entity` in an attribute's `dims` is what says it is per entity; declining to bundle it says it is so for every type — the same thing `dims={"scenario"}` already means along the scenario axis, where no subscription mechanism exists and nobody finds it surprising.

That direction is why [an attribute belonging to no type](proposals/dims-groups-traits.md#what-starts-it) has somewhere to live at all.
Under the previous shape a type _subscribed_ and an attribute reached nothing until one did, which is what forced `attributes` to be nested under types and left snapshot weightings homeless.

`Schema.attributes_for(ctype)` — the untraited attributes addressed by `entity`, plus what the traits naming `ctype` bundle — is what [`flags`](record.md#flags) and the [`add` routing](working-record.md#add-remove) read, so everything downstream asks one question and gets a resolved answer.
An attribute addressed by an axis alone is carried by no type however few traits mention it: a snapshot weighting belongs to the record.

A trait rather than a bare list of attribute names, for two reasons.
The deduplication is real: the boundaries are measured from a framework's registry rather than invented, and `investable` covers six types.
And a trait is **queryable** — a consumer dispatching on "everything investable" asks the schema rather than enumerating types, which is what makes the vocabulary worth declaring at all.

**Declared, not inferred.** No framework ships a trait registry to read, so the mapping from its component registry to traits is authored and maintained. That cost is the price of the vocabulary being useful to something other than this schema.

**Only an entity-type axis may scope a trait.** `on` is keyed by a dim declared `on={"entity"}` and the schema rejects any other, because a trait scoped to, say, `country` would make an attribute's vocabulary depend on data — which attributes an entity carries would follow from what its bus maps to, a per-entity lookup every caller of `attributes_for` treats as answerable from the schema alone.

A trait with an empty `on` narrows nothing: it is a bundle for a consumer to dispatch on, and its attributes stay carried by every type.

A trait may only name an attribute the schema declares: it says which attributes apply, never what they are, so a name with no spec is a typo rather than a shorthand declaration.
Two traits bundling one attribute is fine — they resolve to a set — since [one attribute has one spec](#attributespec) and there is nothing left to conflict.

### `switch` — a trait an entity opts into

A trait narrows two ways, and both are optional: `on` says which entity types carry it, `switch` names an attribute deciding it per entity.

```python
traits = {
    "committable": Trait(
        attributes={"start_up_cost", "min_up_time", "ramp_limit_start_up"},
        on={"entity_type": {"Generator", "Link"}},
        switch="committable",
    ),
}
attributes = {
    "committable": AttributeSpec(dtype="bool", dims={"entity"}, default=False),
}
```

The two never interact: `on` first, then `switch`. A trait with neither reaches every entity of every type; one with a switch and no `on` reaches the entities whose switch is true, whatever their type — which is how a record declaring no types still says that some entities are committable and others are not.

**The switch is an ordinary attribute**, `dims: [entity]` exactly, with a `dtype`, a `default` and a place [an attribute over `entity` alone](format.md#where-a-value-lives) already has. `bool` is the dtype the [proposal](proposals/trait-switches.md#which-dtypes) admits; an `Enum` switch selecting among several traits is deferred.
Its default decides the unset case, and `false` keeps the bundle off every entity that predates the trait.

**It joins the trait's `attributes` on its own**, folded in at parse rather than listed by the author, so a consumer asking what `committable` bundles gets the switch with the rest.
Being in the bundle it would otherwise be narrowed by its own trait, which nothing could then turn on — so it is in `attributes` for discovery and out of the narrowing.

**`attributes_for` is unchanged.** A switched trait's attributes are carried by the type, because the question it answers is which attributes a generator _may_ have; the switch narrows which entities carry a _value_, which is a question about data.
So the switch is a validation and query mechanism rather than a change to the vocabulary. What it is declared for — rejecting a value set on an entity whose switch is false, and asking which entities a trait reaches — reads the switch column, and neither is wired up yet: the declaration lands first, the [checks that read data](proposals/trait-switches.md#what-it-costs) after. Whether `attributes_for` grows a per-entity counterpart is [open](open-questions.md).

## Groups

A group declares **which tuples over several dims exist**: a sparse subset of a dim product, with its own order.

```python
groups = {
    "connection": Group(over={"entity": "entity", "bus": "bus"}),
    "corridor": Group(over={"from": "bus", "to": "bus"}),
    "bus_country": Group(
        over=["bus"], into="country"
    ),  # functional: one country per bus
}
```

Not an axis of labels: a dim declares one, a group declares which _combinations_ are there, which no axis can say because the product is sparse — an entity attaches to two buses out of a thousand.
As a coordinate, though, a group is a dim whose axis is the resolved group: it [broadcasts](record.md#the-broadcast-rule), and is [`partial`](#partial-the-granularity-of-an-override) or owned whole, by the rules a dim follows.

`over` maps **coordinate name → dim** rather than naming a bare set of dims, because two coordinates may draw on the same axis: a corridor between two nodes is `(from, to)`, which a set could not spell.
A list is sugar for the dict with identical keys and values, so `over=["bus"]` is `over={"bus": "bus"}`.

An attribute over a group carries the group as **one struct column named for the group**, a field per [key](#into-a-group-that-classifies) coordinate in `over` order:

```text
inputs/efficiency.parquet       connection STRUCT(entity, bus) | timestep | attribute | breakpoint | value
inputs/flow.parquet             corridor STRUCT("from", "to")  | timestep | attribute | breakpoint | value
inputs/marginal_price.parquet   bus | timestep | attribute | breakpoint | value
```

The column's name says which group constrains the row, so two groups may share a coordinate set without ambiguity, and a group's coordinate never takes the column of the dim it draws on — `bus` inside `connection` is not the dim `bus` beside it.

**A group file is keyed by the same column**: `groups/connection.parquet` is `connection STRUCT(entity, bus) | role`, as `dims/bus.parquet` is keyed by `bus`, so a row and the group it names meet on one column with nothing packed between them.

**A group in `dims` resolves to itself**, so `dims={"connection", "timestep"}` gives the columns `connection | timestep`, groups first — [addressing](#addressing-dims-x) states the full rule.
The fold's key therefore does not vary per attribute: [`partial_dims`](#partial-the-granularity-of-an-override) is one fixed tuple, `entity` and the dims and groups `partial` names.

**An attribute may not name a group beside a dim its key coordinate is named for.** `dims={"connection", "bus"}` is rejected: the row would hold the bus twice, free to disagree — the same "keyed twice over" rule [`into`](#into-a-group-that-classifies) states.

A group's file columns are **not declared here.** They are the attributes whose `dims` name exactly this group ([where a value lives](format.md#where-a-value-lives)) — PyPSA's `role` on a connection is `AttributeSpec(dtype="VARCHAR", dims={"connection"})`, declared by [the tool](tools.md) whose vocabulary the word is.
Declaring them a second time on the `Group` would be two ways to say one thing, disagreeing eventually.

**A group column broadcasts, its axis the resolved group.** A NULL struct means every tuple of the group, expanded against `groups/<group>.parquet` as a NULL dim is against `dims/<dim>.parquet` ([the broadcast rule](record.md#the-broadcast-rule)).
**A group value is a whole tuple or NULL.** A struct with some fields NULL names a tuple no group row matches, so it is refused wherever a row comes in — `set`, `add_group` and the writer.
**A group is keyed where a dim is**: in the fold key where [`partial`](#partial-the-granularity-of-an-override) names it, owned whole otherwise, so a layer touching one connection's value then restates the attribute's whole extent along the group.
**The dims a group draws on broadcast on their own**: `bus` is an ordinary axis whose NULL means "every bus", so a per-bus `marginal_price` beside a `connection` group may hold one value for all of them.
Every group but the [entity-type group](#entity_type-the-axis-of-kinds), which has no column, is such a coordinate (`Schema.group_columns`).

**Connections are one instance**, not a structural category: `Group(over={"entity": "entity", "bus": "bus"})`, with `role` an ordinary attribute over it. `bus` is accordingly one coordinate of one group rather than a column the format fixes, and neither word appears in the record layer — `connection` is whatever a schema calls it, and `role` is [a tool's declaration](tools.md).

### `into` — a group that classifies

`into` names the dim a group is **functional into**: each tuple of `over` carries exactly one of its labels. `bus_country` over `[bus]` into `country` says every bus is in one country.

It is a declaration a bare group cannot make. A group can only _happen_ to be single-valued, which leaves a duplicate row a data error the schema has no name for; `into` names it, so the constraint is declared and checkable on write. No existing system declares it — GAMS's `map(b,c)` is a set over a tuple of sets with single-valuedness left to convention, and a duplicated `b` silently double-counts — which is the argument for the field rather than against it, a schema whose purpose is making shape checkable having no reason to inherit that gap.

**`into` must name a declared dim.** That is what keeps the axis file, and the axis file is the whole of what a functional group has over a bare tuple set: `dims/country.parquet` gives `country` its [order](record.md#axis-order) and somewhere for a per-country CO2 budget to live.

**`into` is sugar, resolved once at parse.** It is a plain column beside the key, so `groups/bus_country.parquet` is `bus_country STRUCT(bus) | country` and is keyed by its struct exactly as `connection` is, and no read path, file layout or fold key branches on whether a group has one. The field is retained for the three things that still need it: the uniqueness constraint (the key being the coordinates minus `into`), a consumer's aggregation, and round-tripping the manifest — writing back `over: [bus, country]` where the author wrote `into:` would silently rewrite their schema.

**It may not key an attribute alongside what it maps from.** `dims={"bus", "country"}` is rejected: `into` says the country follows from the bus, so the row would be keyed twice over and the two free to disagree.

**Nothing assumes one coordinate.** `over: [bus, scenario]` with `into: country` — a bus whose country varies per scenario — is allowed, the constraint being per-tuple already. It costs nothing in storage because [every group is a file](format.md#where-a-value-lives).

### Addressing: `dims: [X]`

An attribute names a group in its `dims` exactly as it names a dim, and one rule resolves both:

**`X` is the dim `X` if one is declared, and otherwise the group `X`, carried as one struct column of its name.**

```python
"efficiency": AttributeSpec(dims={"connection", "timestep"}),  # connection | timestep
"co2_budget": AttributeSpec(dims={"country"}),  # country: the dim
```

A group with no dim of its name has no other spelling, so naming it is the only way to declare an attribute over it; `coordinates_of` lists the attribute's groups first, then its dims in declaration order.

**A group may not share a dim's name**: a group is carried as a column named for it, so sharing a dim's name makes two columns one name.
A functional group is named for the relation rather than the axis it classifies into — `bus_country` into `country` — and the schema's error says so.
The [entity-type group](#entity_type-the-axis-of-kinds) is exempt, having no file or column of its own: its rows are the entity axis, so `dims: [entity_type]` can only mean the dim.

## Existence does not vary along a dim

An entity exists or it does not. There is no declaration making membership vary per value of an axis — no generator present in scenario `high` and absent from `low` — so `dims/entity.parquet` holds one row per entity and a tombstone removes it whole.

That is a deliberate narrowing. An earlier `Dimension.keys` declared exactly this, putting the dim in an entity table's key so an entity existed per scenario; it is gone, with nothing in its place, because [what it should mean is unsettled](open-questions.md) once connections are one group among several rather than a fixed second entity table.

What remains is the distinction that was doing the useful work: a **value** may vary along an axis where the **thing** may not. A stochastic network holding a different `capital_cost` per scenario is expressing exactly that, and it is an attribute with `dims = {"entity", "scenario"}` — long rows in `inputs/`, not an entity that exists twice. [Where a value lives](format.md#where-a-value-lives) already places it.

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

Membership could not vary along a classification either, if it varied along anything: whether an entity exists in Germany is already settled by its bus and that bus's country, so there would be no freedom for it to vary independently ([existence does not vary](#existence-does-not-vary-along-a-dim)).

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

A dim outside `partial` is one a layer owns entirely once it touches it: overriding one timestep of `p_max_pu` means carrying that entity's _entire_ series, because a partial series would resolve across two layers and produce a curve with a hole.
The reason is a consumer's rather than the format's — a framework that splits constant from varying data cannot receive half a series — which is why it belongs to the axis: it is true of every attribute varying over it.

The dims a layer owns an attribute per follow from the declarations:

```text
owned_per(attribute) = attribute.dims ∩ partial_dims
partial_dims         = {entity} ∪ (broadcast coordinates ∩ schema.partial)
```

`partial_dims` is the fold key: `entity`, patched per row by every layer, plus the broadcast coordinates — dims and groups — declared `partial`.
A dim a group draws on is a broadcast dim like any other, so a layer patching a per-bus `marginal_price` one bus at a time needs `bus` in `partial`; a layer patching one connection's `efficiency` needs `connection` there, and the connection's bus is patched with it, inside the `connection` column.
So `p_max_pu` is owned per entity and per scenario — `timestep` is not partial, so a patch to one hour restates that entity-scenario's whole series; `marginal_cost` per entity and scenario; `p_nom` and `carrier` per entity, once across everything else.

Two things this buys.
The schema can distinguish `p_max_pu` from `p_nom`, which a dim-level flag cannot: that would say every attribute is owned per scenario, including those a scenario must not change.
And a `p_nom` row carrying a non-NULL `scenario` becomes a write-time violation rather than something the NULL-broadcast rule absorbs — a first-stage decision quietly turned into a per-scenario one is the error worth catching.

What the fold does with this is unchanged: the inputs key is one fixed tuple over all attributes, and an attribute not varying over a dim writes NULL there.
So the declarations constrain and validate; they do not make the key vary per row.

**An axis file is owned the same way**, and the [attributes it carries](format.md#where-a-value-lives) do not argue for adding it.
Outside `partial`, a layer touching an axis restates it whole — every label, with the static attributes attached to them — because a layer holding one label is not saying the others are unchanged but that they are not there: the fold keys by the axis key, so what this layer carries is what the axis has here.
That is the same "no half-owned extent" rule a series obeys, applied to a set of labels rather than a curve.

**Keep it small.** Every `partial` coordinate widens the fold key, and the key is paid for by every read of every attribute.
The cost of leaving one out is paid once per edit and bounded by the axis or group; the cost of putting it in is paid by every read forever.
So `partial` names only the dims and groups a layer genuinely patches value by value — `scenario` and `connection`, not `timestep`.
`entity` is in the key already; `partial` may not name it.

The [entity-type axis](#entity_type-the-axis-of-kinds) carries an attribute yet stays out of the fold key: its labels are a column of `dims/entity.parquet`, not an addressable coordinate, so it restates whole like any non-`partial` axis and stays out of the fold key.

## Names

A declared dim, group, group coordinate, attribute or result name becomes a column, beside the columns datarecord writes itself. `Schema` refuses one at load that could not stand there, naming it and the reason:

- **`"` or `.`.** A name reaches DuckDB as a column reference, which reads `a.b` as column `b` of table `a` and cannot spell a `"` at all. A space, mixed case, a keyword or a leading digit are fine: those quote.
- **A column the format writes.** `attribute`, `breakpoint` and `value` on [every long row](format.md#the-long-schema), `deleted` as a [tombstone](layers.md#deletion), `varies`, `broadcast`, `breakpoints` and `layer_uuid` in [the owner map](read-path.md#owner-map), and `order_key` on a resolved frame. Ignoring case, as DuckDB resolves a column name. A collision is silent otherwise: a dim named `value` gives the long file two columns of one name, and one named `deleted` reads every label as a tombstone, dropping its rows with nothing reported.
- **A leading `_`.** The prefix of every scratch column a fold adds and drops, so that set can grow without growing this list.
- **`entity_type` as an attribute or result.** It is the column `dims/entity.parquet` carries an entity's type under, whatever [the type axis](#entity_type-the-axis-of-kinds) is called; as that axis's own dim or group name it is the usual spelling.
- **Two spellings differing only in case.** DuckDB resolves `Bus` and `bus` to one column, so a schema declaring both would have one read the other's values. One spelling declared twice, as when a group's coordinate is named after the dim it ranges over, is a single column and fine.

Entity names and labels are data, never identifiers, so none of this constrains them.

`entity` is not reserved: a schema declares it like any other axis, so it cannot be taken by surprise.

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

Which of the two a directory is decides what it is read under. A **standalone** record carries its own `manifest.json`, so `Workspace.at(uri)` reads that file and the record answers the same through any workspace — it is one whole record, and may well be opened from somewhere that knows nothing about it; `at` refuses a directory without one. Its manifest also carries an `id`, a UUID `ws.write` mints, beside the schema's keys rather than wrapping them, so the file still parses as the schema. The id is the record's layer id, which keys the fold tables a workspace shares: derived from the URI instead, a record deleted and written again there, or a relative path read from another working directory, would be served the folds of the record before it. `at` refuses a manifest without one. A tree's manifest needs none, its revisions naming its layers. A single **layer** directory carries none, so it is read through its [store](layers.md#the-store), `store.layer(id)`, under the manifest beside the tree it belongs to.

An edit changes neither. A [`WorkingRecord`](working-record.md) is one more layer over its base, and a layer declares nothing — so it reads under whatever its base's schema is, standalone or not. A staging area that could redeclare would be the layered-schema case above, arriving by a different door.

## Versioning

One schema outlives many layers ([above](#one-schema-per-record)), so a change to it meets data written under the previous one.
`version` records which schema a record's layers were written against, and what matters is which changes existing layers survive.

**Compatible** — old layers stay readable, `version` bumps and nothing else happens:

- adding an attribute, an entity type, a trait, or a group — none changes [the owner map](read-path.md#owner-map)'s key, which only `partial` widens
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
- adding a dim that does not broadcast — `entity`, the type axis aside the only one — since the fold's ownership key changes shape

Adding a functional group is compatible in the same sense adding any group is: the record gains a file, and until some layer writes it every coordinate reads as unclassified — no row, which is what "no country assigned" means anyway.

The compatible changes are those where NULL already means what the new schema needs it to mean, so [the broadcast rule](record.md#the-broadcast-rule) absorbs them without touching a row.

An incompatible change therefore needs the layers rewritten rather than the schema edited, which for a layered record means flattening it to a [standalone directory](writing.md#a-standalone-directory) under the new schema.
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
