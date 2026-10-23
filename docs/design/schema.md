<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

One schema per record, and `manifest.json` is how it is written down — the two words name the same thing, the file and the object.

```python
class Dimension(BaseModel):
    """One axis attribute data may vary over."""

    dtype: DimensionDtype = "str"  # float, int, str or datetime
    one_of: tuple[str, ...] | None = None  # the labels a str dim lists
    unit: str | None = None  # what the labels measure, if anything
    description: str | None = None  # what the axis is, in prose


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take."""

    dtype: ParameterDtype  # float, int, bool or str
    dims: frozenset[str] = frozenset()  # what addresses it: dims and groups
    default: Any | None = None
    default_by: dict[
        str, dict[str, Any]
    ] = {}  # categorizing property, as subject.name -> category -> default
    breakpoints: bool = False  # may carry a piecewise-linear curve
    unit: str | None = None  # what the values measure
    description: str | None = None  # what the attribute is, in prose


class PropertySpec(BaseModel):
    """One value per label of its subject, describing the label."""

    dtype: ParameterDtype | None = None  # float, int, bool or str
    one_of: str | None = None  # or the dim it takes its labels from
    default: Any | None = None
    default_by: dict[str, dict[str, Any]] = {}
    unit: str | None = None
    description: str | None = None


class Group(BaseModel):
    """Which tuples over several dims exist: a sparse subset of a dim product."""

    over: dict[str, str]  # coordinate name -> the dim it draws labels from
    description: str | None = None


class Schema(BaseModel):
    version: int  # bumped by any change to the declarations

    dimensions: dict[str, Dimension]
    attributes: dict[str, AttributeSpec]  # flat: one attribute, one spec
    properties: dict[
        str, dict[str, PropertySpec]
    ]  # subject dim or group -> name -> spec
    groups: dict[str, Group]

    # Which dims a layer may patch value by value; absent for a record with no
    # layers, since nothing overrides anything.
    partial: frozenset[str] | None = None

    meta: dict[str, Any] = {}  # opaque; the package never interprets it
```

`partial` is the only layering-specific part, and so the only optional one.
Everything else describes the data and is always present.

`attribute` is `VARCHAR`, and no dim or property name means anything to this package: those vocabularies belong to a modelling framework, and this package knows none.
A [category](#categories-labels-a-dim-lists) is typed as the schema declares it, which is a record's own statement rather than a framework's — so a category _the schema declares_ but no tool recognises reads back fine and is reported by the tool that cannot build it, not rejected inside the fold.

`meta` is where a framework's own top-level data goes — network attributes, coordinate reference system, free-form metadata.
It is stored and never interpreted, since none of it describes the dimensioned data.

## Dimensions

Every dim is declared: a record with `region`, `technology` or `vintage` needs no code change, and `dtype` is the axis's own property.

A `Dimension` declares the axis's shape — its type. [A dim is flat](#a-dim-is-flat).
It does not declare which dims an _attribute_ varies over (that is [per attribute](#attributespec)), nor [the patch granularity](#partial-the-granularity-of-an-override), nor [order](record.md#axis-order), nor what describes or classifies its labels — its [properties](#properties) say that.

## Dtypes

A dtype is mathspec's: `DimensionDtype` and `ParameterDtype` from `mathspec.program`, stored as the plain string a mathspec model declares, so the manifest and the model a record feeds speak one language. Each maps to one DuckDB type, one constant in `schema/dtypes.py` read wherever a column is typed:

| declares                 | declaration        | DuckDB type                |
| ------------------------ | ------------------ | -------------------------- |
| dim, attribute, property | `dtype: float`     | `DOUBLE`                   |
| dim, attribute, property | `dtype: int`       | `BIGINT`                   |
| attribute, property      | `dtype: bool`      | `BOOLEAN`                  |
| dim, attribute, property | `dtype: str`       | `VARCHAR`                  |
| dim                      | `dtype: datetime`  | `TIMESTAMP` (no time zone) |
| dim                      | `one_of: [labels]` | `ENUM` of the labels       |
| property                 | `one_of: <dim>`    | that dim's type            |

**A dim may list its labels under `one_of: [labels]`**, as a mathspec dimension does. Its `dtype` is `str`, the default; a `one_of` on any other dtype is refused at load, as is an empty list, a label listed twice, a label that is no string, or a dim name in place of the list. The list is the labels the dim allows, not its axis. Its column is an `ENUM`, and [`add`](working-record.md#add-remove) refuses a label outside the list, naming the list. Its axis file holds the labels added and not removed, as any dim's does, which gives them an [order](record.md#axis-order): a listed label never added is absent, [`remove`](working-record.md#add-remove) works as on any dim, and a property that takes its labels from the dim holds a live label like any other. A tool may write the whole list, as [PyPSA's](tools.md) does for `ctype` and `role`; that is its convention.

**A property declares exactly one of `dtype` and `one_of`.** A property that declares `one_of: <dim>` takes its labels from that dim, whether it is a property of a dim or of a group, and takes the dim's type: an `ENUM` where the dim lists its labels, `TIMESTAMP` for a `datetime` dim ([properties](#properties)). Declaring both or neither is refused at load, as is a list of labels in place of the dim.

**An attribute declares no `one_of`**, as a mathspec parameter has none, and **no property is `datetime`**: a timestep a property describes is a label of a `datetime` dim it takes its labels from.

**Struct columns are derived, never declared.** A group's column is a `STRUCT` with a field per key coordinate, each typed as its dim, and an owner flag a `STRUCT` of `BOOLEAN`. The fixed columns stay fixed: `attribute VARCHAR`, `breakpoint DOUBLE`, `deleted BOOLEAN`, `breakpoints BOOLEAN`.

The manifest writes a dtype as its string — `"dtype": "float"` — beside `"one_of": [...]` on a dim that lists its labels, and a property that takes its labels from a dim as `"one_of": "<dim>"` in place of `dtype`.

## `AttributeSpec`

What one attribute may do over those axes:

```python
attributes = {
    # a capacity to build: one decision, evaluated against every scenario
    "p_nom": AttributeSpec(dtype="float", dims={"entity"}),
    # an availability profile: varies over time, and per scenario
    "p_max_pu": AttributeSpec(dtype="float", dims={"entity", "scenario", "timestep"}),
    "marginal_cost": AttributeSpec(
        dtype="float", dims={"entity", "scenario"}, breakpoints=True
    ),
    # addressed by a group rather than by the entity axis
    "efficiency": AttributeSpec(dtype="float", dims={"connection", "timestep"}),
    # addressed by an axis alone: a weighting belongs to no entity
    "objective_weighting": AttributeSpec(dtype="float", dims={"snapshot"}),
}
```

**Flat, one spec per attribute**, [reaching every label it addresses](#an-attribute-reaches-every-label-it-addresses) rather than owned by a category.
The nesting this replaces said an attribute _belongs to_ an entity type, which is false in both directions: `objective_weighting` has no type, and `p_max_pu` was three identical specs under Generator, Link and StorageUnit.

The storage already disagreed with the nesting. `attributes/p_max_pu.parquet` holds every type's rows in one file with one `value` dtype, so two types declaring one attribute with **different dtypes** was expressible in the schema and unrepresentable on disk — a silent wrong read that nothing rejected.
Flat declaration makes it unrepresentable instead, which is the stronger form of the same guarantee: one attribute, one spec, one file, one dtype.

`dims` is the **only addressing mechanism**, and it names dims and [groups](#groups) alike, resolved by [one rule](#addressing-dims-x): a name is the dim of that name if one is declared, and otherwise the group of that name, carried as one column.
Whether a coordinate is the entity axis, a group or a plain axis changes where the labels come from, not how the attribute is declared or stored.
There is no `bus` field: an attribute is a [connection](record.md#connections) attribute because its `dims` name the `connection` group, which is what lets a second group exist without a second field.

`dims` is also what makes a scenario-varying `p_nom` a schema violation: a capacity is a first-stage decision, one value taken before the scenario is known, which is the point of stochastic scenarios differing only in dispatch.

`dims` says only which coordinates address a value, not what kind of value it is: `p_nom` over `entity` alone is data an optimiser decides, where `carrier` describes the entity — so `carrier` is a [property](#properties), and `dims` may name one coordinate without making an attribute one.

`breakpoints` answers what a bare column set cannot: whether it may carry a piecewise-linear curve, so a curve on an attribute that takes one value is rejected on write rather than reported unbuildable later ([wide and long rows](record.md#wide-and-long-rows)).

Every attribute is long rows in `attributes/<attr>.parquet`, whatever its `dims` — [where a value lives](format.md#where-a-value-lives) is decided by the section a name is declared in, not by how many coordinates address it, so widening `dims` adds a column to the same file rather than moving it.

`default_by` is a default per category, beside `default` ([below](#default_by-a-default-per-category)).

## A result is an attribute

What a solve computes is declared in `attributes` like any other value: a result has a dtype, coordinates and a description, and reading one back needs its dtype as much as an input does — a string-valued result cast as a number is lost to `TRY_CAST` with nothing raised.
It is stored in `attributes/<attr>.parquet`, [overlays](read-path.md#resolving-a-relation) like any attribute, the latest layer winning, and meets the [validation of `set`](working-record.md#validation) and the write path's shape check: every label it names must resolve.

**Only a tool knows which attributes it computes.** PyPSA marks them with `status` starting `"Output"`, so [the tool](tools.md#writing-results) reads its vocabulary rather than keeping a list, and an upgrade that adds a result is picked up.
The record layer stores, validates and resolves values; whether one was solved for is a framework's distinction, kept out of core like any other ([module layout](module-layout.md)).

**Results inherit.** A child that changes nothing a solve reads keeps its parent's solution; a child that changes inputs holds results computed from its parent's. Keeping them coherent is the caller's business ([open questions](open-questions.md)).

## Properties

What describes one label of a dim — its type, its carrier, a coordinate on a map — is metadata rather than data, and is declared apart from it, keyed by its **subject**:

```python
dimensions = {
    "ctype": Dimension(one_of=["Generator", "Line", "Load"]),
    "carrier": Dimension(dtype="str"),
    "country": Dimension(dtype="str"),
    ...,
}
properties = {
    "entity": {
        "ctype": PropertySpec(one_of="ctype"),
        "carrier": PropertySpec(one_of="carrier"),
        "sign": PropertySpec(
            dtype="float", default=1, default_by={"entity.ctype": {"Load": -1}}
        ),
    },
    "bus": {
        "carrier": PropertySpec(one_of="carrier", default="AC"),
        "x": PropertySpec(dtype="float", unit="°"),
        "country": PropertySpec(one_of="country"),
    },
    "carrier": {"color": PropertySpec(dtype="str", default="")},
    "connection": {"role": PropertySpec(dtype="str")},
}
```

**A property is one value per label of its subject**, a dim or a group, and so has no `dims`: it never varies along another axis. It is a column of the subject's own file — `dims/<dim>.parquet`, or `groups/<group>.parquet` for a group ([where a value lives](format.md#where-a-value-lives)) — so `ctype` is a column of `dims/entity.parquet` because every property of `entity` is, and the schema attaches no meaning to the name.

**An attribute is data**: what a model reads as part of its problem and what a layer would vary. Which a name is, is the author's declaration rather than derived from its shape; a tool reads it from its registry where it can ([PyPSA](tools.md)).

**A property's name is its subject's.** One name is one spec for an attribute, one file with one `value` dtype ([flat attributes](#attributespec)). A property is a column of its subject's file, so its name is unique there only: `x` on `bus` beside an attribute `x` — a line's reactance — and `carrier` on `entity`, on `bus` and as a dim. Refused at load are a property named like a column of its subject's key — the dim or a group's struct column — and a group's property named like one of its coordinates, the keywords a tuple is addressed by. Two spellings differing only in case stay refused record-wide ([names](#names)).

**A property is addressed as `subject.name`** — `entity.ctype`, `bus.x`, `connection.role` — wherever a caller names one: [`set`](working-record.md#set), a [`default_by`](#default_by-a-default-per-category) key, `default_for`. A plain name is an attribute, never a property, however few subjects declare it, so its meaning does not change when another subject declares it later. `Schema.resolve(name)` returns the `AttributeSpec`, or the subject and the `PropertySpec`, and refuses an unknown plain name naming the dotted forms of the properties so called. A column keeps its plain name: the frames of `properties[subject]` and the categories [`flags`](record.md#flags) and `default_for` take are each read against one subject already, and a [selector keyword](working-record.md#set) is bare — a coordinate of the attribute first, else any property of a dim the attribute reaches.

**A property may take its labels from a dim**, `one_of: <dim>` in place of `dtype`: its values are that dim's labels, typed as the dim ([dtypes](#dtypes)), so `entity.carrier` and `bus.carrier` above take their labels from `carrier`, `bus.country` from `country`, and any subject, a dim or a group, may declare one. A stored value that is not NULL must be a live label, checked where a group's field is: [`set` and `add`](working-record.md#validation) refuse one the record as staged lacks, the [writer](layers.md#deletion) a layer storing one it removed, and [`remove`](working-record.md#add-remove) a label one still holds. NULL is no label. Refused at load is one that takes its labels from an undeclared dim. It [categorizes](#categories-labels-a-dim-lists) only where the dim it takes its labels from lists them: the labels of any other dim are data a layer adds and removes.

A classification of a dim's label is **single-valued by construction**, one column of the label's row — a many-to-many classification is an ordinary group, and expressible as one. A group's property is one column of its tuple's row the same way, but it may be NULL, so a tuple may carry no label ([open questions](open-questions.md)).

**A chain is not denormalised.** bus→state→country is two properties, `bus.state` and `state.country`, never a `country` column on `dims/bus.parquet`. Two columns asserting bus→country would let a layer restating the states leave every bus's country stale with nothing to detect it, so the chain is a join over two axis files.

**The record does not resolve across levels.** An attribute over `country` is handed back keyed by country; projecting it down to buses is a join through the classification, and that is the consumer's work. The fold learns no new operation.

Membership could not vary along a classification either, if it varied along anything: whether an entity exists in Germany is already settled by its bus and that bus's country, so there would be no freedom for it to vary independently ([existence does not vary](#existence-does-not-vary-along-a-dim)).

**Why the split.** Without it, where a value is stored follows how many coordinates address it rather than what it is: `p_nom`, data a model optimises against, and `carrier`, a label describing a generator, would both be columns of the entity axis, and widening `p_nom` to `dims: [entity, scenario]` — a [compatible](#versioning) change — would move it to `attributes/p_nom.parquet`, away from the rows earlier layers wrote.

### Categories — labels a dim lists

**Any property of a dim listed in [`partial`](#partial-the-granularity-of-an-override) that takes its labels from a dim listing them categorizes it** — `entity.ctype: {one_of: ctype}` where `ctype` is `{one_of: [Generator, Line, ...]}`: its values are that dim's **categories** along it, which a [`default_by`](#default_by-a-default-per-category) keys on and a [selector keyword](working-record.md#set) selects by. `Schema.categorizing(dim)` lists them in declaration order. There is no flag declaring the role and no limit of one per dim — a `technology` property that takes its labels from a dim `technology` listing them would categorize `entity` beside `ctype`. Keying on any other property — a `str` one, one that takes its labels from a dim listing none, one of a dim outside `partial`, or one of a group — is refused, naming the property and why.

The two conditions are what a category needs: a dim that lists its labels, so a category a default or a selection names is checked against a declared list — a listed category no label has selects none, whether or not its axis holds it — and a category a label stores is a label with an axis file and an order like any other; and the dim in `partial`, because only there is a dim owned per label, which a default per label needs. A schema whose entities have no kinds declares no such property of `entity`, and nothing else about it differs.

**Category**, because **label** already names an axis value — what `add(dim, labels)` takes. A label's categories, its value of each categorizing property, are asked two things: which labels a category selects — [`flags(**categories)`](record.md#flags), a consumer's per-category frame, a [selector keyword](working-record.md#set) — and `default_for(name, **categories)`, the default of an attribute or property for such a label, each category keyed by its property's plain name.

**A category selects; it never narrows.** It says which labels a question is about, never which attributes or properties a label may hold: every label carries [every name it addresses](#an-attribute-reaches-every-label-it-addresses), whatever its categories.

**A category may change.** It is a property like any other, patched by `set`. A label keeps the values it holds, and a per-category read judges it by its current categories.

### `default_by` — a default per category

`default` stays the fallback; `default_by` maps a categorizing property, as `subject.name`, to per-category overrides, on attributes and properties alike:

```python
"p_min_pu": AttributeSpec(
    dtype="float", dims={"entity", "snapshot"}, default=0,
    default_by={"entity.ctype": {"StorageUnit": -1}},
)
```

A value read unset takes the first `default_by` key, in declaration order, whose map lists the label's category; else `default`.

- **Each key reachable from the value's address.** A key must categorize a dim the value reaches — `entity` directly, or through `connection`'s `entity` field for a value over the group — and a category its dim does not list is refused.
- **Never on a categorizing property.** A category defaulted by a category is circular; such a property takes a plain `default`.
- **Only the default varies.** `dtype`, `unit` and `description` are the spec's own, which is what keeps one name one column and one `attributes/` file.

A default is never stored. `record.properties` and `record.attributes` hand over stored rows, so a consumer applies a default through `default_for` — the PyPSA tool does, building a component's static frame.

## An attribute reaches every label it addresses

An attribute is carried by every label its `dims` address, and a property by every label of its subject; nothing in the schema cuts that down. Writing `entity` in an attribute's `dims` says it is per entity, for every category — the same thing `dims={"scenario"}` means along the scenario axis. An attribute addressed by an axis alone — a snapshot weighting — reaches no entity, and belongs to the record.

So a value scoped to some kinds of label is scoped by the write, not the schema: `set("p_max_pu", 0.9, ctype="Generator")` writes one row per generator, where `set("p_max_pu", 0.9)` is [one row reaching every entity](record.md#the-broadcast-rule), loads included.

**Which attributes a kind of label takes is the tool's knowledge.** PyPSA's registry already says a `Load` takes no `p_max_pu`; copying that into the schema would have the record layer store, fold by and validate against a framework's vocabulary it never defines, which [the module layout](module-layout.md) keeps out of core. A tool needing a per-type vocabulary keeps it itself, as [the PyPSA tool](tools.md#a-value-its-type-does-not-take) does. A category is the record's — it selects labels and keys [`default_by`](#default_by-a-default-per-category), which picks a value per category, never whether one exists.

The cost is accepted: a misplaced value is no longer refused on write, and a NULL row expands over every label in [the owner map](read-path.md#owner-map) rather than some.

## Groups

A group declares **which tuples over several dims exist**: a sparse subset of a dim product, with its own order.

```python
groups = {
    "connection": Group(over={"entity": "entity", "bus": "bus"}),
    "corridor": Group(over={"from": "bus", "to": "bus"}),
    # classifying: a country per bus and scenario, its property
    # `bus_country.country: {"one_of": "country"}`
    "bus_country": Group(over=["bus", "scenario"]),
}
```

Not an axis of labels: a dim declares one, a group declares which _combinations_ are there, which no axis can say because the product is sparse — an entity attaches to two buses out of a thousand.
As a coordinate, though, a group is a dim whose axis is the resolved group: it [broadcasts](record.md#the-broadcast-rule), and is [`partial`](#partial-the-granularity-of-an-override) or owned whole, by the rules a dim follows.

`over` maps **coordinate name → dim** rather than naming a bare set of dims, because two coordinates may draw on the same axis: a corridor between two nodes is `(from, to)`, which a set could not spell.
A list is sugar for the dict with identical keys and values, so `over=["bus"]` is `over={"bus": "bus"}`.

An attribute over a group carries the group as **one struct column named for the group**, a field per key coordinate in `over` order:

```text
attributes/efficiency.parquet       connection STRUCT(entity, bus) | timestep | attribute | breakpoint | value
attributes/flow.parquet             corridor STRUCT("from", "to")  | timestep | attribute | breakpoint | value
attributes/marginal_price.parquet   bus | timestep | attribute | breakpoint | value
```

The column's name says which group constrains the row, so two groups may share a coordinate set without ambiguity, and a group's coordinate never takes the column of the dim it draws on — `bus` inside `connection` is not the dim `bus` beside it.

**A group file is keyed by the same column**: `groups/connection.parquet` is `connection STRUCT(entity, bus) | role`, as `dims/bus.parquet` is keyed by `bus`, so a row and the group it names meet on one column with nothing packed between them.

**A group in `dims` resolves to itself**, so `dims={"connection", "timestep"}` gives the columns `connection | timestep`, groups first — [addressing](#addressing-dims-x) states the full rule.
The fold's key therefore does not vary per attribute: [`partial_dims`](#partial-the-granularity-of-an-override) is one fixed tuple, exactly the dims and groups `partial` names.

**An attribute may not name a group beside a dim its key coordinate is named for.** `dims={"connection", "bus"}` is rejected: the row would hold the bus twice, free to disagree.

A group's file columns are **not declared here.** They are its [properties](#properties), declared under the group's name — PyPSA's `role` on a connection is `properties: {connection: {role: ...}}`, declared by [the tool](tools.md) whose vocabulary the word is — and an attribute over the group is long rows in `attributes/` keyed by the struct ([where a value lives](format.md#where-a-value-lives)).
Declaring them a second time on the `Group` would be two ways to say one thing, disagreeing eventually. A group is never categorized: its tuples carry every property it declares.

**A group column broadcasts, its axis the resolved group.** A NULL struct means every tuple of the group, expanded against `groups/<group>.parquet` as a NULL dim is against `dims/<dim>.parquet` ([the broadcast rule](record.md#the-broadcast-rule)).
**A group value is a whole tuple or NULL.** A struct with some fields NULL names a tuple no group row matches, so it is refused wherever a row comes in — `set`, `add_group` and the writer.
**A group is keyed where a dim is**: in the fold key where [`partial`](#partial-the-granularity-of-an-override) names it, owned whole otherwise, so a layer touching one connection's value then restates the attribute's whole extent along the group.
**The dims a group draws on broadcast on their own**: `bus` is an ordinary axis whose NULL means "every bus", so a per-bus `marginal_price` beside a `connection` group may hold one value for all of them.
Every group is such a coordinate (`Schema.group_columns`).

**Connections are one instance**, not a structural category: `Group(over={"entity": "entity", "bus": "bus"})`, with `role` a property of it. `bus` is accordingly one coordinate of one group rather than a column the format fixes, and neither word appears in the record layer — `connection` is whatever a schema calls it, and `role` is [a tool's declaration](tools.md).

### A group that classifies

A group classifies its tuples through a property that takes its labels from a dim: `bus_country` over `[bus, scenario]` with `bus_country.country: {"one_of": "country"}` says which country a bus is in per scenario, `groups/bus_country.parquet` being `bus_country STRUCT(bus, scenario) | country`, keyed by its struct exactly as `connection` is. The dim keeps its axis file, which gives `country` its [order](record.md#axis-order) and somewhere for a per-country CO2 budget to live. One coordinate needs no group: a bus in one country is [`bus.country`](#properties), a column of the bus axis.

The property may be NULL, so the schema does not promise one label per tuple, and its name is free — `country` is a convention, not a rule ([open questions](open-questions.md)).

### Addressing: `dims: [X]`

An attribute names a group in its `dims` exactly as it names a dim, and one rule resolves both:

**`X` is the dim `X` if one is declared, and otherwise the group `X`, carried as one struct column of its name.**

```python
"efficiency": AttributeSpec(dims={"connection", "timestep"}),  # connection | timestep
"co2_budget": AttributeSpec(dims={"country"}),  # country: the dim
```

A group with no dim of its name has no other spelling, so naming it is the only way to declare an attribute over it; `coordinates_of` lists the attribute's groups first, then its dims in declaration order.

**A group may not share a dim's name**: a group is carried as a column named for it, so sharing a dim's name makes two columns one name.
A classifying group is named for the relation rather than the axis it classifies by — `bus_country`, its property taking its labels from `country` — and the schema's error says so.

## Existence does not vary along a dim

An entity exists or it does not. There is no declaration making membership vary per value of an axis — no generator present in scenario `high` and absent from `low` — so `dims/entity.parquet` holds one row per entity and a tombstone removes it whole.

That is a deliberate narrowing. An earlier `Dimension.keys` declared exactly this, putting the dim in an entity table's key so an entity existed per scenario; it is gone, with nothing in its place, because [what it should mean is unsettled](open-questions.md) once connections are one group among several rather than a fixed second entity table.

What remains is the distinction that was doing the useful work: a **value** may vary along an axis where the **thing** may not. A stochastic network holding a different `capital_cost` per scenario is expressing exactly that, and it is an attribute with `dims = {"entity", "scenario"}` — long rows in `attributes/`, not an entity that exists twice. [Where a value lives](format.md#where-a-value-lives) already places it.

## A dim is flat

Every dim is one label set, keyed by its label alone: `dims/<dim>.parquet` is keyed by the one column `<dim>`, `record.dims[dim]` is that column, and its labels are unique across the dim.

**A label scoped per parent is a flat label and a property that [takes its labels from](#properties) the parent.** The source picks the labels; they need only be unique across the dim. What the source called the point inside its parent, if it matters, is a further property. A chain is a chain of such properties, [not denormalised](#properties); several parents are several.

| scoped per parent                     | declared flat                                                            |
| ------------------------------------- | ------------------------------------------------------------------------ |
| `timestep` `within` `period`          | `snapshot`; `snapshot.period: {one_of: period}`; `snapshot.timestep`     |
| `timestep` in `period` in `horizon`   | `snapshot.period: {one_of: period}`; `period.horizon: {one_of: horizon}` |
| `timestep` `within` `{period, stage}` | `snapshot.period: {one_of: period}`; `snapshot.stage: {one_of: stage}`   |

```python
schema = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "period": Dimension(dtype="int"),
        "timestep": Dimension(dtype="datetime"),
        "snapshot": Dimension(dtype="int"),
    },
    properties={
        "snapshot": {
            "period": PropertySpec(one_of="period"),
            "timestep": PropertySpec(one_of="timestep"),
        }
    },
    attributes={"p_max_pu": AttributeSpec(dtype="float", dims={"entity", "snapshot"})},
)
```

**Selecting by a parent is a selector keyword.** One is bare and matches any property of a dim the attribute reaches ([`set`](working-record.md#set)), so `period=2030` on `p_max_pu` picks the snapshots whose `snapshot.period` is 2030, and `timestep=t0` the snapshot at `t0` in every period. Two reached dims both declaring `period` stay refused.

**The scoping belongs to the source's labels, not to the axis.** `t1` names nothing alone because PyPSA reuses it per period, and a source that numbers its snapshots once has no nesting to declare. As a property, the relation is in the one form the record already checks, folds and selects on, and a consumer reads `snapshot.period` exactly as it reads `bus.carrier`. A source whose inner labels repeat across parents synthesises unique ones; "the same timestep in every period" is a selector over `snapshot.timestep`, which stages one row per matching snapshot.

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
partial_dims         = broadcast coordinates ∩ schema.partial
```

`partial_dims` is the fold key: exactly the broadcast coordinates — dims and groups — declared `partial`. No dim is in it by name; a schema patching entity by entity lists `entity`, as PyPSA's `{entity, bus, scenario, connection}` does.
A dim a group draws on is a broadcast dim like any other, so a layer patching a per-bus `marginal_price` one bus at a time needs `bus` in `partial`; a layer patching one connection's `efficiency` needs `connection` there, and the connection's bus is patched with it, inside the `connection` column. Neither is needed for the group itself: a dim a group draws on may stay out of `partial`, a removed label's tuples being [tombstoned with it](layers.md#deletion).
So `p_max_pu` is owned per entity and per scenario — `timestep` is not partial, so a patch to one hour restates that entity-scenario's whole series; `marginal_cost` per entity and scenario; `p_nom` per entity, once across everything else.

Two things this buys.
The schema can distinguish `p_max_pu` from `p_nom`, which a dim-level flag cannot: that would say every attribute is owned per scenario, including those a scenario must not change.
And a `p_nom` row carrying a non-NULL `scenario` becomes a write-time violation rather than something the NULL-broadcast rule absorbs — a first-stage decision quietly turned into a per-scenario one is the error worth catching.

What the fold does with this is unchanged: the owner-map key is one fixed tuple over all attributes, and an attribute not varying over a dim writes NULL there.
So the declarations constrain and validate; they do not make the key vary per row.

**An axis file is owned the same way**, and the [properties it carries](format.md#where-a-value-lives) do not argue for adding it — save one: a dim with a [category](#categories-labels-a-dim-lists) must be in `partial`, since only there is it checked per label.
Outside `partial`, a layer touching an axis restates it whole — every label, with the properties attached to them — because a layer holding one label is not saying the others are unchanged but that they are not there: the fold keys by the axis key, so what this layer carries is what the axis has here.
That is the same "no half-owned extent" rule a series obeys, applied to a set of labels rather than a curve.

**Keep it small.** Every `partial` coordinate widens the fold key, and the key is paid for by every read of every attribute.
The cost of leaving one out is paid once per edit and bounded by the axis or group; the cost of putting it in is paid by every read forever.
So `partial` names only the dims and groups a layer genuinely patches value by value — `entity`, `bus`, `scenario` and `connection`, not `timestep` or `carrier`: a layer patches one bus's `v_nom`, but restates every carrier's `co2_emissions`, a few dozen rows.

## Names

A declared dim, group, group coordinate, attribute or property name becomes a column, beside the columns datarecord writes itself. `Schema` refuses one at load that could not stand there, naming it and the reason:

- **`"` or `.`.** A name reaches DuckDB as a column reference, which reads `a.b` as column `b` of table `a` and cannot spell a `"` at all. A space, mixed case, a keyword or a leading digit are fine: those quote.
- **A column the format writes.** `attribute`, `breakpoint` and `value` on [every long row](format.md#the-long-schema), `deleted` as a [tombstone](layers.md#deletion), `varies`, `broadcast`, `breakpoints` and `layer_uuid` in [the owner map](read-path.md#owner-map), and `order_key` on a resolved frame. Ignoring case, as DuckDB resolves a column name. A collision is silent otherwise: a dim named `value` gives the long file two columns of one name, and one named `deleted` reads every label as a tombstone, dropping its rows with nothing reported.
- **A leading `_`.** The prefix of every scratch column a fold adds and drops, so that set can grow without growing this list.
- **Two spellings differing only in case.** DuckDB resolves `Country` and `country` to one column, so a schema declaring both would have one read the other's values. One spelling declared twice, as when a group's coordinate is named after the dim it ranges over, is a single column and fine.

Entity names and labels are data, never identifiers, so none of this constrains them.

No dim or property name is reserved — not `entity`, not `ctype`: a schema declares them like any other, so neither can be taken by surprise. What one name may not be twice over is [a section's rule](#properties), not this list's.

## One schema per record

Not one per layer.
A directory record's schema is `manifest.json` in the directory; a layered record's lives **beside** the layers, not inside any of them:

```text
record-root/
├── manifest.json               # the schema — one, for the whole tree
└── layers/<uuid>/              # a layer: dims/, groups/, attributes/ — no manifest
    └── resolved/               # caches (owner map, resolved dims)
```

A schema is not layered data.
Folding it would let a layer change what `p_nom` _means_ — its dtype, which dims it varies over — which is not a patch to data but a redefinition of the thing being patched, and it makes the schema unknowable without walking the ancestry.
One schema makes it a property of the record, validatable before anything is read and stated once for a hundred-layer tree.

The cost is that adding an attribute amends the root schema rather than shipping inside the layer that introduces it.
That is the right trade: a new attribute is a schema change, and one buried several layers deep is exactly what should be visible.

A layer directory therefore holds only data, which is what keeps it a plain parquet directory readable by a tool that knows nothing about layering.

Which of the two a directory is decides what it is read under. A **standalone** record carries its own `manifest.json`, so `Workspace.at(uri)` reads that file and the record answers the same through any workspace — it is one whole record, and may well be opened from somewhere that knows nothing about it; `at` refuses a directory without one. Its manifest also carries an `id`, a UUID `ws.write` mints, as the schema's optional `id` field, so the file stays flat and validates whole, and the record's schema carries it. Rooting a schema into a tree clears it. The id is the record's layer id, which keys the fold tables a workspace shares: derived from the URI instead, a record deleted and written again there, or a relative path read from another working directory, would be served the folds of the record before it. `at` refuses a manifest without one. A tree's manifest needs none, its revisions naming its layers. A single **layer** directory carries none, so it is read through its [store](layers.md#the-store), `store.layer(id)`, under the manifest beside the tree it belongs to.

An edit changes neither. A [`WorkingRecord`](working-record.md) is one more layer over its base, and a layer declares nothing — so it reads under whatever its base's schema is, standalone or not. A staging area that could redeclare would be the layered-schema case above, arriving by a different door.

## Versioning

One schema outlives many layers ([above](#one-schema-per-record)), so a change to it meets data written under the previous one.
`version` records which schema a record's layers were written against, and what matters is which changes existing layers survive.

**Compatible** — old layers stay readable, `version` bumps and nothing else happens:

- adding an attribute, a property, a category, or a group — none changes [the owner map](read-path.md#owner-map)'s key, which only `partial` widens
- adding a dim no existing attribute varies over
- widening an `AttributeSpec.dims`: rows that set fewer dims still decode, since an unset dim is NULL and NULL means "all values" ([the broadcast rule](record.md#the-broadcast-rule))
- adding to `partial`: ownership becomes finer, and an existing layer's rows are simply owned at the coarser granularity they were written with
- changing a [`unit` or `description`](#unit-and-description), which describe the data without deciding how any row decodes

**Incompatible** — existing rows would decode differently, or not at all:

- narrowing `dims`, since a row setting a now-undeclared dim has no valid reading
- changing a `dtype` or `one_of`, which includes dropping a label a dim lists
- moving a name between `properties` and `attributes`, or between subjects: it moves the [file](format.md#where-a-value-lives) the value is read from
- removing from `partial`: a layer that patched one value along that axis is now a partial override of an axis owned whole, which is exactly the hole [`partial`](#partial-the-granularity-of-an-override) forbids

Adding a classifying group is compatible in the same sense adding any group is: the record gains a file, and until some layer writes it every coordinate reads as unclassified — no row, which is what "no country assigned" means anyway.

The compatible changes are those where NULL already means what the new schema needs it to mean, so [the broadcast rule](record.md#the-broadcast-rule) absorbs them without touching a row.

An incompatible change therefore needs the layers rewritten rather than the schema edited, which for a layered record means flattening it to a [standalone directory](writing.md#a-standalone-directory) under the new schema.
A reader encountering a `version` it was not written for should refuse rather than guess, since every failure above is silent.

## `unit` and `description`

A `Dimension`, an `AttributeSpec` and a `PropertySpec` may each carry a `unit` and a `description`.
Neither is interpreted: no conversion, no dimensional analysis, no validation that `MW` and `kW` are not being added.
They are stored, read back, and handed to whatever displays or documents the record.

They belong in the schema rather than in `meta` because they describe the _dimensioned data_ — which is exactly the line `meta` is on the other side of.
A `unit` is a property of an attribute in the same way its `dtype` is, and a consumer asking "what is `p_nom` and what is it measured in" should not have to know a framework's own metadata layout to find out.

`None` means undeclared, not dimensionless.
A quantity that genuinely has no unit is `""` — the distinction matters to a renderer choosing between showing nothing and showing an empty unit, and to a later pass that wants to find what is still undocumented.

A dimension's `unit` describes what its _labels_ measure, which is only sometimes meaningful: a `vintage` axis labelled in years or a `distance` axis in km has one, while `scenario` and `timestep` do not — a timestamp is not a quantity.
`description` applies to any axis.

Neither field changes how a row decodes, so adding or editing one is a [compatible change](#versioning).
