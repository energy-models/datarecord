<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 13: buses and carriers are dims

| DEP      | 13                               |
| -------- | -------------------------------- |
| Title    | buses and carriers are dims      |
| Author   | Jonas Hörsch, Bryn Pickering     |
| Status   | Draft 2026-10-14                 |
| Requires | [DEP 12](dep-0012-properties.md) |
| Created  | 2026-10-14                       |
| Version  | Unreleased                       |

Once implemented, [the tool page](../tools.md), [the schema](../schema.md#properties) and [the format](../format.md#entity-is-unique-across-types) are authoritative where they and this text disagree.

## Abstract

In the PyPSA tool, `Bus` and `Carrier` stop being entity types. A bus is a label of the `bus` dim and nothing else; a carrier is a label of a new `carrier` dim. What PyPSA stores on a bus or a carrier is a property of that label or an attribute over that dim, by [the properties rule](../schema.md#properties), and a bus's results are keyed by `bus`. Two changes to core carry it: a component's `carrier` becomes a property **`into`** the `carrier` dim — a property whose values are labels of a dim — and **a property's name is scoped to its subject**, so `x` on a bus and `x` on a line, `carrier` on an entity and `carrier` on a bus, keep PyPSA's names.

## Motivation

**A bus exists twice.** The `connection` group ranges over `(entity, bus)`, so `north` is a label of the `bus` dim; and `Bus` is a value of `entity_type`, so `north` is also an entity, carrying `v_nom`, `x`, `y`, `carrier` and `control` on the entity axis. The two are joined by nothing but the spelling. `remove("bus", ["north"])` tombstones the label and its connections and leaves the entity; `remove("entity", ["north"])` does the reverse, and a record that did either is half-deleted with nothing to say so.

**It makes a collision that is not one.** [`entity` is unique across types](../format.md#entity-is-unique-across-types), so `to_datarecord` refuses a network with a Bus and a Generator both named `north`, a shape every pypsa-eur network has after clustering — `DE0 0` is a bus and the name of its load. PyPSA permits it because a bus is not a component to it: it is what components attach to.

**A carrier is a string.** `carrier` is a `String` property of `entity`, and `n.carriers` — `co2_emissions`, `color`, `nice_name`, the growth limits — is a `Carrier` entity type whose names are, by convention, the values that string takes. The record checks neither that a generator's carrier is listed nor that removing a carrier leaves no generator pointing at it.

**Bus data is misplaced.** `Bus.x` and `Line.x` are one attribute `x`, a longitude and a reactance, because both are a `Float64` over `entity`; `Bus.p` and `Bus.q` are not written because a bus is an entity and its `p` would be the connection-level `p`; `Bus.control` is dropped because it is an input on a Generator.

PyPSA is going the same way: [PyPSA#1474](https://github.com/PyPSA/PyPSA/issues/1474) makes buses and carriers indices that components reference rather than components themselves.

## Specification

### The schema PyPSA declares

```yaml
dimensions:
  entity: { dtype: String }
  bus: { dtype: String }
  carrier: { dtype: String }
  snapshot: { dtype: { Datetime: us } }
  scenario: { dtype: String }

groups:
  connection:
    over: { entity: entity, bus: bus }

properties:
  entity:
    entity_type:
      dtype: { Enum: [Generator, Line, Link, Load, Process, ShuntImpedance, StorageUnit, Store, SubNetwork, Transformer] }
    carrier: { into: carrier }
    sign: { dtype: Float64, default: 1, default_by: { entity_type: { Load: -1 } } }
    type: { dtype: String, default: "" }
  bus:
    carrier: { into: carrier, default: AC }
    type: { dtype: String, default: "" }
    x: { dtype: Float64, default: 0 }
    y: { dtype: Float64, default: 0 }
    unit: { dtype: String, default: "" }
    location: { dtype: String, default: "" }
  carrier:
    color: { dtype: String, default: "" }
    nice_name: { dtype: String, default: "" }
  connection:
    role: { dtype: { Enum: [input, output, attached] } }

attributes:
  x: { dtype: Float64, dims: [entity], default: 0 } # a Line's reactance, as today
  v_nom: { dtype: Float64, dims: [bus], default: 1 }
  v_mag_pu_set: { dtype: Float64, dims: [bus, snapshot, scenario], default: 1 }
  v_mag_pu_min: { dtype: Float64, dims: [bus], default: 0 }
  v_mag_pu_max: { dtype: Float64, dims: [bus], default: .inf }
  co2_emissions: { dtype: Float64, dims: [carrier], default: 0 }
  max_growth: { dtype: Float64, dims: [carrier], default: .inf }
  max_relative_growth: { dtype: Float64, dims: [carrier], default: 0 }
  p_nom: { dtype: Float64, dims: [entity], default: 0 }
  # ... every other registry input as today

results:
  marginal_price: { dtype: Float64, dims: [bus, snapshot, scenario] }
  v_mag_pu: { dtype: Float64, dims: [bus, snapshot, scenario] }
  v_ang: { dtype: Float64, dims: [bus, snapshot, scenario] }
  p: { dtype: Float64, dims: [connection, snapshot, scenario] }
  # ...

partial: [entity, bus, scenario, connection]
```

**`entity_type` loses `Bus` and `Carrier`.** Its `Enum` is the component types the tool exports as entities; a bus or a carrier is never an entity row, so `dims/entity.parquet` holds none and `remove("entity", ["north"])` cannot touch a bus. `Shape` and `GlobalConstraint` are out of scope here, as today.

**Bus and carrier data is placed by [the properties rule](../schema.md#properties)**, read off the registry per name as `ENTITY_PROPERTIES` is today, not by dtype: a property identifies, classifies or describes the label; an attribute is what a model reads as part of its problem and what a layer would vary. Every name is PyPSA's.

| PyPSA                                                                   | record                                                           |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `Bus.type`, `Bus.carrier`, `Bus.x`, `Bus.y`, `Bus.unit`, `Bus.location` | properties of `bus`                                              |
| `Bus.v_nom`, `Bus.v_mag_pu_min`, `Bus.v_mag_pu_max`                     | attributes over `bus`                                            |
| `Bus.v_mag_pu_set`                                                      | attribute over `bus`, `snapshot` and `scenario`                  |
| `Bus.marginal_price`, `Bus.v_mag_pu`, `Bus.v_ang`                       | results over `bus`, `snapshot` and `scenario`                    |
| `Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator`, `Bus.sub_network`     | not written: derived                                             |
| a custom Bus column (`country`, `busmap_5`)                             | attribute over `bus`, as a custom entity column is over `entity` |
| `Carrier.color`, `Carrier.nice_name`                                    | properties of `carrier`                                          |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth`            | attributes over `carrier`                                        |
| a component's `carrier`                                                 | the property `carrier` of `entity`, `into` `carrier`             |

`x` and `y` are properties because a coordinate on a map describes a bus, as `sign` describes a load — the [schema page's own example](../schema.md#properties) of metadata is `bus: {x: ...}`, and a `Float64` is no argument against it. `v_nom` is data: the per-unit base every impedance is scaled by, and what a layer reinforcing a corridor changes. A custom column is an attribute because the registry says nothing about it, which is the rule custom entity columns follow today.

**A property `into` a dim.** `PropertySpec` gains `into: str | None`, naming a declared dim: the property's values are labels of that dim, so its `dtype` is the dim's and is not declared beside it. A stored value must name a live label — checked where a group tuple's field is, at [`set`, `add`](../working-record.md#validation) and [the writer](../layers.md#deletion) — and NULL means none, which is PyPSA's `""`. A property `into` a dim does **not** categorize it: a [category](../schema.md#categories-an-enum-property-of-a-partial-dim) is a value of a schema-declared `Enum`, and an axis's labels are data; `carrier` keys no trait and no `default_by`. Any subject may declare one, a group included.

This settles what the properties proposal left open: **a functional group over one coordinate is a property.** `bus_country` `over: [bus]` `into: country` is `properties: {bus: {country: {into: country}}}`, one column of `dims/bus.parquet` rather than a file keyed by a one-field struct; a `Group` with one coordinate and an `into` is refused with a pointer to the property form. [`into` on a group](../schema.md#into-a-group-that-classifies) stays for a tuple of two or more coordinates — `over: [bus, scenario] into: country`. [A chain is still not denormalised](../schema.md#within-is-not-into): bus `into` state and state `into` country are two columns on two axis files, and there is no third stating bus→country.

### A property's name is its subject's

**A property is addressed by `(subject, name)`**, the file it is a column of and the column, and its name is unique there and nowhere else. `x` is a property of `bus` and an attribute over `entity`; `carrier` is a property of `entity`, a property of `bus` and a dim; `type` is a property of both. [One name, one spec](../schema.md#properties) stays the rule for **attributes and results**, where one name is one file in `inputs/` or `outputs/` with one `value` column; a property is a column of its subject's file, and two files may each have a column `x` without either reading the other's.

What is still refused, at schema load, is exactly what would collide in one file or one call:

- **A property named like its subject's own key.** `dims/bus.parquet` is keyed by `bus`, and a nested dim's file by `(*parents, dim)` ([`within`](../schema.md#within-an-axis-inside-an-axis)), so a property of `timestep` may be named neither `timestep` nor `period`. A group's file is keyed by its struct column, so a property of `connection` may not be named `connection`.
- **A property of a group named like one of its coordinates.** The coordinates are the struct's fields, not columns, but they are the keywords a tuple is addressed by — `entity=`, `bus=` — and a property of the group would take the same keyword.
- **A property named like an attribute or result whose `dims` name its subject.** A `v_nom` property of `bus` beside a `v_nom` attribute over `bus` would both answer `set("v_nom", 380.0, bus="north")`. An attribute reaching the subject only through a group's field — `efficiency` over `connection` — is no collision: a group value is addressed by the whole tuple, never by `bus=` alone.
- **Two spellings differing only in case**, record-wide, as today: DuckDB resolves `X` and `x` to one column, and the fold unions files by name.

Two properties of one subject cannot share a name — `properties[subject]` is a mapping — and a trait bundling `x` for a dim it categorizes is unambiguous, since within one dim's reach a name is an attribute or a property of that dim, never both.

**`set(name, value, **coords)` has as many readings of `name` as there are specs**: its attribute or result, and each subject declaring it. A reading fits when every keyword is one it takes — a coordinate the attribute reaches, or a categorizing property of a dim it reaches, for the attribute; the subject's key, or a group's fields, for a property. Exactly one fitting reading proceeds; none is the error it is today; more than one is refused naming the readings and the keyword that would settle it. A keyword is read as a **coordinate first**: `carrier="gas"` on `co2_emissions` is the dim `carrier`, which the attribute reaches, and on `p_max_pu` it is a property of `entity`, which it does not.

```python
w.set("x", 10.1, bus="north")  # the property of `bus`
w.set("x", 0.05, entity="line1")  # the attribute, a Line's reactance
w.set("x", 0.0)  # refused: a property of `bus` and an attribute over `entity` both fit
w.set("carrier", "H2", entity="electrolyser")  # the property of `entity`
w.set("carrier", "DC", bus="north DC")  # the property of `bus`
w.set("carrier", {"g1": "wind"})  # refused: keys of `entity` or of `bus`
w.set("carrier", series.rename_axis("entity"))  # settled by the index's name
w.set("co2_emissions", 0.2, carrier="gas")  # `carrier` the dim
w.set("p_max_pu", 0.8, entity_type="Generator")  # `entity_type` a property of `entity`
```

[`add(dim, labels, **columns)`](../working-record.md#add-remove) is unchanged: the subject is its first argument, so `add("bus", ["north"], x=10.1, carrier="AC")` names the properties of `bus` and nothing else. `kind="outputs"` and an `nw.Expr` select the attribute reading, a property being neither.

**In `Schema`**, every lookup of a property takes its subject. `subject_of(name)` and `property_spec(name)` go; `property_spec(subject, name)` stays, `subjects_of(name)` lists the subjects declaring one, in declaration order, for `set` to enumerate its readings, and `default_for(name, **categories)` answers attributes and results only, a property's default being `property_default(subject, name, **categories)`. `properties_for(subject, **categories)` and `categorizing(dim)` are per subject already. A staging table's column type is looked up by `(subject, name)`, which the table for `dims/<dim>.parquet` knows.

**[`flags`](../record.md#flags) says nothing about properties**, so `flags()["x"]` is the attribute's and no change is needed; `record.properties["bus"]` and `record.properties["entity"]` each carry a `carrier` column and `record.dims["carrier"]` the axis, which is what makes `properties["entity"].join(dims["carrier"], on="carrier")` the natural spelling of the reference.

**A custom column clashes only with an attribute.** The tool's rule for a custom column — an attribute over its owner's dim — meets one name, one spec where two owners declare the same name with different `dims`: a custom `Bus.country` beside a custom `Generator.country` is `country` over `bus` and over `entity`, and `to_datarecord` reports it as unbuildable, naming the column and both owners (`Requirements.columns`), never renamed. A custom column named like a property of **another** subject — a `Generator.location` beside the registry's `Bus.location` — no longer clashes, and neither does a custom entity column named like a registry attribute of the same shape, as today. `Requirements.columns` keeps its shape, `(owner, column)`, and its meaning narrows to attributes.

**Results.** `marginal_price`, `v_mag_pu` and `v_ang` are results over `bus` and the varying dims, read off the registry like every result. `Bus.p` and `Bus.q` stay unwritten, for the reasons they are today — the signed sum of the connection `p`/`q` at the bus, and `p` is one spec record-wide, over `connection`. `Bus.control`, `Bus.generator` and `Bus.sub_network` join them: they are what `determine_network_topology` and a power flow write back from the connections, and `sub_network` is already a result over `entity` on a Line. `_DERIVED_RESULTS["Bus"]` names all five.

**`bus` joins `partial`; `carrier` does not.** A layer patches one bus's `v_nom` or `carrier` without restating every bus's, as it patches one entity's `p_nom`, and a category on `bus` would need it there. `carrier` stays out: a layer touching `co2_emissions` restates every carrier's, which is a few dozen rows, where every `partial` coordinate [widens the fold key for every read](../schema.md#partial-the-granularity-of-an-override). The tool page's statement that `bus` is outside `partial` because its connections' deletion is a cascade goes: both are true now, and neither is the reason for the other.

**Deletion.** `remove("bus", ["north"])` is [the cascade every dim has](../layers.md#deletion): the label is tombstoned, every `connection` tuple naming it with it, and its values over `bus` are anti-joined in the fold; a result keyed by it is dropped at read. An entity left with no connection its type needs is `verify`'s to report, as a `role` it cannot place is. `remove("carrier", ["coal"])` while an entity's or a bus's stored `carrier` names it is **refused**, as removing a label a tuple still names is: a tombstone never reaches into a row that stays, so the caller re-points or removes the referrers first. Outside `partial`, a removed carrier's `co2_emissions` is dropped at read.

**Names collide within a dim only.** A bus `north` and a load `north` are labels of two dims. `to_datarecord` reports a collision between two component types it exports as entities — a Generator and a Load both named `north` — and no longer between a Bus and anything.

**`to_datarecord`** writes `dims/bus.parquet` from `n.buses` and `dims/carrier.parquet` from `n.carriers`, in their order. The `carrier` axis also gains every carrier a component or a bus names that `n.carriers` lacks, with no properties, so every stored `carrier` names a live label; `""` is stored as NULL. A bus static column is placed by the table above; `v_mag_pu_set` is long rows keyed by `bus`, constant or per snapshot by PyPSA's own static/series split, as an entity's series is today.

**`build`** reconstructs `n.buses` as `dims["bus"]` in [axis order](../record.md#axis-order), joined with `properties["bus"]` with [`property_default`](../schema.md#default_by-a-default-per-category) applied, and with the bus-only rows of every attribute over `bus`; `v_mag_pu_set` goes static or dynamic by its [flags](../record.md#flags). `n.carriers` likewise, so a carrier `to_datarecord` added comes back as a row `n.carriers` did not have. A component's `carrier` is read from `properties["entity"]`, NULL as `""`. Bus results are set from `outputs` keyed by `bus`, as connection results are from rows keyed by the struct.

### The design pages that change

- [Tools](../tools.md): the collision example becomes a Generator and a Load; `partial` gains `bus` and loses its sentence about why `bus` was out; the properties list gains `bus` and `carrier`; the PyPSA→record table above; `Bus.p`/`q` keep their paragraph and gain the three topology outputs; the custom-column clash narrowed to attributes.
- [Format](../format.md#entity-is-unique-across-types): the collision example likewise; [where a value lives](../format.md#where-a-value-lives) loses "no column on the classified axis"; [the entity axis](../format.md#the-entity-axis)'s "one name has one spec" says attributes and results.
- [Schema](../schema.md#properties): `PropertySpec.into`; **"One name, one spec, across every section"** becomes the per-subject rule and the refused list above; the example drops `Bus` from the `Enum` and declares `carrier: {into: carrier}` on `entity` and `carrier`, `type`, `x` on `bus`; [`into`](../schema.md#into-a-group-that-classifies) states the one-coordinate rule; [names](../schema.md#names) notes a property's name is scoped to its subject; [`partial`](../schema.md#partial-the-granularity-of-an-override)'s "not `timestep` or `bus`" becomes "not `timestep` or `carrier`".
- [Record](../record.md#the-broadcast-rule): `set("p_max_pu", 0.9)` reaches every generator "and never a load" where it said a bus; [`flags`](../record.md#flags) says that a name both an attribute and a property is the attribute's there.
- [Working record](../working-record.md#set): the readings rule and coordinate-first keywords; [validation](../working-record.md#validation) lists an ambiguous reading; [`add` / `remove`](../working-record.md#add-remove): `remove` refusing a label a property `into` the dim still names.
- The usage [schema page](../../usage/schema.md), whose "one name, one spec, record-wide, across every section" becomes attributes and results.

## Rationale

**One thing, one row.** A bus is a label of `bus` and nothing else, so adding, removing and describing it happen in one file, and the connection group and the bus's data agree by construction rather than by spelling.

**Placement follows the rule, not the type.** `x` was an attribute over `entity` because `Bus` was an entity; it is a property of `bus` because a coordinate describes a bus, which is the test the schema states. `Bus.x` and `Line.x` stop being one thing by accident — and keep their name, because a property already has a namespace, its subject, and a prefix would spell that namespace into the name a second time.

**The record-wide rule was protecting a file, and a property has one.** One name, one spec exists so that `inputs/x.parquet` is one file with one `value` dtype; a property's file is its subject's, keyed by the subject, and nothing reads a property without naming the subject first. What the rule still has to protect is a column within one file and a keyword within one call, which is the refused list.

**`into` on a property is the smallest construct that makes `carrier` a reference.** It reuses the word and the check a group already has, answers the open question about single-coordinate `into` groups by removing them, and costs core one optional field, one parse-time check and one write-time join — the join the writer already does for a tuple's field. Sharing the dim's name is what lets the reference column join the axis on its own name.

**What it costs.**

- **Every PyPSA record stops reading.** `entity_type`'s `Enum` shrinks, a [dtype change](../schema.md#versioning); `x`, `type` and `carrier` move subject or section; bus series move from `entity`-keyed rows to `bus`-keyed. A record is re-exported from PyPSA; there is no migration of files, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). Hand-written records following the old tool schema are in the same position.
- **A name no longer identifies one thing record-wide.** `x` needs a subject to be looked up, so `subject_of` and `property_spec(name)` go, and a `set` that fits two readings needs a keyword where it needed none. A consumer reading `inputs/x.parquet` and `dims/bus.parquet` without the schema finds two `x` columns meaning two things.
- **The fold key widens by one** for every read, `bus` being `partial`.
- **A round trip can add carrier rows** — the ones a network referenced without listing — and reads a `""` carrier back as `""` through NULL, so a network with `""` listed in `n.carriers` does not round-trip that row.
- **Removing a carrier is a two-step edit** where it used to be a tombstone plus a dangling string.
- **A one-coordinate `into` group is gone**, so a schema spelling one is refused rather than read.

## Usage

Reading:

```python
record.dims["carrier"]  # carrier
record.properties["bus"]  # bus | carrier | type | x | y | unit | location
record.attributes["x"]  # entity | attribute | breakpoint | value — a Line's reactance
record.attributes["v_nom"]  # bus | attribute | breakpoint | value
record.attributes[
    "v_mag_pu_set"
]  # bus | snapshot | scenario | attribute | breakpoint | value
record.outputs[
    "marginal_price"
]  # bus | snapshot | scenario | attribute | breakpoint | value
record.properties["entity"]  # entity | entity_type | carrier | sign | type

# a generator's carrier's emissions: two frames joined on the label
emissions = record.attributes["co2_emissions"].select("carrier", "value")
by_entity = record.properties["entity"].join(emissions, on="carrier")
```

Writing:

```python
w.add("carrier", ["H2"], color="#f0f", nice_name="Hydrogen")
w.set("co2_emissions", 0.0, carrier="H2")

w.add("bus", ["north H2"], carrier="H2", x=10.1, y=52.0)
w.set("v_nom", 380.0, bus="north")  # one bus, `bus` being partial
w.set("x", 10.2, bus="north")  # the bus's longitude; `entity=` would be a reactance
w.set("v_mag_pu_set", series.rename_axis("snapshot"), bus=["north"])

w.add("entity", ["electrolyser"], entity_type="Link", carrier="H2")
w.add_group(
    "connection", frame
)  # (electrolyser, north) input, (electrolyser, north H2) output
w.set("carrier", "coal", entity="g1")  # refused: `coal` is no label of `carrier`
w.set("carrier", "AC")  # refused: a property of `entity` and of `bus`; say which

w.remove("bus", ["south"])  # tombstones the label and every connection at it
w.remove("carrier", ["gas"])  # refused while an entity's or a bus's carrier names it
```

With PyPSA:

```python
n = pypsa.examples.ac_dc_meshed()
record = PyPSA.to_datarecord(
    n, ws.con
)  # no Bus or Carrier entity; `dims/bus`, `dims/carrier`
back = PyPSA.build(record)
back.buses[["v_nom", "x", "y", "carrier"]]  # column for column, under PyPSA's names
back.carriers  # every carrier `n` listed, plus any a component named and `n` did not
```

## Implementation notes

1. **Core: a property's name is its subject's.** `_check_names` keeps one name, one spec for attributes and results, and refuses a property by the list above; `subject_of` and `property_spec(name)` go for `subjects_of(name)`, `property_spec(subject, name)` and `property_default(subject, name, **categories)`; `set` enumerates its readings and refuses an ambiguous call, reading a keyword as a coordinate first; the staging table's column type is looked up per subject. Tests in `tests/test_schema.py` and `tests/test_working_record.py`.
2. **Core: `PropertySpec.into`.** Parse and validate — a declared dim, no `dtype` beside it, no `Enum`; a one-coordinate `into` group refused. `set` and `add` refuse a value naming no live label; the writer folds the parent's axis to check a layer's stored values, as it does a tuple's fields; `remove` refuses a label a property `into` the dim still names.
3. **Tool: the schema.** `_AXIS_TYPES = {"Bus": "bus", "Carrier": "carrier"}` taken out of the entity loop of `_registry_schema`; the `carrier` dim; bus and carrier properties and attributes by the table, through a `BUS_PROPERTIES`/`CARRIER_PROPERTIES` list beside `ENTITY_PROPERTIES`; `_DERIVED_RESULTS["Bus"]` grown; `partial` gains `bus`.
4. **Tool: the frames.** `_NetworkSource` writes the two axes, their properties and their long rows keyed by `bus` or `carrier`; missing carriers added; `_custom_columns` covers the two axes and `_clashing_columns` reports an attribute-against-attribute clash only; the entity-name collision check drops `Bus`.
5. **Tool: `build`.** `n.buses` and `n.carriers` from the axes, properties and attributes; bus results from `outputs`; `verify` reports an entity with no connection and a column clash.
6. **Docs and fixtures.** The pages listed above, the usage pages, `CHANGELOG.md`; every test fixture re-exported.

The checks that hold once it is built:

- **No `Bus` or `Carrier` in `entity_type`'s `Enum`**, and no row of `dims/entity.parquet` is a bus or a carrier.
- **A Bus and a Generator named `north` round-trip.** `to_datarecord` writes them; `build` finds both by name.
- **`n.buses` and `n.carriers` round-trip** column for column, under PyPSA's names and in order, a carrier only `n.generators` named appearing as a new row.
- **A schema declaring `x` as a property of `bus` and an attribute over `entity` loads**, `set("x", v, bus=...)` and `set("x", v, entity=...)` each reach their own, and `set("x", v)` is refused naming both.
- **A property named like its subject's key, a group's coordinate, or an attribute over its subject is refused** at schema load, naming the collision.
- **`v_mag_pu_set` and `marginal_price` are keyed by `bus`**, with no `entity` column.
- **`set("carrier", "coal", entity="g1")` is refused** while `coal` is no label; `remove("carrier", ["gas"])` is refused while a generator names it; `remove("bus", ["north"])` tombstones its connections and drops its `v_nom`.
- **A `Group` with one coordinate and an `into` is refused** at schema load.
- **`verify` reports** a custom `Bus.country` beside a custom `Generator.country`, naming both, and passes a custom `Generator.location` beside `Bus.location`.

## Rejected Ideas

**A carrier classification as `into` groups** — `entity_carrier` over `[entity]` and `bus_carrier` over `[bus]`, each `into` `carrier`, as the PR this grew from built it. Two files state what one column of the label's own row says, and a group keyed by a one-field struct is the shape the properties proposal asked whether to keep. It is kept for tuples, where a column has nowhere to go.

**`carrier` as a `String` property, unchecked.** No core change, and no reference: a generator may name a carrier that does not exist, `""` and NULL both mean none, and removing a carrier dangles. The check is what makes a carrier an axis rather than a convention.

**Keeping one name, one spec across properties, and renaming by a written table** — `bus_type`, `bus_carrier`, `bus_x`, `bus_y`, inverted by `build` as `p_internal` and `scenario_weight` are. The table is a namespace spelled into four names, when a property already has one in its subject; a consumer learns the table to read `n.buses.x` back, and every further subject with a `type` or an `x` — a `country` axis, a `technology` axis — grows it. `p_internal` and `scenario_weight` stay renamed because they are attributes, where one name is still one file.

**Prefixing by a rule.** Reading the registry prefixes what another type claims — `bus_x` beside `y`; reading the network makes a column's record name depend on what else the network holds, so a spec fixed against one record does not bind to the next. Against the per-subject rule it is worse still: it renames what no longer collides.

**Prefixing every bus column.** `bus_v_nom`, `bus_unit`: consistent and unnecessary; the subject is the prefix, and the record's names are PyPSA's.

**Scoping attributes per dim too** — `country` over `bus` and `country` over `entity` as two attributes. An attribute is one file in `inputs/` with one `value` column and one place in the fold key; two shapes under one name would need two files under one name. A property has a file per subject already; an attribute does not.

**`carrier` in `partial`.** Patching one carrier's `co2_emissions` value by value is not worth a key column on every read; the axis is small enough to restate.

**Writing `Bus.p` and `Bus.q`** now that a bus has its own dim. `p` is still one spec, over `connection`, and the sum is still the connections'.

**`type` on `bus` as an `Enum`.** PyPSA's `Bus.type` is free text, so a category would be a vocabulary the tool invents.

## Open Issues

- **Whether a property `into` a `partial` dim may categorize it.** A trait `on: {carrier: [wind]}` and a `default_by` keyed on `carrier` would key on an axis's labels rather than a schema's `Enum`; the vocabulary becomes data, checkable only against the resolved axis. With `carrier` a property of two subjects, a trait would also have to say which, as its `on` names one property today.
- **Whether a default must name a live label.** A bus's `carrier` defaults to `AC`, which is never stored and so never checked; a record whose `carrier` axis lacks `AC` reads every undeclared bus as `AC` all the same. `to_datarecord` adding `AC` whenever a bus defaults to it would close it at the cost of a row.
- **Whether `bus` earns its place in `partial`.** The fold-key cost is paid by every read and is to be measured on a pypsa-eur-sized record; the alternative is restating `v_nom` for every bus on a one-bus patch.
- **Whether a custom bus column can be a property.** `country` and `busmap_5` describe a bus, and the rule makes them attributes because the registry is silent — which is also what makes a custom `Bus.country` beside a `Generator.country` the one clash left. A hint the network carries, or a tool-level list, would turn both into properties and dissolve it.
- **Whether `set` should fall back to one reading by its value.** A mapping keyed by labels could be matched against each candidate subject's axis; the working-record page refuses that for coordinates — a label is a string like any other — and the same argument applies here, but a name on two subjects makes the refusal bite more often.
- **What PyPSA#1474 lands as.** When PyPSA makes carriers and buses indices, a carrier referenced but unlisted may become an error there, and `""` may stop meaning none.
