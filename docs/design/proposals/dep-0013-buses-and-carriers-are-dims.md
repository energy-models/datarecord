<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 13: buses and carriers are dims

| DEP      | 13                               |
| -------- | -------------------------------- |
| Title    | buses and carriers are dims      |
| Author   | Jonas Hoersch, Bryn Pickering    |
| Status   | Implemented 2026-10-15           |
| Requires | [DEP 12](dep-0012-properties.md) |
| Created  | 2026-10-14                       |
| Version  | Unreleased                       |

Once implemented, [the tool page](../tools.md), [the schema](../schema.md#properties) and [the format](../format.md#entity-is-unique-across-types) are authoritative where they and this text disagree.

## Abstract

In the PyPSA tool, `Bus` and `Carrier` stop being entity types: a bus is a label of the `bus` dim, a carrier a label of a new `carrier` dim, and what PyPSA stores on either is a property of the label or an attribute over the dim. Two core changes carry it: a property may be **`into`** a dim — its values are that dim's labels — and a property's name is scoped to its subject and addressed as **`subject.name`**.

## Motivation

A bus exists twice: as a label of `bus`, which `connection` ranges over, and as an entity of type `Bus` holding `v_nom`, `x`, `carrier`; nothing joins the two but the spelling, so `remove` on either leaves half. Because the bus is an entity, a Bus and a Load both named `north` — every clustered pypsa-eur network — is a [collision](../format.md#entity-is-unique-across-types), `Bus.x` and `Line.x` are one attribute, and `Bus.p` cannot be written. A carrier is a string no axis checks. [PyPSA#1474](https://github.com/PyPSA/PyPSA/issues/1474) makes buses and carriers indices, the same way.

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
  connection: { over: { entity: entity, bus: bus } }

properties:
  entity:
    component: # was `entity_type`; PyPSA's word for Generator, Line, ...
      dtype: { Enum: [Generator, Line, Link, Load, Process, ShuntImpedance, StorageUnit, Store, SubNetwork, Transformer] }
    carrier: { into: carrier }
    sign: { dtype: Float64, default: 1, default_by: { entity.component: { Load: -1 } } }
    type: { dtype: String, default: "" } # PyPSA's standard type, unrenamed
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
  x: { dtype: Float64, dims: [entity], default: 0 } # a Line's reactance
  v_nom: { dtype: Float64, dims: [bus], default: 1 }
  v_mag_pu_set: { dtype: Float64, dims: [bus, snapshot, scenario], default: 1 }
  co2_emissions: { dtype: Float64, dims: [carrier], default: 0 }
  # ... every other registry input as today

results:
  marginal_price: { dtype: Float64, dims: [bus, snapshot, scenario] }
  # ...

partial: [entity, bus, scenario, connection]
```

### The tool

- **`entity.component`**, renamed from `entity_type`, categorizes `entity`; its `Enum` is the types exported as entities — no `Bus`, no `Carrier`. `Shape` and `GlobalConstraint` are out of scope, as today. `on` and `default_by` name a categorizing property dotted.
- **Placement** follows [the properties rule](../schema.md#properties), read off the registry per name; every name is PyPSA's.

| PyPSA                                                                   | record                                               |
| ----------------------------------------------------------------------- | ---------------------------------------------------- |
| `Bus.type`, `Bus.carrier`, `Bus.x`, `Bus.y`, `Bus.unit`, `Bus.location` | properties of `bus`                                  |
| `Bus.v_nom`, `Bus.v_mag_pu_min`, `Bus.v_mag_pu_max`, `Bus.v_mag_pu_set` | attributes over `bus` (and the varying dims)         |
| `Bus.marginal_price`, `Bus.v_mag_pu`, `Bus.v_ang`                       | results over `bus` and the varying dims              |
| `Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator`, `Bus.sub_network`     | not written: derived from the connections            |
| a custom Bus or Carrier column                                          | attribute over its dim, as a custom entity column is |
| `Carrier.color`, `Carrier.nice_name`                                    | properties of `carrier`                              |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth`            | attributes over `carrier`                            |
| a component's `carrier`                                                 | `entity.carrier`, `into` `carrier`                   |

- **`partial`** gains `bus`, not `carrier`: a layer patches one bus's `v_nom`; it restates every carrier's `co2_emissions`, a few dozen rows.
- **Deletion.** `remove("bus", …)` is [the cascade every dim has](../layers.md#deletion); an entity left unattached is `verify`'s to report. `remove("carrier", …)` is refused while a stored `carrier` names the label, as a label a tuple still names is.
- **Collisions are within a dim.** A bus and a load may share a name; two exported types may not.
- **A custom column clashes only with an attribute**: custom `Bus.country` beside custom `Generator.country` is `country` with two `dims`, reported via `Requirements.columns` as today; a name a property has does not clash.
- **`to_datarecord`** writes the two axes in PyPSA's order, adds every carrier named but not listed in `n.carriers`, and stores `""` as NULL. **`build`** rebuilds `n.buses` and `n.carriers` from axis, properties with [defaults](../schema.md#default_by-a-default-per-category) and attribute rows, series by [flags](../record.md#flags), bus results from `outputs`.

### A property `into` a dim

`PropertySpec.into` names a declared dim; the property's values are its labels and its `dtype` is the dim's. A stored value must name a live label, checked where a group field is — [`set`, `add`](../working-record.md#validation), [the writer](../layers.md#deletion); NULL means none. It does not [categorize](../schema.md#categories-an-enum-property-of-a-partial-dim): a category is a schema `Enum`, labels are data. Any subject may declare one. A group `over` one coordinate `into` a dim is refused; [`into` on a group](../schema.md#into-a-group-that-classifies) stays for two or more coordinates, and [a chain is still not denormalised](../schema.md#within-is-not-into).

### A property's name is its subject's

**One name, one spec** holds for attributes and results. A property is a column of its subject's file, and its name is unique there only: `x` on `bus` beside the attribute `x`, `carrier` on `entity`, on `bus` and as a dim, `type` on both, `Shape`'s attribute `component` beside `entity.component`. Refused at schema load:

- a property named like its subject's key — the dim, a [`within`](../schema.md#within-an-axis-inside-an-axis) parent, or a group's struct column;
- a property of a group named like one of its coordinates, which are the keywords a tuple is addressed by;
- two spellings differing only in case, record-wide, and a `.` in any name ([names](../schema.md#names)).

**A property is always addressed as `subject.name`** — `bus.carrier`, `connection.role`, `entity.component`. A plain name is an attribute or result, or unknown, the error suggesting the dotted forms of properties so named. Resolution reads the name alone, never the keywords. `Schema.resolve(name)` returns the `AttributeSpec` or `(subject, PropertySpec)` and is what `set`, `default_for`, `flags` and staging call; `subject_of` and `property_spec(name)` go. A trait bundles a property by its dotted name.

A **selector keyword** is bare and names a coordinate of the attribute or a categorizing property of a dim it reaches, coordinate first: `carrier="gas"` on `co2_emissions` is the dim; `component="Generator"` on `p_max_pu` is `entity.component`, never the `Shape` attribute. [`add(dim, labels, **columns)`](../working-record.md#add-remove) takes the dim's properties bare. `kind="outputs"` and an `nw.Expr` refuse a dotted name. [`flags`](../record.md#flags) and the frames of `properties[subject]` keep plain names.

### Pages that change

- [tools](../tools.md) → collision example Generator/Load; `partial` sentence; properties list; the table above; the five derived bus results; custom-column clash narrowed.
- [format](../format.md#entity-is-unique-across-types) → collision example; [where a value lives](../format.md#where-a-value-lives) loses "no column on the classified axis"; "one name, one spec" scoped to attributes.
- [schema](../schema.md#properties) → `into` on a property, per-subject rule, dotted addressing, the example; [`into`](../schema.md#into-a-group-that-classifies) one-coordinate rule; [names](../schema.md#names); [`partial`](../schema.md#partial-the-granularity-of-an-override) "not `timestep` or `carrier`".
- [record](../record.md#the-broadcast-rule) → "never a load"; [`flags`](../record.md#flags) plain names.
- [working record](../working-record.md#set) → dotted `set`, selector rule; [validation](../working-record.md#validation) unknown-name error; [`add` / `remove`](../working-record.md#add-remove) referenced-label refusal.
- usage [schema](../../usage/schema.md), [reading](../../usage/reading.md), [editing](../../usage/editing.md), `CHANGELOG.md`; `entity_type` → `component` in the tool, the pages and ~20 test files (core: two docstrings).

## Rationale

**`x` and `y` are properties, `v_nom` an attribute**, by the rule and not by dtype: a coordinate describes a bus as `sign` describes a load, and the schema page's own metadata example is `bus: {x: …}`; `v_nom` is the per-unit base the model reads and a reinforcement layer changes.

**The record-wide name rule protects a file, and a property has one.** It exists so `inputs/x.parquet` is one file with one `value` dtype; a property's file is its subject's, so what remains to protect is a column in that file and the keywords addressing it. A prefix (`bus_x`) would spell the subject into the name a second time; the dot spells it once.

**`into` on a property is the smallest reference.** It reuses the word and the live-label check a group field already has, settles the single-coordinate `into` question by removing the group form, and sharing the dim's name lets the column join the axis on itself.

**Costs.** Every PyPSA record stops reading (`component`'s `Enum` and name, names moving section, bus rows re-keyed) and is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). Every property `set` in the tool, pages and tests becomes dotted. The fold key widens by one. A round trip may add carrier rows and reads a listed `""` carrier as none. Removing a referenced carrier is two edits. A one-coordinate `into` group is refused.

## Usage

```python
record.dims["carrier"]  # carrier
record.properties["bus"]  # bus | carrier | type | x | y | unit | location
record.properties["entity"]  # entity | component | carrier | sign | type
record.attributes["v_nom"]  # bus | attribute | breakpoint | value
record.outputs["marginal_price"]  # bus | snapshot | scenario | ...
```

```python
w.add("carrier", ["H2"], color="#f0f")
w.add("bus", ["north H2"], carrier="H2", x=10.1, y=52.0)  # bare: the dim's own
w.set("v_nom", 380.0, bus="north")
w.set("bus.x", 10.2, bus="north")  # `x` alone is a Line's reactance
w.set("entity.component", "Load", entity="g1")
w.set("entity.carrier", "coal", entity="g1")  # refused: no label `coal`
w.set("carrier", "AC")  # unknown; did you mean `bus.carrier` or `entity.carrier`
w.set("p_max_pu", 0.8, component="Generator")  # selector: the category
w.remove("bus", ["south"])  # and every connection at it
w.remove("carrier", ["gas"])  # refused while a carrier names it
```

## Implementation notes

1. Core: `_check_names` per-subject refusals; `Schema.resolve`; dotted `set`, `default_for`, `flags`, staging; `.` already refused.
2. Core: `PropertySpec.into` — parse, live-label check in `set`/`add`/writer, `remove` refusal, one-coordinate `into` group refused.
3. Tool: `_AXIS_TYPES = {Bus: bus, Carrier: carrier}` out of the entity loop; `carrier` dim; `BUS_PROPERTIES`/`CARRIER_PROPERTIES`; `_DERIVED_RESULTS["Bus"]`; `partial`; `entity_type` → `component`.
4. Tool: `_NetworkSource` writes the axes, properties and `bus`/`carrier`-keyed rows; missing carriers; clash check narrowed; `Bus` out of the name-collision check.
5. Tool: `build` from axes, properties, attributes and `outputs`; `verify` reports an unattached entity.
6. Pages, usage, changelog, tests dotted and renamed; fixtures re-exported.

Checks: no `entity_type` outside the proposals; a Bus and a Generator named `north` round-trip; `n.buses`/`n.carriers` round-trip under PyPSA's names; `marginal_price` keyed by `bus`; `x` and `bus.x` reach their own, `set("carrier", "AC")` names both dotted forms; `remove("carrier", …)` refused while referenced; `verify` reports `Bus.country` beside `Generator.country` and passes `Generator.location` beside `bus.location`.

## Rejected Ideas

- **Carrier as `into` groups** (`entity_carrier`, `bus_carrier`): two files stating what one column of the label's row says.
- **`carrier` as an unchecked `String`**: no reference, `""` and NULL both none, removal dangles.
- **Prefixing bus columns** — a written table (`bus_x`, `bus_type`), a registry rule (gives `bus_x` beside `y`), or every column: a namespace spelled into the name when the subject already is one.
- **Resolving a plain name by the keywords it comes with**, or falling back to a uniquely declared property: a name's meaning must not depend on the call's other arguments, nor change when another subject later declares it.
- **Scoping attributes per dim too**: an attribute is one file and one place in the fold key.
- **Renaming PyPSA's `type` to `std_type`** so the category could be `type`: keeps PyPSA's names only where convenient.
- **`carrier` in `partial`**, **writing `Bus.p`/`q`** (`p` is one spec over `connection`), **`bus.type` as an `Enum`** (free text in PyPSA).

## Open Issues

- Whether a property `into` a `partial` dim may categorize it — a vocabulary that is data, keyed as `entity.carrier`.
- Whether a default (`bus.carrier: AC`) must name a live label; a default is never stored or checked.
- Whether `bus` earns its fold-key column, measured on a pypsa-eur-sized record.
- Whether a custom bus column can be a property, which would dissolve the one remaining custom-column clash.
- Whether selector keywords need a qualified form, for a categorizing name on two dims one attribute reaches.
- What PyPSA#1474 lands as: an unlisted carrier may become an error, `""` may stop meaning none.
