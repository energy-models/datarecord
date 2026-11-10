<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 13: buses and carriers are dims

| DEP      | 13                               |
| -------- | -------------------------------- |
| Title    | buses and carriers are dims      |
| Author   | Jonas Hoersch, Bryn Pickering    |
| Status   | Implemented (2026-10-15)         |
| Requires | [DEP 12](dep-0012-properties.md) |
| Created  | 2026-10-14                       |
| Version  | Unreleased                       |

The design landed in [the tools reference](../../reference/tools.md), [the schema](../../reference/schema.md), [the data model](../../reference/data-model.md) and [the record format](../../reference/format.md). Where they and this DEP differ, follow those pages.

## Abstract

In the PyPSA tool, `Bus` and `Carrier` stop being entity types. A bus is a label of the `bus` dim and a carrier a label of a new `carrier` dim. What PyPSA stores on either becomes a property of the label or an attribute over the dim. Two core changes support this. A property may be `into` a dim, so that its values are that dim's labels. A property's name is scoped to its subject and addressed as `subject.name`.

## Motivation

A bus exists twice: as a label of `bus` and as an entity of type `Bus`. Only the spelling joins the two, so `remove` on either leaves half behind. Because a bus is an entity, a Bus and a Load both named `north` collide, and every clustered pypsa-eur network has such a pair. `Bus.x` and `Line.x` are one attribute, and `Bus.p` cannot be written. A carrier is a string that datarecord checks against no axis. [PyPSA#1474](https://github.com/PyPSA/PyPSA/issues/1474) makes buses and carriers indices in PyPSA in the same way.

## Specification

### The tool

The PyPSA tool declares the dims `entity`, `bus`, `carrier`, `snapshot` and `scenario`. Its `partial` is `entity`, `bus`, `scenario` and `connection`.

- `entity.component` replaces `entity_type` and categorises `entity`. Its `Enum` lists the types exported as entities, without `Bus` or `Carrier`. `Shape` and `GlobalConstraint` stay out of scope. `on` and `default_by` refer to a categorising property by its dotted name, as in `sign`'s `default_by: {entity.component: {Load: -1}}`.
- The tool places each PyPSA column by its registry entry, under PyPSA's name, following the properties rule of the [data model](../../reference/data-model.md).

| PyPSA                                                                   | Record                                              |
| ----------------------------------------------------------------------- | --------------------------------------------------- |
| `Bus.type`, `Bus.carrier`, `Bus.x`, `Bus.y`, `Bus.unit`, `Bus.location` | properties of `bus`                                 |
| `Bus.v_nom`, `Bus.v_mag_pu_min`, `Bus.v_mag_pu_max`, `Bus.v_mag_pu_set` | attributes over `bus` and the dims they vary along  |
| `Bus.marginal_price`, `Bus.v_mag_pu`, `Bus.v_ang`                       | results over `bus` and the dims they vary along     |
| `Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator`, `Bus.sub_network`     | not written, because the connections determine them |
| a custom Bus or Carrier column                                          | an attribute over its dim, as for a custom entity   |
| `Carrier.color`, `Carrier.nice_name`                                    | properties of `carrier`                             |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth`            | attributes over `carrier`                           |
| a component's `carrier`                                                 | `entity.carrier`, `into` `carrier`                  |
| a component's `sign`, `type`                                            | properties of `entity`                              |

- `partial` gains `bus` and not `carrier`, so a layer restates every carrier's `co2_emissions`, a few dozen rows.
- `remove("bus", …)` cascades as removal on any dim does, and `verify` reports an entity left unattached. datarecord refuses `remove("carrier", …)` while a stored `carrier` value is the label, as it does while a tuple contains a label.
- Labels collide only within a dim. A bus and a load may share a name, and two exported types may not.
- A custom column clashes only with an attribute. A custom `Bus.country` beside a custom `Generator.country` is `country` with two `dims`, which `Requirements.columns` reports. A name that a property has does not clash.
- `to_datarecord` writes the two axes in PyPSA's order, adds every carrier that a component uses but `n.carriers` lacks, and stores `""` as NULL. `build` rebuilds `n.buses` and `n.carriers` from them.

### A property `into` a dim

`PropertySpec.into` is the name of a declared dim, whose labels are the property's values and whose `dtype` it takes. A stored value must be a live label, which datarecord checks in `set`, in `add` and in the writer, as for a group field. NULL means none. Any subject may declare one. It does not categorise, because a category is a schema `Enum` and labels are data. datarecord refuses a group `over` one coordinate `into` a dim, and `into` on a group stays for two or more coordinates. A chain is still not denormalised.

### A property's name is its subject's

The rule that one name has one spec still holds for attributes and results. A property is a column of its subject's file, so its name is unique there only: `x` on `bus` sits beside the attribute `x`. datarecord refuses at schema load:

- a property named like its subject's key: the dim, a `within` parent, or a group's struct column
- a property of a group named like one of its coordinates, which are the keywords that address a tuple
- two names, record-wide, that differ only in case, and a `.` in any name.

A property is always addressed as `subject.name`, such as `bus.carrier` or `entity.component`. A plain name is an attribute, a result or unknown, and the error suggests the dotted forms of properties with that name. datarecord resolves the name alone, never by the keywords. `Schema.resolve(name)` returns the `AttributeSpec` or `(subject, PropertySpec)`, and `set`, `default_for`, `flags` and staging call it. `subject_of` and `property_spec(name)` go. A trait bundles a property by its dotted name.

A selector keyword is bare. It is a coordinate of the attribute or a categorising property of a dim that the attribute's coordinates cover, and a coordinate comes first. `carrier="gas"` on `co2_emissions` is the dim, and `component="Generator"` on `p_max_pu` is `entity.component`, never the `Shape` attribute. `add(dim, labels, **columns)` takes the dim's properties bare. datarecord refuses a dotted name with `kind="outputs"` and in an `nw.Expr`. `flags` and the frames of `properties[subject]` keep plain names.

## Rationale

`x` and `y` are properties and `v_nom` is an attribute by the properties rule, not by dtype. A coordinate describes a bus as `sign` describes a load, and `v_nom` is the per-unit base that a reinforcement layer changes.

The record-wide name rule exists so that `inputs/x.parquet` is one file with one `value` dtype. A property is a column of its subject's file, so only that column and its keywords need protecting. A prefix such as `bus_x` spells the subject into the name a second time, and the dot spells it once.

`into` on a property is the smallest way to refer to a dim's labels. It reuses the word and the live-label check of a group field, and settles the one-coordinate `into` question by removing the group form. A property named like its dim joins the axis on itself.

The costs: datarecord can no longer read an existing PyPSA record, so each is re-exported, as [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). Every property `set` in the tool, pages and tests becomes dotted. The fold key widens by one. A round trip may add carrier rows, and a listed `""` carrier comes back as none. Removing a referenced carrier takes two edits.

## Usage

```python
w.add("bus", ["north H2"], carrier="H2", x=10.1, y=52.0)  # bare: the dim's own
w.set("bus.x", 10.2, bus="north")  # `x` alone is a Line's reactance
w.set("entity.carrier", "coal", entity="g1")  # refused: no label `coal`
w.set("carrier", "AC")  # unknown; did you mean `bus.carrier` or `entity.carrier`
w.remove("carrier", ["gas"])  # refused while an entity's carrier is `gas`
```

## Implementation notes

1. Core: per-subject name refusals, `Schema.resolve`, and dotted names in `set`, `default_for`, `flags` and staging.
2. Core: `PropertySpec.into`, with its live-label check, its `remove` refusal and the one-coordinate `into` group refused.
3. Tool: `Bus` and `Carrier` leave the entity loop, the network source writes the two axes, and `build` and `verify` read them back.
4. Pages, changelog and about 20 test files take the dotted names and `component`.

## Rejected ideas

- Carrier as `into` groups (`entity_carrier`, `bus_carrier`): two files for what one column of the label's row contains.
- `carrier` as an unchecked `String`: no reference, `""` and NULL both mean none, and removal dangles.
- Prefixed bus columns such as `bus_x`, by table, registry rule or everywhere: a namespace spelled into the name, when the subject is one.
- A plain name resolved by the call's keywords, or by the one property declared with it. A name's meaning must not depend on the other arguments, or change when another subject declares it.
- Attributes scoped per dim too: an attribute is one file and one place in the fold key.
- PyPSA's `type` renamed `std_type`, freeing `type` for the category: it keeps PyPSA's names only where convenient.
- `carrier` in `partial`, writing `Bus.p` and `Bus.q` (`p` is one spec over `connection`), and `bus.type` as an `Enum` (it is free text in PyPSA).

## Open issues

- Whether a property `into` a `partial` dim, such as `entity.carrier`, may categorise.
- Whether a default such as `bus.carrier: AC` must be a live label. datarecord never checks a default.
- Whether `bus` is worth its fold-key column, measured on a pypsa-eur-sized record.
- Whether a custom bus column can be a property, which would remove the one remaining custom-column clash.
- Whether selector keywords need a qualified form, for a categorising name on two dims that one attribute's coordinates cover.
- What PyPSA#1474 lands as: an unlisted carrier may become an error, and `""` may stop meaning none.
