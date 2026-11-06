<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 3: trait switches, a trait a component opts into

| DEP           | 3                                             |
| ------------- | --------------------------------------------- |
| Title         | trait switches, a trait a component opts into |
| Author        | Jonas Hörsch                                  |
| Status        | Superseded (2026-10-20)                       |
| Created       | 2026-08-27                                    |
| Superseded-By | [DEP 16](dep-0016-remove-traits.md)           |

## Abstract

A trait gains an optional `switch`: a boolean attribute over `entity` that turns the trait on per component. A value of a switched trait on a component whose switch is false is refused on write. It shares a direction with [DEP 2](dep-0002-groups.md) without depending on it. What a record states about a component is declared, not encoded in a type name.

## Motivation

A trait is scoped by entity type alone. A distinction finer than a type, or one in a record with no types, has nowhere to go. In PyPSA a generator may be unit-committed, which decides whether `start_up_cost`, `min_up_time` and `ramp_limit_start_up` apply to it. The schema can only declare them on every generator. A record then has them on 3000 generators to describe the fifty that are committable. The same shape recurs for `cyclic_state_of_charge`, `e_cyclic`, link losses and extendable capacity.

Most traits are scoped correctly by type: every generator is dispatchable. A trait scoped by data, such as `country`, is rightly banned, because `attributes_for` must answer from the schema alone. A switch is a declared attribute with a dtype and a default, and both vocabularies it selects between are known before any row is read.

## Specification

- A trait narrows two ways, both optional. `on` lists the entity types that have it and reaches every type when absent. `switch` is an attribute that turns it on per component and gates nothing when absent.
- `switch` is a declared attribute whose `dims` are exactly `{"entity"}`, and its dtype is `bool`.
- The switch joins the trait's `attributes` at parse. The author does not list it.
- The switch is an ordinary attribute, stored where an attribute over `entity` alone is stored. Its default applies where it is unset, and `false` is the one to declare.
- The trait does not narrow its own switch. Every type the trait is `on` has the switch attribute.
- `on` applies first and `switch` second, and neither changes what the other means.
- `attributes_for(ctype)` includes a switched trait's attributes, because it answers which attributes a type may have.
- A value set on a component whose switch is false is refused on write.
- `record.entities_with("committable")` returns the components whose switch is true.

| `on` | `switch` | Reaches                                |
| ---- | -------- | -------------------------------------- |
| no   | no       | every component of every type          |
| yes  | no       | every component of the listed types    |
| no   | yes      | the components whose switch is true    |
| yes  | yes      | those of the listed types, switch true |

## Rationale

The third row of the table is the new case. A record that declares no types can still say that some components are committable, which the type scope could never express. A switch is a validation and query mechanism, not a change to the vocabulary. It turns "3000 generators with a mostly NULL `min_up_time`" into "fifty may, and the fifty-first is a mistake".

Listing the switch in `attributes` as well as in `switch` would be two ways to say one thing, so the schema completes the trait. A trait that gated its own switch could never be turned on. A switch belongs only where components of one type differ: on `dispatchable` it would be a column of `true` with no information.

The design costs three things. A trait now cuts by type and by component, and a reader holds both, though they compose in one direction. The writer reads the switch column to validate a value, a join against `dims/entity.parquet` on a path that already reads it. And a record may still have `committable` as a plain attribute and ignore the trait, so two spellings exist.

## Usage

```yaml
traits:
  committable:
    attributes: [start_up_cost, shut_down_cost, min_up_time, ramp_limit_start_up]
    on: { entity_type: [Generator, Link] }
    switch: committable
attributes:
  committable:
    dtype: bool
    dims: [entity]
    default: false
```

`committable` reaches no `Store` whatever its switch is, and reaches a generator only where its `committable` is true.

## Implementation notes

- At parse, `Trait(attributes={...}, switch="committable")` folds the switch into `attributes`. The model keeps `switch`, as [DEP 2](dep-0002-groups.md) keeps `into`.
- `attributes_for` reads `switch` to exclude it from its own trait's narrowing. The write-time check reads it to find the gating column.
- The existing trait validator checks `switch` as it checks a bundled attribute, plus that its `dims` are `{"entity"}`.

## Rejected ideas

- Scoping a trait on data such as `country`: `attributes_for` must answer from the schema alone.
- A switch over `entity` and `scenario`: coherent, but what a coarser row means is unsettled, as for existence along a dim in [DEP 1](dep-0001-dims-groups-traits.md). Worth revisiting only with a case.

## Open issues

- An `Enum` switch with a `switch_value` field, such as `capacity_mode` of `fixed`, `extendable` or `committable`. Deferred because no tool needs it, and a `bool` switch is its case with the label fixed at `true`.
- Whether `attributes_for` needs a per-entity counterpart. This is the same shape as the open question about a record-level `flags`.
