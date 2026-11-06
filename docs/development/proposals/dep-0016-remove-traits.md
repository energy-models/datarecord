<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 16: remove traits

| DEP      | 16                                                                                                   |
| -------- | ---------------------------------------------------------------------------------------------------- |
| Title    | remove traits                                                                                        |
| Author   | Jonas Hoersch                                                                                        |
| Status   | Implemented (2026-10-20)                                                                             |
| Created  | 2026-10-20                                                                                           |
| Version  | Unreleased                                                                                           |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (its traits), [DEP 3](dep-0003-trait-switches.md) (switches) |

The design landed in [the schema](../../reference/schema.md), [the data model](../../reference/data-model.md) and [the tools reference](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

`Trait` leaves the schema, together with its manifest form and every rule that reads it. An attribute applies to every label its `dims` address, and a property to every label of its subject. Categories and `default_by` stay. A tool that needs a per-type vocabulary keeps it itself.

## Motivation

A trait narrows which labels of a categorised dim have an attribute or property. That one rule touches much of the code:

- On the read path, a NULL over a categorised dim expands only to the labels that have the attribute. `categories.carriers` builds a `(label, attribute)` relation per category combination for this.
- `flags(**categories)` drops the attributes that a category does not have.
- `set` refuses a value on a label whose category does not have the attribute.
- Compatibility compares every category of two schemas in `Schema._dropped_by_category`.
- datarecord declares and validates `switch`, and nothing reads it.
- `_check_trait` checks six rules on `on`, the bundle and the switch.

One reader uses traits: the PyPSA tool. It derives one trait per component type from PyPSA's registry, and asks `attributes_for` which attributes a type takes. That answer is PyPSA's, read back from a schema the tool wrote. datarecord stores it, folds by it and validates against it, for a vocabulary that datarecord never defines. The queryable vocabulary that traits were meant to give, such as everything `investable`, has no caller.

## Specification

### What goes

| Construct                                            | Becomes                                                                            |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `Trait`, `Schema.traits`, `traits:` in a manifest    | removed, and datarecord refuses a manifest that declares `traits` at load          |
| `Trait.switch`                                       | removed, and the switch attribute stays an ordinary attribute                      |
| `Schema.attributes_for(dim, **categories)`           | removed, in favour of `[a for a in schema.attributes if schema.addresses(a, dim)]` |
| `Schema.properties_for(subject, **categories)`       | removed, in favour of `schema.properties[subject]`                                 |
| narrowed broadcast (`categories.carriers`)           | a NULL means every label of the dim                                                |
| `flags(**categories)` filtering by carried attribute | only the rows that apply to a matching label count                                 |
| `set` refusing a label whose category lacks it       | removed, and the label check stays                                                 |
| `_dropped_by_category` in compatibility              | removed                                                                            |

### What stays

- An `Enum` property of a `partial` dim still categorises it. `Schema.categorizing`, `require_categorizing` and `require_listed` stay.
- `default_by` keys on a categorising property, and `default_for(name, **categories)` is unchanged. It never depended on traits.
- `partial` is unchanged. It was a condition for a category, not an input to a trait.
- `flags(ctype="Generator")` and a property keyword in `set` select labels by category.
- `with_defaults` applies `default_for` per category combination. Every name now applies to every label, so its `_carries_` columns go.

### The manifest

`traits` is no field of `Schema`, and `extra="forbid"` would refuse it with a generic message. A `mode="before"` validator refuses it first, as for `within`:

```text
a schema takes no `traits`: an attribute reaches every label its `dims`
address; scope a value with a category keyword on `set`, and keep a
per-type vocabulary in the tool that needs it
```

### The PyPSA tool

- `_registry_schema` declares no traits. It keeps the per-type input names as a cached `_carried(ctype)`.
- `_NetworkSource.schema` adds a type's custom columns to that map, not to a trait.
- `_verify` and `build` call `_carried(ctype)` where they called `attributes_for(ENTITY, ctype=ctype)`.
- The static frame contains the entity properties that `_carried(ctype)` lists, where it called `carried_properties`.

## Rationale

The tool already has the narrowing. PyPSA's registry already lists which attributes a `Load` takes. A copy in the schema makes datarecord enforce a framework's vocabulary, and under the [package layout](../package.md) framework knowledge stays out of core.

A broadcast over every label is the general rule. Every dim that is not categorised already broadcasts that way. A categorised dim was the one exception, and the fold, `flags` and validation each had a branch for it.

`default_by` is not a trait. It sets a value per category, and has no effect on whether a value exists, so it stays unchanged.

The costs: `set("p_max_pu", 0.9)` now also applies to loads, which PyPSA ignores. datarecord no longer refuses a misplaced value on write. A NULL row expands over every label in the owner map, not only the labels that had the attribute. Every record that declares traits is re-exported, as [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md).

## Usage

Before:

```python
Schema(
    ...,
    traits={
        "dispatchable": Trait(
            attributes={"p_max_pu"},
            on={"entity.ctype": {"Generator", "StorageUnit"}},
        )
    },
)
w.set("p_max_pu", 0.9)  # every generator and storage unit, never a load
schema.attributes_for("entity", ctype="Load")  # no p_max_pu
```

After:

```python
Schema(...)  # no traits
w.set("p_max_pu", 0.9, ctype=["Generator", "StorageUnit"])  # one row per label
w.set("p_max_pu", 0.9)  # every entity; PyPSA never reads a load's value
schema.default_for("p_min_pu", ctype="StorageUnit")  # -1, as before
```

## Implementation notes

1. `schema.py`: `Trait`, `Schema.traits`, `_check_trait`, `attributes_for`, `properties_for`, `_carried` and `_dropped_by_category` go. A `traits` key is refused at load.
2. `__init__.py`: `Trait` leaves the exports.
3. `layered/categories.py`: `narrowed`, `label_carriers`, `carriers`, `attributes_among` and `carried_properties` go, and `with_defaults` loses `_carries_`.
4. `layered/resolve.py`: the carriers join in the broadcast goes.
5. `layered/fold.py`: `flags(**categories)` filters by matching labels only.
6. `mutable.py`: the category check in `set` goes.
7. `tools/pypsa.py`: as in [the PyPSA tool](#the-pypsa-tool). `BY_CTYPE` stays for `default_by`.
8. Tests: the trait tests are deleted, and `fixtures.py` declares no traits.
9. Pages, usage pages and `CHANGELOG.md` drop traits and `switch`.
10. DEP 1 and DEP 3 take `Superseded-By` this DEP.

Checks: no `trait` in `src/`. A manifest with `traits` is refused with the replacement named. A PyPSA network round-trips.

## Rejected ideas

- Traits kept as an unenforced bundle for dispatch: a vocabulary that no caller asks for.
- Narrowing moved onto `AttributeSpec` as `carried_by`: the same rule under another name, with every branch kept.

## Open issues

- What `build` does with a value that its type does not take. The recommendation is that `_verify` reports it, as for an unknown type, and does not raise.
- Whether a category still needs `partial`, which existed for per-label narrowing. The recommendation is to keep it, because `default_by` per label needs a dim that the owner map keys per label.
- The `committable` case that DEP 3 raised. The recommendation is to leave it to the reader of the `committable` attribute.
- Owner-map size. The recommendation is to measure the PyPSA example before and after, and change nothing unless it regresses.
