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

## Abstract

`Trait` leaves the schema, with its manifest form and every rule reading it. An attribute reaches every label its `dims` address, and a property every label of its subject. Categories and `default_by` stay. A tool that needs a per-type vocabulary keeps it itself.

## Motivation

A trait narrows which labels of a categorized dim carry an attribute or property. That one rule reaches far:

- **The read path.** A NULL over a categorized dim expands only to the labels carrying it. `categories.carriers` builds a `(label, attribute)` relation per category combination for that.
- **`flags(**categories)`** drops the attributes a category does not carry.
- **`set` validation** refuses a value on a label whose category does not carry the attribute.
- **Compatibility.** `Schema._dropped_by_category` compares every category of two schemas.
- **`switch`** is declared and validated but read by nothing.
- **The schema check.** `_check_trait` holds six rules on `on`, the bundle and the switch.

One consumer uses it: the PyPSA tool. It derives one trait per component type from PyPSA's registry and asks `attributes_for` which attributes a type takes. That answer is PyPSA's, read back from a schema the tool wrote. The record layer stores it, folds by it and validates against it, for a vocabulary it never defines.

The "queryable vocabulary" a trait promised (everything `investable`) has no caller.

## Specification

### What goes

| construct                                            | becomes                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------ |
| `Trait`, `Schema.traits`, `traits:` in a manifest    | gone; a manifest declaring `traits` is refused at load             |
| `Trait.switch`                                       | gone; the switch attribute stays an ordinary attribute             |
| `Schema.attributes_for(dim, **categories)`           | gone; `[a for a in schema.attributes if schema.addresses(a, dim)]` |
| `Schema.properties_for(subject, **categories)`       | gone; `schema.properties[subject]`                                 |
| narrowed broadcast (`categories.carriers`)           | a NULL reaches every label of the dim                              |
| `flags(**categories)` filtering by carried attribute | rows reaching a matching label decide, nothing else                |
| `set` refusing a label whose category lacks it       | gone; the label check stays                                        |
| `_dropped_by_category` in compatibility              | gone                                                               |

### What stays

- **Categories.** An `Enum` property of a `partial` dim still categorizes it. `Schema.categorizing`, `require_categorizing` and `require_listed` stay.
- **`default_by`** keys on a categorizing property, as before. `default_for(name, **categories)` is unchanged. It never depended on traits.
- **`partial`** is unchanged. It was a condition for a category, not a trait input.
- **Category selection.** `flags(ctype="Generator")` and a property keyword in `set` select labels by category, as before.
- **`with_defaults`** applies `default_for` per category combination. Every name is carried, so its `_carries_` columns go.

### The manifest

`traits` is no field of `Schema`, and `extra="forbid"` would refuse it generically. A `mode="before"` validator refuses it first, as `within` is refused:

```text
a schema takes no `traits`: an attribute reaches every label its `dims`
address; scope a value with a category keyword on `set`, and keep a
per-type vocabulary in the tool that needs it
```

### The PyPSA tool

- `_registry_schema` declares no traits. It keeps `carries`, the per-type input names, as a cached `_carried(ctype)`.
- `_NetworkSource.schema` adds a type's custom columns to that map, not to a trait.
- `_verify` and `build` ask `_carried(ctype)` where they asked `attributes_for(ENTITY, ctype=ctype)`.
- The static frame keeps the entity properties `_carried(ctype)` names, where it asked `carried_properties`.

## Rationale

**The narrowing is the tool's knowledge.** PyPSA's registry already says which attributes a `Load` takes. Copying that into the schema makes the record enforce a framework vocabulary. The [module layout](../package.md) keeps framework knowledge out of core.

**A broadcast over every label is the general rule.** Every uncategorized dim already broadcasts that way. A categorized dim was the one exception, and the fold, flags and validation each carried a branch for it.

**`default_by` is not a trait.** It picks a value per category, never whether one exists. So it survives unchanged.

**Costs.** `set("p_max_pu", 0.9)` now also reaches loads, which PyPSA ignores. A misplaced value is no longer refused on write. A NULL row expands over every label in the owner map, not only carriers. Every record declaring traits is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md).

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
w.set("p_max_pu", 0.9)  # every entity; a load's value is never read by PyPSA
schema.default_for("p_min_pu", ctype="StorageUnit")  # -1, as before
```

## Implementation notes

1. `schema.py`: `Trait`, `Schema.traits`, `_check_trait`, `attributes_for`, `properties_for`, `_carried`, `_dropped_by_category` go. A `traits` key is refused at load. Docstrings drop the traits links.
2. `__init__.py`: `Trait` leaves the exports.
3. `layered/categories.py`: `narrowed`, `label_carriers`, `carriers`, `attributes_among`, `carried_properties` go; `with_defaults` loses `_carries_`.
4. `layered/resolve.py`: the carriers join in the broadcast goes.
5. `layered/fold.py`: `flags(**categories)` filters by matching labels only.
6. `mutable.py`: the category check in `set` validation goes.
7. `tools/pypsa.py`: as [above](#the-pypsa-tool); `BY_CTYPE` stays for `default_by`.
8. Tests: trait tests in `test_schema.py`, `test_categories.py`, `test_mutable.py`, `test_overlay.py`, `test_properties.py`, `test_functional_groups.py` are deleted; `fixtures.py` declares no traits.
9. Pages: [schema](../model/schema.md) loses "Traits" and "`switch`"; categories, properties and `default_by` drop trait mentions. [record](../use/reading.md), [read path](../layers/resolution.md), [working record](../use/editing.md), [open questions](../open-questions.md), usage [schema](../../usage/schema.md), `CHANGELOG.md`.
10. On implementation, DEP 1 and DEP 3 take `Superseded-By` this DEP.

Checks: no `trait` in `src/`; a manifest with `traits` is refused naming the replacement; a PyPSA network round-trips.

## Rejected Ideas

- **Keeping traits as an unenforced bundle for dispatch**: a vocabulary no caller asks for.
- **Moving narrowing onto `AttributeSpec` (`carried_by`)**: the same rule under another name, with every branch kept.

## Open Issues

- **What `build` does with a value its type does not take.** Recommend reporting it from `_verify`, as an unknown type is, not raising.
- **Whether a category still needs `partial`.** It guarded per-label narrowing. Recommend keeping it: `default_by` per label needs a dim owned per label.
- **The `committable` case DEP 3 raised.** Recommend leaving it to the consumer reading the `committable` attribute.
- **Owner-map size.** Recommend measuring the PyPSA example before and after; no change unless it regresses.
