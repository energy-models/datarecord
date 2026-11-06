<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 12: properties, metadata declared apart from data

| DEP      | 12                                                                                                           |
| -------- | ------------------------------------------------------------------------------------------------------------ |
| Title    | properties, metadata declared apart from data                                                                |
| Author   | Jonas Hörsch                                                                                                 |
| Status   | Implemented (2026-10-13, steps 1 and 2)                                                                      |
| Requires | [DEP 10](dep-0010-a-group-is-a-struct-column.md), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) |
| Created  | 2026-10-12                                                                                                   |
| Version  | Unreleased                                                                                                   |
| Replaces | [DEP 6](dep-0006-a-member-file-holds-values.md)                                                              |

The design landed in [the data model](../../reference/data-model.md), [the schema](../../reference/schema.md), [the record format](../../reference/format.md), [the `RecordLike` protocol](../../reference/record.md) and [`WorkingRecord`](../../reference/editing.md), which win where they and this DEP differ.

## Abstract

Properties, which classify or describe one label of a dim, get their own section of the manifest, apart from attributes, the data. The entity-type axis becomes an `Enum` property of `entity`, whose values are categories that per-category defaults and traits key on. `entity` becomes a dim like any other, listed in `partial`, and core names neither `entity` nor `entity_type`.

## Motivation

Where a value is read depends on how many coordinates address it, not on what it is. `p_nom` and `carrier` both declare `dims: [entity]`, so both land on the entity axis, and `record.attributes` returns neither. Widening `p_nom` over `scenario` is compatible, yet moves it to `inputs/p_nom.parquet`, away from the rows a layer wrote before.

The entity-type axis is a special case at every level. It is a dim plus a group into it whose rows are the entity axis file, a reserved name, and two entity-axis layouts. `entity` is too: always in the fold key, never broadcast, the only dim whose tombstones cascade, and the key of the read API.

The schema also cannot state a framework's per-type defaults:

| Name       | Default per type                                                     |
| ---------- | -------------------------------------------------------------------- |
| `sign`     | `1` for Generator, Store, StorageUnit, `-1` for Load, ShuntImpedance |
| `carrier`  | `"AC"` for Bus, `""` otherwise                                       |
| `type`     | `"primary_energy"` for GlobalConstraint, `""` otherwise              |
| `p_min_pu` | `-1` for StorageUnit, `0` otherwise                                  |
| `p_set`    | `0` for Load, `NaN` otherwise                                        |

None of PyPSA's 142 input names differs in dtype between types, only in default.

## Specification

- A property is one value per label of its subject dim, declared under `properties.<dim>`, with no `dims`. An attribute is data and keeps `dims`, which may name one coordinate. The author declares which a name is.
- One name has one spec across every section. A name declared as a property and an attribute, or as a property of two subjects, is refused. So is a property named like a dim or a group.
- An `Enum` property of a dim in `partial` categorises its subject, and its values are categories. `Trait.on` and `default_by` may name it, and naming any other property is refused. A dim may have several.
- `Schema.categorizing(dim)` lists them. `attributes_for(dim, **categories)` and `default_for(name, **categories)` answer per label.
- `default_by` maps a categorising property to per-category defaults, on properties and attributes alike. An unset value reads the first key, in declaration order, whose map lists the label's category, otherwise `default`. Each key categorises a dim the value reaches, directly or through a group field. A category outside the `Enum` is refused. Only the default varies.
- `Trait.on` is one categorising property. A trait narrows and does not grant, as [DEP 16](dep-0016-remove-traits.md) states. A switch is addressed by exactly the dim its trait's property categorises, and a trait with a switch must have an `on`.
- A NULL on a categorised dim reaches only the labels that carry the attribute, judged by their categories at read time.
- The fold key is exactly `partial`. Every dim and group broadcasts. Deletion follows [DEP 11](dep-0011-deletion-cascades-through-tombstones.md). A label is unique across types by construction, one axis row per label.
- `Record`, `Resolver`, `Fold` and `LayerData` lose `entity_types`, `entity_type(ctype)` and `entity_axis`, and `flags()` takes no type.
- Every property of a dim is a column of `dims/<dim>.parquet`, NULL where the category does not have it. `dims/entity_type/` goes.
- `WorkingRecord` has `add(dim, labels, **columns)`, `set(attribute, value, **coords)` and `remove(dim, labels)`. `add` takes only columns the axis file has, and an `Enum` value must be a category. `set` takes every coordinate by keyword, a group's fields included. `set` writes a property, a category included, as one column of the axis row. A property keyword selects the matching labels when `set` runs, and nothing of the filter is stored.
- A category may change. Values for attributes the new category does not carry stay stored, outside every per-category read and broadcast.
- Step 2: a property lands in `dims/<dim>.parquet` and every attribute in `inputs/<attr>.parquet`. An axis file has its labels, `within` keys, tombstones and properties only. `record.properties[dim]` returns stored values, `record.attributes["p_nom"]` returns long rows, and `attributes_for` splits into `properties_for` and `attributes_for`. Properties of a group and per-category flags are step 2 too.

## Rationale

`Enum` makes the categories a trait or default lists checkable against a vocabulary. This settles the `String`-typed axis question of [DEP 6](dep-0006-a-member-file-holds-values.md): a set of categories is an `Enum`, or nothing keys on it. The dim must be in `partial`, because only there is it owned and checked per label.

"Category" is used because "label" already means an axis value. A category's frame is a filter and a projection over one file. Per-type files would state each label's category twice, and Parquet stores an all-NULL chunk for almost nothing.

`switch` is not subsumed. A category never varies along another dim, while `p_nom_extendable` per `investment_period` does. Lifting [DEP 3](dep-0003-trait-switches.md)'s restriction to the categorised dim is step 2.

The design costs these:

- No per-type flags, frames or applied defaults before step 2, and PyPSA's `verify` counts an attribute one type owns as owned by every type.
- A schema keying on `entity`'s categories must list `entity` in `partial`, or is refused.
- Adding a carrier to an `Enum` `carrier` is a schema change.
- Entity-only data reads through the fold. A static frame of forty columns is forty attribute reads and a pivot.
- Nothing derives whether a name is metadata or data, and moving it between sections is incompatible.
- A value per entity type alone, such as an `icon`, has no file keyed by category.
- Every manifest is rewritten and every record re-exported.

## Usage

```yaml
properties:
  entity:
    entity_type: { dtype: { Enum: [Bus, Generator, Load, StorageUnit] } }
    sign: { dtype: Float64, default: 1, default_by: { entity_type: { Load: -1 } } }
attributes:
  p_min_pu:
    dtype: Float64
    dims: [entity, snapshot]
    default: 0
    default_by: { entity_type: { StorageUnit: -1 } }
partial: [entity, scenario, connection]
```

```python
w.add("entity", ["g1", "g2"], entity_type="Generator", carrier="wind")
w.set("p_max_pu", 0.9)  # every generator, now and later
w.set("efficiency", 0.7, entity="l1", bus="b0")
w.remove("entity", ["g1"])  # tombstones g1 and its connections
```

## Implementation notes

Steps 1 and 2 landed on `feat/properties-structs`, with two departures:

- `partial` has no rule for groups. A dim a group draws on need not be in `partial`, because its dead labels cover deletion.
- A reader applies `default_by` through `Schema.default_for`, as the PyPSA tool does. `record.properties` and `record.attributes` show stored rows.

The checks: no `entity` or `entity_type` in core outside `tools/`, docstrings and tests. Records with and without categories differ on disk by one column. A `bus_type` category works like `entity_type`, and two categorising properties of one dim combine. `set("p_max_pu", 0.9)` reaches a later generator and no bus. PyPSA round-trips `sign` and `p_min_pu` with nothing written. Widening `p_nom` over `scenario` reads old values in every scenario.

## Open issues

- `members(dim, **categories)`, a per-category read with defaults applied. Deferred until a caller needs it.
- Categories in attribute rows, to be decided. An `inputs/` row would contain a categorising property, so `entity` NULL with `entity_type = Generator` reaches generators added later. It needs a specificity rule within a layer, explicit label over category row over plain NULL. `flags` would count it as broadcast or gain a third state. A group drawing twice on one categorised dim is refused. It is decided by a case where layer data must reach every label of a category, future ones included.
- Whether a functional group over one coordinate, such as `country` over `[bus]`, is a property.
- Whether wide-frame editing returns over `add`, `add_group` and `set`.
- Whether a broadcast survives materialisation. A materialised owner map stores a NULL already expanded, so `set("p_max_pu", 0.9)` misses a generator added in a child. A `WorkingRecord`'s committed prefix behaves the same.
- Whether groups edit like dims, through `add(name, keys, **columns)`.
