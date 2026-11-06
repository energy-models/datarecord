<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 1: dims, mappings, groups and traits

| DEP           | 1                                                                                                                                                                    |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title         | dims, mappings, groups and traits                                                                                                                                    |
| Author        | Jonas Hörsch                                                                                                                                                         |
| Status        | Implemented (2026-08-16)                                                                                                                                             |
| Created       | 2026-08-15                                                                                                                                                           |
| Version       | v0.1.0a1                                                                                                                                                             |
| Superseded-By | [DEP 16](dep-0016-remove-traits.md) (its traits part), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its entity tombstones, and an order the fold generates) |

The design landed in [the data model](../../reference/data-model.md), [the schema](../../reference/schema.md), [the record format](../../reference/format.md) and [the fold](../../reference/resolution.md), which win where they and this DEP differ. [DEP 2](dep-0002-groups.md) absorbs the `on` mapping into groups as `into`.

## Abstract

`AttributeSpec.dims` becomes the one way to address an attribute. The component name becomes a dim, `entity`. Attributes are declared flat, types subscribe through traits, groups replace connections, mappings classify one dim by another, and `Dimension.keys` goes.

## Motivation

An attribute that belongs to no component type has nowhere to live. A snapshot weighting varies by snapshot alone, and `Schema.attributes` is keyed by component type. The PyPSA tool stores it as an undeclared `objective` column on `dims/snapshots.parquet` and recognises it by name.

Such a column has no dtype, default, unit or description, appears in no `flags` and does not fold across layers. Scenario weightings have the same problem. Yet `name` is an axis like `snapshot`, and only looks structural because the schema nests under it.

## Specification

Four mechanisms declare where labels come from. `within` stays a fifth, distinct from a mapping.

| Mechanism | Declares                                            | Lives in                                                       |
| --------- | --------------------------------------------------- | -------------------------------------------------------------- |
| dim       | an axis of labels                                   | `dims/<dim>.parquet`, with any attribute addressed by it alone |
| mapping   | each label of the dim it is `on` has one of its own | its own axis file, plus a column on the classified dim's file  |
| group     | which tuples over several dims exist                | its own table, which subsumes connections                      |
| trait     | which entities an attribute applies to              | a subscription on the component type                           |

- `entity` is a declared dim. `dims/entity.parquet` has the columns `entity | component_type | deleted`, so a label is unique by construction, `entity -> component_type` is a column read, and a deletion has one home.
- `order_key` is a column of the owner map and never of a layer's file, and persists only where a node's maps are materialised. The fold computes it per layer as `max(parent) + row_number() OVER (ORDER BY _row)`, before the union. Nothing sorts eagerly: a reader applies `ORDER BY order_key`. A table that gains rows across layers keeps it, and an ordinary dim does not.
- `dims/components/<Type>.parquet` keeps only the values that vary over nothing, partitioned by type because each type has its own attribute vocabulary.
- `Schema.attributes` is flat: one spec per attribute, one file, one dtype. `AttributeSpec.bus` is deleted.
- An attribute whose `dims` name exactly one addressing coordinate is a column on that thing's table. Any other attribute is long rows in `inputs/<attr>.parquet`. `varying` means "has dims beyond its address". `role` becomes `AttributeSpec(dtype="VARCHAR", dims={"connection"})`, a column of the group's table.
- A trait is a named set of attributes. `Schema.attributes_for(ctype)` returns the union of a type's traits and its own attributes. Two traits that declare one attribute with different specs fail validation.
- A group's `over` maps coordinate name to dim. An attribute over a group has the group's coordinate names as columns and never the group's name, so two groups may share a coordinate set. A group in `dims` expands to its coordinates, and an attribute not addressed by a coordinate writes NULL there.
- `bus` is no longer structural. The long schema's fixed prefix is `entity | attribute | breakpoint | value`. `entity` is the one dim the format knows by name, because the component types partition it.
- A NULL group coordinate broadcasts over the group's rows, not the product of its dims.
- The protocol's `connections` becomes `groups`, keyed by group name. `dims` includes `entity` and every mapping. `shape()` joins `flags(ctype)`, keyed by attribute and record-wide, with `dims`, `broadcast`, `breakpoints` and `types`.
- A mapping is a `Dimension` with `on`, and is a dim. It has its own axis file, is not in `partial`, never scopes existence, and is not denormalised down a chain. Dims and mappings share one namespace, so a name declared twice fails validation. A many-to-many classification is a group. The record does not project an attribute across levels of a mapping.
- `Dimension.keys` is deleted and nothing replaces it. An entity or a group row exists or does not, and neither varies along another dim.

So `{"entity"}` is a column of `dims/components/<Type>.parquet`, `{"scenario"}` a column of `dims/scenario.parquet`, and `{"entity", "snapshot"}` long rows.

## Rationale

Nesting attributes under a type lets two types declare one attribute with different dtypes over one file and one `value` column. Nothing rejects that, and the read is silently wrong. Flat declaration makes the case unrepresentable.

A trait earns its place because the shared sets are measured from PyPSA's registry, and lpspec dispatches an equation on a trait. Traits are authored, which costs upkeep.

`over` maps names to dims because a corridor between two entities needs two columns, `from` and `to`, from one dim. The fold's key stays one tuple, the union of plain dims and coordinate names.

`within` does not subsume `bus`, because groups do it. The recorded blocker was that `bus` inverts the NULL rule. Expansion already depends on whether a dim is in `dims ∩ partial`, so `bus` is not an exception.

The fold assigns `order_key` before the union, because `row_number()` over what `UNION ALL` emits has no defined order. A group keeps it because a framework numbers positional ports by it, and a reshuffle would move a connection's attributes to another port.

A mapping has its own axis file so that it has an order and a place for data such as `co2_budget`. A denormalised chain would let a layer that restates `dims/state.parquet` leave every bus's `country` stale. A generator's country follows from its bus, so a mapping cannot scope existence.

`within` scopes a dim's labels per parent, so the axis key is `(period, timestep)`. `on` classifies another dim with one flat set of labels. Neither can express the other's case.

`keys` cannot survive groups, because `KeyKind` lists a `"connection"` table that groups delete. A replacement was drafted and removed, because its meaning is unsettled. Dropping a mechanism whose meaning is unresolved is better than carrying it into a redesign under a new name.

Flattening makes three vocabulary questions visible that the storage already asked:

- One concept has one name. PyPSA's `p_nom`, `s_nom` and `e_nom` become `capacity`, so `investable` can contain the capacity itself. The record's vocabulary is not PyPSA's.
- The tool's schema renames on the way in and out.
- `Bus.x` and `Line.x` already collide in one `inputs/x.parquet`, and now fail at declaration. A tool prefixes, renames or refuses in `verify`.
- `Link.efficiency2` collapses to a stem over the `connection` group, and `bus0` and `bus1` become rows of the group.

Every step is incompatible: `version` bumps once, a record is rewritten, and scenario-scoped membership becomes unrepresentable.

## Usage

```python
attributes = {
    "p_max_pu": AttributeSpec(dtype="DOUBLE", dims={"entity", "snapshot", "scenario"}, default=1.0),
    "efficiency": AttributeSpec(dtype="DOUBLE", dims={"connection", "snapshot"}, default=1.0),
    "objective_weighting": AttributeSpec(dtype="DOUBLE", dims={"snapshot"}, default=1.0),
}
groups = {
    "connection": Group(over={"entity": "entity", "bus": "bus"}),
    "corridor": Group(over={"from": "entity", "to": "entity"}),
}
```

`objective_weighting` lands on `dims/snapshot.parquet`, declared and typed.

## Implementation notes

1. Flat attributes and traits. Only the manifest changes.
2. Singular parquet names, `dims/<dim>.parquet`, which fixes `dims/buss.parquet`.
3. Mappings, which the fold treats as dims.
4. `entity` as a dim, with no order column. The long schema's columns become schema-derived.
5. Groups, with the fold's key schema-derived, and `keys` deleted in the same step with `KeyKind`, `_keyed`, `component_dims`, `connection_dims` and the tests of scenario-scoped existence. This is the one functional regression, and should not land before the existence question is answered.
6. The protocol: `connections` becomes `groups`, and `shape()` joins `flags`.
7. `WorkingRecord` reaches attributes with no `entity` and groups of two coordinates.

Two breaks do not announce themselves. At step 4, `add` routes by `varying`, which turns true for `dims={"entity"}`. And `_overlay` would put `entity` in `broadcast`, so a staged row with a NULL entity wipes every entity's value. The fix derives the key's address coordinates instead of spelling them, and a group coordinate belongs in the key.

The PyPSA tool's trait table and its `p_nom -> capacity` renames must exist before step 1 ships against a real network.

On implementation, `shape()` was not built, so `flags(ctype)` stands alone. `set` has no `bus=` parameter: every coordinate but `entity` goes through `**dims`, so a group of two coordinates can be addressed.

## Rejected ideas

- A separate payload declaration on `Group` or `Dimension`: two ways to say one thing.
- Trait roles: one more indirection for what the tool's schema already answers.
- Qualified names or per-type namespaces: both put the component type back into an attribute's address.
- Widening `flags` to record-level attributes: `flags` is per type by construction.
- A field declaring per-dim existence: drafted, then removed, because its meaning is unsettled.
- Last-wins for conflicting traits: a conflict fails validation instead.

## Open issues

- Whether `flags(ctype)` survives, or becomes `shape()` filtered by `types`.
- Outputs are out of scope.
- Whether existence may depend on a dim, as a generator built in `high` and absent in `low`. If `entity` varies over `scenario` and `connection` does not, one connection row cannot be both kept and dropped. The fold drops it, pinned by an xfail. A validator could forbid a group whose existence dims are coarser than those of `entity`, which is a decision and not a default.

Three questions follow from the last, and a "no" to the first deletes the whole area:

1. Is per-dim existence needed, or does an `active`-style attribute cover it? PyPSA has both.
2. If needed, may tables disagree, or is uniformity enforced?
3. If they may disagree, what does the coarser table's row mean, and can the fold compute it?
