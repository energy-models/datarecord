<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 11: deletion cascades through tombstones, not through filters

| DEP           | 11                                                                                                                     |
| ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Title         | deletion cascades through tombstones, not through filters                                                              |
| Author        | Jonas Hörsch                                                                                                           |
| Status        | Implemented (2026-10-13)                                                                                               |
| Requires      | [DEP 10](dep-0010-a-group-is-a-struct-column.md)                                                                       |
| Created       | 2026-10-13                                                                                                             |
| Version       | Unreleased                                                                                                             |
| Superseded-By | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its removal by tombstone, and dead labels as winning tombstones) |

The design landed in [layers](../../reference/layers.md) and [the fold](../../reference/resolution.md), which win where they and this DEP differ.

## Abstract

A label is removed by a tombstone and only by a tombstone. Removing one tombstones every group tuple that contains it, and a layer that leaves such a tuple alive is refused. Nothing is inferred on read from a dead label a row refers to. This replaces, for group tuples, the rule of [DEP 10](dep-0010-a-group-is-a-struct-column.md) that the tuples stand, dangling.

## Motivation

Removing an entity drops the values that name it in four places, each covering part of the problem:

| Where                             | What it drops                                                                            | Keyed on                                                          |
| --------------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `fold_inputs`                     | parent owner-map rows hit by this layer's tombstones on `entity`, groups, `partial` dims | owner-key columns, and entity fields through `entity_coordinates` |
| `_living` (read)                  | value rows naming a removed entity in a coordinate outside the owner key                 | the resolved entity axis, through `entity_coordinates`            |
| `WorkingRecord._collapsed_inputs` | staged rows hit by this layer's own entity and tuple tombstones                          | `_tombstoned` and `entity_coordinates`                            |
| `resolve_groups`                  | only the group's own tombstones                                                          | none                                                              |

After `remove("Generator", ["g1"])`, the value `x(g1, bus0)` is gone, but the tuple `(g1, bus0)` still stands in `record.groups["connection"]`. Every path also goes through `entity_coordinates` and `Schema.entity_fields`, which hard-code one dim, so removing a `bus` label reaches nothing inside a struct.

## Specification

1. Removing a label writes the cascade. `WorkingRecord.remove(ctype, names)` tombstones the entities and every group tuple with a field equal to one, read from the resolved groups. The folded group honours its own tombstones, and `resolve_groups` needs no filter.
2. `write_layer` refuses a dangling tuple. A layer that leaves alive a tuple containing a label dead after the layer, or adds a tuple containing one, is refused. A layer written by hand carries its own cascade.
3. The fold drops keyed values. `fold_inputs` anti-joins each coordinate in the owner key, which is `entity` and the dims and groups in `partial`, against the layer's tombstones. A re-added label starts fresh. The entity anti-join reaches the top-level `entity` only.
4. The read drops unkeyed values against dead labels. For each coordinate outside the owner key, value rows are anti-joined against the labels whose winning row in the fold is a tombstone. For a group, those are its dead tuples. The join is skipped when the set is empty.

`partial` stays the granularity of an override. It is not transitive: a group that draws on a dim in `partial` need not be in it.

A materialised node keeps its dead labels as `dead/<coord>.parquet`, beside `dims/` and `groups/`. It does so for each dim and group outside `partial`, and for each dim a group draws on. Rule 4 reads the base's dead labels plus those of the layers above. The resolved axes and groups stay tombstone-free, and `fold_axis` returns the dead labels as a by-product.

`_living`'s filter, the entity-field anti-joins in `fold_inputs` and `_collapsed_inputs`, and every literal `entity` in the cascade go. `entity_coordinates` and `Schema.entity_fields` stay for `Fold.flags(ctype)` and for `set`'s `_keyword_entities`.

## Rationale

Dead labels must survive materialisation. Suppose layer 1 sets `x(g1, bus0)` with `connection` outside `partial`, layer 2 removes `g1`, and layer 3 is materialised. A read at layer 4 takes `x`'s owner, layer 1, from the cached map. Dead labels from above the base alone do not name `(g1, bus0)`, so the value would come back. The fold cannot cut it earlier, because along an unkeyed coordinate the owner map has one row per attribute, not per label.

Re-adding a label revives its unkeyed values. Suppose layer 3 re-adds `g1` and `(g1, bus0)`. The owner map still lists layer 1 for `x`, so `x(g1, bus0)` returns, as does every unkeyed attribute over the tuple. That is what a coordinate outside `partial` means, and a schema that wants a fresh start declares the group in `partial`.

The design costs three things:

- `write_layer` depends on the ancestry: rule 2 is a fold plus a semi-join per group field.
- Removing a bus removes its connections, and PyPSA's `build` reports the component's `bus` as missing.
- The resolved cache grows by a dead-labels file per dim and group outside `partial`.

## Implementation notes

- Rule 1 is `WorkingRecord.remove`. A dim label other than an entity is removed only by a layer written directly.
- Rule 2 runs in `write_layer` for a layer that writes any axis or group, folding the parent's sources and the staged layer. It checks only the tuples the layer adds and those still containing a label it removes. A tuple an ancestor left dangling blocks no later write.
- Rule 4's anti-join is `_living`, and `dead/` contains the coordinates `resolve.mortal` returns.

## Rejected ideas

- A semi-join against the live labels, which the cache already has. It needs no new file, but it runs on every read, where the anti-join against a usually empty set is skipped.

## Open issues

- Whether a nested dim follows rule 1. `fold_axis` keys a nested dim by `(period, timestep)`, so a parent's tombstone does not reach its children. Removing `period=2030` would tombstone `(2030, t)` for every `t`. Not built, because `WorkingRecord` removes no dim label and rule 2 checks no field over a nested dim.
