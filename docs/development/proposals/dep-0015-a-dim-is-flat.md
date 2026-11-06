<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 15: a dim is flat

| DEP      | 15                                                |
| -------- | ------------------------------------------------- |
| Title    | a dim is flat                                     |
| Author   | Jonas Hoersch                                     |
| Status   | Implemented (2026-10-20)                          |
| Requires | [DEP 13](dep-0013-buses-and-carriers-are-dims.md) |
| Created  | 2026-10-19                                        |
| Version  | Unreleased                                        |

The design landed in [the schema](../../reference/schema.md), [the data model](../../reference/data-model.md), [the fold](../../reference/resolution.md) and [the tools reference](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

`Dimension.within` leaves core, so every dim is one label set, keyed by its label alone. A label scoped to a parent becomes a label unique across its dim, plus a property `into` the parent, such as `snapshot.period`. [DEP 13](dep-0013-buses-and-carriers-are-dims.md) added this reference. A chain is a chain of such properties, and several parents are several properties.

## Motivation

A multi-period PyPSA network declares `snapshot` `within` `period`, and every place that touches the snapshot axis has to handle it:

- Every series carries its period. Every input and result over `snapshot` is over `period` too, so `p_max_pu` is keyed by `(entity, period, snapshot)`. A NULL `period` is then a second way of addressing a snapshot.
- Checks skip a nested dim. `remove` and the writer do not check a property `into` one, or a group field over one, and `remove` does not cascade to its tuples. The trait check passes over a nested `partial` dim, and `set("dim.name", mapping)` refuses one.
- Two bugs follow. datarecord accepts an attribute over a nested dim without its parent, so its rows match no point. A pointwise `set` with a NULL `period` drops the rest of the series.
- The framework has no nested dims. In math-spec, the model-spec language datarecord feeds, every dimension has one list of labels. A multi-period snapshot there is one flat dim with a `snapshot → period` relation. Its guidance states that two dependent dimensions, declared as a product cut back by a mask, are one relation. A tool that feeds it flattens the `(period, snapshot)` key anyway.

## Specification

### One key column

`Dimension` has `dtype`, `unit` and `description`. A dim's key is its label: `dims/<dim>.parquet` is keyed by the one column `<dim>`, and `record.dims[dim]` is that column. Its labels are unique across the dim. datarecord refuses a manifest that declares `within` at load, and the error names the property `into` the parent that replaces it.

A label scoped per parent becomes a flat label and a property `into` the parent. The source picks the labels, which need only be unique across the dim. Where the point's name inside its parent matters, it is a further property.

| Declared before                       | Declared under this DEP                                              |
| ------------------------------------- | -------------------------------------------------------------------- |
| `timestep` `within` `period`          | `snapshot`, `snapshot.period` `into` `period`, `snapshot.timestep`   |
| `timestep` in `period` in `horizon`   | `snapshot.period` `into` `period`, `period.horizon` `into` `horizon` |
| `timestep` `within` `{period, stage}` | `snapshot.period` `into` `period`, `snapshot.stage` `into` `stage`   |

A chain is not denormalised, as for any classification. `snapshot` has no `horizon` column, and the horizon is a join through `period`.

A selector keyword selects by a parent. The keyword is bare and matches any property of a dim that the attribute's coordinates cover. `period=2030` on `p_max_pu` selects the snapshots whose `snapshot.period` is 2030, and `timestep=t0` the snapshot at `t0` in every period. datarecord still refuses a keyword when two such dims both declare `period`.

### What loses its special case

| Where                          | Before, for a nested dim                                      | Under this DEP               |
| ------------------------------ | ------------------------------------------------------------- | ---------------------------- |
| axis key, `record.dims[dim]`   | `(*parents, dim)`, `Schema.axis_key`                          | `dim`                        |
| axis fold, dead labels         | keyed by the parents too                                      | by the label                 |
| broadcast                      | a series has its parent, and a NULL parent means every period | a NULL dim means every label |
| `add(dim, …)`                  | parent keys required                                          | properties only              |
| `set("dim.name", mapping)`     | refused                                                       | as on any dim                |
| `remove`, the writer           | no `into` check, no group cascade, no field check             | every dim                    |
| label and trait check on `set` | skipped for a `partial` dim                                   | every `partial` dim          |
| property names                 | none named like a `within` parent                             | `snapshot.period` allowed    |
| versioning                     | changing `within` incompatible                                | the entry goes               |

### The PyPSA tool

- A single-period network's schema is unchanged.
- A multi-period network's `snapshot` is flat. Its label is PyPSA's timestep where the timesteps are unique across periods, and an integer position otherwise.
- `snapshot.period` is `into` `period`. The tool declares `snapshot.timestep` only beside positions, because a timestep label already contains it.
- Inputs and results over `snapshot` are no longer over `period`. `period_weighting_objective` and `_years` stay over `period`.
- `build` rebuilds the snapshot `MultiIndex` in axis order from `snapshot.period` and the label or `snapshot.timestep`.

## Rationale

The scoping comes from how the source picks its labels, not from the axis. `t1` identifies no snapshot alone because PyPSA reuses it per period, and a source that numbers its snapshots once has no nesting to declare. A property records the same relation in the one form that datarecord already checks, folds and selects on. A reader then reads `snapshot.period` as it reads `bus.carrier`.

With one key column, datarecord needs one rule. The axis key, the fold key and the broadcast rule each had a branch for `(*parents, dim)`. Every check that could not place a label by one column skipped it. Removing the shape removes the skips. The bugs above become unrepresentable. When no dim has a parent, no attribute omits one and no series leaves one NULL.

`into` can now express `timestep`. The distinction between `within` and `into` held because `within` scoped a label set and `into` could not. With a flat label set, the snapshot is a classification like any other, and each snapshot has one period.

The NULL-parent `set` bug is narrower than `within`. It is a fold bug with any two dims outside `partial`, which `within` made routine by putting a second such dim on every series. The bug goes from the snapshot and stays for other schemas.

The costs: a source whose inner labels repeat across parents must make unique ones. datarecord can no longer read a record that declares `within`, so each is re-exported, as [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). "The same timestep in every period" is no longer one row with a NULL `period`. It is a selector over `snapshot.timestep`, which stages one row per matching snapshot. To get `(period, timestep)` per row, a reader joins the snapshot's properties.

## Usage

```python
schema = Schema(
    dimensions={
        "entity": Dimension(dtype=nw.String()),
        "period": Dimension(dtype=nw.Int64()),
        "snapshot": Dimension(dtype=nw.Int64()),
    },
    properties={
        "snapshot": {
            "period": PropertySpec(into="period"),
            "timestep": PropertySpec(dtype=nw.Datetime("us")),
        }
    },
    attributes={
        "p_max_pu": AttributeSpec(dtype=nw.Float64(), dims={"entity", "snapshot"})
    },
)
```

```python
w.add("period", [2030, 2040])
w.add(
    "snapshot", [0, 1, 2, 3], period=[2030, 2030, 2040, 2040], timestep=[t0, t1, t0, t1]
)
w.set("p_max_pu", 0.8, entity="g1", period=2030)  # snapshots 0 and 1
w.set("p_max_pu", 0.5, entity="g1", timestep=t0)  # snapshots 0 and 2
w.set("snapshot.period", {3: 2030})  # was refused on a nested dim
w.remove("period", [2040])  # refused while a snapshot.period is 2040
```

## Implementation notes

1. Schema: `Dimension.within`, its validation, `_ancestors` and `axis_key` go, and callers key by the dim. A `within` in a manifest is refused with `into` named. The property-name and versioning rules lose `within`.
2. Read: the axis fold, dead labels and the `dims` and `properties` frames are keyed by the label.
3. Writer: an axis file's columns are the dim and its properties. Every `into` property and group field is checked against live labels.
4. `WorkingRecord`: `add` without parent keys, `set` on any dim's property, and `remove`'s cascade, `into` refusal, trait check and staging keys over every dim.
5. Tool: a flat `snapshot` with `snapshot.period` and `snapshot.timestep`, series over `snapshot` alone, and `build` from the properties.
6. Pages, usage pages, changelog and tests. The links of DEP 12 and DEP 13 to the removed `within` anchors are retargeted, which revises no argument.

Checks: no `within` or `axis_key` in `src/`. A multi-period network with repeating timesteps and one with unique ones round-trip. `remove` and the writer both refuse `remove("period", …)` while a snapshot's `snapshot.period` is the label. A manifest that declares `within` is refused with `into` named.

## Rejected ideas

- A group over `(period, timestep)` as the snapshot: a masked product, which math-spec's guidance rejects, and a struct-valued key in every time series.
- `within` kept beside the property: two spellings of one relation, and every special case above stays for the one that keeps it.
- Composite string labels as a rule (`"2030|t1"`): the source picks its labels, and a rule would make datarecord parse them.

## Open issues

- Whether `Schema` should offer a helper that declares a dim with its parent property in one call.
- Whether the selector needs a qualified form when two dims that the attribute's coordinates cover declare `period`.
- The NULL-parent pointwise `set` bug, for schemas with two dims outside `partial`.
