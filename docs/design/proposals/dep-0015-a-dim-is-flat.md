<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 15: a dim is flat

| DEP      | 15                                                |
| -------- | ------------------------------------------------- |
| Title    | a dim is flat                                     |
| Author   | Jonas Hoersch                                     |
| Status   | Draft 2026-10-19                                  |
| Requires | [DEP 13](dep-0013-buses-and-carriers-are-dims.md) |
| Created  | 2026-10-19                                        |
| Version  | Unreleased                                        |

## Abstract

`Dimension.within` leaves core: every dim is one label set, keyed by its label alone. A label that today names a point only inside a parent becomes a label unique across its dim plus a property **`into`** the parent — `snapshot.period` into `period` — the reference [DEP 13](dep-0013-buses-and-carriers-are-dims.md) added. A chain is a chain of such properties; several parents are several.

## Motivation

A multi-period PyPSA network declares `snapshot` [`within`](../schema.md#within-an-axis-inside-an-axis) `period`, and every place that touches the snapshot axis pays for it:

- **Every series carries its period.** Every input and result over `snapshot` is over `period` too, so `p_max_pu` is keyed by `(entity, period, snapshot)`, and a NULL `period` is a second way of reaching a snapshot ([tools](../tools.md)).
- **Checks skip a nested dim.** A property `into` one, or a group field over one, is not checked by [`remove`](../working-record.md#add-remove) or [the writer](../layers.md#deletion); `remove` does not cascade to its tuples; the trait check passes over a nested `partial` dim; `set("dim.name", mapping)` refuses one. The page lists the first as an [open question](../open-questions.md).
- **Known bugs.** An attribute over a nested dim is accepted without its parent, so its rows name no point. A pointwise `set` with a NULL `period` drops the rest of the series.
- **The consumer has no nested dims.** math-spec, the model-spec language datarecord feeds, gives every dimension one list of members. A multi-period snapshot there is one flat dim with a `snapshot → period` relation, and its guidance reads: two dimensions that depend on each other, declared as a product cut back by a mask, are one relation. A tool feeding it flattens the `(period, snapshot)` key anyway.

## Specification

### One key column

`Dimension` is `dtype`, `unit` and `description`. A dim's key is its label: `dims/<dim>.parquet` is keyed by the one column `<dim>`, `record.dims[dim]` is that column, and its labels are unique across the dim. A manifest declaring `within` is refused at load, the error naming the property `into` the parent that replaces it.

**A label scoped per parent is a flat label and a property `into` the parent.** The source picks the labels; they need only be unique across the dim. What the source called the point inside its parent, if it matters, is a further property.

| declared today                        | declared under this DEP                                              |
| ------------------------------------- | -------------------------------------------------------------------- |
| `timestep` `within` `period`          | `snapshot`; `snapshot.period` `into` `period`; `snapshot.timestep`   |
| `timestep` in `period` in `horizon`   | `snapshot.period` `into` `period`; `period.horizon` `into` `horizon` |
| `timestep` `within` `{period, stage}` | `snapshot.period` `into` `period`; `snapshot.stage` `into` `stage`   |

**A chain is not denormalised**, [as for any classification](../schema.md#within-is-not-into): `snapshot` carries no `horizon` column, the horizon being a join through `period`.

**Selecting by a parent is a selector keyword.** One is bare and matches any property of a dim the attribute reaches ([`set`](../working-record.md#set)), so `period=2030` on `p_max_pu` picks the snapshots whose `snapshot.period` is 2030, and `timestep=t0` the snapshot at `t0` in every period. Two reached dims both declaring `period` stay refused.

### What loses its special case

| where                                                             | today, for a nested dim                                         | under this DEP                 |
| ----------------------------------------------------------------- | --------------------------------------------------------------- | ------------------------------ |
| axis key, `record.dims[dim]`                                      | `(*parents, dim)`, `Schema.axis_key`                            | `dim`                          |
| [axis fold](../read-path.md#one-fold-for-every-axis), dead labels | keyed by the parents too                                        | by the label                   |
| [broadcast](../record.md#the-broadcast-rule)                      | a series carries its parent; a NULL parent reaches every period | a NULL dim reaches every label |
| `add(dim, …)`                                                     | parent keys required                                            | properties only                |
| `set("dim.name", mapping)`                                        | refused                                                         | as on any dim                  |
| `remove`, the writer                                              | no `into` check, no group cascade, no field check               | every dim                      |
| label and trait check on `set`                                    | skipped for a `partial` dim                                     | every `partial` dim            |
| property names                                                    | none named like a `within` parent                               | `snapshot.period` allowed      |
| [versioning](../schema.md#versioning)                             | changing `within` incompatible                                  | entry goes                     |

### The PyPSA tool

- A single-period network's schema is unchanged.
- A multi-period network's `snapshot` is flat: its label is PyPSA's timestep where the timesteps are unique across periods, an integer position otherwise. `snapshot.period` is `into` `period`; `snapshot.timestep` is declared only beside positions, a timestep label holding it already.
- Inputs and results over `snapshot` are no longer over `period`; `period_weighting_objective` and `_years` stay over `period`.
- `build` rebuilds the snapshot `MultiIndex` in axis order from `snapshot.period` and the label or `snapshot.timestep`.

### Pages that change

- [schema](../schema.md) → `Dimension` without `within`; [dimensions](../schema.md#dimensions) "its type"; [`within`](../schema.md#within-an-axis-inside-an-axis) replaced by a short "a dim is flat" with the table above; [`within` is not `into`](../schema.md#within-is-not-into)'s chain and cross-level paragraphs move to [properties](../schema.md#properties); [properties](../schema.md#properties) loses the `within`-parent refusal; [versioning](../schema.md#versioning) loses its entry.
- [record](../record.md) → `dims[dim]` and the axis row without `within` keys.
- [format](../format.md#where-a-value-lives) → an axis file carries its key, tombstones and properties.
- [layers](../layers.md#deletion) and [working record](../working-record.md#add-remove) → the nested-dim exemptions; [validation](../working-record.md#validation) loses the missing parent key.
- [tools](../tools.md) → the multi-period paragraph as above.
- [open questions](../open-questions.md) → the `into`-over-a-nested-dim entry goes; "whether `within` should subsume `bus`" goes.
- usage [schema](../../usage/schema.md), [reading](../../usage/reading.md), [editing](../../usage/editing.md), `CHANGELOG.md`.

## Rationale

**The scoping belongs to the source's labels, not to the axis.** `t1` names nothing alone because PyPSA reuses it per period, and a source that numbers its snapshots once has no nesting to declare. Moving the scoping into a property states the same relation in the one form the record already checks, folds and selects on, and a consumer reads `snapshot.period` exactly as it reads `bus.carrier`.

**One key column is one rule.** The axis key, the fold key and the broadcast rule each had a branch for `(*parents, dim)`, and every check that could not place a label by one column skipped it rather than branch. Removing the shape removes the skips: the bugs above are not fixed, they become unrepresentable — no attribute can be over a nested dim without its parent when no dim has a parent, and no series carries a parent to leave NULL.

**`into` can now express `timestep`.** The [distinction](../schema.md#within-is-not-into) held because `within` scoped a label set and `into` could not. A flat label set makes the snapshot a classification like any other: each snapshot picks one period.

**The NULL-parent `set` bug is narrower than `within`.** It is a fold bug with any two whole-owned dims, which `within` made routine by putting a second whole-owned dim on every series. It goes from the snapshot and stays for other schemas.

**Costs.** A source whose inner labels repeat across parents must synthesise unique ones. Every record declaring `within` stops reading and is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). "The same timestep in every period" is no longer one row with a NULL `period` but a selector over `snapshot.timestep`, which stages one row per matching snapshot. A consumer wanting `(period, timestep)` per row joins the snapshot's properties.

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
w.remove("period", [2040])  # refused while a snapshot.period names it
record.dims["snapshot"]  # snapshot
record.properties["snapshot"]  # snapshot | period | timestep
```

## Implementation notes

1. Schema: `Dimension.within`, its validation, `_ancestors` and `axis_key` go, callers keying by the dim; a `within` in a manifest refused naming `into`; the property-name and versioning rules lose `within`.
2. Read: the axis fold, dead labels and `dims`/`properties` frames keyed by the label.
3. Writer: an axis file's columns are the dim and its properties; every `into` property and group field checked against live labels.
4. `WorkingRecord`: `add` without parent keys, `set` on any dim's property, `remove`'s cascade and `into` refusal, the trait check and staging keys over every dim.
5. Tool: flat `snapshot`, `snapshot.period`, `snapshot.timestep`; series over `snapshot` alone; `build` from the properties.
6. Pages, usage, changelog, tests; DEP 12's and 13's links to the removed `within` anchors retargeted, which fixes a link and revises no argument.

Checks: no `within` or `axis_key` in `src/`; a multi-period network with repeating timesteps and one with unique ones round-trip; `remove("period", …)` is refused while a snapshot names it, by `remove` and by the writer; a manifest declaring `within` is refused naming `into`.

## Rejected Ideas

- **A group over `(period, timestep)` as the snapshot**: a masked product, which math-spec's guidance rejects, and a struct-valued key in every time series.
- **Keeping `within` beside the property**: two spellings of one relation, and every special case above stays for the one that keeps it.
- **Composite string labels as a rule** (`"2030|t1"`): the source picks its labels, and a rule would have the record parse them.

## Open Issues

- Whether `Schema` should offer a helper that declares a dim with its parent property in one call.
- Whether the selector needs a qualified form when two reached dims declare `period`.
- The NULL-parent pointwise `set` bug, for schemas with two whole-owned dims.
