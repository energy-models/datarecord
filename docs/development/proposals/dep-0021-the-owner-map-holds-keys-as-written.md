<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 21: the owner map contains keys as written

| DEP      | 21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | the owner map contains keys as written                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Status   | Implemented (2026-10-25)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Requires | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Created  | 2026-10-21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (flags aggregated from expanded rows), [DEP 8](dep-0008-one-interface-source-and-fold.md) (a fold that expands a broadcast against the resolved axes), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (a NULL struct expanded in the fold), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (a re-added label starts fresh), [DEP 16](dep-0016-remove-traits.md) (a NULL row expanded over every label in the owner map), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (a keyed label's broadcasts staying gone after it is added again) |

The design landed in [the fold](../../reference/resolution.md), [the data model](../../reference/data-model.md) and [the record](../../reference/record.md). Where they and this DEP differ, follow those pages.

## Abstract

The owner map contains each layer's attribute keys as the layer wrote them. Where a deeper layer's row crosses a parent row, the fold adds an overlap row that the owner map assigns to the deeper layer. The fold reads no axis, and a read returns the rows the owner map assigns as written, NULL included. A reader settles each coordinate by the [broadcast rule](../../reference/data-model.md) alone.

## Motivation

`fold_attributes` expands each row that leaves a `partial` coordinate NULL against the resolved axis, one owner-map row per label. One broadcast over 60,000 entities and two scenarios takes 120,000 rows. A materialised revision persists that expansion, and a fold seeded from it misses every label added later.

A child that adds the entity `n` under a broadcast `q = 0.9` reads `n` as 0.9, and as nothing once its parent is materialised. A label staged in a `WorkingRecord` over the parent reads as nothing in both cases. Over 210 random six-layer records, materialising or staging changes an answer in 95 to 128 records.

The expansion only settles which layer the owner map assigns a key to. Within a layer, datarecord keeps the more specific row, and across layers a read returns the deeper layer's row. The rules disagree only where rows of two layers cross, and one row at their overlap makes them agree.

## Specification

### The owner map

The owner map has one row per layer and distinct key, NULL kept, and no depth:

```text
<partial dims>   -- the key: exactly the partial dims and groups, NULL included
attribute
layer_uuid
varies      STRUCT(<coordinate>: BOOLEAN, ...)
broadcast   STRUCT(<coordinate>: BOOLEAN, ...)
breakpoints BOOLEAN
```

An overlap row sets labels where its layer's key is NULL, and has that layer's `varies` and `broadcast`. A read joins it to the layer's value row by one NULL-safe join on the key.

### Crossing

A row sets a `partial` dim where its key has a label, and any other dim it varies along, so a series over `snapshot` sets `snapshot`. Only the dims the attribute uses count: its `dims` and the dims of its groups. Two rows cover a common key where, on each `partial` coordinate, they have the same label or one has NULL. They cross where they cover a common key and each row leaves NULL a dim that the other row sets.

### The fold

The fold folds a layer onto its parent's map in four steps:

1. It drops the parent rows that contain a label the layer removes by [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)'s diff, and never a row that is NULL there.
2. It drops a parent row that a layer row covers, where the layer row sets no `partial` dim that the parent row leaves NULL.
3. For each parent row and layer row that cross, it adds an overlap row that sets every label either row sets. A crossed parent row, or one that sets fewer dims, stays.
4. It adds the layer's rows.

No step reads an axis. An overlap row is an ordinary row to every later fold, which drops, crosses or removes it like any other.

With `partial = {entity, scenario}`, a root writing `(gas, NULL) = 50` and `(wind, NULL) = 100`, and a child writing `(NULL, high) = 999`:

```text
(gas,  NULL) root    # as written
(wind, NULL) root
(NULL, high) child   # as written
(gas,  high) child   # overlap rows
(wind, high) child
```

A child writing `(gas, high) = 70` instead leaves the root's rows, and the broadcast rule gives it `(gas, high)`.

### A dim outside `partial`

The key has no column for a dim outside `partial`, so a `WorkingRecord` settles a crossing there as it stages. A staged row that leaves such a dim NULL is also written out along a base row's labels there. A child writing `(wind, NULL) = 0.9` over a root series at snapshots 0 and 1 also writes `(wind, 0)` and `(wind, 1)`. The fold refuses a layer written otherwise that leaves such a crossing, naming the attribute, the dim and the key.

### The read

A read returns the rows the owner map assigns as written: a NULL stays NULL, and an overlap row comes back with its labels. It expands no NULL and drops no row by depth. The read still drops each row with a dead label, and the row order stays unspecified. A reader applies the broadcast rule over the full coordinates, `partial` or not. Every crossing has a row that sets both, so a reader needs neither depth nor the list of `partial` dims.

### Where a NULL and a row apply

A NULL on a `partial` coordinate applies to every label of that axis, or every tuple of its group, at the revision read. That includes a label added below a materialised revision, a staged label and the first label of an axis that was empty when the broadcast was written. Adding the scenario `mid` below the first example's child reads `(gas, mid)` as 50.

A row applies at every coordinate that no more specific row covers, from any layer. The owner map assigns a deeper layer only the coordinates its rows apply at, not a key's whole extent along a dim outside `partial`. With `x` also over `snapshot`, a root writing `(gas, NULL, 1) = 1.0` and a child writing `(NULL, high, 0) = 2.0`, `(gas, high, 1)` reads 1.0 from the root, where it read nothing before.

A label removed and added again takes every broadcast that covers it, written before or after the removal, and never a row that set it below the removal. [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) changes to match: a keyed label's values from rows that set it below its removal stay gone.

Materialising changes no answer. A map persisted for another set of `partial` dims is ignored, and the read folds again.

### `flags`

`flags()` aggregates `bool_or` over the owner map's `varies`, `broadcast` and `breakpoints`, per attribute. Both structs report only the dims the attribute uses, so `ctype` no longer reads true in `broadcast` for an attribute not over it. A broadcast that deeper layers shadow on every label still counts. `flags(**categories)` aggregates the same way over the rows that apply to the category.

### Files

The workspace keeps the frozen prefix's map as the shared table `owner_map_<key>_<rev>`, `<key>` a hash of the key columns. A materialised revision writes `resolved/owner_map.parquet` last. A map an earlier release wrote is unsupported, and datarecord does not detect it.

## Rationale

The fold settles ownership once, so the map needs no depth and the read drops no row by depth. Settling it by depth at each read costs more per read, in every way measured, and needs a reader that handles layers.

An overlap row takes its labels from the two rows, so the crossed parent row still applies, unchanged, to a later label. It takes its value from the deeper layer, whose value rows have the dims the overlap row's key leaves NULL. Any parent row more specific than the overlap, where the deeper row applies, is one that the deeper row covers, and step 2 has dropped it. A parent row that a deeper, more specific row covers stays as written, because there the two rules agree.

Crossing is judged over every dim the attribute uses, because a reader applies the broadcast rule over the full coordinates. Left as written, the root's `(NULL, 0)` beside the child's `(wind, NULL)` gives two rows at `(wind, 0)`, and datarecord keeps neither.

The figures come from a prototype against DEP 20, each the median of three interleaved rounds run alone. Case 1 has 60,000 entities, 100 attributes and two scenarios over ten layers, with a materialised leaf and a child that adds one entity. Case 2 is a materialised child that broadcasts 20 of 40 attributes. The scaling cases vary the number `k` of `partial` dims: `entity` with 20,000 labels and `k - 1` dims of three labels, every attribute over all of them. The realistic case (R) has nine layers and ten attributes, where the rows of each attribute set at most four different sets of dims. The worst case (W) has `2^k` layers, each setting its own set of dims, so most layers cross the ones below. The series case (S) has 20,000 entities, two scenarios and 8,760 snapshots.

| Case | Map rows, before | Map rows | First read, before | First read | Repeated read, before | Repeated read |
| ---- | ---------------- | -------- | ------------------ | ---------- | --------------------- | ------------- |
| 1    | 12.0 M           | 7.50 M   | 2.60 s             | 2.98 s     | 0.036 s               | 0.025 s       |
| 2    | 4.80 M           | 1.20 M   | 0.77 s             | 0.24 s     | 0.018 s               | 0.007 s       |
| R k3 | 1.80 M           | 223 k    | 0.70 s             | 0.69 s     | 0.019 s               | 0.013 s       |
| R k4 | 5.40 M           | 223 k    | 1.85 s             | 0.75 s     | 0.049 s               | 0.015 s       |
| R k5 | 16.2 M           | 223 k    | 6.25 s             | 0.81 s     | 0.147 s               | 0.017 s       |
| W k3 | 720 k            | 8.8 k    | 0.70 s             | 0.56 s     | 0.028 s               | 0.020 s       |
| W k4 | 2.16 M           | 35 k     | 2.30 s             | 3.02 s     | 0.107 s               | 0.049 s       |
| W k5 | 6.48 M           | 163 k    | 13.7 s             | 20.1 s     | 0.315 s               | 0.160 s       |
| S k2 | 120 k            | 124      | materialise 25.7 s | 0.22 s     |                       |               |

The first read folds, and materialising follows it. The read times leave out the reader's expansion. A read with two entities set in a staged layer over the materialised revision takes 1.0 to 2.8 times as long as before in the scaling cases, and 0.11 times in the series case.

The worst case's first read is 1.3 to 1.5 times as long for `k` of 4 and 5. Pairs of sets where neither contains the other grow from 1 at `k = 2` to 55 at `k = 4` and 285 at `k = 5`, and the fold joins along each pair. This DEP accepts that cost, because no workflow overrides one attribute along ever different sets of dims in a dozen layers.

Over 210 fuzzed records of six edit layers, with `entity`, `scenario` and `connection` in `partial`, reads and `flags` agree plain, materialised at depth 1 and 3, staged, and staged over a materialised parent. The plain read equals an oracle of the broadcast rule on 197. The other 13 hit the oracle's limits, such as a group emptied of tuples, a connection removed and added again within one layer, or a label new in the layer that writes a row over `snapshot` at it. No fuzzed record triggers the refusal of a layer written without `WorkingRecord`.

## Implementation notes

1. The fold without axes, the read of rows as written, and staging along base labels outside `partial`.
2. The tools and `write_layer` apply the broadcast rule over rows as written.
3. Tests, `CHANGELOG.md` and the pages. The open question on broadcasts surviving materialisation closes.

Checks: every probe reads the same materialised or not, staged or committed, and the 210 fuzzed records read the same in all five ways.

## Rejected ideas

- Keys with their depth, settled at every read: 1.55 to 2.1 times as long in the realistic case from `k = 2`, 31 times in the worst case at `k = 5`, and out of memory at 24.6 GiB for `k = 6`. A session cache of settled rows brings repeated reads to 0.15 to 0.90 times, but a first sweep takes 2.5 to 3.6 times, and 31 times at worst. An `arg_max(depth)` window brings the worst repeated read from 31 to 5.5 times, but the realistic case to 3.3 times. Building the cache root to leaf halves the worst first read but slows the realistic one by half. Disabling DuckDB's `build_side_probe_side` optimizer takes a build to 3.9 s against 9.3 s, but holds for every concurrent cursor. A cache shared across sessions needs a key over the `partial` dims and a size cap, and has a per-label file per attribute.
- Splitting a crossed parent row along the axis: none of the pieces applies to a label added later, so `(gas, mid)` reads as nothing. On the first 70 fuzzed records, 8 differ from the oracle. The worst first read takes 65 s against 20 s, with 2.01 million map rows against 163,000.
- Splitting every parent row along a dim that a deeper row sets: it splits where the two rules agree, and dropping shadowed rows at the read by an `arg_max` window reads at 2 to 8 times the split above.
- Crossing judged over `partial` alone: a crossing on `snapshot` stays, which the rule cannot order.
- The per-label map kept beside this one: a delta over every seed, three kinds of shared table, and materialising in 8.5 s against 3.4 s.
- `flags` decided per label keeps the earlier `flags` exactly, at 3 to 24 times the time: 1.016 s against 0.043 s for a repeated `flags()` at the materialised leaf.
- datarecord keeping the more specific row across layers: the fold cannot drop an earlier value set at a label under a later broadcast, so every read expands it, 0.045 s against 0.023 s on case 2.

## Open issues

- The tools settle rows by `settle(rel, labels, flags)`, which expands a NULL only on a coordinate in `broadcast` and picks a row only where a coordinate is also in `varies`. `Attr.resolve` and `Schema.resolve` take `labels` and `flags`.
- `write_layer` accepts two crossing rows where a row that sets both exists, so several layers flatten into one. The read of a staged layer refuses two crossing rows without one.
- No fuzzed record has a series over more than one `partial` dim.
- The worst case's slower first read, which Rationale accepts.
