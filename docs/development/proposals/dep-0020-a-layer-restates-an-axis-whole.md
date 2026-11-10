<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 20: a layer restates an axis whole

| DEP      | 20                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a layer restates an axis whole                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Status   | Implemented (2026-10-22)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Requires | [DEP 19](dep-0019-one-record-and-a-package-layout.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Created  | 2026-10-21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (entity tombstones, and an order the fold generates), [DEP 4](dep-0004-staging-as-a-layer.md) (the per-label axis fold, and `partial` as how many labels an axis layer carries), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (group tombstones, by which the fold drops owner-map rows), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (removal by tombstone, and dead labels as winning tombstones), [DEP 17](dep-0017-results-are-attributes.md) (a SubNetwork as an `entity`, and its tombstones), [DEP 19](dep-0019-one-record-and-a-package-layout.md) (a re-added key introduced where it is re-added) |

The design landed in [layers](../../reference/layers.md), [the fold](../../reference/resolution.md), [the format](../../reference/format.md) and [tools](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

A layer that changes an axis or a group writes its file whole and in order, and the deepest such file is the resolved one. A restatement that omits a label removes it, so no file has a `deleted` column. Attributes stay per-key overrides under `partial`. A PyPSA SubNetwork becomes a label of its own dim.

## Motivation

`_fold_axis` folds an axis per label from every layer that touched it. A label takes its deepest row, is dropped where that row is a tombstone, and sits where its first row after its deepest tombstone sits. No single file lists a revision's axis or its order.

The PyPSA tool numbers a component's ports by connection order, so the order matters. [DEP 19](dep-0019-one-record-and-a-package-layout.md) reshaped the order rule to make a cache agree with a full fold. Every new seed or cache needs that check again.

The per-label fold also hides a file out of order or with a label twice. On an axis, `partial` also sets how much of it a layer writes, a second meaning beside attribute ownership.

## Specification

### An axis is its deepest file

A layer that adds or removes a label, or changes a property, writes `dims/<dim>.parquet` whole: every label at that revision, with its properties, in order. `groups/<group>.parquet` follows the same rule. A revision's resolved axis or group is the deepest such file on its path, in row order. `attributes/<attr>.parquet` still contains only the keys a layer sets.

A materialised revision's `resolved/dims/` and `resolved/groups/` contain copies of the deepest layer file at or below it, so a read still stops there. `resolved/dead/` and `owner_map.parquet` stay folds.

### Removal

A label is removed at a layer whose restatement omits it while the previous restatement lists it. The previous restatement is the deepest file below that layer, a materialised base's resolved axis, or the frozen prefix's table. A first restatement removes nothing.

For a `partial` dim or group, the owner-map fold drops the parent rows that contain a removed label. A mortal coordinate is one outside the owner-map key, or one that a group's field ranges over or a property takes its labels from. Its dead labels are the labels some layer removed that the final axis lacks: the base's dead set, the base's resolved axis and every restatement above the base, minus the final axis. The reference check takes a layer's removed labels from its parent's axis minus its restatement.

In every case probed on a prototype, the rule gives the earlier answers. A keyed label's values from below its removal stay gone after it is added again, and an unkeyed coordinate's come back, as [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) accepted. A label removed and added again in one staging area keeps its parent's values, and a broadcast applies to a label added after a restatement.

`WorkingRecord.remove` restates the axis without the label, and each group over its dim without the tuples that contain it. `set` refuses a label missing from any dim's staged axis, or a tuple missing from a staged group. `add_group` refuses a tuple whose label is missing from a field's staged axis. Each message names the dim or group, the field where there is one, and up to three labels. Where `remove` follows a `set` in one staging area, the fold drops the staged rows that contain the removed label.

### Order

An axis's order is its deepest file's row order. A label added again after its removal sits where the later restatement puts it, which subsumes the rule of DEP 19.

`WorkingRecord` stages an axis or group as a whole table, copied from the base at the first edit, with a position column. `add` appends a new label, or returns a base label to its base position. `StagedSource` hands the table over in position order, so a read composes nothing. `write_layer` refuses a restatement whose order on its parent's labels differs from the parent's, naming the first label out of place.

### Refusals

datarecord refuses a `dims/` or `groups/` file with a `deleted` column, at read and in `write_layer`, so an old layer fails instead of losing labels:

```text
dims/bus.parquet carries a `deleted` column; a layer removes a label by
restating its axis without it (.../design/layers/#deletion). Re-export the
record, or flatten it with `ws.write` under the previous release.
```

The fold and `write_layer` refuse a key that occurs twice in one such file. datarecord refuses at read a column that is neither the key nor a declared property, as `write_layer` already does. `deleted` stays a reserved name.

`write_layer` applies the checks of `set` and `add_group` to every layer, hand-written or not. Each check is one DuckDB anti-join (a query that keeps the rows with no match in another table). It runs against the layer's final axis or group: its own file, else its parent's resolved one.

### The reference check

The reference check reads each subject a layer restates, or whose rows contain a removed label, whole and in its final state. Where an ancestor left a reference dangling, the next write that touches its subject fails, and that write can repair the row. The promise that such a reference "fails no later write" goes.

### SubNetworks are a dim

A solve's SubNetworks written into the entity axis would remove every other entity. A SubNetwork becomes a label of the dim `sub_network`, which a results layer restates alone:

| Before                                                  | Under this DEP                                                |
| ------------------------------------------------------- | ------------------------------------------------------------- |
| the `entity` label `0 SubNetwork`, `ctype="SubNetwork"` | the `sub_network` label `0`                                   |
| `SubNetwork` listed by `ctype`                          | not listed, so datarecord refuses `flags(ctype="SubNetwork")` |
| `entity.carrier`, `slack_bus` over `entity`             | `sub_network.carrier`, `slack_bus` over `sub_network`         |
| a Line's `sub_network` value `0 SubNetwork`             | `0`, still over `entity`                                      |

`sub_network.carrier` takes its labels from `carrier`. `sub_network` stays outside `partial`, because a results layer writes every SubNetwork's values. A full export writes the axis beside `bus` and `carrier`. A solve without SubNetworks over a base with the axis restates it empty, with the declared column types. Over a base without the axis, a results layer writes none.

## Rationale

No seed, cache or staged fold can then disagree with a full fold. Groups follow the rule because port order is connection order.

Each number here is one DuckDB run at PyPSA-Eur scale, 60,000 entities and 150,000 connections, unless stated. A `sub_network` axis is 12 rows and 0.4 kB, or 18 kB where each of 4,000 buses is its own SubNetwork. The change needs no migration of its own, because every record the tool wrote has `deleted` columns and is re-exported anyway.

On a chain of 20 edit layers over 40,000 entities and 80,000 connections, a prototype without the order and duplicate checks wrote the chain in 4.3 s against 5.7 s, and read `p_nom` cold at its leaf in 1.07 s against 2.17 s. The entity fold drops from 80 to 95 ms, whatever the override count, to a 50 ms read of the deepest file.

What it costs: storage grows with the axis, not the edit. A one-entity property edit writes 0.30 MB, where an override wrote 0.8 kB. Adding a connection writes 0.43 MB, removing an entity restates both for 0.73 MB, and a `connection.role` edit restates every tuple. The chain takes 5.57 MB, not 10 kB. The cost multiplies in trees with many small leaves and on a remote store. Each restatement adds a diff to every fold, 6 ms for the entities and 18 ms for the connections. Building the dead set reads every restatement above the base, 23 ms over 20 of the entities. A duplicate check costs 5 to 11 ms per file. A `WorkingRecord`'s first edit of an axis copies it, an estimated 15 ms for the entities and 41 ms for the connections.

A tree with `deleted` columns is flattened with `ws.write` under the previous release, losing its history. A full PyPSA export is refused as a child where its parent's entity order interleaves types, and is written as a root instead. datarecord applies the same refusals to a foreign directory, so not every parquet directory reads as a layer. A hand-written restatement that omits a property column blanks it for every label. A hand-written override without a `deleted` column reads silently as a restatement that removes every label it omits, and datarecord has no format version by which to detect it. `src` loses an estimated 100 to 130 lines and gains 135 to 235.

## Implementation notes

1. The fold, the writer and `WorkingRecord` as above, then the PyPSA tool's `sub_network` dim.
2. Test fixtures restate from the parent, and at least 18 tests change.
3. `CHANGELOG.md` and the pages. DEP 1, 4, 10, 11, 17 and 19 take `Superseded-By` this DEP for the parts under `Replaces`.

Checks: a label removed and re-added reads in one order, materialised or not, staged or committed, with `connection` in `partial` and out. No file that `write_layer` writes under `dims/` or `groups/` has a `deleted` column.

## Rejected ideas

- The 7-line fold fix of DEP 19 fixes the order without a format change, but keeps tombstones, `partial`'s second meaning and a derived order.
- A format version in `manifest.json` would catch an old override, at the cost of a second version beside `schema.version`.
- Restating the entity axis per results layer writes 0.30 MB per solve in 20 ms, plus 10 ms for its order check, small beside one `marginal_price` result over 4,000 buses and 8,760 snapshots, at most 301 MB. It rewrites an axis the solve never changed, and fails on an ancestor's dangling reference.
- Checking only the rows a restatement changes keeps the old promise. It costs one more anti-join per subject, and the dangling row stays.
- Sorting an out-of-order restatement brings the per-label fold back at write time.
- Dropping undeclared columns loses a foreign column silently at the first edit of its axis.
- Pruning the owner map per restatement drops a broadcast's rows for later labels, and against the final axis alone it brings back removed values.
- Without the dead cache, series reads run 4.4 times slower: 78 ms against 18 ms on 43.8M `p_max_pu` rows.
- A pointer in `resolved/` instead of a copy saves 0.73 MB per materialised revision, but a read would open an ancestor's file.

## Open issues

- The order check allows a new label before one of its parent's. In a group, a new tuple there renumbers an entity's later ports. `WorkingRecord` appends, so only a layer written another way can do this.
- [DEP 14](dep-0014-combining-records.md)'s open issue, extending a tree without restating its base, gets no easier.
