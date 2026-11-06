<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 21: the owner map holds keys as written

| DEP      | 21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | the owner map holds keys as written                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Status   | Implemented (2026-10-25)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Requires | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Created  | 2026-10-21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (flags aggregated from expanded rows), [DEP 8](dep-0008-one-interface-source-and-fold.md) (a fold that expands a broadcast against the resolved axes), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (a NULL struct expanded in the fold), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (a re-added label starts fresh), [DEP 16](dep-0016-remove-traits.md) (a NULL row expanded over every label in the owner map), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (a keyed label's broadcasts staying gone after it is added again) |

## Abstract

The owner map holds each layer's attribute keys as the layer wrote them. Where a deeper layer's row crosses a parent row, that is where the two cover a common key and neither names every dim the other names, the parent row stays as written and the fold adds one overlap row, owned by the deeper layer, naming every label either row names. The fold thus reads no axis, and the persisted map grows with the rows layers write and the pairs that cross, not with the labels a broadcast reaches: 0.63 times today's rows on the first benchmark below, 0.01 times on the realistic case at five `partial` dims. A read returns the owned rows as written, NULL included, and a consumer settles each coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule) alone, the more specific row winning, with no depth and no list of `partial` dims. Every broadcast therefore reaches every label the record holds, and materialising a node no longer changes an answer.

## Motivation

`fold_attributes` expands each row that leaves a `partial` coordinate NULL against the axis or group resolved over the whole path, and keeps one owner-map row per label. The owner map thus grows with the labels a broadcast reaches, not with the rows a layer writes: one broadcast over 60,000 entities and two scenarios takes 120,000 rows. A materialised node persists that expansion, and the frozen prefix of a `WorkingRecord` keeps it as a shared table.

A fold seeded from either does not expand again, so a broadcast also misses every label added after the seed.

A node holding `q = 0.9` for every entity, with a child adding the entity `n`, reads `n` as 0.9 while the node is unmaterialised and as nothing once it is materialised. A label staged in a `WorkingRecord` over the node reads as nothing in both cases. Over 210 random six-layer records, materialising or staging changes one of today's answers in 95 to 128 records, depending on where. [Materialised node caches](../layers/resolution.md#materialised-node-caches) promises that materialising changes no answer, and [open questions](../open-questions.md) lists the first two cases.

The expansion settles one thing only: which layer owns a key that rows of two layers cover. Within a layer the more specific row wins, and across layers the deeper one does. The two rules disagree only where two rows of different layers cross, and there a single row at their overlap, owned by the deeper layer, makes them agree. Everywhere else the rows can stay as written and the broadcast rule alone gives every answer.

## Specification

### The owner map

The owner map holds one row per layer and per distinct key, with a NULL key column left NULL, and no depth:

```text
# owner map
<partial dims>   -- the key: exactly the partial dims and groups, NULL included
attribute
layer_uuid
varies      STRUCT(<coordinate>: BOOLEAN, ...)
broadcast   STRUCT(<coordinate>: BOOLEAN, ...)
breakpoints BOOLEAN
```

A row is either a key a layer wrote or an overlap row. An overlap row names labels on dims where the owning layer's key holds NULL and carries that layer's `varies` and `broadcast`, so the layer's value row finds it by one NULL-safe hash join on the key as stored and takes the overlap row's labels.

### Crossing

A row names a `partial` dim where its key holds a label, and any other dim where it varies along it, so a series over `snapshot` names `snapshot`. Only the dims the attribute uses count: its `dims` and the dims of its groups. Two rows cover a common key where, on each `partial` coordinate, they hold the same label or one of them holds NULL. They cross where they cover a common key and neither names every dim the other names.

### The fold

A layer is folded onto its parent's map in four steps, none of which reads an axis:

1. The parent rows naming a label or tuple that the layer removes by [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)'s diff are dropped, and a row NULL there never is.
2. A parent row is dropped where a row of the layer covers it and names no `partial` dim the parent row leaves NULL.
3. For each pair of a parent row and a layer's row crossing on `partial` dims, found by one equi-join per pair of named sets over the dims both name, the layer gains an overlap row naming every label either names. A parent row naming strictly fewer dims than the layer's row it overlaps stays as written, as does a crossed parent row.
4. The layer's rows and its overlap rows are added.

An overlap row is an ordinary row to every later fold, which drops, crosses or removes it like any other.

With `partial = {entity, scenario}`, a root writing `(gas, NULL) = 50` and `(wind, NULL) = 100` and a child writing `(NULL, high) = 999`:

```text
# today's owner map, one row per label
(gas,  low)  root    (gas,  high) child
(wind, low)  root    (wind, high) child

# this DEP
(gas,  NULL) root    # as written
(wind, NULL) root
(NULL, high) child   # as written
(gas,  high) child   # overlap rows, naming both dims
(wind, high) child
```

A child writing `(gas, NULL) = 60` instead names no more than the root's `(gas, NULL)` and drops it. A child writing `(gas, high) = 70` names more and leaves the root's rows as written, and the broadcast rule gives `(gas, high)` to the child.

### A dim outside `partial`

An overlap row cannot settle a crossing on a dim outside `partial`, since the key has no column to name its labels. A `WorkingRecord` therefore settles it as it stages: a staged row leaving such a dim NULL, over a base row naming it, is also written out along the base row's labels there, with the staged value. With `partial = {entity}`, a root writing a series over `snapshot` for every entity and a child writing `(wind, NULL) = 0.9`:

```text
# root:  (NULL, 0) = 0.2   (NULL, 1) = 0.5
# child as staged: (wind, NULL) = 0.9
# child as written: (wind, NULL) = 0.9, (wind, 0) = 0.9, (wind, 1) = 0.9
```

The child's rows at `wind` name `entity` and `snapshot`, a strict superset of the root's, so nothing crosses and the root's series stays as written. This extends the completion a `WorkingRecord` already makes, which carries the base rows a staged key reaches so that a layer holds a key's whole extent along every dim outside `partial`. The fold refuses a layer written otherwise that leaves such a crossing, naming the attribute, the dim and the key.

### The read

A read of an attribute joins the value rows of the owning layers to the owner map's rows, as today, and returns the owned rows as written: a key NULL on a dim stays NULL, and an overlap row reads its labels. It expands no NULL and drops no row by depth. The anti-join against dead labels follows as today, and the row order stays unspecified.

A consumer settles a coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule) over the full coordinates, `partial` or not: a row's value applies to every combination of its NULL dims, and at each coordinate the row naming a strict superset of every other row reaching it wins. Wherever two rows cross, the fold or the staging has written a row naming both, so the rule finds one winner wherever a row reaches, and a consumer needs neither depth nor the list of `partial` dims.

### Reach

A NULL on a `partial` coordinate reaches every label of that coordinate's axis, or every tuple of its group, as resolved at the node read, since the read leaves it NULL and no row is split. A label added below a materialised node is reached, as is a label staged in a `WorkingRecord` and the first label of an axis empty when the broadcast was written. Adding the scenario `mid` below the child of the first example reads `(gas, mid)` as 50, and adding the entity `solar` below the child of the series example reads the root's series for it.

A row reaches every coordinate no more specific row covers, whichever layer wrote it. A deeper layer thus owns only the coordinates its rows reach, not the whole extent of a key along a dim outside `partial`. With `partial = {entity, scenario}` and `x` also over `snapshot`, a root writing `(gas, NULL, 1) = 1.0` and a child writing `(NULL, high, 0) = 2.0`, `(gas, high, 1)` reads 1.0 from the root, where today it reads nothing. Likewise a root writing `x = 1.0` with every coordinate NULL and a child writing `(gas, NULL, 0) = 2.0` reads `(gas, low, 1)` as 1.0, where today it reads nothing.

A label removed and added again takes every broadcast covering it, whether written before or after the removal, and never a row that named it below the removal.

Materialising a node changes no answer, since its persisted map is the fold's result and the read expands nothing against the axes of the node read. A map persisted for another set of `partial` dims holds NULL in a column for a row varying along it, so the read ignores it and folds the node from its layers again.

### `flags`

`flags()` is today's aggregate run over the owner map's rows: per attribute, `bool_or` over each field of `varies` and `broadcast` and over `breakpoints`. Both structs report only the dims the attribute uses, so `ctype` no longer reads true in `broadcast` for an attribute not over it. `flags()` describes the dims used by the rows the fold keeps, and a broadcast that deeper layers shadow on every label still counts. In probe P7, a child of the root sets `s = 0.9` with every coordinate NULL and its child sets `s` over `snapshot` for every entity, and `flags()["s"].broadcast` holds `entity`, where today it lacks it.

`flags(**categories)` aggregates the same way over the rows reaching the category, as today.

### Files and shared tables

The frozen prefix keeps its owner map as the shared table `owner_map_<key>_<rev>`, `<key>` the hash of its key columns ([a cursor per thread](../storage/concurrency.md#a-cursor-per-thread)), onto which a `WorkingRecord` folds its staged layer per read. A materialised node writes its owner map as `resolved/owner_map.parquet`, last, which marks the node as materialised as today. An owner map written by an earlier release is unsupported and not detected, the project being pre-alpha.

### The change to DEP 20

On implementing this DEP, these sentences of [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) change:

| DEP 20, the sentence                                        | becomes                                                                                                                                                                                                                                                      |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| "In every case probed on a prototype, …"                    | In every case probed on a prototype, the rule gives today's answers, except that a label added again takes every broadcast covering it.                                                                                                                      |
| "A keyed label's values from below its removal stay gone …" | A keyed label's named values from below its removal stay gone after it is added again, while every broadcast covering it reaches it again. An unkeyed coordinate's values come back, as [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) accepted. |

## Rationale

The fold settles ownership across layers, so the map needs no depth and the read no anti-join. A depth would let the read or the consumer settle it instead, by taking the deepest covering layer first and the most specific row within it, but every way of doing so measured below costs each read more than the fold costs once, and a consumer would have to know about layers.

An overlap row settles a crossing without an axis. Its labels come from the two rows, so a later label reaches the crossed parent row unchanged, and its value from the deeper layer, whose value rows hold the dims the overlap row's key leaves NULL. Any parent row more specific than the overlap within the deeper row's reach is one that deeper row covers, and step 2 has dropped it.

Crossing is judged over every dim the attribute uses, not over `partial` alone, because the broadcast rule a consumer applies runs over the full coordinates. Left as written, the series example holds the root's `(NULL, 0)` beside the child's `(wind, NULL)`, and at `(wind, 0)` the rule finds two rows, one naming `snapshot` and one naming `entity`, neither the winner. The key holds no `snapshot`, so that crossing is settled where the labels are, as the layer is staged.

A parent row that a deeper, more specific row covers stays as written, since there the two rules agree.

On a prototype on DEP 20, against today, each figure the median of three interleaved rounds run alone. The first benchmark holds 60,000 entities, 100 attributes and two scenarios over ten layers, with a materialised leaf and a child adding one entity, and the second a materialised child broadcasting 20 of 40 attributes. The scaling benchmark varies the number `k` of `partial` dims: `entity` with 20,000 labels and `k - 1` further dims of three labels each, every attribute being over all of them. Its realistic case (R) holds nine layers and ten attributes, with at most four sets of named dims per attribute. Its worst case (W) holds `2^k` layers, each naming the dims of its own set, in counting order, so most layers cross the ones below them. Its series case (S) holds 20,000 entities, two scenarios and 8,760 snapshots.

| case | map rows, today | map rows | first read, today  | first read | repeated read, today | repeated read |
| ---- | --------------- | -------- | ------------------ | ---------- | -------------------- | ------------- |
| 1    | 12.0 M          | 7.50 M   | 2.60 s             | 2.98 s     | 0.036 s              | 0.025 s       |
| 2    | 4.80 M          | 1.20 M   | 0.77 s             | 0.24 s     | 0.018 s              | 0.007 s       |
| R k3 | 1.80 M          | 223 k    | 0.70 s             | 0.69 s     | 0.019 s              | 0.013 s       |
| R k4 | 5.40 M          | 223 k    | 1.85 s             | 0.75 s     | 0.049 s              | 0.015 s       |
| R k5 | 16.2 M          | 223 k    | 6.25 s             | 0.81 s     | 0.147 s              | 0.017 s       |
| W k3 | 720 k           | 8.8 k    | 0.70 s             | 0.56 s     | 0.028 s              | 0.020 s       |
| W k4 | 2.16 M          | 35 k     | 2.30 s             | 3.02 s     | 0.107 s              | 0.049 s       |
| W k5 | 6.48 M          | 163 k    | 13.7 s             | 20.1 s     | 0.315 s              | 0.160 s       |
| S k2 | 120 k           | 124      | materialise 25.7 s | 0.22 s     |                      |               |

The first read folds, and materialising follows it. The read returns rows as written, so its times leave out the consumer's expansion. A read of a record with two entities set in a staged layer over the materialised node takes 1.0 to 2.8 times today in the scaling benchmark, 0.11 times in the series case.

The first read is slowest in the worst case, at 1.3 to 1.5 times today for `k = 4` and 5, since pairs of sets neither naming the other grow from 1 at `k = 2` to 55 at `k = 4` and 285 at `k = 5`, and the fold joins along each pair a parent row and a layer's row hold. This DEP accepts that cost: a record overriding one attribute along ever different sets of dims in a dozen layers is not one a workflow builds.

Over all 210 fuzzed records of six edit layers, with `entity`, `scenario` and the group `connection` as `partial` coordinates, reads and `flags` agree in all five ways: plain, materialised at depth 1 and 3, staged, and staged over a materialised parent. The plain read equals a plain-Python oracle of the broadcast rule over full coordinates, every row reaching the final axes, on 197 records. The 13 others hit the oracle's limits: 3 a group emptied of tuples, where the read keeps the NULL row; 2 a connection removed and added again within one layer, where DEP 20's diff sees no removal; and 5 a label new or added again in the layer writing a row over `snapshot` at it, where the shallower row reaches the other snapshot, as [Reach](#reach) accepts. A layer writing a full broadcast and a named row at once reads as the fold's step 2 gives, which an earlier oracle got wrong. Every probe reads the same materialised or not, staged or committed, and no fuzzed record triggers the refusal of a layer written without `WorkingRecord`.

## Usage

A read returns the rows as written, overlap rows included:

```python
w.set("x", 50.0, entity="gas")  # the root
w.set("x", 100.0, entity="wind")
v.set("x", 999.0, scenario="high")  # its child
child.record.attributes["x"]
# (gas, NULL) 50, (wind, NULL) 100, (NULL, high) 999, (gas, high) 999, (wind, high) 999
```

A broadcast reaches a label added below a materialised node, committed or staged:

```python
c = rev.child(w.edits)  # w.set("p_max_pu", 0.9)
c.materialise()
w = WorkingRecord(c.record)
w.add("entity", ["n"], ctype="Generator")
w.attributes["p_max_pu"]  # (NULL) 0.9, which n takes by the broadcast rule
```

A constant override of a broadcast series is written out along the series' snapshots:

```python
w.set("p_max_pu", [0.2, 0.5], snapshot=[0, 1])  # the root, every entity
v.set("p_max_pu", 0.9, entity="wind")  # its child
# the child holds (wind, NULL), (wind, 0) and (wind, 1) at 0.9
```

## Implementation notes

Paths are under `src/datarecord/`.

1. `resolve/owner_map.py`: `fold_attributes` drops the covered parent rows by `_masks` and `_exactly`, pairs crossing rows by `_pair` and adds the overlap rows by `_overlaps`, and refuses an unsettled crossing outside `partial` by `_refuse_unfilled`. It takes no axes.
2. `resolve/record.py`: `_as_stored` returns the owned rows as written and reads the labels of an overlap row, and `_owners` returns each attribute's rows.
3. `resolve/fold.py` folds without axes, and `resolve/coords.py` loses `Coords.expand_dims`.
4. `mutable.py`: `_complete_owned_whole` also writes a staged row out along the base rows' labels on dims outside `partial` (`_spread`), and a staged layer folds onto the prefix's map the same way.
5. Consumers: `tools/pypsa.py`'s attribute and series reads, `tools/base.py`'s `Attr.resolve`, and `store/write.py`'s refusal of a pair neither more specific when it flattens a record into one layer apply the broadcast rule over rows as written.
6. Tests asserting per-label reads assert rows as written instead. New tests and probes pin a deeper entity winning over a series it crosses, a broadcast reaching a label added below a crossing, a series reaching an entity added below an override, and the two coordinates a shallower row now reaches.
7. `CHANGELOG.md` under `[Unreleased]`: a read returns rows as written, a broadcast reaches every label, a deeper layer owns only the coordinates its rows reach, a re-added label takes broadcasts again, and `flags` counts a shadowed broadcast and reports only an attribute's dims.
8. On landing, update [resolution](../layers/resolution.md), [the layer tree](../layers/tree.md), [reading](../use/reading.md), [writing](../use/writing.md), [editing](../use/editing.md), [the schema](../model/schema.md), [the format](../storage/format.md), [tools](../tools/index.md), [the index](../index.md), [module layout](../package.md) and the usage page on [layers](../../usage/layers.md) where they describe the owner map, its expansion or what a layer owns. In [open questions](../open-questions.md), close the item on a broadcast surviving materialisation.

Checks: every probe reads the same materialised or not, staged or committed, and the 210 fuzzed records read the same in all five ways: plain, materialised at two depths, staged, and staged over a materialised parent.

## Rejected Ideas

Keeping keys as written with their depth and settling ownership at every read, by expanding the NULLs and one anti-join per named mask, reads at 1.55 to 2.1 times today in the realistic case from `k = 2` on and at 31 times today in the worst case at `k = 5`, and runs out of memory at 24.6 GiB for `k = 6`. Caching each attribute's settled rows in a session file restores repeated reads to 0.15 to 0.90 times today, but moves the cost to the first read of each attribute in every session: a first sweep over the realistic case takes 2.5 to 3.6 times today, and 31 times in the worst case at `k = 5`. Picking the owner by a window, `arg_max(depth)`, brings a repeated read of the worst case at `k = 5` from 31 to 5.5 times today but takes the realistic case to 3.3 times. Building the file root to leaf halves the worst case's first read at `k = 5` but slows the realistic case's by half, and disabling DuckDB's `build_side_probe_side` optimizer during a build takes it to 3.9 s against 9.3 s but holds for every concurrent cursor. A cache shared across sessions would pay the first pass once per revision, but needs a key covering the `partial` dims and a size cap, and keeps a per-label file per attribute.

Splitting a crossed parent row instead of adding an overlap row, expanding it along the dims the deeper row names against the axis as the crossing layer resolves it and dropping the pieces a deeper row reaches, reads the same as this DEP wherever no label is added below a crossing. Its pieces name the labels of that axis only, so a label added later reaches none of them: adding `mid` below the child of the first example reads `(gas, mid)` as nothing, and in 8 of the first 70 fuzzed records a label added below a crossing reads differently from the oracle. Its first read takes 65 s against 20 s for the worst case at `k = 5`, and its map holds 2.01 million rows against 163,000. Splitting every parent row a deeper row names a dim of, more specific or not, splits where the two rules agree as well, and dropping at the read the rows a deeper one shadows by an `arg_max` window over a depth reads at 2 to 8 times that.

Judging a crossing over the `partial` dims alone leaves the series example's rows crossing on `snapshot`, which the broadcast rule cannot order.

Keeping the per-label owner map, with a map of keys as written beside it to derive the labels it lacks, needs a delta over every seed and three kinds of shared table, and materialising took 2.5 times today in its own round, 8.5 s against 3.4 s.

Deciding `flags` per label, counting a row only where some label it covers has no deeper covering row, keeps today's `flags` exactly. On a `flags` benchmark of the first benchmark's size it took 3 to 24 times today, 1.016 s against 0.043 s for a repeated `flags()` at the materialised leaf.

Letting the more specific row win across layers keeps an earlier named value under a later broadcast, which the fold then cannot drop, so every read of its attribute expands it against the named rows, 0.045 s against 0.023 s on the second benchmark.

## Open Issues

The tools settle rows by one helper, `settle(rel, labels, flags)`, which expands a NULL only on a coordinate in `broadcast` and picks a row only where a coordinate is also in `varies`; `Attr.resolve` and `Schema.resolve` take `labels` and `flags`. `write_layer` accepts two crossing rows where a row naming both is there, so a record of several layers flattens into one. The read of a staged layer refuses two rows crossing with no row naming both, as `write_layer` refuses them on commit.

No fuzzed record holds a series over more than one `partial` dim.

The worst case's first read stays at 1.3 to 1.5 times today for `k = 4` and 5, which [Rationale](#rationale) accepts.
