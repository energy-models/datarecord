<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 21: the owner map holds keys as written

| DEP      | 21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | the owner map holds keys as written                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Status   | Draft (2026-10-21)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Requires | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Created  | 2026-10-21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (flags aggregated from expanded rows), [DEP 8](dep-0008-one-interface-source-and-fold.md) (a fold that expands a broadcast against the resolved axes), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (a NULL struct expanded in the fold), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (a re-added label starts fresh), [DEP 16](dep-0016-remove-traits.md) (a NULL row expanded over every label in the owner map), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (a keyed label's broadcasts staying gone after it is added again) |

## Abstract

The owner map holds each layer's attribute keys as the layer wrote them, one row per layer and key with the layer's depth, and no longer expands a NULL into one row per label. The persisted map thus grows with the rows layers write rather than with the labels a broadcast reaches, and shrinks to 37% of today's size on the first benchmark below. A read of an attribute expands that attribute's owner-map rows against the axes and groups resolved at the node being read, keeps for each key the deepest layer covering it, and only then joins the value rows. A broadcast therefore also reaches every label the record holds, and materialising a node no longer changes any answer.

## Motivation

`fold_attributes` expands each row that leaves a `partial` coordinate NULL against the axis or group resolved over the whole path, and keeps one owner-map row per label. The owner map thus grows with the labels a broadcast reaches, not with the rows a layer writes: one broadcast over 60,000 entities and two scenarios takes 120,000 rows. A materialised node persists that expansion, and the frozen prefix of a `WorkingRecord` keeps it as a shared table.

A fold seeded from either does not expand again, so a broadcast also misses every label added after the seed.

A node holding `q = 0.9` for every entity, with a child adding the entity `n`, reads `n` as 0.9 while the node is unmaterialised and as nothing once it is materialised. A label staged in a `WorkingRecord` over the node reads as nothing in both cases. Over 210 random six-layer records, materialising or staging changes one of today's answers in 95 to 128 records, depending on where. [Materialised node caches](../layers.md#materialised-node-caches) promises that materialising changes no answer, and [open questions](../open-questions.md) lists the first two cases.

## Specification

### The owner map

The owner map holds one row per layer and per distinct key as the layer wrote it, with a NULL key column left NULL:

```text
# owner map
<partial dims>   -- the key as written: exactly the partial dims and groups, NULL included
attribute
layer_uuid
varies      STRUCT(<coordinate>: BOOLEAN, ...)
broadcast   STRUCT(<coordinate>: BOOLEAN, ...)
breakpoints BOOLEAN
depth       INTEGER  -- the layer's position in the ancestry, deeper being greater
```

A row's named mask is the set of `partial` coordinates its key names, read off which key columns are NULL, and no map persists it. A map persisted before a dim joined `partial` lacks that dim's column, which reads as NULL, so the mask derived from it stays right and adding to `partial` stays [compatible](../schema.md#versioning).

A layer is folded onto its parent's map in four steps, none of which reads an axis. The parent rows naming a label or tuple that the layer removes by [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)'s diff are dropped, and a row NULL there never is. The parent rows whose key equals one of the layer's, compared NULL-safe, are dropped. Every parent row of an attribute is dropped where the layer holds a row of that attribute leaving each `partial` coordinate NULL. The layer's rows are then added with its depth. A parent row that a deeper row covers in any other way stays in the map, and the read drops it.

### Which layer owns a label

A row covers a key where, on each `partial` coordinate the attribute is over, it names the key's label or holds NULL. The deepest layer holding a covering row owns the key. Within that layer, the read keeps at each coordinate the more specific of its rows, and refuses two rows where neither is, as [the broadcast rule](../record.md#the-broadcast-rule) says today. Two layers never share a depth, so across layers the deeper row always wins and nothing is refused.

### Reach

A NULL on a `partial` coordinate reaches every label of that coordinate's axis, or every tuple of its group, as resolved at the node being read. A label added below a materialised node is reached, as is a label staged in a `WorkingRecord` and the first label of an axis empty when the broadcast was written.

A label removed and added again takes every broadcast covering it, whether written before or after the removal, and never a row that named it below the removal. Taking the broadcasts and not the old named values is intended: a value written for every label reaches every label the record has, and a value written for one label goes with that label.

Materialising a node never changes an answer, since its persisted map is the fold's result with its depths shifted and the read expands against the same resolved axes either way.

### The read

A read of an attribute takes its owner-map rows, `S`, and settles ownership on them before it touches a value row. The read expands each row of `S` that is NULL on a `partial` coordinate against that coordinate's resolved axis or group, by a left join on the constant predicate `(d IS NULL) = true`, and carries the owned label beside the key as written. The read then drops each expanded row that a deeper row of `S` covers, with one anti-join per named mask `m`:

```sql
R ANTI JOIN (SELECT <dims m names>, depth AS _deeper FROM S
             WHERE <mask of the key's NULLs> = m AND depth > <lowest depth in S>) q
  ON q.<dims m names> = R.<owned labels of those dims> AND q._deeper > R.depth
```

A mask held only at the attribute's shallowest depth outranks nothing and is skipped. The value rows of the layers in `S` then find their owned keys by one NULL-safe hash join on the layer and the key as written, the join the read makes today, and each NULL coordinate takes its label from the owned key. Where one layer holds several masks, the window keeping the more specific row follows, and then the anti-join against dead labels, both as today. The row order of a resolved relation stays unspecified.

### `flags`

`flags()` is today's aggregate run over the owner map's rows: per attribute, `bool_or` over each field of `varies` and `broadcast` and over `breakpoints`. `flags()` thus describes the dims used by the rows the fold keeps, and a broadcast that deeper layers shadow on every label still counts, which is an intended change. In probe P7, a child of the root sets `s = 0.9` with every coordinate NULL and its child sets `s` over `snapshot` for every entity, and `flags()["s"].broadcast` holds `entity` under this DEP, where today it lacks it.

`flags(**categories)` aggregates the same way over the rows reaching the category. A named row reaches it where its label, on the dim or through a `partial` group's field, is in the category, as today. A row of an attribute over that dim or group, leaving it NULL, reaches the category where the dim holds a label in it, or the group a tuple whose field is in it, so a fully shadowed broadcast counts per category too, as in probe X4.

### Caching and files

No cache holds a per-label relation. The frozen prefix keeps its owner map as the shared table `owner_map_<rev>` ([a cursor per thread](../read-path.md#a-cursor-per-thread)), onto which a `WorkingRecord` folds its staged layer per read. A materialised node writes its owner map as `resolved/owner_map.parquet`, last, which marks the node as materialised as today.

The file and the shared table store `depth` relative to their node, which holds 0 while its ancestors hold negative depths, and a read shifts it by the node's position in its ancestry. Both are ordered by attribute, so a read of one attribute skips every other attribute's row groups, then by the rest of the key, so two maps holding one attribute's rows hold them in one order. An owner map written by an earlier release is unsupported and not detected, the project being pre-alpha.

### The change to DEP 20

This DEP keeps [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)'s pruning of the owner map by the diff of a layer's restatement against the previous one, which takes no NULL row. The prototype measured here ran on today's tombstones, which drop the same parent rows as the diff for every layer `WorkingRecord` writes, so the composition itself was not run. On implementing both, these sentences of DEP 20 change:

| DEP 20, the sentence                                                                  | becomes                                                                                                                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "In every case probed on a prototype, …"                                              | In every case probed on a prototype, the rule gives today's answers, except that a label added again takes every broadcast covering it.                                                                                                                                                     |
| "A keyed label's values from below its removal stay gone …"                           | A keyed label's named values from below its removal stay gone after it is added again, while every broadcast covering it reaches it again. An unkeyed coordinate's values come back, as [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) accepted.                                |
| "Pruning the owner map against each layer's restatement …", and the sentence after it | Pruning the owner map against each layer's restatement matches the diff except on a hand-written row naming a label in neither restatement, at a semi-join of the whole parent map per restating layer. Pruning against the final axis alone brings back named values from below a removal. |

## Rationale

`depth` is persisted because `layer_uuid` is unordered and a table's row order is no contract. With one `partial` dim no order is needed, since a deeper row leaving it NULL drops every parent row of its attribute in the fold, but with two the order decides which layer owns a key, and PyPSA declares four.

The persisted depths are relative because a shared table outlives its ancestry: an ancestor materialised after `owner_map_<rev>` was built truncates a later reader's ancestry, and with absolute depths that reader's staged layer sits below the table's deepest rows. On a prototype with absolute depths, a staged `(b, NULL) = 9.0` over a prefix row `(b, s1) = 4.0` read `(b, s1)` as 4.0.

The named mask is derived rather than stored, because a positional bitmask over `partial_dims` shifts every later bit when a dim joins `partial`. On a prototype persisting one, a node materialised before `snapshot` joined `partial` read a connection's patched 0.7 beside the broadcast 0.5 it shadows, with no error. Deriving the mask costs one projection over the whole map per frozen record, the likely cause of the worst ratio below.

The fold drops exact keys and whole attributes only, because dropping every parent row that a layer's row covers takes one anti-join per NULL pattern, sixteen per layer for PyPSA, and took 6.2 s against 2.3 s on an earlier prototype's benchmark of the first benchmark's shape.

The read settles ownership on the owner-map rows, one per key, before joining the value rows, which also run along every coordinate outside `partial`. On a series benchmark broadcasting one series over 8,760 snapshots to 2,000 entities, with a child naming 20 entities' series, a prototype expanding the value rows first built 17.5 million rows and read at 2.53 times today, 0.380 s against 0.150 s. Expanding the owner-map rows joins the 8,760 value rows once to the broadcast's 1,980 keys, at 0.97 to 1.04 times today on every read.

The first benchmark holds 60,000 entities, 100 attributes and two scenarios over ten layers, with a materialised leaf and a child adding one entity, and the second a materialised child broadcasting 20 of 40 attributes. Each figure is the median of three interleaved rounds, today's figure second. The worst ratio measured is 1.24 times today, the first read at the materialised leaf, 0.263 s against 0.212 s, and most metrics take 0.3 to 0.8 times. A broadcast attribute of the second benchmark reads in 0.011 s against 0.022 s, and every call of a `flags` benchmark of the first one's size takes 0.44 to 0.91 times today. Materialising takes 2.81 s against 4.76 s, and the persisted map holds 6.0 million rows in 25.3 MB against 12.0 million in 67.6 MB.

Over 210 fuzzed records of six edit layers, with `entity`, `scenario` and the group `connection` as `partial` coordinates, the prototype reads the same plainly, materialised at two depths, staged, and staged over a materialised parent. The 65 records that read without adding a label again equal today's plain read, and all but one of the 137 that add one again equal a plain-Python oracle of the rule, the exception being a group emptied of tuples, over which a NULL row reads once as today. `flags` equals an oracle aggregating the rows the fold keeps in every record.

On the prototype, `src` grows by 264 lines and shrinks by 224.

## Usage

A broadcast reaches a label added below a materialised node, committed or staged:

```python
c = rev.child(w.edits)  # w.set("p_max_pu", 0.9)
c.materialise()
w = WorkingRecord(c.record)
w.add("entity", ["n"], ctype="Generator")
w.attributes["p_max_pu"]  # n reads 0.9
c.child(w.edits).record.attributes["p_max_pu"]  # n reads 0.9
```

A label removed and added again takes the broadcasts, not its old named value:

```python
w.set("q", 0.9)
w.set("q", 0.5, entity="b")  # one layer
# a child removes b, a grandchild adds b again
grandchild.record.attributes["q"]  # b reads 0.9
```

The deeper layer owns a key, whichever row names more dims:

```python
w.set("p_max_pu", 1.0, entity="b")  # the root
v.set("p_max_pu", 2.0, scenario="s1")  # its child
child.record.attributes["p_max_pu"]  # (b, s1) reads 2.0, (b, s2) 1.0
# with the two layers swapped, (b, s1) reads 1.0
```

A broadcast shadowed on every label still shows in `flags`:

```python
c = rev.child(w.edits)  # w.set("s", 0.9)
# its child sets s over snapshot for every entity
grandchild.record.flags()["s"].broadcast  # holds entity, which today it lacks
```

## Implementation notes

Paths are under `src/datarecord/`.

1. `schema/schema.py`: the owner map gains `depth`.
2. `resolve/owner_map.py`: `fold_attributes` takes the schema and the depth instead of `Coords`, loses the expansion and gains the whole-attribute drop. `_persisted` makes depths relative and orders the rows, and `_frozen_table` shifts depths back.
3. `resolve/record.py`: `_reach` replaces `_as_stored`, and `_owners` returns each attribute's masks and depths.
4. `resolve/fold.py`: `_owned_reaching` gains the NULL-row arm, and `resolve/coords.py` loses `Coords.expand_dims`.
5. `store/cache.py`: `materialise` writes the map through `_persisted`.
6. Tests: three asserting the per-label map assert the read instead. New tests pin a dim joining `partial` over a materialised node, a shared map outliving its ancestry, reach, re-adding, and probes P7 and X4.
7. `CHANGELOG.md` under `[Unreleased]`: a broadcast reaches every label, a re-added label takes broadcasts again, and `flags` counts a shadowed broadcast.
8. On landing, update [read path](../read-path.md), [layers](../layers.md), [record](../record.md), [writing](../writing.md), [schema](../schema.md), [format](../format.md), [working record](../working-record.md), [tools](../tools.md), [the index](../index.md), [module layout](../module-layout.md) and the usage page on [layers](../../usage/layers.md) where they describe the owner map, its expansion or its file. In [open questions](../open-questions.md), close the item on a broadcast surviving materialisation, and rewrite the settled item on an attribute reaching every label, since a broadcast is one map row whatever it reaches.

Checks: every probe reads the same materialised or not, staged or committed, and the 210 fuzzed records read the same in all five ways.

## Rejected Ideas

Keeping the per-label owner map, with a map of keys as written beside it to derive the labels it lacks, needs a delta over every seed and three kinds of shared table, and materialising took 2.5 times today in its own round, 8.5 s against 3.4 s.

Deciding `flags` per label, counting a row only where some label it covers has no deeper covering row, keeps today's `flags` exactly. On the `flags` benchmark it took 3 to 24 times today, 1.016 s against 0.043 s for a repeated `flags()` at the materialised leaf.

Picking the owning layer by a window, `arg_max(depth)` per label, gives the same answers at 1.21 times today on a root-only attribute, where the anti-joins take 0.68 times, and reorders rows, which three tests notice.

Letting the more specific row win across layers keeps an earlier named value under a later broadcast, which the fold then cannot drop, so every read of its attribute expands it against the named rows, 0.045 s against 0.023 s on the second benchmark.

Keeping only the more specific key per label drops rows at coordinates outside `partial`, since a layer holding `s` at `(entity NULL, snapshot 0)` and at `(b, snapshot 1)` gives `b` both rows.

## Open Issues

A staged layer holding two rows that differ on a dim outside `partial`, neither more specific, reads without refusal, as today, though `write_layer` refuses it on commit.
