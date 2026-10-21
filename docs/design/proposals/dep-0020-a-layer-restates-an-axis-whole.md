<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 20: a layer restates an axis whole

| DEP      | 20                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a layer restates an axis whole                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Status   | Draft (2026-10-21)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Requires | [DEP 19](dep-0019-one-record-and-a-package-layout.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Created  | 2026-10-21                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (entity tombstones, and an order the fold generates), [DEP 4](dep-0004-staging-as-a-layer.md) (the per-label axis fold, and `partial` as how many labels an axis layer carries), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (group tombstones anti-joining the owner map), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (removal by tombstone, and dead labels as winning tombstones), [DEP 17](dep-0017-results-are-attributes.md) (a SubNetwork as an `entity`, and its tombstones), [DEP 19](dep-0019-one-record-and-a-package-layout.md) (a re-added key introduced where it is re-added) |

## Abstract

A layer that changes an axis or a group writes its file whole, in order, and the resolved axis or group is the deepest layer's file for it. A label is removed where a restatement of its axis omits it after the previous one held it, so no `dims/` or `groups/` file carries a `deleted` column. Attributes stay per-key patches under `partial`, which keeps one meaning, attribute ownership. A PyPSA SubNetwork becomes a label of its own dim.

## Motivation

`fold_axis` in `duck.py` folds an axis per label from every layer that touched it. A label takes its deepest row and is dropped where that row is a `deleted = true` tombstone. Its place is that of its first row deeper than its deepest tombstone. No single file says which labels a node's axis has or in which order.

That order is load-bearing, since `_ordered_connections` in `tools/pypsa.py` numbers a component's ports by connection order. [DEP 19](dep-0019-one-record-and-a-package-layout.md) had to reshape the order rule so that a cache holding a removed label only as a dead label agrees with a full fold. Every new seed or cache has to be checked against that rule again.

The per-label fold also hides a writer's mistakes. `_axis_layer` in `mutable.py` writes a non-`partial` axis with its staged rows first. A child adding `z` and then the existing label `x` thus stores `['z', 'x', 'y']`, which the fold re-ranks to `['x', 'y', 'z']`. A file holding label `a` twice reads as one `a`, with the properties of either row. On an axis, `partial` also says how much of it a layer writes ([partial](../schema.md#partial-the-granularity-of-an-override)), a second meaning beside attribute ownership.

## Specification

### An axis is its deepest file

A layer that adds or removes a label of a dim, or changes a property of one, writes `dims/<dim>.parquet` whole: every label the axis has at that node, with its properties, in order. A layer that changes a group's tuples or their properties writes `groups/<group>.parquet` whole the same way. A node's resolved axis or group is the deepest file for it on the path from the root, in that file's row order. `attributes/<attr>.parquet` still holds only the keys a layer sets, owned per key along the `partial` coordinates ([the owner map](../read-path.md#owner-map)).

A materialised node no longer caches a fold of its axes and groups. Its `resolved/dims/` and `resolved/groups/` hold copies of the deepest layer file at or below the node, so a read still stops there. `resolved/dead/` and `owner_map.parquet` stay folds, since no layer file holds either.

### Removal

A label is removed at a layer whose restatement of its axis omits it while the previous restatement holds it. The previous restatement is the deepest file for the axis below that layer, or else a materialised base's resolved axis or the frozen prefix's shared table. A first restatement removes nothing.

For a `partial` dim or group, which the owner map keys, `fold_attributes` drops the parent rows naming a removed label. A `mortal` coordinate (`resolve/coords.py`) is one outside the owner-map key, or one a group's field ranges over or a property refers to. Its dead labels ([a label outside the key](../read-path.md#a-label-outside-the-key)) are the labels some layer removed that the final axis lacks. The fold computes them as the base's dead set, the base's resolved axis and every restatement above the base, minus the final axis. `_require_living_references` takes the labels a layer kills from the same diff, its parent's axis minus its restatement.

In every case probed on a prototype, the rule gives today's answers. A keyed label's values from below its removal stay gone after it is added again, and an unkeyed coordinate's come back, as [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) accepted. A label removed and added again in one staging area keeps its parent's values, and a broadcast reaches a label added after a restatement.

`WorkingRecord.remove` restates the axis without the label, and each group with a field over its dim without the tuples naming it ([add / remove](../working-record.md#add-remove)).

### Order

An axis's order is its deepest file's row order. A label added again after its removal is a row of a later restatement and sits where that file puts it, which subsumes the rule DEP 19 landed.

`WorkingRecord` composes a restatement from its base's resolved axis or group: base order, each staged row in its label's place, removed labels dropped, new labels appended in staging order. `write_layer` refuses a restatement whose order on its parent's labels differs from the parent's, naming the first label out of place.

### Refusals

A `dims/` or `groups/` file with a `deleted` column is refused at read and by `write_layer`. A layer that `WorkingRecord` or the PyPSA tool wrote before this DEP thus fails instead of losing every label it did not touch:

```text
dims/bus.parquet carries a `deleted` column; a layer removes a label by
restating its axis without it (.../design/layers/#deletion). Re-export the
record, or flatten it with `ws.write` under the previous release.
```

The fold and `write_layer` refuse a key that occurs twice in a `dims/` or `groups/` file, at one `GROUP BY` per file. A column that is neither the file's key nor a declared property of its subject is refused at read, as `_refuse_extra` in `store/write.py` refuses it at write today. A restatement composed from a base therefore never carries an undeclared column. `deleted` stays a reserved name and a column of the staging tables, where a staged `remove` is still a row.

### The reference check, not yet ruled on

The one point of this DEP not yet ruled on is a stricter reference check. Under it, `_require_living_references` checks each subject a layer restates or whose rows name a label the layer removes, whole and in its final state, against the final dead labels. A reference an ancestor left dangling then fails the next write touching its subject. The promise of [deletion](../layers.md#deletion) that a reference an ancestor left dangling "fails no later write" goes, with its counterpart in that function's docstring. If the check is struck, it covers the rows a restatement adds or changes and the rows naming a label the layer removes, as today, at one more anti-join against the parent per restated subject. No other rule here depends on the choice.

### SubNetworks are a dim

`_ResultsSource` in `tools/pypsa.py` writes this solve's SubNetworks into the entity axis and tombstones the stale ones of `base`. Read as a restatement, that file removes every other entity. A SubNetwork becomes a label of the dim `sub_network`, which a results layer restates alone:

| today                                                                               | becomes                                                                                                                    |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| the `entity` label `0 SubNetwork`, `ctype="SubNetwork"`                             | the `sub_network` label `0`                                                                                                |
| `SubNetwork` listed by the enum dim `ctype`                                         | not listed, so `flags(ctype="SubNetwork")` is refused; `_AXIS_TYPES` maps `SubNetwork` to `sub_network`, as `Bus` to `bus` |
| `entity.carrier` on a SubNetwork                                                    | `sub_network.carrier`, which refers to `carrier`                                                                           |
| `slack_bus`, an attribute over `entity`                                             | over `sub_network`                                                                                                         |
| a Line's `sub_network` value `0 SubNetwork`                                         | `0`, still an attribute over `entity`                                                                                      |
| `_SUB_NETWORK_SUFFIX`, `_record_name`, `_sub_network_named`, `_sub_network_unnamed` | deleted                                                                                                                    |

`sub_network` stays outside `partial`, since a results layer writes every SubNetwork's values. A full export writes the axis beside `bus` and `carrier`, and `build` adds SubNetworks through `_add_labels`.

## Rationale

With one file per axis, no seed, cache or staged fold can disagree with a full fold about order or membership. A layer's axis file says what the axis is at that node. Groups follow the rule because port order is connection order.

Every timing and size in this DEP is a single DuckDB run at PyPSA-Eur scale, 60,000 entities and 150,000 connections, unless stated. With SubNetworks as a dim, a results layer restates only labels a solve derives. Its `sub_network` axis is 12 rows and 0.4 kB, or 18 kB where each of 4,000 buses is its own SubNetwork, as PyPSA makes one per connected component of lines and transformers. The schema change adds no migration of its own, since every record the tool wrote carries `deleted` columns and is re-exported anyway.

Storage grows with the axis rather than the edit. A one-entity property edit writes 0.30 MB where a one-row patch writes 0.8 kB today. Adding a connection writes 0.43 MB, and removing an entity restates both for 0.73 MB. A `connection.role` edit restates every tuple. A chain of 20 edit layers over 40,000 entities and 80,000 connections holds 5.57 MB against 10 kB today. The cost multiplies in scenario trees with many small leaves and on a remote store.

A prototype without the order and duplicate checks wrote that chain in 4.3 s against 5.7 s and read `p_nom` cold at its leaf in 1.07 s against 2.17 s. The entity fold drops from 80-95 ms, whatever the patch count, to a 50 ms read of the deepest file. Each restatement adds a diff to every fold, 6 ms for the entities and 18 ms for the connections. The dead set reads every restatement above the base, 23 ms over 20 of the entities, where today it reads tombstone rows. A duplicate check costs 5-11 ms per file. A `WorkingRecord` with a staged axis or group edit composes the whole restatement per read, about 15 ms for the entities and 41 ms for the connections, estimated from their write times.

A tree the PyPSA tool did not write is refused where a `dims/` or `groups/` file carries a `deleted` column, as every axis or group edit of `WorkingRecord` writes. It is flattened with `ws.write` under the release before this one, losing its history. A full PyPSA export written as a child is refused where its parent's entity order interleaves types, as after a `WorkingRecord` adds a generator, since an export orders entities by type. It is written as a root instead. A foreign directory meets the same refusals, so not every standard parquet directory reads as a layer any more. A hand-written restatement that omits a property column blanks it for every label, where a patch blanked it only for the labels it held. `src` grows, by an estimated 100-130 lines deleted and 135-235 added.

## Usage

```python
w = WorkingRecord(rev.record)
w.add("bus", ["z"])
child = rev.child(w.edits)
rev.store.layer(child.id).dims["bus"]  # every bus of rev, in its order, then z

w = WorkingRecord(child.record)
w.remove("entity", ["old_coal"])  # entity.parquet without old_coal,
child.child(w.edits)  # connection.parquet without its tuples
```

A layer written by hand holds the whole axis, properties included:

```python
# before: one row
pd.DataFrame({"bus": ["z"]}).to_parquet(f"{layer}/dims/bus.parquet")
# after: the parent's buses with their properties, in their order, then z
buses = parent.record.properties["bus"].df()
new = pd.DataFrame({"bus": ["z"]})
pd.concat([buses, new], ignore_index=True).to_parquet(f"{layer}/dims/bus.parquet")
```

A solve's SubNetworks are labels of their own dim:

```python
solved = inputs.child(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
)
solved.record.dims["sub_network"]  # this solve's; the entity axis is still inputs'
solved.record.attributes["slack_bus"]  # over sub_network
```

## Implementation notes

Paths are under `src/datarecord/`.

1. `duck.fold_axis` reads the deepest relation and refuses a duplicate key, and `duck.dead_keys` computes the dead set, dropping an empty one so that `_living` adds no join. `resolve_coords` and `resolve_groups` in `resolve/coords.py` take the deepest file, and `_resolve_dead` adds the base's resolved axis. In `resolve/owner_map.py`, the removed labels replace `_deleted_relation` in `fold_attributes`, `_fold_map` and `_table`. `_FileLayer.axis` and `group` in `resolve/sources.py` refuse a `deleted` column, then an undeclared one.
2. In `store/write.py`, `_validate_frame` drops `deleted` from an axis or group file's columns. A duplicate check and the order check run on each written `dims/` and `groups/` file beside `_refuse_unordered`. `_require_living_references` derives the killed labels from the parent's axes, and `_living` goes.
3. In `mutable.py`, `_axis_layer` loses its `partial` branch and, with `_collapsed_group`, composes the restatement `StagedSource` hands over.
4. In `tools/pypsa.py`, `_connection_rows`, `_NetworkSource.dims`, `_carriers`, `_entity_frame` and `_tagged` stop writing `deleted`. `_AXIS_TYPES` and `_PROPERTIES` gain `sub_network`, whose `carrier` `_registry` declares explicitly, since it takes no property from an output. `_ResultsSource` writes the `sub_network` axis, `_restated` filters stale rows on `sub_network` instead of `entity`, and the suffix helpers go.
5. In `tests/fixtures.py`, `write_component`, `tombstone`, `write_connections`, `tombstone_connection`, `write_axis`, `write_scenarios` and `write_periods` restate from the layer's parent, and `write_connections` stops writing the undeclared `role` and `scenario`. By a prototype's failures adjusted for this design, at least 18 tests change. Ten write a patch child through those fixtures, and two assert patch-shaped files. `test_sub_networks_are_suffixed_off_the_lines` and `test_a_re_solve_leaves_no_stale_sub_network` read `sub_network`, and four tests write or assert a `deleted` column. New tests pin the refusals, the order check, and one order across a materialised parent.
6. `CHANGELOG.md` under `[Unreleased]`: a layer restates a changed axis or group whole, a `deleted` column is refused, and SubNetworks are labels of `sub_network`.
7. On landing, update the design pages [read path](../read-path.md), [layers](../layers.md), [record](../record.md), [format](../format.md), [schema](../schema.md), [working record](../working-record.md), [tools](../tools.md) and [writing](../writing.md), wherever they describe tombstones, per-label folding, first-introduced order or SubNetwork entities, and the usage pages on editing, tools and the schema.
8. On implementation, DEP 1, 4, 10, 11, 17 and 19 take `Superseded-By` this DEP, for the parts named under `Replaces`.

Checks: a label removed and re-added reads in one order materialised or not, staged or committed, with `connection` in `partial` and out. No file `write_layer` writes under `dims/` or `groups/` has a `deleted` column.

## Rejected Ideas

The 7-line fold fix, now in `fold_axis` as the rule DEP 19 landed, fixes the order bug without a format change but keeps the per-label fold. With it stay the tombstones, `partial`'s second meaning and an order derived from history.

A format version in `manifest.json` would also catch an old patch layer without a `deleted` column, at the cost of a second version beside `schema.version`.

Restating the entity axis in each results layer writes 0.30 MB per solve in 20 ms, plus 10 ms to check its order. That is small beside one `marginal_price` result over 4,000 buses and 8,760 snapshots, 301 MB with random values, an upper bound. It is rejected because every solve would rewrite an input axis it never changed and, under the stricter reference check, fail on a reference an ancestor left dangling there.

Sorting an out-of-order restatement into its parent's order, instead of refusing it, would bring the per-label fold back at write time.

Dropping undeclared columns when composing a restatement, instead of refusing them, would lose a foreign column silently at the first edit of its axis.

Pruning the owner map against each layer's restatement drops a broadcast's rows for labels added later. Pruning against the final axis alone brings back values from below a removal.

Dropping the dead cache and keeping the rows whose label is in the final axis makes series reads 4.4 times slower, 78 ms against 18 ms on 43.8M `p_max_pu` rows. It also silently drops rows naming a label no axis holds, which `set` accepts on a dim outside `partial`.

Recording in `resolved/` which layer holds each axis and group, instead of copying the file, would save about 0.73 MB per materialised node. A read at that node would then open an ancestor's layer file, where [materialised node caches](../layers.md#materialised-node-caches) has it stop at the node.

## Open Issues

A patch layer written before this DEP without a `deleted` column still reads silently as a restatement removing every label it omits. Such a layer was written by hand, as `write_axis` does, or from a `LayerData` whose axis lacked the column.

The order check allows a new label before one of its parent's. In a group, a new tuple placed before an existing one of the same entity renumbers that entity's later ports. `WorkingRecord` appends, so only a layer written another way can do this.

The stricter reference check is not yet ruled on.

[DEP 14](dep-0014-combining-records.md)'s open issue, extending a tree without restating its base, gets no easier.
