<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 22: a layer restates membership whole

| DEP      | 22                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a layer restates membership whole                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Status   | On hold (2026-10-22)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Requires | [DEP 19](dep-0019-one-record-and-a-package-layout.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Created  | 2026-10-22                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (entity tombstones, and an order the fold generates), [DEP 4](dep-0004-staging-as-a-layer.md) (the per-label axis fold, and `partial` as how many labels an axis layer carries), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (group tombstones anti-joining the owner map), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (removal by tombstone, and dead labels as winning tombstones), [DEP 12](dep-0012-properties.md) (every property of a dim as a column of `dims/<dim>.parquet`), [DEP 17](dep-0017-results-are-attributes.md) (a SubNetwork as an `entity`, and its tombstones), [DEP 19](dep-0019-one-record-and-a-package-layout.md) (a re-added key introduced where it is re-added) |

## Abstract

DEP 22 is an alternative to [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), and only one of the two is implemented. A layer that adds or removes a label writes the label column of `dims/<dim>.parquet` whole and in order, and a layer that adds or removes a tuple writes the key column of `groups/<group>.parquet` the same way. Properties leave those files for `properties/<subject>.<name>.parquet`, one file per property holding the labels a layer sets it for, folded last writer wins per label and property. A property edit thus writes the rows it changes, and adding or removing a label writes the label column and the touched property rows, where DEP 20 writes every property of every label.

## Motivation

DEP 20 replaces the per-label axis fold with one deepest file per axis or group, which settles order and membership and drops tombstones, and [its motivation](dep-0020-a-layer-restates-an-axis-whole.md#motivation) holds here unchanged. DEP 20 pays with storage that grows with the axis on every edit, properties included. A one-entity property edit writes 0.30 MB under DEP 20, and a `connection.role` edit restates 150,000 tuples.

A property can be bulky. A bus geometry of 64 vertices makes `dims/bus.parquet` 2.49 MB at 4,000 buses, against 17.5 kB for the labels alone, and DEP 20 writes all of it on every bus edit, a geometry fix to one bus included.

Only membership and order need a whole file, since only a whole file says which labels exist and in which order. A property is one value per label, and no read relates one label's value to another's. DEP 22 restates the label column and patches the properties.

## Specification

### Membership is its deepest file

[An axis is its deepest file](dep-0020-a-layer-restates-an-axis-whole.md#an-axis-is-its-deepest-file) holds with narrower files. `dims/<dim>.parquet` holds the label column alone, and `groups/<group>.parquet` the group's struct column alone. A layer that adds or removes a label writes its dim's file whole, every label the axis has at that node, in order, and a layer that adds or removes a tuple writes its group's file whole. A layer that changes only properties writes neither file. A node's membership and order are the deepest such file on the path from the root.

DEP 20's sections [removal](dep-0020-a-layer-restates-an-axis-whole.md#removal), [order](dep-0020-a-layer-restates-an-axis-whole.md#order) and [SubNetworks are a dim](dep-0020-a-layer-restates-an-axis-whole.md#subnetworks-are-a-dim) apply unchanged, except for the staging table described below. A removal is still the diff of a restatement against the previous one, so DEP 20's owner-map pruning and dead labels carry over. [DEP 21](dep-0021-the-owner-map-holds-keys-as-written.md) uses that diff and works over DEP 22 as over DEP 20.

### A property is a file of patches

| file                                  | holds                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------- |
| `dims/<dim>.parquet`                  | the label column, every label in order, written whole                              |
| `groups/<group>.parquet`              | the struct column, every tuple in order, written whole                             |
| `properties/<subject>.<name>.parquet` | the subject's key column and the column `<name>`, one row per label the layer sets |
| `attributes/<attr>.parquet`           | unchanged                                                                          |

The file name is the property's address ([properties](../schema.md#properties)). `bus.x` lives in `properties/bus.x.parquet`, and `connection.role` in `properties/connection.role.parquet`, keyed by the `connection` struct. A row sets the property for its label, a row holding NULL sets it to NULL, and a label without a row keeps the value a shallower layer gave it. One property per file keeps an unset cell apart from a NULL one with no mask, since a file holds no cell its layer does not set.

A resolved property is, per label, the row of the deepest layer holding one. A resolved axis or group is the membership in its order with every property's fold joined on as a column, the shape `Record.dims`, `Record.properties` and `Record.groups` read today, so neither the API nor `Coords` changes shape. Properties of every subject fold per label, whether or not the subject is in `partial`.

### A removal cuts a label's properties

A property row counts only where it is deeper than every layer that removed its label. The fold takes, per label, the depth of the deepest layer whose diff removes it, from the diffs DEP 20 computes, and drops the label's property rows at that depth and shallower. A label added again after its removal therefore starts with no property and reads only the rows written with or after its re-add, as DEP 20 starts its keyed values empty.

A label removed and added again in one staging area is no removal, since its layer's restatement holds it. `add` stages a row of every declared property of each label it adds, NULL where no keyword names it, so the label reads the properties `add` gave it, as `add` restates a row whole today.

### Refusals

[DEP 20's refusals](dep-0020-a-layer-restates-an-axis-whole.md#refusals) apply. In addition, a `dims/` or `groups/` file with any column beside its key is refused at read and by `write_layer`:

```text
dims/bus.parquet carries columns ['x', 'y']; a property of bus is the file
properties/bus.<name>.parquet of the labels a layer sets it for
(.../design/format/#where-a-value-lives). Re-export the record.
```

A layer written before this DEP with properties in an axis or group file thus fails at read instead of reading as a restatement. DEP 20's silent case, an old patch file read as a restatement, narrows to an old axis file holding labels alone.

`write_layer` refuses a property file of an undeclared property, one with a column beside its key and its property, one holding a key twice, and a row naming a label or tuple outside the layer's resolved membership. A read refuses the first three, and a row naming a non-member falls out of the join onto the membership.

### The reference check

[DEP 20's check](dep-0020-a-layer-restates-an-axis-whole.md#the-reference-check) holds for group tuples, which a restatement holds whole. A property that refers to a dim is checked whole, in its final state, against the final dead labels wherever the layer writes rows of that property or removes a label of the dim it refers to. A layer repairs a dangling value an ancestor left by writing a row for its label, so the strict check costs nothing a patch cannot fix, and an edit of another property never meets the dangling value. `_require_living_references` reads the property's fold over the parents and the layer, instead of rows of the axis file.

### Caches and staging

A materialised node writes `resolved/dims/<dim>.parquet` and `resolved/groups/<group>.parquet` as today: the membership in order, with every property's fold as a column. DEP 20 copies the deepest file there instead. A fold above the node seeds the membership from the key column and each property from its column. A NULL cell there reads as a row holding NULL, which resolves the same as no row. `resolved/dead/` and `owner_map.parquet` are as in DEP 20, and the frozen prefix's shared tables take the same shapes.

`WorkingRecord` stages membership in the table [DEP 20's order](dep-0020-a-layer-restates-an-axis-whole.md#order) describes, narrowed to the key and the position column. The first `add` or `remove` on a dim or group copies the base's resolved labels in, `add` appends a new label at the next position and puts a label of the base back at its base position, `remove` deletes the row, and `StagedSource` hands the table over in position order. A property edit stages one row per label in a table per property, replaced by key, and reads no base row, since no sibling property travels with it. `remove` also deletes the label's staged property rows. A read folds the staged property rows onto the frozen prefix's resolved properties as the deepest layer. No staging table carries `deleted`.

`LayerSource` and `LayerData` gain `properties()`, the `(subject, name)` pairs a layer has a file for, and `property(subject, name)`. `RecordLike.properties` already holds a subject's properties apart from `dims`, so a write takes each of its columns as one property file, and a full export writes every property of every label.

## Rationale

Membership and order are facts about a whole axis, so DEP 20's case for a whole file holds for them. A property is a fact about one label, so a per-label patch loses nothing, and the last writer per label and property decides it as [`set`](../working-record.md#set) already treats it. `partial` keeps the one meaning DEP 20 gives it, attribute ownership: a property never varies along another dim, so it cannot leave the hole `partial` guards against, and every subject's properties are patched per label.

### Added complexity

Every size and time below is a DuckDB run over synthetic data at DEP 20's scale, 60,000 entities with four properties and 150,000 connections with `role`, timed as the median of five, unless stated. Sizes for today and DEP 20 are DEP 20's.

| edit                                                         | today        | DEP 20  | DEP 22                           |
| ------------------------------------------------------------ | ------------ | ------- | -------------------------------- |
| one entity's `carrier`                                       | 0.8 kB       | 0.30 MB | 0.4 kB                           |
| one connection's `role`                                      | one row      | 0.43 MB | 0.4 kB                           |
| add an entity                                                | one row      | 0.30 MB | 0.30 MB, plus 4 rows             |
| remove an entity and its connections                         | one row each | 0.73 MB | 0.72 MB                          |
| edit one bus geometry, 4,000 buses of 64 vertices            | one row      | 2.49 MB | one row                          |
| add or remove a bus, same                                    | one row      | 2.49 MB | 17.5 kB, plus a row per property |
| chain of 20 edit layers, 40,000 entities, 80,000 connections | 10 kB        | 5.57 MB | 3.78 MB, estimated               |

Labels dominate the synthetic axes. The entity labels alone take 297 kB of the whole file's 303 kB, and the connection keys 424 kB of 426 kB. DEP 22 therefore saves nothing over DEP 20 on an add or a removal at this scale, and saves where a layer edits properties or a subject carries bulky ones. The chain alternates removals and property edits, and its DEP 22 figure sums file sizes, a method that gives 5.56 MB for DEP 20's measured 5.57 MB.

A read pays for the split. Assembling the entity axis from its label file and four property files, one of them folded over 20 patch layers, took 37 ms into a table, against 10.5 ms for reading one whole file as DEP 20 does. Folding the same 20 patches onto a materialised node's wide file took 19 ms. The connections with `role` from two files took 42 ms against 8 ms. The cut added 7 ms to a 25 ms fold of `carrier`, and the removal diff is DEP 20's, 6.8 ms for the entities. Today's entity fold takes 80-95 ms by DEP 20's measure, which fetched rows into Python, where the DEP 22 assembly took 89 ms and one whole file 64 ms.

Staging copies less than DEP 20 does. The first membership edit copies the labels and positions in 4.9 ms for the entities and 8.6 ms for the connections, against DEP 20's estimate of 15 ms and 41 ms for whole rows, and a property edit copies nothing. A read takes the membership table in position order, 5.6 ms and 9.8 ms, and composes no restatement, as under DEP 20.

A reader holds six fold rules where DEP 20 needs four and today six. DEP 20's four are the deepest file as membership and order, removal as a restatement's diff, the owner map dropping rows of a removed label, and dead labels as removed labels the final axis lacks. DEP 22 adds the deepest row per label and property, and the cut of a property row at or below its label's latest removal. Today's six are the last row per label, the tombstone, first-introduced order, the owner map's anti-join on tombstones, dead labels as winning tombstones, and `partial` deciding an axis layer's extent.

| `src/datarecord/` module | beyond DEP 20                                                                                  |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| `resolve/sources.py`     | +30-40: `properties()` and `property()`, and the key-only refusal                              |
| `resolve/coords.py`      | +50-80: a property fold seeded from a base column, the cut, and the join onto the membership   |
| `store/write.py`         | +40-60, −25-35: the property file checks, the reference check on the fold, one file per column |
| `mutable.py`             | +20-40, −20-30: per-property staging tables, `add`'s NULL rows, `_patch_axis` without siblings |
| `store/cache.py`         | 0, −5-15: today's resolved files stay, where DEP 20 copies the deepest file                    |

`src` thus grows by an estimated 140-220 lines and shrinks by 50-80 beyond DEP 20, or 275-455 added and 150-210 deleted against today, where DEP 20 adds 135-235 and deletes 100-130. On top of DEP 20's 18 or more changed tests, the fixtures write each property column as its own file, and the tests asserting a property column in `dims/` or `groups/` assert a property file instead. About eight new tests pin per-label-and-property ownership across two layers, NULL against no row, the cut on a re-add, materialised against unmaterialised properties, and the four property file refusals.

## Usage

```python
w = WorkingRecord(rev.record)
w.set("bus.geometry", shape, bus="b7")
child = rev.child(w.edits)  # properties/bus.geometry.parquet, one row; no dims/

w = WorkingRecord(child.record)
w.add("bus", ["z"], x=1.0, y=2.0)
child.child(w.edits)  # dims/bus.parquet: every bus of child in order, then z;
# properties/bus.x.parquet and bus.y.parquet hold z, the other properties NULL rows
```

A layer written by hand holds the property's rows alone, or the whole label column:

```python
pd.DataFrame({"bus": ["b7"], "x": [3.1]}).to_parquet(
    f"{layer}/properties/bus.x.parquet"
)
buses = parent.record.dims["bus"].df()
new = pd.DataFrame({"bus": ["z"]})
pd.concat([buses, new], ignore_index=True).to_parquet(f"{layer}/dims/bus.parquet")
```

A label added again reads none of its old properties, and `bus.geometry` above stands for a property a schema declares:

```python
# the root sets bus.x = 1.0 on b; a child removes b; a grandchild adds b
grandchild.record.properties["bus"]  # b reads x as NULL
```

## Implementation notes

Paths are under `src/datarecord/`.

1. DEP 20's implementation notes 1, 2, 4 and 6 to 8, with `_fold_axis` reading the key column alone and keeping, per removed label, the depth of its deepest removal.
2. In `resolve/sources.py`, `LayerSource` gains `properties()` and `property(subject, name)`, and `_FileLayer.axis` and `group` refuse a column beside the key.
3. In `resolve/coords.py`, a property fold takes the base's resolved column and each layer's property file, cut by the removal depths, keeps the deepest row per label, and `resolve_coords` and `resolve_groups` join each fold onto the membership.
4. In `store/write.py`, `write_layer` writes and checks a `properties` kind, `_require_living_references` reads a property that refers to a dim from the fold, and `_RecordLikeAsLayerData.axis` loses its join on `properties`.
5. In `mutable.py`, DEP 20's note 3 with the staging table narrowed to the key and the position, a staging table per property, `_patch_axis` without the sibling carry, `add` staging NULL rows, and `remove` deleting the label's staged property rows.
6. In `tests/fixtures.py`, DEP 20's note 5, with each property column written as its own file.
7. `CHANGELOG.md` under `[Unreleased]`: DEP 20's entry, and a property is a file of per-label patches under `properties/`.
8. On landing, DEP 20's page list, with [where a value lives](../format.md#where-a-value-lives) and [properties](../schema.md#properties) placing a property in its own file. Whichever of DEP 20 and DEP 22 is implemented, the other takes `Superseded` with `Superseded-By` naming it.

Checks: DEP 20's checks; a property set by two layers on two labels reads per label; a label added again reads no property from below its removal, materialised or not, staged or committed.

## Rejected Ideas

Keeping properties in the axis file beside the whole label column, NULL on labels a layer leaves alone, reads an untouched cell and a cell set to NULL alike. A mask column per property would tell them apart, at a column per property in every restatement.

One `properties/<subject>.parquet` per subject, holding the properties a layer sets as columns, makes a layer that sets `x` on one bus and `country` on another carry each bus's other value from the parent. A hand-written file of that shape blanks the cell it leaves NULL.

Declaring properties as attributes over their subject reuses the owner map, which keys them per label only where the subject is in `partial`, so `carrier.color` would be restated whole. [DEP 12](dep-0012-properties.md) separates properties from attributes for reasons this DEP leaves standing.

Keeping a re-added label's property rows from below its removal reads an old bus's carrier and geometry on a new bus of the same name.

## Open Issues

[DEP 20's open issues](dep-0020-a-layer-restates-an-axis-whole.md#open-issues) apply unchanged.

A layer holds up to one file per declared property, about 14 for a full PyPSA export, which a remote store pays for in requests, and a read of `properties["bus"]` opens up to six files per layer holding them.

An old axis file holding labels alone, without a `deleted` column, still reads silently as a restatement.
