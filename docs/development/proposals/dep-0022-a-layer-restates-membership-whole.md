<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 22: a layer restates membership whole

| DEP      | 22                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a layer restates membership whole                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Author   | Jonas Hoersch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Status   | On hold (2026-10-22)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Requires | [DEP 19](dep-0019-one-record-and-a-package-layout.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Created  | 2026-10-22                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Version  | Unreleased                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Replaces | [DEP 1](dep-0001-dims-groups-traits.md) (entity tombstones, and an order the fold generates), [DEP 4](dep-0004-staging-as-a-layer.md) (the per-label axis fold, and `partial` as how many labels an axis layer carries), [DEP 10](dep-0010-a-group-is-a-struct-column.md) (group tombstones, by which the fold drops owner-map rows), [DEP 11](dep-0011-deletion-cascades-through-tombstones.md) (removal by tombstone, and dead labels as winning tombstones), [DEP 12](dep-0012-properties.md) (every property of a dim as a column of `dims/<dim>.parquet`), [DEP 17](dep-0017-results-are-attributes.md) (a SubNetwork as an `entity`, and its tombstones), [DEP 19](dep-0019-one-record-and-a-package-layout.md) (a re-added key introduced where it is re-added) |

## Abstract

DEP 22 is an alternative to [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), and only one of the two is implemented. A layer that changes membership writes the key column of `dims/<dim>.parquet` or `groups/<group>.parquet` whole and in order. Each property moves to its own file, `properties/<subject>.<name>.parquet`, which the fold resolves per label, keeping the deepest layer's row. A property edit then writes only the rows it changes.

## Motivation

[DEP 20's motivation](dep-0020-a-layer-restates-an-axis-whole.md) holds here unchanged. DEP 20's cost is storage that grows with the axis on every edit, properties included. A one-entity property edit writes 0.30 MB, and a `connection.role` edit restates 150,000 tuples.

A property can be bulky. A bus geometry of 64 vertices makes `dims/bus.parquet` 2.49 MB at 4,000 buses, against 17.5 kB for the labels alone. DEP 20 writes all of it on every bus edit, including a fix to one bus's geometry.

Only membership and order need a whole file. A property is one value per label, and no read relates one label's value to another's.

## Specification

### Membership is its deepest file

DEP 20's rule that an axis is its deepest file holds with narrower files. `dims/<dim>.parquet` has the label column alone, and `groups/<group>.parquet` the group's struct column alone. A layer that changes only properties writes neither file.

DEP 20's removal, order and SubNetwork sections apply unchanged, except for the staging table. A removal is still the diff of a restatement against the previous one, so DEP 20's owner-map pruning and dead labels carry over. [DEP 21](dep-0021-the-owner-map-holds-keys-as-written.md) uses that diff and works over DEP 22 as over DEP 20.

### A property is a file of overrides

`properties/<subject>.<name>.parquet` has the subject's key column and the column `<name>`, one row per label the layer sets. `bus.x` lives in `properties/bus.x.parquet`, and `connection.role` in `properties/connection.role.parquet`, keyed by the `connection` struct. `attributes/` is unchanged. A row sets the property for its label, NULL included. A label without a row keeps a shallower layer's value. One property per file keeps an unset cell apart from a NULL one with no mask, because a file has no cell its layer does not set.

A resolved property is, per label, the row of the deepest layer that has one. A resolved axis or group is the membership in order, with every property's fold joined on as a column. The API and `Coords` keep their shape. Properties of every subject fold per label, whether or not the subject is in `partial`.

### A removal cuts a label's properties

A property row counts only where it is deeper than every layer that removed its label. The fold takes each label's deepest removal from DEP 20's diffs, and drops its property rows at that depth and above. A label added again therefore starts with no property.

datarecord does not count a label removed and added again in one staging area as a removal. `add` stages a row of every declared property of each label it adds, NULL where no keyword sets it, so the label reads only the properties `add` gave it.

### Refusals

DEP 20's refusals apply. datarecord also refuses a `dims/` or `groups/` file with any column beside its key, at read and in `write_layer`:

```text
dims/bus.parquet carries columns ['x', 'y']; a property of bus is the file
properties/bus.<name>.parquet of the labels a layer sets it for
(.../design/format/#where-a-value-lives). Re-export the record.
```

An old layer with properties in an axis file then fails at read. DEP 20's silent case narrows to an old axis file with labels alone.

`write_layer` refuses a property file that is undeclared, has an extra column, has a key twice, or contains a label outside the membership. A read refuses the first three, and a row that contains a non-member falls out of the join onto the membership.

### The reference check

DEP 20's check holds for group tuples. Where a layer writes rows of a property `one_of` a dim, or removes a label of that dim, the property is checked whole, in its final state, against the final dead labels. One row repairs a dangling value an ancestor left, and an edit of another property does not check that value.

### Caches and staging

A materialised revision writes `resolved/dims/` and `resolved/groups/` with every property's fold as a column, and a fold above it seeds from them. A NULL cell there reads as a row with NULL, which resolves the same as no row.

`WorkingRecord` stages membership as DEP 20 does, narrowed to the key and position. A property edit stages one row per label in a table per property, and reads no base row. `remove` also deletes the label's staged property rows. A read folds the staged property rows onto the frozen prefix's resolved properties as the deepest layer.

`LayerSource` and `LayerData` gain `properties()`, the `(subject, name)` pairs a layer has a file for, and `property(subject, name)`. A write takes each column of `RecordLike.properties` as one property file, so a full export writes every property of every label.

## Rationale

Membership and order are facts about a whole axis, so DEP 20's case for a whole file holds for them. A property is a fact about one label, so a per-label override loses nothing, and `set` already treats it that way. `partial` keeps the one meaning DEP 20 gives it: which layer the owner map assigns an attribute's values to. A property varies along no dim except its subject. So a per-label override cannot make a read take part of one label's value from the layer and the rest from its parent, which is the case `partial` prevents.

Each figure is the median of five DuckDB runs over synthetic data at DEP 20's scale, 60,000 entities with four properties and 150,000 connections with `role`. The sizes before and under DEP 20 are DEP 20's.

| Edit                                 | Before       | DEP 20  | DEP 22                           |
| ------------------------------------ | ------------ | ------- | -------------------------------- |
| one entity's `carrier`               | 0.8 kB       | 0.30 MB | 0.4 kB                           |
| one connection's `role`              | one row      | 0.43 MB | 0.4 kB                           |
| add an entity                        | one row      | 0.30 MB | 0.30 MB, plus 4 rows             |
| remove an entity and its connections | one row each | 0.73 MB | 0.72 MB                          |
| edit one bus geometry (4,000 × 64)   | one row      | 2.49 MB | one row                          |
| add or remove a bus                  | one row      | 2.49 MB | 17.5 kB, plus 1 row per property |
| chain of 20 edit layers              | 10 kB        | 5.57 MB | 3.78 MB, estimated               |

The chain has 40,000 entities and 80,000 connections, and alternates removals and property edits. Its DEP 22 figure sums file sizes, a method that gives 5.56 MB for DEP 20's measured 5.57 MB. Labels dominate the synthetic axes: the entity labels take 297 kB of the file's 303 kB, and the connection keys 424 kB of 426 kB. So DEP 22 saves nothing over DEP 20 on an add or a removal. It saves where a layer edits properties, or where a subject has bulky ones.

What it costs: a read is slower, because it joins the split files. Assembling the entity axis from five files, one folded over 20 layers, took 37 ms into a table against 10.5 ms for one whole file. Folding the same 20 layers onto a materialised revision's wide file took 19 ms. The connections with `role` took 42 ms against 8 ms. The cut added 7 ms to a 25 ms fold of `carrier`. By DEP 20's measure, which fetched rows into Python, the fold before took 80 to 95 ms, the DEP 22 assembly 89 ms and one whole file 64 ms.

Staging copies less. The first membership edit copies labels and positions in 4.9 ms for the entities and 8.6 ms for the connections, against DEP 20's estimated 15 ms and 41 ms, and a property edit copies nothing. A read of the membership table takes 5.6 ms and 9.8 ms.

The fold applies six rules, where DEP 20's applies four and the earlier design's six. DEP 22 adds the deepest row per label and property, and the cut at a label's latest removal.

| Module in `src/datarecord/` | Lines beyond DEP 20  | For                                                                     |
| --------------------------- | -------------------- | ----------------------------------------------------------------------- |
| `resolve/sources.py`        | +30 to 40            | `properties()`, `property()` and the key-only refusal                   |
| `resolve/coords.py`         | +50 to 80            | the property fold from a base column, the cut, the join                 |
| `store/write.py`            | +40 to 60, −25 to 35 | the property file checks, the reference check, one file per column      |
| `mutable.py`                | +20 to 40, −20 to 30 | staging per property, `add`'s NULL rows, `_patch_axis` without siblings |
| `store/cache.py`            | 0, −5 to 15          | the resolved files stay, where DEP 20 copies the deepest file           |

Beyond DEP 20, `src` grows by an estimated 140 to 220 lines and shrinks by 50 to 80. Against the earlier design that is 275 to 455 added and 150 to 210 deleted, where DEP 20 adds 135 to 235 and deletes 100 to 130.

## Implementation notes

1. DEP 20's implementation, with the axis fold keeping each removed label's deepest removal.
2. Sources, the fold, the writer and staging gain property files, as above.
3. Fixtures write each property as its own file, and about eight new tests pin the rules above.
4. Whichever of DEP 20 and DEP 22 is implemented, the other takes `Superseded`, with `Superseded-By` naming it.

Checks: DEP 20's checks. A property set by two layers on two labels reads per label. A label added again reads no property from below its removal, materialised or not, staged or committed.

## Rejected ideas

- Properties kept in the axis file, NULL on labels a layer leaves alone: an untouched cell and a cell set to NULL read alike. A mask column per property would tell them apart, at a column per property in every restatement.
- One `properties/<subject>.parquet` per subject: a layer that sets `x` on one bus and `country` on another carries each bus's other value from the parent. A hand-written file of that shape blanks the cell it leaves NULL.
- Properties declared as attributes: the owner map keys them per label only for a subject in `partial`, so `carrier.color` would be restated whole. [DEP 12](dep-0012-properties.md) separates the two for reasons this DEP leaves standing.
- A re-added label keeping its property rows from below its removal: a new bus would read an old bus's carrier and geometry.

## Open issues

- [DEP 20's open issues](dep-0020-a-layer-restates-an-axis-whole.md) apply unchanged.
- A layer has up to one file per declared property, about 14 for a full PyPSA export. Each file adds requests on a remote store, and a read of `properties["bus"]` opens up to six files per layer.
- An old axis file with labels alone, without a `deleted` column, still reads silently as a restatement.
