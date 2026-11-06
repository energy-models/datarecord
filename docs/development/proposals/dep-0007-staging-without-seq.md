<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 7: staging without `_seq`

| DEP     | 7                        |
| ------- | ------------------------ |
| Title   | staging without `_seq`   |
| Author  | Jonas Hörsch             |
| Status  | Implemented (2026-09-02) |
| Created | 2026-09-02               |
| Version | v0.1.0a1                 |

The design landed in [`WorkingRecord`](../../reference/editing.md). Where it and this DEP differ, follow that page.

## Abstract

Each edit replaces the staged rows it names, by deleting their keys and inserting the new rows. `_seq` then leaves all four kinds of staging table, with the four folds that read it. A staging table contains the rows the layer will write, and reading one is a table scan.

## Motivation

The staging rule requires every table to be shaped like the file it becomes. No table meets it, because each has a `_seq` that no file has, and three of the four need a fold to mean anything:

| Kind               | What `_seq` does                                  | Folded by             |
| ------------------ | ------------------------------------------------- | --------------------- |
| axis               | orders two partial rows for one label             | `_collapsed_axis`     |
| entity axis        | orders two whole rows for one name                | `_collapsed_entities` |
| `inputs`/`outputs` | orders two rows for one coordinate                | `_collapsed_inputs`   |
| groups             | orders two rows for one `group_key`               | `_collapsed_group`    |
| any long table     | `_CARRIED_SEQ` marks a row as a fill, not an edit | none                  |

## Specification

- An edit deletes the keys it names and inserts the new rows, from the arrow relation `_values_relation` builds, by replacement scan.
- The key per kind is the one the fold partitions on. It is the label for an axis, `entity` for the entity axis, the coordinates for a long file and `group_key` for a group.
- On the entity axis, `remove` replaces the row with a tombstone. A `remove` then an `add` under another type is one row.
- `_seq`, `_CARRIED_SEQ`, `_latest_per` and the four `_collapsed_*` folds go. The tombstone anti-join (a query that keeps the rows with no match in another table) in `_collapsed_inputs` stays. It stays because a tombstone also applies to other attributes' rows, which is a fact across tables.
- `_complete_owned_whole`'s anti-join and the long-row replace key both read `_long_key`.
- Staging is no longer append-only.

```sql
DELETE FROM <table> WHERE <key> IN (SELECT <key> FROM _rows);
INSERT INTO <table> BY NAME SELECT * FROM _rows;
```

## Rationale

The delete removes what `_latest_per` would have discarded, so the answer does not change. `_CARRIED_SEQ` never fires. `_complete_owned_whole`'s anti-join already drops any coordinate the table contains, so a fill and an edit cannot meet on one coordinate. A later `set` on a filled coordinate now deletes the fill first. Deriving both keys from `_long_key` retires the risk that the two key sets only agree.

An axis row's columns are independently editable, and a membership row's are not. Without a fold, staging handles both kinds of row with the same code. Only the data shows which columns you can edit one at a time.

The design has three benefits. Reads become table scans, where `_axis_layer` ran an aggregate, an outer join and a `coalesce` on every commit and staged read. A staged table goes to `write_record` with no fold between. `StagedSource` members return tables.

The design has three costs. First, an edit now deletes staged rows and inserts their replacements, so code that expects staged rows only to accumulate is wrong. Second, each edit runs two SQL statements instead of one. A workflow runs far fewer edits than reads, so the second statement adds little time. Third, a fill can overwrite an edit unless one step prevents it: `_complete_owned_whole` drops from the fill every coordinate that the staging table already contains.

## Implementation notes

- An axis `set` patches its one column in place, with `UPDATE ... FROM` and an `INSERT` for a label no row has yet. This keeps a sibling attribute without completing the row. The proposed step that staged the complete row from the base, `_complete_axis`, was drafted and dropped for it.
- `_axis_layer` stays, reduced to a scan for an axis in `partial` and a union with the untouched base labels for an axis outside `partial`.
- No staging table declares `_seq`.
- Two `set` calls on different attributes of one label commute.
- `test_a_partial_axis_stays_a_patch` pins that a layer's axis in `partial` has only the touched labels.
- `test_a_freed_name_may_be_reclaimed_by_another_type` passes by construction.
- After a second `set` at another coordinate of an attribute over a dim outside `partial`, the first edit's fills survive and the second edit replaces the fill on its own coordinate.

## Rejected ideas

- `INSERT … ON CONFLICT (<key>) DO UPDATE SET`: it needs a primary key, so DDL after `_shape(...).create(name)`, and a column list built per call. Neither is fatal, and it is one statement. Re-evaluate once this shape settles, on whether `_shape` can have a key and stay the single source of a table's shape.
- `INSERT … VALUES`: it turns rows into Python objects.
