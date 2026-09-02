# Proposal: a staged axis row is complete, so nothing folds it

Status: **Draft** · Drafted 2026-09-02

An axis staging table holds partial rows and a `_seq`, and every read folds them: `max_by` per column, an outer join against the base, a `coalesce` per column, and a semi-join for a `partial` dim. Make each `set` write the whole row instead, and all of that goes — the table *is* what the layer will write, and reading it is a table scan.

## What starts it

[Staging](../working-record.md#staging) already holds the rule this proposal finishes: **every table is shaped like the file it becomes.** The axis tables satisfy it for columns and not for rows. One `set` on one attribute stages a row carrying that attribute's column and NULL for its siblings, so two `set` calls on one label are two rows that only mean something once folded together.

That fold is `_collapsed_axis`, and it is the reason for the one collapse mode the staging area has two of:

```sql
SELECT dim, max_by(a, _seq) FILTER (WHERE a IS NOT NULL) AS a, ... GROUP BY dim
```

The `FILTER` is what stops a sibling's NULL from winning; the `max_by` is what makes two `set`s commute. Both exist only because a row is partial. Downstream of it, `_axis_frame` outer-joins the collapsed rows against the base and `coalesce`s per column — because an untouched label is NULL on the staged side and would otherwise read as cleared — and `_axis_layer` semi-joins back to the touched labels for a `partial` dim.

Four constructs, one cause.

## The construct

**A `set` on an axis attribute stages the complete row for each label it touches:** the base's row for that label, with this edit's column replaced. Where a label has no base row, the staged columns are the whole of it, as today.

The base read is already there. `_stage_axis`'s scalar arm reads `self._base.dims.axes.get(dim)` to know which labels exist; the mapping arm does not, and would start — a point lookup per touched label against a folded relation the `NodeCache` already holds.

**A second `set` replaces the row rather than adding one.** Delete the touched labels, insert the new rows, in that order:

```sql
DELETE FROM staged_axis_<dim> WHERE <dim> IN (SELECT <dim> FROM _rows);
INSERT INTO staged_axis_<dim> BY NAME SELECT * FROM _rows;
```

`_rows` is the arrow relation `_values_relation` builds, registered by replacement scan — the same crossing every insert here already makes, and the same `DELETE`-by-scan `_release_from_other_types` uses. Nothing becomes Python objects per row.

The second `set` reads the *staged* row where one exists, not the base's, so the two edits compose exactly as `max_by` made them.

### What goes

| construct | why it existed |
| ---------------------- | -------------------------------------------------------------- |
| `_seq` on axis tables | ordering two partial rows for one label |
| `_collapsed_axis` | folding them per column |
| `_axis_frame` | outer-joining the fold against the base, `coalesce` per column |
| `_axis_layer`'s semi-join | recovering "which labels did this layer touch" |

The last is the neatest: with rows complete and only touched labels present, **the table's rows are the touch record**, which is what a [`partial`](../schema.md#partial-the-granularity-of-an-override) dim wants. `_axis_layer` becomes the table.

`StagedSource.axis(dim)` and `_staged_axes` then both read the table directly, and the two callers that today disagree about scope have nothing left to disagree about.

### The entity axis joins it

`dims/entity.parquet` has a single-label key (`entity`) and its columns are [one indivisible fact](../format.md#the-entity-axis) — a name exists, is of this type, is alive or dead. So it is the *most* natural fit: `remove` then `add` under another type becomes one row by construction, where today `_collapsed_entities` folds whole-row to get the same answer.

That retires the second collapse mode, which is the deeper win. Today `_collapsed_axis` folds per column and `_collapsed_entities` per row, and the difference is real — an axis row's columns are independently editable, a membership row's are not. Under this proposal neither folds, so the distinction stops being a branch and becomes what it always was: a fact about the data, not about the code.

It does change `remove`: from "append a tombstone" to "replace the row with a tombstone". Worth stating deliberately rather than letting it fall out — the semantics are the same because membership is one fact per name, but the mechanism is no longer append-only.

### `inputs/` and `outputs/` keep `_seq`

They are keyed by coordinate *tuples*, not one label, and they do not have the per-column problem: a long row's `value` is the whole of what it says, so `_latest_per` over the full key is already whole-row. `_complete_owned_whole` also leans on `_CARRIED_SEQ` ordering a carried row below every edit's, which has no analogue here.

So this proposal touches the axis tables and the entity axis, and leaves the long path alone. A later one may find the same argument applies; this one does not claim it.

## The alternative to re-evaluate

**`INSERT … SELECT … ON CONFLICT (<dim>) DO UPDATE SET`** is the same idea in one statement, and DuckDB supports it on the arrow-relation form, so the column-wise insert path survives either way.

Two reasons it is not the proposal:

- **It needs a primary key on the staging table.** Tables are created from a relation by `_shape(...).create(name)`, which carries no constraint, so this would mean DDL after create — a second step back for exactly the tables that rule was introduced to make uniform.
- **`DO UPDATE SET` needs its column list built per call**, where `DELETE`-then-`INSERT` is the same two statements whatever the edit names.

Neither is fatal, and a true upsert is one statement rather than two. **Re-evaluate once the shape here is settled** — if the delete-then-insert version lands cleanly, the question is only whether `_shape` can carry a key without giving up being the single source of a table's shape.

## What this buys

**Reads become table scans.** `_axis_layer` is called per axis on every commit and every staged read; today each call is an aggregate, an outer join, a per-column `coalesce` and possibly a semi-join. The work moves to one base lookup per `set`, paid once.

**One collapse mode instead of two.** The `_collapsed_axis`/`_collapsed_entities` split is the last place the staging area folds differently depending on what it is folding.

**The staging rule holds all the way.** "Shaped like the file it becomes" currently means columns; after this it means rows too, and a staged axis table can be handed to `write_record` without a fold in between.

## What it costs

**A `set` reads the base.** The mapping arm gains a point lookup it does not do today. Against it: the scalar arm already does this, and the read is against a relation the `NodeCache` holds.

**Append-only goes.** Staging becomes read-modify-write for axes, so an edit is no longer a pure append. That is what makes the row complete, and it is what a second `set` on one label has always meant — but a reader of the code who assumes "staged rows accumulate" will be wrong for this kind.

**Two statements per `set` where there was one.** Mitigated by both being scans over the same registered relation, and by `set` not being the hot path.

## How to know it worked

**Two `set` calls on one axis still commute**, which is what `_collapsed_axis`'s `FILTER` bought. The existing coverage for it should pass unchanged — if `set("icon", …)` then `set("budget", …)` on one label loses the icon, the row was not complete.

**A `partial` dim's layer carries the touched labels and no others.** `test_a_partial_axis_stays_a_patch` pins this from the outside today; it is the assertion that says the semi-join was replaced rather than dropped.

**A `remove` then `add` under another type is one row**, unchanged from `test_a_freed_name_may_be_reclaimed_by_another_type` — now by construction rather than by folding.

**`_seq` is absent from the axis tables.** Worth asserting directly on the table shape: it is the column whose removal the whole proposal is downstream of, and a `_shape` that still declares it means something still orders rows.
