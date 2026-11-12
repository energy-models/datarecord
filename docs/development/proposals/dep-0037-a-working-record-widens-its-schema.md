<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 37: a working record widens its schema

| DEP      | 37                                                                                                     |
| -------- | ------------------------------------------------------------------------------------------------------ |
| Title    | a working record widens its schema                                                                     |
| Author   | Bryn Pickering                                                                                         |
| Status   | Draft (2026-11-02)                                                                                     |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md) |
| Created  | 2026-11-02                                                                                             |

## Abstract

`WorkingRecord.declare(name, spec)` adds a dim, a property or an attribute to the schema of a working record, or widens an attribute it declares, so that a `set` or `add` of the name follows in the same revision. `declare` accepts the changes that [`compatible_with`](../../reference/schema.md#compatibility) accepts, except a change to `partial`. `store.commit(w)` writes the layer under the widened schema, which the write accepts already. `set` of a multi-dim's property gains a frame form, so a declared property of `snapshot` takes one value per snapshot.

## Motivation

A tool cannot add a name to an existing record through the API. A `WorkingRecord` reads and commits under its base's schema ([one schema per record](../../reference/schema.md#one-schema-per-record)), so `w.set("snapshot.seg", ...)` fails in `Schema.resolve` with `KeyError: the schema declares no property 'snapshot.seg'`.

The write path takes a wider child. `Claim.commit` checks the layer's schema with `compatible_with` against the parent's manifest, and the fold reads each layer under its own schema ([a wider schema](../../reference/resolution.md#a-wider-schema)). I committed a layer with a wider schema through `store.commit` for each kind. A property, a dim, a multi-dim, an attribute, and a dim added to an attribute's `dims` each read back, the last with and without the dim in `partial`.

So a caller declares a name today by committing a layer with the wider schema and no data, then edits a working record over it. manta's playbook library does this for each time partition `snapshot.seg`. The property is a `PropertySpec(one_of="timestep")` on the multi-dim `snapshot` over `(period, timestep)`, or `one_of="snapshot"` without investment periods. Each costs two revisions, or one revision and a hand-built `LayerData` of about 40 lines, which restates the whole `snapshot` axis and skips every check `set` makes.

## Specification

### `declare`

```python
WorkingRecord.declare(name: str, spec: Dimension | PropertySpec | AttributeSpec) -> None
```

- The type of `spec` says what `name` declares. A `Dimension` declares a dim, or a multi-dim where it has `over`. An `AttributeSpec` declares an attribute. A `PropertySpec` declares a property, named `subject.name` as `set` takes it. `declare` refuses a dotted name with any other spec, and a plain name with a `PropertySpec`.
- The widened schema is `w.schema` with `spec` under `name`, built through `Schema` validation. So `declare` refuses what a schema on disk is refused for: an undeclared subject or dim, a property of a dim with `one_of`, a name that cannot be a column, a malformed `default_by`.
- A name `w.schema` declares already is redeclared. `declare` refuses it where `wider.compatible_with(w.schema)` returns a reason, and where `spec` equals the declaration. So an attribute's `dims` may gain a dim, and a `default`, `default_by`, `unit` or `description` may change. A `dtype`, a `one_of`, an `over` and a dim of `dims` stay as declared.
- `partial` stays as the base's. A declared dim is outside it, so a `set` along it stages the key's other values as the record reads them.
- After a `declare`, `w.schema` is the widened schema, and every read and `w.edits.schema` use it. Nothing is staged by it. A new dim has no labels, a new attribute has no rows, and a new property reads NULL on every label, or its default. An attribute widened over a dim reads its rows as before, broadcast along the dim.
- `set`, `add` and `remove` then treat the name as any other, with the same checks.
- `rollback()` clears the declarations with the staged rows, so `w.schema` is `base.schema` again.
- `declare` refuses once the record is registered ([registered views](../../reference/format.md#registered-views)), because the views were created under the schema at `register`. Call `unregister`, `declare`, then `register` again. It also refuses a closed record, and one another `WorkingRecord` is staged over, as every edit does.

### A frame for a multi-dim's property

`set("snapshot.seg", frame)` patches the column `seg` on the tuples `frame` lists, one row per tuple, and keeps their other properties. The frame has the multi-dim's coordinates as columns, or its struct column as `dims["snapshot"]` returns it, and the property's column. `set` refuses a tuple the axis as staged lacks, a column the axis has no place for, and the frame beside a coordinate keyword. Today a multi-dim's property takes one scalar for the tuples its keywords select. A value per tuple goes through `add`, which restates each tuple's row whole and so sets NULL on every property the frame omits.

### Writing

`store.commit(w)` writes the layer under the widened schema, into its own manifest, and the commit's check against the parent's schema passes by construction. A declared name that no edit reaches is written as its declaration alone, with no file. The name reaches the child and its descendants, and a sibling branch may declare the same name otherwise. `ws.write(w, uri)` writes the whole record under the widened schema.

### Caches

`declare` rebuilds no table of the base. The fold extends its frozen tables to the wider schema as for a committed child, and plans the staged tail again on the next read.

## Rationale

### One call

The type of `spec` names the kind, and `Schema` keeps one mapping per kind that takes it. Three calls would state the kind twice, in the call and in the spec, and each would carry its own list of refusals. One call has one rule: `declare` accepts what `compatible_with` accepts, so the [compatibility table](../../reference/schema.md#compatibility) is also the table of what `declare` takes.

### A repeated declaration is refused

A `declare` of the spec a name has changes nothing. It is refused rather than ignored, so a second time partition of one name cannot silently reuse the first's column.

### Not `partial`

`partial` is the owner map's key. Adding a dim to it changes which layer owns each value of every attribute over the dim. The fold does this for a committed child by splitting the parent's rows. On a working record, every owner table and every staged owner row would change shape. No tool needs it, because the tool that writes the root sets `partial`. A caller that needs it commits a layer whose schema has it, as today.

### What it costs

- A working record's schema stops being fixed at construction. A caller that read `w.schema` before a `declare` holds the narrower one.
- [One schema per record](../../reference/schema.md#one-schema-per-record) loses "a `WorkingRecord` reads and commits under its base's schema".

## Usage

A time partition, then a scenario dim with an attribute over it, in one revision:

```python
w = WorkingRecord(rev.record)
w.declare("snapshot.seg", PropertySpec(one_of="timestep"))
w.set("snapshot.seg", steps)  # columns period, timestep and seg, one row per snapshot
w.declare("scenario", Dimension(dtype="str"))
w.add("scenario", ["low", "high"])
w.declare("weight", AttributeSpec(dtype="float", dims={"scenario"}))
w.set("weight", {"low": 0.3, "high": 0.7})
w.declare("p_nom", AttributeSpec(dtype="float", dims={"entity", "scenario"}))
w.set("p_nom", 2.0, entity="wind", scenario="high")
child = store.commit(w)
```

The child's manifest declares `snapshot.seg`, `scenario`, `weight` and `p_nom` over `scenario`. `p_nom` at `wind` is 2.0 at `high` and, because `scenario` is outside `partial`, the value `wind` read before at `low`.

`declare` refuses each of these calls, for the reason in its comment:

```python
w.declare("snapshot.seg", PropertySpec(one_of="timestep"))  # declared already
w.declare("p_nom", AttributeSpec(dtype="int", dims={"entity"}))  # dtype float -> int
w.declare("seg", PropertySpec(one_of="timestep"))  # a property is subject.name
w.declare("weight", Dimension(dtype="str"))  # an attribute, redeclared as a dim
```

Each call raises at `declare`, and `w.schema` stays as it was.

## Implementation notes

1. `WorkingRecord` keeps its own schema, which `StagedSource.schema` returns. `declare` in `mutable.py` validates the widened schema, checks `compatible_with` and the registration, sets the schema, creates or widens the staging tables, and drops the per-cursor fold.
2. `set` of a multi-dim's property takes a frame, in `_set_multi_dim_property`.
3. `rollback` restores `base.schema` and the staging tables.
4. Pages: [editing](../../reference/editing.md) gains a `declare` section and the frame form, [the schema](../../reference/schema.md#one-schema-per-record) states what a working record declares, and the how-to on editing gains the example. The [open question](../../about/open-questions.md) on a schema change over registered views takes its answer.
5. `CHANGELOG.md` under `[Unreleased]`: `WorkingRecord.declare` and the frame form of `set`.

Checks that hold once built:

- Each kind declared and set reads back through `w` before the write and through the child after it. The child's manifest declares it.
- A declared name no edit reaches reads empty or NULL, before and after the write.
- A `declare` after an edit of the same subject keeps the staged rows, and reads the new column NULL on them.
- No frozen fold table is rebuilt for a `declare`.
- `set` of a multi-dim's property from a frame keeps the other properties of the tuples it patches.
- Every refusal above fails at `declare` and leaves `w.schema` unchanged, and `rollback()` makes `set` of the name fail as it did before.

## Rejected ideas

- The status quo, a schema-only layer and then the edits, costs two revisions per name. The one-revision form is a hand-built `LayerData` that restates a whole axis to add one column, with none of the checks `set` makes.
- `declare_dim`, `declare_property` and `declare_attribute` state the kind twice and split one rule into three lists of refusals.
- `WorkingRecord(base, schema=wider)` takes any wider schema at once, `partial` included. The caller builds it by hand through a dump, which `declare` removes, and cannot declare after reading the base.
- `declare("snapshot.seg", one_of="timestep")` restates the fields of `PropertySpec` as keywords, a second schema language that drifts from the manifest's.
- `set(name, values, declare=True)` infers the dtype from the values. A frame of snapshot labels infers a datetime where `one_of="snapshot"` is meant, and an all-NULL column infers nothing.
- Declaring on any `set` of an unknown name turns a misspelt property into a new column instead of a `KeyError`.
- Replacing a registered record's views on `declare` lets a client see a view's columns change under it.

## Open issues

- Adding a dim to `partial` on a working record.
- Replacing or adding the views of a registered record on `declare`, which a server that keeps one registered working record per client would need.
- Whether `declare` should bump `version`. No write bumps it today.
