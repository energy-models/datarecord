<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 28: a working record declares a property

| DEP      | 28                                   |
| -------- | ------------------------------------ |
| Title    | a working record declares a property |
| Author   | Bryn Pickering                       |
| Status   | Draft (2026-11-02)                   |
| Requires | [DEP 12](dep-0012-properties.md)     |
| Created  | 2026-11-02                           |
| Version  | Unreleased                           |

## Abstract

`WorkingRecord.declare(name, spec)` adds a property to the schema of a working record, so that a `set` of the property can follow. Only a property can be declared, because it is the one widening that changes no key, moves no file and leaves the owner map untouched. `rev.child(w.edits)` writes the layer under the widened schema, which the write already accepts as a compatible widening.

## Motivation

A tool cannot add a property to an existing record, because nothing in datarecord's API declares one. On the [editing page](../use/editing.md#add-remove), a tool that grows a column declares it first, and [schema versioning](../model/schema.md#versioning) accepts the widening. [One schema per record](../model/schema.md#one-schema-per-record) states why no call declares it: "a layer declares nothing, so a working record reads under its base's schema". `WorkingRecord.__init__` hands `base.schema` to the fold, so `set("snapshot.agg_3h", ...)` fails in `Schema.resolve` with `KeyError: "the schema declares no property 'snapshot.agg_3h'"`.

The write path is ready. `Store._write` calls `reconcile_schema`, which writes a layer's schema over the manifest when `compatible_with` returns no reason, and `compatible_with` never returns one for an added property. On a layer written before the property, `with_properties` returns the property as a typed NULL.

manta's playbook library stores time aggregations as properties of `snapshot`. `snapshot.agg_3h`, a `PropertySpec(one_of="snapshot")`, maps each snapshot to the first snapshot of its three-hour block. A record holds one such property per aggregation a user runs, so the PyPSA tool cannot declare them up front. Adding one today takes about 40 lines downstream:

```python
def _widened(schema: Schema, properties: dict[str, PropertySpec]) -> Schema:
    snapshot = {**schema.properties.get("snapshot", {}), **properties}
    widened = {**schema.properties, "snapshot": snapshot}
    return schema.model_validate({**schema.model_dump(), "properties": widened})


@dataclass(frozen=True)
class _SnapshotAxis:  # a LayerData restating the snapshot axis alone
    schema: Schema
    snapshots: DuckDBPyRelation

    def axes(self) -> set[str]:
        return {"snapshot"}

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        return self.snapshots if dim == "snapshot" else None

    def attributes(self) -> set[str]:
        return set()

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return None


frame = base.properties["snapshot"].df()
frame["agg_3h"] = mapping.loc[frame["snapshot"]].to_numpy()
schema = _widened(base.schema, {"agg_3h": PropertySpec(one_of="snapshot")})
rev.child(_SnapshotAxis(schema, ws.con.from_df(frame)))
```

The caller rebuilds the schema through a dump and restates the whole `snapshot` axis in order by hand. It also skips every check `set` makes: the keys, their types, and that each value is a live snapshot.

## Specification

### `declare`

```python
WorkingRecord.declare(name: str, spec: PropertySpec) -> None
```

- `name` is `subject.name`, the form `set` takes for a property. `declare` refuses a plain name, with a message stating that only a property can be declared.
- The widened schema is the base's with `spec` under `subject`, built through `Schema` validation, which refuses each case listed below.
- `declare` refuses a name already declared, by the base or by an earlier `declare` on `w`, whether with the same spec or another.
- After a successful `declare`, `w.schema` is the widened schema. Every read of `w` uses it, and so does `w.edits.schema`.
- Until a `set` reaches a label of `subject`, a read of the property at that label returns NULL, or the default from `default_for`.
- `set`, `add` and `remove` then treat the property as any other, with the same checks.
- `rollback()` clears the declarations together with the staged rows, so `w.schema` is `base.schema` again.

`Schema` validation refuses at `declare` what it refuses in a schema on disk:

- an undeclared subject
- a subject with `one_of`
- a `one_of` naming an undeclared dim or a multi-dim
- a name that cannot be a column, or that collides with its subject's key or coordinates
- a malformed `default_by`

### Writing

`rev.child(w.edits)` writes the layer under the widened schema. `reconcile_schema` then widens the store's manifest, which `compatible_with` accepts by construction. A declared property that no `set` reaches is still written: the schema widens, and a read returns NULL on every label. `ws.write(w, uri)` writes the whole record under the widened schema.

Under [DEP 25](https://github.com/energy-models/datarecord/pull/119), the widened schema goes into the layer's own manifest, and the commit's check against the parent's schema passes for the same reason. The property then reaches the child and its descendants only, where today it reaches every revision of the store. [DEP 26](https://github.com/energy-models/datarecord/pull/120) needs no change for it: a property is not in the owner-map key, so the fold has nothing to extend at the seam.

### Caches

`declare` rebuilds no cached table. The shared fold tables are keyed by revision id and a name, not by schema (`_coord_table`). A `WorkingRecord` reads its base's tables under the base's revision id. A table built before the `declare` lacks the column, and `Record._property_frame` adds it as a typed NULL. The docstring of `Record._coords` already states this for a compatible widening. The owner map is keyed by `partial`, which a property does not touch, so its table is unaffected. A staged axis table of `subject` that exists when `declare` runs gains the column as NULL, as a staging table already grows a column.

## Rationale

### Where the declaration lives

The declaration belongs to the edit that needs it. `w.edits` already has a schema, `StagedSource.schema`, and the write uses it, so a declaration on the working record reaches the write with no new plumbing.

### A `PropertySpec`, not keywords

`declare("snapshot.agg_3h", one_of="snapshot")` would restate the fields of `PropertySpec` as arguments. That is a second schema language beside the manifest's, and it drifts with it. A spec also brings `default`, `unit` and `description` without a keyword each.

### What can be declared

A property adds a column to its subject's axis file and nothing else. An attribute brings `dims`, `default_by`, breakpoints and rows in the owner map. A dim brings an axis, a place in `partial`, which keys the owner map, and, for a multi-dim, its tuples. Each of those is a decision of its own, and the property is the case a tool meets now.

### When a declaration is refused

`declare` refuses a widening that `Schema` would refuse at load. The caller then sees the refusal where they wrote it, and not at the write or the next read. A repeated name is refused, not ignored, so a second aggregation of one name cannot silently reuse the first's declaration.

### What it costs

- A working record's schema stops being fixed at construction. A caller that read `w.schema` before a `declare` holds the narrower one.
- [One schema per record](../model/schema.md#one-schema-per-record) loses "an edit changes neither kind of schema", because a layer written from `w.edits` now widens it.
- Until DEP 25 lands, a property declared on one branch widens every revision of the store, siblings included. Any write that adds an attribute does the same today.

## Usage

With `declare`, the hand-built schema and axis of the motivation become five lines:

```python
w = WorkingRecord(rev.record)
w.declare("snapshot.agg_3h", PropertySpec(one_of="snapshot"))
w.set("snapshot.agg_3h", mapping)  # a Series indexed by snapshot
child = rev.child(w.edits)  # the manifest declares snapshot.agg_3h
child.record.properties["snapshot"]  # agg_3h beside period and timestep
```

The child's manifest declares `snapshot.agg_3h`, and the properties of `snapshot` have the new column beside `period` and `timestep`.

`declare` refuses each of these calls, for the reason in its comment:

```python
w.declare("snapshot.agg_3h", PropertySpec(one_of="snapshot"))  # declared already
w.declare("snapshot.agg_6h", PropertySpec(one_of="snapshots"))  # undeclared dim
w.declare("agg_6h", PropertySpec(dtype="int"))  # a property is subject.name
w.declare("ctype.label", PropertySpec(dtype="str"))  # ctype lists its labels
```

Each call raises at `declare`, and `w.schema` stays as it was.

## Implementation notes

1. `WorkingRecord.declare` in `mutable.py`: validate the widened schema, refuse a declared or plain name, and set `schema`. Where the subject has a staged axis table, add the column to it.
2. `rollback` restores `base.schema`.
3. Pages: [one schema per record](../model/schema.md#one-schema-per-record) on what a working record may declare, [editing](../use/editing.md) gains a `declare` section, and the usage editing page its example.
4. `CHANGELOG.md` under `[Unreleased]`: `WorkingRecord.declare`.

Checks that hold once built:

- A property declared and set reads back through `w` before the write and through the child after it.
- A declared property no `set` reaches reads NULL on every label, before and after the write.
- A declare after an axis edit of the same subject keeps the staged rows and reads the new column NULL on them.
- The base revision's cached tables serve `w` after a `declare`, and no fold table is rebuilt for it.
- Every refusal above fails at `declare`, and leaves `w.schema` unchanged.
- `rollback()` after a `declare` makes `set` of the property fail as it did before.

## Rejected Ideas

- The status quo, a hand-built `LayerData`, costs about 40 lines per tool. It restates a whole axis to add one column, with none of the checks `set` makes.
- `Schema.with_property(name, spec)` removes the round-trip through a dump. The caller still needs a `LayerData` to bring the schema to the write, because a `WorkingRecord` reads under its base's schema.
- `WorkingRecord(base, schema=wider)` takes any wider schema at once, attributes and `partial` included. It opens every question this DEP leaves out, and needs a `compatible_with` check at construction to do it.
- `set(name, values, declare=True)` makes one call both declare and edit, and infers the dtype from the values. A frame of snapshot labels infers a datetime where `one_of="snapshot"` is meant, which loses the check that each value is a live snapshot. An integer column infers an integer where a float is meant, and an all-NULL column infers nothing.
- Declaring on any `set` of an unknown name turns a misspelt property into a new column instead of a `KeyError`.

## Open Issues

- Declaring an attribute: its `dims`, `default_by`, breakpoints and `partial`. With DEP 26 a wider `partial` no longer refolds the ancestry, which leaves the API as the remaining question.
- Declaring a dim or a multi-dim, and whether it joins `partial`.
- Whether `declare` should bump `version`. No write bumps it today.
