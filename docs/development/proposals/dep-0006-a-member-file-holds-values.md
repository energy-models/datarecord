<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 6: a member file contains values, and nothing else

| DEP           | 6                                               |
| ------------- | ----------------------------------------------- |
| Title         | a member file contains values, and nothing else |
| Author        | Jonas Hörsch                                    |
| Status        | Superseded (2026-10-13, implemented 2026-09-04) |
| Created       | 2026-09-02                                      |
| Version       | v0.1.0a1                                        |
| Superseded-By | [DEP 12](dep-0012-properties.md)                |

The design landed in [the record format](../../reference/format.md) and [the schema](../../reference/schema.md). Where they and this DEP differ, follow those pages. [DEP 12](dep-0012-properties.md) makes the entity-type axis an `Enum` property of `entity`. A record with categories and one without then share one entity-axis layout with no member files.

## Abstract

A member file, `dims/entity_type/<Type>.parquet`, has a type's attribute values and nothing else. Where the schema declares no entity-type axis, a non-varying attribute over `entity` is a column of `dims/entity.parquet`, and no member files exist. In every record, `deleted` leaves the member file and stays on the entity axis alone.

## Motivation

A schema may omit the entity-type axis, which makes the type labels data. Values over `entity` alone still land in one member file per label, so a label the schema never declared becomes a filename:

```text
dims/entity.parquet             entity, entity_type, deleted
dims/entity_type/thing.parquet  entity, p_nom
dims/entity_type/other.parquet  entity, p_nom
```

The `entity_type` column there matches no declaration. A special case in `_validate_frame` admits it and a fallback in `_axis_columns` types it as `String`. Every member file also has `deleted`, so `remove` writes a tombstone twice, to the axis and to the member table.

Dropping the column alone does not fix this. `entity_types()`, `entity_type_frame` and `attributes_of` all read the label off the resolved entity axis, so a column of NULLs would make `entity_types()` return `{None}`. Deriving the label from filenames needs a listing per layer per fold, which the entity axis exists to avoid.

## Specification

- Where the schema declares no entity-type axis, which is `entity_type_dim is None`, a non-varying attribute over `entity` is a column of `dims/entity.parquet`. `dims/entity_type/` does not exist, and `attributes_on("entity")` returns those attributes.
- In that case the entity axis a source supplies is `(entity, deleted, *attrs)`, with no `entity_type` column. The materialised `resolved/dims/entity.parquet` has none either.
- `entity_types()` returns the empty set. `entity_type(ctype)` raises. `attributes_of(ctype)` scopes to the whole axis.
- A producer with no types supplies one `dims["entity"]` frame with the member columns merged in, and no `entity_types` mapping.
- A member file has `entity` and its type's columns, and no `deleted`. `remove` writes one row, on the axis.
- A schema that declares the axis keeps the per-type layout unchanged.
- `_validate_frame`'s special case and the `String` fallbacks in `_axis_columns` go.
- `ResolvedLayer`'s `axis` and `axes` become layer-local first, as [DEP 8](dep-0008-one-interface-source-and-fold.md) argues. This DEP depends on that split.

## Rationale

Both changes follow from one claim: a member file has values, and the entity axis has everything about a component's existence. Every column of the format then matches a declaration, and `entity_type` appears only where it has a dtype. A record with no kinds has one axis file, not one file per label an `add` used. `Record.entity_types` stops reporting filenames as a vocabulary.

`_FileLayer`, `ParquetLayer` and `DirectorySource` need no change, because they read whatever columns a file has. A file-backed source reads both configurations the same way. The PyPSA tool always has types, so it is unaffected in practice.

The design adds a second layout for one construct, keyed on one schema property. It replaces the `entity_type` special case and does not add to it. `attributes_on("entity")` changes meaning, so every caller needs an audit. Every record without a type axis is re-exported. A materialised cache written under the old layout needs a rebuild.

## Implementation notes

- `tests/test_untyped.py` asserts that `record.entity_types` is empty and reads values off `dims["entity"]`.
- A written untyped layer has no `dims/entity_type/` directory.
- An `entity_type` column on the entity axis of an untyped record is refused.
- A materialised node of an untyped record reads the same as an unmaterialised one. This test lands before either change.
- The resolved entity axis and the materialised file have no `entity_type` column. Checking only `entity_types() == set()` would pass with a column of NULLs.

## Open issues

- Whether a declared entity-type axis typed `String`, with no `Enum`, is the same case. The build keys on `entity_type_dim is None`, so such a schema keeps the per-type layout.
- Whether `attributes_for` narrows at all without types.
- Whether a materialised cache has a schema version or is invalidated when the schema changes. This is still unsettled.
