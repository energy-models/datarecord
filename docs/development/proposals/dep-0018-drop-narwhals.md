<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 18: drop narwhals

| DEP      | 18                                           |
| -------- | -------------------------------------------- |
| Title    | drop narwhals                                |
| Author   | Jonas Hoersch                                |
| Status   | Implemented (2026-10-20)                     |
| Requires | [DEP 17](dep-0017-results-are-attributes.md) |
| Created  | 2026-10-20                                   |
| Version  | Unreleased                                   |

The design landed in [the schema](../../reference/schema.md), [the record](../../reference/record.md), [editing](../../reference/editing.md), [the format](../../reference/format.md) and [tools](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

datarecord drops narwhals from core and tools, and DuckDB is the one query engine. A record returns bare `DuckDBPyRelation`s, and the write path takes relations and pandas, polars or arrow frames. Dtypes are mathspec's `DimensionDtype` and `ParameterDtype`, stored as plain strings, with one fixed DuckDB type map. A dim may list its labels with `one_of: [...]`. A property may take its labels from a dim with `one_of: <dim>`, which replaces `into`.

## Motivation

Every record is DuckDB underneath, and narwhals wraps it between DuckDB and the caller:

- `Record` wraps a relation with `nw.from_native`, and a caller who wants SQL unwraps it with `to_native()`.
- `set` turns any frame into narwhals and then into a relation: two conversions for one DuckDB scan.
- `duck.py` imports private narwhals modules for its type translation, so datarecord pins narwhals to `>=2,<3,!=2.21.0`.
- The manifest stores narwhals names such as `"Float64"`, and a mathspec model declares `float` and a dimension's `one_of`, so every reader translates.
- Nothing downstream distinguishes `Date`, number widths or time zones.
- `tools/base.to_relation` exists because narwhals has no `PIVOT`.

narwhals takes 175 lines in 9 files under `src/`, and 219 lines in 13 test files.

## Specification

### mathspec

datarecord pins mathspec at `8982c556114992d149a83b3c6f3604c72e569829`, the merge of [energy-models/mathspec#915](https://github.com/energy-models/mathspec/pull/915). There a dimension may write `one_of: [labels]` beside `dtype: str`, and `dtype` defaults to `str`. `DimensionDtype` is `float`, `int`, `str` or `datetime`, and `ParameterDtype` is `float`, `int`, `bool` or `str`. Before a release, the pin moves to the merged commit.

### Dtypes

| Declaration                    | DuckDB type                |
| ------------------------------ | -------------------------- |
| `dtype: float`                 | `DOUBLE`                   |
| `dtype: int`                   | `BIGINT`                   |
| `dtype: bool`                  | `BOOLEAN`                  |
| `dtype: str`                   | `VARCHAR`                  |
| `dtype: datetime`              | `TIMESTAMP` (no time zone) |
| `dtype: str, one_of: [labels]` | `ENUM` of the labels       |
| `one_of: <dim>`                | that dim's type            |

A dim takes every dtype but `bool`, and attributes and properties take every dtype but `datetime`. Only a dim lists labels, and only a property takes `one_of: <dim>`. An attribute takes no `one_of`, as a mathspec parameter takes none. The map is one constant in `duck.py`, read wherever a column is typed. `Date`, `Int32`, `Float32` and time zones go.

A dim that lists its labels has an `ENUM` column, and `add` refuses a label outside the list, naming the list. Only a `str` dim lists its labels, as in mathspec.

A property with `one_of: <dim>` takes its labels from that dim and declares no `dtype`. Its column has the dim's type: an `ENUM` where the dim lists its labels, and `TIMESTAMP` for a `datetime` dim. A stored value that is not NULL is a live label, checked where `into` was checked. The property may be of a dim or a group. mathspec#915 refuses a dimension reference on a parameter, but a datarecord property is a mathspec relation column.

Struct columns are derived and never declared. A group's column is a `STRUCT` with a field per key coordinate, typed as its dim. An owner flag is a `STRUCT` of `BOOLEAN`. The fixed columns stay fixed: `attribute VARCHAR`, `breakpoint DOUBLE`, `deleted BOOLEAN` and `breakpoints BOOLEAN`.

### `into` goes

`PropertySpec.into` and `GroupSpec.into` are removed. `bus.country` `into` `country` becomes `bus.country: {one_of: country}`. A group over `[bus, scenario]` `into` `country` becomes a property `country: {one_of: country}` of that group. A group loses two things that `into` gave it:

- exactly one label per tuple, because the property may be NULL
- a column named after the dim, because the property's name is free and `country` is a convention.

### Categories

`Schema.categorizing(dim)` picks the properties of a dim in `partial` that are `{one_of: <d>}`, where `d` lists its labels. A property `one_of` a dim that lists no labels does not categorise, because its labels are data that a layer adds and removes.

PyPSA's `ctype` becomes a dim `{dtype: str, one_of: [Generator, Line, ...]}`, and `entity.ctype` becomes `{one_of: ctype}`. `connection.role` becomes `{one_of: role}` on a dim `role` that lists `input`, `output` and `attached`.

### The manifest

A dtype is written as its mathspec string, `"dtype": "float"`, and `one_of` as in the table above. An old manifest fails at load, and the error names the replacement:

```text
dimension 'snapshot' declares dtype {"Datetime": "us"}, a narwhals name;
write "datetime" (one of float, int, str, datetime)
```

The error maps `String`, `Float64`, `Int64`, `Boolean` and `Datetime` to `str`, `float`, `int`, `bool` and `datetime`, and `Enum` to `one_of`. For `Date` it names `datetime`, and for `into` it names `one_of: <dim>`.

### The read API

The mappings stay, and their values are relations. Each of `dims`, `properties`, `groups` and `attributes` maps a name to a `duckdb.DuckDBPyRelation` on the record's connection. `Frames` becomes `Relations`, `LazyFrames` becomes `LazyRelations`, and `Record` moves into `record.py`.

`RecordLike` is no longer engine-neutral. A tool's record builds its relations on the caller's connection, and the PyPSA network source scans its pandas frames with `con.from_df`.

### The write API

`set`, `add` and `add_group` take a DuckDB relation, or a pandas, polars or arrow frame. DuckDB reads a frame through the frame's own scanner, uncollected, and no scanner package becomes a dependency. A relation on another connection is still refused, and a scalar stays a scalar.

A column is checked against its dtype's DuckDB type. A lossless widening, such as `INTEGER` to `BIGINT`, is cast, and anything else is refused with the declared dtype named. datarecord refuses a frame with time-zone timestamps and does not convert it to naive time. `check_group_columns` takes a relation and runs in SQL, with the partial-NULL check as one `LIMIT 1` query.

### Snapshot timesteps

A property takes no `datetime` dtype, so PyPSA's `snapshot.timestep` becomes `{one_of: timestep}` on a new `datetime` dim named after PyPSA's index level. Its labels are the timesteps a multi-period network uses. `snapshot.period` becomes `{one_of: period}`.

## Rationale

The resolver already returns relations, so handing them over bare removes a wrap per read and a conversion per write. A record feeds a mathspec model, so the manifest uses mathspec's five dtypes, as the model does.

`into` on a property, `into` on a group and an `Enum` property were three spellings of one idea. `one_of: <dim>` is one, and it is the relation column that a mathspec model reads. A category is then a label that a dim lists, so `ctype` gets an axis file and an order like any dim.

Seven entries in a fixed map replace a private narwhals import, and change only when mathspec does.

What it costs: a caller who reads `record.attributes[...]` with narwhals or polars expressions rewrites them as SQL or relation calls. A non-DuckDB backend for `RecordLike` is no longer possible. A record that declares `Date`, a width, `Enum` or `into` is re-exported, as [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). A functional group loses its single-valuedness guarantee. PyPSA records gain `timestep`, `ctype` and `role` dims.

## Usage

Before:

```python
PropertySpec(dtype=nw.Enum(["Generator", "Line"]))
PropertySpec(into="carrier")
record.attributes["p_nom"].filter(nw.col("value") > 0).collect().to_pandas()
```

After:

```python
Dimension(one_of=["Generator", "Line"])  # the ctype dim
PropertySpec(one_of="ctype")
PropertySpec(one_of="carrier")
record.attributes["p_nom"].filter("value > 0").df()
w.set("p_nom", polars_frame)  # or pandas, arrow, a relation
```

## Implementation notes

1. `pixi.toml`: mathspec pinned at `8982c55`, and narwhals removed from it and `pyproject.toml`.
2. `schema.py` and `duck.py`: mathspec dtypes and `one_of`, the type map, `into` and old names refused at load, and DuckDB struct types.
3. `record.py`: `Relations` and `LazyRelations`, and `Record` moved in from `layered/record.py`.
4. `mutable.py`: `as_relation` for every frame, DuckDB type checks, and `add` refusing a label outside `one_of`.
5. Tools: `to_relation` goes, and the PyPSA tool has `ctype`, `role` and `timestep` dims.
6. Tests, pages and `CHANGELOG.md`, with the group single-valuedness tests deleted.

Checks: no `narwhals` in `src/`, `pyproject.toml` or `pixi.toml`. A manifest with `"Float64"` or `into` is refused with its replacement named. Every `record.*[...]` value is a `DuckDBPyRelation`. A PyPSA network round-trips.

## Rejected ideas

- narwhals kept in tools only: two value types for one record, and the conversion stays.
- DuckDB type names in the manifest: an engine's vocabulary, where mathspec's is the framework's.
- A declared struct dtype: it would restate the group's key.
- `datetime` kept on properties: mathspec's `ParameterDtype` refuses it, so the model could not read it.
- A local `Enum` dtype on properties: mathspec puts the labels on a dimension, and a property list would be a second vocabulary.
- `into` kept beside `one_of`: two spellings of one declaration.

## Open issues

- Recovering single-valuedness for a functional group, where a group property may be NULL. The recommendation is nothing until a reader needs it. A later `required` flag on a property would cover it.
