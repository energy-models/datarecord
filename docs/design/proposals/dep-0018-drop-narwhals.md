<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 18: drop narwhals

| DEP      | 18                                           |
| -------- | -------------------------------------------- |
| Title    | drop narwhals                                |
| Author   | Jonas Hoersch                                |
| Status   | Draft (2026-10-20)                           |
| Requires | [DEP 17](dep-0017-results-are-attributes.md) |
| Created  | 2026-10-20                                   |
| Version  | Unreleased                                   |

## Abstract

narwhals leaves datarecord, core and tools alike. DuckDB is the one query engine. A record answers bare `DuckDBPyRelation`s, and the write path takes relations and pandas, polars or arrow frames. Dtypes are mathspec's `DimensionDtype` and `ParameterDtype`, stored as plain strings, with one fixed DuckDB type map.

## Motivation

Every record is DuckDB underneath. narwhals is a wrapper a reader passes through on the way:

- **The read path.** `Record` resolves a relation, then wraps it with `nw.from_native`. A caller wanting SQL unwraps it again with `to_native()`.
- **The write path.** `set` turns any frame into narwhals, then into a relation (`as_relation`). Two conversions do the work of one DuckDB scan.
- **Private API.** `duck.py` imports `narwhals._duckdb.utils` and `narwhals._utils.Version` for its type translation. The `>=2,<3,!=2.21.0` pin guards that.
- **Two dtype vocabularies.** The manifest stores narwhals class names (`"Float64"`, `{"Datetime": "us"}`). A mathspec model declares `float`, `int`, `bool`, `str` and `datetime`. Every consumer translates between them.
- **A wider vocabulary than any consumer reads.** `Date`, number widths and time zones are expressible. Nothing downstream distinguishes them.
- **Tools.** `tools/base.to_relation` exists because narwhals has no `PIVOT`. The PyPSA tool wraps every pandas frame in narwhals to hand it back.

The footprint is 175 lines in 9 files under `src/`, and 219 lines in 13 test files.

## Specification

### Dtypes

A declared dtype is a string from mathspec. `Dimension.dtype` is a `DimensionDtype`. `AttributeSpec.dtype` is a `ParameterDtype`. `PropertySpec.dtype` is a `ParameterDtype` or the [local enum](#the-local-enum). A property `into` a dim takes that dim's dtype, as today.

| mathspec dtype | declared by              | DuckDB type                |
| -------------- | ------------------------ | -------------------------- |
| `float`        | dim, attribute, property | `DOUBLE`                   |
| `int`          | dim, attribute, property | `BIGINT`                   |
| `bool`         | attribute, property      | `BOOLEAN`                  |
| `str`          | dim, attribute, property | `VARCHAR`                  |
| `datetime`     | dim, property `into` it  | `TIMESTAMP` (no time zone) |
| local enum     | property                 | `ENUM` of its categories   |

The map is one constant in `duck.py`, read wherever a column is typed. `Date`, `Int32`, `Float32` and time zones are gone.

**Struct columns are derived, never declared.** A group's column is a `STRUCT` with a field per key coordinate, each typed as its dim. An owner flag is a `STRUCT` of `BOOLEAN`. The fixed columns stay fixed: `attribute VARCHAR`, `breakpoint DOUBLE`, `deleted BOOLEAN`, `breakpoints BOOLEAN`.

### The manifest

A dtype is written as its mathspec string: `"dtype": "float"`. An enum is written `"dtype": {"enum": ["Generator", "Line"]}`.

An old manifest fails at load, naming the replacement:

```text
dimension 'snapshot' declares dtype {"Datetime": "us"}, a narwhals name;
write "datetime" (one of float, int, bool, str, datetime)
```

`String`, `Float64`, `Int64`, `Boolean`, `Datetime` and `Enum` map to `str`, `float`, `int`, `bool`, `datetime` and `{"enum": [...]}`. `Date` has no replacement and names `datetime`.

### The local enum

`ctype` and `role` stay enum dtypes. mathspec has no enum yet ([energy-models/mathspec#519](https://github.com/energy-models/mathspec/issues/519)). Until it lands, datarecord exports one `Enum`:

```python
class Enum(BaseModel, frozen=True):
    categories: tuple[str, ...]
```

It is the only dtype datarecord defines itself. Only a property declares it, as today only `categorizing` reads it. Once #519 lands and the pin is bumped, mathspec's enum replaces it, with its manifest spelling.

### The read API

The mappings stay; their values are relations. `record.dims[name]`, `record.properties[subject]`, `record.groups[name]` and `record.attributes[name]` are each a `duckdb.DuckDBPyRelation` on the record's connection.

| today                                      | becomes                                                 |
| ------------------------------------------ | ------------------------------------------------------- |
| `Frames = Mapping[str, nw.LazyFrame]`      | `Relations = Mapping[str, DuckDBPyRelation]`            |
| `LazyFrames`                               | `LazyRelations`, built on `__getitem__` as before       |
| `layered/record.py`'s `Record`             | `Record` in `record.py`, the one resolver-backed record |
| `nw.from_native(resolver.attribute(name))` | `resolver.attribute(name)`                              |
| `frame.to_native()` in a caller            | gone                                                    |

`RecordLike` is no longer engine-neutral. A tool's record builds its relations on the caller's connection. The PyPSA `_NetworkSource` scans its pandas frames with `con.from_df`.

### The write API

`set`, `add` and `add_group` take a DuckDB relation, or a pandas, polars or arrow frame. A frame reaches DuckDB through its own scanner, without collecting a relation. A relation on another connection is refused, as today. A scalar stays a scalar.

A column is checked against the DuckDB type its dtype maps to. A lossless widening (`INTEGER` to `BIGINT`) is cast. Anything else is refused, naming the declared dtype.

`check_group_columns` takes a relation and runs in SQL. It reads `rel.columns` and `rel.types`. The partial-NULL check is one `LIMIT 1` query.

### Snapshot timesteps

A property declares no `datetime` dtype, since mathspec's `ParameterDtype` has none. So PyPSA's `snapshot.timestep` becomes a property `into` a new `datetime` dim, `timestep`. Its labels are the timesteps a multi-period network uses.

## Rationale

**One engine, one representation.** The resolver already returns relations. Handing them over bare removes a wrap per read and a conversion per write.

**mathspec owns the vocabulary.** A record feeds a mathspec model, which declares five dtypes. Storing those five keeps the manifest and the model in one language. A wider vocabulary bought distinctions nobody read.

**A fixed map over a translation library.** Six entries replace a private narwhals import. They change only when mathspec does.

**Struct columns derive from the schema.** A group's struct follows from its key. Declaring it would state the key twice.

**Costs.** A caller reading `record.attributes[...]` with narwhals or polars expressions rewrites them as SQL or relation calls. A non-DuckDB backend for `RecordLike` is no longer possible. A record declaring `Date` or a number width is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). PyPSA records gain a `timestep` dim.

## Usage

Before:

```python
Dimension(dtype=nw.Datetime(), description="A snapshot.")
AttributeSpec(dtype=nw.Float64(), dims={"entity"})
PropertySpec(dtype=nw.Enum(["Generator", "Line"]))

p_nom = record.attributes["p_nom"]
p_nom.filter(nw.col("value") > 0).collect().to_pandas()
rel = p_nom.to_native()
```

After:

```python
Dimension(dtype="datetime", description="A snapshot.")
AttributeSpec(dtype="float", dims={"entity"})
PropertySpec(dtype=Enum(categories=("Generator", "Line")))

p_nom = record.attributes["p_nom"]
p_nom.filter("value > 0").df()
w.set("p_nom", polars_frame)  # or pandas, arrow, a relation
```

## Implementation notes

1. `schema.py` (large): dtypes become mathspec strings and `Enum`; `_BARE_DTYPES`, `_dump_dtype` and `_parse_dtype` go; old names are refused at load; `group_type` and `flag_type` return DuckDB structs; `check_group_columns` runs in SQL.
2. `duck.py` (small): the type map replaces `DuckTypes`' narwhals call; `as_relation` takes relations and pandas, polars or arrow frames.
3. `record.py` and `layered/record.py` (medium): `Relations` and `LazyRelations`; `Record` moves into `record.py`, `layered/record.py` goes.
4. `layered/resolve.py`, `layered/write.py` (small): defaults and empty columns read the type map; `nw.from_native` goes.
5. `mutable.py` (medium): `_incoming` becomes `as_relation`; column checks compare DuckDB types; label and group selection are relation filters.
6. `tools/base.py` (small): `to_relation` goes; `to_datarecord` returns relations on `con`.
7. `tools/pypsa.py` (large): frames scanned on `con`; `_DTYPES` and `_column_dtype` return mathspec strings; `snapshot.timestep` is `into` a `timestep` dim.
8. `pyproject.toml`, `pixi.toml` (small): narwhals goes; `pixi lock`.
9. Tests (large): fixtures declare string dtypes; narwhals expressions become relation calls; an old manifest is refused.
10. Pages: [schema](../schema.md), [format](../format.md), [record](../record.md), [read path](../read-path.md), [working record](../working-record.md), [writing](../writing.md), [tools](../tools.md), [module layout](../module-layout.md), the usage pages, `CHANGELOG.md`.

Checks: no `narwhals` in `src/`, `pyproject.toml` or `pixi.toml`; a manifest with `"Float64"` is refused naming `float`; every `record.*[...]` value is a `DuckDBPyRelation`; `set` takes a polars, pandas and arrow frame; a PyPSA network round-trips.

## Rejected Ideas

- **Keeping narwhals in tools only**: two value types for one record, and the conversion stays.
- **DuckDB type names in the manifest**: an engine's vocabulary where mathspec's is the consumer's.
- **A declared struct dtype**: it would restate the group's key.
- **Keeping `datetime` on properties**: mathspec's `ParameterDtype` refuses it, so the model could not read it.

## Open Issues

- **The enum's form once mathspec#519 lands.** Recommend adopting mathspec's spelling verbatim, and refusing `{"enum": [...]}` at load naming it.
- **Whether an attribute may declare an enum.** Recommend no, until a consumer reads one.
- **A time-zone-aware frame passed to `set`.** Recommend refusing it, not converting silently to naive time.
- **The pandas, polars and arrow scanners need those packages.** Recommend no new dependency; DuckDB's own import error names the missing one.
- **The `timestep` dim's name in PyPSA records.** Recommend `timestep`, PyPSA's own index level name.
