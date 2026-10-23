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

## Abstract

narwhals leaves datarecord, core and tools alike. DuckDB is the one query engine. A record answers bare `DuckDBPyRelation`s, and the write path takes relations and pandas, polars or arrow frames. Dtypes are mathspec's `DimensionDtype` and `ParameterDtype`, stored as plain strings, with one fixed DuckDB type map. A dim may list its labels with `{one_of: [...]}`, and a property may name a dim with `{one_of: <dim>}`, which replaces `into` on properties and groups.

## Motivation

Every record is DuckDB underneath. narwhals is a wrapper a reader passes through on the way:

- **The read path.** `Record` resolves a relation, then wraps it with `nw.from_native`. A caller wanting SQL unwraps it again with `to_native()`.
- **The write path.** `set` turns any frame into narwhals, then into a relation (`as_relation`). Two conversions do the work of one DuckDB scan.
- **Private API.** `duck.py` imports `narwhals._duckdb.utils` and `narwhals._utils.Version` for its type translation. The `>=2,<3,!=2.21.0` pin guards that.
- **Two dtype vocabularies.** The manifest stores narwhals class names (`"Float64"`, `{"Datetime": "us"}`). A mathspec model declares `float`, `int`, `bool`, `str` and `datetime`, and a dimension's labels with `one_of`. Every consumer translates between them.
- **A wider vocabulary than any consumer reads.** `Date`, number widths and time zones are expressible. Nothing downstream distinguishes them.
- **Tools.** `tools/base.to_relation` exists because narwhals has no `PIVOT`. The PyPSA tool wraps every pandas frame in narwhals to hand it back.

The footprint is 175 lines in 9 files under `src/`, and 219 lines in 13 test files.

## Specification

### mathspec

datarecord pins mathspec at `04911f1bc6bc6bb2fe03e3d7c24db063b8d6ed52`, the head of [energy-models/mathspec#915](https://github.com/energy-models/mathspec/pull/915). There a dimension may write `dtype: {one_of: [labels]}`. `program.Dimension` carries `dtype: DimensionDtype` (`float`, `int`, `str`, `datetime`; `str` where labels are listed) and `members: tuple[str, ...] | None`. `ParameterDtype` is `float`, `int`, `bool`, `str`, with no `one_of`.

A git pin to a PR branch lasts only while the commit exists on GitHub. Before a release, the pin moves to the merged commit.

### Dtypes

| declares                 | dtype                | DuckDB type                |
| ------------------------ | -------------------- | -------------------------- |
| dim, attribute, property | `float`              | `DOUBLE`                   |
| dim, attribute, property | `int`                | `BIGINT`                   |
| attribute, property      | `bool`               | `BOOLEAN`                  |
| dim, attribute, property | `str`                | `VARCHAR`                  |
| dim                      | `datetime`           | `TIMESTAMP` (no time zone) |
| dim                      | `{one_of: [labels]}` | `ENUM` of the labels       |
| property                 | `{one_of: <dim>}`    | that dim's type            |

The map is one constant in `duck.py`, read wherever a column is typed. `Date`, `Int32`, `Float32` and time zones are gone.

**A dim listing its labels is closed.** Its column is an `ENUM`, and `add` refuses a label outside the list, naming the list.

**A property `{one_of: <dim>}` names labels of that dim.** Its column has the dim's type: an `ENUM` where the dim lists its labels, `TIMESTAMP` for a `datetime` dim. A stored value that is not NULL names a live label, checked where `into` is checked today. The property may be of a dim or of a group. This is datarecord's own spelling: #915 refuses a dimension reference on a parameter, but a datarecord property is a mathspec relation column, not a parameter.

**An attribute takes no `one_of`**, neither a list nor a dim, as a mathspec parameter has none.

**Struct columns are derived, never declared.** A group's column is a `STRUCT` with a field per key coordinate, each typed as its dim. An owner flag is a `STRUCT` of `BOOLEAN`. The fixed columns stay fixed: `attribute VARCHAR`, `breakpoint DOUBLE`, `deleted BOOLEAN`, `breakpoints BOOLEAN`.

### `into` goes

`PropertySpec.into` and `GroupSpec.into` are removed. `bus.country` `into` `country` becomes `bus.country: {dtype: {one_of: country}}`. A group `bus_country` over `[bus, scenario]` `into` `country` becomes the group over `[bus, scenario]` with a property `country: {dtype: {one_of: country}}`.

Lost from group `into`:

- **Exactly one label per tuple.** The property may be NULL, so a tuple may carry none.
- **The column named after the dim.** The property's name is free; `country` is a convention, not a rule.

### Categories

`Schema.categorizing(dim)` today picks the `Enum`-typed properties of a dim in `partial`. Under this DEP it picks the properties of such a dim that are `{one_of: <d>}` where `d` lists its labels. A property naming a dim without a list does not categorize: its labels are data a layer adds and removes.

PyPSA's `ctype` becomes a dim `{one_of: [Generator, Line, ...]}`, and `entity.ctype` becomes `{one_of: ctype}`. `connection.role`, an `Enum` of `_ROLES` today, becomes `{one_of: role}` on a dim `role` listing `input`, `output` and `attached`.

### The manifest

A dtype is written as its mathspec string: `"dtype": "float"`, or `{"one_of": [...]}` on a dim, or `{"one_of": "<dim>"}` on a property.

An old manifest fails at load, naming the replacement:

```text
dimension 'snapshot' declares dtype {"Datetime": "us"}, a narwhals name;
write "datetime" (one of float, int, str, datetime, or {"one_of": [...]})
```

`String`, `Float64`, `Int64`, `Boolean` and `Datetime` map to `str`, `float`, `int`, `bool` and `datetime`. `Enum` names a dim `{"one_of": [...]}` and a property `{"one_of": "<dim>"}`. `Date` has no replacement and names `datetime`. A property or group declaring `into` fails at load, naming `dtype: {one_of: <dim>}`.

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

`set`, `add` and `add_group` take a DuckDB relation, or a pandas, polars or arrow frame. A frame reaches DuckDB through its own scanner, without collecting a relation; no scanner package becomes a dependency. A relation on another connection is refused, as today. A scalar stays a scalar.

A column is checked against the DuckDB type its dtype maps to. A lossless widening (`INTEGER` to `BIGINT`) is cast. Anything else is refused, naming the declared dtype. A frame with time-zone timestamps is refused, not converted to naive time.

`check_group_columns` takes a relation and runs in SQL. It reads `rel.columns` and `rel.types`. The partial-NULL check is one `LIMIT 1` query.

### Snapshot timesteps

A property declares no `datetime` dtype, since mathspec's `ParameterDtype` has none. So PyPSA's `snapshot.timestep` becomes `{one_of: timestep}` on a new `datetime` dim, `timestep`, PyPSA's own index level name. Its labels are the timesteps a multi-period network uses. `snapshot.period` becomes `{one_of: period}`.

## Rationale

**One engine, one representation.** The resolver already returns relations. Handing them over bare removes a wrap per read and a conversion per write.

**mathspec owns the vocabulary.** A record feeds a mathspec model, which declares five dtypes and a dimension's labels. Storing those keeps the manifest and the model in one language. A wider vocabulary bought distinctions nobody read.

**One spelling for "labels of a dim".** `into` on a property, `into` on a group and an `Enum` property were three spellings of one idea. `{one_of: <dim>}` is one, and it is the relation column a mathspec model reads. A category is then a label of a closed dim, so `ctype` gets an axis file and an order like any dim.

**A fixed map over a translation library.** Seven entries replace a private narwhals import. They change only when mathspec does.

**Struct columns derive from the schema.** A group's struct follows from its key. Declaring it would state the key twice.

**Costs.** A caller reading `record.attributes[...]` with narwhals or polars expressions rewrites them as SQL or relation calls. A non-DuckDB backend for `RecordLike` is no longer possible. A record declaring `Date`, a number width, `Enum` or `into` is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). A functional group loses its single-valuedness guarantee. PyPSA records gain `timestep`, `ctype` and `role` dims.

## Usage

Before:

```python
Dimension(dtype=nw.Datetime(), description="A snapshot.")
AttributeSpec(dtype=nw.Float64(), dims={"entity"})
PropertySpec(dtype=nw.Enum(["Generator", "Line"]))
PropertySpec(into="carrier")
Group(over=["bus", "scenario"], into="country")

p_nom = record.attributes["p_nom"]
p_nom.filter(nw.col("value") > 0).collect().to_pandas()
rel = p_nom.to_native()
```

After:

```python
Dimension(dtype="datetime", description="A snapshot.")
Dimension(dtype={"one_of": ["Generator", "Line"]})  # the ctype dim
AttributeSpec(dtype="float", dims={"entity"})
PropertySpec(dtype={"one_of": "ctype"})
PropertySpec(dtype={"one_of": "carrier"})
Group(over=["bus", "scenario"])  # with property country: {"one_of": "country"}

p_nom = record.attributes["p_nom"]
p_nom.filter("value > 0").df()
w.set("p_nom", polars_frame)  # or pandas, arrow, a relation
```

## Implementation notes

1. `pixi.toml` (small): mathspec pinned at `04911f1`; narwhals goes from it and `pyproject.toml`; `pixi lock`.
2. `schema.py` (large): dtypes become mathspec strings and `one_of`; `_BARE_DTYPES`, `_dump_dtype` and `_parse_dtype` go; `PropertySpec.into` and `GroupSpec.into` go, with their checks; old names and `into` are refused at load; `categorizing` reads `one_of`; `group_type` and `flag_type` return DuckDB structs; `check_group_columns` runs in SQL.
3. `duck.py` (small): the type map replaces `DuckTypes`' narwhals call; `as_relation` takes relations and pandas, polars or arrow frames.
4. `record.py` and `layered/record.py` (medium): `Relations` and `LazyRelations`; `Record` moves into `record.py`, `layered/record.py` goes.
5. `layered/resolve.py`, `layered/write.py` (small): defaults and empty columns read the type map; `nw.from_native` goes.
6. `mutable.py` (medium): `_incoming` becomes `as_relation`; column checks compare DuckDB types; label and group selection are relation filters; `add` refuses a label outside a dim's `one_of`.
7. `tools/base.py` (small): `to_relation` goes; `to_datarecord` returns relations on `con`.
8. `tools/pypsa.py` (large): frames scanned on `con`; `_DTYPES` and `_column_dtype` return mathspec strings; `ctype` and `role` become dims; `into` becomes `one_of`; `snapshot.timestep` names a `timestep` dim.
9. Tests (large): fixtures declare string dtypes; narwhals expressions become relation calls; `tests/test_functional_groups.py` moves from `into` to `one_of`, its group single-valuedness tests deleted; an old manifest and `into` are refused.
10. Pages: [schema](../schema.md) (its `into` sections), [format](../format.md), [record](../record.md), [read path](../read-path.md), [working record](../working-record.md), [writing](../writing.md), [tools](../tools.md), [module layout](../module-layout.md), the usage pages, `CHANGELOG.md`.

Checks: no `narwhals` in `src/`, `pyproject.toml` or `pixi.toml`; a manifest with `"Float64"` is refused naming `float`; one with `into` is refused naming `one_of`; every `record.*[...]` value is a `DuckDBPyRelation`; `set` takes a polars, pandas and arrow frame; a PyPSA network round-trips.

## Rejected Ideas

- **Keeping narwhals in tools only**: two value types for one record, and the conversion stays.
- **DuckDB type names in the manifest**: an engine's vocabulary where mathspec's is the consumer's.
- **A declared struct dtype**: it would restate the group's key.
- **Keeping `datetime` on properties**: mathspec's `ParameterDtype` refuses it, so the model could not read it.
- **A local `Enum` dtype on properties**: mathspec puts the labels on a dimension; a property list would be a second vocabulary.
- **Keeping `into` beside `one_of`**: two spellings of one declaration.

## Open Issues

- **Recovering single-valuedness for a functional group.** A group property may be NULL, so a tuple may carry no label. Recommend nothing until a consumer needs it; a later `required` flag on a property would cover it.
