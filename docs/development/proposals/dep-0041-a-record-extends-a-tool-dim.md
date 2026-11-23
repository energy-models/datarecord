<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 41: A record may extend a tool's dim with a coordinate, and address the dim by that coordinate alone

| DEP      | 41                                                                                                                                                                  |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | A record may extend a tool's dim with a coordinate, and address the dim by that coordinate alone                                                                    |
| Author   | Bryn Pickering                                                                                                                                                      |
| Status   | Draft (2026-11-23)                                                                                                                                                  |
| Requires | [DEP 15](dep-0015-a-dim-is-flat.md), [DEP 17](dep-0017-results-are-attributes.md), [DEP 23](dep-0023-multi-dims.md), [DEP 31](dep-0031-a-record-selects-a-view.md) |
| Created  | 2026-11-23                                                                                                                                                          |

## Abstract

A record may declare a tool's dim as a multi-dim with a coordinate the tool does not know, such as `snapshot` over `(weather_year, timestep)`. A selection at one label squeezes the coordinate out. A tool builds from that selection and writes results back at the label. So one record holds independent runs whose time axes differ per label.

An attribute row may address such a multi-dim by one coordinate alone, without the whole tuple. The row gives the label in the coordinate's own dim column and leaves the multi-dim's struct NULL. It then holds for every tuple at that label. So a static `p_max_pu` may differ by weather year without a series at every hour. In core, this changes one schema rule from [DEP 23](dep-0023-multi-dims.md).

## Motivation

A run repeated over weather years or Monte Carlo draws solves one model per label. Today a record holds such runs as a dim of its own ([a collection](../../pypsa/reference.md#a-collection)), which works only where the runs share every axis. The PyPSA tool refuses a collection whose members hold other snapshots: "The network of weather_year 'wy2013' holds other snapshots than that of weather_year 'wy2010'".

PyPSA-Eur solves each weather year on its own, because PyPSA's `scenario` keeps capacities the same in every scenario. Each weather year also wants its own time aggregation. In [Manta PR 315](https://github.com/manta-plan/manta/pull/315), a time aggregation is the `snapshot.<name>` property, which maps each snapshot to the first snapshot of its segment. A property has one value per snapshot label, so two weather years segmented apart need one property name or one record each.

Once `snapshot` holds the weather year, a series attribute's static value has no place for it. PyPSA-Eur's 34 nuclear generators in its HR+SI record hold a static `p_max_pu` set from each weather year, from 0.83 (wy2013) to 0.993 (wy2020). A static row has a NULL `snapshot`, so it holds for every weather year. PyPSA-Eur works around it with a constant series per weather year: 4,467,600 rows for 5 distinct values, 5.6% of the 79.4 M rows of `p_max_pu`.

The need is not specific to PyPSA or to time. Each of these needs an axis that differs per label of a dim the framework does not know: typical days chosen per weather year, a grid that differs per expansion scenario, and regions that differ per study area.

## Specification

### The schema

A record may declare a dim that a tool declares flat, or as a multi-dim, as a multi-dim with one or more extra coordinates. Each extra coordinate draws on a dim of the record's own:

```python
schema.dimensions["weather_year"] = Dimension(dtype="str")
schema.dimensions["timestep"] = Dimension(dtype="datetime")
schema.dimensions["snapshot"] = Dimension(
    over={"weather_year": "weather_year", "timestep": "timestep"}
)
```

A coordinate is **known** to a tool where the tool's own schema declares it on that dim, as PyPSA declares `period` and `timestep` on `snapshot`. Any other coordinate is **extra**. A tool names, for each of its flat dims, the coordinate that holds that dim's labels once it becomes a multi-dim: `timestep` for PyPSA's `snapshot`.

The dim of an extra coordinate is in `partial`, so that a layer at one label keeps the other labels' values. A tool refuses to write results over a record that declares the coordinate but leaves its dim out of `partial`.

Changing `over` or `dtype` is incompatible ([compatibility](../../reference/schema.md#compatibility)), so the record that first declares the dim declares its extra coordinates.

### Addressing a multi-dim by one coordinate

This is the change to core datarecord. An attribute's `dims` may list a multi-dim beside the dim one of its coordinates draws on, where the coordinate is named for that dim. The coordinate and the dim's column are then one coordinate. A row holds the label in the struct, or, with the struct NULL, in the column. A row with a NULL struct and a label in the column holds for every tuple at that label:

| `entity` | `snapshot`                                 | `weather_year` | holds for                          |
| -------- | ------------------------------------------ | -------------- | ---------------------------------- |
| `g`      | `{weather_year: wy2013, timestep: 01:00}`  | NULL           | that one tuple                     |
| `backup` | NULL                                       | `wy2013`       | every tuple of wy2013              |
| `alike`  | NULL                                       | NULL           | every tuple, as the broadcast rule |

[DEP 23](dep-0023-multi-dims.md) refused any attribute over a multi-dim and a dim named like one of its coordinates, because `bus=` would address both. This DEP narrows the refusal to a coordinate that draws on another dim than the one it is named for:

| `connection` over             | `x` over            | Schema                                                  |
| ----------------------------- | ------------------- | ------------------------------------------------------- |
| `{entity: entity, bus: bus}`  | `{connection, bus}` | accepted: `bus` is one coordinate, in two places        |
| `{entity: entity, bus: node}` | `{connection, bus}` | refused: `bus=` would address the dims `bus` and `node` |
| `{entity: entity, port: bus}` | `{connection, bus}` | accepted, as before: two coordinates on `bus`           |

DEP 23 refused the first row, so no existing record changes meaning. Core changes in three places:

1. `Schema` accepts the first row of the table and still refuses the second.
2. `Schema.check_multi_dim_columns` takes `own`, the dims the carrier holds as columns of their own. A column in `own` is not read as a coordinate carried flat.
3. `write_layer` and `WorkingRecord.set` pass the attribute's `dims` as `own`, so a file or frame may carry `weather_year` beside the `snapshot` struct.

Core keeps the rest as it is. The broadcast rule still has a struct NULL whole or a whole tuple, and `write_layer` still refuses a partial struct. `select(weather_year="wy2013")` resolves the address with no new code: it settles the `weather_year` column and drops the field from the struct, as it does for any squeezed dim and coordinate. The fold keys the row by its `weather_year` column, which is in `partial`, as it keys `p_nom`.

Core resolves the address only through `select` today. The PyPSA tool reads a record only after a selection has squeezed the coordinate, so it needs nothing more. On an unselected record, `unpacked` gives two `weather_year` columns, and no rule in `settle` ties the column to the struct field. Core also does not check that a row holding the label in both places holds one label. `WorkingRecord.set(..., weather_year="wy2013")` writes the same label in both places. A frame passed to `set` with two different labels is not refused, and no selection keeps that row.

### A selection

`select` already squeezes a coordinate's dim and drops the coordinate from each multi-dim that draws on it. Nothing in `select` or `lift` changes.

| Selection                         | `snapshot` of the selection                     |
| --------------------------------- | ----------------------------------------------- |
| `record`                          | multi-dim over `(weather_year, timestep)`       |
| `select(weather_year="wy2013")`   | multi-dim over `(timestep)`, wy2013's tuples    |
| `select(weather_year=["wy2013"])` | multi-dim over `(weather_year, timestep)`, kept |

### A tool

Every tool follows four rules:

1. `verify` reports each extra coordinate's dim in `unsupported_dims`, unless a selection squeezes it.
2. `build` reads a multi-dim left with only the coordinate a tool names for a flat dim as that flat dim. `snapshot` over `(timestep)` alone builds a single-period network.
3. `build` refuses a record that keeps an extra coordinate. A collection builds one model per label of it.
4. `to_datarecord(..., results_only=True, base=selection)` writes each row at the labels the selection squeezes. A row over the extended dim has the label inside the multi-dim's struct. A row over the coordinate's dim has it as a column. So a static result, such as `p_nom_opt`, lands at the squeezed label only.

Rule 4 already holds for `period` in the PyPSA tool; this DEP extends it to every squeezed coordinate.

### A collection

A collection over a dim of the record's own may hold members with different labels on an extended dim. `to_datarecord` of such a collection declares the dim extended by the collection's dim, and writes each member's labels under its own label. The caller says which dims to extend:

```python
PyPSACollection.to_datarecord(networks, con, extend=["snapshot"])
```

Without `extend`, a collection whose members differ on a dim is refused, as today. With the dim extended, `to_datarecord(..., results_only=True, base=record)` of such a collection writes each member's results at its label, as rule 4 does for one network.

`build` of a record that keeps the coordinate refuses, and a collection build gives one model per label, each with its own labels on the extended dim:

```python
PyPSA.build(record)
# UnsupportedRecordError: record cannot build a pypsa model; missing: unsupported dims ['weather_year']
PyPSACollection.build(record)
# wy2010: 2010-01-01 00:00 to 02:00, wy2013: 2013-01-01 00:00 to 02:00
```

### A static value of a series attribute

A collection writes a series attribute's static value, such as `p_max_pu` at no snapshot, by one coordinate alone where the members differ. Where the members hold different values for one entity, each member's row has a NULL `snapshot` and its label in `weather_year`. Where they hold one value, it is one row with both NULL, so the common case writes no more rows than before. The members' values agree within the tolerance a collection already applies to static values.

In a collection of wy2010 and wy2013, generator `backup` holds a static `p_max_pu` of 0.8 and 0.6, and generator `alike` holds 0.9 in both:

| `entity` | `snapshot` | `weather_year` | `value` |
| -------- | ---------- | -------------- | ------- |
| `alike`  | NULL       | NULL           | 0.9     |
| `backup` | NULL       | `wy2010`       | 0.8     |
| `backup` | NULL       | `wy2013`       | 0.6     |

`PyPSA.build(record.select(weather_year="wy2013"))` gives `backup` a static `p_max_pu` of 0.6 and no series.

## Rationale

**One coordinate, not one record per label.** Separate records duplicate every shared input, such as topology and costs, and lose the layer history that relates the runs.

**A coordinate of the tool's dim, not a property per label.** A property per label, such as `snapshot.segments_wy2013`, puts the label in a name. Nothing in the schema relates the name to the label. With the coordinate, `snapshot.segments` holds a value per `(weather_year, timestep)` tuple, and a selection gives one year's segments under the plain name.

**Squeeze in the selection, not in the build.** `select` already squeezes and `lift` already restores any coordinate. A tool that squeezed at build would need its own code for both.

**The coordinate in its dim's column, not in a partial struct.** Both spell the same address. The column needs no new rule in `select`, `lift` or the fold, and a NULL `weather_year` already broadcasts over every label, which keeps one row where the members agree. Rule 4 already writes a static result over the coordinate's dim as a column.

**Explicit `extend`.** Extending a dim whenever members differed would make the schema's shape depend on the data.

**The cost.** First, a record cannot gain an extra coordinate in a later layer; the writer of the root must declare it. Second, every row over the extended dim carries the extra field, so each series row is wider. Third, a tool cannot build the whole record as one model; it builds one model per label. Fourth, a row's label of the coordinate sits in the column or in the struct, and core reads it as one coordinate only through `select`.

## Usage

PyPSA-Eur writes five weather years once, each with its own timestamps:

```python
networks = pypsa.NetworkCollection(
    pd.Series({wy: prepared(wy) for wy in weather_years}).rename_axis("weather_year")
)
root = store.commit(
    PyPSACollection.to_datarecord(networks, ws.con, extend=["snapshot"]), parent=None
)
```

Then it solves each weather year alone and writes its results back as a layer:

```python
revision = root
for wy in weather_years:
    view = revision.record.select(weather_year=wy)
    n = PyPSA.build(view)
    n.optimize(solver_name="highs")
    results = PyPSA.to_datarecord(n, ws.con, results_only=True, base=view)
    revision = store.commit(results, parent=revision)
```

`p_nom_opt` of wy2013 is at `weather_year="wy2013"` only, and each `p` row is at `{weather_year: "wy2013", timestep: ...}`. Each nuclear generator's static `p_max_pu` is at its weather year, and PyPSA-Eur drops its constant series.

## Implementation notes

1. PyPSA tool: rule 4 for a squeezed extra coordinate of `snapshot`, tested on a hand-built record.
2. PyPSA tool: refuse results over a record whose extra coordinate's dim is outside `partial`.
3. PyPSA tool: `extend` on `PyPSACollection.to_datarecord`, for a full export and for results.
4. Core: an attribute addresses a multi-dim by one coordinate in that coordinate's dim column ([addressing a multi-dim by one coordinate](#addressing-a-multi-dim-by-one-coordinate)).
5. PyPSA tool: a series attribute's static value the members hold apart is at each member's label of the coordinate.
6. The `Tool` protocol page states the four rules for every tool.
7. A how-to guide on runs per weather year.

Rules 1 to 3 already hold in the PyPSA tool. Steps 1, 4 and 5 are built on the branch of this DEP, and `pixi run test` passes with them. Over a squeezed coordinate, the results source checks the network's snapshots against the whole record and packs the squeezed label into each snapshot, as it already did for `period`. A series result over the extended dim is not widened over the coordinate's dim; a static result, such as `p_nom_opt`, is. Chained layers at wy2010 and wy2013 each keep their own `p_nom_opt`. Step 5 compares the members' static frames first, so it reads the long rows of a series only where a static value differs.

## Rejected ideas

- **Weather years on `scenario`.** PyPSA keeps capacities the same in every scenario of one solve, so `scenario` cannot hold runs whose capacities differ.
- **Core flattens a multi-dim left with one coordinate.** The selection's `snapshot` would change dtype, and `lift` would need to repack it.
- **The address as a partial `snapshot` struct**, `weather_year` set and `period` and `timestep` NULL. The broadcast rule refuses a struct NULL in some fields and not all, and `write_layer` refuses such a row. `select(weather_year=...)` would also drop it, since it keeps a row whose struct is NULL or a tuple of the axis, and the partial struct is neither. Supporting it would change the broadcast rule, the fold and `select`.
- **A coordinate named apart from its dim**, such as `snapshot` over `(member, timestep)` with `member` drawing on `weather_year`. DEP 23 accepts `p_max_pu` over `snapshot` and `weather_year` then, so core would not change. However, DEP 23 reads the field and the column as two coordinates, so a static row would mean one value at every snapshot of every weather year. A reader would also address `snapshot.member` for a weather year.
- **A constant series per member**, as PyPSA-Eur writes today. It pins a yearly value to every hour and holds 4,467,600 rows for 5 values.

## Open issues

- How `settle` and `unpacked` read a row addressed by one coordinate on a record that keeps the coordinate, and whether core refuses a row holding two labels of it.
- Whether a collection should also extend a dim other than `snapshot`, such as `bus`, and whether every tool can build from such a record.
- Whether `extend` belongs on `to_datarecord` or in a schema the caller passes.
- How a time aggregation that clusters over all weather years at once, rather than per weather year, reads an extended `snapshot`.
