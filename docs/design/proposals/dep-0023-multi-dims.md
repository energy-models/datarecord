<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 23: multi-dims replace groups

| DEP      | 23                                                                                                                                                                                                                               |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | multi-dims replace groups                                                                                                                                                                                                        |
| Author   | Bryn Pickering                                                                                                                                                                                                                   |
| Status   | Implemented (2026-10-26)                                                                                                                                                                                                         |
| Requires | [DEP 10](dep-0010-a-group-is-a-struct-column.md), [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 18](dep-0018-drop-narwhals.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) |
| Created  | 2026-10-21                                                                                                                                                                                                                       |
| Version  | Unreleased                                                                                                                                                                                                                       |
| Replaces | the groups of [DEP 2](dep-0002-groups.md) and [DEP 10](dep-0010-a-group-is-a-struct-column.md)                                                                                                                                   |

## Abstract

A group becomes a **multi-dim**: a dim declared under `dimensions` with `over`, whose labels are tuples of other dims' labels. It has its own dimension table in `dims/`, its coordinates are addressed as `connection.bus` as properties are, and a functional group's property `{one_of: <dim>}` becomes a property of the multi-dim, unchanged. The `groups` section, and every public name with `group` in it, goes.

## Motivation

**A group is a dim in all but name.** [DEP 10's amendment](dep-0010-a-group-is-a-struct-column.md#amendment-a-group-is-a-dim) found it so in every way the fold sees: it is one struct column, it broadcasts, it is listed in `partial` or owned whole, it has a table of properties. [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) made its file restated whole, as an axis file is. What still sets it apart is spelling, and the spelling costs:

| a dim                                                                                  | a group                                                                                      |
| -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| declared under `dimensions`                                                            | under `groups`                                                                               |
| `dims/bus.parquet`                                                                     | `groups/connection.parquet`                                                                  |
| `add`, `remove`                                                                        | `add_group`, `remove_group`                                                                  |
| `Schema.dimensions`                                                                    | `Schema.groups`, `group_columns`, `group_key`, `groups_of`                                   |
| [`dims: [X]`](../model/multi-dims.md#attributes-over-a-multi-dim) resolves a dim first | …and a group if no dim is so named                                                           |
| a property `{one_of: <dim>}` may [categorize](../model/properties.md#categories) it    | [never categorized](../open-questions.md#whether-a-multi-dim-in-partial-may-have-categories) |

Every reader of a schema asks twice, and every rule that holds for both is stated twice.

## Specification

### A multi-dim

**A multi-dim is a dim whose labels are tuples of other dims' labels.** It is declared under `dimensions` with `over`, as a group was, and has no `dtype`:

```yaml
dimensions:
  entity: { dtype: str }
  bus: { dtype: str }
  snapshot: { dtype: datetime }
  role: { one_of: [input, output, attached] }
  connection: { over: { entity: entity, bus: bus } }
  corridor: { over: { from: bus, to: bus } }

properties:
  connection:
    role: { one_of: role }

attributes:
  efficiency: { dtype: float, dims: [connection, snapshot], default: 1 }

partial: [entity, bus, connection]
```

- **`over` is a list or a coordinate→dim map**, as a group's was: `over: [entity, bus]` is `over: {entity: entity, bus: bus}`, and `corridor` draws two coordinates on `bus`. Each coordinate names a declared dim that is not a multi-dim.
- **`over` excludes `dtype` and `one_of`.** A dim declaring `over` beside either is refused at load. A dim without `over` keeps its default `dtype: str`.
- **Its table is `dims/connection.parquet`**, keyed by its struct column, with its properties as columns: `connection STRUCT(entity, bus) | role`, what `groups/connection.parquet` was. `dims/bus.parquet` is keyed by `bus` the same way. A layer that changes its tuples or their properties writes it whole and in order, as [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md#an-axis-is-its-deepest-file) has it write a group file today.
- **`Dimension.over`** holds the map; `Schema.dimensions` holds every dim, multi or not.

### What DEP 10 settled carries over

- **A row addresses it by one struct column** named for it, a field per coordinate in `over` order.
- **It may be listed in `partial`**, and is in the fold key there; owned whole otherwise.
- **A value is a whole tuple or NULL.** A struct with some fields NULL is refused wherever a row comes in: `set`, `add` and the writer.
- **A NULL broadcasts over its live tuples**, expanded against `dims/connection.parquet` as a NULL `bus` is against `dims/bus.parquet`.
- **An attribute may not name a multi-dim beside a dim that shares a name with one of its coordinates**, since a keyword such as `bus=` would address both: `dims: [connection, bus]` is refused. A coordinate drawing on `bus` under another name, such as `port`, may sit beside `bus`, as [a group's](../model/multi-dims.md#attributes-over-a-multi-dim) may today.
- **Removing a coordinate's label cascades**: `remove("bus", ["south"])` removes every tuple naming `south`, as it removed every group row.

### Coordinates are addressed as properties are

**`connection.bus` names the `bus` of each tuple**, wherever [DEP 13](dep-0013-buses-and-carriers-are-dims.md#a-propertys-name-is-its-subjects) lets a dotted name stand: `Schema.resolve` among them. A coordinate is not a property column: it is part of the key, so `set("connection.bus", …)` is refused, naming `remove` and `add`. A [`default_by`](../model/properties.md#default_by-a-default-per-category) keyed on `entity.ctype` reaches `entity` through the coordinate already, as it reaches a group's field today.

- A **selector keyword** stays bare: `bus="north"` on an attribute over `connection` selects the tuples whose `bus` is `north`, coordinate first, as today.
- A property of a multi-dim named like one of its coordinates stays refused, as [the schema](../model/properties.md#names) refuses it for a group.
- **A multi-dim is never categorized.** A property of it may refer to a dim with `one_of`, as `role` does, and still categorizes nothing, `partial` listing the multi-dim or not; a `default_by` keyed on one is refused. Whether that changes is left [open](#open-issues).

### A functional group is a property of a multi-dim

**A group property `{one_of: <dim>}`**, which [DEP 18](dep-0018-drop-narwhals.md#into-goes) put in place of a group's `into`, **becomes a property of the multi-dim, declared the same way**:

```yaml
# before
groups:
  plant_period: { over: [plant, period] }
properties:
  plant_period:
    zone: { one_of: zone }

# after
dimensions:
  plant_period: { over: [plant, period] }
properties:
  plant_period:
    zone: { one_of: zone }
```

One `zone` per tuple is what a property is: one value per label of its subject. The live-label check, the `remove` refusal and NULL meaning none are [a property referring to a dim's](../model/properties.md#referring-to-a-dim).

### Groups go

**With that, `groups` holds nothing and is removed.** `Group`, `Schema.groups`, `Schema.group_columns`, `Schema.groups_of`, `Schema.group_key`, `add_group`, `remove_group` and the `groups/` directory go. [`dims: [X]`](../model/multi-dims.md#attributes-over-a-multi-dim) resolves a dim, and nothing else.

**`add` and `remove` take a multi-dim's tuples** as a frame, anything DuckDB scans, holding the struct column named for the multi-dim, which is what `record.dims["connection"]` reads, so a read goes back in as it stands. A frame may instead carry a column per coordinate, and a list of mappings from coordinate to label is made into that frame up front. Every form then takes the same path and the same type checks. A frame carrying both the struct column and a coordinate column is refused as ambiguous:

```python
w.add("connection", other.dims["connection"])  # another record's tuples, as read
w.add("connection", [{"entity": "g1", "bus": "north"}], role="output")
w.add("connection", pd.DataFrame({"entity": ["g2"], "bus": ["south"]}))
w.remove("connection", [{"entity": "g1", "bus": "north"}])
```

A property is a keyword, as for any dim, or a further column of the frame. A tuple naming a label its coordinate's dim lacks is refused, as `add_group` refused it.

## Rationale

**A multi-dim, not a relation usable as a dim.** A tuple set that may stand in `dims` when used there and is a relation otherwise is two kinds of thing decided by the caller. Declaring it under `dimensions` says once what it is, gives it a table for its properties, and lets `add` and `remove` treat it as any other dim.

**A dim table, not a second directory.** `dims/` already holds one table per dim keyed by that dim's column, each restated whole; a multi-dim's is keyed by its struct column. A reader of the format looks in one place.

**What breaks:**

- Every manifest with `groups` is rewritten, and every record re-exported: `groups/` moves to `dims/`.
- Every public name with `group` in it goes; callers of `add_group` and `remove_group` call `add` and `remove`.

## Usage

```python
record.dims["connection"]  # connection STRUCT(entity, bus)
record.properties["connection"]  # connection | role
record.attributes[
    "efficiency"
]  # connection | snapshot | attribute | breakpoint | value
```

```python
w.add("connection", [{"entity": "g1", "bus": "north"}], role="output")
w.set("efficiency", 0.9, entity="g1", bus="north")  # selector: the coordinates
w.set("connection.role", "input", entity="g1", bus="north")
w.set("connection.bus", "south", entity="g1")  # refused: a coordinate is the key
w.remove("bus", ["north"])  # and every connection at it
```

## Implementation notes

1. `Dimension.over`, parsed as a group's was; `over` exclusive with `dtype` and `one_of`; `groups` an unknown key, refused as any other.
2. The group struct-column machinery keyed on the dim: fold key, broadcast, whole-or-NULL, cascade, the coordinate-name clash.
3. `dims/<multi-dim>.parquet` for its table, restated whole; `groups/` and `resolved/groups/` dropped from the writer and reader.
4. `add` and `remove` take tuples, as a frame with the struct column or a column per coordinate, or as a list of mappings; `add_group`, `remove_group` removed.
5. `Schema.resolve` resolves a coordinate.
6. `Group`, `Schema.groups`, `group_columns`, `groups_of`, `group_key` removed; the PyPSA tool declares `connection` under `dimensions`.
7. Pages, usage, changelog; fixtures re-exported.

Checks that hold once built:

- **A multi-dim behaves as a group did**: the connection, broadcast, `partial` and cascade tests pass with `connection` declared under `dimensions`.
- **The word `group` appears in no public name**, and no live page outside the DEPs.
- **A PyPSA network round-trips** with `connection` a multi-dim and `role` its property.
- **A functional mapping is checked as a property is**: a `zone` naming no live label is refused on `set`, and removing a named `zone` refused.

## Rejected Ideas

**A relation usable as a dim** (`dims: [connection]` naming a relation). Makes a tuple set a relation or a dim depending on where it is used.

**No multi-dims at all** (`dims: [entity, bus]`, validity by a condition). Loses the struct column, the whole-or-NULL broadcast and the table for `role`.

**A surrogate port dim**, one label per tuple with two properties `{one_of: entity}` and `{one_of: bus}`. A value is then written against a port label and its entity and bus looked up; the tuple stops being the address.

## Open Issues

- **Whether a multi-dim may be `over` another multi-dim.** Nothing needs it yet; it is refused until something does. Carried on as an [open question](../open-questions.md#whether-a-multi-dim-may-be-over-another-multi-dim).
- **Whether a functional mapping over two coordinates justifies a dim.** `plant_period` exists only to carry `zone`; its labels must be written before the property can be. Carried on as an [open question](../open-questions.md#whether-a-functional-mapping-over-two-coordinates-justifies-a-dim).
- **Whether a multi-dim in `partial` may be categorized.** Once a group is a dim, the [open question](../open-questions.md#whether-a-multi-dim-in-partial-may-have-categories) on a group's categories becomes whether the categories rule extends to a multi-dim. This DEP keeps a multi-dim uncategorized, as its specification says; a later DEP may lift that.
