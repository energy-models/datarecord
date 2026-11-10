<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 23: multi-dims replace groups

| DEP           | 23                                                                                                                                                                                                                               |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title         | multi-dims replace groups                                                                                                                                                                                                        |
| Author        | Bryn Pickering                                                                                                                                                                                                                   |
| Status        | Implemented (2026-10-26)                                                                                                                                                                                                         |
| Requires      | [DEP 10](dep-0010-a-group-is-a-struct-column.md), [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 18](dep-0018-drop-narwhals.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) |
| Created       | 2026-10-21                                                                                                                                                                                                                       |
| Version       | Unreleased                                                                                                                                                                                                                       |
| Replaces      | the groups of [DEP 2](dep-0002-groups.md) and [DEP 10](dep-0010-a-group-is-a-struct-column.md)                                                                                                                                   |
| Superseded-By | [DEP 36](dep-0036-a-connection-is-keyed-by-its-port.md) (the PyPSA tool's `connection` over `(entity, bus)` and its `role`)                                                                                                      |

The design landed in [the data model](../../reference/data-model.md), [the schema](../../reference/schema.md), [editing](../../reference/editing.md) and [the record format](../../reference/format.md). Where they and this DEP differ, follow those pages.

## Abstract

A group becomes a multi-dim: a dim declared under `dimensions` with `over`, whose labels are tuples of other dims' labels. It has its own table in `dims/`, and its coordinates are addressed as `connection.bus`, as properties are. A functional group's property `{one_of: <dim>}` becomes a property of the multi-dim, unchanged. The `groups` section and every public name with `group` in it go.

## Motivation

A group already behaves as a dim does. The amendment to [DEP 10](dep-0010-a-group-is-a-struct-column.md) found that the fold treats it as a dim in every way. It is one struct column, broadcasts, is in `partial` or outside it, and has properties. [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) made a layer restate its file whole, as for an axis file. Only its spelling still differs from a dim's, in each of these places:

| A dim                                          | A group                                                    |
| ---------------------------------------------- | ---------------------------------------------------------- |
| declared under `dimensions`                    | declared under `groups`                                    |
| `dims/bus.parquet`                             | `groups/connection.parquet`                                |
| `add`, `remove`                                | `add_group`, `remove_group`                                |
| `Schema.dimensions`                            | `Schema.groups`, `group_columns`, `group_key`, `groups_of` |
| `dims: [X]` resolves a dim first               | `dims: [X]` resolves a group if no dim has the name        |
| a property `{one_of: <dim>}` may categorise it | never categorised                                          |

So every reader looks up a name as a dim and then as a group, and every rule for both is stated twice.

## Specification

### A multi-dim

A multi-dim is a dim whose labels are tuples of other dims' labels. It is declared under `dimensions` with `over`, as a group was, and has no `dtype`:

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

- `over` is a list or a map from coordinate to dim, as a group's was. `over: [entity, bus]` is `over: {entity: entity, bus: bus}`, and `corridor` has two coordinates on `bus`. Each coordinate maps to a declared dim that is not a multi-dim.
- datarecord refuses at load a dim that declares `over` beside `dtype` or `one_of`. A dim without `over` keeps its default `dtype: str`.
- Its table is `dims/connection.parquet`, keyed by its struct column, with its properties as columns: `connection STRUCT(entity, bus) | role`. A layer that changes its tuples or their properties writes it whole and in order.
- `Dimension.over` has the map, and `Schema.dimensions` has every dim, multi or not.

### What DEP 10 settled

- A row addresses a multi-dim by one struct column named for it, with a field per coordinate in `over` order.
- It may be listed in `partial`, and is then in the fold key. Otherwise it is outside `partial` and in no key.
- A value is a whole tuple or NULL. datarecord refuses a struct with some fields NULL in `set`, in `add` and in the writer.
- A NULL broadcasts over its live tuples, expanded against `dims/connection.parquet` as a NULL `bus` is against `dims/bus.parquet`.
- datarecord refuses an attribute over a multi-dim and a dim named like one of its coordinates, such as `dims: [connection, bus]`, because `bus=` would address both. A coordinate on `bus` under another name, such as `port`, may sit beside `bus`.
- Removing a coordinate's label cascades: `remove("bus", ["south"])` removes every tuple that contains `south`.

### Coordinates are addressed as properties are

`connection.bus` is the `bus` of each tuple, wherever [DEP 13](dep-0013-buses-and-carriers-are-dims.md) lets a dotted name stand, `Schema.resolve` included. A coordinate is part of the key and not a property column, so datarecord refuses `set("connection.bus", …)` and names `remove` and `add`. datarecord resolves a `default_by` keyed on `entity.ctype` through the coordinate `entity`.

- A selector keyword stays bare. `bus="north"` on an attribute over `connection` selects the tuples whose `bus` is `north`, coordinate first.
- datarecord still refuses a property of a multi-dim named like one of its coordinates.
- A multi-dim is never categorised. A property of it may refer to a dim with `one_of`, as `role` does, and still categorises nothing, whether `partial` lists the multi-dim or not. datarecord refuses a `default_by` keyed on one.

### A functional group is a property of a multi-dim

A group property `{one_of: <dim>}`, which [DEP 18](dep-0018-drop-narwhals.md) put in place of a group's `into`, becomes a property of the multi-dim, declared the same way:

```yaml
dimensions:
  plant_period: { over: [plant, period] } # was under `groups`
properties:
  plant_period:
    zone: { one_of: zone }
```

A property has one value per label of its subject, so one `zone` per tuple is a property. The live-label check, the `remove` refusal and NULL meaning none are those of any property that refers to a dim.

### Groups go

The `groups` section then has nothing left and goes. `Group`, `Schema.groups`, `Schema.group_columns`, `Schema.groups_of`, `Schema.group_key`, `add_group`, `remove_group` and the `groups/` directory go, and `dims: [X]` resolves a dim and nothing else. A manifest with `groups` is refused as any unknown key is.

`add` and `remove` take a multi-dim's tuples as any frame DuckDB scans, with the struct column named for the multi-dim. That column is what `record.dims["connection"]` returns, so a read goes back in as it stands. A frame may instead have a column per coordinate, and a list of mappings from coordinate to label becomes that frame first. Every form then takes the same path and type checks. datarecord refuses a frame with both the struct column and a coordinate column as ambiguous:

```python
w.add("connection", other.dims["connection"])  # another record's tuples, as read
w.add("connection", [{"entity": "g1", "bus": "north"}], role="output")
w.add("connection", pd.DataFrame({"entity": ["g2"], "bus": ["south"]}))
w.remove("connection", [{"entity": "g1", "bus": "north"}])
```

A property is a keyword, as for any dim, or a further column of the frame. datarecord refuses a tuple that contains a label missing from its coordinate's dim.

## Rationale

A tuple set that is a dim in `dims` and a relation elsewhere has two kinds, and the caller picks one at each use. Declared under `dimensions`, it has one kind, with a table for its properties, and `add` and `remove` treat it as any dim.

`dims/` already has one table per dim, keyed by that dim's column and restated whole. A multi-dim's table is keyed by its struct column, so a reader of the format looks in one place.

What it costs: every manifest with `groups` is rewritten, and every record re-exported, as `groups/` moves to `dims/`. Every public name with `group` in it goes, and callers of `add_group` and `remove_group` call `add` and `remove`.

## Usage

```python
w.set("efficiency", 0.9, entity="g1", bus="north")  # selector: the coordinates
w.set("connection.role", "input", entity="g1", bus="north")
w.set("connection.bus", "south", entity="g1")  # refused: a coordinate is the key
```

## Implementation notes

1. `Dimension.over`, and the group struct-column machinery keyed on the dim.
2. `dims/<multi-dim>.parquet`, with `groups/` and `resolved/groups/` dropped.
3. `add` and `remove` take tuples, `Schema.resolve` resolves a coordinate, and the group names go.
4. The PyPSA tool declares `connection` under `dimensions`, and fixtures are re-exported.
5. Pages, usage pages and changelog.

Checks: the connection, broadcast, `partial` and cascade tests pass with `connection` under `dimensions`. The word `group` appears in no public name, and on no live page outside the DEPs. A PyPSA network round-trips with `connection` a multi-dim and `role` its property. datarecord refuses a `zone` that is not a live label on `set`, and refuses removing a `zone` that a value still refers to.

## Rejected ideas

- A relation usable as a dim in `dims: [connection]`: a tuple set becomes a relation or a dim by where it is used.
- No multi-dims at all, with `dims: [entity, bus]` and validity by a condition: this loses the struct column, the whole-or-NULL broadcast and the table for `role`.
- A surrogate port dim with properties `{one_of: entity}` and `{one_of: bus}`: a value is written against a port label, and the tuple stops being the address.

## Open issues

- Whether a multi-dim may be `over` another multi-dim. Nothing needs it yet, so datarecord refuses it until something does. It is on the [open questions](../../about/open-questions.md) page.
- Whether a functional mapping over two coordinates justifies a dim. `plant_period` exists only to have `zone`, and its labels must be written before the property can be. It is on the open questions page.
- Whether a multi-dim in `partial` may be categorised, now that the open question on a group's categories becomes one on multi-dims. This DEP keeps a multi-dim uncategorised, and a later DEP may lift that.
