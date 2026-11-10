<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 24: categories are enum dimensions

| DEP      | 24                                                                                                                                                         |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | categories are enum dimensions                                                                                                                             |
| Author   | Bryn Pickering                                                                                                                                             |
| Status   | Implemented (2026-10-26)                                                                                                                                   |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 18](dep-0018-drop-narwhals.md), [DEP 23](dep-0023-multi-dims.md) |
| Created  | 2026-10-21                                                                                                                                                 |
| Version  | Unreleased                                                                                                                                                 |

The design landed in [the schema](../../reference/schema.md), [the data model](../../reference/data-model.md), [the record format](../../reference/format.md) and [the tools reference](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

A `str` dim may list its labels with `one_of`, as [mathspec#915](https://github.com/energy-models/mathspec/pull/915) does for a mathspec dimension. A category is then a label of such a dim, and a categorising property is a property `{one_of: <dim>}` that refers to it. [DEP 18](dep-0018-drop-narwhals.md) landed that, and dropped the `Enum` dtype. This DEP adds two things. A dim with `one_of` has its axis derived from the list, so the schema lists its labels and no layer writes them. A multi-dim may be categorised, so that `connection.role` categorises `connection`.

## Motivation

A closed set of values was two constructs, an `Enum` dtype on a property and a reference to a dim, and only one categorised. [DEP 18](dep-0018-drop-narwhals.md) made them one: `ctype` and `role` are dims that list their labels, and `entity.ctype: {one_of: ctype}` categorises `entity`.

A `one_of` dim's labels are still data. The list only bounds the axis. A layer writes `dims/ctype.parquet` whole, as [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) requires, and may drop a label of the list from it. PyPSA writes the whole list on every export, so the file restates the schema. datarecord checks a category that a `default_by` names against the list, yet that category may not be a live label in a given record.

A multi-dim is still never categorised. `connection.role` is `{one_of: role}`, `role` lists `input`, `output` and `attached`, and PyPSA lists `connection` in `partial`. Yet datarecord refuses a `default_by` keyed on `connection.role`, because only a dim in `partial` that is not a multi-dim has categories. [DEP 23](dep-0023-multi-dims.md) made the group a multi-dim and left open whether the rule extends to one.

## Specification

### What DEP 18 settled

| #81 proposed                                                                     | DEP 18 landed                                            |
| -------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `enum: [...]` beside a `String` dim's `dtype`                                    | `one_of: [...]` on a `str` dim, mathspec#915's final key |
| a property `into` an `enum` dimension                                            | a property `{one_of: <dim>}`                             |
| a categorising property is one `into` an `enum` dimension, of a dim in `partial` | the same rule, spelt `one_of`                            |
| `{Enum: [...]}` refused at load                                                  | refused, naming `one_of`                                 |
| PyPSA's `component` and `role` as `enum` dimensions                              | PyPSA's `ctype` and `role` dims                          |
| categories checked at load in a `default_by` and a selector                      | checked against the dim's `one_of`                       |

### A categorising property of a multi-dim

A property `{one_of: <d>}` of a multi-dim in `partial`, where `d` lists its labels, categorises the multi-dim by the rule for a dim:

```yaml
dimensions:
  role: { one_of: [input, output, attached] }
  connection: { over: { entity: entity, bus: bus } }
properties:
  connection:
    role: { one_of: role }
partial: [entity, bus, connection]
```

- `connection.role` categorises `connection`, because a multi-dim is a dim since [DEP 23](dep-0023-multi-dims.md).
- `Schema.categorizing("connection")` lists `role`, and `default_for` and `flags` key on it by plain name, as on `entity.ctype`.
- A `default_by` keyed on `connection.role` applies to a value of `connection`: an attribute over it, or a property of it. datarecord checks its keys at load against `role`'s `one_of`.
- A selector keyword `role=` on an attribute over `connection`, or on a property of it, selects the tuples with that role. `set("connection.note", v, role="output")` works as `set("entity.note", v, ctype="Generator")` does.

### A `one_of` dim's axis is the schema's

A dim with `one_of: [...]` has its axis derived from the list, in list order:

- No `dims/<dim>.parquet` is written or read for it, and `record.dims["ctype"]` is the list. The writer refuses a layer with such a file.
- A record written whole hands over the axis among its `dims`. The writer drops the axis where it is the list, in the same order, and refuses any other, naming the dim and the difference.
- datarecord refuses at load a property declared on it, because without a file no column can have one.
- datarecord refuses `add` and `remove` on it, naming the schema.
- Otherwise it is a dim. An attribute may range over it and a property may refer to it. A value outside the list is refused as dangling.

| Before                                                                  | Under this DEP                                               |
| ----------------------------------------------------------------------- | ------------------------------------------------------------ |
| the list bounds the axis, and a layer writes `dims/ctype.parquet` whole | the axis is the list, with no layer file                     |
| `add` refuses a label outside the list, and `remove` works              | both refused                                                 |
| PyPSA writes the whole list as the `ctype` and `role` axes on export    | the tool writes nothing for them                             |
| a property that refers to `ctype` is checked against the resolved axis  | checked against the list, with no dead labels and no cascade |
| a `one_of` dim may have properties                                      | refused                                                      |

## Rationale

Categories need a subject whose keys the owner map assigns per label, and `partial` makes a multi-dim such a subject, as it does a dim. Refusing them for a multi-dim alone would keep the one rule that set a group apart.

datarecord checks a category at load where it is named. That needs the vocabulary in the schema, and an axis that a layer may shrink lets a checked category be no live label. Deriving the axis from the list makes the axis and the list the same.

What it costs:

- A record with a `dims/` file for a `one_of` dim is re-exported without it. datarecord refuses a layer that writes one, and a `RecordLike` whose axis of it is not the list.
- datarecord refuses at load a property declared on a `one_of` dim.
- datarecord refuses `add` and `remove` on a `one_of` dim, so adding a PyPSA component type stays a schema change.
- A schema that keyed a `default_by` on a multi-dim's property was refused, and now loads.

## Usage

```python
schema.categorizing("connection")  # ("role",)
schema.default_for("efficiency", role="input")
w.set("efficiency", 0.95, role="output")  # selector: the category
record.flags(role="attached")
record.dims["ctype"]  # its `one_of` labels, in order
w.add("ctype", ["Hydrogen"])  # refused: a `one_of` dim's labels are the schema's
```

## Implementation notes

1. A `one_of` dim's axis is derived from the schema on read. The writer refuses its `dims/` file, `add`, `remove` and properties on it are refused, and the PyPSA tool stops writing `ctype` and `role`.
2. `Schema.categorizing` and `Schema._categorized_by` accept a multi-dim in `partial`, and the refusal of categories on a multi-dim goes.
3. Category selection in `set`, `flags` and `default_for` joins on the multi-dim's struct column, as on a dim's column.
4. The dims, properties, format and PyPSA pages. The open question on a multi-dim's categories moves to settled.

Checks: a `one_of` dim has no layer file, and `record.dims` of it is its list. A `default_by` keyed on `connection.role` loads, and a misspelt role in it fails at load. `default_for` and `flags` agree per role on a PyPSA network.

## Rejected ideas

- `Enum` kept beside `one_of` dims: two ways to state "one of these labels", one categorising and one not, by dtype. [DEP 18](dep-0018-drop-narwhals.md) settled it.
- A `one_of` dim that a layer may grow beyond its list: a category in a `default_by` could then be checked only against the folded record, not at load.

## Open issues

- An axis that a layer writes, bounded by the list, the design before this DEP. A layer writes the axis, `add` refuses a label outside the list and `remove` works. A tool or layer then chooses which labels exist, and a `one_of` dim may have properties. The cost is a file that restates the schema, and a category that may not be a live label.
- Whether a `one_of` dim may grow in a layer. Its labels are the schema's, so a custom PyPSA component is a schema change, not a data edit. That is the cost of checking categories at load.
