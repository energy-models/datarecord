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

## Abstract

A `str` dimension may list its labels with `one_of`, as [mathspec#915](https://github.com/energy-models/mathspec/pull/915) does for a mathspec dimension. A category is then a label of such a dimension, and a categorizing property is a property `{one_of: <dim>}` referring to it. [DEP 18](dep-0018-drop-narwhals.md) landed all of that, and dropped datarecord's `Enum` dtype with it. Two things remain. **A dim with `one_of` has its axis derived from the list**, so its labels are the schema's, not data. **A multi-dim may be categorized**: now that [DEP 23](dep-0023-multi-dims.md) makes a group a dim, `connection.role` categorizes `connection`.

## Motivation

**A closed set of values was two constructs**, an `Enum` dtype on a property and a reference to a dim, and only one categorized. [DEP 18](dep-0018-drop-narwhals.md#categories) made them one: `ctype` and `role` are dims listing their labels, and `entity.ctype: {one_of: ctype}` categorizes `entity`.

**A `one_of` dim's labels are still data.** Under [DEP 18](dep-0018-drop-narwhals.md#dtypes) the list only bounds the axis: a layer owns `dims/ctype.parquet`, writes it whole as [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) has every axis written, and may drop a label of the list from it. PyPSA writes the whole list on every export, so the file restates the schema. A category a `default_by` names is checked against the list, yet may name no live label in a given record.

**A multi-dim is still never categorized.** `connection.role` is `{one_of: role}`, `role` lists `input`, `output` and `attached`, and PyPSA lists `connection` in `partial`, yet a `default_by` keyed on `connection.role` is refused: [only a dim in `partial` that is no multi-dim](../model/properties.md#categories) has categories. [DEP 23](dep-0023-multi-dims.md) made the group a multi-dim and left [whether the rule extends to one](../open-questions.md#whether-a-multi-dim-in-partial-may-have-categories) open.

## Specification

### What DEP 18 already settled

| #81 proposed                                                                     | the stack has                                                                                  |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `enum: [...]` beside a `String` dim's `dtype`                                    | [`one_of: [...]`](../model/dims.md#a-dim-with-one_of) on a `str` dim, mathspec#915's final key |
| a property `into` an `enum` dimension                                            | a property [`{one_of: <dim>}`](../model/properties.md#referring-to-a-dim)                      |
| a categorizing property is one `into` an `enum` dimension, of a dim in `partial` | [the same rule](../model/properties.md#categories), spelled `one_of`                           |
| `{Enum: [...]}` refused at load                                                  | refused, naming `one_of`                                                                       |
| PyPSA's `component` and `role` as `enum` dimensions                              | PyPSA's [`ctype` and `role`](../tools/pypsa.md) dims                                           |
| categories checked at load in a `default_by` and a selector                      | checked against the dim's `one_of`                                                             |

### A categorizing property of a multi-dim

**A property `{one_of: <d>}` of a multi-dim listed in `partial`, where `d` lists its labels, categorizes the multi-dim**, by the rule a dim follows:

```yaml
dimensions:
  role: { one_of: [input, output, attached] }
  connection: { over: { entity: entity, bus: bus } }
properties:
  connection:
    role: { one_of: role }
partial: [entity, bus, connection]
```

- **`connection.role` categorizes `connection`**: a multi-dim is a dim since [DEP 23](dep-0023-multi-dims.md), and a group's property could not.
- `Schema.categorizing("connection")` lists `role`; `default_for` and [`flags`](../use/reading.md#flags) key on it by plain name, as on `entity.ctype`.
- **A `default_by` keyed `connection.role`** applies to a value that reaches `connection`: an attribute over it, or a property of it. Its keys are checked at load against `role`'s `one_of`.
- **A selector keyword `role=`** on an attribute over `connection`, or on a property of it such as `set("connection.note", v, role="output")`, picks the tuples with that role, as `ctype=` picks entities, `set("entity.note", v, ctype="Generator")` included.

### A `one_of` dim's axis is the schema's

**A dim with `one_of: [...]` has its axis derived from the list**, in list order:

- **No `dims/<dim>.parquet` is written or read for it.** The writer refuses a layer that carries one, and the read derives the axis from the schema, so `record.dims["ctype"]` is the list. A `RecordLike` answers it among its `dims`, so a record written whole hands over its axis: the writer drops it where it is the list, the same labels in the same order, and refuses any other, naming the dim and the difference.
- **It carries no properties.** A property declared on it is refused at load: with no file there is no column to hold one.
- **`add` and `remove` on it are refused**, naming the schema.
- **Everything else is a dim's**: an attribute may range over it, a property may refer to it, and a value naming a label it does not list is refused where a dangling label is.

Against the stack today:

| today                                                                                                     | under this DEP                                                           |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| [the list bounds the axis](../model/dims.md#a-dim-with-one_of); a layer writes `dims/ctype.parquet` whole | the axis is the list; no layer file                                      |
| `add` refuses a label outside the list; `remove` works                                                    | both refused                                                             |
| PyPSA writes the whole list as the `ctype` and `role` axes on export                                      | the tool writes nothing for them                                         |
| a property referring to `ctype` is checked against the resolved axis, its dead labels kept by the fold    | checked against the list; `ctype` has no dead labels, nothing to cascade |
| a `one_of` dim may carry properties                                                                       | refused                                                                  |

## Rationale

**A multi-dim, as a dim.** Categories need a subject owned per label, which `partial` gives a multi-dim as it gives a dim. Refusing them for a multi-dim alone would keep the one rule that set a group apart.

**The schema's labels, not data.** A category is checked at load where it is named. That needs the vocabulary in the schema, and an axis a layer may shrink lets a checked category name no live label. Deriving the axis from the list makes the two one.

**What breaks:**

- A record with a `dims/` file for a `one_of` dim is re-exported without it; a layer writing one is refused, as is a `RecordLike` whose axis of it is not the list.
- A property declared on a `one_of` dim is refused at load.
- `add` and `remove` on a `one_of` dim are refused; adding a PyPSA component type stays a schema change.
- A schema that keyed a `default_by` on a multi-dim's property was refused and now loads.

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

1. A `one_of` dim's axis derived from the schema on read; its `dims/` file refused by the writer; `add`, `remove` and properties on it refused; the PyPSA tool stops writing `ctype` and `role`.
2. `Schema.categorizing` and `Schema._categorized_by` accept a multi-dim in `partial`; the refusal "a multi-dim is never categorized" goes.
3. Category selection in `set`, `flags` and `default_for` joins on the multi-dim's struct column, as it joins on a dim's column.
4. Pages: [dims](../model/dims.md#a-dim-with-one_of), [properties](../model/properties.md#categories), [format](../storage/format.md#axis-files), [PyPSA](../tools/pypsa.md); the open question on a multi-dim's categories moves to settled.

Checks that hold once built:

- **A `one_of` dim has no layer file**, and `record.dims` of it is its list.
- **A `default_by` keyed `connection.role` loads**, and a misspelt role in it fails at load.
- **`default_for` and `flags` agree per role** on a PyPSA network.

## Rejected Ideas

**Keeping `Enum` beside `one_of` dimensions.** Two ways to say "one of these labels", one of them categorizing and one not, by dtype. [DEP 18](dep-0018-drop-narwhals.md) settled it.

**A `one_of` dimension a layer may grow beyond its list.** A category named in a `default_by` could then be checked only against the folded record, not at load.

## Open Issues

- **A layer-owned axis bounded by the list**, the stack's design today: a layer writes the axis, `add` refuses a label outside the list and `remove` works. It lets a tool or layer choose which of its labels exist and lets a `one_of` dim carry properties, at the cost of a file restating the schema and a category that may name no live label.
- **Whether a `one_of` dimension may grow in a layer.** Its labels are the schema's, so a custom PyPSA component is a schema change, not a data edit. That is the cost of checking categories at load.
