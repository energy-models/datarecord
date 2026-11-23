<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

This page describes every key a schema accepts, in the order a schema declares them, and what datarecord refuses at load.
A **schema** is the `Schema` of a record: the dims, properties, attributes and `partial` that may exist, with their dtypes.
The [data model](data-model.md) defines each of those words.

The running example declares this schema:

```python
from datarecord import AttributeSpec, Dimension, PropertySpec, Schema

schema = Schema(
    dimensions={
        "entity": Dimension(),
        "bus": Dimension(),
        "carrier": Dimension(),
        "snapshot": Dimension(dtype="int"),
        "ctype": Dimension(one_of=["Generator", "Link", "Load"]),
        "connection": Dimension(over=["entity", "bus"]),
    },
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}, default=0.0, unit="MW"),
        "efficiency": AttributeSpec(dtype="float", dims={"connection"}, default=1.0),
        "p_max_pu": AttributeSpec(
            dtype="float",
            dims={"entity", "snapshot"},
            default=1.0,
            default_by={"entity.ctype": {"Load": 0.0}},
        ),
    },
    properties={
        "entity": {
            "ctype": PropertySpec(one_of="ctype"),
            "carrier": PropertySpec(one_of="carrier"),
        },
    },
    partial={"entity", "bus", "connection"},
)
```

`Schema.default_for("p_max_pu", ctype="Load")` returns `0.0`, and `Schema.default_for("p_max_pu")` returns `1.0`.

A `Schema` and each spec are frozen pydantic models that refuse an undeclared key.
Every refusal on this page raises when the schema is built or read from a manifest.

## `version`

`version` is an integer, `1` by default, that you raise with each change to the declarations.
datarecord stores it and compares nothing against it.
[Compatibility](#compatibility) lists the changes a child revision's schema may make.

## `dimensions`

`dimensions` maps each dim's name to a `Dimension`, multi-dims among them.

| Key           | Value                                                         | Default |
| ------------- | ------------------------------------------------------------- | ------- |
| `over`        | a multi-dim's coordinates, each mapped to the dim it draws on | `None`  |
| `dtype`       | `"float"`, `"int"`, `"str"` or `"datetime"`                   | `"str"` |
| `one_of`      | the dim's labels, in order                                    | `None`  |
| `unit`        | what the labels measure                                       | `None`  |
| `description` | what the dim is, in prose                                     | `None`  |

A schema that declares attributes declares at least one dim, because attribute data over no dim is no record.

### `over`

`over` declares a [multi-dim](data-model.md#multi-dims).
A list is short for the dict whose keys equal its values, so `over=["entity", "bus"]` is `over={"entity": "entity", "bus": "bus"}`.
The keys are the coordinate names, in field order, and `Schema.over("connection")` returns them.
datarecord refuses a multi-dim that also declares `dtype` or `one_of`, and a coordinate that draws on an undeclared dim or on a multi-dim.

### `one_of`

`one_of` declares a [dim with `one_of`](data-model.md#a-dim-with-one_of), whose axis is the list.
Only a `"str"` dim takes it.
datarecord refuses an empty list, a label listed twice, and `one_of` on any other dtype.

## `attributes`

`attributes` maps each attribute's name to an `AttributeSpec`.
One name is one spec, one file and one `value` dtype, record-wide.

| Key           | Value                                             | Default       |
| ------------- | ------------------------------------------------- | ------------- |
| `dtype`       | `"float"`, `"int"`, `"bool"` or `"str"`           | required      |
| `dims`        | the dims and multi-dims that address a value      | `frozenset()` |
| `default`     | the value at a point no row covers                | `None`        |
| `default_by`  | a default per category, which overrides `default` | `{}`          |
| `breakpoints` | whether a row may have a piecewise-linear curve   | `False`       |
| `unit`        | what the values measure                           | `None`        |
| `description` | what the attribute is, in prose                   | `None`        |

datarecord refuses a `dims` entry that is not a declared dim.
It also refuses a multi-dim in `dims` beside a dim named like one of its coordinates, such as `{"connection", "bus"}`.

### `default_by`

`default_by` maps a categorising property, written `subject.name`, to a map from category to default.
At a label whose category a map lists, the first key in declaration order gives the default.
Elsewhere `default` applies.
`Schema.default_for(name, **categories)` returns the default for an attribute or a `subject.name` property, taking each category by the property's plain name.

datarecord refuses a key that is not a [categorising property](data-model.md#categories), naming the reason:

```text
attribute 'p_nom''s `default_by` is keyed by 'entity.carrier', which refers to 'carrier', but 'carrier' is not a one_of dim
```

It also refuses a category that the referred dim's `one_of` does not list.
It refuses a key on a dim that the attribute or property is not [indexed over](data-model.md#attributes).
A property is indexed over its subject and the dims that the subject's coordinates draw on.

## `properties`

`properties` maps each subject dim to a map from property name to `PropertySpec`.

| Key           | Value                                        | Default |
| ------------- | -------------------------------------------- | ------- |
| `dtype`       | `"float"`, `"int"`, `"bool"` or `"str"`      | `None`  |
| `one_of`      | the dim the property refers to               | `None`  |
| `default`     | the value for a label no layer wrote one for | `None`  |
| `default_by`  | a default per category, as on an attribute   | `{}`    |
| `unit`        | what the values measure                      | `None`  |
| `description` | what the property is, in prose               | `None`  |

A property declares exactly one of `dtype` and `one_of`.
With `one_of`, the property [refers to that dim](data-model.md#properties) and takes its type.
`Schema.resolve("entity.carrier")` returns the subject and the spec, and `Schema.categorizing(dim)` lists the categorising properties of `dim`.

datarecord refuses a property:

- that declares both `dtype` and `one_of`, or neither
- of an undeclared subject, or of a dim with `one_of`
- that refers to an undeclared dim or to a multi-dim
- named like its subject's key column, or like a coordinate of its multi-dim
- that categorises its subject and declares `default_by`

## `partial`

`partial` is the set of dims along which the data one revision adds (a [layer](layers.md)) overrides one value per label, a multi-dim among them.
[Layers](layers.md#partial) states what a layer restates along any other dim.
`None`, the default, declares a record with no layers.
datarecord refuses a name in `partial` that the schema does not declare as a dim.

## `meta`

`meta` is a dict of a framework's own data, such as network attributes or a coordinate reference system.
datarecord stores it and never reads it.

## `unit` and `description`

A `Dimension`, an `AttributeSpec` and a `PropertySpec` each take a `unit` and a `description`.
datarecord stores both and reads neither, so it converts no unit.
`None` means undeclared, and `""` declares a quantity without a unit.

## Dtypes

A dtype is mathspec's `DimensionDtype` or `ParameterDtype`, written as its string.
Each declaration maps to one DuckDB type:

| Declares                 | Declaration        | DuckDB type                                       |
| ------------------------ | ------------------ | ------------------------------------------------- |
| dim, attribute, property | `dtype: float`     | `DOUBLE`                                          |
| dim, attribute, property | `dtype: int`       | `BIGINT`                                          |
| attribute, property      | `dtype: bool`      | `BOOLEAN`                                         |
| dim, attribute, property | `dtype: str`       | `VARCHAR`                                         |
| dim                      | `dtype: datetime`  | `TIMESTAMP` (no time zone)                        |
| dim                      | `one_of: [labels]` | `ENUM` of the labels                              |
| multi-dim                | `over: {...}`      | `STRUCT`, a field per coordinate typed as its dim |
| property                 | `one_of: <dim>`    | that dim's type                                   |

An attribute takes no `one_of`, and no attribute or property is `datetime`.
A property whose values are times refers to a `datetime` dim.
The format fixes three more columns: `attribute VARCHAR`, `breakpoint DOUBLE` and `value`, typed as the attribute's `dtype`.

## Names

A declared dim, coordinate, attribute or property name becomes a column, and datarecord refuses a name that cannot be one:

- a name that is not ASCII letters, digits and `_`, starting with a letter, which is mathspec's rule for a name
- a column datarecord writes, compared ignoring case: `attribute`, `breakpoint`, `breakpoints`, `broadcast`, `layer_uuid`, `order_key`, `value` and `varies`
- a name that starts with `_`
- two names that differ only in case, which DuckDB reads as one column

One spelling declared twice is one column and passes, as when a coordinate shares its dim's name.
Labels are data, and these rules do not apply to them.

## Compatibility

`schema.compatible_with(other)` returns one reason for each way that layers written under `other` would not read under `schema`, and an empty list when they would.
`Store.commit` checks a child's schema against its parent's this way and refuses an incompatible one before it writes any data.

| Change                                                    | Compatible |
| --------------------------------------------------------- | ---------- |
| add a dim, an attribute or a property                     | yes        |
| add a dim to an attribute's `dims`                        | yes        |
| add a dim to `partial`                                    | yes        |
| change a `default`, `default_by`, `unit` or `description` | yes        |
| remove a dim, an attribute or a property                  | no         |
| change a `dtype`, a `one_of` or an `over`                 | no         |
| remove a dim from an attribute's `dims`                   | no         |
| remove a dim from `partial`                               | no         |

Moving a name between `attributes` and `properties`, or between subjects, removes it, so `compatible_with` reports it.
An incompatible change needs the record rewritten under the new schema, such as a standalone directory written with `ws.write`.

## One schema per record

A record is read under the schema of its last layer.
Each layer's file of its id, parent and files (its [manifest](format.md)) has the full schema the layer was written under.
A root's schema is free, and a child's passes `compatible_with` against its parent's, so two sibling branches may declare one new name differently.
A `WorkingRecord` starts under its base's schema.
[`declare`](editing.md#declare) widens it by a dim, a property or an attribute, within what `compatible_with` accepts, and `partial` stays the base's.

!!! info "See also"

    - [The data model](data-model.md) defines what each key declares.
    - [Layers](layers.md) describes `partial` and what a layer restates.
    - [Declare a schema](../howto/declare-a-schema.md) shows the steps.
    - The [`Schema` API](api/schema.md) lists every method.
