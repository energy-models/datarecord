<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Declare a schema

This guide shows how to declare each part of a [schema](../reference/schema.md), and how to check a changed schema against the old one. The [data model](../reference/data-model.md) defines each term.

This schema has every part the sections below use:

```python
from datarecord import AttributeSpec, Dimension, PropertySpec, Schema

schema = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "ctype": Dimension(one_of=["Generator", "Load", "StorageUnit"]),
        "bus": Dimension(dtype="str"),
        "carrier": Dimension(dtype="str"),
        "snapshot": Dimension(dtype="datetime"),
        "connection": Dimension(over=["entity", "bus"]),
    },
    properties={
        "entity": {
            "ctype": PropertySpec(one_of="ctype"),
            "carrier": PropertySpec(one_of="carrier"),
            "sign": PropertySpec(
                dtype="float", default=1.0, default_by={"entity.ctype": {"Load": -1.0}}
            ),
        },
        "bus": {
            "carrier": PropertySpec(one_of="carrier", default="AC"),
            "x": PropertySpec(dtype="float"),
        },
    },
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}, default=0.0, unit="MW"),
        "p_max_pu": AttributeSpec(
            dtype="float", dims={"entity", "snapshot"}, default=1.0, unit="per unit"
        ),
        "p_min_pu": AttributeSpec(
            dtype="float",
            dims={"entity", "snapshot"},
            default=0.0,
            default_by={"entity.ctype": {"StorageUnit": -1.0}},
        ),
        "efficiency": AttributeSpec(dtype="float", dims={"connection"}, default=1.0),
    },
    partial={"entity", "bus", "connection"},
)
```

## Declare the dims

Give each dim a `Dimension` with a `dtype` of `"str"`, `"int"`, `"float"` or `"datetime"`. Where the schema fixes the labels, as for `ctype`, pass `one_of` with the list instead. Such a dim has no axis file and no properties, and `add` refuses its labels.

## Declare a multi-dim

Pass `over` with the dims whose labels form its tuples. A list is short for a mapping in which each field is named after its dim:

```python
print(schema.multi_dims, schema.over("connection"))
```

```text
('connection',) {'entity': 'entity', 'bus': 'bus'}
```

`over("connection")` returns the mapping that the list stands for. To name a field differently from its dim, pass the mapping, as in `Dimension(over={"from": "bus", "to": "bus"})` for a line between two buses.

## Declare properties

Under `properties`, key each property by its subject dim and give it exactly one of `dtype` and `one_of`. With `one_of`, the property's values are labels of the dim you give, as the values of `entity.carrier` are labels of `carrier`. A property name belongs to its subject, so `bus.x` and an attribute `x` are two names.

datarecord refuses a spec with both:

```python
PropertySpec(one_of="carrier", dtype="str")
```

```text
pydantic_core._pydantic_core.ValidationError: 1 validation error for PropertySpec
  Value error, a property declares exactly one of `dtype` and `one_of`, not both [type=value_error, input_value={'one_of': 'carrier', 'dtype': 'str'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.13/v/value_error
```

## Set a default per category

A `default_by` overrides `default` for some categories, on a property or an attribute alike. Key it on the dotted name of a [categorising property](../reference/data-model.md#categories), such as `entity.ctype`:

```python
print(schema.categorizing("entity"))
print(schema.default_for("p_min_pu", ctype="StorageUnit"))
print(schema.default_for("entity.sign", ctype="Load"))
```

```text
('ctype',)
-1.0
-1.0
```

`ctype` is the one categorising property of `entity`, and a storage unit's `p_min_pu` and a load's `sign` both default to `-1.0`. If the subject dim is outside `partial`, datarecord refuses the `default_by`:

```python
Schema(**schema.model_dump() | {"partial": {"bus", "connection"}})
```

```text
pydantic_core._pydantic_core.ValidationError: 1 validation error for Schema
  Value error, property entity.sign's `default_by` is keyed by 'entity.ctype', which is a property of 'entity', which `partial` does not list [type=value_error, input_value={'version': 1, 'dimension...onnection'}, 'meta': {}}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.13/v/value_error
```

## Declare attributes

Give each attribute a `dtype` and the `dims` it varies over, multi-dims included, and a `default` for a coordinate that no row covers. `unit` and `description` are stored as written and never interpreted:

```python
print(schema.dims_of("efficiency"))
```

```text
('connection',)
```

`efficiency` is keyed by the one dim `connection`, a multi-dim whose tuples are structs of `entity` and `bus`. datarecord refuses a dim in `dims` that the schema does not declare:

```python
Schema(
    dimensions={"entity": Dimension(dtype="str")},
    attributes={"p_nom": AttributeSpec(dtype="float", dims={"entity", "bus"})},
)
```

```text
pydantic_core._pydantic_core.ValidationError: 1 validation error for Schema
  Value error, attribute 'p_nom' is addressed by undeclared dims ['bus'] [type=value_error, input_value={'dimensions': {'entity':...}), breakpoints=False)}}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.13/v/value_error
```

## Choose `partial`

List in `partial` the dims along which a child layer overrides one label's value. Leave out a dim whose values change together, such as `snapshot` for a time series. [Layers](../reference/layers.md) states the rule.

`PyPSA.verify` reports `snapshot` in `partial`, and `PyPSA.build` raises for it ([tools](../reference/tools.md)).

## Check a changed schema against the old one

`compatible_with` returns one reason per change that stops datarecord reading layers written under the old schema with the new one. An empty list means datarecord can still read them:

```python
cost = AttributeSpec(dtype="float", dims={"entity", "snapshot"})
wider = Schema(
    **schema.model_dump() | {"attributes": schema.attributes | {"marginal_cost": cost}}
)
retyped = Schema(
    **schema.model_dump()
    | {
        "attributes": schema.attributes
        | {"p_nom": AttributeSpec(dtype="int", dims={"entity"})}
    }
)
print(wider.compatible_with(schema))
print(retyped.compatible_with(schema))
```

```text
[]
["attribute 'p_nom' dtype float -> int"]
```

Adding an attribute is compatible, and retyping one is not. `store.commit` runs the same check against the parent's schema, and raises a `ValueError` that lists the reasons.
