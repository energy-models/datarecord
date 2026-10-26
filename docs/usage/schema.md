<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Declaring a schema

A record has one schema, written as `manifest.json` ([design](../design/model/schema.md)). The schema declares the dims, the properties of their labels and the attributes. This example declares a small network:

```python
from datarecord import Schema, Dimension, AttributeSpec, PropertySpec

schema = Schema(
    version=1,
    dimensions={
        "entity": Dimension(dtype="str"),
        "ctype": Dimension(one_of=["Generator", "Link", "Load", "StorageUnit"]),
        "role": Dimension(one_of=["input", "output", "attached"]),
        "bus": Dimension(dtype="str"),
        "carrier": Dimension(dtype="str"),
        "country": Dimension(dtype="str"),
        "scenario": Dimension(dtype="str"),
        "timestep": Dimension(dtype="datetime"),
        "connection": Dimension(over={"entity": "entity", "bus": "bus"}),
    },
    properties={
        "entity": {
            "ctype": PropertySpec(one_of="ctype"),
            "carrier": PropertySpec(one_of="carrier"),
            "sign": PropertySpec(
                dtype="float",
                default=1.0,
                default_by={"entity.ctype": {"Load": -1.0}},
            ),
        },
        "bus": {
            "carrier": PropertySpec(one_of="carrier", default="AC"),
            "x": PropertySpec(dtype="float", default=0.0),
            "country": PropertySpec(one_of="country"),
        },
        "connection": {"role": PropertySpec(one_of="role")},
    },
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}, default=0.0, unit="MW"),
        "x": AttributeSpec(dtype="float", dims={"entity"}, default=0.0),
        "co2_emissions": AttributeSpec(dtype="float", dims={"carrier"}, default=0.0),
        "p_max_pu": AttributeSpec(
            dtype="float", dims={"entity", "scenario", "timestep"}, default=1.0
        ),
        "p_min_pu": AttributeSpec(
            dtype="float",
            dims={"entity", "timestep"},
            default=0.0,
            default_by={"entity.ctype": {"StorageUnit": -1.0}},
        ),
        "efficiency": AttributeSpec(
            dtype="float", dims={"connection", "timestep"}, default=1.0
        ),
    },
    partial={"entity", "bus", "scenario", "connection"},
)
```

## Dims

A `Dimension` declares one axis. Its `dtype` is a mathspec dtype: `"float"`, `"int"`, `"str"` or `"datetime"`. A `"str"` dim may take `one_of=[...]`, its labels: the axis is then that list, in order, with no file and no properties, and `add` and `remove` refuse it ([design](../design/model/dims.md#a-dim-with-one_of)).

A multi-period snapshot is a label of `snapshot` with a property that refers to its period, `snapshot.period: {"one_of": "period"}` ([design](../design/model/dims.md#mapping-labels-to-another-dim)).

## Multi-dims

A `Dimension` with `over` is a multi-dim: its labels are tuples of other dims' labels, and it declares which tuples exist. `over` maps each coordinate name to a dim, and a list is short for the case where the two coincide. A multi-dim has no `dtype` and no `one_of`. Above, `connection` is a multi-dim over `(entity, bus)`. `Schema.dimensions` holds every dim, `Schema.multi_dims` the multi-dims and `Schema.dims` the others ([design](../design/model/multi-dims.md)).

`connection.bus` names a coordinate wherever a dotted `subject.name` stands, and `Schema.resolve("connection.bus")` answers `connection` and the dim `bus` ([design](../design/model/multi-dims.md#coordinates)).

A multi-dim's property may refer to a dim, such as `bus_country.country: {"one_of": "country"}` on the multi-dim `bus_country` over `["bus", "scenario"]`, for a bus's country per scenario. No property refers to a multi-dim ([design](../design/model/multi-dims.md#one-struct-column)).

## Properties

`properties` declares, per subject, the values that identify, classify or describe one of its labels. A subject is a dim, multi-dims included. A property holds one value per label and does not vary along another axis. Each property is a column of its subject's own file, `dims/entity.parquet` or `dims/connection.parquet`. A caller addresses it as `subject.name`, such as `entity.carrier`, `bus.carrier` or `connection.role` ([design](../design/model/properties.md#properties)).

A property name belongs to its subject. It may match an attribute, a dim or another subject's property: `bus.x` and the attribute `x` are two names, as are `entity.carrier` and the dim `carrier`. The schema refuses a property named like its subject's key or like one of a multi-dim's coordinates.

A property declares exactly one of `dtype` and `one_of`. Its `dtype` is `"float"`, `"int"`, `"bool"` or `"str"`. With `one_of="<dim>"` it refers to that dim, and its values are typed as the dim. Above, `entity.carrier` and `bus.carrier` refer to `carrier`, and `bus.country` to `country`. `set` and `add` refuse a label the record lacks, and NULL means no label. `remove("country", ...)` is refused while a bus still holds the label ([design](../design/model/properties.md#referring-to-a-dim)).

```python
w.add("country", ["DE"])
w.add("bus", ["north"], country="DE")
w.set("bus.country", "FR", bus="north")  # refused: no `country` label FR
```

## Categories and defaults

A property of a dim in `partial`, a multi-dim among them, that refers to a dim with `one_of` categorizes its subject, as `connection.role` categorizes `connection`. Its values are the subject's categories. `default_by` keys on them by the property's dotted name, and a `set` or `flags` keyword selects by them. Above, `entity.ctype` refers to `ctype`, which has `one_of`, so it categorizes `entity`. A dim may have several categorizing properties. `entity.carrier` refers to `carrier`, which has no `one_of`, so its labels are data and it categorizes nothing. `Schema.categorizing("entity")` lists the categorizing properties ([design](../design/model/properties.md#categories)).

`default_by` overrides `default` per category, on properties and attributes alike. The first key whose map holds the label's category wins. `Schema.default_for("p_min_pu", ctype="StorageUnit")` answers `-1.0` ([design](../design/model/properties.md#default_by-a-default-per-category)).

## Attributes

`attributes` holds the data. One name is one spec, record-wide. `Schema.resolve` tells the attribute `x` from the property `bus.x`.

A NULL coordinate reaches every label of its dim, so a value set with no `entity` reaches every entity. A property keyword scopes a value to some labels, one row per label. Which labels a framework reads a name for is the tool's business ([design](../design/model/attributes.md#the-broadcast-rule)):

```python
w.set("p_max_pu", 0.9, ctype="Generator")  # one row per generator
w.set("p_max_pu", 0.9, ctype=["Generator", "StorageUnit"])
w.set("p_max_pu", 0.9)  # every entity; PyPSA reads no load's
```

`AttributeSpec.dims` is the only addressing mechanism, and it names multi-dims as it names any dim. `efficiency` names the multi-dim `connection` in its `dims`, and its rows carry it as one struct column ([design](../design/model/multi-dims.md#attributes-over-a-multi-dim)). `dims` makes a scenario-varying `p_nom` a schema violation. Every attribute lands in `attributes/`, whatever its `dims`, so widening `dims` is a compatible change that moves no file.

## `partial`

`partial` names the dims that a layer may patch value by value, and it is the fold key. `entity` in `partial` lets a layer patch one entity, and its categories need it to key defaults. Above, `bus` and `scenario` are patchable, and a layer restates `carrier` whole, which is a few rows. `timestep` is not patchable, so a patch to one hour restates that entity's whole series ([design](../design/layers/tree.md#partial-the-granularity-of-an-override)). A multi-dim follows the same rule. With `connection` in `partial` a layer patches one connection's `efficiency`, and without it the layer restates every connection's.

`partial` applies to attribute values. A layer that changes an axis, a multi-dim's included, restates it whole either way ([design](../design/layers/removal.md)).

## `unit` and `description`

`unit` and `description` are stored and never interpreted. The package does no unit conversion and no dimensional analysis. `None` means undeclared, and `""` means the quantity has no unit ([design](../design/model/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`. It returns one reason per incompatibility, and an empty list when the change is compatible ([design](../design/model/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
