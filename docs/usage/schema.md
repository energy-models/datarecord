<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

One schema per record, written down as `manifest.json` ([design](../design/schema.md)).

```python
from datarecord import Schema, Dimension, AttributeSpec, PropertySpec, Group

schema = Schema(
    version=1,
    dimensions={
        "entity": Dimension(dtype="str"),
        "ctype": Dimension(one_of=["Generator", "Link", "Load", "StorageUnit"]),
        "bus": Dimension(dtype="str"),
        "carrier": Dimension(dtype="str"),
        "country": Dimension(dtype="str"),
        "scenario": Dimension(dtype="str"),
        "timestep": Dimension(dtype="datetime"),
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
        "connection": {"role": PropertySpec(dtype="str")},
    },
    groups={"connection": Group(over={"entity": "entity", "bus": "bus"})},
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

- **`Dimension`** declares one axis: its `dtype`, a mathspec dtype — `"float"`, `"int"`, `"str"` or `"datetime"` — and, for a `"str"` dim, `one_of=[...]` listing its labels: `add` then refuses a label the list lacks ([design](../design/schema.md#dtypes)). A dim is flat: a label scoped per parent — a multi-period snapshot — is a label unique across the dim plus a property that takes its labels from the parent, `snapshot.period: {"one_of": "period"}` ([design](../design/schema.md#a-dim-is-flat)). `entity` is a dim like any other.
- **`properties`** is metadata: per subject — a dim or a group — the values that identify, classify or describe one of its labels, one value per label and never varying along another axis. Each is a column of the subject's own file, `dims/entity.parquet` or `groups/connection.parquet`, so its name is the subject's alone and it is addressed as `subject.name` — `entity.carrier`, `bus.carrier`, `connection.role` ([design](../design/schema.md#properties)). A property may share its name with an attribute, a dim or another subject's property: `bus.x` is not the attribute `x`, nor `entity.carrier` the dim `carrier`; one named like its subject's key, or a group's coordinate, is refused.
- **A property may take its labels from a dim**, `one_of="<dim>"` in place of `dtype`: its values are that dim's labels, typed as the dim. `entity.carrier` and `bus.carrier` above take their labels from `carrier`, `bus.country` from `country`; `set` and `add` refuse one the record lacks, NULL meaning none, and `remove("country", ...)` is refused while a bus still holds the label. A property declares exactly one of `one_of` and `dtype`, which is `"float"`, `"int"`, `"bool"` or `"str"`; an attribute's dtypes are the same, and it takes no `one_of` ([design](../design/schema.md#properties)).

  ```python
  w.add("country", ["DE"])
  w.add("bus", ["north"], country="DE")
  w.set("bus.country", "FR", bus="north")  # refused: no `country` label FR
  ```

- **A property of a `partial` dim that takes its labels from a dim listing them categorizes it**: its values are the dim's categories, which `default_by` keys on by its dotted name and a `set` or `flags` keyword selects by. `ctype` above is one, `entity.ctype` taking its labels from `ctype`, which lists them; a dim may have several, and no name is special. One that takes its labels from a dim listing none, `entity.carrier`, never categorizes: its labels are data. `Schema.categorizing("entity")` lists them ([design](../design/schema.md#categories-labels-a-dim-lists)).
- **`default_by`** overrides `default` per category, on properties and attributes alike: the first key listing the label's category wins. `Schema.default_for("p_min_pu", ctype="StorageUnit")` answers `-1.0` ([design](../design/schema.md#default_by-a-default-per-category)).
- **`attributes` is data**, and flat — one name, one spec, record-wide, results included; a property may share an attribute's name, `Schema.resolve` telling `x` from `bus.x`. **An attribute reaches every label its `dims` address**, whatever its category, so `p_nom` and `p_max_pu` above reach every entity. A value for some kinds of label is scoped by the write, one row per label; which kinds a framework reads a name for is the tool's business ([design](../design/schema.md#an-attribute-reaches-every-label-it-addresses)):

  ```python
  w.set("p_max_pu", 0.9, ctype="Generator")  # one row per generator
  w.set("p_max_pu", 0.9, ctype=["Generator", "StorageUnit"])
  w.set("p_max_pu", 0.9)  # every entity; PyPSA reads no load's
  ```

- **`AttributeSpec.dims`** is the only addressing mechanism, naming dims and groups alike — `efficiency` is a connection attribute because its `dims` name the group, carried as one struct column ([design](../design/schema.md#addressing-dims-x)). It is what makes a scenario-varying `p_nom` a violation rather than data. Every attribute lands in `attributes/`, whatever its `dims`, so widening one is a compatible change that moves no file.
- **`Group`** declares which tuples over several dims exist, mapping coordinate name → dim; a list is sugar where the two coincide. `connection` over `(entity, bus)` is the one every network has. A group classifies its tuples with a property that takes its labels from a dim, `bus_country.country: {"one_of": "country"}` over `[bus, scenario]` ([design](../design/schema.md#a-group-that-classifies)).
- **`partial`** is the layering granularity — which dims and groups a layer may patch value by value, and the fold key. `entity` in it lets a layer patch one entity, and is required for its categories to key defaults. `bus` and `scenario` are patchable, `carrier` restated whole, a few rows; `timestep` is not patchable, so a patch to one hour restates that entity's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). A group is the same: with `connection` in it a layer patches one connection's `efficiency`, without it the layer restates every connection's. `partial` is about attribute values alone: a layer changing an axis or a group restates it whole either way ([design](../design/layers.md#deletion)).
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
