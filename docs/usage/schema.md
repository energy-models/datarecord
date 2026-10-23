<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

One schema per record, written down as `manifest.json` ([design](../design/schema.md)).

```python
import narwhals as nw
from datarecord import Schema, Dimension, AttributeSpec, PropertySpec, Group

schema = Schema(
    version=1,
    dimensions={
        "entity": Dimension(dtype=nw.String()),
        "bus": Dimension(dtype=nw.String()),
        "carrier": Dimension(dtype=nw.String()),
        "country": Dimension(dtype=nw.String()),
        "scenario": Dimension(dtype=nw.String()),
        "timestep": Dimension(dtype=nw.Datetime()),
    },
    properties={
        "entity": {
            "ctype": PropertySpec(
                dtype=nw.Enum(["Generator", "Link", "Load", "StorageUnit"])
            ),
            "carrier": PropertySpec(into="carrier"),
            "sign": PropertySpec(
                dtype=nw.Float64(),
                default=1.0,
                default_by={"entity.ctype": {"Load": -1.0}},
            ),
        },
        "bus": {
            "carrier": PropertySpec(into="carrier", default="AC"),
            "x": PropertySpec(dtype=nw.Float64(), default=0.0),
            "country": PropertySpec(into="country"),
        },
        "connection": {"role": PropertySpec(dtype=nw.String())},
    },
    groups={"connection": Group(over={"entity": "entity", "bus": "bus"})},
    attributes={
        "p_nom": AttributeSpec(
            dtype=nw.Float64(), dims={"entity"}, default=0.0, unit="MW"
        ),
        "x": AttributeSpec(dtype=nw.Float64(), dims={"entity"}, default=0.0),
        "co2_emissions": AttributeSpec(
            dtype=nw.Float64(), dims={"carrier"}, default=0.0
        ),
        "p_max_pu": AttributeSpec(
            dtype=nw.Float64(), dims={"entity", "scenario", "timestep"}, default=1.0
        ),
        "p_min_pu": AttributeSpec(
            dtype=nw.Float64(),
            dims={"entity", "timestep"},
            default=0.0,
            default_by={"entity.ctype": {"StorageUnit": -1.0}},
        ),
        "efficiency": AttributeSpec(
            dtype=nw.Float64(), dims={"connection", "timestep"}, default=1.0
        ),
    },
    partial={"entity", "bus", "scenario", "connection"},
)
```

- **`Dimension`** declares one axis: its `dtype` (a narwhals dtype, translated to its DuckDB name — a DuckDB type name works too, for a type narwhals does not spell). A dim is flat: a label scoped per parent — a multi-period snapshot — is a label unique across the dim plus a property `into` the parent, `snapshot.period` into `period` ([design](../design/schema.md#a-dim-is-flat)). `entity` is a dim like any other.
- **`properties`** is metadata: per subject — a dim or a group — the values that identify, classify or describe one of its labels, one value per label and never varying along another axis. Each is a column of the subject's own file, `dims/entity.parquet` or `groups/connection.parquet`, so its name is the subject's alone and it is addressed as `subject.name` — `entity.carrier`, `bus.carrier`, `connection.role` ([design](../design/schema.md#properties)). A property may share its name with an attribute, a dim or another subject's property: `bus.x` is not the attribute `x`, nor `entity.carrier` the dim `carrier`; one named like its subject's key, or a group's coordinate, is refused.
- **A property may be `into` a dim**: its values are that dim's labels, its dtype the dim's. `entity.carrier` and `bus.carrier` above name labels of `carrier`, `bus.country` one of `country`; `set` and `add` refuse one the record lacks, NULL meaning none, and `remove("country", ...)` is refused while a bus still names the label. It never categorizes: a category is a declared `Enum`, a label is data ([design](../design/schema.md#properties)).

  ```python
  w.add("country", ["DE"])
  w.add("bus", ["north"], country="DE")
  w.set("bus.country", "FR", bus="north")  # refused: no `country` label FR
  ```

- **An `Enum` property of a `partial` dim categorizes it**: its values are the dim's categories, which `default_by` keys on by its dotted name and a `set` or `flags` keyword selects by. `ctype` above is one; a dim may have several, and no name is special. `Schema.categorizing("entity")` lists them ([design](../design/schema.md#categories-an-enum-property-of-a-partial-dim)).
- **`default_by`** overrides `default` per category, on properties and attributes alike: the first key listing the label's category wins. `Schema.default_for("p_min_pu", ctype="StorageUnit")` answers `-1.0` ([design](../design/schema.md#default_by-a-default-per-category)).
- **`attributes` is data**, and flat — one name, one spec, record-wide, results included; a property may share an attribute's name, `Schema.resolve` telling `x` from `bus.x`. **An attribute reaches every label its `dims` address**, whatever its category, so `p_nom` and `p_max_pu` above reach every entity. A value for some kinds of label is scoped by the write, one row per label; which kinds a framework reads a name for is the tool's business ([design](../design/schema.md#an-attribute-reaches-every-label-it-addresses)):

  ```python
  w.set("p_max_pu", 0.9, ctype="Generator")  # one row per generator
  w.set("p_max_pu", 0.9, ctype=["Generator", "StorageUnit"])
  w.set("p_max_pu", 0.9)  # every entity; PyPSA reads no load's
  ```

- **`AttributeSpec.dims`** is the only addressing mechanism, naming dims and groups alike — `efficiency` is a connection attribute because its `dims` name the group, carried as one struct column ([design](../design/schema.md#addressing-dims-x)). It is what makes a scenario-varying `p_nom` a violation rather than data. Every attribute lands in `attributes/`, whatever its `dims`, so widening one is a compatible change that moves no file.
- **`Group`** declares which tuples over several dims exist, mapping coordinate name → dim; a list is sugar where the two coincide. `connection` over `(entity, bus)` is the one every network has. Adding **`into`** declares the group _functional_ into that dim — each `over` tuple carrying exactly one of its labels — which is a constraint checkable on write; it needs two coordinates or more, one being a property `into` the dim ([design](../design/schema.md#into-a-group-that-classifies)).
- **`partial`** is the layering granularity — which dims and groups a layer may patch value by value, and the fold key. `entity` in it lets a layer patch one entity, and is required for its categories to key defaults. `bus` and `scenario` are patchable, `carrier` restated whole, a few rows; `timestep` is not patchable, so a patch to one hour restates that entity's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). A group is the same: with `connection` in it a layer patches one connection's `efficiency`, without it the layer restates every connection's.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
