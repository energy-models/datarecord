# The schema

One schema per record, written down as `manifest.json` ([design](../design/schema.md)).

```python
from datarecord import Schema, Dimension, AttributeSpec, Group, ComponentType

schema = Schema(
    version=1,
    dimensions={
        "entity": Dimension(dtype="VARCHAR"),
        "bus": Dimension(dtype="VARCHAR"),
        "scenario": Dimension(dtype="VARCHAR"),
        "timestep": Dimension(dtype="TIMESTAMP"),
    },
    groups={"connection": Group(over={"entity": "entity", "bus": "bus"})},
    attributes={
        "p_nom": AttributeSpec(dtype="DOUBLE", dims={"entity"}, default=0.0, unit="MW"),
        "carrier": AttributeSpec(dtype="VARCHAR", dims={"entity"}),
        "p_max_pu": AttributeSpec(
            dtype="DOUBLE", dims={"entity", "scenario", "timestep"}, default=1.0
        ),
        "efficiency": AttributeSpec(
            dtype="DOUBLE", dims={"connection", "timestep"}, default=1.0
        ),
    },
    traits={"dispatchable": {"p_max_pu"}},
    component_types={
        "Bus": ComponentType(),
        "Generator": ComponentType(
            traits={"dispatchable"}, attributes={"p_nom", "carrier"}
        ),
        "Link": ComponentType(attributes={"efficiency"}),
    },
    partial={"entity", "bus", "scenario"},
)
```

- **`Dimension`** declares one axis: its `dtype`, `on` for a [mapping](../design/schema.md#on-a-mapping-over-another-axis) classifying another axis's labels, and `within` for an axis whose labels identify a point only inside another's — multi-period time being the case ([design](../design/schema.md#within-an-axis-inside-an-axis)).
- **`attributes` is flat** — one attribute, one spec, record-wide — and a **`ComponentType` subscribes** to attributes and to `traits`, which are named bundles. `Schema.attributes_for("Generator")` resolves the two into what that type carries ([design](../design/schema.md#traits)).
- **`Group`** declares which tuples over several dims exist, mapping coordinate name → dim. `connection` over `(entity, bus)` is the one every network has ([design](../design/schema.md#groups)).
- **`AttributeSpec.dims`** is the only addressing mechanism, naming dims and groups alike — `efficiency` is a connection attribute because its `dims` name the group. It is what makes a scenario-varying `p_nom` a violation rather than data, and it decides the file split: naming exactly one coordinate puts an attribute on that thing's own table, anything more in `inputs/` ([design](../design/format.md#where-a-value-lives)).
- **`partial`** is the layering granularity — which dims a layer may patch value by value. `scenario` is patchable; `timestep` is not, so a patch to one hour restates that component's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). `entity` and every group coordinate must be in it, since [neither broadcasts](../design/record.md#the-broadcast-rule). Omit it entirely for a record with no layers.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
