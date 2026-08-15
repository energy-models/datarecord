# The schema

One schema per record, written down as `manifest.json` ([design](../design/schema.md)).

```python
from datarecord import Schema, Dimension, AttributeSpec

schema = Schema(
    version=1,
    dimensions={
        "scenario": Dimension(dtype="VARCHAR", keys={"component", "connection"}),
        "timestep": Dimension(dtype="TIMESTAMP"),
    },
    attributes={
        "Bus": {},
        "Generator": {
            "p_nom": AttributeSpec(dtype="DOUBLE", default=0.0, unit="MW"),
            "carrier": AttributeSpec(dtype="VARCHAR"),
            "p_max_pu": AttributeSpec(
                dtype="DOUBLE", dims={"scenario", "timestep"}, default=1.0
            ),
        },
    },
    partial={"scenario"},
)
```

- **`Dimension`** declares one axis: its `dtype`, `on` for a [mapping](../design/schema.md#on-a-mapping-over-another-axis) classifying another axis's labels, and `within` for an axis whose labels identify a point only inside another's — multi-period time being the case ([design](../design/schema.md#within-an-axis-inside-an-axis)).
- **`AttributeSpec.dims`** says which axes an attribute may vary over. It is what makes a scenario-varying `p_nom` a violation rather than data, and it decides the file split: varying over nothing puts an attribute in `dims/components/`, anything else in `inputs/` ([design](../design/schema.md#attributespec)).
- **`partial`** is the layering granularity — which dims a layer may patch value by value. `scenario` is patchable; `timestep` is not, so a patch to one hour restates that component's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). Omit it for a record with no layers.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
