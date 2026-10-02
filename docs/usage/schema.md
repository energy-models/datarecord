<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

One schema per record, written down as `manifest.json` ([design](../design/schema.md)).

```python
import narwhals as nw
from datarecord import Schema, Dimension, AttributeSpec, Relation

schema = Schema(
    version=1,
    dimensions={
        "entity": Dimension(dtype=nw.String()),
        "entity_type": Dimension(dtype=nw.Enum(["Bus", "Generator", "Link"])),
        "bus": Dimension(dtype=nw.String()),
        "scenario": Dimension(dtype=nw.String()),
        "timestep": Dimension(dtype=nw.Datetime()),
    },
    relations={
        "connection": Relation(key={"entity": "entity", "bus": "bus"}),
        "entity_type": Relation(key=["entity"], values="entity_type"),
    },
    attributes={
        "p_nom": AttributeSpec(
            dtype=nw.Float64(), dims={"entity"}, default=0.0, unit="MW"
        ),
        "carrier": AttributeSpec(dtype=nw.String(), dims={"entity"}),
        "p_max_pu": AttributeSpec(
            dtype=nw.Float64(), dims={"entity", "scenario", "timestep"}, default=1.0
        ),
        "efficiency": AttributeSpec(
            dtype=nw.Float64(), dims={"connection", "timestep"}, default=1.0
        ),
    },
    partial={"scenario"},
)
```

- **`Dimension`** declares one axis: its `dtype` (a narwhals dtype, translated to its DuckDB name — a DuckDB type name works too, for a type narwhals does not spell) and `within` for an axis whose labels identify a point only inside another's — multi-period time being the case ([design](../design/schema.md#within-an-axis-inside-an-axis)).
- **`Relation`** declares which tuples over several dims exist. Its `key` maps coordinate name → dim; a list is sugar where the two coincide. `connection` keyed by `(entity, bus)` is the one every network has. Adding **`values`** makes the relation _functional_ into that dim — each `key` tuple carrying exactly one of its labels — which is a constraint checkable on write ([design](../design/schema.md#relations)).
- **A type is an ordinary functional relation.** `entity_type` keyed by `["entity"]`, `values="entity_type"`, gives each entity one type, and its rows live in `relations/entity_type.parquet` like those of any relation. An `Enum` pins the vocabulary; a plain `nw.String()` leaves the labels as data. Omit the relation and entities have no types ([design](../design/schema.md#types)).
- **`attributes` is flat** — one attribute, one spec, record-wide. No attribute is narrowed to a type: every declared attribute can be set on any entity its `dims` address.
- **`AttributeSpec.dims`** is the only addressing mechanism, naming dims and relations alike — `efficiency` is a connection attribute because its `dims` name the relation. A name is the dim of that name if one is declared, and otherwise the relation expanded to its columns ([design](../design/schema.md#addressing-dims-x)). It is what makes a scenario-varying `p_nom` a violation rather than data, and it decides the file split: naming exactly one coordinate puts an attribute on that thing's own table, anything more in `attributes/` ([design](../design/format.md#where-a-value-lives)).
- **`partial`** is the layering granularity — which dims a layer may patch value by value. `scenario` is patchable; `timestep` is not, so a patch to one hour restates that component's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). `entity` and every relation _key_ coordinate are patched per row already, since [neither broadcasts](../design/record.md#the-broadcast-rule), so `partial` does not name them. Omit it entirely for a record with no layers.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
