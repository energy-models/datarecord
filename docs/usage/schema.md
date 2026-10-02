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
        "port": Dimension(dtype=nw.String()),
        "scenario": Dimension(dtype=nw.String()),
        "timestep": Dimension(dtype=nw.Datetime()),
    },
    relations={
        "entity_type": Relation(key=["entity"], values="entity_type"),
        "port_entity": Relation(key=["port"], values="entity"),
        "port_bus": Relation(key=["port"], values="bus"),
    },
    attributes={
        "p_nom": AttributeSpec(
            dtype=nw.Float64(), dims={"entity"}, default=0.0, unit="MW"
        ),
        "carrier": AttributeSpec(dtype=nw.String(), dims={"entity"}),
        "p_max_pu": AttributeSpec(
            dtype=nw.Float64(), dims={"entity", "scenario", "timestep"}, default=1.0
        ),
        "role": AttributeSpec(dtype=nw.String(), dims={"port"}),
        "efficiency": AttributeSpec(
            dtype=nw.Float64(), dims={"port", "timestep"}, default=1.0
        ),
    },
    partial={"entity", "port", "scenario"},
)
```

- **`Dimension`** declares one axis: its `dtype` (a narwhals dtype, translated to its DuckDB name — a DuckDB type name works too, for a type narwhals does not spell) and `within` for an axis whose labels identify a point only inside another's — multi-period time being the case ([design](../design/schema.md#within-an-axis-inside-an-axis)).
- **`Relation`** declares which tuples over several dims exist. Its `key` maps coordinate name → dim; a list is sugar where the two coincide. Adding **`values`** makes the relation _functional_ into that dim — each `key` tuple carrying exactly one of its labels — which is a constraint checkable on write ([design](../design/schema.md#relations)).
- **A type is an ordinary functional relation.** `entity_type` keyed by `["entity"]`, `values="entity_type"`, gives each entity one type, and its rows live in `relations/entity_type.parquet` like those of any relation. An `Enum` pins the vocabulary; a plain `nw.String()` leaves the labels as data. Omit the relation and entities have no types ([design](../design/schema.md#types)).
- **`attributes` is flat** — one attribute, one spec, record-wide. No attribute is narrowed to a type: an attribute over `entity` can be set on any entity, whatever its type.
- **`AttributeSpec.dims`** names dims only, as the `dims` of a mathspec parameter do. Data on a relation's rows goes over a dim of its own: each attachment of a component to a bus is one `port`, `port_entity` and `port_bus` tie it to its component and its bus, and `role` and `efficiency` are over `port` ([design](../design/schema.md#data-on-a-relations-rows)). The schema refuses an attribute over a relation, with a message that names this rewrite. `dims` is what makes a scenario-varying `p_nom` a violation rather than data, and it names the columns of the attribute's own `attributes/<attr>.parquet` ([design](../design/format.md#where-a-value-lives)).
- **`partial`** is the layering granularity — which dims a layer may patch value by value. `scenario` is patchable; `timestep` is not, so a patch to one hour restates that component's whole series rather than leaving a curve resolved across two layers with a hole in it ([design](../design/schema.md#partial-the-granularity-of-an-override)). `partial` must name every dim a relation is keyed by, `entity` and `port` here, since a layer adds or removes one relation row at a time.
- **`unit`** and **`description`** are stored and never interpreted — no conversion, no dimensional analysis. `None` is undeclared, `""` genuinely dimensionless ([design](../design/schema.md#unit-and-description)).

## Compatibility

`schema.compatible_with(other)` answers whether layers written under `other` still read under `self`, returning one reason per incompatibility and an empty list when the change is compatible ([design](../design/schema.md#versioning)).

```python
reasons = new_schema.compatible_with(old_schema)
if reasons:
    raise ValueError("\n".join(reasons))
```
