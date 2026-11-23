<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Edit a record

This guide shows how to stage edits on a [`WorkingRecord`](../reference/editing.md), check them, and commit them as a child revision. The examples use `record`, `rev`, `store` and `ws` from [Your first record](../tutorials/first-record.md).

You make a `WorkingRecord` over a committed `Record`, and a read of it returns that record with the edits applied. Each edit runs in its own transaction, so a refused edit stages nothing.

## Set attribute values

Name the attribute, then the value, then a keyword per coordinate. Pass a scalar for one value at each label named, a list for one value per label in order, or a mapping keyed by label:

```python
import pandas as pd

from datarecord import WorkingRecord

w = WorkingRecord(record)
w.set("p_nom", 120.0, entity=["wind1", "gas1"])
w.set("p_nom", [120.0, 60.0], entity=["wind1", "gas1"])
w.set("p_nom", {"wind1": 120.0, "gas1": 60.0})
print(w.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  120.0
1   gas1     p_nom         NaN   60.0
```

Each `set` replaces the rows it names, so the last one stands. Pass a series to set values along the coordinate its index is named after. Pass a property as a keyword to set values at the labels that match it when you call `set`:

```python
series = pd.Series([0.2, 0.6, 0.4], index=pd.Index([0, 1, 2], name="snapshot"))
w.set("p_max_pu", series, entity="wind1")
w.set("p_max_pu", 0.95, ctype="Generator", carrier="gas")
print(w.attributes["p_max_pu"].df())
```

```text
  entity  snapshot attribute  breakpoint  value
0  wind1         2  p_max_pu         NaN   0.40
1   gas1      <NA>  p_max_pu         NaN   0.95
2  wind1         1  p_max_pu         NaN   0.60
3  wind1         0  p_max_pu         NaN   0.20
```

`wind1` has its series, and `gas1`, the one gas generator, has one row with `snapshot` NULL. A coordinate you pass no keyword for is NULL, which means every label, those added later included. To set only the labels there are now, name them or their category. `set` also takes a long frame from pandas, polars, Arrow or DuckDB with a column per coordinate and a `value` column.

## Set a property

Name a property by its subject, `subject.name`:

```python
w.set("entity.carrier", "gas", entity="wind1")
print(w.properties["entity"].df())
```

```text
  entity      ctype carrier
0  wind1  Generator     gas
1   gas1  Generator     gas
```

`wind1` now has the carrier `gas`. A plain name is an attribute, so `w.set("carrier", ...)` raises a `KeyError` that suggests `entity.carrier`.

## Roll back the staged edits

`rollback` discards every staged edit:

```python
w.rollback()
print(w.properties["entity"].df())
```

```text
  entity      ctype carrier
0  wind1  Generator    wind
1   gas1  Generator     gas
```

The properties are as committed again.

## Add and remove labels

`add` takes a dim, its labels, and a keyword per property. A multi-dim takes tuples as mappings from coordinate to label. Add a label before you set a value at it or use it in a property or a tuple:

```python
w.add("carrier", ["solar"])
w.add("entity", ["pv1"], ctype="Generator", carrier="solar")
w.add("connection", [{"entity": "pv1", "bus": "south"}])
w.set("p_nom", 40.0, entity="pv1")
w.remove("bus", ["north"])
print(w.dims["connection"].df())
```

```text
                           connection
0  {'entity': 'gas1', 'bus': 'south'}
1   {'entity': 'pv1', 'bus': 'south'}
```

`remove` takes out every tuple with `north` in its `bus` field, with its values. It refuses a label that is still a property's value:

```python
w.remove("carrier", ["gas"])
```

```text
ValueError: `remove('carrier', ...)` is refused: `entity.carrier` names them at [('gas1', 'gas')]; `set` those to another label or None first
```

The message lists where `entity.carrier` still has the value `gas`. [Layers](../reference/layers.md) describes what else a removal removes.

## Check the staged edits

A read of the `WorkingRecord` returns its edits applied, and `edits` contains only what you staged:

```python
print(sorted(w.edits.axes()), sorted(w.edits.attributes()))
print(w.edits.attribute("p_nom").df())
```

```text
['bus', 'carrier', 'connection', 'entity'] ['p_nom']
  entity attribute  breakpoint  value
0    pv1     p_nom         NaN   40.0
```

`edits` has the four axes you changed, each restated whole, and the one `p_nom` row of `pv1`. A relation you took before an edit returns the edit too:

```python
rel = w.attributes["p_nom"]
w.set("p_nom", 45.0, entity="pv1")
print(rel.filter("entity = 'pv1'").df())
```

```text
  entity attribute  breakpoint  value
0    pv1     p_nom         NaN   45.0
```

`rel` returns the 45.0 that you staged after taking it.

## Declare a name

`declare` adds a dim, a property or an attribute to the schema, so that you can set it in the same revision. Name it, pass its spec, then add or set it as any other:

```python
from datarecord import AttributeSpec, Dimension, PropertySpec

w.declare("scenario", Dimension(dtype="str"))
w.declare("weight", AttributeSpec(dtype="float", dims={"scenario"}))
w.declare("entity.note", PropertySpec(dtype="str"))
w.add("scenario", ["low", "high"])
w.set("weight", {"low": 0.3, "high": 0.7})
w.set("entity.note", "new in this revision", entity="pv1")
print(w.attributes["weight"].df())
print(w.properties["entity"].df())
```

```text
  scenario attribute  breakpoint  value
0     high    weight         NaN    0.7
1      low    weight         NaN    0.3
  entity      ctype carrier                  note
0  wind1  Generator    wind                   NaN
1   gas1  Generator     gas                   NaN
2    pv1  Generator   solar  new in this revision
```

`weight` has a value per scenario, and `note` is NULL on the entities you did not set. A declaration stays until you commit or roll back. To change one, declare the name again with the change. `declare` refuses a change that `compatible_with` refuses:

```python
w.declare("weight", AttributeSpec(dtype="int", dims={"scenario"}))
```

```text
ValueError: `declare('weight', ...)` redeclares it incompatibly: attribute 'weight' dtype float -> int (https://energy-models.github.io/datarecord/reference/schema/#compatibility)
```

The message names the rule, and the schema is as it was. [Editing](../reference/editing.md#declare) lists what `declare` accepts and refuses.

## Commit the edits

`store.commit(w)` writes the edits as a child of the revision `w` was made over. Close the `WorkingRecord` once you no longer read it:

```python
child = store.commit(w)
w.close()
print(child.parent == rev.id)
print(child.record.dims["bus"].df())
```

```text
True
     bus
0  south
```

The child's `bus` axis has `south` alone.

To write a `WorkingRecord` whole, as a standalone directory with no parent, pass it to `ws.write`. A `with` block closes it on exit:

```python
import os

with WorkingRecord(child.record) as w:
    w.set("p_nom", 30.0, entity="pv1")
    flat = ws.write(w, os.path.join(tempfile.mkdtemp(), "flat"))
print(flat.attributes["p_nom"].df())
```

```text
  entity attribute  breakpoint  value
0  wind1     p_nom         NaN  100.0
1   gas1     p_nom         NaN   50.0
2    pv1     p_nom         NaN   30.0
```

The directory contains the values of every layer, with the edit applied.
