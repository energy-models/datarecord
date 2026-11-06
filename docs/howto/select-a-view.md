<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Select part of a record

This guide shows how to read a record at some of its labels with [`select`](../reference/selecting.md), and put squeezed labels back on rows you compute. The examples use `record` from [Your first record](../tutorials/first-record.md).

`select(record, **labels)` takes any [`RecordLike`](../reference/record.md) and returns a selection, which is a record too. `record.select(**labels)` is the same call. `select` writes nothing, and the selection's attributes stay plans on `record.con`.

## Keep some labels of a dim

Pass a list to keep those labels and the dim:

```python
from datarecord import select

view = select(record, bus=["north"])
print(view.dims["connection"].df())
print(view.dims["entity"].df())
```

```text
                            connection
0  {'entity': 'wind1', 'bus': 'north'}
  entity
0  wind1
1   gas1
```

`select` drops the connection at `south` with its bus. `gas1` stays, because `select` drops the tuples over a dropped label and keeps the other labels of those tuples. A `slice(start, stop)` on a number or datetime axis, or a partial date string such as `"2013-02"`, keeps the dim too.

## Squeeze a dim

Pass one label to squeeze its dim out of the selection's schema:

```python
view = select(record, carrier="wind")
print(view.schema.dims)
print(view.properties["entity"].df())
```

```text
('entity', 'ctype', 'bus', 'snapshot')
  entity      ctype
0  wind1  Generator
```

`carrier` and the property `entity.carrier` are gone, and so is `gas1`, whose carrier was dropped. On an attribute, `select` settles rows along the squeezed dim and drops its column:

```python
view = record.select(snapshot=1)
print(view.attributes["p_max_pu"].df())
```

```text
  entity attribute  breakpoint  value
0   gas1  p_max_pu         NaN    0.9
1  wind1  p_max_pu         NaN    0.8
```

`gas1`'s value, stored with `snapshot` NULL, applies at snapshot 1. To keep the dim at one label, pass a list of one:

```python
print(record.select(snapshot=[1]).attributes["p_max_pu"].df())
```

```text
  entity  snapshot attribute  breakpoint  value
0   gas1      <NA>  p_max_pu         NaN    0.9
1  wind1         1  p_max_pu         NaN    0.8
```

The dim stays, and `gas1`'s row keeps its NULL `snapshot`.

## Narrow a selection

Call `select` on a selection to add keywords. `labels` returns what you asked for, and `base` returns the record selected from:

```python
narrow = view.select(carrier=["gas"])
print(narrow.labels, narrow.base is record)
print(narrow.dims["entity"].df())
```

```text
{'snapshot': 1, 'carrier': ['gas']} True
  entity
0   gas1
```

The keywords merge, and `base` is the record you started from, never a selection. `narrow.base.select(**narrow.labels)` rebuilds the same selection, so pass `labels` on to rebuild it elsewhere.

## Lift rows back to the base

Rows computed over a selection lack the squeezed dims. `lift` adds each squeezed label back on the columns that the base declares the attribute over:

```python
doubled = view.attributes["p_max_pu"].project("entity, value * 2 AS value")
lifted = view.lift("p_max_pu", doubled)
print(lifted.df())
```

```text
  entity  snapshot  value
0  wind1         1    1.6
1   gas1         1    1.8
```

The lifted rows have snapshot 1 again, so `w.set("p_max_pu", lifted)` stages them on a `WorkingRecord` over the base. An attribute that the base declares over no squeezed dim comes back unchanged.

## Check what `select` refuses

`select` raises at the call, naming the keyword:

```python
record.select(snapshot=7)
```

```text
ValueError: `select(snapshot=7)` names a label the axis lacks (https://energy-models.github.io/datarecord/reference/selecting/)
```

The message contains the call and a link to the rule. [Selecting](../reference/selecting.md) lists every keyword form and refusal.
