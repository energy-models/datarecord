<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Use the PyPSA tool

This guide shows how to export a PyPSA network as a record, check and edit it, solve it, and commit the results. You need `datarecord[pypsa]`. The [tools](../reference/tools.md) page lists what the PyPSA tool reads from a record and writes back.

## Export a network

`PyPSA.to_datarecord(n, con)` presents a `pypsa.Network` as a record on the workspace's connection. Commit it as a root:

```python
import tempfile

import pandas as pd
import pypsa

from datarecord import WorkingRecord, Workspace
from datarecord.tools.pypsa import PyPSA

n = pypsa.Network(snapshots=pd.date_range("2030-01-01", periods=3, freq="h"))
n.add("Carrier", ["wind", "gas"])
n.add("Bus", ["north", "south"])
n.add(
    "Generator",
    "wind1",
    bus="north",
    carrier="wind",
    p_nom=100,
    p_max_pu=[0.9, 0.4, 0.1],
)
n.add("Generator", "gas1", bus="south", carrier="gas", p_nom=80, marginal_cost=50)
n.add("Load", "load1", bus="south", p_set=60)
n.add("Line", "line", bus0="north", bus1="south", x=0.1, s_nom=100)

ws = Workspace()
store = ws.store(tempfile.mkdtemp())
root = store.commit(PyPSA.to_datarecord(n, ws.con), parent=None)
print(root.record.properties["connection"].df())
```

```text
                            connection      role
0  {'entity': 'wind1', 'bus': 'north'}  attached
1   {'entity': 'gas1', 'bus': 'south'}  attached
2  {'entity': 'load1', 'bus': 'south'}  attached
3   {'entity': 'line', 'bus': 'north'}     input
4   {'entity': 'line', 'bus': 'south'}    output
```

Each component is a label of `entity` with its type in `entity.ctype`, and each of its ports is a `connection` tuple with a `role`. Buses and carriers are labels of the dims `bus` and `carrier`. `to_datarecord` raises `UnsupportedRecordError` for a network in which two components of different types share a name.

## Check a record before a build

`PyPSA.verify(record)` returns a `Requirements` that is falsy when PyPSA can build the record. A value that a component type does not take is left out of the build and listed in `ignored`:

```python
w = WorkingRecord(root.record)
w.set("p_max_pu", 0.95)
report = PyPSA.verify(w)
print(bool(report), sorted(report.ignored))
```

```text
False [('Line', 'p_max_pu'), ('Load', 'p_max_pu')]
```

The value with no `entity` applies to every entity, and `PyPSA.build` passes it only to the types that take it, here the generators. A missing input makes the report truthy, and `report.describe()` lists it:

```python
w.rollback()
w.remove("bus", ["north"])
print(PyPSA.verify(w).describe())
```

```text
attributes [('Generator', 'bus'), ('Line', 'bus1')]
```

`wind1` and `line` lost their connections at `north`, so each lacks a port's bus. `PyPSA.build` raises `UnsupportedRecordError` with the same list for such a record.

## Edit the inputs

Stage edits with PyPSA's names. A property is named by its subject, so `bus.x` is a bus's x position and `x` is a line's reactance:

```python
w.rollback()
w.set("p_max_pu", 0.95, ctype="Generator", carrier="gas")
w.set("bus.x", 10.2, bus="north")
inputs = store.commit(w)
```

Commit edited inputs before a solve, so that the results layer has a parent.

## Build and solve the network

`PyPSA.build(record)` returns a `pypsa.Network`:

```python
net = PyPSA.build(inputs.record)
print(net.generators.p_max_pu)
print(net.optimize(solver_name="highs", log_to_console=False))
```

```text
name
wind1    1.00
gas1     0.95
Name: p_max_pu, dtype: float64
('ok', 'optimal')
```

`wind1`'s series is in `net.generators_t.p_max_pu`, and its static value is PyPSA's default.

## Write the results back

With `results_only=True` and the record the network was built from as `base`, `to_datarecord` presents only what the solve produced. Commit it as a child of `inputs`:

```python
results = PyPSA.to_datarecord(net, ws.con, results_only=True, base=inputs.record)
solved = store.commit(results, parent=inputs)
price = solved.record.attributes["marginal_price"].df()
print(price.pivot(index="snapshot", columns="bus", values="value"))
```

```text
bus                  north  south
snapshot
2030-01-01 00:00:00   -0.0   -0.0
2030-01-01 01:00:00   50.0   50.0
2030-01-01 02:00:00   50.0   50.0
```

A result is an attribute like any input, and a read of `solved` returns both. `PyPSA.build(solved.record)` puts the results back into `net.buses_t.marginal_price`. A later solve over `solved` writes another child, and where both set the same row, a read returns the later solve's value.
