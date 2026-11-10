<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Use the PyPSA tool

This guide shows how to export a PyPSA network as a record, check and edit it, solve it, and commit the results. It also shows how to build from a selection, write an edited network over its parent and solve a collection. You need `datarecord[pypsa]`. The [tools](../reference/tools.md) page lists what the PyPSA tool reads from a record and writes back.

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

## Build from a selection

`PyPSA.build` takes a [selection](../reference/selecting.md) of a record and builds the network of its labels. Along a dim the network keeps, such as `snapshot` or `bus`, pass a list, because one label of it is refused:

```python
try:
    PyPSA.build(inputs.record.select(bus="south"))
except ValueError as err:
    print(err)
south = PyPSA.build(inputs.record.select(bus=["south"]))
print(list(south.generators.index), list(south.lines.index))
```

```text
A network is built from a selection squeezing `period`, `scenario` or a dim of the record's own, not ['bus']: select one label as a list of one to keep the dim
['gas1'] []
```

`wind1` and `line` attach to the dropped bus `north`, so `build` leaves them out of the network.

One label of `period` or `scenario` squeezes the dim, and a list of one keeps it. A network with two investment periods shows the difference:

```python
mp = pypsa.Network(snapshots=pd.date_range("2030-01-01", periods=2, freq="h"))
mp.set_investment_periods([2030, 2040])
mp.add("Bus", "hub")
mp.add("Generator", "gas", bus="hub", p_nom=100, marginal_cost=50)
mp.add("Load", "demand", bus="hub", p_set=40)
periods = store.commit(PyPSA.to_datarecord(mp, ws.con), parent=None)
view = periods.record.select(period=2030)
one = PyPSA.build(view)
kept = PyPSA.build(periods.record.select(period=[2030]))
print(list(one.investment_periods), one.snapshots.nlevels)
print(list(kept.investment_periods), kept.snapshots.nlevels)
```

```text
[] 1
[2030] 2
```

`one` is a single-period network over the timesteps of 2030, and `kept` is a multi-period network with 2030 as its only period.

Write the results of a network built from a selection with `base=view`. `to_datarecord` puts each row back at the labels the selection squeezed:

```python
one.optimize(solver_name="highs", log_to_console=False)
results = PyPSA.to_datarecord(one, ws.con, results_only=True, base=view)
solved_2030 = store.commit(results, parent=periods)
print(solved_2030.record.attributes["marginal_price"].df()[["snapshot", "value"]])
```

```text
                                            snapshot  value
0  {'period': 2030, 'timestep': 2030-01-01 00:00:00}   50.0
1  {'period': 2030, 'timestep': 2030-01-01 01:00:00}   50.0
```

The prices are at the snapshots of period 2030, and 2040 has none. [Selecting before a build](../reference/tools.md#selecting-before-a-build) lists what `build` drops on top of the selection.

## Write an edited network over its parent

To write back a network you edited in PyPSA, pass the record it was built from as `base`. `to_datarecord` then writes only the values that changed:

```python
edit = PyPSA.build(inputs.record)
edit.generators.loc["gas1", "p_max_pu"] = 1.0
edit.generators.loc["gas1", "marginal_cost"] = 60.0
layer = PyPSA.to_datarecord(edit, ws.con, base=inputs.record)
print({name: len(rows) for name, rows in layer.attributes.items() if len(rows)})
edited = store.commit(layer, parent=inputs)
print(edited.record.attributes["p_max_pu"].filter("entity = 'gas1'").df())
```

```text
{'p_max_pu': 1, 'marginal_cost': 1}
  snapshot scenario entity attribute  breakpoint  value
0      NaT     None   gas1  p_max_pu         NaN    1.0
```

The layer has one row for each edit. `gas1`'s `p_max_pu` is back at PyPSA's default of `1.0`, and the layer writes it, so the `0.95` of `inputs` no longer shows through.

## Solve a collection

A set of networks solved apart, such as one per weather year, is a `pypsa.NetworkCollection` whose index names a dim of its own. `PyPSACollection.to_datarecord` writes it as one record:

```python
from datarecord.tools.base import UnsupportedRecordError
from datarecord.tools.pypsa import PyPSACollection

calm = PyPSA.build(inputs.record)
calm.generators_t.p_max_pu["wind1"] = [0.3, 0.2, 0.1]
members = pd.Series({2012: PyPSA.build(inputs.record), 2013: calm})
collection = pypsa.NetworkCollection(members.rename_axis("weather_year"))
years = store.commit(PyPSACollection.to_datarecord(collection, ws.con), parent=None)
attributes = years.record.schema.attributes
print(sorted(attributes["p_max_pu"].dims), sorted(attributes["p_nom"].dims))
```

```text
['entity', 'scenario', 'snapshot', 'weather_year'] ['entity', 'scenario']
```

`p_max_pu` differs between the members, so it varies over `weather_year`. `p_nom` is the same in both and keeps the dims PyPSA declares.

`PyPSA.build` refuses a record with a dim of its own. Build it with `PyPSACollection.build`, solve each member, and write the results over the record:

```python
try:
    PyPSA.build(years.record)
except UnsupportedRecordError as err:
    print(err)
built = PyPSACollection.build(years.record)
for member in built.networks:
    member.optimize(solver_name="highs", log_to_console=False)
results = PyPSACollection.to_datarecord(
    built, ws.con, results_only=True, base=years.record
)
solved_years = store.commit(results, parent=years)
price = solved_years.record.attributes["marginal_price"].df()
south_price = price[price["bus"] == "south"]
print(south_price.pivot(index="snapshot", columns="weather_year", values="value"))
```

```text
record cannot build a pypsa model; missing: unsupported dims ['weather_year']
weather_year         2012  2013
snapshot
2030-01-01 00:00:00  -0.0  50.0
2030-01-01 01:00:00  50.0  50.0
2030-01-01 02:00:00  50.0  50.0
```

In the calm year 2013, gas runs at every snapshot, so the price at `south` is gas's marginal cost of 50 throughout. A collection over `period` is a myopic solve, one network per investment period, and [a collection](../reference/tools.md#a-collection) lists the rules of both kinds.
