<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Export a model workflow's network

This guide shows how to export the network your model workflow hands to the solver, as a record the next step reads. The steps come from the export step of pypsa-eur, which runs them on its own networks. You need `datarecord[pypsa]` and a network your workflow has composed. [Use the PyPSA tool](use-the-tool.md) shows the export of a single network, and the [reference](reference.md) lists what the PyPSA tool reads and writes. The outputs on this page leave out PyPSA's own warnings.

## Export the network the solver receives

Run your workflow's own preparation on the network before `PyPSA.to_datarecord`, so that the record has what the solver would receive. Here `prepare` stands in for that step and adds load shedding at `south`:

```python
import tempfile

import pandas as pd
import pypsa

from datarecord import Workspace
from datarecord.tools.pypsa import PyPSA


def network(year: int, wind: list[float]) -> pypsa.Network:
    n = pypsa.Network(snapshots=pd.date_range(f"{year}-01-01", periods=3, freq="h"))
    n.add("Carrier", ["wind", "gas"])
    n.add("Bus", ["north", "south"])
    n.add("Generator", "wind1", bus="north", carrier="wind", p_nom=100, p_max_pu=wind)
    n.add("Generator", "gas1", bus="south", carrier="gas", p_nom=80, marginal_cost=50)
    n.add("Load", "load1", bus="south", p_set=60)
    n.add("Line", "line", bus0="north", bus1="south", x=0.1, s_nom=100)
    return n


def prepare(n: pypsa.Network) -> None:
    n.add("Carrier", "shedding")
    n.add(
        "Generator",
        "shed",
        bus="south",
        carrier="shedding",
        p_nom=1e3,
        marginal_cost=1e4,
    )


n = network(2012, [0.9, 0.4, 0.1])
prepare(n)
ws = Workspace()
record = PyPSA.to_datarecord(n, ws.con)
print(record.properties["entity"].df()[["entity", "ctype", "carrier"]])
```

```text
  entity      ctype   carrier
0  wind1  Generator      wind
1   gas1  Generator       gas
2   shed  Generator  shedding
3  load1       Load       NaN
4   line       Line       NaN
```

The record has the generator `shed`, which `prepare` added. A constraint that your workflow adds only when the solver runs, through PyPSA's `extra_functionality`, is not part of the network. So the record lacks it, and a solve of the record uses PyPSA's own constraints and the record's `GlobalConstraint` entities alone.

## Write custom columns of one dtype

A flag column read from a network file often has `True`, `False` or an empty value as objects. `to_datarecord` gives each [custom column](reference.md#custom-columns) one dtype from its values. A type whose column is all empty takes the dtype of the other types' columns, so a line with the flag and a link without it are written together:

```python
n.add("Link", "dc", bus0="north", bus1="south", p_nom=20)
n.c.Line.static["underground"] = pd.Series([True], index=n.lines.index, dtype=object)
n.c.Link.static["underground"] = None
record = PyPSA.to_datarecord(n, ws.con)
print(record.attributes["underground"].df()[["entity", "value"]])
```

```text
  entity  value
0     dc   <NA>
1   line   True
```

`underground` is an attribute over `entity` of dtype `bool`, with NULL for the link. Two types whose columns have values of two dtypes are refused:

```python
n.c.Link.static["underground"] = 1.0
try:
    PyPSA.to_datarecord(n, ws.con)
except ValueError as err:
    print(err)
```

```text
The custom column `underground` is an attribute over `entity` with one dtype, but its types carry it as {'Link': 'float', 'Line': 'bool'}: write values of one dtype in each type's column, or leave a column NULL
```

Cast the column to one dtype on each type that has it:

```python
n.c.Link.static["underground"] = n.c.Link.static["underground"].astype(bool)
record = PyPSA.to_datarecord(n, ws.con)
print(record.attributes["underground"].df()[["entity", "value"]])
```

```text
  entity  value
0     dc   True
1   line   True
```

## Add region and country shapes

Add each bus region as a `Shape` with `component="Bus"`, and each country as a `Shape` with `component="country"`. The country shapes make a `country` dim, and the text column `bus.country` then refers to it, as [geometry](reference.md#geometry) and [custom columns](reference.md#custom-columns) state. `to_datarecord` refuses a bus whose country has no shape:

```python
from shapely.geometry import box

n.c.Bus.static["country"] = ["DE", "NL"]
n.add(
    "Shape",
    ["Bus north onshore", "Bus south onshore"],
    geometry=[box(0, 1, 1, 2), box(0, 0, 1, 1)],
    component="Bus",
    idx=["north", "south"],
    type="onshore",
)
n.add(
    "Shape",
    "country DE onshore",
    geometry=box(0, 1, 2, 2),
    component="country",
    idx="DE",
    type="onshore",
)
try:
    PyPSA.to_datarecord(n, ws.con)
except ValueError as err:
    print(err)
```

```text
`bus.country` names ['NL'], no label of `country`: a custom text column named like a dim refers to it, so add a `country` shape for each, or set the value to NULL
```

If the country should stay on the bus, add a shape for it. Otherwise set the bus's country to NULL:

```python
named = n.shapes.query("component == 'country'").idx
n.c.Bus.static["country"] = n.buses.country.where(n.buses.country.isin(named))
record = PyPSA.to_datarecord(n, ws.con)
print(record.properties["bus"].df()[["bus", "country", "shape_onshore"]])
print(record.properties["country"].df())
```

```text
     bus country                        shape_onshore
0  north      DE  POLYGON ((1 1, 1 2, 0 2, 0 1, 1 1))
1  south     NaN  POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))
  country                        shape_onshore
0      DE  POLYGON ((2 1, 2 2, 0 2, 0 1, 2 1))
```

Each bus has its region in `bus.shape_onshore`, and `DE` is a label of `country` with its own `shape_onshore`. `south` has no country, so no row of `country` applies to it.

## Combine weather years into one record

Write one network per weather year as a `pypsa.NetworkCollection` whose index names a dim of its own, such as `weather_year`. [Over a dim of its own](reference.md#over-a-dim-of-its-own) lists what the members must share. Networks of two weather years have snapshots in two years, so `PyPSACollection.to_datarecord` refuses them:

```python
from datarecord.tools.pypsa import PyPSACollection

calm = network(2013, [0.3, 0.2, 0.1])
calm.add("Carrier", "solar")
calm.add("Generator", "solar1", bus="south", carrier="solar", p_nom=30)
members = [network(2012, [0.9, 0.4, 0.1]), calm]
for m in members:
    prepare(m)
index = pd.Index([2012, 2013], name="weather_year")
try:
    PyPSACollection.to_datarecord(pypsa.NetworkCollection(members, index=index), ws.con)
except ValueError as err:
    print(err)
```

```text
The network of weather_year 2013 holds other snapshots than that of weather_year 2012: it lacks 3 of them, first 2012-01-01 00:00:00 and holds 3 more, first 2013-01-01 00:00:00; give every member the same snapshots, such as by relabelling each to one reference year
```

Relabel each member's timesteps to the year of the first member. `set_snapshots` reindexes the series and the weightings, so keep them and put them back on the new labels:

```python
def relabel(n: pypsa.Network, year: int) -> None:
    new = n.snapshots.map(lambda t: t.replace(year=year))
    series = {(c.name, a): df for c in n.components for a, df in c.dynamic.items()}
    weightings = n.snapshot_weightings
    n.set_snapshots(new)
    n.snapshot_weightings = weightings.set_axis(new)
    for (c, a), df in series.items():
        n.c[c].dynamic[a] = df.set_axis(new)


relabel(calm, 2012)
try:
    PyPSACollection.to_datarecord(pypsa.NetworkCollection(members, index=index), ws.con)
except ValueError as err:
    print(err)
```

```text
The network of weather_year 2013 holds other Carrier than that of weather_year 2012: ['solar']
```

The snapshots now agree, and the members still differ in their components. A member's timesteps must all exist in the first member's year, so drop leap days before you relabel.

Add each component that a member lacks at zero capacity and not extendable, so that it cannot change a solve of that member. Then order every member's components as the first member orders them:

```python
def pad(members: list[pypsa.Network]) -> None:
    for c in members[0].components:
        rows = pd.concat([m.c[c.name].static for m in members])
        rows = rows[~rows.index.duplicated()]
        inputs = rows.columns.difference(c.attrs.index[c.attrs.status == "Output"])
        for m in members:
            missing = rows.index.difference(m.c[c.name].static.index)
            padded = rows.loc[missing, inputs]
            for nom in padded.columns.intersection(["p_nom", "s_nom", "e_nom"]):
                padded[[nom, f"{nom}_min", f"{nom}_max"]] = 0.0
                padded[f"{nom}_extendable"] = False
            m.add(c.name, missing, **padded)
            m.c[c.name].static = m.c[c.name].static.loc[rows.index]


pad(members)
collection = pypsa.NetworkCollection(members, index=index)
years = PyPSACollection.to_datarecord(collection, ws.con)
p_max_pu = years.attributes["p_max_pu"].df()
print(
    p_max_pu.pivot(index="snapshot", columns=["entity", "weather_year"], values="value")
)
```

```text
entity              wind1
weather_year         2012 2013
snapshot
2012-01-01 00:00:00   0.9  0.3
2012-01-01 01:00:00   0.4  0.2
2012-01-01 02:00:00   0.1  0.1
```

`wind1` keeps the series of each weather year, now at the timesteps of 2012. Without the last line of `pad`, `to_datarecord` refuses `calm`, because it orders `carrier` as `wind, gas, solar, shedding` and the first member as `wind, gas, shedding, solar`.

## Write the record to a directory

`ws.write(record, path)` writes the record as a [standalone directory](../reference/store.md#a-standalone-directory), which the next step of the workflow opens with `ws.at(path)`:

```python
import os

path = os.path.join(tempfile.mkdtemp(), "network")
written = ws.write(record, path)
print(sorted(os.listdir(path)), "country" in written.dims)
```

```text
['attributes', 'dims', 'manifest.json'] True
```

The directory has the record's files and its manifest, and the record read back has the `country` dim. A weather-year record such as `years` is written the same way. `ws.write` refuses a `path` that exists, even as an empty directory, so give each run of the step a new output directory. [Solve a record in a model workflow](solve-in-a-workflow.md) opens this directory as the root of a store.
