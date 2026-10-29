<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tools

A tool is one modelling framework's view of a record.
Tools live under `datarecord/tools/`, and a caller reaches one by importing it.
Nothing in the core imports a tool, so importing the record layer pulls in no framework ([design](../design/tools/index.md), [the package](../design/package.md)).

```python
from datarecord.tools.pypsa import PyPSA

missing = PyPSA.verify(record)
if missing:
    raise RuntimeError(missing.describe())

n = PyPSA.build(record)  # a pypsa.Network
n.optimize()

record_back = PyPSA.to_datarecord(n, ws.con)  # the inverse, results included
```

`to_datarecord` plans its relations on the connection it receives, the one of the workspace that writes them, so they reach the write path with no copy.
`build` reads `record.con` itself.

`verify` catches what the record layer cannot: an entity type the framework has no registry entry for, a connection `role` it cannot place, and a `partial` set that breaks the framework's split between constant and varying data.
It returns a `Requirements` that is falsy when the record is usable.
A stored attribute that a type does not take, such as `p_max_pu` on a load, is left out of the build and listed in `ignored`, and the result stays falsy.
`build` raises `UnsupportedRecordError` for a record it cannot build in full.

`build` takes `select`, the labels to build from by dim, as xarray's `sel` takes them ([design](../design/tools/pypsa.md#selecting-before-a-build)):

```python
PyPSA.build(record, select={"period": 2030})  # single-period, its active entities
PyPSA.build(record, select={"period": [2030, 2040]})  # multi-period, those two
PyPSA.build(record, select={"period": slice(2030, None)})  # 2030 onwards, multi-period
PyPSA.build(record, select={"snapshot": slice("2013-02", "2013-03")})  # Feb and March
PyPSA.build(record, select={"snapshot": "2013-02-01 06:00"})  # in every period
PyPSA.build(record, select={"scenario": ["low", "high"]})  # weights renormalised
PyPSA.build(record, select={"weather_year": 2012})  # a Network, not a collection
PyPSA.build(record, select={"bus": ["north", "south"]})  # and what attaches to them
PyPSA.build(record, select={"carrier": ["AC", "wind"]})  # AC buses, wind entities
```

One label of `period`, `scenario` or the record's dim of its own squeezes the dim, and a list keeps it.
`snapshot`, `bus`, `entity` and `carrier` take a label or a list and are never squeezed.
A snapshot is named by its timestamp, and a partial date string such as `"2013-02"` names every snapshot in its span.
A slice keeps an ordered dim, `snapshot`, `period` or a dim of its own of numbers or datetimes, at the labels it covers, both ends included and a partial date string covering its span, as pandas' `.loc` slices.
A label referring to one the selection drops goes too, such as an entity attached to a dropped bus, and any other dim, an unknown label or an empty list is refused.

## Buses and carriers

PyPSA's buses and carriers are dims.
A bus is a label of `bus`, and a carrier is a label of `carrier`.
What PyPSA stores on either is a property of the label or an attribute over the dim, and every name stays PyPSA's ([design](../design/tools/pypsa.md#buses-and-carriers-are-dims)).

```python
record.dims["carrier"]  # carrier
record.properties["bus"]  # bus | carrier | type | x | y | unit | location
record.properties["entity"]  # entity | ctype | carrier | sign | type
record.attributes["v_nom"]  # bus | attribute | breakpoint | value
record.attributes["marginal_price"]  # bus | snapshot | scenario | ..., a result
```

A property is named by its subject, so `x` is a line's reactance and `bus.x` is a bus's coordinate.
A column PyPSA does not know is a property where it holds text, such as pypsa-eur's `Bus.country` as `bus.country`, and an attribute where it holds numbers, such as `Bus.nom_max_onwind_2050` ([design](../design/tools/pypsa.md#custom-columns)).
`entity.carrier` and `bus.carrier` are `{"one_of": "carrier"}`, so they refer to `carrier`:

```python
w.add("carrier", ["H2"], color="#f0f")
w.add("bus", ["north H2"], carrier="H2", x=10.1, y=52.0)  # bare: the dim's own
w.set("v_nom", 380.0, bus="north")
w.set("bus.x", 10.2, bus="north")  # `x` alone is a Line's reactance
w.set("entity.ctype", "Load", entity="g1")
w.set("entity.carrier", "coal", entity="g1")  # refused: no label `coal`
w.set("carrier", "AC")  # refused, naming `bus.carrier` and `entity.carrier`
w.set("p_max_pu", 0.8, ctype="Generator")  # selector: `entity.ctype`
w.remove("bus", ["south"])  # and every connection at it
w.remove("carrier", ["gas"])  # refused while a carrier names it
```

A bus and a load may share a name, since they are labels of two dims.
`to_datarecord` adds every carrier that a component or bus names and `n.carriers` lacks, and it stores PyPSA's `""` carrier as NULL.
`verify` reports an entity attached to no bus.

## Writing an edited network over its parent

A network edited in PyPSA is written back whole as a child of the record it was built from, with that record as `base`:

```python
n = PyPSA.build(rev.record)
n.c.Generator.static.loc["wind1", "p_nom_extendable"] = False  # PyPSA's default
edited = rev.child(PyPSA.to_datarecord(n, ws.con, base=rev.record))
```

The layer holds what differs from `base`, a value back at its default included, so `base`'s value does not show through ([design](../design/tools/pypsa.md#writing-results)).

## Solving and writing results back

A result is an attribute.
With `results_only=True`, `to_datarecord` presents only what the solve produced, as a thin layer over `base`, the record the network was built from:

```python
w = WorkingRecord(rev.record)
w.set("p_max_pu", 0.8, entity=["wind1"])
inputs = rev.child(w.edits)  # edited inputs are committed first
n = PyPSA.build(inputs.record)
n.optimize()
solved = inputs.child(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
)
solved.record.attributes["marginal_price"]  # a child of solved reads it too
PyPSA.build(solved.record)  # marginal_price is in the network
```

`base` is required with `results_only`.
A SubNetwork is a label of the dim `sub_network`, `0`, `1`, … as PyPSA names it.
The results layer restates that axis with this solve's SubNetworks and leaves the entity axis to `base`, so a re-solve leaves none stale ([design](../design/tools/pypsa.md#writing-results)).

Several networks solved apart are written as one `pypsa.NetworkCollection`, its index labelling `collection_dim`, as a full export or as results over `base`.
A myopic solve, one network per investment period, is a collection over `period`, and its layer is the one a perfect-foresight solve of `base` would write:

```python
members = pypsa.NetworkCollection({2030: n_2030, 2040: n_2040})
solved = inputs.child(
    PyPSA.to_datarecord(
        members, ws.con, results_only=True, base=inputs.record, collection_dim="period"
    )
)
```

Two members that hold different static results for one entity, such as `p_nom_opt`, are refused over `period`.
Over `scenario`, deterministic members are written as one stochastic record whose scenarios weigh equally, and they may differ only where PyPSA varies a value by scenario, such as `marginal_cost` or a series; members solved apart, whose `p_nom_opt` differs, are refused.
Name a dim of your own for them instead: an attribute whose values differ across the members varies over it, one whose values agree is written once, as declared, and `build` returns a `NetworkCollection` indexed by it:

```python
sweep = pypsa.NetworkCollection({"low": n_low, "high": n_high})
record = ws.write(
    PyPSA.to_datarecord(sweep, ws.con, collection_dim="weather_year"), "out/"
)
record.attributes["p_nom_opt"]  # a row per entity and weather_year, where they differ
PyPSA.build(record)  # a NetworkCollection over weather_year: n_low and n_high
```

A record has one dim of its own, since collections do not nest: a collection over another is refused over a `base` that has one.
A member holds the components the first one does, its axes join the others' in one order, and a label's properties agree across the members holding it.
Every check runs when `to_datarecord` is called ([design](../design/tools/pypsa.md#a-collection)).
