<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tools

A **tool** is one modelling framework's view of a record. Tools live under `datarecord/tools/`, are reached by importing them (no registry), and nothing in the core imports one — so importing the record layer pulls in no framework ([design](../design/tools.md), [module layout](../design/module-layout.md)).

```python
from datarecord.tools.pypsa import PyPSA

missing = PyPSA.verify(record)
if missing:
    raise RuntimeError(missing.describe())

n = PyPSA.build(record)  # a pypsa.Network
n.optimize()

record_back = PyPSA.to_datarecord(n, ws.con)  # the inverse; a `Record`
results = PyPSA.results(n, ws.con)  # Frames, in the long schema
```

`to_datarecord` and `results` plan their frames on the connection they are handed — the workspace's that writes them, or the record's they are `set` on — so they reach the write path without a copy; `build` reads `record.con` itself.

`verify` catches what the record layer cannot — an entity type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks its constant-versus-varying split — and returns a falsy `Requirements` when the record is usable. `build` raises `UnsupportedRecordError` rather than producing a partial model.

## Buses and carriers

PyPSA's buses and carriers are dims, not entity types: a bus is a label of `bus`, a carrier a label of `carrier`, and what PyPSA stores on either is a property of the label or an attribute over the dim, every name PyPSA's ([design](../design/tools.md)).

```python
record.dims["carrier"]  # carrier
record.properties["bus"]  # bus | carrier | type | x | y | unit | location
record.properties["entity"]  # entity | component | carrier | sign | type
record.attributes["v_nom"]  # bus | attribute | breakpoint | value
record.outputs["marginal_price"]  # bus | snapshot | scenario | ...
```

A property is named by its subject, so `x` is a line's reactance and `bus.x` a bus's coordinate. `entity.carrier` and `bus.carrier` are `into` `carrier`, naming its labels:

```python
w.add("carrier", ["H2"], color="#f0f")
w.add("bus", ["north H2"], carrier="H2", x=10.1, y=52.0)  # bare: the dim's own
w.set("v_nom", 380.0, bus="north")
w.set("bus.x", 10.2, bus="north")  # `x` alone is a Line's reactance
w.set("entity.component", "Load", entity="g1")
w.set("entity.carrier", "coal", entity="g1")  # refused: no label `coal`
w.set("carrier", "AC")  # refused, naming `bus.carrier` and `entity.carrier`
w.set("p_max_pu", 0.8, component="Generator")  # selector: `entity.component`
w.remove("bus", ["south"])  # and every connection at it
w.remove("carrier", ["gas"])  # refused while a carrier names it
```

A bus and a load may share a name, being labels of two dims. `to_datarecord` adds every carrier a component or bus names but `n.carriers` does not list, and stores PyPSA's `""` carrier as NULL. `verify` reports an entity attached to no bus.

## Solving and writing results back

`results` returns the same `Frames` type `Record.outputs` presents, so a solve's output goes straight back:

```python
w = WorkingRecord(rev.record)
w.set("p_max_pu", 0.8, entity=["wind1"])
n = PyPSA.build(w)  # a WorkingRecord is a Record
n.optimize()
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame, kind="outputs")
rev.child(w.edits)  # one layer, inputs and results together
```

A result is checked against the schema's `results`, its name and its dims, but its labels need not resolve — a solve may produce rows for labels it derived rather than read ([design](../design/working-record.md#results-through-kindoutputs)).
