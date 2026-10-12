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

record_back = PyPSA.to_datarecord(n)  # the inverse; a `Record`
results = PyPSA.results(n)  # Frames, in the long schema
```

`verify` catches what the record layer cannot — a component type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks its constant-versus-varying split — and returns a falsy `Requirements` when the record is usable. `build` raises `UnsupportedRecordError` rather than producing a partial model.

## Solving and writing results back

`results` returns the same `Frames` type `Record.outputs` presents, so a solve's output goes straight back:

```python
w = WorkingRecord(record, con)
w.set("p_max_pu", 0.8, entity=["wind1"])
n = PyPSA.build(w)  # a WorkingRecord is a Record
n.optimize()
for attr, frame in PyPSA.results(n).items():
    w.set(attr, frame, kind="outputs")
w.commit(NewChild())  # one layer, inputs and results together
```

Two checks are skipped for `kind="outputs"`: the attribute need not be schema-declared, and a result's `entity` need not resolve to a declared member — a solve may produce rows for a component type it derived rather than read ([design](../design/working-record.md#results-through-kindoutputs)).

## The component type of a PyPSA record

A record from `PyPSA.to_datarecord` has no entity-type axis. The PyPSA type of each component (`Generator`, `Link`, ...) is `entity_group`, a column of `dims/entity.parquet`, and `record.entity_types` is empty ([design](../design/tools.md#the-pypsa-tool)).
To add a component to such a record, put its type in the frame. The label that `add` takes does not go into the record:

```python
w.add(
    "Generator",
    pd.DataFrame({"entity": ["wind3"], "entity_group": ["Generator"], "p_nom": [50.0]}),
)
```

`build` reads the type from `entity_group`, and a mathspec spec compares it with a value: `entity_group == 'Generator'`.
