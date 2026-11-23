<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Solve a record in a model workflow

This guide shows how to solve an exported record in a model workflow, and commit its results and a scenario patch as revisions. It also shows how to solve the investment periods of a record one at a time, or one period alone. The steps come from the solve step of pypsa-eur. You need `datarecord[pypsa]` and a record directory, which [Export a model workflow's network](export-a-workflow-network.md) writes. [Use the PyPSA tool](use-the-tool.md) shows a build and a solve outside a workflow. The outputs on this page leave out PyPSA's own warnings.

## Open the exported record as a store's root

The examples use a record with two investment periods, written as the export step would write it. `wind1` is extendable from 2030, and the load grows in 2040:

```python
import os
import tempfile

import pandas as pd
import pypsa

from datarecord import Workspace
from datarecord.tools.pypsa import PyPSA

n = pypsa.Network(snapshots=pd.date_range("2030-01-01", periods=2, freq="h"))
n.set_investment_periods([2030, 2040])
n.add("Carrier", ["wind", "gas"])
n.add("Bus", ["north", "south"])
n.add(
    "Generator",
    "wind1",
    bus="north",
    carrier="wind",
    p_nom_extendable=True,
    capital_cost=30,
    build_year=2030,
    p_max_pu=[0.9, 0.4, 0.9, 0.4],
)
n.add("Generator", "gas1", bus="south", carrier="gas", p_nom=80, marginal_cost=50)
n.add("Load", "load1", bus="south", p_set=[40, 40, 70, 70])
n.add("Line", "line", bus0="north", bus1="south", x=0.1, s_nom=100)
ws = Workspace()
path = os.path.join(tempfile.mkdtemp(), "network")
ws.write(PyPSA.to_datarecord(n, ws.con), path)
```

Read the directory with `ws.at(path)` and commit it as the root of the step's own store. Give each revision a `meta` entry, so that a later step can [find it](../howto/branch-and-materialise.md#find-a-revision):

```python
store = ws.store(tempfile.mkdtemp())
inputs = store.commit(ws.at(path), parent=None, meta={"step": "inputs"})
print(inputs.parent, inputs.meta)
```

```text
None {'step': 'inputs'}
```

`inputs` is a root, with no parent. The directory stays as the export step wrote it, and the store has its own copy of the record.

## Solve and commit the results

Check the record with `PyPSA.verify` before the build, and raise with its `describe()` where the report is truthy. The step then fails before PyPSA builds anything, with the inputs the record lacks, as [Check a record before a build](use-the-tool.md#check-a-record-before-a-build) shows. Then solve, and commit the results as a child of the revision the network was built from:

```python
from datarecord.store import Revision


def solve(revision: Revision, step: str) -> Revision:
    missing = PyPSA.verify(revision.record)
    if missing:
        raise RuntimeError(missing.describe())
    net = PyPSA.build(revision.record)
    net.optimize(
        solver_name="highs", multi_investment_periods=True, log_to_console=False
    )
    results = PyPSA.to_datarecord(net, ws.con, results_only=True, base=revision.record)
    return store.commit(results, parent=revision, meta={"step": step})


results = solve(inputs, "results")
print(results.record.attributes["p_nom_opt"].df()[["entity", "value"]])
```

```text
  entity      value
0  wind1  77.777778
1   gas1  80.000000
```

With perfect foresight over both periods, the solver builds 77.8 MW of `wind1`, which meets the 2040 load of 70 MW where its `p_max_pu` is 0.9. [Writing results](reference.md#writing-results) lists what the results layer has.

## Add a scenario patch as a sibling branch

A scenario patch is a [`WorkingRecord`](../reference/editing.md) over the inputs. To scale an attribute for the entities of one carrier, join its rows to `entity.carrier`. Then [`set`](../reference/editing.md#values) a relation of the new values, keyed by the attribute's own columns:

```python
from datarecord import RecordLike, WorkingRecord


def scale(
    record: RecordLike, attribute: str, carrier: str, factor: float
) -> WorkingRecord:
    entities = record.properties["entity"].filter(f"carrier = '{carrier}'")
    rows = record.attributes[attribute].join(entities.select("entity"), "entity")
    keys = [c for c in rows.columns if c not in ("attribute", "breakpoint", "value")]
    w = WorkingRecord(record)
    w.set(attribute, rows.project(", ".join(keys) + f", value * {factor} AS value"))
    return w


patch = store.commit(
    scale(inputs.record, "capital_cost", "wind", 2.0), meta={"step": "patch"}
)
patch_results = solve(patch, "patch results")
print(patch_results.record.attributes["p_nom_opt"].df()[["entity", "value"]])
```

```text
  entity      value
0  wind1  44.444444
1   gas1  80.000000
```

At twice the capital cost, the solver builds `wind1` for the 2030 load alone. `scale` changes only the rows the record has, so an entity at the attribute's declared default keeps it.

The store now has two branches over `inputs`:

```python
steps = {r.id: r.meta["step"] for r in store.revisions()}
for r in sorted(store.revisions(), key=lambda r: (len(r.ancestry()), r.meta["step"])):
    print(f"{r.meta['step']:<13} parent: {steps.get(r.parent)}")
```

```text
inputs        parent: None
patch         parent: inputs
results       parent: inputs
patch results parent: patch
```

`results` and `patch` are siblings, and `patch results` is a child of `patch`. A read of `patch results` returns the patched inputs and their results.

## Solve investment periods one at a time

For a myopic solve, build each period's network from a selection of that period as a list of one, which keeps the network multi-period. Before each period after the first, fix every extendable asset built earlier at the capacity the previous period's solve gave it. Write the solved networks as a `pypsa.NetworkCollection` over `period`:

```python
from datarecord.tools.pypsa import PyPSACollection


def period_network(period: int, previous: pypsa.Network | None) -> pypsa.Network:
    m = PyPSA.build(inputs.record.select(period=[period]))
    if previous is not None:
        g = m.c.Generator.static
        earlier = g.index[g.p_nom_extendable & (g.build_year < period)]
        g.loc[earlier, "p_nom"] = previous.generators.p_nom_opt[earlier]
        g.loc[earlier, "p_nom_extendable"] = False
    m.optimize(solver_name="highs", multi_investment_periods=True, log_to_console=False)
    return m


members: list[pypsa.Network] = []
for period in [2030, 2040]:
    members.append(period_network(period, members[-1] if members else None))
index = pd.Index([2030, 2040], name="period")
collection = pypsa.NetworkCollection(members, index=index)
layer = PyPSACollection.to_datarecord(
    collection, ws.con, results_only=True, base=inputs.record
)
myopic = store.commit(layer, parent=inputs, meta={"step": "myopic"})
print(myopic.record.attributes["p_nom_opt"].df()[["entity", "value"]])
```

```text
  entity      value
0  wind1  44.444444
1   gas1  80.000000
```

The 2030 solve sees only the 2030 load, so it builds 44.4 MW of `wind1`, and the 2040 solve keeps that capacity. The layer has the shape of the perfect-foresight `results`: one `p_nom_opt` per entity, and series over the snapshots of both periods. The example fixes generators only, so fix each other type with extendable assets the same way. [Over `period`](reference.md#over-period) lists what `to_datarecord` refuses in such a collection.

## Solve one investment period alone

For an overnight solve of one period, build that period's network as above, with no earlier capacities to fix. `PyPSA.to_datarecord` writes its results over the whole record, so no collection is needed:

```python
overnight = period_network(2040, None)
layer = PyPSA.to_datarecord(overnight, ws.con, results_only=True, base=inputs.record)
solved = store.commit(layer, parent=inputs, meta={"step": "overnight 2040"})
gen = solved.record.attributes["p"].filter("connection.entity = 'gas1'").df()
print(gen.assign(period=gen.snapshot.str["period"])[["period", "value"]])
```

```text
   period      value
0    2040  -0.000000
1    2040  38.888889
```

`gas1` has output at the snapshots of 2040 only. 2030 has no result, because `inputs` has none for the layer to leave in place.

## Solve each weather year

A record over a dim of its own, such as `weather_year`, is solved one member at a time with `PyPSACollection.build`, as [Solve a collection](use-the-tool.md#solve-a-collection) shows. Commit its results over the inputs as the `solve` function above does.
