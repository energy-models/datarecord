<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Solve a record with PyPSA

In this tutorial you build a record of a bus, two generators and a load, solve it with PyPSA and commit the results. It takes about 20 minutes, and it follows [Your first record](first-record.md).

You need the PyPSA tool, which comes with PyPSA and the HiGHS solver:

```bash
pip install datarecord[pypsa]
```

PyPSA prints its own warnings and solver log as it runs, which the outputs below leave out.

## The tool's schema

The PyPSA [tool](../reference/tools.md) is the module that translates between a record and PyPSA. It declares its schema from PyPSA's own registry. Export a network that has only its snapshots, and commit it as the root:

```python
import tempfile

import pandas as pd
import pypsa

from datarecord import Workspace
from datarecord.tools.pypsa import PyPSA

ws = Workspace()
store = ws.store(tempfile.mkdtemp())

n = pypsa.Network(snapshots=pd.date_range("2030-01-01", periods=3, freq="h"))
root = store.commit(PyPSA.to_datarecord(n, ws.con), parent=None)
print(root.record.dims["snapshot"].df())
print(list(root.record.schema.properties["entity"]))
```

Check it:

```text
             snapshot
0 2030-01-01 00:00:00
1 2030-01-01 01:00:00
2 2030-01-01 02:00:00
['ctype', 'carrier', 'sign', 'type']
```

The root has the three snapshots, and every entity takes the four properties that PyPSA keeps on a component.

## The components

A component is a label of `entity`, its type is its `ctype`, and each of its ports is a `connection` tuple with a `role`. Stage them over the root, with their values:

```python
from datarecord import WorkingRecord

w = WorkingRecord(root.record)
w.add("carrier", ["AC", "wind", "gas"])
w.add("bus", ["north"])
w.add("entity", ["wind1", "gas1"], ctype="Generator", carrier=["wind", "gas"])
w.add("entity", ["load1"], ctype="Load")
w.add(
    "connection",
    [{"entity": e, "bus": "north"} for e in ["wind1", "gas1", "load1"]],
    role="attached",
)
w.set("p_nom", [100.0, 80.0], entity=["wind1", "gas1"])
w.set("marginal_cost", 50.0, entity="gas1")
w.set("p_max_pu", [0.9, 0.4, 0.1], entity="wind1", snapshot=list(n.snapshots))
w.set("p_set", 60.0, entity="load1")
print(PyPSA.verify(w).describe())
```

Check it:

```text
nothing
```

`verify` reports what a record lacks for a PyPSA build, and here it lacks nothing. `AC` is there because a bus's `carrier` defaults to it.

## The network

Commit the inputs, then build a `pypsa.Network` from the revision:

```python
inputs = store.commit(w)
net = PyPSA.build(inputs.record)
print(net.generators[["bus", "carrier", "p_nom", "marginal_cost"]])
print(net.generators_t.p_max_pu)
```

```text
         bus carrier  p_nom  marginal_cost
name
wind1  north    wind  100.0            0.0
gas1   north     gas   80.0           50.0
name                 wind1
snapshot
2030-01-01 00:00:00    0.9
2030-01-01 01:00:00    0.4
2030-01-01 02:00:00    0.1
```

`build` turned each connection into its generator's `bus`, and `wind1` takes the default `marginal_cost`.

## A solve

Solve the network with HiGHS:

```python
print(net.optimize(solver_name="highs", log_to_console=False))
print(net.generators_t.p)
```

```text
('ok', 'optimal')
name                 wind1  gas1
snapshot
2030-01-01 00:00:00   60.0  -0.0
2030-01-01 01:00:00   40.0  20.0
2030-01-01 02:00:00   10.0  50.0
```

Wind covers the load where it can, and gas covers the rest.

## A results revision

With `results_only=True`, `to_datarecord` presents only what the solve produced, as a layer over the record the network was built from. Commit it as a child of `inputs`:

```python
results = PyPSA.to_datarecord(net, ws.con, results_only=True, base=inputs.record)
solved = store.commit(results, parent=inputs)
print(solved.parent == inputs.id)
```

```text
True
```

`solved` is a child of `inputs`, and each result is an attribute like any other. Read the bus's price:

```python
price = solved.record.attributes["marginal_price"].df()
print(price[["bus", "snapshot", "value"]])
```

```text
     bus            snapshot  value
0  north 2030-01-01 00:00:00   -0.0
1  north 2030-01-01 01:00:00   50.0
2  north 2030-01-01 02:00:00   50.0
```

The price is gas's marginal cost wherever gas runs. A read of `solved` returns its parent's inputs too:

```python
print(solved.record.attributes["p_nom"].df())
```

```text
  scenario entity attribute  breakpoint  value
0     None  wind1     p_nom         NaN  100.0
1     None   gas1     p_nom         NaN   80.0
```

`p_nom` comes from `inputs`, with the `scenario` dim that the tool's schema declares left NULL.

## What you have built

You have built a record that the PyPSA tool accepts and solved it. Its results are a child revision, and a read of it returns both the results and the inputs.

## Where to next

- [Use the PyPSA tool](../howto/use-the-pypsa-tool.md) exports an existing network and checks it with `verify`.
- [Tools](../reference/tools.md) lists what the PyPSA tool reads from a record and writes back.
- [Present a framework object](../howto/present-a-framework-object.md) is for writing a tool of your own.
