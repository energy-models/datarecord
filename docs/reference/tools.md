<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tools

This page describes the `Tool` protocol, then the rules of the PyPSA tool in the order the tool reads a record and then writes one.
A tool is a module under `datarecord.tools` that translates between a [record](record.md) and one modelling framework.

## The `Tool` protocol

```python
class Tool(Protocol):
    name: str
    schema: Schema

    def requires(self, record: RecordLike) -> Requirements: ...
    def verify(self, record: RecordLike) -> Requirements: ...
    def build(self, record: RecordLike) -> Any: ...
    def to_datarecord(
        self,
        model: Any,
        con: DuckDBPyConnection,
        *,
        results_only: bool = False,
        base: RecordLike | None = None,
    ) -> RecordLike: ...
```

| Member          | Does                                                                                    |
| --------------- | --------------------------------------------------------------------------------------- |
| `name`          | is the framework's name, which error messages quote                                     |
| `schema`        | is the map from the tool's attribute names to the record's ([below](#the-tools-schema)) |
| `requires`      | returns what the tool needs from this record: dims, categories and attributes           |
| `verify`        | returns what the record fails to supply, falsy when the tool can build it               |
| `build`         | returns the framework's own object, built from any `RecordLike`                         |
| `to_datarecord` | presents a framework object as a `RecordLike` whose relations are planned on `con`      |

Each tool's module defines the tool as a module-level object, such as `PyPSA`, and nothing outside `datarecord.tools` imports a tool.
`build` reads `record.con` for its queries, so it builds from a standalone directory as it does from a revision.
`build` raises `UnsupportedRecordError` for a record that `verify` finds unusable.

`to_datarecord` plans its relations on the connection it receives, which is `ws.con` of the [workspace](store.md) that writes them.
Its result goes straight to `store.commit` or `ws.write`.
With `results_only=True` it presents only what a solve produced, as a layer to commit over `base`, the record the model was built from.
It raises `ValueError` where `base` comes without `results_only`, or `results_only` without `base`.

### `Requirements`

`Requirements` is what `requires` and `verify` return, with one set per kind of fault.

| Field                | Contains                                                                                  |
| -------------------- | ----------------------------------------------------------------------------------------- |
| `dims`               | dims the tool needs and the schema does not declare                                       |
| `categories`         | categories the framework has no counterpart for                                           |
| `attributes`         | `(category, attribute)` pairs with no value, named in the record's words                  |
| `unsupported_keys`   | `(key, dim)` pairs of a `partial` the framework cannot take                               |
| `unsupported_values` | `(category, attribute)` pairs stored as a curve where the framework takes one number      |
| `names`              | labels that two of the framework's categories share                                       |
| `columns`            | `(category, column)` pairs of a framework column whose name the schema declares otherwise |
| `ignored`            | `(category, attribute)` pairs that the record stores and the category does not take       |

A `Requirements` is truthy when any field but `ignored` has an entry.
`describe()` returns a one-line summary, which `UnsupportedRecordError` quotes.

### The tool's schema

A tool's `Schema` lists, per category, the tool attributes that are renamed or computed, each an `Attr` that lists its source attributes.
An attribute with no entry keeps its name, so an empty `Schema` maps every name to itself.
`Attr.resolve` [settles](record.md) each source attribute before it renames or computes, so a computed attribute combines one row per point of each source.

!!! info "See also"

    [Present a framework object as a record](../howto/present-a-framework-object.md) shows a tool author's `to_datarecord`.

## The PyPSA tool

```python
from datarecord.tools.pypsa import PyPSA
```

The PyPSA tool needs the optional extra `datarecord[pypsa]`.
Its `schema` is empty, because the record's names are PyPSA's.

### The schema

`to_datarecord` writes every network under one schema, declared from PyPSA's component registry:

| Dim           | Is                                                            |
| ------------- | ------------------------------------------------------------- |
| `entity`      | a component of any type but `Bus`, `Carrier` and `SubNetwork` |
| `ctype`       | a dim with `one_of`: the component types                      |
| `bus`         | a bus                                                         |
| `carrier`     | a carrier                                                     |
| `sub_network` | a SubNetwork, labelled as PyPSA numbers them                  |
| `connection`  | a multi-dim over `(entity, bus)`: one port of a component     |
| `role`        | a dim with `one_of`: `input`, `output` and `attached`         |
| `snapshot`    | a snapshot                                                    |
| `timestep`    | a timestep of a multi-period network                          |
| `period`      | an investment period, labelled by its year                    |
| `scenario`    | a scenario of a stochastic network                            |

`partial` is `{"entity", "bus", "scenario", "connection"}`.
The properties of `entity` are `ctype`, `carrier`, `sign` and `type`, and `role` is a property of `connection`.
Every other input is an attribute, `p_nom` among them.

Every component attribute is declared over `scenario`, except those PyPSA keeps the same in every scenario, such as `p_nom_extendable`, `build_year`, `lifetime` and the `*_nom_opt` results.

### `verify`

`verify` reports, in a `Requirements`:

- each of `snapshot`, `period` and `scenario` that the schema does not declare, in `dims`.
- `snapshot` in `partial`, in `unsupported_keys`, because PyPSA takes a component's series from one place.
- each `ctype` that PyPSA's registry lacks, in `categories`.
- each required attribute of a type that has no row, no property and no declared default, in `attributes`. For an entity that no connection attaches to a bus, it lists its type's `bus`.
- each attribute stored as a curve, in `unsupported_values`.
- each attribute that a type stores a value for and does not take, in `ignored`.

### Entities

`build` builds each type's static frame from the type's entities, their properties and the rows of attributes over `entity` alone.
A default that differs per type, such as `sign`, `p_min_pu` or `p_set`, is a [`default_by`](schema.md) on `entity.ctype`, and `build` applies it.
`build` passes a type's members as the labels of `entity`, so a row with `entity` NULL applies to each member of the type it builds.

### Buses and carriers

A bus is a label of `bus`, and a carrier is a label of `carrier`.
What PyPSA stores on either is a property of the label or an attribute over the dim, under PyPSA's name:

| PyPSA                                                        | Record                                      |
| ------------------------------------------------------------ | ------------------------------------------- |
| `Bus.type`, `carrier`, `x`, `y`, `unit`, `location`          | properties of `bus`                         |
| `Bus.v_nom`, `v_mag_pu_min`, `v_mag_pu_max`, `v_mag_pu_set`  | attributes over `bus`                       |
| `Bus.marginal_price`, `v_mag_pu`, `v_ang`                    | result attributes over `bus`                |
| `Bus.p`, `q`, `control`, `generator`, `sub_network`          | not written                                 |
| `Line.v_nom`, `ShuntImpedance.v_nom`                         | not written                                 |
| `Carrier.color`, `nice_name`                                 | properties of `carrier`                     |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth` | attributes over `carrier`                   |
| a component's `carrier`                                      | `entity.carrier`, which refers to `carrier` |

`x` is a line's reactance and `bus.x` a bus's x position.
`bus.carrier` defaults to `AC`.
`build` rebuilds `n.buses` and `n.carriers` from the axes, their properties with defaults and the attributes over them.

### Ports

Each port of a component is a `connection` tuple `(entity, bus)`, and a per-port input or result is an attribute over `connection`.
`build` turns the tuples of a component back into PyPSA's numbered columns:

<figure class="diagram">
--8<-- "docs/static/tools-ports.svg"
</figure>

`bus0` is the connection with role `input`, and the other ports follow in [axis order](data-model.md).
The `efficiency` row at the second port is PyPSA's unsuffixed `efficiency`, and a third port's row is `efficiency2`.

- A Process's `rate0`, `rate1`, … are the one attribute `rate`, numbered from 0.
- `delay` and `cyclic_delay` are attributes over `connection`.
- A one-port component's connection has the role `attached`, and `build` writes its unsuffixed `bus`.

### A value its type does not take

A row with `entity` NULL applies to every entity, so `set("p_max_pu", 0.9)` stores a value that applies to a load too.
`build` gives each type only the names it takes: its registry inputs and results, and its custom columns.
`to_datarecord` lists each type's custom columns in the schema's `meta`.
`build` gives every type a name that no type takes.
`verify` lists each left-out `(type, attribute)` in `ignored`, which leaves the result falsy.

### Snapshots and weightings

A multi-period network's `(period, timestep)` snapshots are one flat `snapshot` axis, in the network's order.
Each snapshot has the property `snapshot.period`, which refers to `period`.
Where no timestep repeats across periods, a snapshot's label is its timestep.
Otherwise its label is its position, from `0`, and its timestep is the property `snapshot.timestep`, which refers to `timestep`.
`build` rebuilds the `(period, timestep)` index from the axis order and these properties.

PyPSA's weightings are attributes over their axis:

| Attribute                                                                                    | Over       |
| -------------------------------------------------------------------------------------------- | ---------- |
| `scenario_weight`                                                                            | `scenario` |
| `snapshot_weighting_objective`, `snapshot_weighting_stores`, `snapshot_weighting_generators` | `snapshot` |
| `period_weighting_objective`, `period_weighting_years`                                       | `period`   |

A label that no row applies to keeps PyPSA's default.

### Geometry

`Shape.geometry` is an attribute of dtype `str`, with each value as well-known text (WKT).
The schema's `meta` has the network's coordinate reference system as `crs`, such as `EPSG:3035`.
`build` sets the network's CRS from `crs` before it adds a component, and builds in `EPSG:4326` where `meta` has no `crs`.

### Writing a network

`to_datarecord(n, con)` presents the whole network.

- It writes `n.buses`, `n.carriers` and the components' frames in PyPSA's order.
- It adds each carrier that a component or bus has as its carrier and `n.carriers` lacks, and stores PyPSA's `""` carrier as NULL.
- It writes a value that is the same in every scenario as one row with `scenario` NULL.
- It writes each result except one still at its default, so `to_datarecord` writes no result for an unsolved network.
- A static column that the registry lacks is an attribute over its type's dim, and `meta` lists it per type.

`to_datarecord` raises `UnsupportedRecordError` for two components of two types with one name, reported in `names`.
It also raises for a custom column whose name the schema declares with other dims, such as `Carrier.efficiency`, reported in `columns`.

### Results on connections

A per-port result is an attribute over `connection`, as a per-port input is.
`p0`, `p1`, … of a Line, Link or Process and the one-port `p` of a Generator or Load are all the attribute `p`.
Each row is at its port's connection, with PyPSA's sign for that port.
`q0`, `q1` and `q` are `q`.
A Link's and a Process's own `p` is `p_internal` over `entity`.

### A results layer

`to_datarecord(n, con, results_only=True, base=record)` presents a layer to commit as a child of `base`:

```python
n = PyPSA.build(inputs.record)
n.optimize()
results = PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
solved = store.commit(results, parent=inputs)
```

`solved.record` has the results over `inputs`.

- The layer has the result attributes and the intermediate results they need, at their defaults too, and none of `base`'s inputs.
- A result row of `base` that this solve does not write again is restated as NULL, which `build` reads as PyPSA's default.
- The layer restates the `sub_network` axis with this solve's SubNetworks, and leaves the entity axis to `base`. Over a record without a `sub_network` axis, a solve without SubNetworks writes none.

!!! info "See also"

    [Use the PyPSA tool](../howto/use-the-pypsa-tool.md) shows a build, a solve and a results layer.
    [The API reference](api/tools.md) lists every signature.
