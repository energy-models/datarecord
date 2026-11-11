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
`base` is the record the layer is committed over as a child, and the record the model was built from.
With `results_only=True` it presents only what a solve produced, as a layer over `base`.
It raises `ValueError` where `results_only` comes without `base`.

### `Requirements`

`Requirements` is what `requires` and `verify` return, with one set per kind of fault.

| Field                 | Contains                                                                                                          |
| --------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `dims`                | dims the tool needs and the schema does not declare                                                               |
| `categories`          | categories the framework has no counterpart for                                                                   |
| `attributes`          | `(category, attribute)` pairs with no value, named in the record's words                                          |
| `unsupported_partial` | dims in `partial` the framework cannot take                                                                       |
| `unsupported_dims`    | dims the record declares that the framework's model has no place for                                              |
| `unsupported_values`  | `(category, attribute)` pairs stored as a curve, or varying along a dim the framework holds the attribute without |
| `names`               | labels that two of the framework's categories share                                                               |
| `ignored`             | `(category, attribute)` pairs that the record stores and the category does not take                               |

A `Requirements` is truthy when any field but `ignored` has an entry.
`a | b` returns a `Requirements` with the union of each field.
`describe()` returns a one-line summary, which `UnsupportedRecordError` quotes.

### The tool's schema

A tool's `Schema` lists, per category, the tool attributes that are renamed or computed, each an `Attr` that lists its source attributes.
An attribute with no entry keeps its name, so an empty `Schema` maps every name to itself.
`Attr.resolve` [settles](record.md) each source attribute before it renames or computes, so a computed attribute combines one row per coordinate of each source.

!!! info "See also"

    [Present a framework object as a record](../howto/present-a-framework-object.md) shows a tool author's `to_datarecord`.

## The PyPSA tool

```python
from datarecord.tools.pypsa import PyPSA, PyPSACollection
```

The PyPSA tool needs the optional extra `datarecord[pypsa]`.
`PyPSA` builds and writes a `pypsa.Network`, and `PyPSACollection` a `pypsa.NetworkCollection`.
Their `schema` is empty, because the record's names are PyPSA's.

### The schema

`to_datarecord` writes every network under one schema, declared from PyPSA's component registry:

| Dim                              | Is                                                                    |
| -------------------------------- | --------------------------------------------------------------------- |
| `entity`                         | a component of any type but `Shape` and those with a dim of their own |
| `ctype`                          | a dim with `one_of`: the component types                              |
| `bus`, `carrier`, `sub_network`  | a bus, a carrier, a SubNetwork as PyPSA numbers it                    |
| `line_type`, `transformer_type`  | a standard type                                                       |
| `connection`                     | a multi-dim over `(entity, bus)`: one port of a component             |
| `role`                           | a dim with `one_of`: `input`, `output` and `attached`                 |
| `snapshot`                       | a timestamp, or a multi-dim over `(period, timestep)` if multi-period |
| `timestep`, `period`, `scenario` | a timestep, an investment period by its year, a scenario              |

| Subject                         | Properties                                      |
| ------------------------------- | ----------------------------------------------- |
| `entity`                        | `ctype`, `carrier`, `sign`, `type`              |
| `bus`                           | `carrier`, `type`, `x`, `y`, `unit`, `location` |
| `carrier`                       | `color`, `nice_name`                            |
| `sub_network`                   | `carrier`                                       |
| `line_type`, `transformer_type` | every registry input of the type                |
| `connection`                    | `role`                                          |

`partial` is `{"entity", "bus", "scenario", "connection"}`.
`to_datarecord` writes no axis of `ctype` or `role`, since a dim with `one_of` lists its labels.
Every other registry input is an attribute, `p_nom` among them.
Every component attribute is over `scenario`, except those PyPSA keeps the same in every scenario.
These are `active`, `build_year`, `committable`, `lifetime`, `weight`, `*_nom_extendable`, `*_nom_mod` and the results `*_nom_opt`.

### `verify`

`verify` takes a record or a [selection](#selecting-before-a-build), and reports in a `Requirements`:

| Field                 | Entries                                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------- |
| `dims`                | `snapshot`, `period` or `scenario`, where the schema lacks it and no selection squeezes it   |
| `unsupported_partial` | `snapshot`, where it is in `partial`                                                         |
| `unsupported_dims`    | each dim of the record's own ([a collection](#a-collection))                                 |
| `categories`          | each `ctype` that PyPSA's registry lacks                                                     |
| `attributes`          | each required attribute with no row, property or declared default, a port's `bus` among them |
| `unsupported_values`  | each attribute stored as a curve, or varying along a dim PyPSA holds it without              |
| `ignored`             | each attribute a type has a value for and does not take                                      |

When `verify` finds a dim in `partial` or a dim it cannot take, it reports nothing else.
It raises `ValueError` for a selection that `build` refuses.

### Entities

`build` builds each type's static frame from its entities, their properties and the rows of attributes over `entity` alone.
A default that differs per type, such as `sign` or `p_set`, is a [`default_by`](schema.md#default_by) on `entity.ctype`.
A row with `entity` NULL applies to each member of the type `build` builds.

### Buses and carriers

A bus is a label of `bus`, and a carrier is a label of `carrier`:

| PyPSA                                                        | Record                                      |
| ------------------------------------------------------------ | ------------------------------------------- |
| `Bus.type`, `carrier`, `x`, `y`, `unit`, `location`          | properties of `bus`                         |
| `Bus.v_nom`, `v_mag_pu_min`, `v_mag_pu_max`, `v_mag_pu_set`  | attributes over `bus`                       |
| `Bus.marginal_price`, `v_mag_pu`, `v_ang`                    | result attributes over `bus`                |
| `Bus.p`, `q`, `control`, `generator`, `sub_network`          | not written                                 |
| `Line.v_nom`, `ShuntImpedance.v_nom`                         | not written                                 |
| `SubNetwork.obj`                                             | not written                                 |
| `Carrier.color`, `nice_name`                                 | properties of `carrier`                     |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth` | attributes over `carrier`                   |
| a component's `carrier`                                      | `entity.carrier`, which refers to `carrier` |
| a custom column                                              | see [custom columns](#custom-columns)       |

`bus.x` is a bus's x position, and `x` a line's reactance.
`bus.carrier` defaults to `AC`.

### Standard types

A line type is a label of `line_type`, and a transformer type a label of `transformer_type`.
Every registry column of a type is a property of its dim, so `transformer_type.s_nom` is apart from a transformer's attribute `s_nom`.
`entity.type` is text that refers to no dim.
`to_datarecord` writes only the types that PyPSA's library lacks, and `build` adds them to the new network's library.
A library type has the library's values in the built network.

### Ports

Each port of a component is a `connection` tuple `(entity, bus)`, and a per-port input or result is an attribute over `connection`.
`build` turns the tuples of a component back into PyPSA's numbered columns:

<figure class="diagram">
--8<-- "docs/static/tools-ports.svg"
</figure>

`bus0` is the connection with role `input`, and the other ports follow in [axis order](data-model.md#dims-and-labels).
The `efficiency` row at the second port is PyPSA's `efficiency`, and at a third port it is `efficiency2`.
A Process's `rate0`, `rate1`, … are the one attribute `rate`.
`rate` declares no default, and a port with no row keeps PyPSA's default for that port.
`delay` and `cyclic_delay` are over `connection`.
A one-port component's connection has the role `attached`.

### Results on connections

`p0`, `p1`, … of a Line, Transformer, Link or Process are the attribute `p` over `connection`.
So is the one-port `p` of a Generator, Load, Store, StorageUnit or ShuntImpedance.
Each row has PyPSA's sign for its port, and `q0`, `q1` and `q` are `q`.
A Link's and a Process's own `p` is `p_internal` over `entity`.

### Custom columns

A custom column is a static column that PyPSA's registry lacks, placed on its type's dim:

| Custom column                                                                       | Placed as                 |
| ----------------------------------------------------------------------------------- | ------------------------- |
| a text column, such as `Bus.country`                                                | a property, `bus.country` |
| a column of a standard type                                                         | a property                |
| a column that types of two dims have, or that the registry declares over other dims | a property                |
| any other column, such as `Bus.nom_max_onwind_2050`                                 | an attribute over the dim |

A custom property takes its dtype from its values, and has no default.
A text property named like a dim refers to that dim.
`meta["custom_columns"]` lists each component type's custom columns.
`to_datarecord` raises `ValueError` for a custom property that two types of one dim have with two dtypes.
It also raises for a value of a referring property that names no label.
It raises for a custom column on a Shape, or named `shape_…`, `snapshot`, `timestep`, `period`, `scenario`, `entity` or `connection`.

### A value its type does not take

A row with `entity` NULL applies to every entity, so `set("p_max_pu", 0.9)` applies to a load too.
`build` gives each type only its registry inputs and results, its custom columns, and the names that no type takes.
`verify` lists each left-out `(type, attribute)` in `ignored`.
`build` also leaves out an `entity` property that a type does not take, and `verify` does not list it.

### Snapshots and weightings

A single-period `snapshot` is a `datetime` dim.
A multi-period `snapshot` is a multi-dim over `(period, timestep)`, where `timestep` has each timestep once.
`build` rebuilds PyPSA's index from the tuples in axis order.
[`set`](editing.md#set) takes a multi-period snapshot by `period` and `timestep` together, and refuses one alone.

| Attribute                                                                                    | Over       |
| -------------------------------------------------------------------------------------------- | ---------- |
| `scenario_weight`                                                                            | `scenario` |
| `snapshot_weighting_objective`, `snapshot_weighting_stores`, `snapshot_weighting_generators` | `snapshot` |
| `period_weighting_objective`, `period_weighting_years`                                       | `period`   |

A label that no weighting row applies to keeps PyPSA's default, which for a scenario is an equal share.

### Geometry

A shape is the property `shape_{type}` of the label `idx` it outlines, as well-known text (WKT) of dtype `str`.
A `Bus` shape is a property of `bus`, and a component type's of `entity`.
Any other `component`, such as `country`, is a dim of that name, outside `partial`, with the shapes' `idx` as labels.
Such a dim is no dim of the record's own.
So Germany's offshore waters are `country.shape_offshore` at label `DE`.

`to_datarecord` raises `ValueError` for a shape without a `component`, an `idx` or a `type`, and for two shapes with one `(component, idx, type)`.
It also raises for an `idx` that names no label its type writes, and for a `component` that names a dim of the PyPSA schema.
`meta["crs"]` is the network's coordinate reference system, such as `EPSG:3035`.
`to_datarecord` does not write PyPSA's `srid`.
`build` sets the CRS before it adds a component, and uses `EPSG:4326` where `meta` has no `crs`.
`build` names each shape `"{component} {idx} {type}"`, such as `"Bus DE onshore"`.

### Selecting before a build

`build` and `verify` take a [selection](selecting.md) as they take a record:

| Selected                                 | One label                                            | A list, a slice or a partial date               |
| ---------------------------------------- | ---------------------------------------------------- | ----------------------------------------------- |
| `period`                                 | a single-period network, without `investment_period` | a multi-period network                          |
| `scenario`                               | a deterministic network                              | a stochastic network, weights renormalised to 1 |
| a dim of the record's own                | the member at that label                             | a `PyPSACollection` build                       |
| another dim, such as `snapshot` or `bus` | refused                                              | the network at those labels                     |

`build` then drops each period left without snapshots, and each entity attached to a dropped bus.
Over fewer periods than the record has, it drops each entity active in none, where `build_year <= period < build_year + lifetime`.
An entity without a row of `build_year` or `lifetime` takes the declared default, else `0` or infinity.
`build` also drops each `GlobalConstraint` whose `investment_period` names a dropped period, and keeps one with `investment_period` NULL.
A kept scenario that no `scenario_weight` row applies to takes its equal share of the record's scenarios before the renormalisation.
`build` raises `ValueError` for a selection with no snapshot, and for kept scenarios whose weights sum to 0.
It raises for an attribute whose rows name several labels of a dim PyPSA holds it without, such as `marginal_cost` over `period`.
Over fewer periods, it raises where `build_year`, `lifetime` or `investment_period` varies along a dim the selection keeps.

### Writing results

`to_datarecord(n, con)` writes the whole network, its frames in PyPSA's order.
It adds each carrier that `n.carriers` lacks, writes PyPSA's `""` carrier as NULL, and writes a value shared by every scenario with `scenario` NULL.
It leaves out a static value at its default, so an unsolved network has no result.
It writes each input at a port, at its default too, and each custom column placed as an attribute.
It raises `UnsupportedRecordError` for two types with one name, and `ValueError` for a property that differs between scenarios.

`to_datarecord(n, con, base=record)` writes only what changed from `base`:

| Value          | Written where                                                                      |
| -------------- | ---------------------------------------------------------------------------------- |
| a static value | it differs from `base`'s resolved value, or from the default where `base` has none |
| a series       | whole, where it differs from `base`'s at one snapshot or more                      |
| an axis        | whole, always                                                                      |

A value with `scenario` NULL is left out only where `base` resolves the same value at each scenario.
An input at a port or a custom column placed as an attribute is written wherever `base` has no value for it.
A value set back to its default over a `base` with another is written.
Values compare exactly, and NULL equals NULL.
`to_datarecord` raises `ValueError` where `base` is a selection, has a dim of its own, or has other investment periods than the network.

`to_datarecord(n, con, results_only=True, base=record)` presents only the solve's results:

```python
results = PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
solved = store.commit(results, parent=inputs)
```

- The layer has the results and the intermediate results they need, at their defaults too. It has no input, shape or standard type.
- A result of `base` that the solve does not write again is NULL, at the scenarios, buses, entities and connections the network has.
- A series is written at the network's snapshots, and `base`'s values stand at the others.
- Over a multi-period `base`, a result such as `p_nom_opt` is written for the entities active in the network's periods.
- Over a selection, [`lift`](selecting.md#lift) puts each row back at the squeezed labels.
- The layer restates the `sub_network` axis with the solve's SubNetworks, empty where the solve has none. It writes no such axis where neither the solve nor `base` has one.
- Over a network with fewer buses or entities than `base`, the layer leaves the `sub_network` axis to `base`. It also leaves to `base` the results that name a SubNetwork, such as `slack_bus` and a line's `sub_network`.

With `results_only=True`, `to_datarecord` raises `ValueError` for:

- a missing `base`.
- a network with a snapshot, bus or entity that `base` lacks.
- a deterministic network over a `base` with scenarios, rather than over the selection at one scenario.
- a result over no `scenario` from fewer scenarios than the record has.
- a multi-period network over a single-period `base`, or a single-period one over a multi-period `base` that squeezes no period.
- a network over a record with a dim of its own, rather than over the selection at one label.

### A collection

`PyPSACollection` writes and builds a `pypsa.NetworkCollection`, each member through `PyPSA`.
Its dim is the name of the collection's one index level, either `period` or a dim of the record's own.
A record has at most one dim of its own.
`to_datarecord` checks every member before it returns, and raises `ValueError` for:

- `base` without `results_only=True`.
- an index of more than one level, or one named for a dim of the PyPSA schema but `period`.
- an index named for a dim that the members' shapes make.
- a `base` with another dim of its own.
- members that differ where they must agree.
- a member that orders shared labels otherwise than the members before it.

Two floats agree within `numpy.isclose`'s default tolerance, and other values agree where equal.
The record's `meta` and `crs` are the first member's.
`verify` checks each member as `PyPSA.verify` checks a selection, and reports what any member lacks.
`build` raises `ValueError` for a single-period record without a dim of its own.
`verify` reports each dim of its own of a record with several in `unsupported_dims`, and `build` raises `UnsupportedRecordError` for it.

#### Over `period`

`to_datarecord` writes a myopic solve as one multi-period network with every member's period.
Each member is deterministic and has its own period alone.
Two members that hold an entity active and give it different static values are refused.
An asset that a later member lacks is retired, its `lifetime` ending at the first period without it.
An asset held again after a gap is refused.
`to_datarecord` numbers the SubNetworks once for the whole network.
`build` gives member `p` as `PyPSA.build(record.select(period=[p]))`.

#### Over a dim of its own

`to_datarecord` adds the dim to the schema and to `partial`.
An attribute whose values agree across the members is written once, as declared, and one whose values differ varies over the dim.
A later layer whose values differ widens the attribute's `dims` by the dim.
Members must have the same snapshots, components, schema and custom columns.
Their axes are one union in the first member's order, and a label's properties agree across them.
`build` gives member `label` as `PyPSA.build(record.select(dim=label))`.
Results of a collection of fewer labels than `base` has are at each member's label, even where the members agree.
They leave the other labels, the dim's axis and the SubNetworks to `base`.
`to_datarecord` raises `ValueError` for a member's label that `base`'s axis of the dim lacks.

!!! info "See also"

    [Use the PyPSA tool](../howto/use-the-pypsa-tool.md) shows a build, a solve, a selection, a change-only export and a collection.
    [The API reference](api/tools.md) lists every signature.
