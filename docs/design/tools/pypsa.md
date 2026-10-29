<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# PyPSA

The PyPSA tool maps components to `entity` labels with `ctype` as a category, and buses and carriers to dims of their own.
Each port of a component is a `connection` tuple, and per-port inputs and results are attributes over that multi-dim.
Multi-period snapshots are one flat axis with a `period` property.
A solve writes its results as a thin child layer.

## Entities and their properties

PyPSA's registry decides what is a property and what is an attribute.
The properties of `entity` are exactly `ctype`, `carrier`, `sign` and `type`, and `role` is a property of `connection`.
Every other input, every number among them, is an attribute, so `p_nom` is long rows of `attributes/p_nom.parquet` ([where a value lives](../storage/format.md#where-a-value-lives)).

`entity.ctype` is `{one_of: ctype}`, where `ctype` is a dim with `one_of` that holds the types exported as entities.
`connection.role` is `{one_of: role}`, where `role` is a dim with `one_of` that holds `input`, `output` and `attached`.
`to_datarecord` writes neither axis: the axis of a [dim with `one_of`](../model/dims.md#a-dim-with-one_of) is its list, every type and role, those the network leaves unused included.
`connection` is in `partial`, so `role` [categorizes it](../model/multi-dims.md#categories-of-a-multi-dim): `flags(role="output")` answers for the output connections, and `role="output"` selects them in `set`.

A default that differs per component, such as `sign`, `p_min_pu` or `p_set`, is a [`default_by`](../model/properties.md#default_by-a-default-per-category) on `entity.ctype`.
`build` applies it when it builds a component's static frame from the type's entities and the entity-only rows of its attributes.

The PyPSA schema declares `partial={"entity", "bus", "scenario", "connection"}`.
A layer then patches one link's `efficiency` series, or one bus's `v_nom`, and inherits every other value from its parent ([`partial`](../layers/tree.md#partial-the-granularity-of-an-override)).
`entity` is in `partial` also because `ctype` [categorizes it](../model/properties.md#categories).
`carrier` is outside `partial`, so a layer restates every carrier's `co2_emissions`, a few dozen rows.

## What `verify` reports

`verify` catches what the record layer cannot: an entity type that PyPSA has no registry entry for, a connection `role` it cannot place, and a `partial` set that breaks PyPSA's split between constant and varying data.
The record checks a category against the `one_of` of its dim, and the tool checks it against PyPSA's registry.

`verify` is also where PyPSA's names per type meet [names unique across the record](../storage/format.md#a-label-is-unique-within-its-dim).
PyPSA permits a `Generator` and a `Load` both called `north`, and `to_datarecord` reports such a network as unbuildable.
A record cannot hold two entities under one key.
Collisions are within a dim.
A bus is a label of `bus`, so a `Bus` and a `Load` called `north`, as in every clustered pypsa-eur network, do not collide.
SubNetworks, which PyPSA names `0`, `1`, … on every topology pass as it numbers Lines, are labels of their own dim `sub_network` ([writing results](#writing-results)).

The tool reports a collision and repairs none.
A renamed `Generator:north` would come back as a component that PyPSA cannot find by its own name, and the framework's vocabulary belongs to the framework.
PyPSA itself is moving to names unique across the network, which removes the constraint.

## Buses and carriers are dims

`Bus` and `Carrier` are no entity types.
A bus is a label of `bus`, and a carrier is a label of `carrier`.
[The properties rule](../model/properties.md#properties) places what PyPSA stores on either, and every name stays PyPSA's:

| PyPSA                                                                   | record                                               |
| ----------------------------------------------------------------------- | ---------------------------------------------------- |
| `Bus.type`, `Bus.carrier`, `Bus.x`, `Bus.y`, `Bus.unit`, `Bus.location` | properties of `bus`                                  |
| `Bus.v_nom`, `Bus.v_mag_pu_min`, `Bus.v_mag_pu_max`, `Bus.v_mag_pu_set` | attributes over `bus` (and the varying dims)         |
| `Bus.marginal_price`, `Bus.v_mag_pu`, `Bus.v_ang`                       | result attributes over `bus` and the varying dims    |
| `Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator`, `Bus.sub_network`     | not written: derived from the connections            |
| `Line.v_nom`, `ShuntImpedance.v_nom`                                    | not written: derived from the bus                    |
| a custom Bus or Carrier column                                          | attribute over its dim, as a custom entity column is |
| `Carrier.color`, `Carrier.nice_name`                                    | properties of `carrier`                              |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth`            | attributes over `carrier`                            |
| a component's `carrier`                                                 | `entity.carrier`, `{one_of: carrier}`                |

So `x` is a line's reactance and `bus.x` is a bus's coordinate, each addressed by its own name.
`entity.carrier` and `bus.carrier` [refer to](../model/properties.md#properties) `carrier`, and `bus.carrier` defaults to `AC`.
`to_datarecord` writes both axes in PyPSA's order.
It adds every carrier that a component or bus names and `n.carriers` lacks, and it stores PyPSA's `""` carrier as NULL, which names no carrier.
`build` rebuilds `n.buses` and `n.carriers` from the axis, the properties with their defaults and the attribute rows, each series by its [flags](../use/reading.md#flags), the bus results among them.

PyPSA's `calculate_dependent_values` copies `Line.v_nom` and `ShuntImpedance.v_nom` from the bus.
The tool does not write them, and `build` gets them back when PyPSA recomputes them.

`remove("bus", …)` is [the cascade every dim has](../layers/removal.md).
An entity that it leaves attached to no bus is `verify`'s to report, as its type's missing `bus`.
`remove("carrier", …)` is refused while a stored `carrier` names the label.

## Ports

Each port of a component is a `connection` tuple `(entity, bus)`.
The schema declares `connection` under `dimensions` as the [multi-dim](../model/multi-dims.md) `Dimension(over={"entity": "entity", "bus": "bus"})`, and its tuples live in `dims/connection.parquet` with `role` beside them.
A per-connection input follows its connection's position: `efficiency` on a Link's `bus2` connection is PyPSA's `efficiency2`, static or per snapshot.
The tool collapses bus-keyed connections back to PyPSA's positional encoding, ordered by the [connection order](../model/dims.md#axis-order).
It translates a curve or reports it unbuildable.

A Process's `rate0`, `rate1`, `rate2`, … are likewise the one input `rate`.
They are numbered from `0` because PyPSA registers `rate0` and leaves port `1`'s `efficiency` unsuffixed.
A Process's ports default differently, `-1` at the input end and `1` at the others, so `rate` declares no default, and a port with no row keeps PyPSA's own.

The tool reads which stems are per port off PyPSA's registry: a name that ends in a port index, plus a Link's unsuffixed `efficiency`, `delay` and `cyclic_delay`.
Every per-port input, `delay` and `cyclic_delay` included, is long rows of `attributes/` over `connection`, since a number is an attribute whatever its `dims`.

## Results on connections

A per-port result is stored like a per-port input: one attribute over the [`connection` multi-dim](#ports), one row per `(entity, bus)`, carried as the one `connection` struct column.
`to_datarecord` packs the struct, and `build` unpacks it back to `entity` and `bus` where it places ports.
PyPSA's `p0`, `p1`, `p2`, … on a Line, Transformer, Link or Process and the one-port `p` on a Generator, Load, Store, StorageUnit or ShuntImpedance are all the result `p`.
Each row sits on the connection its port attaches to and keeps PyPSA's sign for that port.
`q0`, `q1` and the one-port `q` are `q`.
A third port needs no declaration of its own: a Link's `p2` is `p` on its `bus2` connection, which the schema built from the bare registry already declares.

The tool names the two results that follow another rule.
`Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator` and `Bus.sub_network` are not written.
`p` and `q` are the signed sum of the connection `p` and `q` at that bus, so they are derivable, and a bus-level `p` would collide with the connection-level one.
The other three follow from the connections and the topology.
`Link.p` and `Process.p` are written as `p_internal`.
PyPSA's internal power sits at no bus, so it is a result over `entity`, and it takes another name because one name holds one spec across the record.

The schema's attributes and the frames that `to_datarecord` presents follow one rule, so every result is written under a name the schema declares.
`build` turns `p` back into PyPSA's per-port columns as it does a per-port input, and `p_internal` into `Link.p`.

## Custom columns

A static column that PyPSA's registry lacks is an attribute over its type's dim.
Where that dim is `entity`, `build` puts it onto that type alone ([below](#a-value-its-type-does-not-take)).
One name is one spec, so a custom `Bus.country` beside a custom `Generator.country` would be one `country` with two `dims`.
`Carrier.efficiency` likewise meets the per-port `efficiency`.
`to_datarecord` reports either clash in `Requirements.columns`.
A custom column clashes only with an attribute of the same name, so a custom `Generator.location` is an attribute beside the property `bus.location`.

## A value its type does not take

A NULL `entity` reaches every entity ([the broadcast rule](../model/attributes.md#the-broadcast-rule)), so `set("p_max_pu", 0.9)` stores a value that a load reaches.
PyPSA's registry says which attributes and `entity` properties each type takes, and the tool keeps that knowledge.
`build` hands a type only the names it takes, so it leaves the load's value out.

A type takes its registry inputs and results and the custom columns its network was exported with.
`to_datarecord` records those per type in the schema's `meta`.
A name that no type claims, such as an attribute a record adds, reaches every type.
So a custom `Generator.e_nom` is built onto generators, though the registry gives `e_nom` to stores.

`verify` reports each `(type, attribute)` whose stored value is left out in `Requirements.ignored`.
An `entity` property left out goes unreported, since its column exists on every entity whatever its type.
The `ignored` report is informational: it leaves the result falsy, and `build` does not raise for it.
A value meant for the types that take it is written per label, as in `set("p_max_pu", 0.9, ctype="Generator")`.

## Snapshots and weightings

PyPSA's multi-period `(period, timestep)` snapshots are one flat `snapshot` axis.
Each snapshot carries its period as `snapshot.period`, a property that [refers to](../model/properties.md#properties) `period`, and `period` is an axis of its own beside it.
A snapshot is labelled by its timestep where no timestep repeats across periods.
Otherwise it is labelled by its position, `0` to `n-1` across every period in the network's order, and its timestep becomes the property `snapshot.timestep`.
That property refers to the `datetime` dim `timestep`, PyPSA's own index level name, whose axis holds each timestep once.
The `pd.MultiIndex.from_product` of `set_investment_periods` repeats every timestep, so its snapshots are positions.

A multi-period network declares its attributes over `snapshot` as a single-period network does, and a single-period network's schema declares no property of `snapshot`.
`build` rebuilds the `(period, timestep)` index from the axis order, `snapshot.period`, and the label or `snapshot.timestep`.
A period is selected like any property of a dim that an attribute reaches: `set("p_max_pu", 0.5, entity="wind", period=2030)` stages the snapshots of 2030 alone ([`set`](../use/editing.md#set)).

A weighting is an attribute over its axis.
PyPSA's weighting frames become `scenario_weight` over `scenario`, `snapshot_weighting_objective`, `snapshot_weighting_stores` and `snapshot_weighting_generators` over `snapshot`, and `period_weighting_objective` and `period_weighting_years` over `period`.
The tool writes them for each axis the network holds, and a multi-period network's snapshot weightings are over its flat `snapshot` like any other.
`build` sets them back, and a label that no row reaches keeps PyPSA's own default, an equal share for a scenario.

## Unique coordinates

The tool's source [vouches](../use/writing.md#uniquecoordinates) for an attribute's rows where every pandas index they are built from is unique, and on a stochastic network only for an attribute over `scenario`, which it reads off the network in milliseconds, before a row is melted.
The writer then skips its check for two rows naming the same dims at one coordinate there, the costliest check of an hourly network's write.
The answer is per attribute: one type's repeated name spoils only the attributes that type contributes to.
Names are compared as strings, as the check on names shared across entity types compares them.

`is_unique(a)` is True where all of the following hold, for every type contributing rows to `a`:

1. **The snapshot labels are unique**, for a dynamic frame and a snapshot weighting. A time series is melted by position and each label looked up, so two equal labels put two rows of one entity at one snapshot. A multi-period network whose timesteps repeat is labelled by position and always passes.
2. **The type's static index is unique**, `(scenario, name)` on a stochastic network. A static value is one row per index entry, and a per-port value joins the port's bus column once per repeat of a name.
3. **Each contributing dynamic frame's columns are unique and name members of the type**, a `(scenario, name)` pair each on a stochastic network. A column naming another type's member lands two types' rows on one entity, which the check on shared names misses, since it reads static indexes alone.
4. **No entity attaches one bus at two ports**, for a per-port attribute, whose rows are keyed by `(entity, bus)`. The `connection` axis refuses such a pair on a deterministic network, but a stochastic network's connections collapse it to one tuple.
5. **No scenario or name holds `\x1f`** in a stochastic network's dynamic frame, the separator its columns are melted through.
6. **A weighting's axis labels are unique**: the snapshot labels, `investment_period_weightings.index` or `scenario_weightings.index`.
7. **An attribute other than a weighting is declared over `scenario`**, on a stochastic network. One that is not, a static input among them, keeps an entity's row of each scenario where its values differ, and its rows carry no `scenario` column, so they land at one coordinate.

Two things need no condition.
An entity's static and series rows never meet: the static value of a name that the dynamic frame has a column for is left out, and a static row's `snapshot` is NULL where a melted row's is a label.
Names shared across entity types are refused before `to_datarecord` returns a source.

A results layer vouches by the same conditions over the solve's outputs, for a result that `base` holds no rows of.
For one it does, `base`'s rows restated as NULL are those the solve does not write again, so they never meet its rows, and they are twin-free where `base`'s are.
There it vouches where `base` does too, which a `Record` always does.

## Writing results

A framework's outputs are [attributes](../model/attributes.md).
`to_datarecord` writes them with every other attribute, so a solved network round-trips its results through `attributes/`.
`build` copies every attribute the record holds into the network, a solved bus's `marginal_price` included.
Only the tool knows which attributes are outputs, from PyPSA's registry status "Output".
It reads that status on import alone, and the status decides only what `results_only` presents.
A full export leaves out a result still at its default, so an unsolved network writes none.

With `results_only=True`, `to_datarecord` presents only what the solve produced.
It is a thin layer written as a child of `base`, the record the model was built from.
`base` is required with `results_only` and refused without it.

```python
inputs = rev.child(w.edits)  # edited inputs are committed first
n = PyPSA.build(inputs.record)
n.optimize()
solved = inputs.child(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
)
```

The results layer goes straight to a write, and no `WorkingRecord` stages it.
It holds the result attributes and the intermediate results they need, since a solve derives structure as well as values.
A result at its default is written too, so a re-solve's value stands over its parent's.
`base`'s inputs are never written.

A result row that `base` holds and the solve does not write again, such as the `mu_*` duals under `assign_all_duals=False`, is restated as NULL, so no stale value is inherited.
NULL reads as no value, where a `marginal_price` at its default `0.0` would read as a price, and `build` leaves PyPSA its own default there.
A layer removes a label or a tuple by restating its axis without it, a multi-dim's included, and has no way to remove a value, so restating is how a value is replaced.

A PyPSA SubNetwork is a label of the dim `sub_network`, `0`, `1`, … as PyPSA names it.
`sub_network.carrier` is a property that refers to `carrier`, `slack_bus` is an attribute over `sub_network`, and a line's `sub_network` is an attribute over `entity` that holds the plain label.
`sub_network` stays outside `partial`, since a results layer writes every SubNetwork's values.
A results layer restates the `sub_network` axis alone and leaves the entity axis to `base`.
A re-solve that numbers SubNetworks differently therefore leaves no stale label behind and rewrites no input axis.
A solve without SubNetworks over a record with a `sub_network` axis restates the axis empty, its columns typed as the schema declares them, so the record holds none of the earlier SubNetworks.
Over a record without that axis, it writes no `sub_network` axis.
A full export writes the axis beside `bus` and `carrier`, and `build` adds the SubNetworks from it.
A results layer meets full validation, so it carries the labels its results name.

## Geometry

A geometry is text.
PyPSA's `Shape.geometry`, the column its registry types `geometry`, is an attribute like `idx` and `component` beside it.
Its values are [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) in `attributes/geometry.parquet`, declared `String`.
A network has one coordinate reference system, so the CRS is `crs` in the schema's [`meta`](../model/schema.md), as the CRS's authority string (`EPSG:3035`) where it has one.
The tool does not write PyPSA's `srid` network attribute, that CRS's EPSG code.

`build` sets the network's CRS from `crs` before it adds any component, and parses each geometry back into a GeoSeries in that CRS.
A record without `crs` builds in PyPSA's default, `EPSG:4326`.
