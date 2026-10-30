<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# PyPSA

The PyPSA tool maps components to `entity` labels with `ctype` as a category, and buses, carriers, SubNetworks, line types and transformer types to dims of their own.
A shape is a property of the label it outlines.
Each port of a component is a `connection` tuple, and per-port inputs and results are attributes over that multi-dim.
Multi-period snapshots are one flat axis with a `period` property.
A solve writes its results as a thin child layer.

## Entities and their properties

PyPSA's registry decides what is a property and what is an attribute.
The registry's properties of `entity` are exactly `ctype`, `carrier`, `sign` and `type`, and `role` is a property of `connection`.
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

## Standard types

Every new PyPSA network holds a library of `LineType` and `TransformerType` rows.
A network may add types of its own: PyPSA-Earth adds a `ZM_330kV_bison_2bundle` that its lines use.
`entity` is the network's topology, and a type is no entity: a line type is a label of the dim `line_type`, and a transformer type a label of `transformer_type`.
Every registry column of a type, such as `x_per_length` or `s_nom`, is a property of its dim, since a type is catalog metadata that describes its label.
A type's columns therefore clash with no attribute: `transformer_type.s_nom` is a column of `dims/transformer_type.parquet`, and a transformer's own `s_nom` an attribute over `entity`.
`entity.type` is plain text that names the type and refers to no dim, so a selection of lines keeps every type.
`to_datarecord` writes a type the library lacks as a label of its dim, and `build` adds it to the library that the new network already holds.

A type is the library's by name, as in PyPSA's own netCDF export.
`to_datarecord` writes no row the library names, even one whose values the network has changed.
`build` therefore restores that row with the library's values.
A results layer writes no type.
`line_type` and `transformer_type` stay outside `partial`, as do `carrier` and `sub_network`.

## What `verify` reports

`verify` catches what the record layer cannot: an entity type that PyPSA has no registry entry for, a connection `role` it cannot place, and a `partial` set that breaks PyPSA's split between constant and varying data.
The record checks a category against the `one_of` of its dim, and the tool checks it against PyPSA's registry.

`verify` is also where PyPSA's names per type meet [names unique across the record](../storage/format.md#a-label-is-unique-within-its-dim).
PyPSA permits a `Generator` and a `Load` both called `north`, and `to_datarecord` reports such a network as unbuildable.
A record cannot hold two entities under one key.
Collisions are within a dim.
A bus is a label of `bus`, so a `Bus` and a `Load` called `north`, as in every clustered pypsa-eur network, do not collide.
SubNetworks, which PyPSA names `0`, `1`, … on every topology pass as it numbers Lines, are labels of their own dim `sub_network` ([writing results](#writing-results)).
A line type and a transformer type are labels of their own dims too, so a type may share its name with a line ([standard types](#standard-types)).

The tool reports a collision and repairs none.
A renamed `Generator:north` would come back as a component that PyPSA cannot find by its own name, and the framework's vocabulary belongs to the framework.
PyPSA itself is moving to names unique across the network, which removes the constraint.

## Buses and carriers are dims

`Bus` and `Carrier` are no entity types.
A bus is a label of `bus`, and a carrier is a label of `carrier`.
[The properties rule](../model/properties.md#properties) places what PyPSA stores on either, and every name stays PyPSA's:

| PyPSA                                                                   | record                                            |
| ----------------------------------------------------------------------- | ------------------------------------------------- |
| `Bus.type`, `Bus.carrier`, `Bus.x`, `Bus.y`, `Bus.unit`, `Bus.location` | properties of `bus`                               |
| `Bus.v_nom`, `Bus.v_mag_pu_min`, `Bus.v_mag_pu_max`, `Bus.v_mag_pu_set` | attributes over `bus` (and the varying dims)      |
| `Bus.marginal_price`, `Bus.v_mag_pu`, `Bus.v_ang`                       | result attributes over `bus` and the varying dims |
| `Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator`, `Bus.sub_network`     | not written: derived from the connections         |
| `Line.v_nom`, `ShuntImpedance.v_nom`                                    | not written: derived from the bus                 |
| `SubNetwork.obj`                                                        | not written: `build` makes a fresh one            |
| a custom Bus or Carrier column                                          | [property or attribute](#custom-columns)          |
| `Carrier.color`, `Carrier.nice_name`                                    | properties of `carrier`                           |
| `Carrier.co2_emissions`, `max_growth`, `max_relative_growth`            | attributes over `carrier`                         |
| a component's `carrier`                                                 | `entity.carrier`, `{one_of: carrier}`             |

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

A static column that PyPSA's registry lacks is placed by what it holds and what its name meets, on any type.
Its subject is its type's dim: `entity`, `bus`, `carrier`, `sub_network`, `line_type` or `transformer_type`.
On a type it is a property, as every column of a type is.

- A text column describes its label, so it is a property of the subject.
  pypsa-eur's `Bus.country` is `bus.country`, a column of `dims/bus.parquet` beside `x` and `y`, and a sector `Generator.country` is `entity.country`.
- A column of any dtype whose name, as an attribute, would hold two shapes is a property too.
  One name is one attribute spec, so a custom `Generator.v_nom` would meet `v_nom` over `bus`, `Carrier.efficiency` the per-port `efficiency`, and a numeric custom column on `Bus` and on `Generator` would be one attribute over two dims.
  A property is its subject's column, so it clashes with nothing.
- Any other column is an attribute over its subject, such as pypsa-eur's `Bus.nom_max_onwind_2050`, which a scenario patches as it does `p_nom_max`.

A text property named like a dim the record declares [refers to](../model/properties.md#referring-to-a-dim) that dim.
pypsa-eur's `bus.country` is `{one_of: country}` where its shapes make a `country` dim ([geometry](#geometry)), so a value is a country's label or NULL, and `to_datarecord` refuses one that names no label.
A custom column named `shape_…` is refused, since that prefix names a shape's geometry, and so is a custom column on a Shape.
So is one named like a dim that keys a record's rows, `snapshot`, `timestep`, `period`, `scenario`, `entity` or `connection`, which no property or attribute is named like.

A name placed as a property is one for every type of that dim carrying it, typed from its values and without a default.
`to_datarecord` refuses one that two types carry with different dtypes.
`build` sets each back on the types that carried it alone ([below](#a-value-its-type-does-not-take)).
A custom column on one type never clashes with a property of another subject, so a custom `Generator.location` is `entity.location` beside `bus.location`.

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
A multi-period snapshot is labelled by its position, `0` to `n-1` across every period in the network's order, and its timestep becomes the property `snapshot.timestep`.
That holds whether or not a timestep repeats across periods, as the `pd.MultiIndex.from_product` of `set_investment_periods` repeats every one, so a child whose timesteps repeat where its parent's did not keeps the parent's labels and restates `snapshot.timestep`.
That property refers to the `datetime` dim `timestep`, PyPSA's own index level name, whose axis holds each timestep once.
A single-period network's snapshot is labelled by its timestep.

A multi-period network declares its attributes over `snapshot` as a single-period network does, and a single-period network's schema declares no property of `snapshot`.
`build` rebuilds the `(period, timestep)` index from the axis order, `snapshot.period`, and the label or `snapshot.timestep`.
A period is selected like any property of a dim that an attribute reaches: `set("p_max_pu", 0.5, entity="wind", period=2030)` stages the snapshots of 2030 alone ([`set`](../use/editing.md#set)).

A weighting is an attribute over its axis.
PyPSA's weighting frames become `scenario_weight` over `scenario`, `snapshot_weighting_objective`, `snapshot_weighting_stores` and `snapshot_weighting_generators` over `snapshot`, and `period_weighting_objective` and `period_weighting_years` over `period`.
The tool writes them for each axis the network holds, and a multi-period network's snapshot weightings are over its flat `snapshot` like any other.
`build` sets them back, and a label that no row reaches keeps PyPSA's own default, an equal share for a scenario.

## Writing results

A framework's outputs are [attributes](../model/attributes.md).
`to_datarecord` writes them with every other attribute, so a solved network round-trips its results through `attributes/`.
`build` copies every attribute the record holds into the network, a solved bus's `marginal_price` included.
Only the tool knows which attributes are outputs, from PyPSA's registry status "Output".
It reads that status on import alone, and the status decides only what `results_only` presents.
A full export leaves out a static value still at its default, so an unsolved network writes no result.

A full export may take `base`, the record it is written over as a child.
It then leaves out a static value only where it repeats what `base` resolves there: `base`'s value for that entity and scenario, else the default.
A value back at its default over a `base` holding another is written, so `base`'s value does not show through.
A value is compared exactly, and NULL repeats NULL.
A collection is exported over nothing.

With `results_only=True`, `to_datarecord` presents only what the solve produced.
It is a thin layer written as a child of `base`, the record the model was built from.
`base` is required with `results_only`.

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
Only a row at the scenarios, buses, entities and connections the network holds is restated, so a re-solve of a [selection](#selecting-before-a-build) leaves every other label's results to `base`.
A result over no `scenario`, such as `p_nom_opt`, is one value across every scenario, so a re-solve of some scenarios writes it for all of them.
A series is written at the network's snapshots, and `base`'s values stand at the others.
Since `snapshot` is outside `partial`, the layer restates such a series whole, writing `base`'s values at the snapshots it did not solve again.
Over a multi-period `base`, a result over no `snapshot`, such as `p_nom_opt`, is written for the entities PyPSA holds active in the network's periods alone, `build_year <= period < build_year + lifetime`.
So `base`'s results stand at every period, snapshot and entity the network does not hold, and writing one period's solve after another's leaves the earlier one's dispatch and the capacities of the entities idle in the later period in place.
NULL reads as no value, where a `marginal_price` at its default `0.0` would read as a price, and `build` leaves PyPSA its own default there.
A layer removes a label or a tuple by restating its axis without it, a multi-dim's included, and has no way to remove a value, so restating is how a value is replaced.

A PyPSA SubNetwork is a label of the dim `sub_network`, `0`, `1`, … as PyPSA names it.
`sub_network.carrier` is a property that refers to `carrier`, `slack_bus` is an attribute over `sub_network`, and a line's `sub_network` is an attribute over `entity` that holds the plain label.
`sub_network` stays outside `partial`, since a results layer writes every SubNetwork's values.
A results layer restates the `sub_network` axis alone and leaves the entity axis to `base`.
A re-solve that numbers SubNetworks differently therefore leaves no stale label behind and rewrites no input axis.
A solve without SubNetworks over a record with a `sub_network` axis restates the axis empty, its columns typed as the schema declares them, so the record holds none of the earlier SubNetworks.
Over a record without that axis, it writes no `sub_network` axis.
A solve of a network holding fewer buses or entities than `base` writes neither the axis nor `slack_bus` and the `sub_network` of a line, since it numbers its SubNetworks over the selection alone, and leaves the whole network's to `base`.
A full export writes the axis beside `bus` and `carrier`, and `build` adds the SubNetworks from it.
A results layer meets full validation, so it carries the labels its results name.

A solve of several networks writes them as one [collection](#a-collection).

## A collection

`to_datarecord` takes a `pypsa.NetworkCollection` with `collection_dim`, the dim whose labels its one index level holds.
It is required with a collection and refused without one, since nothing in the collection says what its index means.
A collection is written on either path: as a full export over nothing, or as results over `base`.
A record the tool writes builds back, so what a member may hold is decided by the dim:

- `period` is a myopic solve, one network per investment period.
- `scenario` is a set of deterministic networks, written as one stochastic record.
- Any other name, such as `sensitivity` or `weather_year`, is a dim of the record's own, and the record builds back as a `NetworkCollection` indexed by it.

`period` and `scenario` are PyPSA's dims, and a record over them follows PyPSA's declarations, so `build` gives the network PyPSA would hold.
A name the PyPSA schema declares otherwise, such as `bus` or `snapshot`, is refused.
The record's network attributes, `meta` and CRS are the first member's.
Every check runs when `to_datarecord` is called, on the members in memory, so a collection that does not fit is refused before anything is written.
The members' axes and properties are checked before any attribute, so members that cannot be one union are refused before their attributes are compared.
Two values agree where a float is within `numpy.isclose`'s default tolerance and any other value is equal, which decides both what must agree and what differs, and where the record holds one value it is the first member's.

### Over `period`

A myopic solve is written as the one multi-period network holding every member's period, so a myopic and a perfect-foresight solve of one record are read the same way.
A member holds its own period alone.
A series is each member's at its snapshots, and a static value such as `p_nom_opt` or a global constraint's `mu` comes from the members that hold the entity.
Two members holding different static values for one entity are refused, and an entity no member holds is at its default.
The SubNetworks are numbered once for the whole network rather than per member.
A static value counts only from the members holding the entity active in their period, so an entity a member holds idle, such as a generator retired before it, agrees with no one; an entity active in no member takes the value the members holding it agree on.
Over `base`, a member holds only snapshots, buses and entities that `base` holds, and `base`'s results stand at every period, snapshot and entity no member holds ([writing results](#writing-results)).
A `base` with a dim of its own is refused, since it builds to a collection rather than the one network the members split.
A single network over a multi-period `base` may carry any of its investment periods, one built at one period ([selecting before a build](#selecting-before-a-build)) writing at that period, and one carrying fewer than all is written as results.

```python
members = pypsa.NetworkCollection({2030: n_2030, 2040: n_2040})
solved = inputs.child(
    PyPSA.to_datarecord(
        members, ws.con, results_only=True, base=inputs.record, collection_dim="period"
    )
)
```

### Over `scenario` or a dim of its own

Each member is written as a network on its own would be.
An attribute whose values agree across the members is written once, as declared, without the dim.
One whose values differ, a member holding a value another lacks included, is over the dim, its members compared per owner key: its dims in `partial` but the collection's ([`partial`](../layers/tree.md#partial-the-granularity-of-an-override)).
An owner key every member holds with values that agree along every other dim is written once, NULL on the dim, and any other is written whole at each member's label, so one entity's differing series puts that entity alone at every label.
A member holds the components the first one does, and one holding others is refused: the record holds a component at every label, so one a member lacked would read its defaults at that member's label.

The collection's axis holds its labels in order, restated whole on either path.
The members' axes are one union, in the first member's order with each later member's new labels after it, and a member that orders shared labels otherwise is refused ([axis order](../model/dims.md#axis-order)).
A property describes a label whatever the member, so a label's properties agree across the members holding it, such as a bus's `x`.
That holds for the `sub_network` axis too: each member numbers its own SubNetworks, so members whose topologies differ, as a sweep over lines may, number them differently and are refused.
The members declare one schema, so a custom column one member carries and another lacks is refused.

Over `scenario`, the deterministic members are one stochastic record, its scenarios the collection's labels in order, each weighing equally.
That holds for a full export and for results over a `base` without scenarios.
A member must be deterministic, and a `base` holding scenarios is refused.
An attribute may differ across the members only where PyPSA declares it over `scenario`, as it does a series such as `p_max_pu` or `p` and a varying input such as `marginal_cost`.
One it holds once for every scenario, such as `p_nom_opt` or a global constraint's `mu`, is a first-stage value, and members differing in it are refused naming the entity, the attribute and the values, so the record is a stochastic network PyPSA could have solved to.
Members solved apart differ in exactly those, so they are a sweep over a dim of their own.

Over a dim of its own, an attribute whose values differ gains the dim, whatever PyPSA declares it over.
The tool cannot tell in advance which values a sweep varies, since every member is a whole network, so the schema depends on the data: the record is the smallest that holds every member, an attribute carrying the dim only where its values vary along it.
Members solved apart differ in their capacities and duals, which gain the dim, while an input the sweep leaves alone stays as declared.
The dim is added to the schema, typed from the labels, and joins `partial`, as `scenario` is in it, so a later layer patches one member's values without restating the others.
A results layer starts from its `base`'s schema, so a later layer whose values differ where an earlier one's agreed widens an attribute's `dims`, which is [compatible](../model/schema.md#versioning): a row written without the dim broadcasts to every label.
A later layer whose values agree keeps the `dims` it found and writes its rows once, NULL on the dim, so they reach every label.
A single network's results over such a `base` are refused, since NULL on the dim they would stand over every member's.

`build` gives a record with a dim of its own a `NetworkCollection` indexed by that dim, its labels in axis order.
Each member is the network a record of the rows at its label would build, a row NULL on the dim reaching every label, and a row at the label standing over it ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).
A record has at most one dim of its own: a `NetworkCollection` has one index, and collections do not nest.
A dim the network's shapes make, such as `country` ([geometry](#geometry)), is no dim of its own: it indexes no member, and a collection is never over it.
A collection over a new dim is refused over a `base` that has another, and `verify` reports each dim of its own of a record declaring more than one as an unsupported dim, so `build` refuses it.
A re-solve over the same dim keeps the record buildable.

## Selecting before a build

`build(record, select={...})` builds from a selection of the record's labels, by dim, as xarray's `sel` takes them.
The selection is the tool's view of the record, not a record of its own: nothing is written, and `verify(record, select=...)` checks the view `build` builds.

`period`, `scenario` and the record's dim of its own squeeze at one label: the dim leaves the network.
Each attribute's rows are those at the label and those NULL on the dim, settled by [the broadcast rule](../model/attributes.md#the-broadcast-rule), as a member of a collection is built.
An attribute over squeezed dims alone, such as a weighting of the dim, goes with it, and so does a property referring to it, such as `snapshot.period`.
A list of labels, one label in a list included, keeps the dim at those labels in axis order: an attribute row at another label is dropped, and one NULL on the dim kept.
`snapshot`, `bus`, `entity` and `carrier` are never squeezed, so one label is a list of one.
Any other dim, such as `role`, `ctype`, `sub_network`, `line_type`, `transformer_type`, `timestep` or a dim shapes made ([geometry](#geometry)), is refused, as is a dim the record does not declare, a label its axis lacks, an empty list and a selection leaving no snapshot.

A snapshot is selected by its timestamp: `snapshot.timestep` where declared, the label otherwise ([snapshots and weightings](#snapshots-and-weightings)).
Over several periods the label is a position, so a timestamp names its snapshot in every period holding it.
One key is matched as pandas' `.loc` matches it: a timestamp exactly, and a partial date string, such as `"2013-02"`, naming every label in its span and so keeping the dim, as xarray's `sel` does.
A key in a list is matched exactly, read as `pd.Timestamp` reads it on a datetime axis.

A slice, `slice(start, stop)`, selects along an ordered axis: `snapshot` by timestamp, `period`, and a dim of the record's own whose labels are numbers or datetimes.
It is pandas' label slice: both ends inclusive, `None` an open end, and a partial date string covering its span, so `slice("2013-02", "2013-03")` is February and March.
A slice keeps the dim at the labels it covers, even one, and never squeezes it.
A slice with a step, one along any other dim, such as `bus`, `entity`, `carrier` or `scenario`, or an axis of text, one whose end the axis cannot compare and one covering no label are refused.

| selected                               | one label                                                                   | a list or a slice                                                                         |
| -------------------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `period`                               | a single-period network over the period's snapshots, of its active entities | a multi-period network over the periods' snapshots, of the entities active in one of them |
| `scenario`                             | a deterministic network of the scenario's values                            | a stochastic network, its weights renormalised to 1                                       |
| a dim of its own                       | a `Network`                                                                 | a `NetworkCollection` over the labels                                                     |
| `snapshot`, `bus`, `entity`, `carrier` | a list of one                                                               | the dim at those labels                                                                   |

A label whose property refers to a selected dim stays only where its value is selected: a snapshot whose `snapshot.period` is a selected period, a bus, entity or SubNetwork whose `carrier`, its default applied, is a selected carrier.
A NULL property names no label, so it refers to none the selection drops and stays: PyPSA's `""` carrier is NULL ([buses and carriers are dims](#buses-and-carriers-are-dims)).
A period none of whose snapshots is selected goes, since PyPSA holds no period without one.
An entity attached to a dropped bus goes, with its connections, so no component reaches the network with a dangling port.
Selecting `entity` keeps those entities and their connections, every bus left in place.
A shape goes with the label it outlines, and a dim shapes made keeps every label, since `select` takes none of its labels, though a property such as `bus.country` refers to it.

One period is a single-period network indexed by the period's timesteps, `snapshot.timestep` where declared and the label otherwise ([snapshots and weightings](#snapshots-and-weightings)), without the period weightings.
It holds the entities PyPSA holds active in the period, `build_year <= period < build_year + lifetime`, each value resolved with its declared default and PyPSA's, `0` and infinity, where none is declared.
An entity whose value of either varies over a dim the selection keeps, such as a collection's dim of its own, is refused.
A list or a slice of periods holds the entities active in at least one of them, so an entity idle in every kept period goes.
The period one label squeezes out is recorded in the network's `meta`, so its results write back at that period, and a full export leaves the record out.
A `GlobalConstraint` whose `investment_period`, its declared default applied, names a period the selection drops goes, and one NULL on it, applying in every period, stays.
One period drops `investment_period` too, since a single-period network has no period to name, and its value varying over a dim the selection keeps is refused, as `build_year` is.
One scenario drops `scenario_weight`, and a list renormalises the kept scenarios' weights, a scenario no row reaches taking its equal share of the whole axis first, since PyPSA takes weights summing to 1; kept weights summing to 0 are refused.

A network built at selected labels writes its results back over the whole record at those labels ([writing results](#writing-results)), a snapshot at `base`'s label of its period and timestep, and one built at one label of `period` at that period.
One built at one label of `scenario` or of a dim of the record's own is refused there, since its results, NULL on the squeezed dim, would reach every label.
A scenario is kept in a list of one instead, and a dim of the record's own is written back as the whole collection.

Several dims combine, so `select={"period": 2030, "scenario": "high", "carrier": ["AC", "wind"]}` builds one deterministic, single-period network of the AC buses and the wind generators at them.

## Geometry

A shape describes the label it outlines, a bus's onshore region or a country's offshore waters, so it lives on that label rather than as a label of its own.
PyPSA's `Shape` is no entity type and makes no `shape` dim.

A shape's `component` names its dim: a PyPSA type names that type's dim, `Bus` names `bus` and an entity type such as `Generator` names `entity`.
Any other `component`, such as pypsa-eur's `country`, is a dim of its own with that name, its labels the shapes' `idx` in the network's order.
`type` names the property, `shape_{type}`, and `idx` the label: a bus's onshore region is `bus.shape_onshore`, and Germany's offshore waters are `country.shape_offshore` at label `DE`.
The `shape_` prefix is how the tool, and any reader, recognises a geometry.
A geometry is text: its values are [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry), declared `str`.

`to_datarecord` refuses a shape without a `component`, an `idx` or a `type`, two shapes sharing `(component, idx, type)`, an `idx` that is not a label its PyPSA type writes (a standard type or a carrier `n.carriers` lacks is none), and a `component` that is no PyPSA type but names a dim the PyPSA schema declares.
A dim that shapes make stays outside `partial`, is written by a full export alone, and is no [dim of the record's own](#over-scenario-or-a-dim-of-its-own).
A results layer writes no shape.

A network has one coordinate reference system, so the CRS is `crs` in the schema's [`meta`](../model/schema.md), as the CRS's authority string (`EPSG:3035`) where it has one.
The tool does not write PyPSA's `srid` network attribute, that CRS's EPSG code.

`build` rebuilds `n.shapes` from every non-NULL `shape_{type}` property of every dim, named `"{component} {idx} {type}"`, such as `"Bus DE onshore"` beside `"country DE onshore"`, unique since `(component, idx, type)` is.
Its `component` is the dim's PyPSA type, an entity's `ctype`, or the name of a dim shapes made.
`build` sets the network's CRS from `crs` before it adds any component, and parses each geometry back into a GeoSeries in that CRS.
A record without `crs` builds in PyPSA's default, `EPSG:4326`.
