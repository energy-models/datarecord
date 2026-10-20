<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Consuming a record

A **tool** is the framework-specific translation target built from a record.
The call runs from the tool inward:

```python
class Tool(Protocol):
    name: str
    schema: Schema  # attribute mapping

    def requires(self, record: Record) -> Requirements: ...
    def verify(self, record: Record) -> Requirements: ...  # falsy when usable
    def build(self, record: Record) -> Any: ...
    def to_datarecord(
        self,
        model: Any,
        con: DuckDBPyConnection,
        *,
        results_only: bool = False,
        base: Record | None = None,
    ) -> Record: ...
```

`to_datarecord` is the inverse of `build`. It takes the connection its frames are planned on, required; in practice `ws.con` of the [workspace](layers.md#the-store) that writes them.
A tool is handed a connection, not a workspace or a store: it plans relations and has no business with directories, locations or the tree.
A frame planned on another connection is [rejected at the boundary](read-path.md#a-relation-from-another-connection-is-rejected), so a tool that picked its own would fail on the first write.
`build` takes none: it reads `record.con`.

```python
root = store.root(PyPSA.to_datarecord(n, ws.con))
```

It presents [`Frames`](record.md#frames) keyed by attribute, each frame in [the long schema](format.md#the-long-schema), so they go straight to [a write](writing.md).

A framework holds its values per entity type, so reaching this shape means concatenating each attribute's types into one frame.
That is free: the frames are lazy, so the union is a plan rather than a copy, and nothing materialises until a caller collects.
The concatenation needs no category column to distinguish the arms, since [`entity` is unique across them](format.md#entity-is-unique-across-types) — which is what makes the union a plain one rather than a tagged one.

A framework's per-port results are [stored on connections](#results-on-connections), like its per-port inputs, and its results are written as a [thin layer](#writing-results).

Lazy is what the protocol asks for rather than what any implementation must do.
A tool reshaping a solved model's in-memory containers has nothing to defer and wraps its eager frames with `.lazy()`; one that could fetch an attribute from a solver on demand is free to, and a caller wanting three of forty then pays for three.
That is [the `Frames` argument](record.md#frames), on the write side.

A record is the input to a translation, not the owner of one, so there is no registry and no name dispatch: a tool is a module-level singleton reached by importing it, `build` returns the framework's own type, and nothing in the record layer imports a tool.

`build` takes a [`Record`](record.md) rather than a record, so a tool builds from a directory as readily as from an overlay and has no reason to know layering exists.

A tool's `verify` catches what the record layer cannot: an entity type the framework has no registry entry for — the record checks a category against its `Enum`, not against the framework — a connection `role` it cannot place, a `partial` set that breaks the framework's constant-versus-varying split.
It is also where a framework scoping names **per type** meets [a record scoping them record-wide](format.md#entity-is-unique-across-types).
PyPSA permits a `Generator` and a `Load` both called `north`, so `to_datarecord` reports such a network as unbuildable rather than writing a record whose two entities share one key.
Collisions are within a dim: a bus is a label of `bus`, not an entity, so a `Bus` and a `Load` called `north` — every clustered pypsa-eur network — are no collision.
Reported rather than repaired: renaming to `Generator:north` would hand back a network whose components the framework can no longer find by their own names, and the record layer does not own a framework's vocabulary.
PyPSA is itself moving to record-wide unique names, so this is a constraint that resolves rather than one to design around.
SubNetworks are the exception: PyPSA names them `0`, `1`, … on every topology pass, as it numbers Lines, so there is no caller to report to. The record keys them `0 SubNetwork`, …, as a caller suffixes a clashing `Load`, the `sub_network` results pointing at them likewise, and `build` takes the suffix off, so the framework finds them by their own names again.
It is also where bus-keyed connections are collapsed back to a framework's positional encoding, ordered by [`order_key`](read-path.md#owner-map), and where a curve is either translated or reported unbuildable.
A per-connection input follows its connection's position: `efficiency` on a Link's `bus2` connection is PyPSA's `efficiency2`, static or per snapshot.
The PyPSA schema declares `partial={"entity", "bus", "scenario", "connection"}`, so a layer patches one link's `efficiency` series, or one bus's `v_nom`, without restating every other's ([`partial`](schema.md#partial-the-granularity-of-an-override)); `entity` is there also because `ctype` [categorizes it](schema.md#categories-an-enum-property-of-a-partial-dim).
`carrier` is not: a layer restates every carrier's `co2_emissions`, a few dozen rows.
`remove("bus", …)` is [the cascade every dim has](layers.md#deletion), and an entity it leaves attached to no bus is `verify`'s to report, as its type's missing `bus`; `remove("carrier", …)` is refused while a stored `carrier` names the label.
A Process's `rate0`, `rate1`, `rate2`, … are likewise the one input `rate`, numbered from `0` because PyPSA registers `rate0` where it leaves port `1`'s `efficiency` unsuffixed.
Its ports default differently, `-1` at the input end and `1` at the others, so `rate` declares no default and a port with no row keeps PyPSA's own.
Which stems are per-port is read off PyPSA's registry, a name ending in a port index plus a Link's unsuffixed `efficiency`, `delay` and `cyclic_delay`.
Every per-port input, `delay` and `cyclic_delay` included, is long rows of `attributes/` over `connection`: a number is an attribute whatever its `dims`.

**Properties and attributes are read off the registry.** The properties of `entity` are exactly `ctype`, `carrier`, `sign` and `type`, and `role` is a property of `connection`; every other input, every number among them, is an attribute, so `p_nom` is long rows of `attributes/p_nom.parquet` ([where a value lives](format.md#where-a-value-lives)). `ctype`'s `Enum` is the types exported as entities. A default that differs per component — `sign`, `p_min_pu`, `p_set` — is a [`default_by`](schema.md#default_by-a-default-per-category) on `entity.ctype`, and `build` applies it when it builds a component's static frame from `members` and the entity-only rows of its attributes.

**Buses and carriers are dims.** `Bus` and `Carrier` are no entity types: a bus is a label of `bus`, a carrier a label of `carrier`, and what PyPSA stores on either is placed by [the properties rule](schema.md#properties), every name PyPSA's:

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
| a component's `carrier`                                                 | `entity.carrier`, `into` `carrier`                   |

So `x` is a line's reactance and `bus.x` a bus's coordinate, each addressed by its own name.
`entity.carrier` and `bus.carrier` are [`into`](schema.md#properties) `carrier`, `bus.carrier` defaulting to `AC`.
`to_datarecord` writes both axes in PyPSA's order, adds every carrier a component or bus names but `n.carriers` does not list, and stores PyPSA's `""` carrier as NULL, naming none; `build` rebuilds `n.buses` and `n.carriers` from the axis, the properties with their defaults and the attribute rows, a series by its [flags](record.md#flags), the bus results among them.

**A custom column clashes only with an attribute.** A static column PyPSA's registry lacks is an attribute over its type's dim, built onto that type alone where the dim is `entity` ([below](#a-value-its-type-does-not-take)). One name being one spec, custom `Bus.country` beside custom `Generator.country` would be `country` with two `dims`, and `Carrier.efficiency` meets the per-port `efficiency`: `to_datarecord` reports either via `Requirements.columns`. A name a property has does not clash, so a custom `Generator.location` is an attribute beside `bus.location`.
`Line.v_nom` and `ShuntImpedance.v_nom`, which PyPSA's `calculate_dependent_values` copies from the bus, are no custom column: they are not written, and `build` gets them back as PyPSA recomputes them.

**A multi-period network's snapshots are one flat axis.** PyPSA's `(period, timestep)` snapshots are the `snapshot` axis, each with its period as `snapshot.period`, a property [`into`](schema.md#properties) `period`; `period` is an axis of its own beside it.
A snapshot is labelled by its timestep where no timestep repeats across periods, and otherwise by its position, `0` to `n-1` across every period in the network's order, its timestep then the property `snapshot.timestep`; the `pd.MultiIndex.from_product` of `set_investment_periods` repeats every timestep, so its snapshots are positions.
Every attribute over `snapshot` has the dims it has in a single-period network, and a single-period network's schema declares no property of `snapshot`.
`build` rebuilds the `(period, timestep)` index from the axis order, `snapshot.period` and the label or `snapshot.timestep`.
A period is selected like any property of a dim an attribute reaches, `set("p_max_pu", 0.5, entity="wind", period=2030)` staging the snapshots of 2030 alone ([`set`](working-record.md#set)).

**A weighting is an attribute over its axis.** PyPSA's weighting frames become `scenario_weight` over `scenario`, `snapshot_weighting_objective`, `snapshot_weighting_stores` and `snapshot_weighting_generators` over `snapshot`, and `period_weighting_objective` and `period_weighting_years` over `period`, written for each axis the network holds; a multi-period network's snapshot weightings are over its flat `snapshot` like any other.
`build` sets them back, a label no row reaches keeping PyPSA's own default, an equal share for a scenario.

The tool's own `Schema` reconciles vocabularies: per entity type, which record attribute a tool's attribute is renamed from, or which several it is computed from.
Since [the schema](schema.md) makes a record's attribute names _declared_ rather than conventional, this maps one declared vocabulary to another.

Frames are built and handed over one entity type at a time, so peak memory is one type's frames rather than the whole model.

## A value its type does not take

A record carries every attribute on every label it addresses ([the schema](schema.md#an-attribute-reaches-every-label-it-addresses)), so `set("p_max_pu", 0.9)` stores a value a load reaches. Which attributes and `entity` properties a type takes is PyPSA's registry, and the tool keeps it: `build` hands a type only the names it takes, so the load's value is left out, never assigned.

A type takes its registry inputs and results and the custom columns its network was exported with, which `to_datarecord` records per type in the schema's `meta`; a name no type claims — an attribute a record adds — reaches every type. So a custom `Generator.e_nom` is built onto generators, though the registry gives `e_nom` to stores.

`verify` reports each `(type, attribute)` a stored value is left out for as `Requirements.ignored`; an `entity` property left out goes unreported, since its column exists on every member whatever its type. The report is like a type it does not know — but a report, not a failure: `ignored` leaves the result falsy, and `build` does not raise for it. A value scoped to the types taking it is written per label, `set("p_max_pu", 0.9, ctype="Generator")`.

## Results on connections

A per-port result is stored exactly like a per-port input: one attribute over the [`connection` group](record.md#connections), one row per `(entity, bus)`, carried as the one `connection` struct column — `to_datarecord` packs it and `build` unpacks it back to `entity` and `bus` where it places ports.
PyPSA's `p0`, `p1`, `p2`, … on a Line, Transformer, Link or Process and the one-port `p` on a Generator, Load, Store, StorageUnit or ShuntImpedance are all the result `p`, each row on the connection its port attaches to and keeping PyPSA's sign for that port; `q0`, `q1` and the one-port `q` are `q`.
So a third port needs no declaration of its own: a Link's `p2` is `p` on its `bus2` connection, which the schema built from the bare registry already declares.

Two results do not follow that rule, and the tool names both rather than inferring them:

- **`Bus.p`, `Bus.q`, `Bus.control`, `Bus.generator` and `Bus.sub_network` are not written.** `p` and `q` are the signed sum of the connection `p`/`q` at that bus, so derivable, and a bus-level `p` would collide with the connection-level one; the other three follow from the connections and the topology.
- **`Link.p` and `Process.p` are `p_internal`.** PyPSA's "internal power" is at no bus, so it is an entity-level result, renamed because one name holds one spec record-wide.

The schema's attributes and the frames `to_datarecord` presents are read off one rule, so every result is written under a name the schema declares.
`build` turns `p` back into PyPSA's per-port columns as it does a per-port input, and `p_internal` into `Link.p`.

## Writing results

A framework's outputs are [attributes](schema.md#a-result-is-an-attribute). `to_datarecord` writes them with every other attribute, so a solved network round-trips its results through `attributes/`, and `build` copies every attribute the record holds into the network, a solved bus's `marginal_price` included.
Only the tool knows which attributes are outputs — PyPSA's registry status "Output" — and it reads that on import alone: it decides what `results_only` presents, never what `build` or a full export drops.
A full export leaves out a result still at its default, so an unsolved network writes none.

With `results_only=True`, `to_datarecord` presents only what the solve produced, a thin layer written as a child of `base`, the record the model was built from. `base` is required with `results_only` and refused without it.

```python
inputs = rev.child(w.edits)  # edited inputs are committed first
n = PyPSA.build(inputs.record)
n.optimize()
solved = inputs.child(
    PyPSA.to_datarecord(n, ws.con, results_only=True, base=inputs.record)
)
```

Nothing is staged through a `WorkingRecord`. The layer holds the result attributes, a value at its default included so a re-solve stands over its parent's, and the **intermediate results** they need: a solve derives structure as well as values.
A result row `base` holds that the solve does not write again — `mu_*` duals under `assign_all_duals=False` — is restated at its default, which is what the network holds there, so no stale value is inherited. The layer format has no tombstone for a value, only for a label or a tuple, so restating is how it is replaced. `base`'s inputs are never written.
A PyPSA SubNetwork is an `entity` with `ctype="SubNetwork"` and a line's `sub_network` an attribute naming one, declared like any other name.
The model does not say what `base` held, so the tool reads `base`'s SubNetwork entities and tombstones those it does not write again: a re-solve that numbers SubNetworks differently leaves no stale label behind.
A results layer meets full validation, so it carries the labels its results name.

## Geometry

A geometry is text.
PyPSA's `Shape.geometry`, the column its registry types `geometry`, is an attribute like `idx` and `component` beside it, its values [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) in `attributes/geometry.parquet`, declared `String`.
The coordinate reference system is one per network rather than one per value, so it is not data but `crs` in the schema's [`meta`](schema.md), the CRS's authority string (`EPSG:3035`) where it has one.
PyPSA's `srid` network attribute, that CRS's EPSG code, is not written beside it.

`build` sets the network's CRS from `crs` before adding any component, and parses each geometry back into a GeoSeries in it; a record without `crs` builds in PyPSA's default, `EPSG:4326`.
