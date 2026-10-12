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
    def to_datarecord(self, model: Any) -> Record: ...  # inverse of build
    def results(self, model: Any) -> Frames: ...
```

`results` returns [`Frames`](record.md#frames) — the same type `Record.outputs` presents, keyed by attribute, each frame in [the long schema](format.md#the-long-schema).
So a tool's results go straight to [`write_record`](writing.md), or one at a time to [`set(attr, frame, kind="outputs")`](working-record.md#results-through-kindoutputs) with no key to unpack.

A framework holds its results per component type, so reaching this shape means concatenating each attribute's types into one frame.
That is free: the frames are lazy, so the union is a plan rather than a copy, and nothing materialises until a caller collects.
The concatenation needs no `entity_type` column to distinguish the arms, since [`entity` is unique across them](format.md#entity-is-unique-across-types) — which is what makes the union a plain one rather than a tagged one.

Lazy is what the protocol asks for rather than what any implementation must do.
A tool reshaping a solved model's in-memory containers has nothing to defer and wraps its eager frames with `.lazy()`; one that could fetch a result attribute from a solver on demand is free to, and a caller wanting three of forty then pays for three.
That is [the `Frames` argument](record.md#frames), on the write side.

A record is the input to a translation, not the owner of one, so there is no registry and no name dispatch: a tool is a module-level singleton reached by importing it, `build` returns the framework's own type, and nothing in the record layer imports a tool.

`build` takes a [`Record`](record.md) rather than a record, so a tool builds from a directory as readily as from an overlay and has no reason to know layering exists.

A tool's `verify` catches what the record layer cannot: a component type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks the framework's constant-versus-varying split.
It is also where a framework scoping names **per type** meets [a record scoping them record-wide](format.md#entity-is-unique-across-types).
PyPSA permits a `Generator` and a `Load` both called `north`, so `to_datarecord` reports such a network as unbuildable rather than writing a record whose two components share one key.
Reported rather than repaired: renaming to `Generator:north` would hand back a network whose components the framework can no longer find by their own names, and the record layer does not own a framework's vocabulary.
PyPSA is itself moving to record-wide unique names, so this is a constraint that resolves rather than one to design around.
It is also where bus-keyed connections are collapsed back to a framework's positional encoding, ordered by [`order_key`](read-path.md#owner-map), and where a curve is either translated or reported unbuildable.

The tool's own `Schema` reconciles vocabularies: per component type, which record attribute a tool's attribute is renamed from, or which several it is computed from.
Since [the schema](schema.md) makes a record's attribute names _declared_ rather than conventional, this maps one declared vocabulary to another.

Frames are built and handed over one component type at a time, so peak memory is one type's frames rather than the whole model.

## The PyPSA tool

`PyPSA.to_datarecord(n)` writes a network in the record's terms, and `PyPSA.build(record)` inverts it.
It follows the direction of [PyPSA#1474](https://github.com/PyPSA/PyPSA/issues/1474): components are physical assets, and what is not physical — buses, carriers, technology groups — is an axis that components reference through a group.

| PyPSA                              | record                                                                     |
| ---------------------------------- | -------------------------------------------------------------------------- |
| `Generator`, `Link`, `Line`, …     | entities, rows of `dims/entity.parquet`; the type is `entity_group`        |
| `Bus`                              | `dims/bus.parquet`: every static column, custom ones (`busmap_*`) included |
| `Carrier`                          | `dims/carrier.parquet`                                                     |
| a component's or bus's `carrier`   | the groups `entity_carrier` and `bus_carrier`, `into` `carrier`            |
| `technology_group`                 | the group `entity_technology_group`, `into` `technology_group`             |
| `bus0`, `bus1`, …                  | the `connection` group, with `role`; `efficiency2`, … over it              |
| bus series (`v_mag_pu_set`)        | `inputs/<attr>.parquet`, keyed by `bus`                                    |
| `snapshot_weightings.<w>`          | `snapshot_weighting_<w>`, columns of `dims/snapshot.parquet`               |
| `investment_period_weightings.<w>` | `period_weighting_<w>` and `period_year`, columns of `dims/period.parquet` |
| `scenario_weightings.weight`       | `scenario_weight`, a column of `dims/scenario.parquet`                     |
| `Shape` bus regions                | `region_onshore`, `region_offshore` on `dims/bus.parquet`                  |
| `GlobalConstraint`                 | not carried                                                                |

**The component type is an attribute, not an axis.** The record declares no [entity-type axis](schema.md#entity_type-the-axis-of-kinds), no entity-type group and no traits. The PyPSA type of each entity is `entity_group`, a `str` attribute over `entity`. It is a column of `dims/entity.parquet`, so it is not a dim of the record or of a mathspec spec.
Without the type axis, the constant columns of all types are columns of `dims/entity.parquet` ([where a value lives](format.md#where-a-value-lives)). A column is NULL on the rows of a type that does not have it.
`build` reads the type of each entity from `entity_group`. For each type, it keeps the columns that PyPSA's registry (`c.defaults`) declares for that type, and the custom columns in which a member of the type has a value.
`verify` and `build` read the flags of each type from the rows of its members, because the record's own `flags` cannot scope by an attribute.
`verify` reports an `entity_group` that PyPSA does not know. An entity with no `entity_group` is an error.

**A functional group is named `<key>_<into>`**, never after the dim it maps into: a mathspec spec binds to a record by name, and a relation there may not share a dimension's name.

**The attribute namespace is flat**, so an axis column whose name another PyPSA type's registry or a dim or group already claims is written `<axis>_<name>`: `Bus.x` is `bus_x` beside `Line.x`, and `Bus.p` is the result `bus_p`. An unclaimed name such as `y` or `busmap_5` is unchanged. The rule reads the registry and the structural names only — never what else the network holds — so one column has one record name in every network, and a fixed mathspec spec binds to every record a network writes.
That leaves custom columns, which no registry lists: an axis column whose record name is also a custom entity column or another axis's column has no rule to tell the two apart, so `to_datarecord` raises `UnsupportedRecordError` naming the column and both owners rather than renaming one silently.
`schema.meta["names"]` records each axis's record-to-PyPSA mapping, which is what `build` inverts.

**Multi-period.** On a network with investment periods, `snapshot` is [`within`](schema.md#within-an-axis-inside-an-axis) `period`, so the snapshot axis is keyed `(period, snapshot)` — PyPSA's `(period, timestep)` — and every snapshot-varying attribute is addressed by `period` too.

### Technology groups

A component's `technology_group` column becomes the group `entity_technology_group`, written only where some type has the column. Its axis, `dims/technology_group.parquet`, carries what is a function of the group: `resource_class` and `nice_name` move there off the members, and `technology_group_carrier` maps each group to its one carrier.
`to_datarecord` fails where a group's members have other than exactly one carrier or more than one value of a moved column, and where a component outside every group carries one. `build` puts the columns back on the members.

### Geometry

Geometry is WKT text, in `schema.meta["crs"]` (`n.crs`, EPSG:4326 by default). A shapely object in any static column — a GeoSeries above all — is written as WKT; a WKT string is written as is.

### Shapes

`Shape` is not an entity. A row with `component == "Bus"` and a `type` of `onshore` or `offshore` is a bus region, a WKT column of `dims/bus.parquet`. A row with no component and a `type` of `country` or `offshore` — pypsa-eur's context shapes — is dropped: it describes no element of the network, and nothing downstream of the record reads it. Any other row, such as a bus's `admin` region, fails the write rather than being lost. `build` restores bus regions as `Shape` rows numbered in bus order, so a shape's own name does not round-trip.

### Global constraints

`GlobalConstraint` is not carried. A global constraint is math, not data — a CO2 cap is a constraint over the emissions of the components it names — so it lives in a mathspec fragment beside the shipped spec.

### The mathspec spec

`datarecord/tools/pypsa.mathspec.yaml` (`datarecord.tools.pypsa.MATHSPEC`) states the linear optimal power flow over the record's own names: dims `snapshot`, `period`, `scenario`, `entity`, `bus`, `carrier`, `technology_group`; relations as the groups are named; PyPSA's attribute names per type (`p_nom_min`, `s_nom_min`, `e_nom_min`), unified by named expressions rather than renamed.
The PyPSA type is the `str` parameter `entity_group`, not a dim or a relation. A mask compares it with a value: `generator` is `entity_group == 'Generator'`.
The bus balance reads the `connection` group's `role` and the per-connection `efficiency`.

mathspec has no nested dimension, so `snapshot` is flat there and its period is the relation `snapshot_period`, which the record's `within` supplies as the nested axis file's key.
A dimension label is never a value to mathspec, so an asset's activity `build_year <= period < build_year + lifetime` compares against `period_year`, a column the tool writes on the period axis, rather than a precomputed `active[entity, period]`.

`PyPSA.check_spec(spec, record)` fails where the spec names a dimension, relation or parameter the record does not carry, a nested dim offering `<dim>_<parent>` as a relation.
Two kinds of name are not missing. The first is an attribute that only types the record holds no member of register: the record reads the types it holds from its `entity_group` values. The second is the technology-group axis and its groups, where the record has no `technology_group` dim.
