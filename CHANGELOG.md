<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- **Buses and carriers are dims** in the PyPSA tool, carried by two core
  changes:
  - **A property's name is its subject's**, addressed as `subject.name`:
    `set("bus.x", ...)`, a trait's `on` and bundled properties, and
    `default_by` keys (`{"entity.ctype": {...}}`). One name, one spec
    now holds across attributes and results only, so a property may share
    its name with an attribute, a dim or another subject's property; one
    named like its subject's key (the dim, a group's struct column or
    `into`), or a group's property named like one of its
    coordinates, is refused. A plain name is an attribute or result, one
    only properties are called refused naming their dotted forms.
    `Schema.resolve(name)` replaces `subject_of` and `property_spec`, and
    `Schema.column_type` takes the property's subject.
    `add(dim, labels, **properties)`, `flags` and `properties[subject]` keep
    plain names. A bare selector keyword of `set` names a coordinate of the
    attribute first, else any property of a dim the attribute reaches,
    categorizing or not; one several reached dims declare is refused. Every manifest
    declaring a trait or `default_by` on a property is rewritten.
  - **A property may be `into` a dim** (`PropertySpec.into`): its values
    are that dim's labels and its dtype the dim's, declared on any subject.
    `set` and `add` refuse a value naming no label of the dim, NULL naming
    none; the writer refuses a layer storing one it removed, and
    `remove(dim, labels)` is refused while such a property still names one.
    It never categorizes. A group over one coordinate `into` a dim is now
    refused at load: declare the property `into` the dim instead.
  - **The PyPSA tool**: `Bus` and `Carrier` are no entity types, a bus
    being a label of `bus` and a carrier of a new `carrier` dim.
    `bus.carrier`, `type`, `x`, `y`, `unit` and `location`, and
    `carrier.color` and `nice_name` are properties; `v_nom`, `v_mag_pu_*`,
    `co2_emissions`, `max_growth`, `max_relative_growth` and custom bus or
    carrier columns attributes over their dim; `marginal_price`,
    `v_mag_pu` and `v_ang` results over `bus`, which `build` sets back.
    `Bus.p`, `q`, `control`, `generator` and `sub_network` are not written,
    nor the `Line.v_nom` and `ShuntImpedance.v_nom` PyPSA copies from the
    bus, so a solved network exports. `entity.carrier` and `bus.carrier`
    are `into` `carrier`: `to_datarecord` adds every carrier named but not
    listed and stores `""` as NULL. `entity_type` is `entity.ctype`,
    and `bus` joins `partial`. A bus and a load may share a name; a custom
    column clashes only with an attribute over another dim. `verify`
    reports an entity attached to no bus. Every PyPSA record is
    re-exported.
- **Properties**, metadata apart from data: a new `properties` section of
  the schema (`PropertySpec`) declares per dim or group the values describing
  one label, stored as columns of `dims/<dim>.parquet` or
  `groups/<group>.parquet`. An `Enum` property of a `partial` dim
  categorizes it; traits key on categories, and `default_by` gives a default
  per category (`Schema.categorizing`, `attributes_for(dim, **categories)`,
  `properties_for`, `default_for`). This replaces the `entity_type` dim, its
  group over `entity` and the `dims/entity_type/<Type>.parquet` member files.
  `entity` is an ordinary dim: it broadcasts, and must be in `partial` to be
  patched per label or categorized; a NULL on a categorized dim reaches only
  the labels whose categories carry the attribute. Every attribute,
  whatever its `dims`, is stored in `inputs/`. On reading, `dims[dim]` holds
  key columns only, `properties[subject]` the stored property values, and
  `attributes` every attribute; `entity_types` is gone and `flags` takes
  categories (`flags(ctype="Generator")`). `WorkingRecord` edits any
  dim: `add(dim, labels, **properties)`, `remove(dim, labels)` (cascading to
  group tuples), and `set(name, value, **coords)` writing attributes and
  properties alike, a property keyword selecting labels at call
  time; `set` loses `indexed_by`. The PyPSA tool declares `ctype`,
  `carrier`, `sign` and `type` as properties of `entity` and `role` of
  `connection`, with `partial={"entity", "scenario", "connection"}`. Every
  manifest and record is rewritten.
- An attribute over a group carries it as one struct column named for the
  group, a field per key coordinate (`connection STRUCT(entity, bus)`), in
  place of the coordinates flat. This is an on-disk
  format change: `inputs/` and `outputs/` files over a group are rewritten.
  The dims a group draws on now broadcast, so a per-bus value with
  `bus = NULL` holds for every bus, and patching one bus at a time needs
  `bus` in `partial`. `WorkingRecord.set` still takes `bus=`; a long frame
  carries the struct, its coordinates flat being rejected. Naming a group
  beside a dim its key is named for (`dims={"connection", "bus"}`) is
  rejected.
- A group is a broadcast coordinate like a dim, its axis the resolved group:
  a NULL struct means every tuple of the group, and a struct with some
  fields NULL is refused by `set`, `add_group` and the writer. The owner
  map's key is `entity` plus the `partial` dims and groups; `partial` may now
  name a group, and a group outside it is owned whole, so a layer patching
  one connection's value restates every connection's. The PyPSA tool
  declares `partial={"scenario", "connection"}`, keeping per-connection
  ownership. `Schema.membership_keys` is gone, and `Schema.group_columns` is
  every group but the entity-type group. Materialised owner maps are
  rebuilt.
- A group file is keyed by one struct column named for the group, as an
  axis file is by its dim: `groups/connection.parquet` is
  `connection STRUCT(entity, bus) | role | deleted`, an `into` label a plain
  column beside it. This is an on-disk format change. `Record.groups[...]`
  hands back that shape; `add_group` takes the struct column and rejects the
  coordinates flat; `remove_group` still takes a tuple per key. The PyPSA
  tool no longer declares `bus` as an attribute: a port's bus is the
  connection tuple. `Schema.group_coordinates` and `Group.coordinates` are
  gone.
- `WorkingRecord.set(..., entity=...)` fills only a coordinate named
  `entity`. An attribute over a group with no such field, a contract
  `(from, to)`, takes every party as a keyword and refuses `entity=`; it
  used to stage one duplicate row per entity of the type. `add` refuses a
  frame carrying a group's columns without its full key, which used to
  stage a half-keyed row. Likewise `set` on a group attribute without one
  of its key coordinates (`set("efficiency", 0.9, entity=["dc"])`, no
  `bus=`) is refused; it used to store a value no read reached.
- A group may not share a dim's name, the entity-type group excepted: a
  group is carried as a column named for it. Rename a functional group for
  the relation, `bus_country` into `country`.
- A `Workspace` holds the DuckDB connection and a `Store` one location;
  `connect`, `layer_dir`, `DATARECORD_BASE_URI`, `Record.at` and the process
  default connection are gone. A store's revisions live beside its layers as
  `revisions/<id>.json`, so a `Revision` serialises as its location and reads
  its ancestry back in a new process. `ws.con` is the calling thread's cursor,
  and a relation from another connection or thread is rejected at the write
  and edit boundary.
- Writing is `store.root(source)`, `rev.child(source)` and
  `ws.write(source, uri)`; `write_record`, `Revision.create` and the
  source-less `Revision.child()` are gone. A `WorkingRecord` no longer
  commits: `rev.child(w.edits)` writes its edits as a child,
  `ws.write(w, uri)` the flattened record, and `NewChild`, `Directory` and
  `commit` are gone. A tree write writes its revision file last, so a failed write leaves
  no revision and never widens the manifest.
- `Tool.to_datarecord(model, con)` and `Tool.results(model, con)` take the
  connection their frames are planned on, so `PyPSA.results(n, w.con)` sets
  straight onto `w` rather than failing on a relation from DuckDB's default
  connection; `on_connection` is gone.
- `Resolver.group_frame` and `Resolver.entity_type` return an empty relation,
  rather than `None`, where every row written is tombstoned: telling the two
  apart ran the whole fold a second time on every read. `None` still means no
  layer wrote a row.
- A schema declaring a dim, group, group coordinate, attribute or result
  whose name holds `"` or `.`, starts with `_`, or is a column datarecord
  writes itself (`value`, `deleted`, `layer_uuid`, ... ignoring case, and
  `entity_type` for an attribute), or that differs from another declared
  name only in case, is refused at load, naming each offender.
  Such a name used to collide silently: a dim named `deleted` read every
  label as a tombstone. A manifest declaring one no longer loads; rename it.
  Based on #26 by @FBumann.
- The PyPSA tool stores per-port results on connections, like per-port
  inputs: `p0`, `p1`, `p2`, ... and a one-port type's `p` are one result `p`
  over the `connection` group, a row per `(entity, bus)` keeping each port's
  sign, and likewise `q`. `Bus.p` and `Bus.q`, the sum of those rows at a
  bus, are no longer written, and `Link.p`/`Process.p` are `p_internal`. A
  three-port Link's `p2` is now a declared result.
- The PyPSA tool stores a Process's `rate0`, `rate1`, `rate2`, ... as one
  input `rate` over the `connection` group, like `efficiency`, and `build`
  puts each row back in its port's column, static or per snapshot. `rate`
  declares no default, its ports defaulting to `-1` and `1`.

- Removing an entity removes the group tuples naming it: `WorkingRecord.remove`
  tombstones every tuple with a field over `entity` naming it, in the same
  layer, and a value over a group goes with its tuple rather than with the
  entity. An `add` of the name revives the entity alone. A layer adding a
  tuple that names a dead label, or removing a label a tuple still names, is
  refused at write. A value over a dim or group outside `partial` naming a removed label
  is dropped at read, a removed `bus` label's per-bus values included. A
  materialised node caches the dead labels in `resolved/dead/`; one
  materialised before reads as having none.
- **A manifest with an unknown key fails to load** (breaking): `Schema` and
  every model it nests refuse a key they do not declare with pydantic's
  `ValidationError`, so a misspelt or removed field is no longer ignored.
- **A standalone record's id is `Schema.id`**, an optional field its
  `manifest.json` carries as before and the record's `schema.id` answers; a
  tree's manifest still carries none.

### Added

- A store or standalone directory on `s3://`, through boto3 behind the
  `datarecord[remote]` extra and configured by the AWS environment alone.
  Revision files are written create-only and the manifest by
  compare-and-swap, so two writers widening one schema never silently lose
  a widening.

### Removed

- **`set` no longer takes an expression** (breaking): compute the new
  values from `attributes[name]` and `set` the frame.
- **A dim is flat** (breaking): `Dimension.within` is gone and every axis is
  keyed by its label alone. A nested dim becomes a property `into` the
  parent dim (`snapshot.period` into `period`), with labels unique across
  the dim. Every earlier manifest carries `within`, if only as `[]`, so none
  loads: drop the key, or re-export the record.
  `add` takes no parent keys, and `set` on a property, `remove`'s refusal and
  group cascade, the writer's live-label check and the label and trait check
  of `set` now cover every dim. `Schema.axis_key` is gone. `PyPSA.results`
  no longer carries an always-NULL `period` column.
- **Traits are gone** (breaking): `Trait`, `Schema.traits`,
  `Schema.attributes_for` and `Schema.properties_for` are removed. An
  attribute reaches every label its `dims` address and a property every
  label of its subject, whatever their categories: a NULL on a categorized
  dim reaches every label, `flags(**categories)` holds every attribute with
  a row reaching a matching label, and `set` no longer refuses a label whose
  category lacked the attribute. Scope a value with a category keyword,
  `set("p_max_pu", 0.9, ctype="Generator")`. The PyPSA tool keeps the
  per-type vocabulary itself: `build` leaves out a value its type does not
  take, reported in the new `Requirements.ignored` without failing
  `verify`, and `to_datarecord` records a network's custom entity columns
  per type in the schema's `meta`. Every record declaring traits is
  re-exported.
- **Results are attributes** (breaking): a layer holds every attribute in
  one `attributes/` directory; `Schema.results`, `Record.outputs`, `Kind`,
  `set(..., kind=...)` and `Tool.results` are gone, and a result inherits
  through layers like any attribute. The PyPSA tool declares its outputs as
  attributes, `build` copies them, and
  `to_datarecord(n, con, results_only=True, base=record)` writes a solve as a
  thin layer replacing `base`'s SubNetworks and restating as NULL the
  results of `base` it does not write again; a NULL series value, input or
  result, leaves the component at its static value on export. Every record
  is re-exported.
- **narwhals is gone** (breaking): `record.dims`, `properties`, `groups` and
  `attributes` hand over bare `DuckDBPyRelation`s, `Frames` and `LazyFrames`
  are `Relations` and `LazyRelations`, and `Record` moves to
  `datarecord.record`. `set`, `add_group` and the writer take a relation or a
  pandas, polars or arrow frame, read by DuckDB's own scanner, and refuse
  timestamps with a time zone, casting a column only to a wider type of its
  kind and refusing any other. A dtype is mathspec's (`float`, `int`, `bool`, `str`, `datetime`);
  a dim may list the labels it allows, `{"one_of": [...]}`, which makes it an enum-dtype dim: its
  column is an `ENUM`, `add` refuses an unlisted label, and its axis holds only the labels added, and
  a property may name a dim, `{"one_of": "<dim>"}`, which replaces `into` on
  properties and groups. The PyPSA tool declares enum-dtype `ctype` and
  `role` dims, writing every label they list, and a `timestep` dim. Every record is
  re-exported.

### Fixed

- A later `set` replaces the rows an earlier one staged that it covers,
  that is, at each of their coordinates it names the same label or leaves
  NULL, labels selected by a property keyword or a group's fields included:
  `set("p_nom", 0.9, entity=["a"])` and then `set("p_nom", 0.95)` read
  0.95 at `a` rather than 0.9. The other order keeps both rows, and `a`
  reads 0.9.
- A layer holding a default and its exception, such as
  `set("p_nom", 0.95)` and then `set("p_nom", 0.9, entity=["a"])`, reads one
  value at each coordinate: within one layer, the more specific row wins, a row naming a
  label of a dim being more specific than one leaving that dim NULL. A write
  of two rows covering one coordinate where each names a dim the other leaves
  NULL is refused, and a read of such a layer written another way fails, each
  naming the attribute and the coordinate.
- The writer holds a column to its declared dtype as `set` does: an
  attribute's `value` is stored as its dtype, an integer column under a
  `float` attribute as `DOUBLE` rather than `BIGINT`, and a column of a type
  that does not fit its dtype - a string for a `datetime` dim or a `float`
  property - is refused naming the attribute, property or dim and its dtype,
  rather than parsed or cast. A column holding only NULLs passes as its
  declared type's NULL, in `set` and `add_group` too, whatever type its
  dataframe library gave it. The PyPSA
  tool's static rows no longer widen an integer `snapshot` to `DOUBLE`.
- A label of a type that does not fit its dim's dtype - a series index, mapping
  keys or a keyword of `set`, a group property's field keyword, the labels of
  `add` and `remove`, or a value of a property naming a dim - is refused with
  the dim and its dtype named, by the rule a frame's column is held to, rather
  than failing deep in DuckDB or being parsed: `snapshot="2030-01-01"` on a
  `datetime` dim was read as a timestamp, and is now refused.
- The PyPSA tool round-trips a network's snapshot and investment period
  weightings: `to_datarecord` writes them as `snapshot_weighting_objective`,
  `_stores` and `_generators` over `snapshot` and
  `period_weighting_objective` and `_years` over `period`, beside
  `scenario_weight`, and `build` sets them back rather than leaving PyPSA's
  defaults.
- The PyPSA tool round-trips a multi-period network, which `to_datarecord`
  failed to write: its `snapshot` is one flat axis beside `period`, each
  snapshot carrying `snapshot.period`, a property `into` `period`, so
  `period=2030` selects a period's snapshots. A snapshot is labelled by its
  timestep where no timestep repeats across periods, else by its position,
  the timestep then `snapshot.timestep`. Every input and result keeps the
  dims it has in a single-period network, whose schema is unchanged. `build`
  restores the snapshots, investment periods, time series and both
  weightings.
- The PyPSA tool round-trips a network's shapes, which `to_datarecord`
  failed to write: a geometry is stored as WKT in `inputs/geometry.parquet`
  and the network's CRS as `crs` in the schema's `meta`, and `build` restores
  `n.shapes` as a GeoDataFrame in that CRS. `srid` is no longer written among
  the network attributes.
- Removing an entity drops the values of every group row naming it, not only
  where it is the group's first entity coordinate: a `contract` over
  `(from, to)` loses its values when either party is removed, and
  `flags(ctype)` counts such a row towards the type of each party.
- Writing a layer whose `manifest.json` write keeps losing, as it does with a
  credential that can write the manifest but not read it, gives up after ten
  attempts with an error naming `s3:GetObject`, rather than looping forever.
- `materialise()` writes `owner_map/inputs.parquet` last, each file through
  a temporary name, so a node never reads as materialised from half its
  cache.
- A fold cached before the manifest widened now reads under the wider schema.
- Reading an attribute over a `partial` dim no longer slows with the square
  of that dim's length.
- Folding the owner map no longer compares every row with every label of a
  `partial` dim's axis; the first read of a record with long series is linear
  again.
- A record whose layers are all written scans its owner map once for the
  layers owning each attribute, rather than once per attribute read.
- A pickled `Revision` unpickles through `Workspace.process()` and reads its
  ancestry back, rather than reading empty, and pickles after `.record` is
  read rather than failing on its DuckDB connection.
- A standalone record's `manifest.json` carries an `id` that `ws.write`
  mints, and which keys its folds in the workspace in place of its URI. A
  record deleted and written again at one URI, or one relative path read
  from another working directory, no longer reads the folds of the record
  before it. `ws.at` resolves a local path as `ws.store` does, and refuses a
  manifest without an id: write such a record again.
- `ws.write` to an existing empty directory raises its `FileExistsError` up
  front, saying a standalone record is written to a URI that does not exist
  yet, rather than passing the check and failing inside the write.
- `PyPSA.to_datarecord` melts a time series whose component name holds a
  `"`, or is `snapshot` or `period`, rather than failing to parse or
  duplicating an id column: a name is no longer pasted into the `UNPIVOT`
  as an SQL identifier.
- `PyPSA.build` keeps the time series of a component named `snapshot`, or
  `period` in a multi-period network, rather than dropping it from
  `dynamic`: the `PIVOT` is on each component's position, so no name
  becomes an SQL identifier there either.
- `PyPSA.to_datarecord` accepts components whose names differ only by case,
  such as `Gen` and `gen`, rather than reporting them as one name: neither
  the melt nor the pivot folds case any more.
- `PyPSA.results` and a network's `outputs` key exactly the results the
  PyPSA schema declares, so setting each one with `kind="outputs"` no longer
  raises `KeyError`: `Bus.control` and `SubNetwork.carrier`, which PyPSA
  also registers as inputs, are no longer emitted as results.
- `PyPSA.build` turns a per-connection input back into PyPSA's port columns:
  `efficiency` on a Link's bus2 connection is `efficiency2` again, static or
  per snapshot, rather than every port's value landing in `efficiency` and
  `efficiency2` reading as its default.
