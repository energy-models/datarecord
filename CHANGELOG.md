<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

All notable changes to datarecord are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Properties. The schema has a `properties` section (`PropertySpec`) that
  declares, per dim or multi-dim, the values that describe one label. A record
  stores them as columns of `dims/<dim>.parquet`.
  A caller names a property as `subject.name`, as in `set("bus.x", ...)` and
  `default_by` keys such as `{"entity.ctype": {...}}`. `Schema.resolve(name)`
  returns the spec of a name, and `Schema.column_type` takes the property's
  subject. A property may share its name with an attribute, a dim or a
  property of another subject.
- A property declares exactly one of `dtype` and `one_of: <dim>`. A property
  with `one_of` refers to that dim, and its values are live labels
  of that dim. `set` and `add` check them, and the writer checks every layer.
  `remove(dim, labels)` fails while such a property still holds a label.
- A property that refers to a dim with `one_of`, on a subject in
  `partial`, a multi-dim among them, categorizes the subject. Categories select labels in
  `flags(ctype="Generator")` and in `set("p_max_pu", 0.9, ctype="Generator")`.
  `default_by` gives a default per category. `Schema.categorizing` lists the
  categorizing properties of a subject.
- A dim of dtype `str` may declare `one_of: [...]`, as in mathspec. Its column
  is an `ENUM` of those labels, and its axis is the list, in order.
- `WorkingRecord` edits any dim: `add(dim, labels, **properties)`,
  `remove(dim, labels)` and `set(name, value, **coords)`. `set` writes
  attributes and properties, and a property keyword selects labels when `set`
  is called.
- A multi-dim is a broadcast coordinate like any dim. A NULL multi-dim value
  means every tuple of the multi-dim, and `partial` may name a multi-dim.
- `Workspace` holds the DuckDB database and `Store` holds one location. A
  store keeps a revision file per revision as `revisions/<id>.json`, so a
  `Revision` serialises as its location and id and reads its ancestry in a new
  process. `ws.con` is the cursor of the calling thread. The write and edit
  boundary rejects a relation from another connection or thread.
- Writing is `store.root(source)`, `rev.child(source)` and
  `ws.write(source, uri)`. `rev.child(w.edits)` writes the edits of a
  `WorkingRecord` as a child layer, and `ws.write(w, uri)` writes the
  flattened record. A tree write puts the revision file last, so a failed
  write leaves no revision and does not widen the manifest.
- A store or standalone directory on `s3://`, through boto3 behind the
  `datarecord[remote]` extra and configured by the AWS environment. Revision
  files are written create-only and the manifest by compare-and-swap, so two
  writers that widen one schema both keep their widening.
- `Tool.to_datarecord(model, con)` takes the connection that its frames are
  planned on.
- Removing an entity removes the group tuples that name it, and the values of
  a tuple go with it. An `add` of the name revives the entity alone. A layer
  that adds a tuple naming a removed label, or removes a label that a tuple
  names, fails at write.
- A layer that changes an axis or a group restates it whole, in order. A label
  is removed where a restatement omits a label that the previous one held,
  and its order is the file's row order. The deepest file for an axis or group
  is the answer for a node. A `WorkingRecord` stages an edited axis or group
  whole, copied from the base at its first edit. A materialised node holds
  copies of the deepest files in `resolved/dims/` and `resolved/groups/`.
- `WorkingRecord.set` checks that an attribute value names a label that its
  dim's axis, as staged, holds, for every dim, and a tuple that its group, as
  staged, holds. `add` checks that each label of a tuple is on its axis
  as staged, and that a value of a property with `one_of` is a label of that
  dim as staged. `write_layer` checks every attribute row against the final
  axes and groups of the layer, and every group tuple against the final axes.
  Each check is one DuckDB anti-join.
- A schema is checked at load. A dim, group, group coordinate or attribute
  name that holds `"` or `.`, starts with `_`, is a column that datarecord
  writes itself (`value`, `layer_uuid` and others, ignoring case), or differs
  from another name only in case fails with an error that names each offender.
  Based on #26 by @FBumann.
- A manifest with an unknown key fails to load with pydantic's
  `ValidationError`.
- The PyPSA tool declares buses and carriers as dims. A bus is a label of
  `bus` and a carrier a label of `carrier`. The properties of `bus` are
  `carrier`, `type`, `x`, `y`, `unit` and `location`, and those of `carrier`
  are `color` and `nice_name`. `v_nom`, `v_mag_pu_*`, `co2_emissions`,
  `max_growth` and `max_relative_growth` are attributes over their dim. `marginal_price`, `v_mag_pu` and `v_ang` are
  results over `bus`, and `build` sets them back. `to_datarecord` adds every
  carrier that a component names and `carrier` does not list, and stores `""`
  as NULL. `verify` reports an entity attached to no bus.
- The PyPSA tool declares the dims `ctype` and `role` with `one_of`, and
  `entity.ctype`, `entity.carrier` and `connection.role` as properties.
  `partial` is `{"entity", "bus", "scenario", "connection"}`.
- PyPSA SubNetworks are labels of the dim `sub_network`, without a suffix.
  `sub_network.carrier` refers to `carrier`, and `slack_bus` is a
  property over `sub_network`. A solve that has SubNetworks restates the
  `sub_network` axis, and a solve without them restates it empty where the
  record has SubNetworks. `SubNetwork.obj` is not written, and `build`
  gives each SubNetwork a fresh one, so a built network passes PyPSA's
  topology code and optimises.
- A static column that PyPSA's registry lacks is a property of its type's
  subject if it holds text, such as pypsa-eur's `Bus.country` as
  `bus.country`, or if as an attribute its name would hold two shapes, such as
  a custom `Generator.v_nom`. Any other, such as `Bus.nom_max_onwind_2050`, is
  an attribute over its type's dim. `build` sets either back on the types
  that carried it. `to_datarecord` refuses a custom property that types of
  one dim carry with different dtypes. `Requirements.columns` is gone, as no
  custom column clashes any more.
- The PyPSA tool stores per-port results on connections like per-port inputs.
  `p0`, `p1`, `p2` and a one-port type's `p` are one result `p` over the
  `connection` group, with a row per `(entity, bus)` that keeps the port's
  sign. `q` works the same way. `Link.p` and `Process.p` are `p_internal`, and
  a three-port Link's `p2` is a declared result.
- The PyPSA tool stores a Process's `rate0`, `rate1`, `rate2` as one input
  `rate` over `connection`, static or per snapshot, and `build` puts each row
  back in its port's column. `rate` declares no default, so its ports default
  to `-1` and `1`.
- The PyPSA tool round-trips multi-period networks. `snapshot` is one flat dim
  next to `period`, and each snapshot has a property `snapshot.period` with
  `one_of: period`, so `period=2030` selects the snapshots of one period. A
  snapshot is labelled by its position, whether or not a timestep repeats
  across periods, with the timestep in `snapshot.timestep`, so a child whose
  timesteps repeat otherwise than its parent's keeps the parent's labels.
  `build` restores snapshots, investment periods, time series and both
  weightings.
- The PyPSA tool round-trips snapshot and period weightings:
  `snapshot_weighting_objective`, `_stores` and `_generators` over `snapshot`,
  `period_weighting_objective` and `_years` over `period`, and
  `scenario_weight`.
- The PyPSA tool round-trips a network's shapes. A shape is the property
  `shape_{type}` of the label it outlines, its geometry as WKT: a `Bus` shape
  is `bus.shape_onshore`, and a `component` that is no PyPSA type, such as
  pypsa-eur's `country`, is a dim of its own whose labels are the shapes'
  `idx`. Such a dim is no dim of the record's own and `build(select=...)`
  refuses it. A custom text column named like a dim refers to it, so
  pypsa-eur's `bus.country` is `{one_of: country}`. The CRS is `crs` in the
  schema's `meta`, and `build` restores `n.shapes` in that CRS.
  `to_datarecord` refuses a shape without a `component`, `idx` or `type`, two
  sharing `(component, idx, type)`, one naming no member of its PyPSA type, a
  custom column named `shape_...` or like a key dim (`scenario`, `entity`,
  ...) and a custom value naming no label of the dim it refers to. `build`
  names each shape `"{component} {idx} {type}"`.
- `PyPSA.to_datarecord(n, con, results_only=True, base=record)` writes a solve
  as a thin layer. The layer restates the SubNetworks of `base` and sets
  NULL on the results of `base` that it does not write again. A NULL series
  value on export leaves the component at its static value.
- `PyPSA.to_datarecord` takes a `pypsa.NetworkCollection` with
  `collection_dim`, the dim its index labels, as a full export or as results
  over `base`. `collection_dim` is required for a collection and refused for
  a single network. Over `period` it is a myopic solve, written as the one network
  holding every member's period, whose static values must agree. Over
  `scenario` its deterministic members are one stochastic record, weighing
  equally, and members may differ only in an attribute PyPSA declares over
  `scenario`: members solved apart, whose `p_nom_opt` differs, are refused at
  the call. Over any other name, a dim of the record's own, that dim is added
  to the schema and to `partial`, an attribute whose values differ across the
  members varies over it, and one whose values agree is written once, as
  declared. Members hold one set of components, their axes are one union in
  one order, their properties must agree, and every check runs when
  `to_datarecord` is called. A record has at most one dim of its own: a
  collection over another is refused over a `base` that has one.
- `PyPSA.build` returns a `pypsa.NetworkCollection` indexed by a record's dim
  of its own, each member the network at its label. `verify` reports each such
  dim of a record declaring more than one in `Requirements.unsupported_dims`,
  a new field for dims a tool has no place for.
- `PyPSA.build(record, select={...})` builds from a selection of the record's
  labels, as xarray's `sel` takes them. One label of `period`, `scenario` or a
  dim of the record's own squeezes the dim: a single-period network indexed by
  its timesteps holding the entities active in that period, a global
  constraint for another period dropped, a deterministic
  network, or one `Network` of a collection; the period squeezed out is
  recorded in `n.meta`, so the network's results write back at it. A list
  keeps the dim at those labels, renormalising the kept scenarios' weights,
  and a list of periods holds the entities active in at least one of them.
  `snapshot`, `bus`,
  `entity` and `carrier` take a label or a list, and a label referring to a
  dropped one goes with it: a snapshot of another period, a bus or entity of
  another carrier, an entity attached to a dropped bus. A snapshot is named by
  its timestamp, in every period where the periods repeat their timesteps, and
  a partial date string names every one in its span. A
  slice, as pandas' `.loc` takes it, keeps `snapshot`, `period` or a dim of
  the record's own of numbers or datetimes at the labels it covers. `verify`
  takes the same `select`.

### Changed

- datarecord requires DuckDB 2.0. A relation it returns is a DuckDB 2.0
  relation: it re-runs on every fetch and has no `fetchone()`.
- The `pypsa` extra requires PyPSA 1.4.
- `PyPSA.to_datarecord` takes `base` on a full export too, and drops the
  `ValueError` for `base` without `results_only`. Over `base` a static value
  is left out only where it repeats `base`'s value, else the default, so a
  value set back to PyPSA's default over a parent holding another is written
  and no longer lets the parent's show through. A `NetworkCollection` takes
  `base` only with `results_only=True`.

- The PyPSA tool melts a time series one entity at a time, so a parquet row
  group holds few entity names and encodes them by dictionary. The
  `p_max_pu` file of an 80,000-generator hourly network shrinks from 4.7 GB to
  2.3 GB.

- Every parquet file datarecord writes is zstd-compressed parquet v2, with a
  row group's dictionary capped at 4 KB, so a series' snapshots are
  delta-encoded and its values byte-stream-split. That `p_max_pu` file comes
  to 406 MB. Attribute files are written without keeping row order, so writing
  a long series no longer spills tens of GB; dims files keep theirs, which is
  the axis order.

- Groups are multi-dims. A dim declared under `dimensions` with
  `over`, and neither `dtype` nor `one_of`, is a multi-dim: its labels are
  tuples of other dims' labels, carried as one struct column named for it.
  Its table is `dims/<multi-dim>.parquet`, keyed by that column with its
  properties beside it; `groups/` and `resolved/groups/` are gone, so every
  record with a group is written again. `record.dims["connection"]` is the
  struct column alone and `record.properties["connection"]` adds `role`.
  `add` and `remove` take a multi-dim's tuples as a frame holding its struct
  column, so `add("connection", record.dims["connection"])` round-trips, or
  one with a column per coordinate instead, or as a list of mappings from
  coordinate to label.
  `connection.bus` names a coordinate wherever `subject.name` stands, and
  `set` refuses it. No property refers to one. New on `Schema`: `multi_dims`, `over`, `multi_dims_of`,
  `check_multi_dim_columns` and `coordinate_over`; `dims` lists every dim but
  the multi-dims. The PyPSA tool declares `connection` under `dimensions`.
- A dim with `one_of` has its axis derived from the list: `record.dims["ctype"]`
  is the list, and no `dims/<dim>.parquet` or `resolved/dims/<dim>.parquet` is
  written or read for it. The writer refuses a `LayerData` handing one over, a
  read refuses a layer holding one, and a `RecordLike`'s axis of it is dropped
  where it is the list, same labels in the same order, and refused otherwise,
  naming a NULL, a repeated, missing or extra label, or else the order.
  `add` and `remove` on it are refused, and a property declared on it is
  refused at load. A record carrying such a file is written again. New on
  `Schema`: `one_of_dims`; `Schema.require_listed` is `require_one_of`. The
  PyPSA tool no longer writes the `ctype` and `role` axes.
- A multi-dim in `partial` is categorized by a property that refers to a dim
  with `one_of`, as a dim is: `connection.role` categorizes `connection`, so a
  `default_by` keyed `connection.role` loads, `Schema.categorizing("connection")`
  lists `role`, and `flags(role="output")`, `default_for(name, role="output")`
  and the `set` keyword `role="output"` select by it. A role keyword picks the
  tuples of an attribute over `connection`, narrowed by a coordinate keyword,
  and of a property of it: `set("connection.note", v, role="output")` patches
  the output connections, as `set("entity.note", v, ctype="Generator")` does
  the generators, and `ctype="Link"` on it the connections of links.
- The design pages are grouped by topic: the data model, layers, using a
  record, storage, frameworks and the package. Each old design page URL
  redirects to its new page. The usage pages are organised by task, and the
  API reference uses the design section names.
- An attribute over a multi-dim carries it as one struct column named for it,
  with a field per coordinate, such as `connection STRUCT(entity, bus)`. Its
  table is keyed by that column, and `dims/connection.parquet` is
  `connection STRUCT(entity, bus) | role`. The PyPSA tool declares no `bus`
  attribute. A port's bus is the connection tuple.
- The dims that a multi-dim draws on broadcast. A per-bus value with
  `bus = NULL` holds for every bus, and `bus` must be in `partial` to patch one
  bus at a time. `WorkingRecord.set` still takes `bus=`. Naming a multi-dim
  next to a dim that one of its coordinates is named for, as in
  `dims={"connection", "bus"}`, is an error.
- `entity` is an ordinary dim. It broadcasts, and it must be in `partial` to
  be patched or categorized per label. `entity_type` is `entity.ctype`.
- An attribute reaches every label that its `dims` address, whatever the
  categories of the labels. A NULL on a categorized dim reaches every label.
  `flags(**categories)` holds every attribute with a row that reaches a
  matching label. The PyPSA tool keeps the per-type vocabulary. `build`
  leaves out a value that its type does not take and reports it in
  `Requirements.ignored`, and `to_datarecord` records custom entity columns
  per type in the schema's `meta`.
- Every layer holds its attributes in one `attributes/` directory, and a
  result inherits through layers like any attribute. The PyPSA tool declares
  its outputs as attributes, and `build` copies them.
- `record.dims`, `properties` and `attributes` return
  `DuckDBPyRelation`s. `Frames` and `LazyFrames` are now `Relations` and
  `LazyRelations`. `set`, `add` and the writer take a relation or a
  pandas, polars or arrow frame, which DuckDB reads with its own scanner.
  They refuse timestamps with a time zone and cast a column only to a wider
  type of its kind. A dtype is mathspec's: `float`, `int`, `bool`, `str` or
  `datetime`.
- The owner map's key is the `partial` dims and groups plus `attribute`. A dim or
  group outside `partial` is owned whole, so a layer that patches one
  connection's value restates every connection's. The PyPSA tool declares
  `partial` so that it keeps per-connection ownership.
  `Schema.input_key` is `Schema.owner_map_key`, `Schema.input_columns` is
  `Schema.owner_map_columns`, and `Requirements.unsupported_keys` reports
  `"owner_map_key"`.
- A `Record` is constructed from `revision_id`, `sources`, `workspace` and
  `schema`. A caller reads it through its mappings: `rev.record.sources`,
  `dims[dim]`, `properties[dim]` and `attributes[name]`.
  `attributes[name]` raises `KeyError` for an undeclared attribute.
- `PyPSA.build` raises `TypeError` for a `RecordLike` that is not a `Record`.
  `Schema.long_columns_for` raises `KeyError` for an attribute that the schema
  does not declare.
- `LayerData` declares no `frozen`, and its `axes()`, `groups()` and
  `attributes()` return `set[str]`. A whole `Record` reaches the writer
  through the adapter for a `RecordLike`.
- The modules live in the subpackages `datarecord.schema`, `datarecord.resolve`
  and `datarecord.store`. `datarecord.record` is `datarecord.protocols`,
  `datarecord.files` is `datarecord.store.files`, and
  `datarecord.layered.categories` is `datarecord.schema.categories`. The
  dtype helpers moved from `datarecord.duck` to `datarecord.schema.dtypes`.
  Every public name imports from `datarecord`.
- A `WorkingRecord` folds its staged layer onto the folded axes, groups and
  removed labels of its base, which the workspace keeps as tables. Folding the
  staged edits opens no file of the base.
- A label or group tuple that one layer removes and a later layer adds again
  takes its place where it is added again, in a working record, a written
  record and a materialised node.
- A key held twice in a `dims/` or `groups/` file, and a column that is
  neither the key nor a declared property of its subject, fail at read.
  `write_layer` refuses a restatement that orders the labels of its parent
  differently.
- A reference to a removed label in a group tuple or a property is checked in
  every subject that a layer restates, whole. A reference that an ancestor
  left dangling fails the next layer that restates its subject.
- A standalone record's id is `Schema.id`, an optional field that its
  `manifest.json` carries. A tree's manifest carries none.
- A value over a dim or group outside `partial` that names a removed label is
  dropped at read, including the per-bus values of a removed `bus` label. A
  materialised node keeps the removed labels in `resolved/dead/`.
- `attributes[name]` returns the owned rows as written. A NULL coordinate
  stays NULL, rows of two layers may cover one coordinate, and the row order
  is unspecified. A consumer settles each coordinate by the broadcast rule,
  the row naming every dim the others there name winning. The owner map holds
  each layer's keys as written, plus one row naming both wherever a deeper
  layer's row crosses a parent's, so it grows with the rows layers write
  rather than with the labels a broadcast reaches.
- A deeper layer owns only the coordinates its rows reach. A shallower row
  keeps every coordinate that no more specific row covers.
- A label removed and added again takes every broadcast covering it, written
  before or after the removal, and no row that named it below the removal.
- `flags()` counts a broadcast that more specific rows shadow at every label,
  and reports in `varies` and `broadcast` only the dims an attribute uses.
- `WorkingRecord.set` writes a row that leaves a dim outside `partial` NULL
  out along the labels a parent row names there. The fold refuses a layer
  written otherwise that leaves such a crossing, naming the layer, the
  attribute, the dim and the key.
- The writer accepts two rows that each name a dim the other leaves NULL
  where a row naming both is there, so `ws.write(w, uri)` writes a record of
  several layers. The read of a `WorkingRecord` refuses two staged rows that
  cross with no staged row naming both.
- A materialised owner map keyed on another `partial` set than the schema's
  is ignored, and the read folds that node from its layers.
- `Attr.resolve` and `Schema.resolve` take `labels` and `flags`, and settle
  each source by the broadcast rule over `labels` before a rename or a
  computation. `settle(rel, labels, flags)`, exported from `datarecord`, does
  the same for one relation, dropping rows that name a label outside `labels`.
- A `NetworkCollection` over a dim of its own or `scenario` compares its
  members in DuckDB rather than in pandas, and checks their axes and
  properties before any attribute, so differing SubNetworks are refused before
  the attributes are compared. An attribute whose values differ is written at
  each label only for the owner keys, such as an entity, where the members
  differ; the rest is written once, NULL on the dim. An owner key's whole
  extent along `snapshot` and every other dim outside `partial` is written
  one way or the other, never split. Members' values now agree within the
  tolerance across all of them at once rather than each against the first.

### Removed

- `Group`, the schema's `groups` section, `Schema.groups`,
  `Schema.group_columns`, `Schema.group_type`, `Schema.groups_of`,
  `Schema.group_key`, `Schema.check_group_columns`, `Schema.field_over`,
  `RecordLike.groups`, `LayerData.groups()` and `LayerData.group()`,
  `WorkingRecord.add_group` and `WorkingRecord.remove_group`.
- `set` takes no expression. Compute the new values from `attributes[name]`
  and `set` the frame.
- `Dimension.within` is gone. Every axis is keyed by its label alone, with
  labels unique across the dim. A nested dim is a property with `one_of` on
  the parent dim, such as `snapshot.period` with `one_of: period`. `add`
  takes no parent keys, and `Schema.axis_key` is gone.
- Traits are gone: `Trait`, `Schema.traits`, `Schema.attributes_for` and
  `Schema.properties_for`.
- `Schema.results`, `Record.outputs`, `Kind`, `set(..., kind=...)` and
  `Tool.results` are gone.
- narwhals is not a dependency. `Frames` and `LazyFrames` are replaced as
  listed under Changed.
- `Resolver` and `Revision.resolver` are gone, with `with_source`, `inputs`
  and the enumerators `axes()`, `groups()` and `attributes()` of the resolver.
- `write_record`, `Revision.create`, the source-less `Revision.child()`,
  `NewChild`, `Directory`, `commit`, `connect`, `layer_dir`,
  `DATARECORD_BASE_URI`, the process default connection and `on_connection`
  are gone. `Record.at` takes a workspace.
- `Schema.membership_keys`, `Schema.group_coordinates`,
  `Group.coordinates`, `WorkingRecord.set`'s `indexed_by`, `entity_types` and
  the `entity_type` dim with its member files are gone.

### Fixed

- Reading a property's default keeps the rows in their order. A relation
  filtered by several `IN (SELECT …)` subqueries no longer comes back
  reordered, as when the PyPSA tool builds a network with `select=` on two
  dims.
- The PyPSA tool keeps a network's own line and transformer types. A
  `LineType` or `TransformerType` that PyPSA's standard library does not name
  is a label of the dim `line_type` or `transformer_type`, every column of it
  a property, and `build` adds it to the library. A
  PyPSA-Earth network, whose lines use its own `ZM_330kV_bison_2bundle`, no
  longer builds back with lines that reference a missing type. As in PyPSA's
  netCDF export, a library row the network has changed is not written.
- A results layer writes a series at the network's snapshots alone,
  restating `base`'s values at the others, and over a multi-period `base` a
  result over no snapshot, such as `p_nom_opt`, for the entities active in
  the network's periods alone, so `base`'s results stand at every period,
  snapshot and entity the network does not hold. Writing one period's solve after
  another's no longer zeroes the earlier period's dispatch and capacities,
  and the write no longer builds `base` first. The network may carry any of
  `base`'s investment periods, or none where `build` squeezed one out; one
  carrying fewer than all is written with `results_only=True`, and one
  holding a snapshot, bus or entity `base` lacks is refused at the call. A `period`
  collection counts a static value only from the members holding the entity
  active in their period, so an idle entity's value no longer clashes.
- The PyPSA tool declares a custom column of dtype `object` by its non-null
  values, so a column of bools, ints or floats holding a NaN, numpy scalars
  among them, is written as `bool`, `int` or `float` rather than failing as
  `str`, and one of mixed types is written as text.
- A results layer of a network built from selected snapshots of a record
  repeating its timesteps across periods lands on `base`'s snapshots, not on
  the first ones, and one holding a snapshot `base` lacks is refused at the
  call. A results layer restates as NULL only `base`'s results at
  the scenarios, buses, entities and connections the network holds, so a
  re-solve of a selection keeps every other label's results. A result over
  no `scenario`, such as `p_nom_opt`, is one value across every scenario, so
  a re-solve of some scenarios writes it for all of them. A re-solve of
  fewer buses or entities than `base` leaves `base`'s SubNetworks, since it
  numbers its own over the selection alone. The
  results of a deterministic network over a `base` holding scenarios, or of a
  single network over a `base` with a dim of its own, are refused rather than
  written over every label.
- `PyPSA.build` of a stochastic record indexes `n.shapes` by
  `(scenario, name)`, as PyPSA indexes every other component.
- A collection whose members carry one custom column on different types is
  refused, as one whose members declare different schemas is.
- The writer's check that no coordinate of a file is held twice groups 64-bit
  hashes in passes of at most 100 million rows, so a file of hundreds of
  millions of rows is checked in bounded memory and spill.
- A pointwise `set` leaving one of two dims outside `partial` NULL keeps the
  rest of the series.
- A broadcast reaches every label the record holds, including one added below
  a materialised revision or staged in a `WorkingRecord`, so materialising
  changes no answer.
- `set` refuses a frame column that the schema does not declare for the
  attribute, and names the column and the columns it takes.
- A later `set` replaces the rows of an earlier one that it covers. A row is
  covered where, at each coordinate, it names the same label or leaves NULL,
  including labels that a property keyword or a group's fields select.
  `set("p_nom", 0.9, entity=["a"])` and then `set("p_nom", 0.95)` reads 0.95
  at `a`. The other order keeps both rows, and `a` reads 0.9.
- Within one layer the more specific row wins, so a layer that holds a default
  and its exception, such as `set("p_nom", 0.95)` and then
  `set("p_nom", 0.9, entity=["a"])`, reads one value at each coordinate. A
  row that names a label of a dim is more specific than a row that leaves that
  dim NULL. Two rows that cover one coordinate, where each names a dim that
  the other leaves NULL, fail at write and at read, naming the attribute and
  the coordinate.
- A layer with two rows of one attribute that name the same labels at one
  breakpoint fails at write and at read, naming the attribute and the
  coordinate.
- The writer casts every column to its declared dtype, as `set` does. An
  attribute's `value` is stored as its dtype, and an integer column under a
  `float` attribute is `DOUBLE`. A column of a type that does not fit its
  dtype, such as a string for a `datetime` dim or a `float` property, fails
  with the attribute, property or dim and its dtype named. A column that holds
  only NULLs passes as the NULL of its declared type, in `set` and `add` as
  well. The PyPSA tool's static rows keep an integer `snapshot`.
- A label of a type that does not fit its dim's dtype fails with the dim and
  its dtype named. This covers a series index, mapping keys, a keyword of
  `set`, a multi-dim's coordinate keyword, the labels of `add` and `remove`,
  and a value of a property that refers to a dim.
  `snapshot="2030-01-01"` on a `datetime` dim is refused.
- `add` checks that a property value is a label of its dim, and its error
  says to use one of the labels where the dim has `one_of`.
- Removing an entity drops the values of every group row that names it, in any
  field. A `contract` over `(from, to)` loses its values when either party is
  removed, and `flags(ctype)` counts such a row for the type of each party.
- Writing a layer whose `manifest.json` write keeps losing, as it does with a
  credential that can write the manifest but not read it, stops after ten
  attempts with an error that names `s3:GetObject`.
- `materialise()` writes `owner_map/inputs.parquet` last and each file through
  a temporary name, so a node reads as materialised only when its cache is
  complete.
- A fold cached before the manifest widened reads under the wider schema.
- Reading an attribute over a `partial` dim scales linearly with the length of
  that dim. Folding the owner map no longer compares every row with every
  label of the dim's axis, so the first read of a record with long series is
  linear.
- A record whose layers are all written scans its owner map once for the
  layers that own each attribute.
- A pickled `Revision` unpickles through `Workspace.process()` and reads its
  ancestry, and pickles after `.record` was read.
- A standalone record's `manifest.json` carries an `id` that `ws.write` mints,
  and the workspace keys its folds by that id. A record deleted and written
  again at one URI, or one relative path read from another working directory,
  reads its own folds. `ws.at` resolves a local path as `ws.store` does and
  refuses a manifest without an id.
- `ws.write` to an existing empty directory raises `FileExistsError` up front
  and says that a standalone record is written to a URI that does not exist
  yet.
- `PyPSA.to_datarecord` melts a time series whose component name holds a `"`
  or is `snapshot` or `period`, and accepts components whose names differ only
  by case, such as `Gen` and `gen`. `PyPSA.build` keeps the time series of a
  component named `snapshot`, or `period` in a multi-period network. No name
  is pasted into the `UNPIVOT` or `PIVOT` as an SQL identifier.
- `PyPSA.build` turns a per-connection input back into PyPSA's port columns.
  `efficiency` on a Link's bus2 connection is `efficiency2`, static or per
  snapshot.
- The PyPSA tool skips `Line.v_nom` and `ShuntImpedance.v_nom`, which PyPSA
  derives from the bus, and writes no `Bus.p`, `q`, `control`, `generator` or
  `sub_network`, so a solved network exports.
