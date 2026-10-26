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
  declares, per dim or group, the values that describe one label. A record
  stores them as columns of `dims/<dim>.parquet` or `groups/<group>.parquet`.
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
  `partial`, categorizes the subject. Categories select labels in
  `flags(ctype="Generator")` and in `set("p_max_pu", 0.9, ctype="Generator")`.
  `default_by` gives a default per category. `Schema.categorizing` lists the
  categorizing properties of a subject.
- A dim of dtype `str` may declare `one_of: [...]`, as in mathspec. Its column
  is an `ENUM` of those labels, `add` refuses a label outside the list, and
  its axis holds the labels that were added.
- `WorkingRecord` edits any dim: `add(dim, labels, **properties)`,
  `remove(dim, labels)` and `set(name, value, **coords)`. `set` writes
  attributes and properties, and a property keyword selects labels when `set`
  is called.
- A group is a broadcast coordinate like a dim. A NULL group value means every
  tuple of the group, and `partial` may name a group. A group name may not
  equal a dim name.
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
  staged, holds. `add_group` checks that each label of a tuple is on its axis
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
  `max_growth`, `max_relative_growth` and custom bus or carrier columns are
  attributes over their dim. `marginal_price`, `v_mag_pu` and `v_ang` are
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
  record has SubNetworks.
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
  snapshot is labelled by its timestep where no timestep repeats across
  periods, and by its position otherwise, with the timestep in
  `snapshot.timestep`. `build` restores snapshots, investment periods, time
  series and both weightings.
- The PyPSA tool round-trips snapshot and period weightings:
  `snapshot_weighting_objective`, `_stores` and `_generators` over `snapshot`,
  `period_weighting_objective` and `_years` over `period`, and
  `scenario_weight`.
- The PyPSA tool round-trips a network's shapes. A geometry is stored as WKT
  in `attributes/geometry.parquet`, the CRS is `crs` in the schema's `meta`,
  and `build` restores `n.shapes` as a GeoDataFrame in that CRS.
- `PyPSA.to_datarecord(n, con, results_only=True, base=record)` writes a solve
  as a thin layer. The layer restates the SubNetworks of `base` and sets
  NULL on the results of `base` that it does not write again. A NULL series
  value on export leaves the component at its static value.

### Changed

- The design pages are grouped by topic: the data model, layers, using a
  record, storage, frameworks and the package. Each old design page URL
  redirects to its new page. The usage pages are organised by task, and the
  API reference uses the design section names.
- An attribute over a group carries the group as one struct column named for
  the group, with a field per key coordinate, such as
  `connection STRUCT(entity, bus)`. A group file is keyed by that column, and
  `groups/connection.parquet` is `connection STRUCT(entity, bus) | role`.
  `Record.groups[...]` returns that shape, `add_group` takes the struct column,
  and `remove_group` takes a tuple per key. The PyPSA tool declares no `bus`
  attribute. A port's bus is the connection tuple.
- The dims that a group draws on broadcast. A per-bus value with `bus = NULL`
  holds for every bus, and `bus` must be in `partial` to patch one bus at a
  time. `WorkingRecord.set` still takes `bus=`. Naming a group next to a dim
  that its key names, as in `dims={"connection", "bus"}`, is an error.
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
- `record.dims`, `properties`, `groups` and `attributes` return
  `DuckDBPyRelation`s. `Frames` and `LazyFrames` are now `Relations` and
  `LazyRelations`. `set`, `add_group` and the writer take a relation or a
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
  `dims[dim]`, `properties[dim]`, `groups[name]` and `attributes[name]`.
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

### Removed

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
  only NULLs passes as the NULL of its declared type, in `set` and `add_group`
  as well. The PyPSA tool's static rows keep an integer `snapshot`.
- A label of a type that does not fit its dim's dtype fails with the dim and
  its dtype named. This covers a series index, mapping keys, a keyword of
  `set`, a group property's field keyword, the labels of `add` and `remove`,
  and a value of a property that refers to a dim.
  `snapshot="2030-01-01"` on a `datetime` dim is refused.
- `add_group` checks that a property value is a label of its dim, and its
  error says to use one of the labels where the dim has `one_of`.
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
