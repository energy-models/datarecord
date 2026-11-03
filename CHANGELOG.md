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
- `Workspace` holds the DuckDB database and `Store` holds one location. Each
  layer under `layers/<id>/` carries its own `manifest.json`, a `Manifest`
  holding its id, its parent, the full schema it was written under, the files
  it wrote and a flat `meta` of strings, integers, finite floats and booleans.
  A read opens exactly the files the manifest lists and globs nothing. A
  `Revision` is its location and id: it compares, hashes and serialises as
  them, and reads its ancestry in a new process. `rev.manifest` and
  `rev.meta` answer from the workspace's `revisions` and `schemas` tables,
  which cache each manifest whole. `ws.con` is the cursor of the calling
  thread. The write and edit boundary rejects a relation from another
  connection or thread.
- Writing is `store.commit(source, parent=..., meta=...)`, with `parent=None`
  for a new root, and `ws.write(source, uri, meta=...)`.
  `store.claim(parent=, meta=)` hands out a layer id by a create-only
  `claim.json` before any data is written, and `claim.commit(source, meta=...)`
  writes the data into `layers/<id>/` and then the manifest, create-only.
  Until then the layer is pending and no read reaches it. A commit may add
  `meta` keys but not change a claimed one. A child whose schema is not
  `compatible_with` its parent's, or a parent from another store, is refused.
  `store.commit(w)` writes the edits of a `WorkingRecord` as a child of the
  revision it was built over, and refuses one built over anything else,
  naming `ws.write(w, uri)`, which writes the flattened record. Each claim and
  manifest is written once, create-only.
- A `Claim` serialises and pickles as its store's location and id, and reads
  its parent, `meta` and claim time back from its `claim.json`, so a job in
  another process commits it. `store.claims()` lists the pending layers,
  oldest first, with their `meta`, and `store.revisions(meta={...})` returns
  the revisions whose `meta` holds every pair given.
- `store.cleanup(ids=[...])` and `store.cleanup(older_than=timedelta(...))`
  delete pending layers, and refuse an id that is not pending. Nothing else
  deletes.
- A store or standalone directory on `s3://`, through boto3 behind the
  `datarecord[remote]` extra and configured by the AWS environment. Claims and
  manifests are written create-only, and each writer checks its schema
  against its parent's manifest, which never changes, so two writers never
  race.
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
  whole, copied from the base at its first edit. A materialised node names
  the layer holding each axis's deepest file in `resolved/dim_owners.parquet`.
- `WorkingRecord.set` checks that an attribute value names a label that its
  dim's axis, as staged, holds, for every dim, and a tuple that its group, as
  staged, holds. `add` checks that each label of a tuple is on its axis
  as staged, and that a value of a property with `one_of` is a label of that
  dim as staged. `write_layer` checks every attribute row against the final
  axes and groups of the layer, and every group tuple against the final axes.
  Each check is one DuckDB anti-join.
- A schema is checked at load. A dim, group, group coordinate or attribute
  name that is not a plain identifier (mathspec's rule: ASCII letters, digits
  and `_`, not starting with a digit), starts with `_`, is a column that datarecord
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
- `PyPSA.build` takes any `RecordLike`, so `PyPSA.build(PyPSA.to_datarecord(n, con))`
  builds the network its committed record does.

### Changed

- Breaking: `RecordLike` declares `con`, the connection its relations are
  planned on; a hand-written `RecordLike` adds it.
- `WorkingRecord` over a `RecordLike` that is no `Record` raises a `TypeError`
  naming the fix, `WorkingRecord(ws.write(source, uri))`.
- Breaking: the PyPSA dialect declares every component attribute over
  `scenario`, except those PyPSA keeps the same in every scenario
  (`*_nom_extendable`, `*_nom_opt`, `committable`, `lifetime`, ...), so a
  static `p_nom` that differs per scenario round-trips (#28). A keyed
  `set("p_nom", {...})` is then open in `entity` and `scenario`; pass the
  labels as `entity=[...]` or as a series indexed by `entity`.

- A workspace validates each distinct schema once. Revisions sharing a schema
  hand out one `Schema` object, so reading a long lineage no longer re-runs the
  schema's checks per layer. A `Schema` obtained from a revision is shared:
  copy it before changing it.
- A materialised node no longer copies its axes into `resolved/dims/`.
  `resolved/dim_owners.parquet` names, per dim, the layer whose
  `dims/<dim>.parquet` is the deepest at or below the node, and a read opens
  that file. Its `has_dead` column says which dims have dead labels in
  `resolved/dead/`, so a read opens those by name and lists no directory.
  `Store.dim_owners_uri(id)` is where it lives. A node materialised
  before holds no such file and fails the read: delete its `resolved/` and
  materialise it again.
- datarecord requires DuckDB 2.0. A relation it returns is a DuckDB 2.0
  relation: it re-runs on every fetch and has no `fetchone()`.

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
  is the list, and no `dims/<dim>.parquet` is written or read for it, nor is
  it named in `resolved/dim_owners.parquet`. The writer refuses a `LayerData`
  handing one over, a read refuses a layer holding one, and a `RecordLike`'s
  axis of it is dropped where it is the list, same labels in the same order,
  and refused otherwise, naming a NULL, a repeated, missing or extra label, or
  else the order.
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
- A `Record` is constructed from `revision_id`, `sources` and `workspace`, and
  reads under its last source's schema. A caller reads it through its
  mappings: `rev.record.sources`, `dims[dim]`, `properties[dim]` and
  `attributes[name]`. `attributes[name]` raises `KeyError` for an undeclared
  attribute.
- `PyPSA.build` raises `TypeError` for a `RecordLike` that is not a `Record`.
  `Schema.long_columns_for` and `Schema.coordinates_of` raise `KeyError` for an
  attribute that the schema does not declare.
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
- A layer's id is its manifest's. A standalone directory is one layer with
  no parent, and `ws.at` refuses a manifest naming one, a layer of a tree
  being read alone through `store.layer(id)`.
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
- The writer refuses a `dims/` file of a dim that the schema does not declare,
  as it refuses an undeclared attribute. A descendant declaring the dim would
  read its labels.
- The writer accepts two rows that each name a dim the other leaves NULL
  where a row naming both is there, so `ws.write(w, uri)` writes a record of
  several layers. A `set` staging two rows that cross with no row naming
  both is refused.
- The fold reads each layer under the schema in its own manifest. Where a
  deeper layer's schema is wider, the map folded so far, materialised or not,
  is extended to it, so a materialised node keeps counting after a descendant
  adds a dim to `partial`, and a workspace holds one fold table per node,
  `owner_map_<id>`. A layer that restates a key along a dim outside `partial`
  and omits a label its parent held there deletes that value, and a later
  widening of `partial` no longer brings it back.
- The fold of a layer onto the map below it is one query whose joins its
  schema fixes, whatever named sets its rows hold (`fold_layer`, replacing
  `fold_attributes`). A frozen layer leaving a dim outside `partial` unfilled
  is refused when a workspace first folds it, not on every read.
- Every `Record` lists every dim, property subject and attribute its schema
  declares, a typed empty relation where no layer holds a row, rather than
  raising `KeyError`.
- A relation of a `WorkingRecord` is planned without reading a staged row,
  only the frozen layers' rows, so one built before an edit reads it. Every staging table exists
  from the start, an axis holding a copy of the base's, and `rollback()`
  empties them rather than dropping them. Each edit runs in one transaction,
  which also refreshes the staged owner-map rows the fold, `flags()` and the
  write read.
- A `set` whose row crosses a staged row, with no staged row naming both, also
  stages the row naming both with its value: the later edit wins where the
  two meet, rather than the read failing.
- The edits of a `WorkingRecord` restate an axis only where it differs from
  the base's, and write no attribute without rows.
- A fold starts from the deepest proper ancestor whose fold is materialised
  or already held by the workspace, so a record opened on a child after a
  commit folds only the child's layer.
- A `Record` builds the coords of its frozen layers once per thread rather
  than on every relation it reads, about halving a warm read of a deep
  record. A write checks a new layer's axes against its parent's fold.
- The cold fold of a deep record's owner map takes about a third of the
  time: each layer's owner rows, the parent rows they leave and the pairs
  the check reads are each built once, the crossing pairs are taken from the
  check's, unions bind once per arm, the parts of the plan a schema fixes are
  built once per schema, and a layer's flags test only the attributes it
  holds.
  `Revision.materialise` writes the fold its workspace already holds rather
  than folding again.
- An edit inside a transaction the caller holds open on the same cursor is
  refused with a `TransactionException` that says so, since an edit runs in
  its own transaction.
- `Attr.resolve` and `Schema.resolve` take `labels` and `flags`, and settle
  each source by the broadcast rule over `labels` before a rename or a
  computation. `settle(rel, labels, flags)`, exported from `datarecord`, does
  the same for one relation, dropping rows that name a label outside `labels`.
- A failed write, local or remote, leaves its files in the pending layer
  rather than staging them locally and renaming the directory into place.

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
- `store.root`, `rev.child`, `Schema.id`, the store's one `manifest.json`
  with its compare-and-swap, and the `revisions/` tree are gone, as is the
  local staging directory of a write. A store written before has no layer
  manifests, so it is unreadable and is written again.

### Fixed

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
- `materialise()` writes `owner_map/inputs.parquet` last and each file through
  a temporary name, so a node reads as materialised only when its cache is
  complete.
- A map materialised under a parent's schema reads under a child's wider one.
- An attribute whose `dims` gained a dim reads its earlier rows, NULL along
  it, where that dim is in `partial` too. `flags()` reports such rows as
  broadcast along the gained dim, whether it is in `partial` or not, and
  whether or not the schema declared it before.
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
