<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Resolution

The owner map records each layer's attribute keys as the layer wrote them, with removals applied.
Where a deeper layer's row crosses a parent row, the map adds one overlap row, owned by the deeper layer.
For axes, a multi-dim's tuples included, the deepest file for each one is the answer.
Reading an attribute joins the owning layers' files to the map with one hash join and returns the owned rows as written.
The read then drops rows that name a removed label outside the key.
A consumer settles each coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule).
A single directory folds as a tree with one layer.
A revision can materialise its owner map, and pointers to the layers holding its resolved axes, under `resolved/`, and a read then stops at that revision.

## Owner map

The owner map is one relation over every attribute.
An attribute's winning key and its winning value live in different files, since its ownership spans every layer's `attributes/<attr>.parquet`.
The map names the owning layer per key, and a read then goes to that layer's file for the value.
Every axis is a single keyed file that a layer restates whole, so it needs no map and resolves to a [resolved relation](#one-fold-for-every-axis).

The map's columns, keys first:

```text
# owner map
<partial dims>          -- the fold key: exactly the partial dims, multi-dims included, NULL as written
  <owned_per coordinates> -- e.g. entity, scenario; a partial multi-dim as one STRUCT: connection STRUCT(entity, bus)
attribute
layer_uuid              -- the owning layer
varies      STRUCT(<coordinate>: BOOLEAN, ...)   -- a field per dim, multi-dims included
broadcast   STRUCT(<coordinate>: BOOLEAN, ...)
breakpoints BOOLEAN
```

The map holds no `value`, no value of a varying dim and no `breakpoint`, so its size is independent of the series data and of the length of a curve.
It holds no depth either: the fold settles ownership across layers, so a read needs none.

The key derives from the schema: it is [`partial_dims`](tree.md#partial-the-granularity-of-an-override) plus `attribute`.
A coordinate that an attribute's own file lacks reads as NULL, so the key is one fixed tuple across attributes whose columns differ.
A [multi-dim](../model/multi-dims.md) is one column, so the map itself shows which columns name a multi-dim's tuple.
A multi-dim outside `partial` is in no key, as any dim outside it is in none.
A layer that touches one connection's value restates the attribute's whole extent along that multi-dim.
An attribute over such a multi-dim and no other `partial` coordinate has the key `(attribute)` alone, the grain of a record-level weighting.

The map holds one row per layer and per distinct key the layer wrote, a NULL key column left NULL, and the overlap rows of [crossing rows](#crossing-rows).
The fold builds it along the path from the root: the parent's map minus removals and the rows the layer covers, union the layer's own keys and its overlap rows.
A revision whose caches are [materialised](#materialised-node-caches) persists the map and pointers to the layers holding its resolved axes.
A read then needs the ancestry only back to the nearest materialised revision, which is what lets a deep tree scale.

The fold applies removals to every key column.
`fold_attributes` drops the parent rows that name a label of a partial dim, or a tuple of a partial multi-dim, which the layer's restatement omits and the previous restatement held ([removing labels](removal.md)).
A multi-dim's removed tuples are one NULL-safe anti-join on its struct column, its file's own key, which is the shape every dim's removal takes.
A layer's removals apply to its parents' keys.
Its own rows pass through.
A plain dim's anti-join matches its top-level column only.
A label inside a struct is reached through its tuple, since [removing a label](removal.md) restates each multi-dim without the tuples naming it in the same layer.
A row that leaves a key column NULL is not addressed by it, and the NULL-safe anti-join keeps it.
Only a row that names a removed label or tuple is dropped.

### Crossing rows

A row names a `partial` dim where its key holds a label, and any other dim where it varies along it, so a series over `snapshot` names `snapshot`.
Only the dims the attribute uses count: its `dims` and the coordinates' dims of its multi-dims.
Two rows cover a common key where, on each `partial` coordinate, they hold the same label or one of them holds NULL.
They cross where they cover a common key and neither names every dim the other names.

A layer folds onto its parent's map in four steps, none of which reads an axis:

1. The parent rows naming a label or tuple that the layer removes are dropped, and a row NULL there never is.
2. A parent row is dropped where a row of the layer covers it and names no `partial` dim the parent row leaves NULL.
3. For each parent row and row of the layer that cross on `partial` dims, the layer gains an overlap row naming every label either names.
   The crossed parent row stays as written, as does a parent row naming strictly fewer dims than a row of the layer covering it.
4. The layer's rows and its overlap rows are added.

An overlap row carries the owning layer's `varies` and `broadcast`, so the layer's value row finds it by the same NULL-safe join as its own key and reads the overlap row's labels.
To a later fold it is an ordinary row, which that fold drops, crosses or removes like any other.
The pairs are found by one equi-join per pair of named sets, over the dims both name, so no join compares every row with every other.

### Why the map holds keys as written

Two rules decide which row gives a value.
Within one layer, the more specific row wins ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).
Across layers, the deeper layer wins.
The two disagree only where rows of two layers cross.
There an overlap row, owned by the deeper layer and naming the labels of both, makes them agree.
Everywhere else the rows stay as written, and the broadcast rule alone gives every answer.

Take `partial = {entity, scenario}`, a root layer that sets `p_nom` per entity, and a child that sets it for one scenario:

```text
root layer                     child layer
entity  scenario  value        entity  scenario  value
gas     NULL       50          NULL    high      999
wind    NULL      100
```

At `(wind, high)` the root's row names `entity` and the child's row names `scenario`, so neither row is more specific.
The child is deeper, so 999 wins there, while `(wind, low)` keeps the root's 100.
The fold adds an overlap row for each crossing pair:

```text
# owner map                          # attributes["p_nom"]
entity  scenario  attribute  layer   entity  scenario  value
gas     NULL      p_nom      root    gas     NULL       50
wind    NULL      p_nom      root    wind    NULL      100
NULL    high      p_nom      child   NULL    high      999
gas     high      p_nom      child   gas     high      999   -- overlap rows
wind    high      p_nom      child   wind    high      999
```

At `(wind, high)` the overlap row names every dim the other two name, so the broadcast rule gives 999.
A child writing `(gas, NULL) = 60` instead names no more than the root's `(gas, NULL)`, and step 2 drops the root's row.
A child writing `(gas, high) = 70` names more and leaves the root's rows as written, and the broadcast rule gives `(gas, high)` to the child.

The map grows with the rows layers write and the pairs that cross, not with the labels a broadcast reaches.
A depth per row would let the read settle ownership instead, but every read would then pay what the fold pays once, and a consumer would have to know about layers.

### What a row reaches

A NULL on a `partial` coordinate reaches every label of that coordinate's axis, or every tuple of its multi-dim, as resolved at the node read.
The fold reads no axis and splits no row, so a label added below a materialised node is reached, as is a label staged in a `WorkingRecord` and the first label of an axis that was empty when the broadcast was written.
Adding the scenario `mid` below the child above reads `(gas, mid)` as 50.

A row reaches every coordinate that no more specific row covers, whichever layer wrote it.
A deeper layer thus owns only the coordinates its rows reach.
With `partial = {entity, scenario}` and `x` also over `snapshot`, a root writing `(gas, NULL, 1) = 1.0` and a child writing `(NULL, high, 0) = 2.0`, `(gas, high, 1)` reads 1.0 from the root.

A label removed and added again takes every broadcast covering it, written before or after the removal, and no row that named it below the removal ([what a removal reaches](removal.md#what-a-removal-reaches)).

### A dim outside `partial`

An overlap row cannot settle a crossing on a dim outside `partial`, since the key has no column to name its labels.
A [`WorkingRecord`](../use/editing.md#edits) settles it as it stages: a staged row leaving such a dim NULL, over a base row naming it, is also written out along the base row's labels there, with the staged value.
With `partial = {entity}`, a root writing a series over `snapshot` for every entity and a child setting `wind` to 0.9:

```text
root:             (NULL, 0) = 0.2   (NULL, 1) = 0.5
child as staged:  (wind, NULL) = 0.9
child as written: (wind, NULL) = 0.9   (wind, 0) = 0.9   (wind, 1) = 0.9
```

The child's rows at `wind` name `entity` and `snapshot`, a strict superset of the root's, so nothing crosses and the root's series stays as written.
The fold refuses a layer written otherwise that leaves such a crossing, naming the layer, the attribute, the dim and the key.

### Flags

The fold computes the [flags](../use/reading.md#flags) of each map row in the group-by that builds the layer's rows, so they cost nothing beyond it.
It computes them per key, so per entity: whether this entity's `p_max_pu` sets `timestep` differs from whether any entity's does.
An overlap row carries the flags of the layer's row it comes from.
`flags()` aggregates the map's rows per attribute, `bool_or` over each field of `varies` and `broadcast` and over `breakpoints`.
It describes the rows the map keeps, so a broadcast that deeper rows shadow at every label still counts.

Each struct has a field per coordinate: every declared dim, multi-dims included.
A dim that a multi-dim draws on has its own field, so a `marginal_price` constant over buses reports `bus` as broadcast.

The flags are two structs because [which dims exist is declared](../model/dims.md).
A column per dim would make the map's column set depend on the schema.
[Versioning](../model/schema.md#versioning) calls adding a dim compatible, and that has to hold for a map persisted at a [materialised revision](#materialised-node-caches) too.
In a struct, the new dim is a missing field, which `UNION ALL BY NAME` fills with NULL as it would a missing column.
The new dim then reads as unset, which is true, since no row mentions it.
The map's columns stay fixed, and only the fields change.

`breakpoints` sits outside both structs, since a breakpoint is no dim ([wide and long rows](../model/attributes.md#wide-and-long-rows)).
The dim namespace lives entirely inside `varies` and `broadcast`, so a dim named `breakpoints` collides with nothing.

`Record.flags()` unions the flags over the record.
`flags(**categories)` unions them over the rows reaching the labels of those categories, a row NULL on the coordinate reaching every label.
[Reading](../use/reading.md#flags) says what the union means to a consumer.

## One fold for every axis

Every dim's axis is one construct: a keyed relation that a layer restates whole.
A plain dim's key is the column named for it, and a multi-dim's key is its one struct column.
Every axis resolves by one path.
A revision's resolved relation for an axis is the deepest file for it on the path from the root.
Each row carries its properties, such as `ctype` on the entity axis and `role` on `connection`, and no `layer_uuid`.
The fold refuses a file that holds a key twice, or that carries a column other than its key and its subject's declared properties.

The resolved relation comes in member order, which is the deepest file's row order.
A revision's caches persist it in that order, so a reader recovers member order from the resolved file's own row number.
A key that one layer removes and a later layer adds again is a row of the later layer's file, and sits where that file puts it.
Member order is the file's row order and is no stored column.
A consumer that wants positional ports numbers an entity's connections by this order.
So the writer refuses a restatement that orders its parent's labels differently, naming the first label out of place, which prevents the [positional-keying failure](tree.md#partial-the-granularity-of-an-override).
A new label may sit anywhere among its parent's.
A [`WorkingRecord`](../use/editing.md#staging) appends one, so a layer it writes renumbers no existing port.
The order holds across a materialised parent, whose resolved file is a copy of the deepest layer file at or below it, read in its own row order.

Every property is a column of its subject's file, so the resolved axis holds everything a per-category read needs.
One category's labels are `properties[dim]` filtered on its categorizing columns.

Over the unmaterialised tail of an ancestry, the fold runs live and caches its results in the workspace's database as [shared tables](../storage/concurrency.md#a-cursor-per-thread).
[Layers are write-once](tree.md#a-layers-data-is-write-once), so such a cache never needs invalidating.

## Resolving a relation

A resolved relation joins the owning layers' files to the owner map and keeps only owned rows:

```sql
SELECT u.timestep,
       COALESCE(u.scenario, o.__owned_scenario) AS scenario,   -- one per owned_per coordinate, entity likewise
       u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/attributes/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN ( -- the map keyed as its owning layer stored each key
  SELECT * EXCLUDE (scenario), scenario AS __owned_scenario,
         UNNEST(list_concat(
           CASE WHEN COALESCE(varies.scenario, false)   THEN [scenario] END,
           CASE WHEN COALESCE(broadcast.scenario, true) THEN [NULL] END
         )) AS scenario
  FROM owner_map
) o
  ON o.attribute  = u.attribute
 AND o.layer_uuid = u.layer_uuid
 AND o.scenario   IS NOT DISTINCT FROM u.scenario  -- owned-per coordinates: as stored, NULL included
```

The query projects the attribute's own coordinates ([the long schema](../storage/format.md#the-long-schema)).
That is `connection` for an attribute over that multi-dim, `entity` for a per-entity one, and none of them for a record-level weighting.
The join conditions follow from the key columns the attribute's file carries, so the schema derives the query's shape.
A coordinate outside the key, here `timestep` or a multi-dim outside `partial`, is in no join condition and passes straight through.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
A read runs no `MAX` or group-by of its own.
It returns the owned rows as written: a key NULL on a dim stays NULL, an overlap row reads its labels, and no row is expanded or dropped by depth.
The row order is unspecified.
A consumer settles each coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule) over the full coordinates, `partial` or not, and needs neither depth nor the list of `partial` dims.
Wherever two rows cross, the fold or the staging has written a row naming both, so the rule finds one winner wherever a row reaches.
Its only filter of removed labels covers what the map cannot see ([below](#a-label-outside-the-key)).

The join condition for each owned-per coordinate is NULL-aware.
A stored NULL means every value.
The row joins its own map row, where it stays NULL, and each overlap row of its layer that names its key, where it takes that row's label in the output.
The plain condition `u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario` is no hash key.
DuckDB would join on the other columns and filter the dim afterwards, which costs the square of the dim's length per key.

So the map is keyed as its owning layer stored each key, which its [flags](#flags) record.
A map row keeps its value where the layer's row set the dim (`varies`), and holds NULL where the row left it NULL (`broadcast`), with the owned value carried beside it.
A layer row then finds its owned keys by its stored values, NULLs included, on NULL-safe equality.
That is one hash join, which scans the layers and the map once each.
A flag field missing from a map folded before the dim was declared counts as `broadcast` here, since the layers' rows hold NULL there.
The flags a consumer reads count it as unset.

An overlap row reads its labels, so a layer that also wrote a row naming those labels answers that coordinate twice.
Where a layer holds rows of an attribute naming different sets of `partial` dims, the read keeps at each coordinate the row that names every `partial` dim another row there names, [the more specific one](../model/attributes.md#the-broadcast-rule), through a window over the attribute's coordinates.
A coordinate with no such row, or with two at one breakpoint, fails the read, naming the attribute and the coordinate.
`breakpoint` is no coordinate, so the window keeps or drops a curve whole.
An attribute whose layers each name one set of dims, the common case, skips the window.

A staged layer has not met [the write's refusal](../use/writing.md#shape) of two rows neither more specific.
So the read of a [`WorkingRecord`](../use/editing.md#reading-with-pending-edits) fails where two staged rows cross and no staged row names both, naming the attribute and the coordinate.

A `partial` multi-dim column such as `connection` joins NULL-aware exactly as `scenario` does, and a NULL there [reaches](../model/attributes.md#the-broadcast-rule) every tuple of the resolved multi-dim.
DuckDB hash-joins `IS NOT DISTINCT FROM` on a `STRUCT` key directly.
A dim the multi-dim draws on, such as `bus` beside it on a per-bus attribute, is a coordinate of its own and joins NULL-aware where it is `partial`.

The query projects `breakpoint` and does not join on it, since it is no part of the key.
A curve is owned whole ([wide and long rows](../model/attributes.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.
Dims outside the key pass through unchanged, because the rows of one key come from one layer.

An attribute that no layer wrote is absent from the map.
Its relation is empty, and the consumer applies [the schema's `default`](../model/attributes.md#attributespec).

### A label outside the key

The fold prunes removed labels from [every key coordinate](#owner-map), so a row whose label of a partial dim or tuple of a partial multi-dim was removed is already gone from the map.
A dim outside `partial`, multi or not, is in no key, so the fold cannot see its labels.
With `connection` owned whole, a connection attribute's map row is `(attribute)` alone and owns every tuple's value.

The read drops those rows instead.
It anti-joins each such coordinate against its dead labels: the labels that some layer [removed](removal.md) and the final axis lacks.
A multi-dim's dead labels are its dead tuples, so a removed tuple's values go whichever way the multi-dim is keyed.
The fold computes dead labels for each coordinate outside the owner-map key, and for each dim that a multi-dim's coordinate draws on or a property refers to.
They are the base's dead set, the base's resolved axis and every restatement above the base, minus the final axis.
The fold keeps them beside the resolved axes.
A [materialised revision](#materialised-node-caches) caches them, since its resolved relations hold only live labels and a read above it would otherwise miss a label removed below.
The set is usually empty, and the join against it cheap.

## One record over one fold

There is one `Record`, and it is the fold itself.
Its mappings hand over the folded axes, multi-dims included, and each attribute as the [owner map](#owner-map) resolves it.
`Workspace.at(uri)` folds over a single [`DirectorySource`](../storage/format.md).
Over one source the fold is a scan of it: every key is owned by the one layer, and the anti-join has nothing to evict.

A directory therefore has the same properties as a revision:

- `flags` come from the ownership group-by in the same scan.
  They need a real aggregate in any case, since parquet's footer statistics are per row group.
  A file that mixes one label's series rows with another label's constant says nothing about either in its footer.
- Member order is the file's row order, the general rule over one source.
- `schema.partial` sets the granularity of a patch, and one layer patches nothing, so `partial` is inert there.

The fold requires a layer layout, and a record directory is one.
A directory copied out of any tree, with no revision file, reads identically.

## Materialised node caches

A revision's owner map, the layers holding its resolved axes and its dead labels may be materialised under `layers/<id>/resolved/`.
The schema is not, since there is [one for the whole record](../model/schema.md#one-schema-per-record) and nothing per revision to resolve.

Where a materialised map exists, a read stops there.
The map is already folded over everything above it, so a read walks the ancestry back to the nearest materialised revision and no further.
That truncation keeps a deep chain cheap.
The same rule decides where resolved axes come from: the layers a revision's own `resolved/dim_owners.parquet` names where materialised, and its raw `dims/` otherwise.
The presence of the cache answers this, and no recorded state is involved.
A map whose key columns are not those the schema's [`partial`](tree.md#partial-the-granularity-of-an-override) gives is no cache: one persisted before a dim joined `partial` holds NULL there for a row varying along it, so the read folds from the layers as if nothing were materialised.
The two are distinct paths within one directory, so a record read as an ancestor and the same record read as itself never alias.
`resolved/dim_owners.parquet` holds a row `(dim, layer_uuid)` per axis, a multi-dim's included: the layer whose `dims/<dim>.parquet` is the deepest at or below the revision. A read of an axis stops there too, and opens that layer's file. A dim it does not name has no axis. A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) is never named, its axis being the schema's list.
The file points into ancestor layers by UUID, as the owner map does, rather than copying their files. A layer is write-once, so the pointer never goes stale, and on object storage it is one read where a directory of copies is a listing.

Beside them, `resolved/dead/<coord>.parquet` holds the [dead labels](#a-label-outside-the-key) of each coordinate outside the owner-map key, and of each dim that a multi-dim's coordinate draws on or a property refers to.
It is written only where there are any.
A read above the revision seeds its dead labels from these files, since the axis files hold only live labels.
`resolved/dead/` and `owner_map.parquet` are folds, since no layer file holds either.

Materialising is a policy: every N layers, at a branch point, or on demand.
It only adds files under `resolved/`, so it changes no answer, only how many layers a read touches.
The persisted map is the fold's result, and a NULL in it reaches the labels of the node read, so a label added below the node is reached as it would be without the cache.
