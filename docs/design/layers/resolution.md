<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Resolution

The owner map records, for each override key, which layer owns it, with removals applied.
For axes and groups, the deepest file for each one is the answer.
Reading an attribute joins the owning layers' files to the map with one hash join.
The read then drops rows that name a removed label outside the key.
A single directory folds as a tree with one layer.
A revision can materialise its owner map and resolved axes under `resolved/`, and a read then stops at that revision.

## Owner map

The owner map is one relation over every attribute.
An attribute's winning key and its winning value live in different files, since its ownership spans every layer's `attributes/<attr>.parquet`.
The map names the owning layer per key, and a read then goes to that layer's file for the value.
Every axis and group is a single keyed file that a layer restates whole, so it needs no map and resolves to a [resolved relation](#one-fold-for-every-axis).

The map's columns, keys first:

```text
# owner map
<partial dims>          -- the fold key: exactly the partial dims and groups
  <owned_per coordinates> -- e.g. entity, scenario; a partial group as one STRUCT: connection STRUCT(entity, bus)
attribute
layer_uuid              -- the owning layer
varies      STRUCT(<coordinate>: BOOLEAN, ...)   -- a field per dim and group
broadcast   STRUCT(<coordinate>: BOOLEAN, ...)
breakpoints BOOLEAN
```

The map holds no `value`, no value of a varying dim and no `breakpoint`, so its size is independent of the series data and of the length of a curve.

The key derives from the schema: it is [`partial_dims`](tree.md#partial-the-granularity-of-an-override) plus `attribute`.
A coordinate that an attribute's own file lacks reads as NULL, so the key is one fixed tuple across attributes whose columns differ.
A group is one column, so the map itself shows which columns name a group's tuple.
A group outside `partial` is in no key, as a dim outside it is in none.
A layer that touches one connection's value owns the attribute's whole extent along that group.
An attribute over such a group and no other `partial` coordinate has the key `(attribute)` alone, the grain of a record-level weighting.

The fold builds the map along the path from the root: the parent's map minus removals and overrides, union the layer's own keys.
A revision whose caches are [materialised](#materialised-node-caches) persists the map and its resolved axes.
A read then needs the ancestry only back to the nearest materialised revision, which is what lets a deep tree scale.

The fold applies removals to every key column.
`fold_attributes` drops the parent rows that name a label of a partial dim, or a tuple of a partial group, which the layer's restatement omits and the previous restatement held ([removing labels](removal.md)).
A group's removed tuples are one NULL-safe anti-join on its struct column, the group file's own key, which is the shape a dim's removal takes too.
A layer's removals apply to its parents' keys.
Its own rows pass through.
A dim's anti-join matches its top-level column only.
A label inside a struct is reached through its tuple, since [removing a label](removal.md) restates each group without the tuples naming it in the same layer.
A row that leaves a key column NULL is not addressed by it, and the NULL-safe anti-join keeps it.
Only a row that names a removed label or tuple is dropped.

The fold expands a NULL against the whole axis, so `set("p_max_pu", 0.9)` reaches every entity, loads included ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).
A value scoped to generators is written per label, `set("p_max_pu", 0.9, ctype="Generator")`.
A materialised ancestor's map is seeded already expanded, so its NULL rows do not reach labels a descendant adds ([open](../open-questions.md)).

### Why the map holds one row per label

Two rules decide which row gives a value.
Within one layer, the more specific row wins ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).
Across layers, the deeper layer wins, however specific its row is.
With two `partial` dims, the two rules can disagree, so the fold settles ownership per label.

Take `partial = {entity, scenario}`, a root layer that sets `p_nom` per entity, and a child that sets it for one scenario:

```text
root layer                     child layer
entity  scenario  value        entity  scenario  value
gas     NULL       50          NULL    high      999
wind    NULL      100
```

At `(wind, high)` the root's row names `entity` and the child's row names `scenario`, so neither row is more specific.
The child is deeper, so 999 wins there, while `(wind, low)` keeps the root's 100.
No set of rows with NULLs states that answer without the depth of each row.
The fold therefore expands each NULL along the `partial` dims and keeps the deepest layer per label:

```text
# owner map                          # attributes["p_nom"]
entity  scenario  attribute  layer   entity  scenario  value
gas     high      p_nom      child   gas     high      999
gas     low       p_nom      root    gas     low        50
wind    high      p_nom      child   wind    high      999
wind    low       p_nom      root    wind    low       100
```

The map grows with the product of the `partial` labels each attribute reaches.
A dim outside `partial`, such as `snapshot`, adds no row, and the read leaves its NULLs for the consumer ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).

### Flags

The fold computes the [flags](../use/reading.md#flags) in the same group-by that settles ownership, so they cost nothing beyond it.
It computes them per key, so per entity: whether this entity's `p_max_pu` sets `timestep` differs from whether any entity's does.

Each struct has a field per coordinate: every declared dim and every group.
A dim that a group draws on has its own field, so a `marginal_price` constant over buses reports `bus` as broadcast.

The flags are two structs because [which dims exist is declared](../model/dims.md).
A column per dim would make the map's column set depend on the schema.
[Versioning](../model/schema.md#versioning) calls adding a dim compatible, and that has to hold for a map persisted at a [materialised revision](#materialised-node-caches) too.
In a struct, the new dim is a missing field, which `UNION ALL BY NAME` fills with NULL as it would a missing column.
The new dim then reads as unset, which is true, since no row mentions it.
The map's columns stay fixed, and only the fields change.

`breakpoints` sits outside both structs, since a breakpoint is no dim ([wide and long rows](../model/attributes.md#wide-and-long-rows)).
The dim namespace lives entirely inside `varies` and `broadcast`, so a dim named `breakpoints` collides with nothing.

`Record.flags()` unions the flags over the record.
`flags(**categories)` unions them over the labels of those categories, restricted to what those labels carry.
[Reading](../use/reading.md#flags) says what the union means to a consumer.

## One fold for every axis

A dim's axis and a group are one construct: a keyed relation that a layer restates whole.
A dim's key is the column named for it, and a group's key is its one struct column.
They resolve by one path.
A revision's resolved relation for an axis or group is the deepest file for it on the path from the root.
Each row carries its properties, such as `ctype` on the entity axis and `role` on a group, and no `layer_uuid`.
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

Every property is a column of its subject's file, so the resolved axis or group holds everything a per-category read needs.
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
That is `connection` for an attribute over that group, `entity` for a per-entity one, and none of them for a record-level weighting.
The join conditions follow from the key columns the attribute's file carries, so the schema derives the query's shape.
A coordinate outside the key, here `timestep` or a group outside `partial`, is in no join condition and passes straight through.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
A read runs no `MAX` or group-by of its own.
Its only filter of removed labels covers what the map cannot see ([below](#a-label-outside-the-key)).

The join condition for each owned-per coordinate is NULL-aware.
A stored NULL means every value, and the map may assign it to the layer for only some of them.
The row then joins every map entry that names its layer, and takes that entry's value in the output.
The plain condition `u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario` is no hash key.
DuckDB would join on the other columns and filter the dim afterwards, which costs the square of the dim's length per key.

So the map is keyed as its owning layer stored each key, which its [flags](#flags) record.
A map row keeps its value where the layer's row set the dim (`varies`), and holds NULL where the row left it NULL (`broadcast`), with the owned value carried beside it.
A layer row then finds its owned keys by its stored values, NULLs included, on NULL-safe equality.
That is one hash join, which scans the layers and the map once each.
The keyed map holds one row per owned key, and two for a key its layer wrote both ways.
A map row NULL in the dim itself, broadcast over an empty axis, has `varies` false, so only its NULL form exists and it matches once.
A flag field missing from a map folded before the dim was declared counts as `broadcast` here, since the layers' rows hold NULL there.
The flags a consumer reads count it as unset.

A layer may hold a row that names a label beside one that leaves its dim NULL.
Both join the key the label names, and the map records that key as written both ways.
Where an attribute has such a key, the read keeps at each coordinate the row that names every `partial` dim another row there names, [the more specific one](../model/attributes.md#the-broadcast-rule), through a window over the attribute's coordinates.
A coordinate with no such row, or with two at one breakpoint, fails the read, naming the attribute and the coordinate.
`breakpoint` is no coordinate, so the window keeps or drops a curve whole.
An attribute with no such key, the common case, skips the window.

A `partial` group column such as `connection` joins NULL-aware exactly as `scenario` does, and [broadcasts](../model/attributes.md#the-broadcast-rule) over the resolved group.
DuckDB hash-joins `IS NOT DISTINCT FROM` on a `STRUCT` key directly.
A dim the group draws on, such as `bus` beside it on a per-bus attribute, is a coordinate of its own and joins NULL-aware where it is `partial`.

The query projects `breakpoint` and does not join on it, since it is no part of the key.
A curve is owned whole ([wide and long rows](../model/attributes.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.
Dims outside the key pass through unchanged, because the rows of one key come from one layer.

An attribute that no layer wrote is absent from the map.
Its relation is empty, and the consumer applies [the schema's `default`](../model/attributes.md#attributespec).

### A label outside the key

The fold prunes removed labels from [every key coordinate](#owner-map), so a row whose label of a partial dim or tuple of a partial group was removed is already gone from the map.
A dim or group outside `partial` is in no key, so the fold cannot see its labels.
With `connection` owned whole, a connection attribute's map row is `(attribute)` alone and owns every tuple's value.

The read drops those rows instead.
It anti-joins each such coordinate against its dead labels: the labels that some layer [removed](removal.md) and the final axis lacks.
A group's dead labels are its dead tuples, so a removed tuple's values go whichever way the group is keyed.
The fold computes dead labels for each coordinate outside the owner-map key, and for each dim that a group's field ranges over or a property refers to.
They are the base's dead set, the base's resolved axis and every restatement above the base, minus the final axis.
The fold keeps them beside the resolved axes and groups.
A [materialised revision](#materialised-node-caches) caches them, since its resolved relations hold only live labels and a read above it would otherwise miss a label removed below.
The set is usually empty, and the join against it cheap.

## One record over one fold

There is one `Record`, and it is the fold itself.
Its mappings hand over the folded axes and groups, and each attribute as the [owner map](#owner-map) resolves it.
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

A revision's owner map and resolved axes and groups may be materialised under `layers/<id>/resolved/`.
The schema is not, since there is [one for the whole record](../model/schema.md#one-schema-per-record) and nothing per revision to resolve.

Where a materialised map exists, a read stops there.
The map is already folded over everything above it, so a read walks the ancestry back to the nearest materialised revision and no further.
That truncation keeps a deep chain cheap.
The same rule decides where resolved axes come from: a revision's own `resolved/dims/` where materialised, and its raw `dims/` otherwise.
The presence of the cache answers this, and no recorded state is involved.
The two are distinct paths within one directory, so a record read as an ancestor and the same record read as itself never alias.
`resolved/dims/` and `resolved/groups/` hold copies of the deepest layer file for each axis and group at or below the revision, so a read of an axis stops there too.

Beside them, `resolved/dead/<coord>.parquet` holds the [dead labels](#a-label-outside-the-key) of each coordinate outside the owner-map key, and of each dim that a group's field ranges over or a property refers to.
It is written only where there are any.
A read above the revision seeds its dead labels from these files, since the copied axis files hold only live labels.
`resolved/dead/` and `owner_map.parquet` are folds, since no layer file holds either.

Materialising is a policy: every N layers, at a branch point, or on demand.
It only adds files under `resolved/`, so it changes no answer, only how many layers a read touches.
