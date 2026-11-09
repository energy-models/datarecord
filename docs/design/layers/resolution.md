<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Resolution

A revision reads the layers on its path from the root, and each layer patches the one below it ([a tree of layers](tree.md)).
Resolution decides which layer's row gives each value a read returns.
Two rules decide it.
Within one layer, the more specific row wins ([the broadcast rule](../model/attributes.md#the-broadcast-rule)).
Across layers, the deeper layer wins.
The value itself stays in the owning layer's `attributes/<attr>.parquet`, so resolution names an owner per key and the read fetches the value from there.

The base example runs through this page.
Take `partial = {entity, scenario}`, a root layer that sets `p_nom` per entity, and a child that sets it for one scenario:

```text
root layer                     child layer
entity  scenario  value        entity  scenario  value
gas     NULL       50          NULL    high      999
wind    NULL      100
```

At `(wind, high)` the root's row names `entity` and the child's row names `scenario`, so neither row is more specific.
The child is deeper, so 999 wins there, while `(wind, low)` keeps the root's 100.

<figure class="diagram">
--8<-- "docs/static/resolution-overview.svg"
</figure>

Resolution has two parts, a fold and a read.
The fold builds the [owner map](#owner-map), which records each layer's attribute keys as the layer wrote them, under its own schema, with removals applied.
Where a deeper layer's row [crosses](#crossing-rows) a parent row, as the child's `(NULL, high)` crosses the root's `(wind, NULL)`, the map adds one overlap row, owned by the deeper layer.
Reading an attribute joins the owning layers' files to the map with one hash join and returns the owned rows as written.
The read then drops rows that name a removed label outside the key.
A consumer settles each coordinate by the broadcast rule.
A revision can materialise its owner map, and pointers to the layers holding its resolved axes, under `resolved/`, and a read then stops at that revision.
A fold the workspace holds as shared tables lets a later fold start partway too ([the nearest fold at hand](#a-fold-starts-from-the-nearest-fold-at-hand)).

## Folding dimensions {#one-fold-for-every-axis}

Axes are the easy case of the fold.
A layer never patches an axis label by label, so an axis needs no owner map: it resolves to one layer's file.

<figure class="diagram">
--8<-- "docs/static/resolution-axes.svg"
</figure>

### Restatement {#axis-restatement}

A layer that touches an axis restates it whole: every label, with its properties, in order, whatever `partial` says ([`partial`](tree.md#partial-the-granularity-of-an-override)).
A layer that holds no file for a dim leaves its parent's axis as it is.
So a revision's resolved axis is the deepest file for the dim on the path from the root.
In the figure, the child's entity file replaces the root's, and the scenario axis stays the root's.

A plain dim's file is keyed by the column named for it, and a multi-dim's by its one struct column, so both fold the same way.
A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) has no file: its axis is the schema's list.
The fold refuses a file that holds a key twice, or that carries a column other than its key and its subject's declared properties.
[Removing labels](removal.md) is a restatement without them, and the fold keeps the labels a restatement drops as [dead labels](#a-label-outside-the-key).

??? note "Why not patch an axis label by label?"

    An axis is small next to the attributes over it, so restating it costs little.
    A patched axis would need its own owner map, and its member order would have no single file to come from.

### Member order {#member-order}

A resolved axis comes in member order, the deepest file's row order, which is no stored column.
A materialised parent keeps it, since its axis is that same file.
A key that one layer removes and a later layer adds again is a row of the later layer's file, and sits where that file puts it.

A consumer that wants positional ports numbers an entity's connections by this order.
So the writer refuses a restatement that orders its parent's labels differently, naming the first label out of place, which prevents the [positional-keying failure](tree.md#partial-the-granularity-of-an-override).
A new label may sit anywhere among its parent's, and a [`WorkingRecord`](../use/editing.md#staging) appends one, so a layer it writes renumbers no existing port.

### Properties {#axis-properties}

A property is a column of its subject's axis file, such as `ctype` on the entity axis and `role` on `connection`.
So a property changes only with a restatement of its axis, and its rows carry no `layer_uuid`.
The resolved axis holds everything a per-category read needs: one category's labels are `properties[dim]` filtered on its categorizing columns.

## Owner map

An attribute is not restated whole, so the deepest file is no answer for it.
Its winning key and its winning value live in different files, since its ownership spans every layer's `attributes/<attr>.parquet`.
The owner map is one relation over every attribute that names the owning layer per key, and a read then goes to that layer's file for the value.

The map holds no `value`, no value of a varying dim and no `breakpoint`, so its size is independent of the series data and of the length of a curve.
It holds no depth either: the fold settles ownership across layers, so a read needs none.

### Why the map holds keys as written

The rule within a layer and the rule across layers disagree only where rows of two layers cross.
There an overlap row, owned by the deeper layer and naming the labels of both, makes them agree.
Everywhere else the rows stay as written, and the broadcast rule alone gives every answer.
In the base example, the map keeps the root's `(gas, NULL)` and `(wind, NULL)` and the child's `(NULL, high)` as written, and adds the overlap rows `(gas, high)` and `(wind, high)`, owned by the child ([crossing rows](#crossing-rows)).
The map grows with the rows layers write and the pairs that cross, not with the labels a broadcast reaches.

??? note "Why not a depth per row?"

    A depth per row would let the read settle ownership instead, but every read would then pay what the fold pays once, and a consumer would have to know about layers.

??? note "Why not expand a broadcast to the labels it reaches?"

    The map would then grow with the labels a broadcast reaches rather than with the rows layers write.
    A key held as written keeps its NULL, which reaches every label of the axis as resolved at the node read.
    So a label added below a materialised node, or staged in a `WorkingRecord`, is reached ([what a row reaches](#what-a-row-reaches)).

### The key and its rows {#the-key-and-its-rows}

The key derives from the schema: it is [`partial_dims`](tree.md#partial-the-granularity-of-an-override) plus `attribute`.
A layer's rows are keyed by the schema in its own manifest, and a node's map by the node's, which the fold [extends the map to](#extending-the-map-to-a-wider-schema) where a deeper layer's is wider.
A coordinate that an attribute's own file lacks reads as NULL, so the key is one fixed tuple across attributes whose columns differ.
A [multi-dim](../model/multi-dims.md) is one column, so the map itself shows which columns name a multi-dim's tuple.
A multi-dim outside `partial` is in no key, as any dim outside it is in none.
A layer that touches one connection's value restates the attribute's whole extent along that multi-dim.
An attribute over such a multi-dim and no other `partial` coordinate has the key `(attribute)` alone, the grain of a record-level weighting.

The map holds one row per layer and per distinct key the layer wrote, a NULL key column left NULL, and the overlap rows of [crossing rows](#crossing-rows).

### Columns {#owner-map-columns}

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

## Folding a layer onto its parent {#folding-a-layer-onto-its-parent}

The fold builds the map along the path from [the nearest fold at hand](#a-fold-starts-from-the-nearest-fold-at-hand), one layer at a time, in this order: the parent's map, [extended](#extending-the-map-to-a-wider-schema) to the layer's schema, minus [removals](#removed-labels) and the [rows the layer covers](#covered-rows), union the layer's own keys and its [overlap rows](#crossing-rows).

Three terms describe how the rows of two layers meet.

_names_
: A row names a `partial` dim where its key holds a label, and any other dim where it varies along it, so a series over `snapshot` names `snapshot`.
Only the dims the attribute uses count: its `dims` and the coordinates' dims of its multi-dims.

[_covers_](#covered-rows)
: Two rows cover a common key where, on each `partial` coordinate, they hold the same label or one of them holds NULL.

[_crosses_](#crossing-rows)
: Two rows cross where they cover a common key and neither names every dim the other names.

The extended example adds to the base example: the root also sets `(coal, NULL) = 80`, and the child, staged by a `WorkingRecord`, removes `coal`, sets `(gas, NULL) = 60` and then `(NULL, high) = 999`.
The second [`set`](../use/editing.md#set) crosses the first, so it also stages `(gas, high) = 999`, the row naming both.
Both layers share one schema, so nothing is extended.

<figure class="diagram">
--8<-- "docs/static/resolution-fold.svg"
</figure>

The root's `(coal, NULL)` names the removed label and is pruned.
The child's `(gas, NULL)` covers the root's and names nothing more, so the root's row is dropped.
The root's `(wind, NULL)` crosses the child's `(NULL, high)`, so the child gains the overlap row `(wind, high)`.
The child's three rows join the map as written.

### Removed labels {#removed-labels}

First, the fold applies the layer's removals to every key column of the parent's map.
`fold_layer` drops the parent rows that name a label of a partial dim, or a tuple of a partial multi-dim, which the layer's restatement omits and the previous restatement held ([removing labels](removal.md)).
A layer's removals apply to its parents' keys.
Its own rows pass through.
A row that leaves a key column NULL is not addressed by it, and the NULL-safe anti-join keeps it.
Only a row that names a removed label or tuple is dropped.

A multi-dim's removed tuples are one NULL-safe anti-join on its struct column, its file's own key, which is the shape every dim's removal takes.
A plain dim's anti-join matches its top-level column only.
A label inside a struct is reached through its tuple, since [removing a label](removal.md) restates each multi-dim without the tuples naming it in the same layer.

### Covered rows {#covered-rows}

Next, a parent row is dropped where a row of the layer covers it and names no `partial` dim the parent row leaves NULL.
In the extended example, the child's `(gas, NULL) = 60` names no more than the root's `(gas, NULL)`, and the root's row is dropped.
A child writing only `(gas, high) = 70` names more and leaves the root's rows as written, and the broadcast rule gives `(gas, high)` to the child.
So a parent row naming strictly fewer dims than a row of the layer covering it stays as written.

### Crossing rows

Where a parent row and a row of the layer cross on `partial` dims, neither is more specific, and the broadcast rule alone finds no winner where both reach.
So, for each such pair, the layer gains an overlap row naming every label either names.
The crossed parent row stays as written.
Last, the layer's rows and its overlap rows are added.
In the base example, the child's `(NULL, high)` crosses both of the root's rows:

<figure class="diagram">
--8<-- "docs/static/resolution-crossing.svg"
</figure>

At `(wind, high)` the overlap row names every dim the other two name, so the broadcast rule gives 999.

An overlap row carries the owning layer's `varies` and `broadcast`, so the layer's value row finds it by the same NULL-safe join as its own key and reads the overlap row's labels.
To a later fold it is an ordinary row, which that fold drops, crosses or removes like any other.

### What a row reaches

A NULL on a `partial` coordinate reaches every label of that coordinate's axis, or every tuple of its multi-dim, as resolved at the node read.
The fold reads no axis, and splits a row only by its own layer's labels where a dim [joins `partial`](#extending-the-map-to-a-wider-schema), so a label added below a materialised node is reached, as is a label staged in a `WorkingRecord` and the first label of an axis that was empty when the broadcast was written.
In the base example, adding the scenario `mid` below the child reads `(gas, mid)` as 50.

A row reaches every coordinate that no more specific row covers, whichever layer wrote it.
A deeper layer thus owns only the coordinates its rows reach.
With `partial = {entity, scenario}` and `x` also over `snapshot`, a root writing `(gas, NULL, 1) = 1.0` and a child writing `(NULL, high, 0) = 2.0`, `(gas, high, 1)` reads 1.0 from the root.

A label removed and added again takes every broadcast covering it, written before or after the removal, and no row that named it below the removal ([what a removal reaches](removal.md#what-a-removal-reaches)).

### One plan for every layer

The steps above run as one query per layer, whose joins do not depend on which named sets the rows hold.
A row's named set over the `partial` dims is a bitmask, a column of the row rather than a loop in the plan, so pruning, covering and crossing are a fixed number of joins:

- Pruning is one anti-join per `partial` dim.
- Covering expands each parent row to one copy per subset of its named set, tagged with the subset and its labels on it.
  One semi-join on attribute, subset and labels against the layer's rows, keyed by their own named set, finds every parent row a row of the layer covers, and one anti-join drops them.
- Crossing expands both sides to the named sets the other side's row may hold without either containing the other.
  One equi-join keeps the pairs whose labels agree on the dims both name, and each pair gives an overlap row.
  An attribute over at most one `partial` dim cannot cross, and the schema decides that it runs no crossing join.

In the extended example, the root's `(gas, NULL)` names `{entity}` and expands to copies tagged `{}` and `{entity}`.
The copy tagged `{entity}` with the label `gas` meets the child's `(gas, NULL)`, whose own named set is `{entity}`, and the root's row is dropped.

Each row's candidate masks come from its own mask and its attribute's domain, the `partial` dims the attribute is over, so the plan does not grow with the masks a schema allows.
Under a layer's own schema the masks are taken over that schema's `partial` dims, after the map is [extended to it](#extending-the-map-to-a-wider-schema).
A named set that no row holds costs nothing, and only owner-map rows are expanded, never value rows.
A row is copied at most 2^k times for k `partial` dims, and no join compares every row with every other.
One fold serves every purpose: the frozen layers and their materialised map, [the staged step](../use/editing.md#reading-with-pending-edits), `flags()` and the write of a whole record.

??? note "Why not enumerate the named sets a schema allows?"

    Enumerating the named sets a schema allows would also give a fixed plan, but most of its joins would run on empty inputs in every fold of every layer.

??? note "Why not a constant list of candidate masks?"

    A constant list of candidate masks in the plan grows as 4^k, and at nine `partial` dims building the plan alone takes seconds.

## Resolving a relation

The map already names the winning layer per key, so resolution reads only the owning layers' files.
A resolved relation joins those files to the owner map and keeps only owned rows.
A read runs no `MAX` or group-by of its own.
It returns the owned rows as written: a key NULL on a dim stays NULL, an overlap row reads its labels, and no row is expanded or dropped by depth.
The row order is unspecified.

<figure class="diagram">
--8<-- "docs/static/resolution-read.svg"
</figure>

The join condition for each owned-per coordinate is NULL-aware.
A stored NULL means every value.
The row joins its own map row, where it stays NULL, and each overlap row of its layer that names its key, where it takes that row's label in the output.
In the base example, the child's value row `(NULL, high)` joins its own map row and the overlap rows `(gas, high)` and `(wind, high)`, and reads 999 at all three.

The map is keyed as its owning layer stored each key, which its [flags](#flags) record.
A map row keeps its value where the layer's row set the dim (`varies`), and holds NULL where the row left it NULL (`broadcast`), with the owned value carried beside it.
A layer row then finds its owned keys by its stored values, NULLs included, on NULL-safe equality.
That is one hash join, which scans the layers and the map once each:

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
           CASE WHEN varies.scenario    THEN [scenario] END,
           CASE WHEN broadcast.scenario THEN [NULL] END
         )) AS scenario
  FROM owner_map
) o
  ON o.attribute  = u.attribute
 AND o.layer_uuid = u.layer_uuid
 AND o.scenario   IS NOT DISTINCT FROM u.scenario  -- owned-per coordinates: as stored, NULL included
```

??? note "Why not join a stored NULL to every label directly?"

    The plain condition `u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario` is no hash key.
    DuckDB would join on the other columns and filter the dim afterwards, which costs the square of the dim's length per key.

The query projects the attribute's own coordinates ([the long schema](../storage/format.md#the-long-schema)).
That is `connection` for an attribute over that multi-dim, `entity` for a per-entity one, and none of them for a record-level weighting.
The join conditions follow from the key columns the attribute's file carries, so the schema derives the query's shape.
A coordinate outside the key, here `timestep` or a multi-dim outside `partial`, is in no join condition and passes straight through.

A consumer settles each coordinate by [the broadcast rule](../model/attributes.md#the-broadcast-rule) over the full coordinates, `partial` or not, and needs neither depth nor the list of `partial` dims.
Wherever two rows cross, the fold or the staging has written a row naming both, so the rule finds one winner wherever a row reaches.
The read's only filter of removed labels covers what the map cannot see ([below](#a-label-outside-the-key)).

### Contested layers {#contested-layers}

An overlap row reads its labels, so a layer that also wrote a row naming those labels answers that coordinate twice.
A layer is contested for an attribute where its rows name different sets of `partial` dims, so it reaches some coordinates twice.
The child of the [extended example](#folding-a-layer-onto-its-parent) is one: its `(gas, NULL)`, `(NULL, high)` and `(gas, high)` all reach `(gas, high)`.
Where a layer holds rows of an attribute naming different sets of `partial` dims, the read keeps at each coordinate the row that names every `partial` dim another row there names, [the more specific one](../model/attributes.md#the-broadcast-rule), through a window over the attribute's coordinates.
A coordinate with no such row, or with two at one breakpoint, fails the read, naming the attribute and the coordinate.
`breakpoint` is no coordinate, so the window keeps or drops a curve whole.

Only the value rows of a contested layer need the window.
Whether a layer is contested is a column of the map's rows, so the read joins the rows of contested layers in one arm, which ranks them, and those of every other layer in a second arm, which passes them straight through.
Over frozen layers alone, an attribute that no layer contests, the common case, takes the second arm only.
Above the frozen layers the plan cannot ask the rows, so every attribute is planned with both arms.

??? note "Why not rank every value row?"

    Ranking every value row instead would keep the plan fixed too, but makes the read of a single base layer with a long series several times slower.

A staged layer meets [the write's refusal](../use/writing.md#shape) of two rows neither more specific as it is staged.
A [`set`](../use/editing.md#set) that crosses an earlier staged row also stages the row naming both, and one whose own rows cross is refused.

### Multi-dims, curves and absent attributes {#multi-dims-curves-and-absent-attributes}

A `partial` multi-dim column such as `connection` joins NULL-aware exactly as `scenario` does, and a NULL there [reaches](../model/attributes.md#the-broadcast-rule) every tuple of the resolved multi-dim.
DuckDB hash-joins `IS NOT DISTINCT FROM` on a `STRUCT` key directly.
A dim the multi-dim draws on, such as `bus` beside it on a per-bus attribute, is a coordinate of its own and joins NULL-aware where it is `partial`.

The query projects `breakpoint` and does not join on it, since it is no part of the key.
A curve is owned whole ([wide and long rows](../model/attributes.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.
Dims outside the key pass through unchanged, because the rows of one key come from one layer.

An attribute that no layer wrote is absent from the map.
Its relation is empty and typed, listed under its name like any declared attribute, and the consumer applies [the schema's `default`](../model/attributes.md#attributespec).

## What the key cannot see {#what-the-key-cannot-see}

The owner map's key holds only the `partial` dims, so the fold sees neither the labels of any other dim nor a crossing along one.
The read covers the first, and the staging the second.

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
A layer written otherwise that leaves such a crossing is refused, naming the layer, the attribute, the dim and the key.
The check runs when a workspace first folds the frozen layer, so a layer from any writer is checked, and once per workspace rather than on every read.

## Extending the map to a wider schema

The fold reads each layer under the schema in its own manifest: its `partial` keys the layer's rows, and its attributes' `dims` scope their flags.
`compatible_with` holds along every ancestry, so both only grow from a layer to its child.
A node's map depends on its ancestry alone, so its fold table and its materialised map are valid forever, whatever a descendant declares.

Before a layer folds onto its parent's map, the map is brought to the layer's schema where that schema is wider, whether it was folded live or read from a materialised node.
Two changes need it, whether or not the dim `d` is new to the schema:

| change in the layer's schema       | extension of the map                                                                                                                   |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `d` added to an attribute's `dims` | the field `d` in both flag structs, `broadcast.d` set on the attribute's rows, which hold NULL along it                                |
| `d` joins `partial`                | a NULL key column for rows not varying along `d`; each row varying along it is split by the labels its owning layer wrote there, below |

The first states in the flags what the rows are, a [broadcast](../model/attributes.md#the-broadcast-rule), so the read joins them as one and [`flags()`](../use/reading.md#flags) reports them.
Two consecutive layers under one schema extend nothing.

A NULL key column does not mean the same for every row.
Take a root under `partial = {entity}` writing `(gas, low) 1` and `(gas, high) 2` along `scenario`, and a child under `{entity, scenario}` writing `(gas, high) 3`.
Extended with NULL, the root's map row `(gas, NULL)` would keep `varies.scenario`, and the read would join it only to the root's rows stored NULL along `scenario`, of which there are none.
Split, the root owns `(gas, low)` and `(gas, high)`, the child's row covers the second, and the read gives `(gas, low) 1` and `(gas, high) 3`.

<figure class="diagram">
--8<-- "docs/static/resolution-widen.svg"
</figure>

The split is one join of the varying rows, an [overlap row](#crossing-rows) included, keyed as their layer stored them and NULL-safe on the old key, against the owning layers' files of the attributes concerned.
It gives a row per label and, where the layer also wrote the key NULL along `d`, one row NULL there; a multi-dim's labels are its tuples.
Each split row's flags are recomputed per new key by the aggregate a layer's own rows use.
Inherited, they would let a row that held a key both ways also join the layer's row NULL along `d`, answering its label twice.

The split rows are then anti-joined against the [dead labels](#a-label-outside-the-key) of `d` at the parent, since the read hid those rows there and no read will once `d` is a key column.
While `d` is outside `partial` the fold keeps every restatement of it, seeded from a materialised base's dead labels, and the dead labels at the parent are those these held and the parent's axis lacks.
Usually there are none, and the anti-join is skipped.
From the layer on, the fold prunes `d` by each layer's removals as it does any `partial` dim.

No other compatible change reaches the fold.
The refusal of a crossing along [a dim outside `partial`](#a-dim-outside-partial) judges the layer's rows against the extended map, under the layer's schema.

## Flags

The fold computes the [flags](../use/reading.md#flags) of each map row in the group-by that builds the layer's rows, so they cost nothing beyond it.
It computes them per key, so per entity: whether this entity's `p_max_pu` sets `timestep` differs from whether any entity's does.
An overlap row carries the flags of the layer's row it comes from.
`flags()` aggregates the map's rows per attribute, `bool_or` over each field of `varies` and `broadcast` and over `breakpoints`.
It describes the rows the map keeps, so a broadcast that deeper rows shadow at every label still counts.

Each struct has a field per coordinate: every declared dim, multi-dims included.
A dim that a multi-dim draws on has its own field, so a `marginal_price` constant over buses reports `bus` as broadcast.

<figure class="diagram">
--8<-- "docs/static/resolution-flags.svg"
</figure>

The flags are two structs because [which dims exist is declared](../model/dims.md).
A column per dim would make the map's column set depend on the schema.
In a struct, the map's columns stay fixed, and only the fields change.
A dim new to a layer's schema gains its field where the fold [extends the map](#extending-the-map-to-a-wider-schema) to that schema, so no map lacks a field its node's schema declares.

`breakpoints` sits outside both structs, since a breakpoint is no dim ([wide and long rows](../model/attributes.md#wide-and-long-rows)).
The dim namespace lives entirely inside `varies` and `broadcast`, so a dim named `breakpoints` collides with nothing.

`Record.flags()` unions the flags over the record.
`flags(**categories)` unions them over the rows reaching the labels of those categories, a row NULL on the coordinate reaching every label.
[Reading](../use/reading.md#flags) says what the union means to a consumer.

## Materialised node caches

A revision's owner map, the layers holding its resolved axes and its dead labels may be materialised under `layers/<id>/resolved/`.
The schema is not, since each layer's manifest carries [its schema whole](../model/schema.md#one-schema-per-record) and nothing folds it.

Where a materialised map exists, a read stops there.
The map is already folded over everything above it, so a read walks the ancestry back to the nearest materialised revision and no further.
That truncation keeps a deep chain cheap.
The same rule decides where resolved axes come from: the layers a revision's own `resolved/dim_owners.parquet` names where materialised, and its raw `dims/` otherwise.
The presence of the cache answers this, and no recorded state is involved.
A materialised map always counts: it is keyed by its node's schema, and a read under a wider one [extends it](#extending-the-map-to-a-wider-schema) as it would a map folded live.
The two are distinct paths within one directory, so a record read as an ancestor and the same record read as itself never alias.

`resolved/dim_owners.parquet` holds a row `(dim, layer_uuid, has_dead)` per axis, a multi-dim's included: the layer whose `dims/<dim>.parquet` is the deepest at or below the revision, and whether the dim has dead labels.
A read of an axis stops there too, and opens that layer's file.
A dim it does not name has no axis.
A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) is never named, its axis being the schema's list.
The file points into ancestor layers by UUID, as the owner map does, rather than copying their files.
A layer is write-once, so the pointer never goes stale, and on object storage it is one read where a directory of copies is a listing.

Beside them, `resolved/dead/<coord>.parquet` holds the [dead labels](#a-label-outside-the-key) of each coordinate outside the owner-map key, and of each dim that a multi-dim's coordinate draws on or a property refers to.
It is written only where there are any, and `has_dead` in `dim_owners.parquet` says which are written, so a read opens each by name rather than listing the directory.
A coordinate with dead labels always has an axis, so `dim_owners.parquet` names every one.
A read above the revision seeds its dead labels from these files, since the axis files hold only live labels.
`resolved/dead/` and `owner_map.parquet` are folds, since no layer file holds either.

Materialising is a policy: every N layers, at a branch point, or on demand.
It only adds files under `resolved/`, so it changes no answer, only how many layers a read touches.
No manifest lists them: they are written after the commit, at any later time, and a read opens each by name.
The persisted map is the fold's result, and a NULL in it reaches the labels of the node read, so a label added below the node is reached as it would be without the cache.
Where the workspace already holds the node's fold, materialising writes it rather than folding again.

### A fold starts from the nearest fold at hand

A fold starts from the deepest proper ancestor whose fold is at hand: materialised on disk, or held by the workspace as [shared tables](../storage/concurrency.md#a-cursor-per-thread) for as long as its connection lives.
It takes that fold's owner map, axes and dead labels as its seed, and folds and checks only the layers above it.
A record opened on a child after a commit thus folds one layer onto its parent's fold.
A node never seeds its own fold, since that would return the cached answer and fold nothing on top.

<figure class="diagram">
--8<-- "docs/static/resolution-nearest.svg"
</figure>

When a deep ancestry should materialise its map is [open](../open-questions.md#when-a-deep-ancestry-materialises-its-map).

### A checkpoint per layer {#a-checkpoint-per-layer}

Each layer's fold, with its owner rows built once, is kept in a table before the next layer folds onto it.
DuckDB binds a relation's whole tree at every step, and the checks and the crossing join each read the map below, so a plan left unchecked for several layers doubles with each one.
The fixed plan cuts the joins per layer, not the statements, so a cold fold stays linear in depth.

??? note "Why not a checkpoint every few layers?"

    A checkpoint every fourth layer makes a depth-16 fold about eight times slower than one every layer.

## A directory is a fold over one layer {#one-record-over-one-fold}

A plain record directory has no reader of its own: `Workspace.at(uri)` reads it as a fold over one layer, through a single [`DirectorySource`](../storage/format.md), whose layout is a layer's.
So one `Record` and one rule read every record, and a consumer cannot tell a directory from a revision.
Over one layer every key is owned by that layer, member order is the file's row order, and `schema.partial` is inert, since it sets the granularity of a patch and one layer patches nothing.
[`flags`](#flags) come from the same scan that finds each key's owner, which a directory needs in any case: parquet's footer statistics are per row group, so a file that mixes one label's series rows with another label's constant says nothing about either in its footer.
