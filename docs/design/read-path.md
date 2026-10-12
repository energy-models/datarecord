<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The DuckDB read path

## Owner map

The owner map answers, for a node, which layer owns each key.
**One map: `inputs`.** It is the only relation whose winning _key_ and winning row's _values_ live in different files — an attribute's ownership spans every `inputs/<attr>.parquet`, so the map names the owning layer per key and a read then goes to that layer's file for the value.
Every axis — a [dim](schema.md#partial-the-granularity-of-an-override)'s coordinates, the entity axis, a [group](schema.md#groups) — is a single keyed file whose winning row _is_ the whole row, so none needs a map; each folds to a [resolved relation](#one-fold-for-every-axis) read inline.

The inputs map's columns, keys first:

```text
# inputs
<partial dims>          -- the fold key: membership keys + partial value dims
  entity     -- NULL where the row is keyed by a group or by no entity
  <group columns>         -- one STRUCT per keyed group: connection STRUCT(entity, bus)
  <owned_per value dims>
attribute
layer_uuid              -- the owning layer
varies      STRUCT(<dim>: BOOLEAN, ...)
broadcast   STRUCT(<dim>: BOOLEAN, ...)
breakpoints BOOLEAN
```

It maps each key to the owning `layer_uuid`, with deletions already applied.
It carries no `value`, no varying dim's value, and no `breakpoint`, so it stays small regardless of the series data or the size of a curve.

The **inputs key is schema-derived**, not spelled: it is [`partial_dims`](schema.md#partial-the-granularity-of-an-override) plus `attribute`.
`partial_dims` is the _membership keys_ — `entity` and one struct column per keyed group, which [address a row rather than broadcasting](record.md#the-broadcast-rule) — plus the broadcast value dims a layer may patch per value (`partial`).
A coordinate an attribute's own file does not carry reads as NULL, which is what keeps the key one fixed tuple across attributes whose columns differ.
Each membership is one column, so which columns name a connection is legible from the map itself, with no schema to say which belong together.

**Only the groups a long input row can carry are keyed** (`Schema.group_columns`): a group some varying input attribute names beside another coordinate, `efficiency` over `{connection, timestep}`.
Any other would be a column NULL on every row, widening the key every read pays for to identify nothing — the entity-type group, a group a dim shadows, a group addressed only alone (its attributes are [columns of its own file](format.md#where-a-value-lives)), a group only results name ([outputs never overlay](#outputs)), and a group nothing names.
A map folded before an attribute keyed a group lacks its column and reads it as NULL, which is correct: no row folded then named the group.

`entity_type` is **not** on `inputs`: an attribute row is addressed by `entity` alone ([entity is unique across types](format.md#entity-is-unique-across-types)), and the resolved entity axis is what says which type an entity is.
That axis is the entity mapping every type-scoped question goes through — [`flags(ctype)`](record.md#flags) joins it, as does a consumer wanting one type's frame — and it carries `entity_type` as a column, functionally determined by `entity` rather than keyed alongside it.
Keying on the type would let an entity resolve to two rows, admitting at read time the collision [name uniqueness](format.md#entity-is-unique-across-types) rejects at write time; so the fold aggregates the type over the group-by instead of grouping on it, and the staging area collapses a `remove` under one type followed by an `add` under another to the later edit ([edits](working-record.md#edits)).

The map is built by folding along the root→node path: parent map minus deletions and overrides, union the layer's own keys.
A node whose caches are [materialised](layers.md#materialised-node-caches) persists it (and the resolved axes beside it), so a read needs only the ancestry **back to the nearest materialised node** — the key scalability property.
Every membership's tombstones reach this map in the fold: `fold_inputs` anti-joins the parent against the deleted rows of the entity axis, of each keyed group, and of each partial dim — read from the same file that membership folds from — so a key whose entity, connection tuple or dim coordinate was deleted is absent from the resolved map rather than filtered at read.
A group's tombstones are one NULL-safe anti-join on its struct column, packed from the flat group file — the same shape as the entity axis's.
A layer's tombstones apply to its parents' keys, not to its own rows.
The entity axis's anti-join matches a row's _owning entity_: its `entity`, or else the entity inside a keyed group's struct — a group drawing twice on `entity` by its first coordinate — so deleting an entity drops its connections' values too.
This is not a cascade: each membership honours only its _own_ tombstones, so deleting an entity leaves its connection tuples in the group file until they are deleted in turn.
A membership column [never broadcasts](record.md#the-broadcast-rule), so a row not addressed by one carries NULL there and its NULL-safe anti-join never takes it; only a row naming a dead membership is dropped.

## One fold for every axis

A dim's coordinates, the entity axis, and a group are one construct: a keyed relation a layer patches per key.
They fold by one path — last-writer-wins per key, [`deleted` honoured](layers.md#deletion), static columns carried on the winning row (`weight` on `dims/scenario.parquet`, `entity_type` on the entity axis, `into` on a group) — producing one **resolved relation** with no `deleted` column and no `layer_uuid`.

The resolved relation is returned **in first-introduced member order** — root first, then file order within a layer — and a node's caches persist it _in that order_, so a reader recovers member order from the resolved file's own row number.
There is no persisted `order_key` column: member order is the file's row order.
A consumer wanting positional ports numbers an entity's connections by this order, so a patch layer adding a connection appends rather than renumbering — the [positional-keying failure](record.md#connections) that order exists to prevent.
Across a materialised parent it still holds: the resolved seed is read in its own row order and a descendant's new rows number after it.

An entity's wide static columns are the one axis value that lives in another file — per type, in `dims/entity_type/<ctype>.parquet`, not on the entity axis — so a node materialises the resolved per-type frames beside the axis, and [`entity_type`](#resolving-a-relation) reads them and gates against the resolved entity axis.

The fold runs live over an unmaterialised tail, cached in the workspace's database as [shared tables](#a-cursor-per-thread); since [layers are write-once](layers.md#a-layers-data-is-write-once), such a cache never needs invalidating.

The [flags](record.md#flags) are folded in alongside the ownership group-by, so they cost nothing beyond it.
They are computed **per key**, so per entity: whether _this_ entity's `p_max_pu` sets `timestep` is a different question from whether any does.

The structs have a field per **broadcast** dim rather than per declared dim: an address coordinate [never broadcasts](record.md#the-broadcast-rule), so "did a row set it" is not a question about it — `entity` and a group's column are always set, by construction.
A dim a group draws on is a broadcast dim and has its field, so a constant-over-buses `marginal_price` reports `bus` as broadcast.

Two **structs** rather than a `varies_<dim>` column per dim, because which dims exist is [declared](schema.md#dimensions) and a flat layout would make the map's _column set_ depend on the schema.
[Versioning](schema.md#versioning) calls adding a dim compatible; that has to hold for a map already persisted at a [materialised node](layers.md#materialised-node-caches), not only for the layers.
With a struct the difference is a missing _field_, which `UNION ALL BY NAME` fills with NULL exactly as it would a missing column, and the new dim reads as unset — which it is, since no row mentions it.
The map's columns are then fixed, and only the fields move.

`breakpoints` stays outside both structs, being no dim ([wide and long rows](record.md#wide-and-long-rows)).
That also means the dim namespace lives entirely inside `varies`/`broadcast`, so a dim named `breakpoints` would collide with nothing.

`Record.flags(ctype)` unions them over the entities of one type, which is [the granularity every consumer works at](record.md#flags) — where what the union means, and why it stops at the type boundary, is argued.

## Resolving a relation

A resolved relation joins the owning layers' files to the `inputs` map, keeping only owned rows:

```sql
SELECT u.connection, u.timestep,
       COALESCE(u.scenario, o.__owned_scenario) AS scenario,   -- one per owned_per dim
       u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/inputs/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN ( -- the map keyed as its owning layer stored each key
  SELECT * EXCLUDE (scenario), scenario AS __owned_scenario,
         UNNEST(list_concat(
           CASE WHEN COALESCE(varies.scenario, false)   THEN [scenario] END,
           CASE WHEN COALESCE(broadcast.scenario, true) THEN [NULL] END
         )) AS scenario
  FROM inputs
) o
  ON o.connection IS NOT DISTINCT FROM u.connection  -- address columns: NULL-safe, never
 AND o.attribute  = u.attribute                       -- expanded against an axis
 AND o.layer_uuid = u.layer_uuid
 AND o.scenario   IS NOT DISTINCT FROM u.scenario  -- owned-per dims: as stored, NULL included
```

The projected coordinates are the **attribute's own**, not a fixed prefix: `connection` for a connection attribute, `corridor` for one over a corridor, `entity` for a per-entity one, none of them for a record-level weighting ([the long schema](format.md#the-long-schema)).
The join's address columns follow from the same place, so the query shape is derived from the schema rather than spelling `entity` and `connection` as literals — which is what lets a second group resolve through the identical code.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
There is no per-read `MAX`/group-by and no tombstone filter — deletions are already absent from the map.

Each owned-per dim's arm is **NULL-aware**: a stored NULL means "all values", and the map may own it for only some of them, so the row joins every entry naming its layer and takes that value in the output.
The arm cannot be the obvious condition: `u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario` is no hash key, so DuckDB joins on the other columns and filters the dim afterwards, which costs the square of the dim's length per key.
So the map is keyed the way its owning layer stored each key, which its [flags](#owner-map) record: a map row keeps its value where the layer's row set the dim (`varies`) and holds NULL where the row left it NULL (`broadcast`), with the owned value carried beside it.
A layer row then finds its owned keys by its stored values, NULLs included, on NULL-safe equality — one hash join, scanning the layers and the map once each; the keyed map holds one row per owned key, two for a key its layer wrote both ways.
A map row NULL in the dim itself, broadcast over an empty axis, has `varies` false, so only its NULL form exists and it matches once.
A flag field missing from a map folded before the dim was declared counts as `broadcast` here, unlike in the flags a consumer reads: the layers' rows hold NULL there.

A **group column** like `connection` is joined **NULL-safely** as one value rather than NULL-aware against the map, being [an address rather than a broadcast dim](record.md#the-broadcast-rule); DuckDB hash-joins `IS NOT DISTINCT FROM` on a `STRUCT` key directly, a NULL field comparing equal to a NULL field.
A dim the group draws on, `bus` beside it on a per-bus attribute, is a broadcast dim and joins NULL-aware like `scenario` where it is `partial`.
There is no membership gate at read: an attribute row is keyed by every membership its coordinates name — the entity, each group tuple, each dim coordinate — and each of those is [tombstone-pruned in the fold](#owner-map), so a row whose entity, connection tuple or dim coordinate was deleted is already gone from the map.
A connection attribute's row carries no top-level `entity`, so it goes with its connection's tombstone or with its owning entity's, the one inside the struct.

`breakpoint` is projected but not joined on, being no part of the key: a curve is owned whole ([wide and long rows](record.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.

Non-key dims pass through unchanged, because within one key-dim combination the rows come from one layer.

An attribute no layer wrote is absent from the map; its relation is empty, and the consumer applies [the schema's `default`](schema.md#attributespec).

## One record over one fold

There is one `Record`, and it is the narwhals interface over a [`Resolver`](#owner-map). A plain parquet directory is not a second implementation of it: `Workspace.at(uri)` folds over a single [`DirectorySource`](format.md), and over one source the fold degenerates to a scan of it — there is one layer, so every key is owned by it and the anti-join has nothing to evict.

So a directory takes the same properties as a tree node, rather than its own column of exceptions:

- **`flags`** are computed in the ownership `GROUP BY`, one scan rather than the second one a separate aggregate would cost. They need a real aggregate either way: parquet's footer statistics are per row group, not per entity type, so a file mixing one type's series rows with another's constant says nothing about either.
- **Member order** is `order_key`, which over one source is `(0, file order)` — file order, arrived at by the general rule.
- **`schema.partial`** is the granularity of a patch, and one layer patches nothing, so it is inert rather than absent.

Being a node in a layer tree is not what the fold requires; being a layer _layout_ is, and that is what a record directory is. A directory copied out of any tree, with no revision file, reads identically.

## A cursor per thread

A DuckDB connection is not safe to use from two threads at once. DuckDB's answer is a cursor per thread, so the workspace's `con` is **the calling thread's cursor** on its one database, made on first use in that thread by a `threading.local` subclass — which reruns its `__init__` in each new thread, so there is no existence check and no lock, and a thread's cursor is dropped with the thread. A dict keyed by thread id would reuse ids; a `ContextVar` is copied into a task runner's workers, which would share the parent's cursor.

Tables are shared between cursors — the `revisions` cache, the staging tables, the folded caches. A `DuckDBPyRelation` is not: it is bound to the cursor that built it. So the line runs around relations alone:

- **Nothing but a relation holds a cursor.** A store, a revision, a `Resolver` and a layer source hold their workspace and look up `workspace.con` per call, so every relation they build lands on the calling thread's cursor, and a `Record` is usable from any thread. What they cache besides is plain data any thread computes the same.
- **A relation, a narwhals frame over one, and a `WorkingRecord` belong to the thread that built them.** One reaching another thread's write path is rejected as [a relation from another connection](#a-relation-from-another-connection-is-rejected) is.

**What a fold caches is shared, not held.** The frozen prefix's folded axes and groups are tables, `dims_<dim>_<id>` and `group_<name>_<id>`, beside the `owner_map_<kind>_<id>` [owner map](#owner-map); a last `coords_<id>` table lists them and is created after them, so a reader finding it finds them all. A dim or group name is hashed into the table name, being a caller's string. `<id>` is the node whose frozen layers were folded: a revision's id, the base's for a `WorkingRecord`, the [id its manifest carries](schema.md#one-schema-per-record) for a standalone directory. The schema is not in the key, so a fold made before the manifest widened is read as it is under the wider one. A [compatible widening](schema.md#versioning) changes no column a table already has and only adds; the one addition a table can lack is the new dim's field in the [flag structs](#owner-map), and each reader of a flag takes a missing field as unset, decided from the struct's type when the query is built rather than per row. Nor are they cast to the declared types as a materialised map is: each holds what the same fold would return uncached.

A fold one thread builds serves every thread. Two threads missing at once both fold and both create, and DuckDB fails the second with a write-write conflict rather than a name clash; the miss path catches that and reads the winner's table. The duplicate fold is wasted, not wrong.

### A relation from another connection is rejected

Copying a relation onto the target connection would collect it on its own — a full materialisation the caller never asked for, which fails anyway if that connection has been closed. So `as_relation`, the boundary every frame handed to a write or an edit crosses, binds it to the target cursor first, which costs no read and fails for a relation from another connection or another thread's cursor, and re-raises that as a `ValueError` naming the fix: build the frame on `record.con` in this thread, or hand over a non-DuckDB frame to land.

## Outputs

`outputs/<attr>.parquet` does not overlay.
An output relation reads the node's own layer only: if that layer has no `outputs/`, the record has no results, and an ancestor's are not inherited.
