<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The DuckDB read path

## Owner map

The owner map answers, for a node, which layer owns each key.
**One map: `attributes`.** It is the only table whose winning _key_ and winning row's _values_ live in different files — an attribute's ownership spans every `attributes/<attr>.parquet`, so the map names the owning layer per key and a read then goes to that layer's file for the value.
Every axis — a [dim](schema.md#partial-the-granularity-of-an-override)'s coordinates, the entity axis, a [relation](schema.md#relations) — is a single keyed file whose winning row _is_ the whole row, so none needs a map; each folds to a [resolved relation](#one-fold-for-every-axis) read inline.

The columns of the `attributes` map, keys first:

```text
# attributes
<partial dims>          -- the fold key: the dims declared `partial`
attribute
layer_uuid              -- the owning layer
varies      STRUCT(<dim>: BOOLEAN, ...)
broadcast   STRUCT(<dim>: BOOLEAN, ...)
breakpoints BOOLEAN
```

It maps each key to the owning `layer_uuid`, with deletions already applied.
It carries no `value`, no varying dim's value, and no `breakpoint`, so it stays small regardless of the series data or the size of a curve.

The **key of the `attributes` map is schema-derived**, not spelled: it is [`partial_dims`](schema.md#partial-the-granularity-of-an-override) plus `attribute`.
`partial_dims` is the dims declared `partial`, which include every dim a [relation](schema.md#relations) is keyed by.
A coordinate an attribute's own file does not carry reads as NULL, which is what keeps the key one fixed tuple across attributes whose columns differ.

The type is no part of an attribute row's key: an attribute over `entity` is keyed by `entity` alone, and the `entity_type` [relation](schema.md#types) says which type an entity is.
A type-scoped question goes through that relation — [`flags`](record.md#flags) over the names of one type, or a consumer that wants the frame of one type.

The map is built by folding along the root→node path: parent map minus deletions and overrides, union the layer's own keys.
A node whose caches are [materialised](layers.md#materialised-node-caches) persists it (and the resolved axes beside it), so a read needs only the ancestry **back to the nearest materialised node** — the key scalability property.
Every fold-key axis's tombstones reach this map in the fold: `fold_inputs` anti-joins the parent against the deleted rows of each dim in the fold key, `entity` among them — read from the same file that axis folds from — so a key whose label on any of those dims was deleted is absent from the resolved map rather than filtered at read.
A removed label takes the relation rows keyed on it too ([one fold for every axis](#one-fold-for-every-axis)), so a removed component loses its connection rows and its `entity_type` row with it.
A NULL there is a [broadcast](record.md#the-broadcast-rule) over every label rather than a label, so the NULL-safe anti-join never takes it; only a row naming a dead label is dropped.

## One fold for every axis

A dim's coordinates, the entity axis, and a relation are one construct: a keyed table a layer patches per key.
They fold by one path — last-writer-wins per key, [`deleted` honoured](layers.md#deletion), static columns carried on the winning row (`weight` on `dims/scenario.parquet`, `p_nom` on the entity axis, the `values` column on a relation) — producing one **resolved relation** with no `deleted` column and no `layer_uuid`.
A resolved relation keeps only the rows whose key names live labels: a relation row keyed on a label removed from a fold-key dim leaves with that label.

The resolved relation is returned **in first-introduced member order** — root first, then file order within a layer — and a node's caches persist it _in that order_, so a reader recovers member order from the resolved file's own row number.
There is no persisted `order_key` column: member order is the file's row order.
A consumer wanting positional ports numbers a component's ports by this order, so a patch layer adding a port appends rather than renumbering — the [positional-keying failure](record.md#connections) that order exists to prevent.
Across a materialised parent it still holds: the resolved seed is read in its own row order and a descendant's new rows number after it.

The fold runs live over an unmaterialised tail, cached per connection; since [layers are write-once](layers.md#a-layers-data-is-write-once), such a cache never needs invalidating.

The [flags](record.md#flags) are folded in alongside the ownership group-by, so they cost nothing beyond it.
They are computed **per key**, so per component: whether _this_ component's `p_max_pu` sets `timestep` is a different question from whether any does.

The structs have a field per declared dim, since every dim [broadcasts](record.md#the-broadcast-rule): "did a row set it" is a question about each of them.

Two **structs** rather than a `varies_<dim>` column per dim, because which dims exist is [declared](schema.md#dimensions) and a flat layout would make the map's _column set_ depend on the schema.
[Versioning](schema.md#versioning) calls adding a dim compatible; that has to hold for a map already persisted at a [materialised node](layers.md#materialised-node-caches), not only for the layers.
With a struct the difference is a missing _field_, which `UNION ALL BY NAME` fills with NULL exactly as it would a missing column, and the new dim reads as unset — which it is, since no row mentions it.
The map's columns are then fixed, and only the fields move.

`breakpoints` stays outside both structs, being no dim ([wide and long rows](record.md#wide-and-long-rows)).
That also means the dim namespace lives entirely inside `varies`/`broadcast`, so a dim named `breakpoints` would collide with nothing.

`Record.flags(entities)` unions them over the named entities; [flags](record.md#flags) says what the union means.

## Resolving a relation

A resolved attribute semi-joins the owning layers' files to the `attributes` map, keeping only owned rows:

```sql
SELECT COALESCE(u.port, o.port) AS port,             -- one per owned_per dim
       u.timestep, u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/attributes/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN attributes o
  ON o.attribute   = u.attribute
 AND o.layer_uuid  = u.layer_uuid
 AND o.entity      IS NOT DISTINCT FROM u.entity    -- fold-key dims it is not over
 AND o.scenario    IS NOT DISTINCT FROM u.scenario
 AND (u.port IS NULL OR u.port IS NOT DISTINCT FROM o.port)
```

The projected coordinates are the **attribute's own**, not a fixed prefix: `port | timestep` for `efficiency`, `entity | snapshot` for `p_max_pu`, and no entity column for a record-level weighting ([the long schema](format.md#the-long-schema)).
The join's columns follow from the same place, so the query shape is derived from the schema rather than spelling any dim as a literal.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
There is no per-read `MAX`/group-by and no tombstone filter — deletions are already absent from the map.

Each owned-per dim's arm is **NULL-aware**: a stored NULL means "all values", and the map may own it for only some of them, so the row joins every entry naming its layer and takes that value in the output.

A fold-key dim the attribute is not over is joined **NULL-safely**: it is NULL on both sides, so it matches and expands nothing.
There is no membership gate at read: an attribute row is keyed by the labels of the fold-key dims it is over, and each of those axes is [tombstone-pruned in the fold](#one-fold-for-every-axis), so a row whose label on one of them was deleted is already gone from the map.
The fold anti-joins each axis against its own `deleted` rows, read from the same file that axis folds from.
A NULL label is a broadcast rather than a label, so the anti-join never takes it — only a row naming a dead label is dropped.

`breakpoint` is projected but not joined on, being no part of the key: a curve is owned whole ([wide and long rows](record.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.

Non-key dims pass through unchanged, because within one key-dim combination the rows come from one layer.

An attribute no layer wrote is absent from the map; its relation is empty, and the consumer applies [the schema's `default`](schema.md#attributespec).

## One record over one fold

There is one `Record`, and it is the narwhals interface over a [`Resolver`](#owner-map). A plain parquet directory is not a second implementation of it: `Record.at(uri)` folds over a single [`DirectorySource`](format.md), and over one source the fold degenerates to a scan of it — there is one layer, so every key is owned by it and the anti-join has nothing to evict.

So a directory takes the same properties as a tree node, rather than its own column of exceptions:

- **`flags`** are computed in the ownership `GROUP BY`, one scan rather than the second one a separate aggregate would cost. They need a real aggregate either way: parquet's footer statistics are per row group, not per component type, so a file mixing one type's series rows with another's constant says nothing about either.
- **Member order** is `order_key`, which over one source is `(0, file order)` — file order, arrived at by the general rule.
- **`schema.partial`** is the granularity of a patch, and one layer patches nothing, so it is inert rather than absent.

Being a node in a layer tree is not what the fold requires; being a layer _layout_ is, and that is what a record directory is. A directory copied out of any tree, with no `revisions` row, reads identically.
