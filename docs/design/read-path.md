# The DuckDB read path

## Owner map

The owner map answers, for a node, which layer owns each key.
Several maps, not one: `inputs`, `components`, and **one per declared [group](schema.md#groups)**.

Key columns first, then what each map carries over them:

```text
# inputs                            # components               # <group>
<partial dims>                      entity                     <group coordinates>
  entity     -- never NULL          --                         --
  <group coordinates>               --                         --
  <owned_per dims>                  --                         --
attribute                           entity_type             entity_type
layer_uuid                          layer_uuid                 layer_uuid
varies      STRUCT(<dim>: BOOLEAN, ...)   order_key            order_key
broadcast   STRUCT(<dim>: BOOLEAN, ...)   STRUCT(depth, row)   STRUCT(depth, row)
breakpoints BOOLEAN
```

All of them map to the owning `layer_uuid`, with deletions already applied.
None carries `value`, a varying dim's value, or `breakpoint`, so all stay small regardless of the series data or the size of a curve.

Splitting them keeps each row shape honest — `attribute` and the flags are meaningless for a component or a group row — and lets each persist as its own file.

**One kind per group rather than a `connections` map**, because a group is not a structural category: `connection` is one instance, `corridor` another, and a fold that named the first in its own code would have nothing to do with the second.
Each group folds the same way — keys, tombstones, an `order_key` — differing only in which columns key it, which is `Group.over`'s coordinate names.

The **inputs key is schema-derived**, not spelled: it is [`partial_dims`](schema.md#partial-the-granularity-of-an-override) plus `attribute`, where `partial` necessarily contains `entity` and every group coordinate because [neither broadcasts](record.md#the-broadcast-rule).
A coordinate an attribute's own file does not carry reads as NULL, which is what keeps the key one fixed tuple across attributes whose columns differ.

`entity_type` is on the **entity** maps only, never on `inputs`: an attribute row is addressed by `entity` alone ([entity is unique across types](format.md#entity-is-unique-across-types)), and the components map is what says which type an entity is.
So that map is the entity mapping every type-scoped question goes through — [`flags(ctype)`](record.md#flags) joins it, as does a consumer wanting one type's frame.

Where it is present it is a **column, not part of the key.**
Every map is keyed on its own coordinates — `entity` for components, the group's for a group, `partial_dims` plus `attribute` for inputs; the components map carries `entity_type` because it is the table that answers "what type is this entity", and that answer is functionally determined by the key rather than keying alongside it.
The fold therefore aggregates the type over the group-by instead of grouping on it.

The distinction is load-bearing rather than pedantic: keying on the type would let an entity resolve to two rows, admitting at read time the collision [name uniqueness](format.md#entity-is-unique-across-types) rejects at write time.
The same holds in the staging area, where `remove` under one type followed by `add` under another must collapse to the later edit ([committing](working-record.md#committing)).

`order_key` is a `STRUCT(depth BIGINT, row BIGINT)`, ordering lexicographically by field — DuckDB's native rule for a struct comparison — so `ORDER BY order_key` gives first-introduced order across layers ([axis order](record.md#axis-order)).
`depth` is one past the parent map's own deepest `depth` (`-1` folding to `0` for the root), so it depends only on the parent's rows, never on this layer's position in the ancestry list; `row` is file order within that layer alone.
It is assigned pre-union, per layer, because the fold's own output has no order of its own — a bare `row_number()` over what `UNION ALL` returns would scramble which row counts as first.
A key introduced at one depth and restated at a deeper one keeps its introducing `(depth, row)`: the parent's rows pass an anti-join with their own key intact, and only a genuinely new row gets a fresh one.

It is **derived, never stored**: no layer's parquet carries it, and [`write_record`](writing.md) writes no such column.
The fold computes it from each layer file's own row order, so file order is the input and `order_key` the answer, persisted only where a node's maps are [materialised](layers.md#materialised-node-caches) — where a child folding onto a materialised parent reads that parent's greatest `depth` exactly as it would any unmaterialised ancestor's.
Nothing sorts eagerly either — the maps carry it as a column, and a consumer wanting order applies `ORDER BY order_key`.

`order_key` is on the components and group maps only; the axes need none, since an axis row's order comes from its file ([axis order](record.md#axis-order)).
The two kinds carry it for different reasons, and only one is a correctness requirement.

For a **group** it is load-bearing.
A framework wanting positional ports numbers a component's connections by this order, so a patch layer adding a third connection appends rather than renumbering.
Without it the port index would follow whatever order the fold happened to emit, and adding a connection could silently move an existing one's attributes to a different port — the positional-keying failure [connections](record.md#connections) exists to prevent, reappearing at the point where position is reconstructed.

For **components** it is a stability guarantee rather than a correctness one: nothing resolves differently, since a component's rows are keyed the same way whatever order they come back in.
What it buys is that member order is deterministic across reads and recognisable — a record round-tripped through the write path comes back in the order it was authored, with additions appended, rather than reshuffled.

Each map is built by folding along the root→node path: parent map minus deletions and overrides, union the layer's own keys.
A node whose maps are [materialised](layers.md#materialised-node-caches) persists every one of them, so a read needs only the ancestry **back to the nearest materialised node** — the key scalability property.
They are written together, so the presence of one answers for all: `inputs` is the probe, being the kind every schema has whatever groups it declares.

**A component tombstone reaches a group's rows** where that group is over `entity`: deleting a component deletes every connection of it, so the group's fold anti-joins the parent against component tombstones as well as its own.
A group not over `entity` gets no such treatment — `corridor` over `(from, to)` draws both coordinates from the entity axis under names of its own, and which of them a tombstone should match is not the fold's to guess.
Elsewhere the fold runs live over that node's persisted maps, cached per connection; since [layers are write-once](layers.md#a-layers-data-is-write-once), such a cache never needs invalidating.

The [flags](record.md#flags) are folded in alongside the ownership group-by, so they cost nothing beyond it.
They are computed **per key**, so per component: whether _this_ component's `p_max_pu` sets `timestep` is a different question from whether any does.

The structs have a field per **broadcast** dim rather than per declared dim: an address coordinate [never broadcasts](record.md#the-broadcast-rule), so "did a row set it" is not a question about it — `entity` and a group's coordinate are always set, by construction.

Two **structs** rather than a `varies_<dim>` column per dim, because which dims exist is [declared](schema.md#dimensions) and a flat layout would make the map's _column set_ depend on the schema.
[Versioning](schema.md#versioning) calls adding a dim compatible; that has to hold for a map already persisted at a [materialised node](layers.md#materialised-node-caches), not only for the layers.
With a struct the difference is a missing _field_, which `UNION ALL BY NAME` fills with NULL exactly as it would a missing column, and the new dim reads as unset — which it is, since no row mentions it.
The map's columns are then fixed, and only the fields move.

`breakpoints` stays outside both structs, being no dim ([wide and long rows](record.md#wide-and-long-rows)).
That also means the dim namespace lives entirely inside `varies`/`broadcast`, so a dim named `breakpoints` would collide with nothing.

`Record.flags(ctype)` unions them over the entities of one type, which is [the granularity every consumer works at](record.md#flags) — where what the union means, and why it stops at the type boundary, is argued.

## Resolving a relation

A resolved relation semi-joins the owning layers' files to the `inputs` map, keeping only owned rows:

```sql
SELECT u.entity, u.bus, u.timestep,
       COALESCE(u.scenario, o.scenario) AS scenario,   -- one per owned_per dim
       u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/inputs/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN inputs o
  ON o.entity      IS NOT DISTINCT FROM u.entity  -- address coordinates: NULL-safe,
 AND o.bus         IS NOT DISTINCT FROM u.bus     -- never expanded against an axis
 AND o.attribute   = u.attribute
 AND o.layer_uuid  = u.layer_uuid
 AND (u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario)
```

The projected coordinates are the **attribute's own**, not a fixed prefix: `entity | bus` for a connection attribute, `from | to` for one over a corridor, neither for a record-level weighting ([the long schema](format.md#the-long-schema)).
The join's address columns follow from the same place, so the query shape is derived from the schema rather than spelling `entity` and `bus` as literals — which is what lets a second group resolve through the identical code.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
There is no per-read `MAX`/group-by and no tombstone filter — deletions are already absent from the map.

Each owned-per dim's arm is **NULL-aware**: a stored NULL means "all values", and the map may own it for only some of them, so the row joins every entry naming its layer and takes that value in the output.

A **group coordinate** like `bus` is joined **NULL-safely** rather than NULL-aware, being [an address rather than a broadcast dim](record.md#the-broadcast-rule).
It is that group's own map that decides which of its rows exist at all; a row whose connection was tombstoned is gone because that tombstone removed its `inputs` keys from the map, not by a filter here.

`breakpoint` is projected but not joined on, being no part of the key: a curve is owned whole ([wide and long rows](record.md#wide-and-long-rows)), so every breakpoint of a key comes from the winning layer.

Non-key dims pass through unchanged, because within one key-dim combination the rows come from one layer.

An attribute no layer wrote is absent from the map; its relation is empty, and the consumer applies [the schema's `default`](schema.md#attributespec).

## What differs between the implementations

|                  | `DirectoryRecord`              | `LayeredRecord`                             |
| ---------------- | ------------------------------ | ------------------------------------------- |
| resolution       | none — one record              | owner-map fold along ancestry               |
| `flags`          | `GROUP BY` scan over `inputs/` | free, folded with ownership                 |
| member order     | file order                     | `order_key`, first-introduced across layers |
| `schema.partial` | absent                         | the granularity of every patch              |

`flags` from a directory needs a real aggregate: parquet's footer statistics are per row group, not per component type, so a file mixing one type's series rows with another's constant says nothing about either.

The table describes a `DirectoryRecord` read as itself. A [`WorkingRecord`](working-record.md) _over_ one is a different reading of the same files: it folds them as a layer with its staged rows on top, so it builds an owner map and takes every property in the right-hand column. That map belongs to the `WorkingRecord`, exactly as a `LayeredRecord`'s belongs to the node rather than to the layers it folds — the directory itself gains nothing and still scans.

## Outputs

`outputs/<attr>.parquet` does not overlay.
An output relation reads the node's own layer only: if that layer has no `outputs/`, the record has no results, and an ancestor's are not inherited.
