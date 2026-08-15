# The DuckDB read path

## Owner map

The owner map answers, for a node, which layer owns each key.
Three maps, not one:

Key columns first, then what each map carries over them:

```text
# inputs                            # components               # connections
name                                name                       name
bus  -- NULL for component-level    <component key dims>       bus  -- never NULL
<owned_per dims>                    --                         <connection key dims>
attribute                           component_type             --
--                                  layer_uuid                 component_type
layer_uuid                          order_key                  layer_uuid
varies      STRUCT(<dim>: BOOLEAN, ...)                        order_key
broadcast   STRUCT(<dim>: BOOLEAN, ...)
breakpoints BOOLEAN
```

All three map to the owning `layer_uuid`, with deletions already applied.
None carries `value`, a varying dim's value, or `breakpoint`, so all stay small regardless of the series data or the size of a curve.

Splitting them keeps each row shape honest — `attribute` and the flags are meaningless for a component or connection row — and lets each persist as its own file.

`component_type` is on the **entity** maps only, never on `inputs`: an attribute row is addressed by `name` alone ([name is unique across types](format.md#name-is-unique-across-types)), and the components map is what says which type a name is.
So that map is the entity mapping every type-scoped question goes through — [`flags(ctype)`](record.md#flags) joins it, as does a consumer wanting one type's frame.

Where it is present it is a **column, not part of the key.**
Every one of the three maps is keyed on `name` (plus `bus` and the dims that apply); the components map carries `component_type` because it is the table that answers "what type is this name", and that answer is functionally determined by the key rather than keying alongside it.
The fold therefore aggregates the type over the group-by instead of grouping on it.

The distinction is load-bearing rather than pedantic.
Keying on the type would mean a name could resolve to two rows — one per type — which is exactly the collision [name uniqueness](format.md#name-is-unique-across-types) forbids, silently admitted at read time instead of rejected at write time.
The same holds in the staging area: `remove` under one type followed by `add` under another must collapse to the later edit ([committing](working-record.md#committing)), and a type-partitioned key keeps both.

`order_key` is monotonic across the fold history, giving first-introduced order across layers ([axis order](record.md#axis-order)).
It is assigned pre-union, per layer, because the fold's own output has no order of its own — a bare `row_number()` over what `UNION ALL` returns would scramble which row counts as first.

It is **derived, never stored**: no layer's parquet carries it, and [`write_record`](writing.md) writes no such column.
The fold computes it from each layer file's own row order, so file order is the input and `order_key` the answer, persisted only where a node's maps are [materialised](layers.md#materialised-node-caches).
Nothing sorts eagerly either — the maps carry it as a column, and a consumer wanting order applies `ORDER BY order_key`.

`order_key` is on the components and connections maps only; the axes need none, since an axis row's order comes from its file ([axis order](record.md#axis-order)).
The two carry it for different reasons, and only one is a correctness requirement.

For **connections** it is load-bearing.
A framework wanting positional ports numbers them by this order, so a patch layer adding a third connection appends rather than renumbering.
Without it the port index would follow whatever order the fold happened to emit, and adding a connection could silently move an existing one's attributes to a different port — the positional-keying failure [connections](record.md#connections) exists to prevent, reappearing at the point where position is reconstructed.

For **components** it is a stability guarantee rather than a correctness one: nothing resolves differently, since a component's rows are keyed the same way whatever order they come back in.
What it buys is that member order is deterministic across reads and recognisable — a record round-tripped through the write path comes back in the order it was authored, with additions appended, rather than reshuffled.

Each map is built by folding along the root→node path: parent map minus deletions and overrides, union the layer's own keys.
A node whose maps are [materialised](layers.md#materialised-node-caches) persists all three, so a read needs only the ancestry **back to the nearest materialised node** — the key scalability property.
Elsewhere the fold runs live over that node's persisted maps, cached per connection; since [layers are write-once](layers.md#a-layers-data-is-write-once), such a cache never needs invalidating.

The [flags](record.md#flags) are folded in alongside the ownership group-by, so they cost nothing beyond it.
They are computed **per key**, so per component: whether _this_ component's `p_max_pu` sets `timestep` is a different question from whether any does.

Two **structs** rather than a `varies_<dim>` column per dim, because which dims exist is [declared](schema.md#dimensions) and a flat layout would make the map's _column set_ depend on the schema.
[Versioning](schema.md#versioning) calls adding a dim compatible; that has to hold for a map already persisted at a [materialised node](layers.md#materialised-node-caches), not only for the layers.
With a struct the difference is a missing _field_, which `UNION ALL BY NAME` fills with NULL exactly as it would a missing column, and the new dim reads as unset — which it is, since no row mentions it.
The map's columns are then fixed, and only the fields move.

`breakpoints` stays outside both structs, being no dim ([wide and long rows](record.md#wide-and-long-rows)).
That also means the dim namespace lives entirely inside `varies`/`broadcast`, so a dim named `breakpoints` would collide with nothing.

`Record.flags(ctype)` unions them over the names of one type, which is [the granularity every consumer works at](record.md#flags).
The union is not a loss of the per-key answer so much as the question being asked of a type: a framework assigns containers per type, so a type whose components disagree must be told so, and a dim landing in both sets is exactly that message.
The union stops at the type boundary, since across types it would describe neither.

## Resolving a relation

A resolved relation semi-joins the owning layers' files to the `inputs` map, keeping only owned rows:

```sql
SELECT u.name, u.bus, u.timestep,
       COALESCE(u.scenario, o.scenario) AS scenario,   -- one per owned_per dim
       u.attribute, u.breakpoint, u.value
FROM ( -- one arm per distinct layer the map names for this attribute
  SELECT ?::UUID AS layer_uuid, * FROM read_parquet(<layer>/inputs/<attr>.parquet)
  UNION ALL BY NAME
  ...
) u
JOIN inputs o
  ON o.name        = u.name
 AND o.bus         IS NOT DISTINCT FROM u.bus   -- NULL-safe: component-level keys NULL against NULL
 AND o.attribute   = u.attribute
 AND o.layer_uuid  = u.layer_uuid
 AND (u.scenario IS NULL OR u.scenario IS NOT DISTINCT FROM o.scenario)
```

`name` joins on equality rather than NULL-safely: it is required and unique ([name is unique across types](format.md#name-is-unique-across-types)), so there is no NULL to be safe about — the one column the old key needed a second equality for is simply gone.

The map already names the winning layer per key, so resolution reads only the owning layers' files.
There is no per-read `MAX`/group-by and no tombstone filter — deletions are already absent from the map.

Each owned-per dim's arm is **NULL-aware**: a stored NULL means "all values", and the map may own it for only some of them, so the row joins every entry naming its layer and takes that value in the output.

`bus` is joined **NULL-safely** rather than NULL-aware: it is part of the key but a required column rather than a broadcast dim, so NULL means "this attribute is the component's, not a connection's" and never "every bus".
It is the `connections` map that decides which connections exist at all; a row whose connection was tombstoned is gone because that tombstone removed its `inputs` keys from the map, not by a filter here.

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

## Outputs

`outputs/<attr>.parquet` does not overlay.
An output relation reads the node's own layer only: if that layer has no `outputs/`, the record has no results, and an ancestor's are not inherited.
