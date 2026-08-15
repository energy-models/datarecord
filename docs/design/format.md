# The record format

A record's **on-disk form** is a parquet directory: [`write_record`](writing.md) produces it, `DirectoryRecord` reads it, and a foreign tool can consume it knowing nothing about this package.
A record that is never written has no directory, and answers [the protocol](record.md) all the same.

```text
record/
├── manifest.json                   # the schema
├── dims/
│   ├── components/<Type>.parquet   # members + non-varying attribute columns
│   ├── connections/<Type>.parquet  # component↔bus connections
│   └── <dim>.parquet               # one axis table per declared dim
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```

Every file under `dims/` is named for what it holds, singular: `dims/scenario.parquet` for the `scenario` axis, as `inputs/p_nom.parquet` is for `p_nom`.
A dim's file is its name and nothing else — no pluralisation, which would be English grammar applied to a declared identifier and would spell a dim named `bus` as `buss.parquet`.

## Where a value lives

Decided by the attribute's [declared `dims`](schema.md#attributespec), not by a particular value:

- **`dims/components/<Type>.parquet`** — attributes that vary over nothing (`dims = {}`): one column per attribute, indexed by `entity`.
  A component's membership is its row here, and the file it is in is what gives it its type ([entity is unique across types](#entity-is-unique-across-types)).
- **`inputs/<attr>.parquet`** — every attribute that may vary, even where a given component's value happens to be constant.
  That component is then a row with the varying dim NULL.

So a component type's constant frame is assembled from both: the non-varying columns, and the dim-NULL rows of the varying files.

[Connections](record.md#connections) are rows in `dims/connections/<Type>.parquet`, keyed by `(entity, bus, *connection key dims)` and carrying their own tombstones.
A record with no such directory has no connections.

## The long schema

Every `inputs/` and `outputs/` file carries the long columns [the protocol](record.md#wide-and-long-rows) describes, in this order, whatever its rows use:

```text
entity | bus | attribute | breakpoint | value | <dim> ...
```

So `bus` and `breakpoint` are all-NULL columns in a record with no connections and no curves.
That uniformity is what lets one `UNION ALL BY NAME` and one join shape serve every kind of attribute row ([resolving a relation](read-path.md#resolving-a-relation)), and it means a file written without one of these columns still reads back correctly, since `union_by_name` supplies the NULL.

One attribute per file, so `value` carries that attribute's dtype.
There is **no `component_type` column**: `entity` is unique across every type ([below](#entity-is-unique-across-types)), so `inputs/p_max_pu.parquet` holds every type's `p_max_pu` keyed by entity alone, and a reader wanting one type's rows joins `dims/components/`.

A connection's `role` is not in the long schema: it lives on the connection row and identifies nothing in `inputs/` ([connections](record.md#connections)).

## `entity` is unique across types

An `entity` identifies one component across the whole record.
Two types may not share one: a `Bus` and a `Generator` both called `north` are a collision, not two components.

That is what removes `component_type` from every attribute key.
An `inputs/` row addresses `(entity, bus, …, attribute)`, and the type it belongs to is recoverable but not part of the address.
The alternative — carrying the type in the key — makes it a _component's_ identity in one place and a _row's_ in another, and every join then has to agree about which.

**The entity tables are the mapping.** `dims/components/<Type>.parquet` already partitions membership by type, one file per type, so `entity -> component_type` is the file a name's row is in.
Nothing new is stored to answer it: no separate entity table, because the component tables _are_ one.
Where the union of those files needs the type as data — [the owner map](read-path.md#owner-map), a glob across types — `component_type` is a column of the **entity** rows, never of the attribute rows.

So a consumer wanting one type's `p_max_pu` joins the resolved attribute frame to the components map on `entity`.
That join is what the `component_type` filter used to be, and it is against a relation the read path already builds.

Two things follow, and they are the reason to want this.
An attribute row is addressed the way a component is, so `set("p_nom", 150.0, names=["wind1"])` needs no type: the entity determines it ([set](working-record.md#set)).
And the inputs key loses a column, so the fold's key is `(entity, bus, *owned_per dims, attribute)` — one less column to compare NULL-safely in every join in [the read path](read-path.md).

**Enforced, not assumed.** [`write_record`](writing.md) rejects a record whose component tables share an entity, and `add` rejects an entity the record already resolves under another type ([add / remove](working-record.md#add-remove)).
A collision cannot be left to be discovered: it would silently merge two components' attribute rows, since the rows themselves no longer record which type they meant.

A modelling framework that scopes names per type must therefore reconcile before it writes.
Its [tool's `verify`](tools.md) is where that is reported, rather than something the record layer mangles a name to paper over: a record's `entity` is the framework's own name, and a record that renamed them would hand back components a framework cannot find.
