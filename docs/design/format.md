<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The record format

A record's **on-disk form** is a parquet directory: [a write](writing.md) produces it, `Workspace.at(uri)` reads it, and a foreign tool can consume it knowing nothing about this package.
A record that is never written has no directory, and answers [the protocol](record.md) all the same.

```text
record/
├── manifest.json                   # the schema, and the record's id
├── dims/
│   ├── entity.parquet              # which entities exist, and of what type
│   ├── entity_type/<Type>.parquet  # non-varying attribute columns, per type
│   └── <dim>.parquet               # one axis table per declared dim
├── groups/<group>.parquet          # which tuples of the group exist
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```

Every file under `dims/` and `groups/` is named for what it holds, singular: `dims/scenario.parquet` for the `scenario` axis, as `inputs/p_nom.parquet` is for `p_nom`.
A dim's file is its name and nothing else — no pluralisation, which would be English grammar applied to a declared identifier and would spell a dim named `bus` as `buss.parquet`.

## The entity axis

`dims/entity.parquet` is what an entity's identity **is**: which entities the layer names, what type each is, and which are tombstoned.

```text
entity | entity_type | <entity key dims> | deleted     -- a schema declaring the type axis
entity | <entity key dims> | deleted | <attr> ...       -- a schema declaring none
```

It is **an axis file like any other**: a `Record` hands it over as `dims["entity"]` and [the writer](writing.md) writes it, with no derivation step of its own. A producer that knows its entities knows their types — `PyPSA` builds this frame from the same per-type frames it supplies as `entity_types` — so asking it for one file is cheaper than reconstructing it from the others, and it makes the writer a thing that writes frames rather than one that also computes one.

Two things live here in every record, and — where the schema declares no type axis — a third:

- **Membership.** An entity exists because it has a row here, not because some type's file mentions it.
- **Its tombstone.** Deleting an entity is deleting its entity row. A member file carries no `deleted`; membership is this file's alone, so the fold reads every tombstone here.
- **Its type**, where a group [declares the axis](schema.md#entity_type-the-axis-of-kinds). `entity_type` is then a column, so `entity -> entity_type` is one read rather than a glob across every type's file with the type taken from the filename.

**Where the schema declares no type axis there is no `entity_type` column** — nothing classifies an entity, so there is nothing to classify into — and an entity's non-varying attribute values are columns of this file rather than a per-type member file. A record with two labels then has one `dims/entity.parquet` and no `dims/entity_type/` at all, which is the honest shape of "these entities have no kinds"; [`entity_types()`](record.md) is empty, matching what the schema says.

Where a group **does** declare the axis, `entity_type` is a column and the non-varying values are partitioned into `dims/entity_type/<Type>.parquet` per type — that being the one thing genuinely per type: every type has a different column set. What remains in a member file is then only [what is addressed by `entity` alone](#where-a-value-lives).

The [entity axis](#the-entity-axis) folds from `dims/entity.parquet`, and so do entity tombstones. Both must read the same source: membership from one and deletions from another would resolve a deletion the fold never saw. The wide static columns here are read after the fold names a live entity, resolved [inline](read-path.md#one-fold-for-every-axis) rather than through a map.

`entity` is the one dim the format knows by name, and not merely because it is an axis — it is the axis the entity types partition. No other dim decides an attribute vocabulary.

## Where a value lives

Decided by the attribute's [declared `dims`](schema.md#attributespec), not by a particular value.

The rule: **an attribute naming exactly one addressing coordinate is a column on that thing's own table; anything more is long rows in `inputs/`.**

| `dims`                       | lands in                                                               |
| ---------------------------- | ---------------------------------------------------------------------- |
| `{"entity"}`, type axis      | `dims/entity_type/<Type>.parquet` — one column per attribute, per type |
| `{"entity"}`, no type axis   | `dims/entity.parquet` — a column of the entity axis itself             |
| `{"connection"}`             | `groups/connection.parquet` — the group's own file                     |
| `{"scenario"}`               | `dims/scenario.parquet` — the axis file                                |
| `{"country"}`                | `dims/country.parquet` — the axis file                                 |
| `{"entity", "snapshot"}`     | `inputs/<attr>.parquet`                                                |
| `{"connection", "snapshot"}` | `inputs/<attr>.parquet`                                                |

So "varying" is not "has dims" but **"has dims beyond its address"**, and one rule now covers what were three unrelated stories: an entity's constant columns, a connection's `role`, and an axis's payload.

- **`dims/entity_type/<Type>.parquet`** (where a group [declares the type axis](schema.md#entity_type-the-axis-of-kinds)) — attributes addressed by `entity` alone: one column per attribute, indexed by `entity`.
  **Values only**: a member file holds `entity` and one column per attribute, nothing else. An entity's _membership_ is its row on the [entity axis](#the-entity-axis), not its presence here, and its _tombstone_ is there too — a member file carries no `deleted`, so a removal is one row on the axis rather than two kept in step.
  And no `entity_type` column: the type is the file the rows are in, and the [entity axis](#the-entity-axis) is what carries `entity -> entity_type` for every later reader. A column repeating it would be a second copy of one fact, and the one that could disagree with the other.
  Where the schema declares **no** type axis there is no member file at all: these columns are on `dims/entity.parquet` itself, which is [the entity axis](#the-entity-axis) for every other purpose.
- **A [group](schema.md#groups)'s file** — attributes addressed by that group alone, PyPSA's `role` on a connection being one.
- **An axis file** — attributes addressed by one dim alone. A snapshot weighting is a number per snapshot and belongs to no entity, so `dims/snapshot.parquet` carries it as a declared column with a `dtype`, a `default` and a `description`.
- **`inputs/<attr>.parquet`** — every attribute addressed by more than its own coordinate, even where a given entity's value happens to be constant.
  That entity is then a row with the varying dim NULL.

So an entity type's constant frame is assembled from both: the non-varying columns, and the dim-NULL rows of the varying files.

A [group](schema.md#groups)'s rows are in `groups/<group>.parquet`, keyed by that group's coordinates and carrying their own tombstones — `groups/connection.parquet` for the `connection` group over `(entity, bus)`.
**The file stays flat**, `entity | bus | role`, one column per coordinate: the struct an attribute row carries is how a group is referenced, packed where a row is joined against the group.
A record with no such file has no rows of that group.

**One file per group, never split by type.** A group's rows are keyed by its coordinates and `entity_type` is not one of them: splitting put `connection` rows for a `Link` and a `Line` in different files despite identical keys, forcing a union on every read and privileging `entity` among the coordinates. A `corridor` between two buses has no type to split on at all, and a `contract` between two entities has two — one per coordinate, neither of them the row's — so the split never generalised beyond the case it was written for.

The [entity-type group](schema.md#entity_type-the-axis-of-kinds) is the one exception, its rows being the [entity axis file](#the-entity-axis) rather than a `groups/` file of their own: an entity's type is what its identity already states, so storing it twice is what would let the two disagree.

A functional group's [`into`](schema.md#into-a-group-that-classifies) label is a column of the group's file like any coordinate, so `groups/bus_country.parquet` is `bus | country`. No column on the classified axis: `dims/bus.parquet` does not gain a `country`, the relation being the group's own file. That costs a join where a column read would have done, and buys a uniform rule — `into` decides nothing about storage, so nothing in the layout branches on it.

## The long schema

Every `inputs/` and `outputs/` file carries its attribute's own coordinates, then the columns every row has:

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are what the attribute's [`dims`](schema.md#attributespec) declare, groups first, with a [group](schema.md#groups) carried as **one struct column named for it**, a field per key coordinate — so `inputs/efficiency.parquet` over the `connection` group carries `connection STRUCT(entity, bus)`, and `inputs/p_max_pu.parquet` over `entity` and `snapshot` carries `entity | snapshot` and no `bus`.
The writer rejects a group column that is not a struct of exactly the group's key: DuckDB casts a struct by field name, so a missing field would turn NULL and an extra one be dropped, silently re-keying every row.
An attribute over one dim alone has no file here at all: it is [a column of that dim's own table](#where-a-value-lives).

**Per attribute rather than schema-wide.** One attribute is one file, so one column set per file; a fixed prefix of `entity | bus` would put an all-NULL `entity` on a record-level weighting, claiming an entity the value has none of, and would privilege one group's spelling of `bus` over every other group's coordinates.

That the shapes differ costs nothing, because `UNION ALL BY NAME` supplies NULL for a column a file does not carry — which is also what lets a file written before a dim was declared still read back correctly ([resolving a relation](read-path.md#resolving-a-relation)).
The fold's _key_ is uniform even though the files are not: it is [`partial_dims`](schema.md#partial-the-granularity-of-an-override) plus `attribute`, one fixed tuple over every attribute, and a coordinate an attribute does not carry reads as NULL there.

One attribute per file, so `value` carries that attribute's dtype.
There is **no `entity_type` column**: `entity` is unique across every type ([below](#entity-is-unique-across-types)), so `inputs/p_max_pu.parquet` holds every type's `p_max_pu` keyed by entity alone, and a reader wanting one type's rows joins `dims/entity_type/`.

An attribute addressed by a group alone is not here either: it is a column of that group's own file ([where a value lives](#where-a-value-lives)) rather than a long row. PyPSA's `role` on a connection is the case.

## `entity` is unique across types

An `entity` identifies one entity across the whole record.
Two types may not share one: a `Bus` and a `Generator` both called `north` are a collision, not two entities.

That is what removes `entity_type` from every attribute key.
An `inputs/` row addresses `(entity, connection, …, attribute)`, and the type it belongs to is recoverable but not part of the address.
The alternative — carrying the type in the key — makes it an _entity's_ identity in one place and a _row's_ in another, and every join then has to agree about which.

**The entity axis carries the classification**, where a group [declares one](schema.md#entity_type-the-axis-of-kinds). `dims/entity.parquet` carries `entity_type` as a column, so `entity -> entity_type` is one read of one file rather than a glob over every type's — the [entity-type group](schema.md#entity_type-the-axis-of-kinds)'s rows, stored here rather than in `groups/` because an entity's type is part of the identity this file already states. A schema declaring no type axis has no such column and no types to recover; the rest of this section is the typed case.
`entity_type` is a column of that axis, carried on its resolved row, never of the attribute rows.
The type also has an axis file of its own, `dims/entity_type.parquet`, which is where a value addressed by the type alone — a per-type icon — lives; that is a column keyed by type, not an attribute row keyed by entity, so it takes nothing back from the paragraph above.

So a consumer wanting one type's `p_max_pu` joins the resolved attribute frame to the resolved entity axis on `entity`.
That join is what the `entity_type` filter used to be, and it is against a relation the read path already builds.

Two things follow, and they are the reason to want this.
An attribute row is addressed the way an entity is, so `set("p_nom", 150.0, entity=["wind1"])` needs no type: the entity determines it ([set](working-record.md#set)).
And the inputs key loses a column, so the fold's key is `(entity, connection, *owned_per dims, attribute)` — one less column to compare NULL-safely in every join in [the read path](read-path.md).

**Enforced, not assumed.** [The writer](writing.md) rejects a record whose entity tables share an entity, and `add` rejects an entity the record already resolves under another type ([add / remove](working-record.md#add-remove)).
A collision cannot be left to be discovered: it would silently merge two entities' attribute rows, since the rows themselves no longer record which type they meant.

A modelling framework that scopes names per type must therefore reconcile before it writes.
Its [tool's `verify`](tools.md) is where that is reported, rather than something the record layer mangles a name to paper over: a record's `entity` is the framework's own name, and a record that renamed them would hand back entities a framework cannot find.
