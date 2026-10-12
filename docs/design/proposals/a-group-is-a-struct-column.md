<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Proposal: a group is one struct column

Status: **Implemented** · Drafted 2026-10-12 · Implemented 2026-10-12

Landed in the [steps below](#landing) on [the schema](../schema.md#groups), [the record](../record.md#the-broadcast-rule), [the format](../format.md#the-long-schema), [the read path](../read-path.md#owner-map), [the working record](../working-record.md#set) and [consuming a record](../tools.md#results-on-connections); this page is kept as the argument for the change rather than as the current description, and those pages are authoritative where the two disagree.
[What the implementation found](#what-the-implementation-found) records where it departs from or adds to the argument below.

An attribute over a [group](../schema.md#groups) carries the group as **one column, named for the group, of a `STRUCT` dtype over the group's key** — `connection STRUCT(entity, bus)` — rather than the group's coordinates spread out as columns of their own.

That gives back broadcasting to every dim a group draws on, and makes the [owner map](../read-path.md#owner-map) key one column per membership, so a group shows up there as itself.

## What starts it

[Addressing](../schema.md#addressing-dims-x) expands a group in `dims` to its coordinates, so `inputs/efficiency.parquet` is `entity | bus | timestep | …` and the fold key is the flat union of every group's coordinate names ([`partial_dims`](../schema.md#partial-the-granularity-of-an-override)).
A column in the fold key is either broadcast or membership, never both, and a group coordinate [never broadcasts](../record.md#the-broadcast-rule).
So `Schema.broadcast_dims` drops every coordinate any group is keyed by, **schema-wide**:

```python
sparse = {c for g in self.groups.values() for c in g.key}
```

The cost lands on attributes that have nothing to do with the group.
Declaring `connection` over `(entity, bus)` means no attribute anywhere may hold a value once for every bus: a per-bus `marginal_price` over `{bus, timestep}` reads a NULL `bus` as "no bus", not "every bus".
Declaring `country` over `bus` into `country` — a classification, which addresses no attribute row at all — does the same.
The broadcast rule is right that _a connection's_ `bus` has no axis to expand against; it is wrong that _the dim_ `bus` therefore has none.
The two are one column today only because the group was flattened into the dim's name.

The owner map pays the same flattening in legibility.
Its key is `entity | bus | from | to | <partial dims>`, and which of those columns together name a connection, a corridor or nothing is recoverable only from the schema.
A group's tombstones are a multi-column anti-join over coordinates that other groups and plain dims share names with.

## The construct

**An attribute row carries a group as one struct column named for the group**, with one field per [key](../schema.md#into-a-group-that-classifies) coordinate, in `over` order:

```text
inputs/efficiency.parquet   connection STRUCT(entity, bus) | timestep | attribute | breakpoint | value
inputs/flow.parquet         corridor STRUCT("from", "to")  | timestep | attribute | breakpoint | value
inputs/marginal_price       bus | timestep | attribute | breakpoint | value
```

`coordinates_of("efficiency")` is `("connection", "timestep")`: [addressing](../schema.md#addressing-dims-x) still resolves a name in `dims` to the dim of that name first and to the group otherwise, but a group resolves to itself rather than expanding.

**The membership keys are `entity` and one column per group**, and every declared dim but `entity` and the type axis broadcasts.
`broadcast_dims` loses its `sparse` exclusion; `bus` broadcasts like `scenario`.
The fold key stays [one fixed tuple](../schema.md#partial-the-granularity-of-an-override), its columns now `entity | connection | corridor | <partial value dims>`.

**A group column does not broadcast**, for the reason the broadcast rule already gives: it addresses a sparse subset only the group's table knows.
It is compared NULL-safely as one value, which DuckDB hash-joins on directly — checked on 1.5.5, `k IS NOT DISTINCT FROM k` over a `STRUCT` key plans a `HASH_JOIN`, and a NULL field compares equal to a NULL field.
So [resolving a relation](../read-path.md#resolving-a-relation) joins `o.connection IS NOT DISTINCT FROM u.connection` where it joined `entity` and `bus` separately, and the address columns still derive from the schema.

**A shadowed group never enters a row.** `country` over `bus` into `country` is spelled `dims: [country]` as the dim, so no attribute row carries the group, and its file stays `bus | country` — the struct is how a group is _referenced_, not how it is stored.

**A group file stays flat**: `groups/connection.parquet` is `entity | bus | role`, keyed by its coordinates, folded [like any axis](../read-path.md#one-fold-for-every-axis).
The struct is built where a row is joined against it, `{'entity': entity, 'bus': bus} AS connection`, which is the one place the two shapes meet.

## The owner map

```text
# inputs
entity        -- never NULL where the attribute addresses an entity
connection    STRUCT(entity, bus)
corridor      STRUCT("from", "to")
<partial value dims>
attribute
layer_uuid
varies        STRUCT(<broadcast dim>: BOOLEAN, ...)   -- now with a `bus` field
broadcast     STRUCT(<broadcast dim>: BOOLEAN, ...)
breakpoints   BOOLEAN
```

**Only the groups a long input row can carry are keyed.** A group column belongs in the map where some attribute in `attributes` names the group beside another coordinate, so that it is long rows in `inputs/` — `efficiency` over `{connection, timestep}`.
Every other group would be a column NULL on every row of the map, widening the key every read pays for to identify nothing:

- the entity-type group, whose rows are the entity axis and which no attribute names beside `entity`;
- a group a dim shadows, `country` into `country`, which `dims: [country]` names as the dim;
- a group addressed only alone, whose attributes are [columns of its own file](../format.md#where-a-value-lives) — `role` on `connection`, were it the only one;
- a group only results name, `outputs/` [never overlaying](../read-path.md#outputs);
- a group nothing names.

So the map's membership columns are `entity` and `Schema.group_columns`, derived like `partial_dims` rather than declared.
Adding an attribute stays [compatible](../schema.md#versioning) though it may add a group to the key: a map folded before has no such column, which is correct — no row there names the group — so a reader takes a missing group column as NULL, as it takes a missing [flag field](../read-path.md#one-fold-for-every-axis) as unset.

Each membership is one column, so reading the map needs no schema to say which columns belong together.
A group's tombstones are a single-column NULL-safe anti-join, the same shape as the entity axis's; the [no-cascade rule](../read-path.md#owner-map) is unchanged.
The flag structs gain a field for each dim that now broadcasts, so `flags` reports a constant-over-buses `marginal_price` as broadcast over `bus` rather than not at all.

## What changes for a caller

`Record.attributes["efficiency"]` hands back a `connection` struct column; a consumer wanting flat columns takes `nw.col("connection").struct.field("bus")`, or unnests.
The [PyPSA tool](../tools.md) does that once, where it reads and builds connections.

`WorkingRecord.set` keeps [coordinate keywords](../working-record.md#set): `bus="north"` addresses one connection and the stage packs `{entity, bus}`.
A long frame handed to `set` may carry either the struct column or its coordinates flat; the coordinates are packed on the way in, so the frame a tool already builds keeps working.

## What it costs

- **An on-disk format change.** Every `inputs/` and `outputs/` file over a group is rewritten, and materialised owner maps are rebuilt. The project is pre-1.0 and holds no compatibility promise; no reader for the flat form survives.
- **Struct support at the boundary.** narwhals' `struct.field` works on DuckDB, polars and pyarrow-backed frames; a numpy-backed pandas frame has no struct dtype, so a pandas consumer unnests on the DuckDB side first.
- **A filter by entity on a connection attribute** becomes `connection.entity = 'g1'`. That costs nothing measurable: DuckDB 1.5.5 pushes a struct-field filter into the parquet scan and skips row groups on the field's statistics as it does for a top-level column — over 2M rows in 20 row groups, `conn.entity = …` and `entity = …` both read one row group (2.0 ms and 1.8 ms), and a field value outside every range skips all of them.

## One behaviour that moves

Today a connection attribute row carries a top-level `entity`, so [the fold](../read-path.md#owner-map) drops it when the entity is tombstoned even though the connection tuple survives.
Under the struct it has no top-level `entity`, and only the connection's own tombstone removes it.
That is the no-cascade rule applied consistently — a row is keyed by the memberships its coordinates name, and a connection attribute names the connection — but it means removing an entity must remove its connections to drop their values, which `WorkingRecord.remove` should then do.

## A rule it adds

An attribute naming a group and a dim one of its key coordinates is named for — `dims={"connection", "bus"}` — is rejected: the row would hold the bus twice, free to disagree.
It is unexpressible today only because the two land on one column; the struct makes it expressible, so the schema says so instead.
It is the same "keyed twice over" rule [`into`](../schema.md#into-a-group-that-classifies) already states.

## Landing

1. `Schema`: `coordinates_of` resolves a group to itself; `broadcast_dims` drops `sparse`; `membership_keys` gains the group columns; the rule above.
2. The fold and the read path: owner-map key and the group tombstone anti-join on the struct column; the resolve join on it.
3. Staging: pack coordinate keywords and flat frame columns into the struct.
4. The PyPSA tool packs on `to_datarecord` and unpacks on `build`.
5. [Schema](../schema.md#groups), [record](../record.md#the-broadcast-rule), [format](../format.md#the-long-schema) and [read-path](../read-path.md#owner-map) pages rewritten; this page marked landed.

## What the implementation found

- **Per-bus patching needs `bus` in `partial`.** `bus` is no membership key any more, so a layer patching a per-bus `marginal_price` one bus at a time declares it [`partial`](../schema.md#partial-the-granularity-of-an-override) like any value dim; a connection's bus is patched per row regardless, inside `connection`.
- **A value staged without `bus=` matches no connection.** It is stored keyed `{entity, NULL}`, and a NULL field compares equal only to a NULL field. The "every connection of this entity" reading the pages gave was never implemented; it is left [open](../open-questions.md).
- **`remove` then `add` leaves the entity without its connections.** `remove` tombstones every tuple naming the entity, and nothing revives them until they are restated.
- **A layer's own tombstones apply only to its parents.** So the staged collapse drops its own rows itself: `remove_group` also drops the rows it staged on the tuple, as `remove` does on the entity.
- **An empty frame must still type the group column as the struct.** A source emitting no rows for an attribute hands over a typed empty relation, or the writer rejects a column that is not a struct of the group's key.
- **`flags(ctype)` scopes a group by its entity field.** A group drawing on `entity` twice is scoped by its first coordinate, and an attribute with no entity at all — a per-bus value — is in no type's `flags`.
