<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 10: a group is one struct column

| DEP           | 10                                                                                                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title         | a group is one struct column                                                                                                                                                                |
| Author        | Jonas Hörsch                                                                                                                                                                                |
| Status        | Superseded (2026-10-26, implemented 2026-10-12, [amended](#amendment-a-group-is-a-dim) 2026-10-13)                                                                                          |
| Created       | 2026-10-12                                                                                                                                                                                  |
| Version       | Unreleased                                                                                                                                                                                  |
| Superseded-By | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its group tombstones, by which the fold drops owner-map rows), [DEP 23](dep-0023-multi-dims.md) (its groups, which become multi-dims) |

The design landed in [the data model](../../reference/data-model.md), [the record format](../../reference/format.md), [the fold](../../reference/resolution.md), [`WorkingRecord`](../../reference/editing.md) and [tools](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

An attribute over a group has the group as one column, named for the group, of a `STRUCT` over the group's key: `connection STRUCT(entity, bus)`. The group's coordinates no longer spread out as columns of their own. Every dim a group draws on broadcasts again, and the owner map has one key column per group.

## Motivation

A group in `dims` expands to its coordinates, so the fold key is the flat union of every group's coordinate names. A column in the fold key is broadcast or membership, never both, and a group coordinate never broadcasts. `Schema.broadcast_dims` therefore drops every coordinate any group is keyed by, schema-wide.

Attributes that have nothing to do with the group need extra handling. Once `connection` is over `(entity, bus)`, a NULL `bus` in a per-bus `marginal_price` over `{bus, timestep}` means "no bus". A connection's `bus` has no axis to expand against, but the dim `bus` does. The owner map's key, `entity | bus | from | to | ...`, needs the schema to list which columns identify a connection.

## Specification

- A long row has a group as one struct column named for the group, with one field per key coordinate in `over` order. `coordinates_of("efficiency")` is `("connection", "timestep")`.
- `groups/<group>.parquet` is keyed by its struct column, as an axis file is keyed by its dim.
- `broadcast_dims` loses its exclusion, so `bus` broadcasts like `scenario`.
- The membership keys are `entity` and one column per group that some long input row carries, which is `Schema.group_columns`. Adding an attribute stays compatible. In a map folded before a group was added, the fold reads the missing column as NULL. The flag structs gain a field per dim that now broadcasts, such as `bus`.
- A group column does not broadcast. It is compared NULL-safely as one value. The fold drops each row whose group column matches one of the group's tombstones.
- The entity tombstone matches every entity in a row, at the top level and inside each struct. Removing an entity drops its connections' values and leaves the tuples.
- An attribute over a group and one of its key dims, such as `dims={"connection", "bus"}`, is refused, because the row would have the bus twice, free to disagree.
- A group does not share a dim's name, except the entity-type group, which has no file or column.
- `set` keeps coordinate keywords: `bus="north"` packs `{entity, bus}`.
- `Record.attributes["efficiency"]` returns the struct column, and a reader takes `struct.field("bus")` or unnests. The PyPSA tool packs on `to_datarecord` and unpacks on `build`.

## Rationale

DuckDB 1.5.5 plans `k IS NOT DISTINCT FROM k` over a `STRUCT` key as a `HASH_JOIN`, and a NULL field equals a NULL field. It pushes a struct-field filter into the parquet scan. Over 2M rows in 20 row groups, `conn.entity = …` and `entity = …` both read one row group, in 2.0 ms and 1.8 ms.

The design needs a format change to every `inputs/` and `outputs/` file over a group, and rebuilt owner maps. A numpy-backed pandas frame has no struct dtype, so a pandas reader unnests on the DuckDB side.

## Implementation notes

It landed in four steps: the schema, the fold and read path, staging, then the PyPSA tool. The build added to the argument:

- Patching a per-bus value one bus at a time needs `bus` in `partial`.
- A value staged without `bus=` is keyed `{entity, NULL}` and matches no connection. The reading "every connection of this entity" was never built.
- A layer's own tombstones apply only to its parents, so `remove_group` also drops the rows it staged on the tuple.
- A first cut had `remove` tombstone every tuple with the entity, which left a re-added entity without its parent's connections.
- A long frame handed to `set` has the struct only. Accepting flat coordinates too would have needed a second path with no caller.
- An empty frame still types the group column as the struct.
- Shadowing a dim's name went, because a group column and a dim column would share one name.
- Keying the group file by the struct removed every conversion where a row met it. Packing stays only where a caller hands flat values in, and in the PyPSA tool.
- The PyPSA tool declares no `bus` attribute over `connection`. A port's bus is the connection tuple.

## Open issues

- What a value staged without `bus=` means.

## Amendment: a group is a dim

Implemented 2026-10-13, drafted the same day.

A group is a dim whose axis is the resolved group, `groups/<group>.parquet`. It broadcasts, enters the owner map only where `partial` lists it, and otherwise a layer restates it whole, as it does `snapshot`.

### The amended design

- A struct value is a whole tuple or NULL. A NULL struct means every tuple of the group and expands against the resolved group. `set`, `add_group` and the writer refuse a partly NULL struct.
- The membership key is `entity` alone. A group is keyed in `partial_dims` exactly where `partial` lists it.
- Outside `partial`, a connection attribute's key is `(attribute)`, so the owner map assigns every connection's value to a layer that touches it. Staging adds the rest of the extent, and a layer written directly has to contain it.
- In `partial`, a layer patches one tuple, and the fold drops each parent row that has a tuple a group tombstone names.
- The flag structs gain a field per group.
- The PyPSA tool declares `connection` in `partial`, so `Link.efficiency` over `snapshot` is patched without restating every link.

This replaces "a group column does not broadcast": the fold already has the group's table in `Coords.groups`. Pulling the struct's entity fields into the key would narrow ownership to one entity's connections. The owner map would then have to key on struct fields, and `partial` already gives that grain.

The amendment has three costs. `flags(ctype)` cannot scope a group attribute outside `partial`, because its map rows contain no entity, so the open question on record-level flags widens. With `connection` in `partial`, PyPSA's attributes are unaffected. The owner map's format changes. And a schema that relied on per-connection ownership now has to declare `partial={"connection"}`.

### What the amendment found

- `expand_dims` broadcasts a NULL only on rows whose attribute's coordinates include it, for a dim as for a group. Otherwise every attribute multiplied by the group's size.
- Removing an entity needs a filter at read, because a connection row outside `partial` contains no entity. The read drops a row whose unkeyed group field contains an entity the resolved axis no longer has. Since [DEP 11](dep-0011-deletion-cascades-through-tombstones.md), it checks against the group's dead tuples.
- `group_columns` stayed, meaning every group but the entity-type group. The derivation of keyed groups went.
- `set`'s keywords stage no NULL struct, and a missing key coordinate is still refused. A NULL struct comes in through a long frame.
- The struct change had left `src/` about 340 lines larger than the flat layout. The amendment was estimated to remove 40 to 80 lines. Measured, `src/` grew by 19 lines net, 119 added and 100 removed, because of the read filter, the scoped expansion and the data check. In return, datarecord has one rule where it had two.

### What the amendment leaves open

- A NULL struct spans entity types: a constant meant for links also applies to buses' connections.
