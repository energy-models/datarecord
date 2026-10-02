<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The record format

A record's **on-disk form** is a parquet directory: [`write_record`](writing.md) produces it, `Record.at(uri)` reads it, and a foreign tool can consume it knowing nothing about this package.
A record that is never written has no directory, and answers [the protocol](record.md) all the same.

```text
record/
├── manifest.json                   # the schema
├── dims/
│   ├── entity.parquet              # which entities exist, and their constant values
│   └── <dim>.parquet               # one axis table per declared dim
├── groups/<group>.parquet          # which tuples of the group exist
├── inputs/<attr>.parquet           # one varying input attribute per file
└── outputs/<attr>.parquet          # one result attribute per file
```

Every file under `dims/` and `groups/` is named for what it holds, singular: `dims/scenario.parquet` for the `scenario` axis, as `inputs/p_nom.parquet` is for `p_nom`.
A dim's file is its name and nothing else — no pluralisation, which would be English grammar applied to a declared identifier and would spell a dim named `bus` as `buss.parquet`.

## The entity axis

`dims/entity.parquet` is what an entity's identity **is**: which entities the layer names, which are tombstoned, and the value of each attribute declared over `entity` alone.

```text
entity | deleted | <attr> ...
```

It is **an axis file like any other**: a `Record` hands it over as `dims["entity"]` and [`write_record`](writing.md) writes it, with no derivation step of its own.

- **Membership.** An entity exists because it has a row here.
- **Its tombstone.** Removing an entity is a `deleted` row here, so the fold reads every entity tombstone from this file.
- **Its constant values.** An attribute over `entity` alone is a column of this file ([where a value lives](#where-a-value-lives)).

**One row per entity.** An entity is one label of one axis, so a name has one row and one set of constant values.
Its type is a row of the `entity_type` [group](schema.md#types), keyed by `entity` in `groups/entity_type.parquet`. The group is functional, so an entity has one type, and a `Bus` and a `Generator` cannot share a name.

The [entity axis](#the-entity-axis) folds from `dims/entity.parquet`, and so do entity tombstones. Both must read the same source: membership from one and deletions from another would resolve a deletion the fold never saw.

`entity` is the one dim the format knows by name. A NULL there is a value that belongs to no entity, so it never [broadcasts](record.md#the-broadcast-rule).

A modelling framework that scopes names per type reconciles them before it writes.
Its [tool](tools.md) reports a clash rather than the record layer renaming to hide it: a record's `entity` is the framework's own name, and a record that renamed them would hand back components the framework cannot find.

## Where a value lives

Decided by the attribute's [declared `dims`](schema.md#attributespec), not by a particular value.

The rule: **an attribute naming exactly one addressing coordinate is a column on that thing's own table; anything more is long rows in `inputs/`.**

| `dims`                       | lands in                                                          |
| ---------------------------- | ----------------------------------------------------------------- |
| `{"entity"}`                 | `dims/entity.parquet` — a column of the entity axis itself        |
| `{"entity_type"}`            | `dims/entity_type.parquet` — the axis file                        |
| `{"connection"}`             | `groups/connection.parquet` — the group's own file                |
| `{"scenario"}`               | `dims/scenario.parquet` — the axis file                           |
| `{"country"}`                | `dims/country.parquet` — the axis file, a dim shadowing the group |
| `{"entity", "snapshot"}`     | `inputs/<attr>.parquet`                                           |
| `{"connection", "snapshot"}` | `inputs/<attr>.parquet`                                           |

So "varying" is not "has dims" but **"has dims beyond its address"**, and one rule covers a component's constant columns, a connection's `role`, and an axis's payload.

- **[The entity axis](#the-entity-axis)** — attributes addressed by `entity` alone: one column per attribute, beside `entity` and `deleted`.
- **A [group](schema.md#groups)'s file** — attributes addressed by that group alone, PyPSA's `role` on a connection being one.
- **An axis file** — attributes addressed by one dim alone. A snapshot weighting is a number per snapshot and belongs to no component, so `dims/snapshot.parquet` carries it as a declared column with a `dtype`, a `default` and a `description`. A per-type icon is a column of `dims/entity_type.parquet` in the same way.
- **`inputs/<attr>.parquet`** — every attribute addressed by more than its own coordinate, even where a given component's value happens to be constant.
  That component is then a broadcast row, with the varying dims NULL.

So the constant values of an entity come from both: its columns on the entity axis, and its broadcast rows in the varying files.

A [group](schema.md#groups)'s rows are in `groups/<group>.parquet`, keyed by that group's coordinates and carrying their own tombstones — `groups/connection.parquet` for the `connection` group over `(entity, bus)`.
A record with no such file has no rows of that group.

**One file per group, never split by type.** A group's rows are keyed by its coordinates, and a type is not one of them, so `groups/connection.parquet` holds the connections of every type.
The `entity_type` [group](schema.md#types) is no exception: its rows are `groups/entity_type.parquet`, keyed by `entity`.

A functional group's [`into`](schema.md#into-a-group-that-classifies) label is a column of the group's file like any coordinate, so `groups/country.parquet` is `bus | country`. No column on the classified axis: `dims/bus.parquet` does not gain a `country`, the relation being the group's own file. That costs a join where a column read would have done, and buys a uniform rule — `into` decides nothing about storage, so nothing in the layout branches on it.

## The long schema

Every `inputs/` and `outputs/` file carries its attribute's own coordinates, then the columns every row has:

```text
<coordinate> ... | attribute | breakpoint | value
```

The coordinates are what the attribute's [`dims`](schema.md#attributespec) declare, with a [group](schema.md#groups) expanding to its coordinate names — so `inputs/efficiency.parquet` over the `connection` group carries `entity | bus`, and `inputs/p_max_pu.parquet` over `entity` and `snapshot` carries `entity | snapshot` and no `bus`.
An attribute over one dim alone has no file here at all: it is [a column of that dim's own table](#where-a-value-lives).

**Per attribute rather than schema-wide.** One attribute is one file, so one column set per file; a fixed prefix of `entity | bus` would put an all-NULL `entity` on a record-level weighting, claiming a component the value has none of, and would privilege one group's spelling of `bus` over every other group's coordinates.

That the shapes differ costs nothing, because `UNION ALL BY NAME` supplies NULL for a column a file does not carry — which is also what lets a file written before a dim was declared still read back correctly ([resolving a relation](read-path.md#resolving-a-relation)).
The fold's _key_ is uniform even though the files are not: it is [`partial_dims`](schema.md#partial-the-granularity-of-an-override) plus `attribute`, one fixed tuple over every attribute, and a coordinate an attribute does not carry reads as NULL there.

One attribute per file, so `value` carries that attribute's dtype.
There is **no `entity_type` column** on an attribute over `entity`: `inputs/p_max_pu.parquet` holds the `p_max_pu` rows of every entity, keyed by `entity`. A reader that wants the rows of one type joins the `entity_type` group on `entity`.

An attribute addressed by a group alone is not here either: it is a column of that group's own file ([where a value lives](#where-a-value-lives)) rather than a long row. PyPSA's `role` on a connection is the case.
