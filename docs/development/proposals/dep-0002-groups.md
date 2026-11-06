<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 2: groups, one construct for classification and sparsity

| DEP           | 2                                                                      |
| ------------- | ---------------------------------------------------------------------- |
| Title         | groups, one construct for classification and sparsity                  |
| Author        | Jonas Hörsch                                                           |
| Status        | Superseded (2026-10-26, implemented 2026-09-01)                        |
| Created       | 2026-08-27                                                             |
| Version       | v0.1.0a1                                                               |
| Superseded-By | [DEP 23](dep-0023-multi-dims.md) (its groups, which become multi-dims) |

The design landed in [the data model](../../reference/data-model.md) and [the record format](../../reference/format.md), which win where they and this DEP differ.

## Abstract

A group with an optional `into` replaces both the group and the `on` mapping of [DEP 1](dep-0001-dims-groups-traits.md). Every group is one file, `groups/<name>.parquet`. `into` declares that each tuple of `over` has one label of a declared dim. The group is then checkable, and a reader can aggregate along it with `by=`.

## Motivation

The schema declares "which tuples over dims exist" twice. A `Group` states it directly, and `Dimension(on=...)` states it for the case where each label appears once. The two are stored, addressed, aggregated and validated by different code, for the difference of one uniqueness constraint.

A modeller pays for this when writing maths. Summing along a mapping is `GROUP BY country`, while summing along a group must say which coordinate survives. GAMS, Calliope and PyPSA all use one construct for both, so the split is datarecord's and not the domain's.

## Specification

- `groups` absorbs `Dimension.on`. `Dimension.on`, `Dimension.mapping` and `Schema.mappings_on` are deleted.
- `over` declares the coordinates, as a list where coordinate names equal dim names, or as a dict where they do not. The list is sugar for the dict.
- `into` is the name of a declared dim. Each tuple of `over` has exactly one label of `into`, checked on write. `into` may sit over several coordinates, such as `over: [bus, scenario]`.
- Without `into`, a group is a set of sparse tuples, is not a dim and never appears as a column.
- A group may share its name with a dim. The rule against the two colliding is dropped.
- In an attribute's `dims`, `X` is the dim `X` if one is declared, and otherwise the group `X` expanded to its coordinates. `into` is not consulted.
- Every group's rows are `groups/<name>.parquet`, keyed by its coordinates. The `dims/<group>/<Type>.parquet` split and the mapping column on the classified dim's axis file are gone, and `Schema.group_columns` no longer leads with `entity_type`.
- A reader's maths language collapses coordinates with `over=<group>[coords]`, whose brackets list what goes. The bracket is required on every `over=` over a group of several coordinates. `by=<group>` keeps the group's `into` dim and collapses all of `over`, and exists only where `into` is declared.
- A NULL group coordinate still means the group's rows, and a group in `dims` still expands to its coordinates. The record does not project across levels. `within` and traits are unchanged.

## Rationale

`into` is the declaration that was missing. Without it, a group can only happen to be single-valued, and a duplicate row is an error the schema has no name for. GAMS and Calliope both leave functional lookups undeclared, so a duplicated label silently double-counts in every sum. A schema meant to make the shape of data checkable should close that gap.

`into` must be a declared dim because a functional group needs the axis file. It gives the labels an order and a place for a per-country CO2 budget. The argument for the axis file in DEP 1 stands unchanged.

Sharing a name is the natural spelling for a group with `into`. `dims: [country]` finds the dim and `by=country` finds the group, so a rule against sharing would force a name like `country_map`. Addressing resolves dims first, so the case without `into` is not ambiguous either. The cost is that a shadowed group without `into` loses its only spelling in `dims` without an error.

`group` stays the word, because `country` groups buses too. `GROUP BY`, PyPSA's `group_constraints` and Calliope's `group` overload it, so prose about aggregation says which is meant.

One file per group gives the no-denormalisation property of DEP 1 for free: a layer that restates a group restates one file. The `<Type>` split put a `Link`'s and a `Line`'s connection rows in two files with identical keys. It had no type to split on for `corridor` or `contract`.

A relation between two entities, such as a contract between a seller and a buyer, gets a file, an aggregation spelling and an optional `into`. `over: [entity], into: parent` declares a hierarchy of components, which `Dimension.on` could not express.

`by=` and `over=` are two keywords because they mean opposite things. A bare `over=country` read as "keep country" would give one keyword two senses. The bracket is required because a functional group needs it wherever a coordinate must survive that `by=` would collapse: `over=country[bus]` on `over: [bus, scenario]` keeps a per-scenario country total. Inference from the enclosing scope also fails under nesting. In `sum(sum(x, over=connection), over=siting)`, the inner sum would collapse `bus`, which the outer sum needs, and return the right shape with the wrong number.

The change costs a rename across the tree and a format migration with no reader of both layouts. Reading a bus's country becomes a join instead of a column read. `WorkingRecord`'s `kind=` and the `CONNECTION` constant in `mutable.py` stop naming one group specially.

## Usage

```yaml
groups:
  country:
    over: [bus]
    into: country
  connection:
    over: [entity, bus]
  corridor:
    over: { from: bus, to: bus }
attributes:
  efficiency:
    dims: [connection, timestep] # entity | bus | timestep
  co2_budget:
    dims: [country] # the dim
```

`sum(x, over=connection[entity])` keeps `bus`, and `sum(x, by=country)` keeps `country`.

## Implementation notes

- `into` is sugar resolved at parse: `Group(over=["bus"], into="country")` returns `["bus", "country"]` from `coordinates`. The file layout, the fold's key and `dims` expansion read `coordinates` and never branch on `into`.
- The model keeps `into` for the uniqueness constraint, whose key is `coordinates` minus `into`, for `by=`, and to write the manifest back as authored.
- A validator beside the `over` check refuses an `into` that is no declared dim. Otherwise `dims: [country]` would expand silently to `bus | country`. The collision check goes.

## Rejected ideas

- A group whose `into` is a label set of its own: it loses the axis file.
- Keeping the rule against a group sharing a dim's name: a lint naming the shadowed group is the better answer.
- Restricting `into` to one coordinate: needed only if a functional group were a column on an axis file, which is not kept.
- Overloading `over=` for "keep": one keyword would have two senses.
- Inferring the bracket from the enclosing scope: wrong under nesting.

## Open issues

- Whether a shadowed group warrants a lint at load.
- Whether a group's rows are checked against the labels of its coordinate dims, as GAMS does for a relation.
- Whether every aggregation a reader needs factors into a declared dependency plus a reader's operation. This is untested against a second reader.
