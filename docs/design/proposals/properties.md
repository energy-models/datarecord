<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Proposal: properties — metadata declared apart from data

Status: **Draft** · Drafted 2026-10-12

Two changes that are one. **Properties** — what classifies or describes one thing, its metadata — get their own section of the manifest, and **attributes** keep the data, however few coordinates address it; the section decides where a value is stored and read, not the count of its `dims`. And the entity-type axis stops being a dim with a group over `entity`: it becomes an ordinary property marked as a **discriminator**, which per-kind defaults and trait scoping key on.

## What starts it

**Where a value is read depends on how many coordinates address it, not on what it is.** `p_nom` and `carrier` are both declared under `attributes` with `dims: [entity]`, so both land on the entity axis — and `record.attributes` hands over neither, since it [holds long frames only](../record.md); a caller finds them on `record.dims["entity"]` or `record.entity_types["Generator"]`. Yet `p_nom` is data a model optimises against, and `carrier` a label describing the generator. The rule placing them — [exactly one addressing coordinate](../format.md#where-a-value-lives) — groups the two by an accident of shape.

The same rule makes a compatible schema change move a file. Widening `p_nom` to `dims: [entity, scenario]` is [compatible](../schema.md#versioning) because NULL broadcasts, but it moves `p_nom` from a column of the entity axis to `inputs/p_nom.parquet`, so the rows a layer wrote before are in a file the new schema no longer reads it from.

**The entity-type axis is a special case at every layer.** It is a dim, plus a group over `entity` `into` it, whose rows are [the entity axis file rather than a `groups/` file](../format.md#where-a-value-lives) — the one exception to a file per group. Its name is reserved as an attribute name. Whether it is declared decides the [layout of the entity axis](../format.md#the-entity-axis): per-type member files under `dims/entity_type/` in one case, columns of `dims/entity.parquet` in the other. Traits may be scoped by it and by nothing else. And its `Enum` dtype is what makes it a vocabulary, while a `String`-typed declaration of it is an [open question](a-member-file-holds-values.md#what-it-opens-rather-than-settles).

**And it cannot say what a framework's registry says.** PyPSA defaults differ per component for one name:

| name       | default per type                                                     |
| ---------- | -------------------------------------------------------------------- |
| `sign`     | `1` for Generator, Store, StorageUnit; `-1` for Load, ShuntImpedance |
| `carrier`  | `"AC"` for Bus; `""` otherwise                                       |
| `type`     | `"primary_energy"` for GlobalConstraint; `""` otherwise              |
| `p_min_pu` | `-1` for StorageUnit; `0` otherwise                                  |
| `p_set`    | `0` for Load; `NaN` otherwise                                        |

`AttributeSpec.default` is one value, so today a tool either picks one and writes the others out as data, or loses them. None of the 142 input names differs in **dtype** between types — only in default.

## The construct

### Properties and attributes

```yaml
properties:
  entity:
    entity_type:
      dtype: "Enum[Bus, Generator, Load, Link, Store, StorageUnit]"
      discriminator: true
    carrier:
      dtype: str
      default: ""
      default_by:
        entity_type: { Bus: AC }
    sign:
      dtype: float64
      default: 1
      default_by:
        entity_type: { Load: -1 }
  bus:
    x: { dtype: float64, unit: "°" }
    y: { dtype: float64, unit: "°" }
  connection:
    role: { dtype: str }

attributes:
  p_nom:
    dtype: float64
    dims: [entity]
    default: 0
  p_max_pu:
    dtype: float64
    dims: [entity, snapshot]
    default: 1
  p_min_pu:
    dtype: float64
    dims: [entity, snapshot]
    default: 0
    default_by:
      entity_type: { StorageUnit: -1 }
  weight:
    dtype: float64
    dims: [scenario]
    default: 1
```

**A property is one value per row of one dim or group, its subject**, and is declared under that subject's key — there is no `dims` on a property, and it never varies along another axis. It identifies, classifies or describes the row: a type, a carrier, a sign convention, a coordinate on a map, a connection's role.

**An attribute is data**: what a model reads as part of its problem and what a scenario or a layer would vary. It keeps `dims`, which may name a single coordinate — `p_nom` over `entity`, a scenario `weight` over `scenario` — without that making it a property.

Which a name is, is the author's declaration, not derived. The test is the one above, and a tool reads it from its registry where it can.

**One name, one spec, across both sections.** A name declared as a property and as an attribute is rejected: `add`'s split of a wide frame and [`flags`](../record.md#flags) would have two answers for it. This is the [flat-attributes argument](../schema.md#attributespec) extended to the new section.

### The section decides where a value lives

| declared as           | lands in                                            |
| --------------------- | --------------------------------------------------- |
| property of a dim     | `dims/<dim>.parquet` — a column of the axis file    |
| property of a group   | `groups/<group>.parquet` — a column of its own file |
| attribute, any `dims` | `inputs/<attr>.parquet` — long rows                 |

That replaces [where a value lives](../format.md#where-a-value-lives) as a rule over `len(dims)`. `inputs/p_nom.parquet` is `entity | attribute | breakpoint | value`, one row per entity, and widening `p_nom` over `scenario` adds a column to the same file — NULL in every row written before, which is what [the broadcast rule](../record.md#the-broadcast-rule) needs to read them unchanged.

An axis file then carries only its labels, its `within` keys, its tombstones and the subject's properties; no data.

### `entity_type` is a property, marked as a discriminator

`discriminator: true` says a property partitions its subject's rows into kinds. It must be `Enum`-typed — the labels are what `default_by` and `Trait.on` name, and an unenumerated label set would leave both unchecked. At most one per subject.

That replaces the whole entity-type construct: no `entity_type` dim, no group over `entity` into it, no exception to a file per group, and no reserved name — `entity_type` is a column of `dims/entity.parquet` because every property of `entity` is. A schema whose entities have no kinds declares no discriminator, and nothing else about it differs. This settles the [`String`-typed axis question](a-member-file-holds-values.md#what-it-opens-rather-than-settles): a kind vocabulary is declared as an `Enum` or is not a discriminator.

The schema attaches no meaning to the name `entity_type`; it is what PyPSA and every existing record call it.

### `default_by` — a default per kind

`default` stays the fallback. `default_by` maps a discriminator's name to per-label overrides:

```yaml
default: 0
default_by:
  entity_type: { StorageUnit: -1 }
```

A value read unset — a NULL property column, or no `inputs/` row reaching the entity — reads `default_by.entity_type[<its entity_type>]` if declared, else `default`.

- **On properties and attributes alike.** `p_min_pu` and `p_set` need it as much as `sign` does, and a default is never stored, so it costs no file layout: it is a `CASE` on the discriminator where the value is filled.
- **Keyed by the discriminator's name, not implied.** The discriminator is found through the value's own coordinates: `entity` directly, or through `connection`'s `entity` coordinate for a value over the group. The schema rejects a key that is not a discriminator reachable from the value's address, a label outside its `Enum`, and a key two coordinates could resolve — a `corridor` over `(from, to)` with a discriminator on `bus` has no single kind per row.
- **Only the default varies.** `dtype`, `unit` and `description` are the spec's own and never per kind, which is what keeps one name one column and one `inputs/` file.

### Traits are scoped by a discriminator

`Trait.on` keeps its shape and its spelling — `on: {entity_type: [Generator, Link]}` — and now names the discriminator of `entity` where it named an entity-type axis. Only that discriminator may scope a trait, as only an entity-type axis may today: a discriminator of `bus` gets per-kind defaults, but no per-kind vocabulary. It narrows properties and attributes alike, so [a trait narrows; it does not grant](../schema.md#traits) holds unchanged, and so does every trait in the PyPSA tool.

**`switch` is not subsumed.** A discriminator is a property — one value per entity, never varying — while a switch may vary along other axes: `p_nom_extendable` per `investment_period` is a trait an entity carries in some periods and not others. So `switch` names either a property of `entity` or an attribute addressed by `entity` and further dims, and the [trait-switches restriction](trait-switches.md#what-it-opens) to `dims: [entity]` exactly is lifted. Which kinds _may_ carry a value stays a schema answer read from discriminators; which entities _do_ in a given period stays a question about data.

### One file per subject on disk, a wide frame per kind in the API

**On disk**, every property of `entity` is a column of `dims/entity.parquet`, kinds and all, NULL on a row whose kind does not carry it. `dims/entity_type/<Type>.parquet` goes, and with it the second entity-axis layout [the member-file proposal](a-member-file-holds-values.md#what-it-costs) introduced: the typed and untyped records become one layout, differing by one column.

Per-type files existed because every type has a different column set. The schema now says which columns each kind carries, so a kind's frame is a filter on `entity_type` and a projection to those columns — recoverable from one file, where per-type files would state each entity's kind twice, once in the column and once in the filename. Parquet stores an all-NULL column chunk for almost nothing and DuckDB reads only what it projects. Only a per-kind **dtype** would need per-kind files, and that is ruled out above.

**In the API** each accessor holds one section:

```python
record.dims["entity"]  # entity — labels only
record.properties["entity"]  # entity | entity_type | carrier | sign | ...
record.entity_types[
    "Generator"
]  # entity | carrier | sign | ... — its rows, its properties
record.attributes["p_nom"]  # entity | attribute | breakpoint | value
record.attributes["p_max_pu"]  # entity | snapshot | attribute | breakpoint | value
```

`record.entity_types` keeps its name and narrows its content: frames keyed by the labels of `entity`'s discriminator, each projected to the properties that type carries with `default_by` applied — no attribute columns. A framework's static frame — PyPSA's `n.generators` — is that frame joined with the entity-only rows of its attributes, which is how [the constant frame is assembled](../format.md#where-a-value-lives) from `inputs/` dim-NULL rows already; the join now carries `p_nom` too.

### `attributes_for` splits

`Schema.attributes_for(ctype)` answers which names a type carries. Under two sections its callers want one or the other: `flags` reads attributes, `record.entity_types` reads properties, and only `add`'s wide-frame split wants both.

So it becomes `properties_for(label)` and `attributes_for(label)`, both narrowed by the same traits, and `add` calls the two.

## What this buys

**Each section answers where its values are read.** `record.properties[subject]` for `properties.<subject>`, `record.attributes` for `attributes` — `p_nom` included, which is the confusion this starts from.

**Widening an attribute's `dims` is compatible in fact.** One attribute is one `inputs/` file whatever its `dims`, so a widened attribute adds a column rather than moving files.

**The entity-type special cases go.** The dim, the group into it, the group-file exception, the reserved name, the `into`-on-`entity` rejection rule, and the two entity-axis layouts — all of them were there to make one property of `entity` special.

**A registry's per-type defaults round-trip.** `sign`, `carrier`, `p_min_pu` and `p_set` are declared as PyPSA defines them instead of written out as data or lost.

## What it costs

**Entity-only data reads through the fold, not the axis.** `p_nom` was a column resolved with the entity row; it is now long rows keyed by the [fold key](../schema.md#partial-the-granularity-of-an-override) like any attribute. One row per entity, and the same fold every varying attribute already takes — but a static frame of forty columns is forty attribute reads and a pivot where it was one file.

**The choice is the author's, and can be wrong.** Nothing derives whether `x` on a bus is metadata or data; a schema declaring it either way is valid. Moving a name between sections moves its file and is an [incompatible change](../schema.md#versioning).

**A per-type value addressed by the type alone loses its home.** An `icon` per entity type lived on `dims/entity_type.parquet`; with no `entity_type` dim there is no file keyed by kind. Nothing in the tools declares one; if a case appears, it is a property of a dim declared for the purpose.

**A default is no longer a constant.** Filling an unset value is a `CASE` on the discriminator, which needs the discriminator joined in where the value is read — for an attribute, the resolved entity axis on `entity`, a join the per-kind view already does.

**Every manifest is rewritten**, and every record re-exported: properties move sections, entity-only data moves from axis columns to `inputs/`, and the entity-type dim and group become one property. [Breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). The PyPSA tool needs a registry-to-section mapping, which its registry does not carry and so is authored, as [traits](../schema.md#traits) are.

## What it opens

**Whether a functional group over one coordinate is a property.** `country` over `[bus]` `into` `country` is one label per bus — metadata, of a dtype ranging over a dim. The old entity-type axis was exactly that, and this proposal turns it into a property; `into` might go the same way, or [the chain argument](../schema.md#within-is-not-into) might keep it a group.

## How to know it worked

**`p_nom` is in `record.attributes`, and `inputs/p_nom.parquet` exists with one row per entity.**

**`dims/entity.parquet` holds labels, tombstones and properties only** — no column the schema declares under `attributes`.

**No `entity_type` in `schema.py` outside docstrings and tests.** The name is only data — a property a schema happens to declare.

**A typed and an untyped record differ on disk by one column.** Write the same entities with and without a discriminator: same files, `dims/entity.parquet` with or without `entity_type`, no `dims/entity_type/` in either.

**PyPSA round-trips `sign` and `p_min_pu` with nothing written.** An `add` of a Load and a StorageUnit setting neither reads back `sign == -1` and `p_min_pu == -1`, with no rows in `inputs/p_min_pu.parquet` and NULL in the `sign` column.

**Widening `p_nom` over `scenario` reads an old layer's values back.** Written under `dims: [entity]`, read under `[entity, scenario]`: every scenario sees the old value.
