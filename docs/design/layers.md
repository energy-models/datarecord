<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Layered resolution

A `Record` resolves a tree of layers.
Each node adds one layer; a node's data is its layer resolved over its ancestors', last-writer-wins.

```python
class Revision(BaseModel):
    id: UUID
    parent: UUID | None  # None for a root
```

Each record has one layer at `layers/<id>/`; the path derives from the UUID rather than being stored.
Caches derived from the ancestry rather than from layer data live in a `resolved/` subdirectory of it, so the layer's own top level stays exactly what that layer wrote.

Nodes form a tree: branching is several children sharing a parent, and they share the parent's layer and all further ancestry by pointing at it, not by duplication.
Resolution order along a root→node path is ancestry order; a node's own layer is last and wins.

The node metadata — `(id, parent)` — is persisted in the `revisions` table, so a revision id resolves to its ancestry, and thus its layer set, through it.

Two of [the protocol's](record.md) columns take their meaning from the overlay key.

The dims a [relation](schema.md#relations) is keyed by are part of the key of `attributes/`, `(*partial dims, attribute)`, because [`partial`](schema.md#partial-the-granularity-of-an-override) must name them — `port` among them for the `port_bus` relation. An attribute not over `port` is NULL there, and the key is compared NULL-safely, so that case is unaffected.
That is what makes a per-port attribute owned _per port_: without it, a patch changing one port's `efficiency` would own — and so have to restate — every port's.
It is also why a port is keyed by its own label rather than by position: a patch layer would otherwise have to know a port's current index, so an ancestor inserting one earlier would silently redirect that patch to a different bus.

`breakpoint` ([wide and long rows](record.md#wide-and-long-rows)), by contrast, is deliberately **not** part of the overlay key.
A layer owns a whole curve, the same rule a non-`partial` dim follows ([partial](schema.md#partial-the-granularity-of-an-override)), so a parent's breakpoints and a descendant's can never resolve into one curve with a hole.

## A layer's data is write-once

A node has no mutable state.

A layer's data is created by one act — [`write_record`](writing.md), whether called directly or by a [commit](working-record.md#committing) — which refuses an existing directory and stages into a sibling path, so a layer is complete when it first becomes visible and never changes afterwards.
Editing needs no mutable layer: edits accumulate in [the staging area](working-record.md#staging) and become a layer at commit.

The one thing written into a layer directory after that act is its [`resolved/` cache](#materialised-node-caches), which is why the invariant is stated over the layer's _data_ rather than over the directory.
That is not a loosening that costs anything: a cache is derived from immutable layers, so writing one cannot change an answer, and nothing downstream reads it as data.

Two properties follow. **Any node may be a parent**, since an immutable base cannot shift under its descendants.
And **a cache never needs invalidating**, since one derived from immutable layers cannot go stale.

## Materialised node caches

A node's owner map and resolved dims may be materialised under `layers/<id>/resolved/`.
Not the schema: there is [one for the whole record](schema.md#one-schema-per-record), so there is nothing per node to resolve.

Where a materialised map exists, a read stops there: the map is already folded over everything above it, so a read walks the ancestry only back to the nearest materialised node rather than to the root.
That truncation is what keeps a deep chain cheap.
The same rule decides where resolved dims come from — a node's own `resolved/dims/` where materialised, its raw `dims/` otherwise — answered by the cache's presence rather than by any recorded state.
The two are distinct paths within one directory, so a record read as an ancestor and the same record read as itself never alias.

Materialising is a policy: every N layers, at a branch point, on demand.
It is purely additive, writing files under `resolved/` and changing no answer, only how many layers a read touches to reach it.

## Deletion

A `deleted = true` row on [the entity axis](format.md#the-entity-axis) tombstones a component from every attribute, and from every value of every dim — [existence does not vary along one](schema.md#existence-does-not-vary-along-a-dim), so there is nothing to scope a deletion by.
A `deleted = true` row in `relations/<relation>.parquet` tombstones one row of that relation — the row itself and its `attributes/` rows — leaving the component and its other rows intact, so a connection is removed without touching the component it attached.

A tombstone is honoured by the [one fold](read-path.md#one-fold-for-every-axis) that resolves every axis: the deepest statement of a key wins, and where it is a tombstone the key leaves the resolved relation (a deeper restatement reviving it).
An _attribute's_ orphaned rows stop surfacing the same way: an attribute row is keyed by the labels of the dims in the fold key, and [`fold_inputs`](read-path.md#owner-map) anti-joins the map against each of those axes' `deleted` rows as it folds — so a key whose entity, port or other label was deleted is absent from the resolved map, not filtered at read.
A removed label also takes the relation rows keyed on it: a removed component loses its connection rows and its `entity_type` row, and a removed bus loses the connection rows that name it.
A tombstone only affects the branch that carries it; sibling branches keep the component.

The fold treats an absent `deleted` column as "tombstones nothing", so a layer may be any standard parquet directory, not only one this package wrote.
Every derived cache lives under `resolved/` for the same reason: every glob the read path issues into a layer is single-level — `attributes/*.parquet`, `dims/*.parquet` — so nothing under `resolved/` is reachable by one.
A reader pointed at a layer directory therefore sees exactly what that layer wrote, and materialising a node's caches never changes what it sees.
