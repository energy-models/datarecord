<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# A tree of layers

A revision adds one layer on top of its parent, and the revisions form a tree.
A layer's data is written once, so any revision can be a parent and every cache stays valid.
Reading a revision folds its ancestry from the root, and a deeper layer's row wins where it crosses a shallower one.
A layer restates each axis it touches as a whole, a [multi-dim](../model/multi-dims.md)'s tuples included.
For attributes, `partial` sets what one layer may override.
Along a partial dim, a layer overrides one value per label.
Along any other dim, it restates the whole extent of each key it touches.

```python
class Revision(BaseModel):
    id: UUID
    parent: UUID | None  # None for a root
    store: Store  # serialised as its location
```

A revision's layer sits at `<location>/layers/<id>/`.
The path derives from the id and the revision's [store](../storage/store.md#the-store), so the revision stores no path.
Branching is several revisions sharing one parent.
The children point at the parent, so they share its layer and all further ancestry.
Each revision's `(id, parent)` is a file [beside the layers](../storage/store.md#the-tree-beside-the-layers), so the location alone resolves a revision id to its ancestry and its layers.

The fold reads the layers in ancestry order along the path from the root, and the revision's own layer comes last.
[Resolution](resolution.md) is how it settles each key, and [removing labels](removal.md) is a restatement of the axis without them.

## A layer's data is write-once

A revision has no mutable state.
One act creates a layer's data: [`store.root` or `rev.child`](../use/writing.md).
`rev.child` is also how a `WorkingRecord`'s [edits](../use/editing.md#edits) become a layer.
The write refuses an existing directory and stages into a sibling path, so a layer is complete when it first becomes visible.
Its [revision file is written last](../storage/store.md#a-revision-exists-only-by-its-file), and that file makes the layer reachable.
Editing needs no mutable layer: edits accumulate in [the staging area](../use/editing.md#staging) until they are written as a child.

After that act, the only thing written into a layer directory is its [`resolved/` cache](resolution.md#materialised-node-caches), so the invariant covers the layer's data.
A cache derives from immutable layers, so writing one changes no answer, and no read takes it for data.
Every glob the read path issues into a layer is single-level: `attributes/*.parquet` and `dims/*.parquet`.
No glob reaches `resolved/`, so a reader pointed at a layer directory sees exactly what that layer wrote.

Two things follow.
Any revision may be a parent, since an immutable base cannot shift under its descendants.
A cache never needs invalidating, since a cache derived from immutable layers cannot go stale.

## `partial`: the granularity of an override

A layer exists to override, and every value is overridable.
`partial` decides how finely a layer overrides along each axis.
That question differs from the one [`AttributeSpec`](../model/attributes.md#attributespec) answers, because each belongs to a different thing:

- Which dims may an attribute vary over?
  This belongs to the attribute, as `AttributeSpec.dims`.
  `p_max_pu` varies over scenario and snapshot, and `p_nom_opt` over neither.
- May a layer patch single values along an axis, or must it restate the axis whole?
  This belongs to the dim, as `schema.partial`.
  A layer patches `scenario` value by value, and restates `snapshot` whole for every attribute.

```python
partial = {"scenario"}  # snapshot absent, so a patch restates the series
```

A layer that touches one value along a dim outside `partial` restates that key's whole extent along the dim.
Overriding one snapshot of `p_max_pu` therefore carries the entity's entire series, which a [`WorkingRecord`](../use/editing.md#edits) stages beside the edit.
Ownership itself follows reach: a row reaches every coordinate no more specific row covers, whichever layer wrote it ([what a row reaches](resolution.md#what-a-row-reaches)).
A partial series would resolve into a mix: the layer's values at the snapshots it wrote, and its parent's at the rest.
A framework that splits constant from varying data cannot receive half a series.
That holds for every attribute over the axis, so the rule belongs to the axis.

The dims a layer owns an attribute per follow from the declarations:

```text
owned_per(attribute) = attribute.dims ∩ partial_dims
partial_dims         = the dims in schema.partial, multi-dims included
```

`partial_dims` is the fold key: the dims that `partial` names, multi-dims included, in declaration order.
A schema that patches entity by entity lists `entity`, as PyPSA's `{entity, bus, scenario, connection}` does.
A layer that patches a per-bus `marginal_price` one bus at a time needs `bus` in `partial`.
A layer that patches one connection's `efficiency` needs `connection` there, and the connection's bus is patched with it inside the `connection` column.
Under PyPSA's `partial`, `p_max_pu` is owned per entity and per scenario, so a patch to one hour restates that entity and scenario's whole series.
`marginal_cost` is owned per entity and scenario, and `p_nom_opt` per entity, once across everything else.

Splitting the two questions lets the schema tell `p_max_pu` from `p_nom_opt`.
A flag on the dim alone would make every attribute owned per scenario, including the ones a scenario must not change.
A `p_nom_opt` row that carries `scenario` is refused at write, since `p_nom_opt` is not addressed by `scenario` ([the long schema](../storage/format.md#the-long-schema)).
A first-stage decision turned into a per-scenario one is the error worth catching.

The owner-map key is `(*partial_dims, attribute)`, one fixed tuple for every attribute.
An attribute that does not vary over a key coordinate writes NULL there, and the key compares NULL-safe.
A multi-dim in `partial` sits in the key as its struct column, so a per-connection attribute is owned per connection.
With the multi-dim outside `partial`, a patch to one connection's `efficiency` restates every connection's value.
A tuple is keyed by its labels, so an ancestor that inserts a tuple earlier cannot redirect a patch.

`breakpoint` ([wide and long rows](../model/attributes.md#wide-and-long-rows)) is outside the overlay key.
A layer owns a whole curve, as it restates a whole extent along a dim outside `partial`.
So a resolved curve never mixes a parent's breakpoints with a descendant's.

`partial` governs attribute ownership alone.
A layer that touches an axis file restates it whole, every label with its properties in order, whatever `partial` says ([removing labels](removal.md)).
The [properties an axis carries](../storage/format.md#where-a-value-lives) are no reason to add it to `partial`, with one exception.
A property [categorizes](../model/properties.md#categories) its dim only when the dim is in `partial`.

Keep `partial` small.
Every `partial` coordinate widens the fold key, and every read of every attribute pays for the key.
Leaving a coordinate out costs once per edit, bounded by the size of the axis.
Putting it in costs every read.
PyPSA's `partial` names only the dims a layer patches value by value: `entity`, `bus`, `scenario` and `connection`.
`snapshot` and `carrier` stay out.
A layer patches one bus's `v_nom`, and restates every carrier's `co2_emissions`, a few dozen rows.
