# Proposal: a member file holds values, and nothing else

Status: **Draft** · Drafted 2026-09-02

Two claims about `dims/entity_type/<Type>.parquet`, which are one claim: that file holds a type's attribute values, and everything about a component's _existence_ belongs to the [entity axis](../format.md#the-entity-axis).

Today it holds more. A record whose schema declares no entity-type axis still writes one member file per type — types that are not declared, not validated and not constrainable, yet decide which file a value is in, so a reader must know them to reach it. And every member file carries `deleted`, which the fold reads as a tombstone alongside the axis's own, so a removal is written twice to say one thing.

This proposal puts those values on `dims/entity.parquet` where the schema declares no entity-type axis, takes `deleted` off the member file in every record, and makes `entity_type` conditional in the [owner map](../read-path.md#owner-map) as well as in the file — the map's shape being where the column is currently asserted rather than derived. What is left in a member file is `entity` and one column per attribute.

## What starts it

[The schema](../schema.md#entity_type-the-axis-of-kinds) is explicit that the axis is optional, and that omitting it makes the labels data:

> The entity-type axis's enum categories, so a schema declaring it as a plain `String` has none: the labels are then data rather than declarations, and `attributes_for` accepts any of them.

`tests/test_untyped.py` exercises that: a schema with two attributes over `entity` and no classifying group, components added under `thing` and `other`, and both round-trip. So far so good.

The problem is where their values land. `p_nom` is `dims={"entity"}` and non-varying, and the format gives that exactly one home — [`dims/entity_type/<Type>.parquet`](../format.md#where-a-value-lives). It cannot be a column of `dims/entity.parquet`, because `attributes_on("entity")` returns `()` by explicit early return; it cannot be an `inputs/` row, because it does not vary. So a label the schema never declared becomes a filename:

```
dims/entity.parquet             entity, entity_type, deleted
dims/entity_type/thing.parquet  entity, p_nom
dims/entity_type/other.parquet  entity, p_nom
```

**`entity_type` there is a column no declaration accounts for.** Every other column of an axis file answers to something: its key comes from `axis_key`, its attribute columns from `attributes_on`, `deleted` and `order_key` are structural. This one is admitted by a special case in `_validate_frame` and typed as a plain `String` by a fallback in `_axis_columns`, because there is no `Dimension` to read a dtype from. It is the one column in the format whose presence is not derivable from the schema.

## Why it cannot simply be dropped

The obvious move — no declared axis, no `entity_type` column — was tried and does not close. The label is in the components [owner map](../read-path.md#owner-map), and five things read it there:

| reader                        | what it does with it                                      |
| ----------------------------- | --------------------------------------------------------- |
| `NodeCache.entity_types()`    | `distinct_values(entity_map, "entity_type")`              |
| `NodeCache.entity_type_frame` | filters the map to one type, then reads its file          |
| `NodeCache.attributes_of`     | filters the map to one type, to scope the flags           |
| `_fold_ordered`'s `typed` arm | projects it, then `any_value` in the components aggregate |
| `schema.entity_columns`       | hardcodes it into the map's column set                    |

`entity_columns` is the one that makes this structural rather than local. It is the map's declared shape, used in three places — the fold, `_empty_relation` for a record with no rows, and the **materialised map on disk** (`resolved/owner_map/entities.parquet`). So the column exists whether or not anything fills it, and gating only the axis file leaves `entity_types()` yielding `{None}` rather than `{thing, other}`.

Deriving the label from the filenames instead works, but costs a `LayerSource.entity_types()` listing member and a per-layer `LIST` on the read path — which is what [the entity axis exists to avoid](../format.md#the-entity-axis). Trading a stored string for a directory listing per layer per fold is the wrong direction.

So the label is load-bearing _given the current layout_. The layout is what this proposal changes.

## The construct

**Where a schema declares no entity-type axis, a non-varying attribute over `entity` is a column of `dims/entity.parquet`.** No `dims/entity_type/` directory, no per-type files, and `entity_type` is then genuinely absent — nothing classifies, so there is nothing to classify into.

```
dims/entity.parquet   entity, deleted, p_nom, ...
```

Which is what the entity axis already is for every other purpose: the file keyed by `entity` that carries what is true of a component. `attributes_on`'s early return — "Never `entity`, whose sole-coordinate attributes are the _component_ frame's columns" — becomes conditional on a type axis existing, which is the one line of schema that decides the whole layout.

### The column leaves the owner map too

The layout change is not enough on its own: the map's shape is `schema.entity_columns`, so **that** is where the column has to become conditional.

```python
# today
return ("entity_type", "entity", "layer_uuid", "order_key")
# proposed
return (
    *(("entity_type",) if self.entity_type_dim else ()),
    "entity",
    "layer_uuid",
    "order_key",
)
```

One property, and the five readers follow from it:

- **`entity_types()`** returns the empty set — `distinct_values` is not called, rather than called on a column of NULLs.
- **`entity_type_frame(ctype)`** has no type to be asked for. `Record.entity_types` iterates an empty mapping, so nothing reaches it; a direct call is a caller error, and should say so rather than filter on a missing column.
- **`attributes_of(ctype)`** scopes the flags by type, which is meaningless without types. Its semi-join to `of_type` becomes the whole map — every component, which is what "no types" means for a per-type question.
- **`_fold_ordered`'s `typed` arm** is skipped. This is the state the flag was written to express: `typed = "entity_type" in columns` already reads the column set rather than assuming, so it needs no edit at all once `entity_columns` is conditional.
- **`_empty_relation`** builds from the same tuple, so it follows for free.

**The materialised map is the consequence to be deliberate about.** `resolved/owner_map/entities.parquet` is written from this column set, so the cache of a record with no declared type axis stops carrying `entity_type` — and a cache written before the change has it. [Materialised node caches](../layers.md#materialised-node-caches) are derived data that can be rebuilt, so this is a rebuild rather than a migration; but a stale cache read against the new column set is the failure mode to check for, and it argues for the cache carrying a schema version or being invalidated on schema change. This proposal does not settle that, and it is the second thing to look at after the `ResolvedLayer` prerequisite.

### `deleted` belongs to the axis alone

A member file carries `deleted` today, and the fold reads it from there as well as from the entity axis. That is the same duplication the axis exists to remove: [membership is the axis's](../format.md#the-entity-axis) — "a component exists because it has a row here, not because some type's file mentions it" — and a tombstone is a statement about membership.

It shows up as a coupling in the staging area. `remove` has to write twice, once to the axis and once to the type's member table, because a removal that left no member row would read as a member the layer never mentioned. Two writes to say one thing, kept in step by hand.

So this proposal also takes `deleted` off the member file: a member file holds `entity` and that type's columns, nothing else. `remove` then writes one row, on the axis, and the fold reads tombstones from one place.

The two changes belong together because they are the same claim about that file — a member file holds _values_, and everything about a component's existence is the axis's. Separately each looks like a detail; together they leave the member file with no column that is not an attribute.

### What a schema declaring the axis does, unchanged

A schema declaring the axis keeps today's layout exactly: `dims/entity_type/<Type>.parquet` per type, `entity_type` on the entity axis, typed by the declared `Dimension`. This proposal adds no case there; it removes one from the side where nothing declares the axis.

### Every source and every producer

The layout is one thing; what each implementation of it must do is another, and the change is only coherent if all of them agree. There are four `LayerSource`s and two record-side producers.

| implementation                   | `axis("entity")` under this proposal              | `entity_type(name)`              |
| -------------------------------- | ------------------------------------------------- | -------------------------------- |
| `_FileLayer` (shared)            | `dims/entity.parquet`, now with attribute columns | `None` — no such directory       |
| `ParquetLayer`                   | inherited                                         | inherited                        |
| `ResolvedLayer`                  | `layers/<id>/dims/entity.parquet` — **see below** | inherited, from `layers/<id>/`   |
| `DirectorySource`                | inherited                                         | inherited                        |
| `StagedSource` / `WorkingRecord` | `_ENTITY_AXIS` table, now with attribute columns  | `None` — no member tables staged |
| `PyPSA` (tool)                   | `dims["entity"]`, its member frames merged in     | `entity_types` empty             |

Three of the four sources need no code change at all: `_FileLayer` reads whatever columns the file has, and `ParquetLayer` and `DirectorySource` inherit it. `entity_type(name)` answering `None` follows from the directory being absent, which `try_read_parquet` already handles. That is the test of whether the layout is right — a file-backed source should not have to know which configuration it is reading.

**The `PyPSA` tool** currently supplies `entity_types` per type and, since the entity axis became a supplied member, `dims["entity"]` from `_entity_axis_frame`. Under this proposal an untyped export merges the member columns into that frame and stops supplying `entity_types` — but PyPSA always has types (`n.components`), so it is unaffected in practice. It matters as a statement about the protocol rather than about this tool: a producer with no types has one axis frame to fill and no `entity_types` mapping, which is exactly what the `Record` protocol should let it say.

### The prerequisite: `axis` means one thing on every source

`ResolvedLayer` overrides `axis` and `axes` to read `resolved/dims/` — the _folded_ answer, standing for everything above this node — so `axis(dim)` means something different on it than on every other source, where it means "this layer's own rows".

That is invisible today because all three of its callers want the folded answer wherever a `ResolvedLayer` can reach them:

| caller                               | role      | folded is correct because                       |
| ------------------------------------ | --------- | ----------------------------------------------- |
| `resolve_dims` (`axes`, `axis`)      | fold seed | the sources above this node are not in the list |
| `_component_deleted_for_connections` | fold step | same                                            |
| `fold_entities`                      | fold step | same                                            |

This proposal adds a fourth caller in a **second** role: `_owned_frame` reads the rows behind a key the map says a layer owns, and for a `ResolvedLayer` that row is in `layers/<id>/`, not in the cache beside it — which [its own docstring already states](../layers.md#materialised-node-caches). Today `_owned_frame` reaches member rows through `entity_type(name)`, which is inherited and layer-local, so the two roles never meet. Move those columns onto the axis and they do.

The fix is not to teach `_owned_frame` about folded columns. It is that **`axis` should mean the same thing on every source**, and the folded read belongs to the seed path that wants it:

- `ResolvedLayer.axis`/`axes` become layer-local, inherited like every other member.
- The cache read moves to `resolved_axis(dim)`/`resolved_axes()`, beside the `map_uri` that is already there for exactly this purpose.
- `resolve_dims` calls those for a `ResolvedLayer`, as `_fold_map` already calls `head.map_uri(kind)` for the seeded owner map — the same pattern, one member over.

Then `_owned_frame` needs no change, and this section reduces to "nothing to do".

**Worth doing regardless of this proposal.** `axes()` on a `ResolvedLayer` reports its whole ancestry's axis set, where `StagedSource.axes()` reports one layer's — two meanings for one protocol member, currently unexercised because `resolve_dims` is its only caller. `StagedSource` shows the intended shape here: a source that is not simply its own layer's rows says so with a flag (`frozen = False`) rather than by redefining what a member returns.

## What this buys

**Every column of the format answers to a declaration.** `entity_type` stops being the exception admitted by a special case, because it appears only where a group declares the axis — which is also when it has a dtype. The `_validate_frame` special case and the `_axis_columns` `String` fallback both go, and so does the `nw.String()` fallback that types a column no `Dimension` describes.

**The owner map's shape follows the schema.** `entity_columns` is currently the one place a column is asserted rather than derived, and the map carries it — on disk, in a materialised cache — for records that have no types. Making it conditional is one line, and the five readers of that column all resolve to "there are no types" rather than to "the type is NULL".

**A record whose schema declares no type axis has one member file, not N.** Today `add("thing", …)` and `add("other", …)` write two files that no declaration distinguishes, and a reader must enumerate them to find a component. One axis file is the honest shape of "these components have no kinds".

**`Record.entity_types` stops lying.** It currently returns `{thing, other}` where the schema declares no entity-type axis — reporting filenames as a vocabulary. Empty is the true answer, and `entity_types` being empty is already what `schema.entity_types` says.

## What it costs

**A second layout for one construct.** The entity axis carries attribute columns in one configuration and not the other, which is a branch in `_axis_columns`, in `_validate_frame`'s `known` set, and in whatever `add` uses to route a wide frame. Against that: the branch replaces the `entity_type`-column special case rather than adding to it, and it is keyed on one schema property.

**A prerequisite in `ResolvedLayer`.** Its `axis`/`axes` must become layer-local first, with the folded read moved to the seed path (above). That is a small change and a defect fix in its own right, but it lands before this proposal rather than with it, and it touches `resolve_dims` - the one place the fold reads axes across sources.

**`attributes_on("entity")` changes meaning.** Its early return is load-bearing in more places than the writer — worth auditing every caller before moving it, since "never `entity`" is currently an invariant a reader may lean on.

**Migration.** A record written under the old layout has `dims/entity_type/thing.parquet` and an `entity_type` column; one written under the new has neither. Pre-1.0 and [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md), so this is a re-export rather than a compatibility path — but it is a re-export of every existing record whose schema declares no type axis, not a no-op.

## What it opens rather than settles

**Whether a declared axis with no `Enum` is the same case.** `schema.entity_types` is empty for a `String`-typed entity-type dim too — the labels are data there as well, yet a group declares the axis, so the type is a real dim with a dtype. This proposal keys on `entity_type_dim is None`, not on whether the labels are enumerable, and the two differ for exactly that schema. Which side it belongs on is a question about what declaring the axis _means_, and this page does not answer it.

**Whether `attributes_for` should narrow at all without types.** It currently returns every entity-addressed attribute for any label. If there are no types, "which attributes does type X carry" has no meaning, and the method's contract could say so rather than accepting any string.

## How to know it worked

**`tests/test_untyped.py` changes shape, and that is the deliverable.** `test_a_component_round_trips_without_a_type` and `test_an_unknown_label_is_accepted` both assert on `record.entity_types` returning labels; under this proposal they assert it is empty and read the values off `dims["entity"]` instead. The round trip is the same round trip — a value written and read back — through one file rather than two.

**A written untyped layer has no `dims/entity_type/` at all.** The clearest single assertion: the directory does not exist, where today it holds one file per label an `add` happened to use.

**The `_validate_frame` special case is gone.** Asserting that an `entity_type` column on the entity axis of a record with no declared type axis is now _rejected_ is what says the exception was removed rather than relocated.

**`sources.py` is untouched, `ResolvedLayer` aside.** The strongest signal that the layout is right rather than merely different: if `_FileLayer`, `ParquetLayer` and `DirectorySource` need no edit, then a file-backed source genuinely does not have to know which configuration it reads. A diff that adds a branch to any of them means the layout is being carried by the readers instead of by the format.

**A resolved record with no declared types reads the same as an unresolved one.** Materialise a node of such a record and read a component's value through both; they must agree. This is the assertion the `ResolvedLayer` prerequisite above exists to protect, so it belongs in the suite before either change rather than after.

**The owner map has no `entity_type` column where the schema declares no type axis** — asserted on the relation's columns, and on the materialised `resolved/owner_map/entities.parquet` for a node that was materialised. Today both carry it, filled with NULLs, which is the state that makes `entity_types()` answer `{None}`; a test that only checks `entity_types() == set()` would pass with the column still there and still NULL.
