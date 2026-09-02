# Proposal: a member file holds values, and nothing else

Status: **Draft** · Drafted 2026-09-02

Two claims about `dims/entity_type/<Type>.parquet`, which are one claim: that file holds a type's attribute values, and everything about a component's *existence* belongs to the [entity axis](../format.md#the-entity-axis).

Today it holds more. A record whose schema declares no entity-type axis still writes one member file per type — types that are not declared, not validated and not constrainable, yet decide which file a value is in, so a reader must know them to reach it. And every member file carries `deleted`, which the fold reads as a tombstone alongside the axis's own, so a removal is written twice to say one thing.

This proposal puts an undeclared record's values on `dims/entity.parquet`, and takes `deleted` off the member file in every record. What is left in a member file is `entity` and one column per attribute.

## What starts it

[The schema](../schema.md#entity_type-the-axis-of-kinds) is explicit that the axis is optional, and that omitting it makes the labels data:

> The entity-type axis's enum categories, so a schema declaring it as a plain `String` has none: the labels are then data rather than declarations, and `attributes_for` accepts any of them.

`tests/test_untyped.py` exercises that: a schema with two attributes over `entity` and no classifying group, components added under `thing` and `other`, and both round-trip. So far so good.

The problem is where their values land. `p_nom` is `dims={"entity"}` and non-varying, and the format gives that exactly one home — [`dims/entity_type/<Type>.parquet`](../format.md#where-a-value-lives). It cannot be a column of `dims/entity.parquet`, because `attributes_on("entity")` returns `()` by explicit early return; it cannot be an `inputs/` row, because it does not vary. So an undeclared label becomes a filename:

```
dims/entity.parquet             entity, entity_type, deleted
dims/entity_type/thing.parquet  entity, p_nom
dims/entity_type/other.parquet  entity, p_nom
```

**`entity_type` there is a column no declaration accounts for.** Every other column of an axis file answers to something: its key comes from `axis_key`, its attribute columns from `attributes_on`, `deleted` and `order_key` are structural. This one is admitted by a special case in `_validate_frame` and typed as a plain `String` by a fallback in `_axis_columns`, because there is no `Dimension` to read a dtype from. It is the one column in the format whose presence is not derivable from the schema.

## Why it cannot simply be dropped

The obvious move — no declared axis, no `entity_type` column — was tried and does not close. Three readers need the label:

| reader | what it does with it |
| ------------------------------- | ------------------------------------------------- |
| `NodeCache.entity_types()` | `distinct_values(entity_map, "entity_type")` |
| `NodeCache.entity_type_frame` | filters the map to one type, then reads its file |
| `_fold_ordered`'s `typed` arm | carries it through the components `GROUP BY` |

And `schema.entity_columns` hardcodes `entity_type` into the owner map's column set, so the map has the column whether or not anything fills it. Gate the axis and `entity_types()` yields `{None}` rather than `{thing, other}`.

Deriving the label from the filenames instead works, but costs a `LayerSource.entity_types()` listing member and a per-layer `LIST` on the read path — which is what [the entity axis exists to avoid](../format.md#the-entity-axis). Trading a stored string for a directory listing per layer per fold is the wrong direction.

So the label is load-bearing *given the current layout*. The layout is what this proposal changes.

## The construct

**Where a schema declares no entity-type axis, a non-varying attribute over `entity` is a column of `dims/entity.parquet`.** No `dims/entity_type/` directory, no per-type files, and `entity_type` is then genuinely absent — nothing classifies, so there is nothing to classify into.

```
dims/entity.parquet   entity, deleted, p_nom, ...
```

Which is what the entity axis already is for every other purpose: the file keyed by `entity` that carries what is true of a component. `attributes_on`'s early return — "Never `entity`, whose sole-coordinate attributes are the *component* frame's columns" — becomes conditional on a type axis existing, which is the one line of schema that decides the whole layout.

The three readers above then need no label: `entity_types()` is empty, `entity_type_frame` has no type to be asked for, and `_fold_ordered`'s `typed` arm is skipped — the state the `typed` flag was already written to express.

### What a declared record does, unchanged

A schema declaring the axis keeps today's layout exactly: `dims/entity_type/<Type>.parquet` per type, `entity_type` on the entity axis, typed by the declared `Dimension`. This proposal adds no case there; it removes one from the undeclared side.

### Every source and every producer

The layout is one thing; what each implementation of it must do is another, and the change is only coherent if all of them agree. There are four `LayerSource`s and two record-side producers.

| implementation | `axis("entity")` under this proposal | `entity_type(name)` |
| -------------------------------- | ---------------------------------------------------- | ------------------------------------- |
| `_FileLayer` (shared) | `dims/entity.parquet`, now with attribute columns | `None` — no such directory |
| `ParquetLayer` | inherited | inherited |
| `ResolvedLayer` | `resolved/dims/entity.parquet` — **see below** | inherited, from `layers/<id>/` |
| `DirectorySource` | inherited | inherited |
| `StagedSource` / `WorkingRecord` | `_ENTITY_AXIS` table, now with attribute columns | `None` — no member tables staged |
| `PyPSA` (tool) | `dims["entity"]`, its member frames merged in | `entity_types` empty |

Three of the four sources need no code change at all: `_FileLayer` reads whatever columns the file has, and `ParquetLayer` and `DirectorySource` inherit it. `entity_type(name)` answering `None` follows from the directory being absent, which `try_read_parquet` already handles. That is the test of whether the layout is right — a file-backed source should not have to know which configuration it is reading.

**`ResolvedLayer` is the one that does not fall out.** It overrides `axis` to read `resolved/dims/`, the *folded* axis standing for everything above this node, while `entity_type` stays inherited and reads this layer's own `layers/<id>/dims/entity_type/`. Today those carry different things — membership versus attributes — so the split is invisible. Under this proposal the attribute columns are *on the axis*, so a resolved node would serve them from the fold while the rows they replaced came from the layer alone. That is a scope change, not a relocation: `entity_type_frame` semi-joins the owner map to this node's own rows, and folded columns would widen what it returns.

Resolving it is part of the work, not a footnote. Either the materialised `resolved/dims/entity.parquet` keeps carrying folded attribute columns and `entity_type_frame` learns that a resolved node's member columns are already folded, or the cache stores membership only and the attributes are read from the layer — in which case `ResolvedLayer` needs an `axis` that splits the two, which is the first place in this design where one file's columns come from two locations.

**The `PyPSA` tool** currently supplies `entity_types` per type and, since the entity axis became a supplied member, `dims["entity"]` from `_entity_axis_frame`. Under this proposal an untyped export merges the member columns into that frame and stops supplying `entity_types` — but PyPSA always has types (`n.components`), so it is unaffected in practice. It matters as a statement about the protocol rather than about this tool: a producer with no types has one axis frame to fill and no `entity_types` mapping, which is exactly what the `Record` protocol should let it say.

## What this buys

**Every column of the format answers to a declaration.** `entity_type` stops being the exception admitted by a special case, because it appears only where a group declares the axis — which is also when it has a dtype. The `_validate_frame` special case and the `_axis_columns` `String` fallback both go.

**A record with no types has one member file, not N.** Today `add("thing", …)` and `add("other", …)` write two files that no declaration distinguishes, and a reader must enumerate them to find a component. One axis file is the honest shape of "these components have no kinds".

**`Record.entity_types` stops lying.** It currently returns `{thing, other}` for a record whose schema says it has no entity types — reporting filenames as a vocabulary. Empty is the true answer, and `entity_types` being empty is already what `schema.entity_types` says.

## What it costs

**A second layout for one construct.** The entity axis carries attribute columns in one configuration and not the other, which is a branch in `_axis_columns`, in `_validate_frame`'s `known` set, and in whatever `add` uses to route a wide frame. Against that: the branch replaces the `entity_type`-column special case rather than adding to it, and it is keyed on one schema property.

**`ResolvedLayer` splits one file across two locations.** The case above is the sharpest cost, and it is the one to settle before writing any code — it is the only place where the proposal makes a source's two members disagree about scope, and both ways out have a price: teaching `entity_type_frame` about folded member columns, or giving `ResolvedLayer` an `axis` that reads membership from the cache and attributes from the layer.

**`attributes_on("entity")` changes meaning.** Its early return is load-bearing in more places than the writer — worth auditing every caller before moving it, since "never `entity`" is currently an invariant a reader may lean on.

**Migration.** A record written under the old layout has `dims/entity_type/thing.parquet` and an `entity_type` column; one written under the new has neither. Pre-1.0 and [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md), so this is a re-export rather than a compatibility path — but it is a re-export of existing untyped records, not a no-op.

### `deleted` belongs to the axis alone

A member file carries `deleted` today, and the fold reads it from there as well as from the entity axis. That is the same duplication the axis exists to remove: [membership is the axis's](../format.md#the-entity-axis) — "a component exists because it has a row here, not because some type's file mentions it" — and a tombstone is a statement about membership.

It shows up as a coupling in the staging area. `remove` has to write twice, once to the axis and once to the type's member table, because a removal that left no member row would read as a member the layer never mentioned. Two writes to say one thing, kept in step by hand.

So this proposal also takes `deleted` off the member file: a member file holds `entity` and that type's columns, nothing else. `remove` then writes one row, on the axis, and the fold reads tombstones from one place.

The two changes belong together because they are the same claim about that file — a member file holds *values*, and everything about a component's existence is the axis's. Separately each looks like a detail; together they leave the member file with no column that is not an attribute.

## What it opens rather than settles

**Whether a declared axis with no `Enum` is the same case.** `schema.entity_types` is empty for a `String`-typed entity-type dim too — the labels are data there as well, yet a group declares the axis, so the type is a real dim with a dtype. This proposal keys on `entity_type_dim is None`, not on whether the labels are enumerable, and the two differ for exactly that schema. Which side it belongs on is a question about what declaring the axis *means*, and this page does not answer it.

**Whether `attributes_for` should narrow at all without types.** It currently returns every entity-addressed attribute for any label. If there are no types, "which attributes does type X carry" has no meaning, and the method's contract could say so rather than accepting any string.

## How to know it worked

**`tests/test_untyped.py` changes shape, and that is the deliverable.** `test_a_component_round_trips_without_a_type` and `test_an_unknown_label_is_accepted` both assert on `record.entity_types` returning labels; under this proposal they assert it is empty and read the values off `dims["entity"]` instead. The round trip is the same round trip — a value written and read back — through one file rather than two.

**A written untyped layer has no `dims/entity_type/` at all.** The clearest single assertion: the directory does not exist, where today it holds one file per label an `add` happened to use.

**The `_validate_frame` special case is gone.** Asserting that an `entity_type` column on an untyped record's entity axis is now *rejected* is what says the exception was removed rather than relocated.

**`sources.py` is untouched, `ResolvedLayer` aside.** The strongest signal that the layout is right rather than merely different: if `_FileLayer`, `ParquetLayer` and `DirectorySource` need no edit, then a file-backed source genuinely does not have to know which configuration it reads. A diff that adds a branch to any of them means the layout is being carried by the readers instead of by the format.

**A resolved untyped record reads the same as an unresolved one.** Materialise a node of an untyped record and read a component's value through both; they must agree. This is the assertion the `ResolvedLayer` cost above will break first, so it belongs in the suite before the change rather than after.
