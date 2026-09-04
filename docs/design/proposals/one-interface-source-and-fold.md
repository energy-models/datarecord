# Proposal: one interface, two objects — a source is its own rows, a fold is the resolution

Status: **Draft** · Drafted 2026-09-02

One claim: **`axis(dim)` should mean the same thing wherever it is written — "the rows of the thing I am holding".** You pick "one layer's contribution" versus "the resolved answer to here" by _which object you hold_, not by a `resolved_` prefix on a method nor by a mode flag. A [`LayerSource`](../read-path.md#owner-map) answers for its own layer; a fold answers for everything folded into it; both answer under the same names.

The codebase is already most of the way there and inconsistent about the last step. [`WorkingRecord`](../working-record.md) does it the clean way — the fold and the staged-layer's-own-rows are _two objects_ (`WorkingRecord` the [`Record`](../record.md) over a `NodeCache`, `StagedSource` the source). `ResolvedLayer` does it the muddy way — _one object_ that is a fold (a persisted `NodeCache`) but is typed as a source and put in the fold's own input list, so it answers for its own layer through some methods and for the fold-to-here through others.

This is the prerequisite [`a-member-file-holds-values`](a-member-file-holds-values.md) names, taken at its root rather than at the symptom. That page proposes adding `resolved_axis`/`resolved_axes` beside `map_uri` — a third folded-answer method under a distinct name, papering the dual role. The root cause is that a fold-result (`ResolvedLayer`, a persisted `NodeCache`) is masquerading as a fold-input (a `LayerSource`); untangle _that_ and no new method name is needed.

## What starts it

Three abstractions, each with a one-line contract the code states explicitly:

- **`LayerSource`** — "One layer's own rows, however they are stored" (the `sources.py` protocol docstring). `axis` "means 'this layer's own rows'". Every implementation obeys it: `ParquetLayer`, `DirectorySource`, `StagedSource`.
- **`NodeCache`** — "A record's resolved view: owner map, dims, schema, and the relations over them" (`resolve.py`). It holds a `sources` list and _folds_ it: the `inputs` owner map, `dims` (the entity axis and groups among them), and every `relation`/`entity_type_frame`/`group_frame` are computed from those sources.
- **`Record`/`RecordLike`** — "the narwhals interface over one fold" ([`record.md`](../record.md)). A `Record` wraps exactly one `NodeCache` and presents its fold as narwhals frames.

So `NodeCache` _is_ the fold, and a `Record` is its public face. The line between "own rows" (`LayerSource`) and "the fold" (`NodeCache`) is clean until one object straddles it.

### `ResolvedLayer` _is_ a `NodeCache`, persisted

This is the fact the rest of the page turns on. `Revision.materialise()` folds a node's sources and writes the result to `resolved/` — `_fold_kind` for the `inputs` owner map, `resolve_dims` for the axes (the entity axis and groups among them) ([`materialise`](../layers.md#materialised-node-caches)). That is _exactly_ what a `NodeCache` computes in memory; materialising is persisting a `NodeCache`'s output. `ResolvedLayer` is the handle that reads it back, and `NodeCache` seeds from it to skip the re-fold:

```python
NodeCache._fold_map: seed = read(
    head.map_uri(kind)
)  # a ResolvedLayer's stored owner map
NodeCache.resolve_dims: seed = head.resolved_axes()  # a ResolvedLayer's stored dims
```

So `ResolvedLayer` and `NodeCache` are **the same thing — the fold to a node — in two tenses**: `NodeCache` computed live, `ResolvedLayer` computed earlier and read from `resolved/`. A `NodeCache` reads a `ResolvedLayer` as its own prior incarnation, which is the whole point of `sources_to_read` stopping at a materialised ancestor: its `ResolvedLayer` already carries what re-folding from the root would recompute.

The defect is that this persisted fold is typed as a **`LayerSource`** and put in the `sources` list — a list of _own-layer_ things. **A fold-result is masquerading as a fold-input.** Everything below follows from untangling that one type error.

### The masquerade shows as a dual role

Because the persisted fold sits in the `sources` list, the head is asked two different questions by two different consumers of the _same list_ — the fold-input question and the fold-result question it should never have had to answer at once:

| consumer                  | holds the head as | wants                               | reads                 |
| ------------------------- | ----------------- | ----------------------------------- | --------------------- |
| `resolve_dims` seed       | fold seed         | resolved axes to here               | `resolved/dims/`      |
| `_fold_map` seed          | fold seed         | resolved `inputs` map to here       | `resolved/owner_map/` |
| `source_for` → `relation` | a source          | the head layer's _own_ winning rows | `layers/<id>/`        |

`source_for(layer_uuid)` returns the head `ResolvedLayer` for a key the head owns, then reads its member with `source.attribute(...)` — the _own-rows_ accessor. The seed paths read the _folded_ answer. One object, both roles, and the only thing keeping them apart is that the folded answer hides under `map_uri` (and, in `a-member-file-holds-values`'s sketch, would hide under `resolved_axis`).

`relation` already shows the intended shape by accident: for a layer the map names but the truncated source list does not hold, `source_for` fabricates `ParquetLayer(layer_uuid, con)` — a plain own-rows source. It _wants a source_, and where the list does not give it one it builds one. The head is the case where the list gives it a source that is secretly also a fold.

### `WorkingRecord` is the model, and has the latent version of the same seam

`WorkingRecord` keeps the two views as two objects:

- The **resolved view** (base + staged) is `WorkingRecord` itself, a `Record`, built as `base_cache.with_source(StagedSource(self, ...))` and answered by the inherited fold.
- The **own-layer view** (staged rows alone, for commit) is `StagedSource` the source, and `staged_only()` → `_Written`, built from the `_staged_*` helpers.

That is the shape this proposal generalises. But the seam is present here too, quieter: `staged_only()` returns a `_Written`, a _third_ type, rather than handing over the `StagedSource` that already _is_ the staged layer's rows. `_Written` and `StagedSource` are two spellings of "the staged layer's own rows" — one shaped for the writer, one for the fold — because `write_record`'s input is spelled as neither the source interface nor the interface a fold answers, but wants what both can give. The write-path half below closes that by naming the interface the two share. Left alone, `_Written` is the next `ResolvedLayer` waiting to happen.

## The construct

**A fold and a source answer the same interface; the object decides the meaning.** Concretely, three moves.

### The base is a `NodeCache`, and the head layer is a plain source

`sources_to_read` puts one object at the head to play two parts. Separate them into what each already is:

- the **fold base** is a persisted `NodeCache` — `resolved/` read back — standing for everything above the truncation. It is the fold-to-here, in its stored tense, and a `NodeCache` already knows how to start from one (`_fold_map` reads `map_uri`, `resolve_dims` reads the resolved axes). So the base is not a new `LayerSource` subtype under a `resolved_` prefix; it is the same fold the live `NodeCache` is, read rather than computed. It is `base`, not `seed`, to match `WorkingRecord`'s existing `base_cache` — the resolution this one builds on top of — rather than name it an arbitrary fold accumulator.
- the head layer's **own rows** are a plain `ParquetLayer(head_id)` — exactly what `source_for` wants and what it already fabricates for owners past the truncation.

Then `sources` becomes what its type says: a list of _own-layer_ sources, root-truncation first. The fold starts from the `base` and steps over those sources; `source_for` resolves an owner id to a source and never to the base. The fold-result stops being an entry in the fold-input list.

Concretely this is `NodeCache` starting from a prior `NodeCache` instead of from a `ResolvedLayer` masquerading as a source — the recursion the two-tense reading makes obvious: a fold to a node is a fold over that node's own layers _on top of the fold to its materialised ancestor_.

### `NodeCache` becomes `Resolver`, live or materialised

The rename says what the class is: it _resolves_ a node's layers into the answer a `Record` presents. "Cache" named an implementation detail (the frozen-prefix materialisation) as if it were the identity; "resolver" names the job. One class, two constructors — folded now, or read from `resolved/` — because live and materialised are one concept in two tenses, not two types:

```python
class Resolver:
    """Resolves a node's layers into its owner map, dims and the relations over them."""

    revision_id: UUID
    sources: list[LayerSource]  # own-layer sources, root-truncation first
    base: Resolver | None  # the materialised ancestor, itself a Resolver
    con: DuckDBPyConnection
    schema: Schema  # resolved at construction, not a `declared` fallback

    @classmethod
    def materialised(
        cls, revision_id, con
    ) -> Resolver: ...  # reads resolved/, folds nothing
```

`schema` is a required field, resolved by the caller, not the `declared: Schema | None` the class carries today. `NodeCache.schema` is a property that returns `declared` if set and otherwise `read_schema(con)`, threading the override through `with_source` — but every fold reads the schema, the two construction sites both already hold it (the tree node reads the root schema where it builds the resolver, the standalone `Revision.over` has the manifest in hand), and `node_cache` is already lazy, so the property's fallback buys nothing. Moving `read_schema(con)` to the one tree-node construction site makes `schema` a plain field, deletes `declared` and the property, and lets `with_source` pass `self.schema` already resolved. The tree-vs-standalone distinction the property encoded survives as _which caller supplies the value_, each unconditionally right.

`ResolvedLayer` is deleted. Its fold-role becomes a `Resolver.materialised(...)` at `base`; its own-rows role, where `source_for` reaches the head layer's winning rows, becomes a plain `ParquetLayer`. The materialised `Resolver` keeps the full interface — `with_source`, `attributes_of`, everything — since a materialised node is as much a record as a live one; what "materialised" changes is only that its maps and dims are read rather than folded.

`Resolver.frozen` (the `LayerData` member) is today's `NodeCache.stable`: `frozen_prefix(sources) == len(sources)`, whether the whole source list is write-once. No new logic — the rename to the shared vocabulary is the only change.

### The interface `Resolver` and a layer share

A `Resolver` and a `LayerSource` answer the _same questions_ — "the keys of kind X" and "the rows for one key of kind X" — one over a fold, one over a single layer. That is a shared read protocol; call it `LayerData`. Each is that protocol plus its own extras.

| the shared question    | `LayerData` member      | a layer answers (own rows)                                        | a `Resolver` answers (folded)              |
| ---------------------- | ----------------------- | ----------------------------------------------------------------- | ------------------------------------------ |
| the schema in force    | `schema`                | — the writer supplies it                                          | `schema`                                   |
| which axes exist       | `axes()`                | `axes()` ✓                                                        | keys of `dims`                             |
| one axis's rows        | `axis(dim)`             | `axis(dim)` ✓                                                     | `dims.axes[dim]`                           |
| which types exist      | `entity_types()`        | _new: list member files; `∅` if the schema declares no type axis_ | `entity_types()` ✓                         |
| one type's rows        | `entity_type(name)`     | `entity_type(name)` ✓                                             | `entity_type_frame` → renamed              |
| which groups exist     | `groups()`              | _new: list group files; only what the schema declares_            | _from schema_ → made explicit              |
| one group's rows       | `group(name)`           | `group(name)` ✓                                                   | `group_frame` → renamed                    |
| which attributes exist | `attributes(kind)`      | _new: list attr files_                                            | `attribute_names`/`output_names` → renamed |
| one attribute's rows   | `attribute(name, kind)` | `attribute(name, kind)` ✓                                         | `relation`/`outputs` → renamed             |

`schema` heads the table but governs it: which of the pairs below it are populated depends on what the schema declares — see [`schema` governs which pairs are live](#schema-governs-which-pairs-are-live-it-is-not-a-peer-of-them).

So `LayerData` is: `schema`, `frozen`, `axes`/`axis`, `entity_types`/`entity_type`, `groups`/`group`, `attributes`/`attribute`, and `all_attributes` — the enumerate-and-read pairs (each an `enumerate() -> set[str]` and a `read(key) -> Rel | None`) plus the two members that mean the same over a layer and a fold. A `LayerSource` gains the three enumerators it lacks (`entity_types`, `groups`, `attributes` — directory listing for a `_FileLayer` as `axes()` already is, the staging tables for `StagedSource`); a `Resolver` renames its fold-named readers (`entity_type_frame` → `entity_type`, `relation` → `attribute`, and the `dims` keys become `axes()`) so the two answer under one vocabulary.

`all_attributes(kind)` and `frozen` are on `LayerData`, not source-only. `all_attributes` is an enumerate-and-read member — the unioned bulk read over the same keys `attributes(kind)` enumerates — and a fold folds exactly it in its ownership `GROUP BY`, so it answers over a fold as much as over a layer. `frozen` is a real property of a fold, not a convention-constant: a `Resolver` can hold a `StagedSource` (this is what `WorkingRecord` is), so it is over a mix of frozen and unfrozen sources, and its `frozen` is `all(s.frozen for s in sources)` — whether its whole source list is write-once. (Distinct from `frozen_prefix`, which counts how far the leading run is frozen to decide materialisation, not whether all of it is.)

Beyond the shared protocol, each keeps what only it has:

- **`LayerSource` also has** `layer_id` — the per-source key `source_for` dispatches a winning `layer_uuid` back through — and `uri` on the file-backed ones. `layer_id` stays source-only because a fold has no single one: it spans many layers, keyed by `revision_id` and routed by the owner map, so "the last layer's `layer_id`" is not a value `source_for` could dispatch on.
- **`Resolver` also has** the fold machinery a single layer has no answer for: `with_source`, `source_for`, `inputs` (the one owner map), `entity_axis`/`group`/`dims` (the folded axes, no map), `attributes_of` (flags), `base`, `materialised`, and `revision_id` in place of a `layer_id`. (`stable` is now `frozen` on the shared protocol.)

`Resolver` fulfils `LayerData`; so does `LayerSource`. `write_record` takes a `LayerData` — a single `StagedSource` for the `NewChild` commit, a `Resolver` for the `Directory` commit — and `_Written` is deleted, being the adapter that filled the gap `LayerData` now closes.

The discipline that keeps this from becoming "one protocol for two unlike things": `LayerData` holds _only_ the enumerate-and-read members that mean the same for a layer and a fold. A method only one has — `all_attributes` on a source, `with_source` on a `Resolver` — stays off the shared protocol. The shared names are for the shared idea; the rest are each type saying it is not the other.

### `schema` governs which pairs are live, it is not a peer of them

The table above lists `schema` as one shared member among the enumerate-and-read pairs, but it is not their peer — it is what decides _which of them exist_. [`a-member-file-holds-values`](a-member-file-holds-values.md#what-starts-it) establishes that the [entity-type axis is optional](../schema.md#entity_type-the-axis-of-kinds): where a schema declares no type axis (`entity_type_dim is None`), there are no member files, no `entity_type` column, and a component's attribute values live on `dims/entity.parquet` directly. So the `entity_types()`/`entity_type(name)` pair is not universal — it is populated exactly when the schema declares the axis, and `groups()` reflects only the groups the schema declares.

`LayerData` therefore reads _through_ `schema`, not alongside it: `schema` is the one member the other pairs are conditional on, and a `LayerData` for an untyped record is a first-class shape, not the typed shape with types nulled out. The new source enumerators inherit `a-member-file-holds-values`'s own acceptance test on this — the failure mode is an enumerator answering `{None}` rather than `∅`:

- `entity_types()` on a source whose schema declares no type axis returns the **empty set**, not `{None}`. `entity_type(name)` is then never a valid question — a direct call is a caller error, mirroring `Resolver.entity_type_frame` on the same record.
- `groups()` returns only the groups the schema declares, and no more.

This is the point where the two proposals are the same work at two altitudes. `a-member-file-holds-values` is the read/write-path consequence of `entity_type` being optional, and it names the `ResolvedLayer` `axis`-means-one-thing fix as its **prerequisite** — which is [the base/source split](#the-base-is-a-nodecache-and-the-head-layer-is-a-plain-source) above. The two land together: this note gives `LayerData` the enumerators, and the untyped case is where those enumerators must answer `∅` rather than carry a phantom type.

### `axis` is layer-local on every `LayerSource`, no exception

`ResolvedLayer`'s `axis`/`axes` overrides go; it inherits the layer-local `_FileLayer` ones like every other source. The folded read moves to the `base`, which is not a `LayerSource`. The test of correctness is [`a-member-file-holds-values`'s own](a-member-file-holds-values.md#how-to-know-it-worked): `sources.py` needs no branch, because a file-backed source no longer has to know whether it is being read as a layer or as a fold.

### The writer takes a `LayerData`, and `_Written` disappears

`_Written` is not a second shape of `StagedSource` — it is a `RecordLike`, because `write_record(source: RecordLike)` speaks the record dialect and `StagedSource` speaks the source dialect. `_Written` is the _adapter_: `staged_only()` rebuilds the staged layer's rows as per-kind `Frames` so the writer can eat what the fold already reads as a source. It exists only because the writer's input was spelled as neither of the two things that can supply it.

But the writer needs exactly `LayerData`. It uses `schema` and, per kind, enumerates the keys this thing holds and reads each — which is the enumerate-and-read protocol above, no more. (It never touches `flags`, and `outputs` is `attributes(kind="outputs")`.) So `write_record` takes a `LayerData`: the `NewChild` commit hands it the `StagedSource`, the `Directory` commit hands it the `Resolver`, each the object it already is. `_Written` is deleted, not collapsed — it was the adapter for a gap `LayerData` closes.

This is why `LayerData` earns its three new enumerators. The _reader_ never needs them — the fold learns which keys exist from the schema and the owner map, so a `LayerSource` was point-accessor. The _writer_ must enumerate what a layer actually holds, where the schema over-declares. Adding `entity_types`/`groups`/`attributes` to the source is what lets the reader's interface serve the writer too.

The `Directory` case proves the interface rather than excepting it. It writes the _resolved_ record — base and staged flattened — which is not one layer's rows, so it cannot be a `StagedSource`; but "enumerate what I hold, hand each over" is exactly what a fold answers, where "what I hold" means every folded key. That is this proposal's thesis on the write path: [one interface](#the-construct), the object deciding whether it means a layer or a fold.

This is the larger of the two halves and the most safely deferred, since it moves `write_record`'s contract and reconciles names across the fold and the source. It belongs to the same idea: `_Written` is the write-path twin of a `resolved_axis` prefix — a third spelling of a thing the common interface should already carry.

## What this buys

**No object answers `axis` two ways.** The overload that `a-member-file-holds-values` has to route around disappears at the source, so that proposal's prerequisite section reduces to "already true".

**A file-backed source needs no branch.** `_FileLayer`/`ParquetLayer`/`DirectorySource` read whichever columns a file has and never learn which configuration they are in — the strongest single signal the layout is carried by the format, not by the readers.

**One vocabulary for "the rows of this thing".** `axis`, `entity_type`, `attribute`, `group` mean "the rows of the object I hold", whether that object is a single layer or a fold. A reader learns the interface once.

**The `WorkingRecord` and resolved-node mechanisms become one pattern.** Both are "a fold-object over sources, with a distinguished own-rows source for the layer that is also the fold's boundary". Today they rhyme; after, they are the same construct.

**The writer takes `LayerData`, the interface a source and a `Resolver` share.** `write_record`'s input is the enumerate-and-read surface both fulfil, so both commit callers pass the object they already hold and `_Written`, the adapter that existed only because the input was spelled as neither, is deleted.

## What it costs

**It reshapes the `Resolver`'s (`NodeCache`'s) source list.** The head stops being a `LayerSource` that is secretly a fold; the `base` becomes a materialised `Resolver` the fold starts from, and `source_for` stops returning it. `sources_to_read`, `_fold_map`, `resolve_dims`, and `source_for` all move together. This is the change `a-member-file-holds-values` deferred as a prerequisite, taken at its root — the masquerade — rather than papered with a `resolved_` prefix.

**A shared interface across a source and a fold invites `isinstance` where a method belongs.** The discipline that keeps it honest: if the fold-object needs a method a source does not have (a map has no `entity_type`; a source has no `map`), that is the interface telling you they are _not_ the same type — the shared members are the ones that mean "my rows", and the rest stay apart. This proposal is not "one protocol for both"; it is "the same names for the same idea where the idea is shared".

**Teaching the writer to take `LayerData` touches its contract.** `write_record` moves from `RecordLike` to `LayerData`, `LayerSource` gains the three enumerators, and `_Written` and its `staged_only()` builder go. `_Written` has callers in the tests and one in `commit`, and the `Directory` path hands over a `Resolver` under the new contract rather than the record it built. A real change to the write path, not only a read-path cleanup, and the part most safely deferred.

## What it settles

**The base is a full `Resolver`.** `Resolver.materialised(revision_id, con)` reads `resolved/` and folds nothing, keeping the whole interface — a materialised node is as much a record as a live one. Not a slimmer read-only view: the base answers whatever a `Resolver` does, its maps and dims read rather than computed.

**`LayerData` is a `Protocol` both types satisfy structurally, with `layer_id` the sole source-only member.** `LayerSource` is `LayerData` plus `layer_id` (and `uri` on file-backed ones); `Resolver` is `LayerData` plus the fold machinery and `revision_id`. `all_attributes` and `frozen` are on `LayerData`, not source-only (above). Neither type inherits from `LayerData` — each `satisfies` it by shape, as `LayerSource` implementations already satisfy that protocol.

**The untyped case keys on `entity_type_dim is None`, enum or not.** An enumerator answers `∅` exactly when the schema declares no entity-type axis — not when `schema.entity_types` is empty, which is also true for a declared `String`-typed axis whose labels are data. A declared-but-non-enum axis is still a declared axis: its member files exist and `entity_types()` lists them. `a-member-file-holds-values` [leaves whether those two cases should ever converge open](a-member-file-holds-values.md#what-it-opens-rather-than-settles); this note does not, keying only on declaration.

## What it opens rather than settles

**How far the name reconciliation reaches.** The write-path half already reconciles the _rows_ interface a fold and a source share (`entity_type_frame`/`entity_type`, `relation`/`attribute`, `dims`/`axes` become one vocabulary the writer reads). Left open is whether `Record`'s _public narwhals_ surface joins it — `Record.dims` returns `Frames`, not the relation a source's `axis` does, so it is a presentation over the same idea rather than the idea itself. Whether the resolved-and-own pair should share the outward name too, or only the internal rows interface does, is the larger claim this page does not force.

## How to know it worked

**`ResolvedLayer` is gone; the base is a `Resolver`.** The single assertion the resolved-node half is downstream of: the persisted fold is a `Resolver.materialised(...)` the fold starts from, not a source in its input list. `def axis` in `sources.py` is found only on `_FileLayer`, and nothing folded-answer-shaped is typed as a source.

**A resolved record reads the same as an unresolved one.** Materialise a node, read a component's value through the resolved node and through the same node read from its own layers unmaterialised; they agree. This is the invariant the whole split protects, and it belongs in the suite _before_ the change, not after — it is what says the base carries the fold correctly once `axis` stops doing so.

**`Resolver.sources` holds only `LayerSource`s, and the base is a `Resolver`.** The masquerade is over exactly when no `ResolvedLayer` appears in a `sources` list — the head is a plain `ParquetLayer` and the fold-to-here it stood for is the `Resolver` at `base`. `source_for` then always hands back an own-rows source, never the base. Assertable on the type of the head entry and on what `source_for` returns for it.

**`sources.py` is untouched but for deletion.** As in `a-member-file-holds-values`: if the file-backed sources need no _added_ branch, the layout is carried by the format. A diff that adds a conditional to `_FileLayer` means the fold is leaking into the source again.

**A source's new enumerators answer `∅`, not `{None}`, for an untyped record.** `entity_types()` on a `LayerSource` whose schema declares no type axis returns the empty set, and `groups()` only the declared groups — the same `∅`-not-`{None}` check `a-member-file-holds-values` makes on the owner map. A test that materialises an untyped node and asserts `entity_types() == set()` on both the base `Resolver` and a `ParquetLayer` of the head is what says `schema` governs the pairs rather than the pairs carrying a phantom type.

**`_Written` is gone and `write_record` took the `StagedSource` directly** — once that half lands. The `NewChild` commit passes the staged source to the writer, the `Directory` commit passes the resolved record, and both satisfy one input contract. Until it lands, `_Written` standing unchanged is the marker that this is the deferred part, not a regression.
