# Proposal: staging as a layer — one fold, two sources

Status: **Draft** · Drafted 2026-09-01 · Revised 2026-09-01

[`WorkingRecord`](../working-record.md) already claims to be a layer:

> A set of pending edits **is** a layer — an unwritten one. So the reads compose the same way: the staged rows are the last layer, resolved over whatever the record was reading before. ([reading with pending edits](../working-record.md#reading-with-pending-edits))

The code does not deliver that. `mutable.py` reimplements the overlay in its own terms — an anti-join and a union per kind, a `_collapsed_*` per kind, its own flags aggregate — while `layered/resolve.py` folds. This proposal makes the sentence true: one fold, reading parquet layers, a plain directory or a staging area, differing in where a layer's rows come from and nothing else.

## What starts it

Three pieces of the same algebra exist twice, and the duplication is not incidental — each pair was written from the same design paragraph and drifted anyway.

**The overlay.** `fold_inputs` anti-joins the parent on `input_key` and unions the layer's own rows ([resolve.py](https://github.com/energy-models/datarecord/blob/main/datarecord/layered/resolve.py)); `_overlay`, `_entity_union` and `_group_union` do the same against staged rows. Five sites, one rule.

**The flags.** The fold computes `varies`/`broadcast`/`breakpoints` in its ownership `GROUP BY`, so `LayeredRecord.flags` is free. `WorkingRecord._flags_arm` computes the identical two structs over the staging tables, one arm per attribute, then unions them into the base's answer. `flags_from_rows` is shared, so the coordinate scoping [`flags` depends on](../record.md#flags) is written once; the aggregate feeding it is not.

**The collapse.** `_latest_per` collapses staged rows last-write-wins per coordinate; the fold does the same across layers by depth. Different mechanisms (`_seq` versus layer order) for one rule.

## The construct

The fold's three kind-folds are hardcoded to read one layer's parquet:

```python
def fold_inputs(revision_id, keys, con, parent):
    rel = try_read_parquet(layer_dir(revision_id) + "inputs/*.parquet", con, ...)
```

`revision_id` is used for exactly two things: deriving those paths, and stamping `layer_uuid`. Replace it with a **source** that answers the same questions:

```python
class LayerSource(Protocol):
    """One layer's own rows, however they are stored.

    "The layer as it would be written", not "the rows as stored": a source
    hands over what `write_record` would persist, so `_seq` collapsing and
    tombstone application happen behind it and the fold never learns about
    either. `None` means this layer wrote nothing of that kind.

    Rows only. Everything the fold does to them - padding to the long
    schema, expanding broadcasts against an axis, the ownership aggregate,
    `order_key` - stays in the fold, which is the point: an implementation
    that computed any of it would be the second copy this proposal exists
    to delete.
    """

    layer_id: UUID  # what the fold stamps as `layer_uuid`
    frozen: bool = True  # False for a staging area, which changes under the reader

    def axes(self) -> list[str]: ...
    def axis(self, dim: str) -> DuckDBPyRelation | None: ...
    def entity_type(self, name: str) -> DuckDBPyRelation | None: ...
    def group(self, name: str) -> DuckDBPyRelation | None: ...
    def attribute(self, name: str, kind: Kind = "inputs") -> DuckDBPyRelation | None: ...
    def all_attributes(self, kind: Kind = "inputs") -> DuckDBPyRelation | None: ...
```

Each member is one file, addressed by what keys it — `dims/<dim>.parquet`, `dims/entity_types/<name>.parquet`, `groups/<name>.parquet`, `inputs|outputs/<name>.parquet`. `Kind` is `Literal["inputs", "outputs"]` — the same alias [`set`](../working-record.md#set) already takes, kept a literal rather than promoted to an enum so the two signatures stay one vocabulary.

Two asymmetries, both the format's rather than the protocol's:

**`entity` is a dim like any other.** `dims/entity.parquet` is an axis file, so it is `axis("entity")`; there is no separate `entities` member. What differs is only what the fold does with it — the entity axis is folded into the components map, with `order_key` and tombstones, where `period.parquet` is folded by `resolve_dims` on `axis_key`. Same file accessor, two consumers. `entity_type(name)` is a different thing entirely: one type's wide member rows, read after the map named a winner.

**Only `inputs/` has a plural.** `all_attributes` exists because `fold_inputs` decides ownership across every attribute in one `GROUP BY` — the files share `input_key`, so one scan answers for all of them. Nothing else does: each axis folds on its own `axis_key(dim)`, each group on its own `group_key`, so a combined read would have no key to fold on and `union_by_name` across them would be meaningless. `attribute(name)` is not the singular of it in disguise — it is the *owned* read, one file with its own exact columns, which `relation()` needs because a padded column is ambiguous against [the broadcast rule](../record.md#the-broadcast-rule).

`all_attributes` returns the union **unprojected**. `fold_inputs` keeps its `with_columns` padding to `long_columns`, and `StagedSource` gets that for free: its staging tables carry the same per-attribute column variation as the files do, so one call fixes both.

`axes()` is the one discovery member, and it earns its place: `resolve_dims` today probes `{dim}.parquet` for every *declared* dim in every ancestry directory, and [`fold_axis`'s own docstring](https://github.com/energy-models/datarecord/blob/main/datarecord/duck.py) notes most of those miss. One `glob` per directory replaces D probes with one listing — D × A probes become A listings, which remotely is the difference between forty HEADs and five LISTs. `ParquetLayer` globs; `StagedSource` reads `self._staged`.

Two implementations, plus a third under [question 2](#2-what-a-non-layered-base-does):

- **`ParquetLayer(revision_id)`** — what exists today, each member a `try_read_parquet` under `layer_dir(revision_id)`.
- **`StagedSource(working_record)`** — the collapsed staging tables under a synthetic `layer_id`: `_collapsed_inputs` behind `attribute`, `_collapsed_entities` behind `axis("entity")`, `_collapsed_group` behind `group`, `_collapsed_axis` behind `axis`.

`flags` then falls out of the ownership group-by for every backing, and `_flags_arm`, `_overlay`, `_entity_union` and `_group_union` are deleted rather than parametrised.

### The cache stops at the last frozen source

`NodeCache` folds a list of sources rather than a list of UUIDs:

```python
@dataclass(frozen=True)
class NodeCache:
    revision_id: UUID
    sources: list[LayerSource]  # root first, ending in this node's own layer
    con: DuckDBPyConnection
```

A `WorkingRecord` is then `sources + [StagedSource(self)]`. No second class and no mode flag: the staged layer is the last entry, which is what "staging is a layer" was supposed to mean.

What that list has to carry is the caching rule, because the sources differ in exactly one respect that matters. `_table` materialises a folded map with `.create(name)` and never invalidates it; `dims` and `schema` are `cached_property`; the class docstring says outright that nothing an instance caches can go stale. All of that rests on [layers being write-once](../layers.md#a-layers-data-is-write-once) — which a staging area is not, since every `set` changes its rows under the reader.

Hence `frozen` on the protocol, and one rule over it:

> **The fold is materialised up to the last frozen source; everything after it stays a relation.**

So `_map(kind)` folds the frozen prefix and `.create()`s it exactly as today, then folds the remaining sources on top and hands back the relation. `dims` is the same rule applied to the other fold: `resolve_dims` folds each axis over `dims_dirs` on `schema.axis_key(dim)`, outside the owner map entirely, so the frozen prefix's axes cache and a staged `axis(dim)` folds over them per access. That is what carries a staged axis edit into the reads, and where `_collapsed_axis`'s per-column `max_by` has to survive: two `set` calls for two attributes on one axis each stage only their own column, so a whole-row last-writer-wins would blank the earlier one.

Nothing needs invalidating, because nothing past the prefix is materialised: a DuckDB relation over a staging table reads whatever the table holds when it is collected, so a `set` is picked up with no bookkeeping at all — no generation counter, no cache key, no drop-and-rebuild.

What re-executes per read is one anti-join, one union and the ownership `GROUP BY` over the staging tables, against an already-materialised parent map. The ancestry does not. That is [what the design already promises](../working-record.md#reading-with-pending-edits) — "exactly one more fold step over the same owner-map machinery … it costs what one more layer costs" — with the qualifier that page leaves implicit, that a written layer pays that cost once while the staged one pays it per read, being the one layer that can still change.

Two things fall out of stating it as a property of the sources rather than a mode of the cache. A second staged layer, or a frozen one under an unfrozen one, needs no new code — the prefix is wherever `frozen` stops being true. And `ancestry_to_read`'s truncation at the deepest materialised ancestor becomes *source construction*: fewer sources rather than a shorter list of UUIDs, with `materialised()` unchanged, being a question about a revision.

The one wart: `dims` cannot stay a `cached_property` where the tail is live. It becomes a plain property that caches the frozen prefix's axes and re-folds the rest — the same rule as `_map`, so it is one concept applied twice rather than a special case. `schema` is unaffected: [one schema per record](../schema.md#one-schema-per-record), not layered data, so no source contributes to it.

## What makes the staged source foldable

The fold ends each kind by evicting the parent's matching keys:

```python
kept = parent.join(own, null_safe("p", "o", keys.schema.input_key), how="anti")
```

That is correct for a written layer *because* [`_restated`](../working-record.md#committing) has already completed the layer's extent along every non-`partial` axis. Evicting the parent's key loses nothing, since the new layer replaced all of it. A staging area has not restated, so anti-joining on `input_key` would let one staged point displace the base's whole series — the loss `_overlay`'s comment describes, and the reason the staged overlay keys by coordinate instead.

So the precondition, not the algebra, is what differs. **Establish the precondition and the difference is gone**: `_restated` runs on first touch of an attribute rather than at commit.

`_ensure` is already the first-touch hook — a staging table is created exactly once per attribute, keyed in `self._staged`, and `_owned_whole(attribute)` says whether the attribute has an axis that obliges completion at all. So the rule is: when creating the staging table for an attribute with a non-empty `_owned_whole`, seed it with the base's extent for that attribute. Once. Every later `set` on it hits an already-complete series and does nothing extra.

This is the same total work commit does today, paid earlier, and it is what makes `staged_only()` literally true rather than assembled: the staging tables already hold what gets written, so `_restated` leaves the commit path entirely.

Two consequences to accept deliberately:

- **`set` reads the base on first touch of a completed attribute** — a fold, where a deep unmaterialised ancestry makes one. It must not retrigger: the guard is table creation, which happens once per attribute per `WorkingRecord`, so a second `set` on the same attribute reads nothing. An attribute with no owned-whole dim never triggers it at all.
- **`pending` counts rows the caller did not stage.** Carried rows are indistinguishable from edited ones in the staging table, so a one-value `set` on a thousand-snapshot attribute would report a thousand pending rows.

## What has to be settled first

### 1. `pending` goes

Settled: **remove it** rather than teach the staging tables to distinguish carried rows from edited ones. That distinction is a second row class in every staging table and a condition in `_collapsed_inputs`, which is more machinery than the accessor is worth.

It is not free. `Pending` is public, documented in [`docs/usage/editing.md`](../../usage/editing.md), and pinned by around a dozen assertions in `test_mutable.py` — those tests assert *that an edit was staged*, which after this change is better asked of the reads (`w.attributes[attr]`) than of a counter. Pre-1.0, so this is a delete rather than a deprecation; the PR says which coverage moved where.

### 2. What a non-layered base does

Settled by `DirectorySource`: a `DirectoryRecord` is a parquet directory in the layer layout, so every member of the protocol is one of its `_read` calls and a `WorkingRecord` over one folds `[DirectorySource(base), StagedSource(self)]`. No conditional path, and the `_overlay` cluster goes for every base.

**A base that is neither is out of scope**, deliberately. A framework object satisfying `Record` hands over narwhals frames, and `as_relation` would turn those into relations — but the protocol has no `Record` accessor behind it: `axis("entity")` wants the entity axis, and `Record` exposes `components` keyed by type with the wide rows, not the axis. Synthesising one means unioning every type's frame and projecting `(entity, entity_type)`, which is reconstructing the layer format from a protocol that deliberately does not have it. `all_attributes` is worse — it would materialise the whole record per read.

Nothing constructs such a `WorkingRecord` today, and `_base_revision` already refuses it with a clear message. If a caller appears, it is its own proposal.

**One design note this owes.** [read-path](../read-path.md#what-differs-between-the-implementations) makes "no owner map" a property of `DirectoryRecord`, and that stays true — a bare `DirectoryRecord` still scans for `flags`. What is new is that a `WorkingRecord` *over* one builds a map, which belongs to the `WorkingRecord` exactly as a `LayeredRecord`'s map belongs to the node rather than to the layers it folds. The page needs that sentence, or the next reader reads the map as a contradiction.

### 3. Results become schema-declared

Today a result attribute is not declared: [`Tool.results`](../tools.md) derives which attributes count as results from the framework's own registry, and `write_record` persists `outputs/` without consulting the schema. So `set(..., kind="outputs")` accepts any name, and `output_names()` has to *discover* what is there by globbing.

**Declare them.** `Schema` gains result attributes beside its input ones, and three things follow:

- **The last glob goes.** `all_attributes("outputs")` names its files from the schema instead of listing a directory. `output_names()` itself stays a data read — it mirrors `attribute_names`, which reads the owner map rather than the schema, because a record may declare a result no layer computed exactly as it may declare an input no layer wrote.
- **`long_columns_for` stops guessing.** An undeclared attribute currently falls back to `long_columns` — "every declared dim, the widest shape" — and its docstring says only a result reaches that branch. Declared results get their own coordinates like inputs, and the branch goes with them.
- **One vocabulary.** `Schema` stops having two classes of attribute, one it validates and one it cannot see.
- **`value_hint` retires.** `_empty_long` types a staging table's `value` as `schema.value_type(attribute) or value_hint or String()`, where the hint is the dtype the caller's frame arrived with — "that being the only thing that knows" for an undeclared result. Once results are declared the schema knows, so the fallback is dead. Retiring it removes the parameter from `_ensure` and `_empty_long`, its three call sites, and the two helpers that compute it (`_value_dtype`, `_scalar_dtype`) — check those have no other caller first.

The cost is real and lands outside this repo. A tool must declare a result before attaching it, where today it may attach anything its registry produced — and PyPSA's `SubNetwork` exists only *after* a solve, which is exactly the case [results through `kind="outputs"`](../working-record.md#results-through-kindoutputs) cites for not requiring declaration. So a tool either declares its full result vocabulary up front, or amends the schema at solve time, and [`Schema.compatible_with`](../schema.md#versioning) is what says whether existing layers survive the amendment.

The membership check stays relaxed regardless: a result may name a component the record never declared, which is a separate rule from whether the *attribute* is declared and is not changed here.

### 4. What the staged step re-executes

Settled by [the frozen-prefix rule](#the-cache-stops-at-the-last-frozen-source): the staged fold is a relation, never a table, so a `set` needs no invalidation and the cached ancestry below it is reused. Two things to check rather than assume, both consequences of running that step per read:

- **`_fold_ordered`'s `order_key` subquery.** It computes `(SELECT max(order_key::BIGINT) FROM parent)` inside `con.sql`, which under the current code runs once when the map is created. Uncached, it runs on every collect — a scan of the parent map per read. Measure it on a large component set; if it is not free, the offset can be hoisted to `NodeCache` construction, the frozen prefix's map being fixed. [`order_key` as `(depth, row)`](#what-lands-first-and-separately) removes the subquery outright rather than hoisting it, which is why that change is listed there.
- **`StagedSource`'s collapses are rebuilt per call.** Each member applies its collapse (`_latest_per`, the tombstone anti-join, `_collapsed_axis`'s per-column `max_by`) before handing the relation over. Measured on 50k staged rows over 500 entities × 20 periods: **~1.4 ms to build the plan, ~4.3 ms to execute it**. So a read of a `WorkingRecord` over a schema with inputs, components and two groups pays roughly four of each — tens of milliseconds, scaling with what is staged.

  It can be memoised, contrary to how the live tail might read: what must not be cached is a *stale* answer, and `_seq` is already a monotonic per-edit counter, so the source knows exactly when its own answer changed. Key each collapse on the highest `_seq` staged; a read after a `set` misses and rebuilds, two reads with nothing between them hit. This is local to `StagedSource`, invisible to `NodeCache`, and leaves [the frozen-prefix rule](#the-cache-stops-at-the-last-frozen-source) untouched — the tail is still recomputed whenever it changed, just not when it did not. It is also the one place a generation counter is cheap, because `_seq` exists already; the owner map has no equivalent signal, which is why it is folded live instead.

### 5. Whether `_seq` collapsing belongs to the source

Yes, and it is stated in the protocol docstring: a source hands over "the layer as it would be written". `_seq` is staging bookkeeping, so the fold must never see it, and nobody should add a raw-rows member later.

### 6. What `DirectoryRecord.flags` keeps, and the aggregate it shares

`DirectoryRecord` is not affected by the fold, and must not be. It deliberately pays the `GROUP BY` scan — [the table in read-path](../read-path.md#what-differs-between-the-implementations) makes that a design property, not an omission.

So after this change the flags aggregate exists twice: once inside `fold_inputs` reading `_raw_<dim>` columns, once in `DirectoryRecord` reading parquet columns directly. Sharing them is in scope, done **last**, once `_flags_arm` is gone and the remaining pair is what has to fit.

The two differ only in what the per-dim `bool_or` reads, so the shared aggregate takes that as its one parameter:

```python
def flags_aggregate(rel, members, dims, per_dim, *, attribute=None): ...


# per_dim: (dim) -> (varies_expr, broadcast_expr)
#   fold       bool_or(col("_raw_<d>").isnotnull())  /  ...isnull()
#   directory  bool_or(col(d).isnotnull())           /  ...isnull()
# attribute: a literal where the caller aggregates one attribute's own table,
#   `None` where it groups by an `attribute` column.
```

`flags_from_rows` already shares the tail — the coordinate scoping — and stays as it is.

With two callers rather than three the case is weaker than it was, so this is the part to drop if it does not fit cleanly: two honest aggregates beat one helper with a flag for each caller. What must not happen is attempting it _first_ — the parameter list would be shaped by three callers, one of which this change deletes.

## What lands first, and separately

Two changes this rests on are worth their own pull requests, because both stand alone and neither should be buried in a fold rewrite.

**`components` → `entity_type`.** `entity_type` is already the column name everywhere — 78 uses across core — while `component` survives in the API surface and the `dims/components/` path, which never got carried along when the column was renamed. `component_frame`, `component_columns`, `fold_components`, `_component_deleted`, `component_types`, `Record.components`, `dims/components/` on disk, and `tools/pypsa.py`'s module docstring all say the older word for the thing `entity_type` names. Landing that first means this proposal is written in one vocabulary and keeps its "no format change" line honestly — the directory move is that PR's, not this one's.

**Glob-based axis discovery.** `axes()` above is the protocol's shape for it, but the underlying fix is to `resolve_dims`/`fold_axis` and is worth having with or without this change: stop probing every declared dim in every ancestry directory, list each directory once instead.

**`order_key` as `(depth, row)`.** Today it is a scalar `BIGINT`, assigned as `COALESCE((SELECT max(order_key) FROM parent), -1) + row_number() OVER (ORDER BY _row)`. That expresses "later layers come after earlier ones" arithmetically, and the flattening costs one thing: **a layer's numbering depends on its parent's values**, so the fold step has to read the parent's map to number its own rows.

A struct says the same thing directly — `depth` is the source's position, `row` its file order within that layer — and DuckDB orders structs lexicographically by field, so `ORDER BY order_key` keeps working unchanged at `_ordered`, `tools/pypsa.py` and the tests, and `max(order_key)` still means what it means. First-introduced order is preserved by construction: a key introduced at depth 2 and restated at depth 5 keeps `(2, n)`, because the parent's rows pass through the anti-join with their own key intact.

Why it belongs with this proposal rather than in it: the subquery it deletes is exactly the one [question 4](#4-what-the-staged-step-re-executes) flags as re-running per read once the tail is live. With a tuple there is nothing to hoist — a source's contribution becomes computable from the source and its index alone, which is what "one fold over a list of sources" ought to mean, and the fold stops reaching into its parent's data to number rows.

It is a format change: `nw.Int64()` becomes a struct in `component_columns` and every `group_columns`, and materialised maps change shape. Pre-1.0, so that is a cost rather than a blocker, but it is why this is its own pull request. The case to test is the restated key above, and a child folding onto a *materialised* parent, whose depths are the fold's result rather than a position in any list — its `max(order_key.depth) + 1` is the same arithmetic one level up.

## Where the code is

| what                    | where                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------ |
| the three kind-folds    | `layered/resolve.py` — `fold_inputs`, `fold_components`, `fold_group`, sharing `_fold_ordered`                     |
| what drives them        | `_fold_map` (folds one kind down an ancestry), `map_kinds` (which kinds a schema has), `_fold_kind`                |
| what caches them        | `_table` (`.create()` per kind), `NodeCache.dims` / `.schema`, `Revision.node_cache` — all justified by write-once |
| what builds the ancestry | `revision.py` — `Revision.node_cache`, `ancestry`; `resolve.ancestry_to_read`, which becomes source construction |
| where a layer's paths   | `duck.py` — `layer_dir`, `resolved_dir`; read through `try_read_parquet`. Every call site becomes a source member  |
| the axis fold           | `resolve_dims` + `duck.py` — `dims_dirs`, `fold_axis`; keyed by `schema.axis_key`, outside the owner map           |
| the reads over the map  | `NodeCache.relation` / `component_frame` / `group_frame` / `_owned_frame` / `attributes_of`                        |
| the directory's reads   | `directory.py` — `_read`, `_require`, `_keyed_by`                                                                  |
| the staged counterparts | `mutable.py` — `_overlay`, `_entity_union`, `_group_union`, `_collapsed_inputs` / `_entities` / `_group` / `_axis` |
| the staged flags        | `mutable.py` — `flags`, `_flags_arm`                                                                               |
| first-touch hook        | `mutable.py` — `_ensure`, `_empty_long`, `_owned_whole`, `_restated`                                              |
| what commit hands over  | `mutable.py` — `staged_only`, `flattened`, both building a `_Written`                                              |
| the shared tail         | `record.py` — `flags_from_rows` (scoping), `duck.py` — `null_safe`, `broadcast_match`, `union_all_by_name`         |

`_fold_ordered` is worth reading first: `fold_components` and `fold_group` already differ only in file, key columns, whether a type is carried and whether a second tombstone applies — so the source is a further parameter of a shape that already exists, not a new idea.

Note that `mutable.py` imports from `layered/` only inside function bodies, to avoid a cycle (`_base_revision`, `commit`). Keep it that way.

## A suggested order

0. **Assert the identity first, as a spike.** A `WorkingRecord` with nothing staged must read identically to its base — same keys, same rows, same `flags`. Today that path goes through `_overlay` rather than delegating, so the assertion may already fail on column order, a restored `entity_type`, or `order_key`. Half an hour, and it decides whether step 1 is a fixture or a bug fix: everything below assumes the identity holds, so finding out it does not is worth knowing before any of it.
1. **Extend the `both` fixture to a `WorkingRecord`** ([below](#how-to-know-it-worked)). The net everything after this falls into.
2. **Move `_restated` to first touch.** Self-contained, testable against the existing commit tests, and it is what the rest depends on. `pending` goes here, with its tests.
3. **Declare results.** Independent of the fold, and it is what lets `all_attributes("outputs")` name its files rather than glob. `value_hint` is retired here — see [question 3](#3-results-become-schema-declared).
4. **`LayerSource` and `ParquetLayer`**, replacing `layer_dir(uuid)` in the fold and in `NodeCache`'s reads with a source lookup. No behaviour change and no second implementation: one source doing exactly what `layer_dir` did.
5. **Then the fold over sources**, one kind at a time: `fold_components` first, being the simplest (no flags, no broadcast, one key column), `fold_group` next, `fold_inputs` last.
6. **Then the frozen-prefix rule**: `NodeCache` takes `sources` rather than an ancestry of UUIDs, materialises up to the last `frozen` one, and dispatches a winning `layer_uuid` through the source that carries it. With one implementation every source is frozen, so this is still no behaviour change.
7. **Then `DirectorySource` and `StagedSource`**, and the deletions they enable.
8. **Then [the flags aggregate](#6-what-directoryrecordflags-keeps-and-the-aggregate-it-shares)**, if it fits.

Steps 0–4 are independently valuable and reversible: step 0 is information either way, step 2 simplifies the commit path whatever happens next, step 3 tightens the schema, and step 4 is a refactor with one implementation. If the thing stalls after any of them, what landed is still an improvement.

Three things this order deliberately does not fix, recorded so they are not mistaken for oversights:

- **`attributes_of` re-executes under a live tail.** It aggregates the map a second time — a `bool_or` union across a type's members — and `fetchall()`s, so with the tail live that is a query per call rather than a plan. Accepted: it is bounded by the map rather than by the staging area, and the deletions are worth it.
- **`commit` still needs a revision.** `_base_revision` requires a `LayeredRecord` base and reads `node_cache.revision_id`, so `NodeCache` keeps that field and a `DirectorySource`-backed `WorkingRecord` still cannot `NewChild()`. `DirectorySource` makes such a record *readable* through the fold, not committable to a layer tree.

## How to know it worked

This change rewrites resolution without changing a single answer, so the whole of its correctness is "nothing moved". Three things pin that, and they exist already.

**`tests/test_records.py` is the harness.** Its `both` fixture writes one record and reads it back as a `LayeredRecord` and a `DirectoryRecord`, then asserts the two agree on every key set, on `flags` per type, and on the resolved rows per attribute. That is exactly the invariant a fold rewrite can break, and it already runs. `test_backings_agree_on_flags` is the one to extend: it asks two backings to agree, and the duplicated aggregate is exactly what a third would catch.

**The gap: a `WorkingRecord` is not in that fixture.** It satisfies `Record`, so it can be — a `WorkingRecord` with nothing staged must read identically to its base, and one with staged edits must read identically to the record its `commit` produces. Both over a layered base *and* over a directory base, which is what pins step 7. Adding those before touching the fold is the cheapest way to make this change verifiable.

**`test_mutable.py:362-380` pins the flags union across a staged edit** (`before`/`after` around a `set`), which is the behaviour step 5 must preserve when `_flags_arm` goes.

**A read-after-`set` test is what pins the live tail.** `set`, read, `set` again, read again: the second read must see the second edit. That is the whole content of "the staged step is not cached", and it fails loudly if anyone later caches past the frozen prefix — a `cached_property` over the tail, or a `.create()` that does not stop where `frozen` does.

Three performance claims to check rather than assert:

- **`set` on first touch** now folds the base for a completed attribute. Time it on a deep unmaterialised ancestry, and confirm a second `set` on the same attribute reads nothing.
- **A read after a `set`** re-executes the staged fold, where a written layer's fold is cached. The claim is that it costs one layer, not one ancestry — so time a read on a deep unmaterialised ancestry with one staged edit, against the same read with none.
- **A `WorkingRecord` over a directory** now folds where it scanned. `tests/test_scaling.py` measures peak materialisation rather than ancestry depth, so none of these is covered today.

**One correctness claim worth a test of its own.** Once `StagedSource` restates, a staged layer's map rows are keyed like a written layer's — but the fold's flags are computed per group, and `attributes_of` `bool_or`-unions them again across a type. That should make the grouping grain invisible to the answer. Should, not does: assert it, since it is the assumption that lets the staged and parquet paths share one aggregate.

## What it costs

**A protocol in `layered/` that `mutable.py` and `directory.py` implement.** `mutable.py` currently imports from `layered` only at call time, to avoid a cycle (`_base_revision`, `commit`). A `LayerSource` satisfied structurally needs no import either way. `directory.py` already imports from `layered.resolve`, so nothing changes there. Worth checking the first stays true.

**`pending` is removed**, with its documentation and its tests. The public surface loses an accessor that answered "what have I staged".

**Results must be declared**, which constrains a tool that attaches results for components it discovered mid-solve. [Question 3](#3-results-become-schema-declared) is the whole of that argument. What does *not* change is discovery: `outputs` keys off what a layer holds, sorted, exactly as `attributes` does.

**`set` grows a first-touch base read** for attributes with a non-`partial` axis. Bounded by what is staged, not by how often it is staged.

**A read with pending edits re-folds the staged step every time.** Today `_collapsed_inputs` rebuilds per attribute read, so this is the same bill at a coarser grain: the whole map rather than one attribute. It buys the deletions and the shared flags, and [`materialise` over a staging area](#what-it-opens) is the escape hatch if a long-lived `WorkingRecord` ever makes it hurt.

**`NodeCache` gains a rule its docstring must carry.** "Nothing an instance caches can go stale" becomes "nothing up to the last `frozen` source can", and `dims` stops being a `cached_property`. The invariant is no weaker — it is derived from the sources rather than assumed of them — but it is one more thing to hold while reading the module the design calls "relational algebra of real complexity".

**Roughly 250 lines deleted from `mutable.py`** — `_overlay`, `_entity_union`, `_group_union`, `_flags_arm`, and the per-kind frame assembly (`_attribute_frame`, `_entity_frame`, `_group_frame`, `_axis_frame`) they serve — plus `_restated`'s move out of the commit path. The `_collapsed_*` cluster is not deleted but moved: it becomes `StagedSource`'s member bodies, which is where "the layer as it would be written" was always being computed.

Those deletions are what make the two smaller cleanups this grew out of unnecessary rather than pending: the [flags aggregate](#6-what-directoryrecordflags-keeps-and-the-aggregate-it-shares) is folded into the last step above, and a shared `overlay(older, newer, on)` helper for the five anti-join-then-union sites has no second caller left once four of them are gone.

**No format change of its own.** The two that touch disk — [the rename and `order_key`](#what-lands-first-and-separately) — land first and separately, and nothing here adds to them: this change is entirely about who computes the overlay.

**Three design pages owe an edit**, none of them optional — when behaviour changes, the page changes, not just the code:

- [committing](../working-record.md#committing) calls `_restated` a commit-time step. It becomes a property of how a staged layer reads, which is arguably where it belonged: it describes what the layer *is*, not what commit does to it.
- [`pending`](../working-record.md#pending) is a section, and the staging section's "these tables are the only place a staged row exists: `pending` counts them and the reads fold them" names it. Both go.
- [results through `kind="outputs"`](../working-record.md#results-through-kindoutputs) says a result attribute is not schema-declared, and gives the reason. Question 3 reverses that; the page keeps the membership rule and loses the declaration one.
- [reading with pending edits](../working-record.md#reading-with-pending-edits) says the staged fold "costs what one more layer costs". True per read, and the page should say per read — a written layer pays that once and is cached forever, the staged one pays it on every read, being the only layer that can still change.

## What it opens

**Whether `materialise` could run over a staging area.** [The frozen-prefix rule](#the-cache-stops-at-the-last-frozen-source) names this exactly: freeze the tail into the prefix. A long-lived `WorkingRecord` whose edits have settled could fold its staged step once into a `NodeCache` and read from that, at the price of having to discard it on the next `set` — which is the generation bookkeeping this proposal avoids needing, deferred to the one case that would pay for it. Nothing here needs it.

**Whether the commit path collapses further.** `staged_only()` and `flattened()` build two `Record`s out of one staging area. With `_restated` moved to first touch, `staged_only()` *is* the staged source and `flattened()` is the fold's own output — so both readings become projections of one object rather than two assembled `_Written`s.

**Whether a `DirectoryRecord` gains a map when something wants one.** The fold now runs over a directory, so the option exists. It should stay unexercised: the scan is a design property, and the sentence in [read-path](../read-path.md#what-differs-between-the-implementations) is what keeps it one.
