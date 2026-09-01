# Proposal: staging as a layer — one fold, two sources

Status: **Draft** · Drafted 2026-09-01

[`WorkingRecord`](../working-record.md) already claims to be a layer:

> A set of pending edits **is** a layer — an unwritten one. So the reads compose the same way: the staged rows are the last layer, resolved over whatever the record was reading before. ([reading with pending edits](../working-record.md#reading-with-pending-edits))

The code does not deliver that. `mutable.py` reimplements the overlay in its own terms — an anti-join and a union per kind, a `_collapsed_*` per kind, its own flags aggregate — while `layered/resolve.py` folds. This proposal makes the sentence true: one fold, reading either parquet layers or a staging area, differing in where a layer's rows come from and nothing else.

## What starts it

Three pieces of the same algebra exist twice, and the duplication is not incidental — each pair was written from the same design paragraph and drifted anyway.

**The overlay.** `fold_inputs` anti-joins the parent on `input_key` and unions the layer's own rows ([resolve.py](https://github.com/energy-models/datarecord/blob/main/datarecord/layered/resolve.py)); `_overlay`, `_entity_union` and `_group_union` do the same against staged rows. Five sites, one rule.

**The flags.** The fold computes `varies`/`broadcast`/`breakpoints` in its ownership `GROUP BY`, so `LayeredRecord.flags` is free. `WorkingRecord._flags_arm` computes the identical two structs over the staging tables, one arm per attribute, then unions them into the base's answer. This one had a real defect until recently: the staged arm was not scoped to the attribute's own coordinates, so a dim the attribute was not addressed by could be reported as broadcast — [what `flags` says must not happen](../record.md#flags). The shared `flags_from_rows` now fixes it, but the aggregate itself is still written twice.

**The collapse.** `_latest_per` collapses staged rows last-write-wins per coordinate; the fold does the same across layers by depth. Different mechanisms (`_seq` versus layer order) for one rule.

The cost is not the line count. It is that a change to how resolution works has to be made twice, in two vocabularies, and the second one is easy to forget — as the flags scoping was.

## The construct

The fold's three kind-folds are hardcoded to read one layer's parquet:

```python
def fold_inputs(revision_id, keys, con, parent):
    rel = try_read_parquet(layer_dir(revision_id) + "inputs/*.parquet", con, ...)
```

`revision_id` is used for exactly two things: deriving those paths, and stamping `layer_uuid`. Replace it with a **source** that answers the same questions:

```python
class LayerSource(Protocol):
    """One layer's own rows, however they are stored."""

    @property
    def layer_id(self) -> UUID: ...  # what the fold stamps as `layer_uuid`

    def inputs(self) -> DuckDBPyRelation | None: ...
    def entities(self) -> DuckDBPyRelation | None: ...  # dims/entity.parquet
    def group(self, name: str) -> DuckDBPyRelation | None: ...
```

Three methods, because the fold reads three shapes. `None` means the layer wrote nothing of that kind, which is what `try_read_parquet` already answers.

Two implementations:

- **`ParquetLayer(revision_id)`** — what exists today, reading `layer_dir(revision_id)`.
- **`StagedLayer(working_record)`** — returns the collapsed staging tables, with a synthetic `layer_id`.

Then `NodeCache` folds over a list of sources rather than a list of UUIDs, and a `WorkingRecord` over a layered base is the same fold with one more source appended. `flags` falls out of the ownership group-by for both, and `_flags_arm` is deleted rather than parametrised.

## What has to be settled first

Four things. The first is the one that decides whether this is worth doing at all.

### 1. Two key modes, or eager restatement

This is the blocker.

The fold keys ownership by `input_key` — `partial_dims` plus `attribute`. The staged overlay keys by **coordinate**, and both `_overlay` and `_collapsed_inputs` carry a comment saying why:

> Per _coordinate_, not per input key: the input key excludes the dims an attribute is not owned per, so keying on it alone would let one staged snapshot displace the base's whole series on read.

The fold gets away with `input_key` because by the time a layer is on disk, [`_restated`](../working-record.md#committing) has already completed the extent along every non-`partial` axis — that is the "one commit-time read of parent data". A staging area has not done that yet, and must not: restating eagerly on every `set` would read the parent series on every edit, which is exactly the cost `_restated` exists to pay once.

So a staged layer is **not** shaped like a written one. Three ways out:

- **(a) The fold learns a second key mode.** `fold_inputs` takes the key columns as a parameter, `input_key` for a parquet layer and coordinates for a staged one. Smallest change, but it puts a mode flag in the middle of the fold, and the two modes are not interchangeable — a reader has to know which applies.
- **(b) `StagedLayer.inputs()` restates lazily.** The source applies `_restated` when asked, so what it hands the fold is already a complete extent. The fold stays single-mode. Cost: every read of a `WorkingRecord`'s attributes pays the parent read that commit pays once — on a deep ancestry, the thing materialised caches exist to avoid.
- **(c) Keep the read overlay separate and unify only the write path.** `staged_only()` becomes a `LayerSource`, so commit and the fold agree; reads keep `_overlay`. Deletes the least, and leaves the flags duplication in place.

**(a) is the recommendation**, on the grounds that the two modes correspond to a real distinction — owned-whole-yet versus not — and naming it as a parameter is more honest than hiding it behind a lazy restate. But it needs a name: `owned_key` versus `coordinate_key` is the distinction, and if it cannot be named crisply, that is evidence for (c).

### 2. What a non-layered base does

`WorkingRecord`'s base may be a `DirectoryRecord`, or a framework object satisfying `Record`. Neither has an ancestry of UUIDs, and `DirectoryRecord` has no owner map at all — [by design](../read-path.md#what-differs-between-the-implementations), its `flags` is a scan and its resolution is "what the files hold".

So the fold cannot be the only path: a `WorkingRecord` over a directory has nothing to fold against. Options:

- The staged fold produces a one-layer owner map over the base's frames, treating the base as the root. Uniform, but it means building an owner map for a directory record, which the design says that implementation does not have.
- `WorkingRecord` keeps two paths — fold where the base is layered, overlay where it is not. Honest, but it is the duplication this proposal set out to remove, now conditional.

**Unsettled.** The second is probably right and considerably less satisfying. Worth checking how much of `mutable.py` actually survives it before committing to the whole change — if the non-layered path keeps `_overlay` alive anyway, the win shrinks to the flags and the commit path.

### 3. Whether `_seq` collapsing belongs to the source

Staged rows are appended and collapsed last-write-wins per key within the layer (`_latest_per`); a parquet layer's rows are already unique per key. If `StagedLayer` collapses before handing over, the fold never learns about `_seq` — which is right, `_seq` being staging bookkeeping.

Straightforward, but it makes the source more than a reader: it is "the layer as it would be written", not "the rows as staged". That is the correct framing, and it should be stated in the protocol's docstring so nobody adds a raw-rows method later.

### 4. What `DirectoryRecord.flags` keeps, and the aggregate it shares

`DirectoryRecord` is not affected by the fold, and must not be. It deliberately pays the `GROUP BY` scan — [the table in read-path](../read-path.md#what-differs-between-the-implementations) makes that a design property, not an omission.

So after this change the flags aggregate exists **twice**: once inside `fold_inputs` reading `_raw_<dim>` columns, once in `DirectoryRecord` reading parquet columns directly. **Sharing those two is in scope for this change**, done last, once `_flags_arm` is gone and the remaining pair is what has to fit.

The two differ only in what the per-dim `bool_or` reads, so the shared aggregate takes that as its one parameter:

```python
def flags_aggregate(rel, members, dims, per_dim, *, attribute=None): ...


# per_dim: (dim) -> (varies_expr, broadcast_expr)
#   fold       bool_or(col("_raw_<d>").isnotnull())  /  ...isnull()
#   directory  bool_or(col(d).isnotnull())           /  ...isnull()
# attribute: a literal where the caller aggregates one attribute's own table,
#   `None` where it groups by an `attribute` column.
```

`flags_from_rows` already shares the tail — the coordinate scoping, which is where [the defect](#what-starts-it) was — and stays as it is.

With two callers rather than three the case is weaker than it was, so this is the part to drop if it does not fit cleanly: two honest aggregates beat one helper with a flag for each caller. What must not happen is attempting it _first_ — the parameter list would be shaped by three callers, one of which this change deletes.

## Where the code is

| what                     | where                                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| the three kind-folds     | `layered/resolve.py` — `fold_inputs`, `fold_components`, `fold_group`, sharing `_fold_ordered`                      |
| what drives them         | `_fold_map` (folds one kind down an ancestry), `map_kinds` (which kinds a schema has), `_fold_kind`, `_table`       |
| where a layer's paths    | `duck.py` — `layer_dir`, `resolved_dir`; read through `try_read_parquet`                                            |
| the reads over the map   | `NodeCache.relation` / `component_frame` / `group_frame` / `_owned_frame` / `attributes_of`                         |
| the staged counterparts  | `mutable.py` — `_overlay`, `_entity_union`, `_group_union`, `_collapsed_inputs` / `_entities` / `_group` / `_axis`  |
| the staged flags         | `mutable.py` — `flags`, `_flags_arm`                                                                                |
| what commit hands over   | `mutable.py` — `staged_only`, `flattened`, both building a `_Written`                                                |
| the shared tail          | `record.py` — `flags_from_rows` (scoping), `duck.py` — `null_safe`, `broadcast_match`, `union_all_by_name`          |

`_fold_ordered` is worth reading first: `fold_components` and `fold_group` already differ only in file, key columns, whether a type is carried and whether a second tombstone applies — so the source protocol is a fourth parameter of a shape that already exists, not a new idea.

Note that `mutable.py` imports from `layered/` only inside function bodies, to avoid a cycle (`_base_revision`, `commit`). Keep it that way.

## A suggested order

The two blockers are cheap to answer empirically and expensive to guess at, so they come before the refactor rather than during it.

1. **Extend the `both` fixture to a `WorkingRecord`** ([above](#how-to-know-it-worked)). Standalone value, no design decisions, and it is the net everything after this falls into.
2. **Answer (2) by counting.** Grep what `WorkingRecord` actually does when its base is a `DirectoryRecord` — if the non-layered path has to keep `_overlay` alive, the deletion shrinks to the flags and the commit path, and the whole proposal may be worth reducing to (1)(c). This is a half-hour question and it decides the size of everything else.
3. **Answer (1) by trying to name it.** Write the `LayerSource` protocol and the two key modes; if the parameter cannot be named crisply — `owned_key` versus `coordinate_key`, or better — take that as the evidence for (c) it is.
4. **Then the fold**, one kind at a time: `fold_components` first, being the simplest (no flags, no broadcast, one key column), `fold_group` next, `fold_inputs` last.
5. **Then the deletions**, and only then [the flags aggregate](#4-what-directoryrecordflags-keeps-and-the-aggregate-it-shares).

Steps 1–3 are reversible and answer whether 4–5 are worth doing. If the answer is no, the honest outcome is a shorter change under (1)(c) plus the test from step 1 — which is a good result, not a failed one.

## How to know it worked

This change rewrites resolution without changing a single answer, so the whole of its correctness is "nothing moved". Three things pin that, and they exist already.

**`tests/test_records.py` is the harness.** Its `both` fixture writes one record and reads it back as a `LayeredRecord` and a `DirectoryRecord`, then asserts the two agree on every key set, on `flags` per type, and on the resolved rows per attribute. That is exactly the invariant a fold rewrite can break, and it already runs. `test_backings_agree_on_flags` in particular is what would have caught [the scoping defect](#what-starts-it) had it been asked of a third backing.

**The gap: a `WorkingRecord` is not in that fixture.** It satisfies `Record`, so it can be — a `WorkingRecord` with nothing staged must read identically to its base, and one with staged edits must read identically to the record its `commit` produces. Adding those two before touching the fold is the cheapest way to make this change verifiable, and they are worth having whatever is decided about (1) and (2).

**`test_mutable.py:362-380` pins the flags union across a staged edit** (`before`/`after` around a `set`), which is the behaviour question 4 must preserve when `_flags_arm` goes.

Under (1)(b) there is also a performance claim to check rather than assert: a lazily-restating source pays a parent read per attribute read. `tests/test_scaling.py` measures peak materialisation rather than ancestry depth, so it would not catch that — a deep-ancestry read benchmark would have to be written, which is itself an argument for (a).

## What it costs

**A protocol in `layered/` that `mutable.py` implements.** `mutable.py` currently imports from `layered` only at call time, to avoid a cycle (`_base_revision`, `commit`). A `LayerSource` implemented by a `WorkingRecord` inverts that: `layered` defines the protocol, `mutable` satisfies it structurally, so no import is needed either way. Worth checking that stays true.

**The fold gains a parameter it did not have.** Under (a), `fold_inputs` takes its key columns rather than reading `input_key`. That is one more thing to get wrong in the module the design calls "relational algebra of real complexity".

**Roughly 250 lines deleted from `mutable.py`** if (2) resolves toward the uniform path — the `_collapsed_*` cluster, `_overlay`, `_entity_union`, `_group_union`, `_flags_arm`. Materially less if `WorkingRecord` keeps a non-layered path.

Those deletions are what make the two smaller cleanups this grew out of unnecessary rather than pending: the [flags aggregate](#4-what-directoryrecordflags-keeps-and-the-aggregate-it-shares) is folded into the last step above, and a shared `overlay(older, newer, on)` helper for the five anti-join-then-union sites has no second caller left once four of them are gone. Only under (1)(c), which keeps the read overlay, would that helper still be worth extracting.

**No format change.** Nothing on disk moves; this is entirely about who computes the overlay.

## What it opens

**Whether `materialise` could run over a staging area.** If staged edits are a layer, a long-lived `WorkingRecord` could materialise its own owner map, which is what makes reads over a big pending edit set cheap. Nothing here needs it, and it is only reachable once the fold accepts a non-parquet source.

**Whether the commit path collapses further.** `staged_only()` and `flattened()` build two `Record`s out of one staging area. If the staged rows are already a layer, `staged_only()` is that layer and `flattened()` is the fold's own output — so both readings might be projections of one object rather than two assembled `_Written`s.

**Whether `_restated` moves.** If (1) resolves toward (b), `_restated` stops being a commit-time step and becomes a property of how a staged layer reads — which is arguably where it belonged, since it is describing what the layer *is* rather than what commit does to it. Under (a) it stays exactly where it is.
