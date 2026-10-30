<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 25: a layer carries its manifest

| DEP      | 25                           |
| -------- | ---------------------------- |
| Title    | a layer carries its manifest |
| Author   | Jonas Hörsch                 |
| Status   | Draft (2026-10-29)           |
| Requires | [DEP 9](dep-0009-store.md)   |
| Created  | 2026-10-29                   |

## Abstract

Each layer carries its own `manifest.json`: its id, its parent, the full schema it was written under, the files it wrote and a flat `meta` map. The manifest replaces both the store's one `manifest.json` and the `revisions/` tree. A layer is claimed by a create-only `claim.json` when its id is handed out and committed by its manifest, so a job may hold a revision open for days. Every file in a store is then written once, and the manifest's compare-and-swap goes.

## Motivation

Layers and revision files are write-once, but the store's `manifest.json` is not. Every write that widens the schema races on it through a [bounded compare-and-swap](../storage/concurrency.md#the-manifests-compare-and-swap), a writer can lose that race after its layer is written, and a credential that cannot read the manifest fails every write. Because the store has one schema, adding an attribute [amends the root schema](../model/schema.md#one-schema-per-record), so one branch's experiment widens its siblings.

A workflow that writes one revision per run has nowhere to put the run's id beside the revision, and keeps the mapping outside the store. A run that takes days writes its layer files long before the [revision file](../storage/store.md#a-revision-exists-only-by-its-file) makes them reachable; until then they look like the leftovers of a failed write, and a cleanup cannot tell the two apart.

A standalone directory and a layer differ only by where the manifest sits, yet `ws.at` and `store.layer` read the same files under two rules.

## Specification

### Layout

```text
<location>/
└── layers/<id>/
    ├── claim.json          # written when the id is handed out, kept
    ├── manifest.json       # written last: makes the revision exist
    ├── dims/<dim>.parquet
    ├── attributes/<attr>.parquet
    └── resolved/           # node caches, unchanged
```

`<location>/manifest.json` and `<location>/revisions/` go. A standalone directory is one layer at its own URI: `manifest.json` with `parent: null`, beside `dims/` and `attributes/`.

### The layer manifest

```python
class Manifest(BaseModel):
    id: UUID
    parent: UUID | None
    schema: Schema  # full, not a delta
    files: Files  # {"dims": [...], "attributes": [...]}
    meta: dict[str, str | int | float | bool] = {}
```

`schema` is the full schema the layer was written under; a child's must pass `compatible_with` against its parent's, and a root's is free. `files` lists what the layer wrote, by declared name: a read opens exactly these files and globs nothing, and ignores any file the list does not name. `meta` is the revision's own: flat, immutable, set at the write and not inherited by a child. `schema.meta` stays, and is inherited with the rest of the schema. `Schema.id` goes, since the id is the manifest's.

### Claim and commit

| step   | file                   | write         | holds                         |
| ------ | ---------------------- | ------------- | ----------------------------- |
| claim  | `claim.json`           | create-only   | `id`, `parent`, `meta`, start |
| data   | `dims/`, `attributes/` | unconditional | the layer's parquet           |
| commit | `manifest.json`        | create-only   | the layer manifest            |

A claim is create-only and is kept after the commit, so a second claim of one id fails at any time and an id names one revision forever. The manifest is the commit: a revision exists by its manifest, and a layer with a claim and no manifest is _pending_, which no read reaches. The commit checks that the manifest's `parent` is the claim's and that its `meta` keeps every key of the claim's unchanged; it may add keys, such as an outcome the job learns at its end.

Data goes straight into `layers/<id>/`, locally as remotely, and the local staging directory and its rename go. Each `resolved/` file keeps its local temporary name and rename, since no manifest commits a node cache and its presence is the signal. A remote write needs none, an object appearing whole or not at all.

### Writing

```python
store.claim(*, parent: Revision | None, meta: Meta = {}) -> Claim
claim.commit(source, *, meta: Meta = {}) -> Revision
store.commit(source, *, parent: Revision | None, meta: Meta = {}) -> Revision
store.commit(w, *, meta: Meta = {}) -> Revision  # w a WorkingRecord
ws.write(source, uri, *, meta: Meta = {}) -> Record
```

`store.commit` is `store.claim(...).commit(source)` in one call, and `store.root` and `rev.child` go. `parent` is a required keyword, so a new root is always written out as `parent=None`; a parent from another store is refused, naming both locations.

A `WorkingRecord` brings its parent. `store.commit(w)` writes `w`'s edits, a delta, as a child of the revision `w` was built over, `store.revision(w.revision_id)`, and refuses a `parent` passed beside it. A base that is no revision of the store, a standalone directory or another store's revision, is refused, naming `ws.write(w, uri)`, which writes `w` whole. `claim.commit(w)` refuses a `w` whose base is not the claim's parent.

A `Claim` crosses a process as a `Revision` does: it serialises as its location and id, and validates back by reading its claim file.

### Cleanup

Only an explicit call deletes, and only a pending layer:

```python
store.cleanup(ids=[...])  # these pending layers
store.cleanup(older_than=timedelta(days=14))  # pending, claimed before then
```

A layer with a manifest is never deleted. An id that is not pending is refused, naming it.

### Listing and the cache

`revisions()` lists `layers/` once and reads the manifest of each layer the workspace's `revisions` table lacks. `revisions(meta={...})` returns those whose `meta` holds every pair given, as one query over the table. `claims()` lists the pending ones from their claim files. The table caches `(location, id, parent, meta)` and only committed revisions, since a pending one can still commit.

### `Files`

`Files` gains `delete(prefix)` for the cleanup. Its `Version` precondition loses its last user and goes.

## Rationale

The schema's compare-and-swap was the one place two writers met. With a manifest per layer, each writer checks compatibility against its parent's manifest, which cannot change, so there is no race to settle and no retry. Each manifest holds the full schema rather than a delta, so one file answers a revision's schema without walking its ancestry; a schema is small beside a layer, and repeating it costs little.

The `files` inventory saves a remote read its listing per layer. Node caches are already opened by name, so a read lists no directory at all, and only `revisions()` lists `layers/`. A retried job that left partial files under its id is harmless, since only the files the committing attempt lists are read. Removal stays a [restatement of the axis](../layers/removal.md), so the fold keeps one rule.

A random id is unique without a lock; the claim exists to make a pending revision visible, to a listing by its `meta` and to the cleanup, which must not delete a job still running. Keeping it after the commit makes "an id is claimed once" a single create-only write, and no lost delete can leave the store between two states. A job may run for days, so no timeout is both safe and useful, and cleanup happens only on request.

Root and child were one path below the API already; a keyword `parent` makes the difference an argument. A `WorkingRecord` has exactly one valid parent, so it supplies it. `schema.meta` carries a framework's data about the record, such as PyPSA's network attributes, which a child inherits; a revision's `meta` says how one layer was made, such as a run id, which a child must not inherit.

It costs:

- An existing store is unreadable, having no layer manifests, and is re-exported, as a breaking change is pre-1.0.
- `store.root` and `rev.child` go. `store.commit(w)` writes `w`'s edits, where `rev.child(w)` wrote its whole resolved record; a whole record goes to `ws.write`.
- A call that names the store and a parent names the store twice, so a mismatch has to be refused, where `rev.child` could not get it wrong.
- A new attribute no longer reaches existing branches, only a revision whose ancestry declares it.
- Sibling branches may declare one name differently, which matters where records from two branches are [combined](dep-0014-combining-records.md).
- A cold listing reads one manifest per revision, as it reads one revision file today, and each is larger by its schema.
- A failed local write now leaves files behind, as a remote one does. They are pending, and the cleanup reaches them.

## Usage

```python
rev = store.commit(source, parent=None, meta={"run_id": "base"})
child = store.commit(source, parent=rev, meta={"run_id": "2026-10-29-a"})
child.meta  # {"run_id": "2026-10-29-a"}
child.schema  # the schema child was written under

w = WorkingRecord(child.record)
w.set("p_nom", 150.0, entity="wind1")
store.commit(w, meta={"run_id": "2026-10-29-b"})  # a child of `child`

claim = store.claim(parent=rev, meta={"run_id": "2026-10-29-c"})
submit(job, claim)  # the claim pickles; days later, in the job:
claim.commit(result, meta={"status": "optimal"})

store.revisions(meta={"run_id": "2026-10-29-c"})
store.claims()  # pending layers, with their meta
store.cleanup(older_than=timedelta(days=14))
```

## Implementation notes

1. `Manifest` and `claim.json`; `Schema.id` goes.
2. The write path: claim, data into `layers/<id>/`, check against the parent's schema, manifest create-only. `store.claim`, `claim.commit` and `store.commit` replace `store.root` and `rev.child`; the staging rename and `reconcile_schema` go.
3. The read path: a layer's schema and files from its manifest; `ws.at` and `store.layer` share it; no glob into a layer.
4. `revisions()`, `revision()` and `ancestry()` read manifests; the `revisions` table gains `meta`.
5. `Claim` and its serialisation, `claims()`, `revisions(meta=...)` and `cleanup`; `Files.delete`; `Files`' `Version` goes.
6. Pages: [store](../storage/store.md), [concurrency](../storage/concurrency.md), [format](../storage/format.md), [schema](../model/schema.md#one-schema-per-record), [resolution](../layers/resolution.md#materialised-node-caches), [tree](../layers/tree.md#a-layers-data-is-write-once), [writing](../use/writing.md).

Checks that hold once built:

- No file in a store is written twice, `resolved/` aside.
- A second claim of an id fails, before and after its commit.
- A pending layer is invisible to `revisions()` and to every read, and listed by `claims()`.
- A child whose schema narrows its parent's is refused at commit, and its layer stays pending.
- `cleanup` deletes no committed layer.
- `store.commit(w)` lands on `w`'s base, and refuses a `w` over a standalone directory.
- A commit that changes a claimed `meta` key is refused, and one that adds a key passes.

## Rejected Ideas

- A schema delta per layer: reading a schema would fold the ancestry, and each delta would need its own compatibility rule.
- Claiming an id by creating its directory: atomic only on a local filesystem, and S3 has no directories.
- Dropping the claim after the commit: a reused id would fail only at its manifest write, days later, and a failed delete would leave a committed layer that looks pending.
- A heartbeat in the claim: a file a job rewrites for days, for a timeout no long job can live with.
- `rev.child`, with the parent as receiver: it names the store once, but splits one write into a root call and a child call, and leaves a pending claim without a receiver.
- A commit that keeps the claim's `meta` exactly: a job learns its outcome at its end, and would have to put it outside the store.
- Removals in the inventory: a second way to remove, which the fold would also have to obey.

## Open Issues

- Combining records across branches: two branches may declare one name differently, and [combining](dep-0014-combining-records.md) them needs a schema merge or a refusal.
- Materialising automatically, every K layers below the deepest materialised ancestor: deferred until owner-map size and fold time are measured on a large record.
- Writing in steps, `claim.write(...)` before a final `claim.commit()`, for a job that stores results as they come: the writer's checks against the layer's final axes would move to the commit.
- A moving pointer, such as a named head or one child per parent: needs a mutable file with a compare-and-swap, and is left out.
