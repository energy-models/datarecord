<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 25: a layer carries its manifest

| DEP      | 25                           |
| -------- | ---------------------------- |
| Title    | a layer carries its manifest |
| Author   | Jonas Hörsch                 |
| Status   | Implemented (2026-11-03)     |
| Requires | [DEP 9](dep-0009-store.md)   |
| Created  | 2026-10-29                   |

The design landed in [store](../../reference/store.md), [format](../../reference/format.md), [schema](../../reference/schema.md), [layers](../../reference/layers.md) and [resolution](../../reference/resolution.md), which win where they and this DEP differ.

## Abstract

Each layer has its own `manifest.json`, which lists the layer's id, parent, full schema, files and a flat `meta` map. The layer manifests replace the store's one `manifest.json` and its `revisions/` tree. A create-only `claim.json` reserves a layer's id and the manifest commits it, so a job may keep a revision open for days. Every file in a store is then written once.

## Motivation

Before this DEP, every write that widens the schema races on the store's one `manifest.json` through a bounded compare-and-swap. A writer can lose that race after writing its layer, and a credential that cannot read the manifest fails every write. With one schema per store, one branch's new attribute widens its siblings.

A workflow has no place in the store for a run's id. A run that takes days writes layer files that no revision file reaches yet, and a cleanup cannot tell them from a failed write. A standalone directory and a layer differ only by where the manifest sits, yet `ws.at` and `store.layer` read them under two rules.

## Specification

### Layout

```text
<location>/layers/<id>/
├── claim.json          # written when the id is reserved, kept
├── manifest.json       # written last: the revision exists from then
├── dims/<dim>.parquet
├── attributes/<attr>.parquet
└── resolved/           # materialised owner map, unchanged
```

`<location>/manifest.json` and `<location>/revisions/` go. A standalone directory is one layer at its own URI, with `parent: null` in its `manifest.json`.

### The layer manifest

`schema` is the full schema the layer was written under, not a delta. `files` lists the dims and attributes the layer wrote, by declared name. `meta` is a flat map of `str`, `int`, `float` or `bool` values, set at the write, immutable and not inherited.

A child's `schema` must pass `compatible_with` against its parent's, and a root's is free. A read opens exactly the files that `files` lists, globs nothing and ignores any other file in the layer. `schema.meta` stays, and a child inherits it with the rest of the schema. `Schema.id` goes.

### Claim and commit

| Step   | File                   | Write         | Contains                          |
| ------ | ---------------------- | ------------- | --------------------------------- |
| claim  | `claim.json`           | create-only   | `id`, `parent`, `meta`, `created` |
| data   | `dims/`, `attributes/` | unconditional | the layer's parquet               |
| commit | `manifest.json`        | create-only   | the layer manifest                |

A claim stays after the commit, so a second claim of one id fails at any time. A revision exists from the moment its manifest does. A layer with a claim and no manifest is _pending_, and no read reaches it. The commit checks that the manifest's `parent` equals the claim's, and that its `meta` keeps every key of the claim's unchanged. It may add keys, such as an outcome the job learns at its end.

Data goes straight into `layers/<id>/`, so the local staging directory and its rename go. A local `resolved/` file keeps its temporary name and rename, because no manifest commits it. A remote write needs no rename, since an object appears whole or not at all.

### Writing

```python
store.claim(*, parent: Revision | None, meta: Meta | None = None) -> Claim
claim.commit(source, *, meta: Meta | None = None) -> Revision
store.commit(source, *, parent: Revision | None, meta: Meta | None = None) -> Revision
store.commit(w, *, meta: Meta | None = None) -> Revision  # w a WorkingRecord
ws.write(source, uri, *, meta: Meta | None = None) -> Record
```

`store.commit` is `store.claim(...).commit(source)` in one call, and `store.root` and `rev.child` go. `parent` is a required keyword, so a new root is written with `parent=None`. `store.commit` refuses a parent from another store, naming both locations.

`store.commit(w)` writes `w`'s edits as a child of `w`'s base revision, and refuses a `parent` beside it. It refuses a base outside the store, and points to `ws.write(w, uri)`, which writes `w` whole. `claim.commit(w)` refuses a `w` whose base is not the claim's parent.

A `Claim` crosses a process as a `Revision` does: it serialises as its location and id, and validates by reading its claim file.

### Cleanup

`store.cleanup(ids=[...])` and `store.cleanup(older_than=timedelta(days=14))` delete pending layers, and nothing else deletes. `cleanup` never deletes a layer with a manifest, and refuses an id that is not pending, naming it.

### Listing and the cache

The workspace caches each committed manifest whole, since none ever changes, in a table `revisions` keyed by location and id. Its `meta` column is JSON, which keeps value types. Its `schema` column is a hash into `schemas`, which stores each distinct schema once. With a warm cache, building a `Record` reads no file.

- `revisions()` lists `layers/` once, and it and `revision(id)` read only the manifests the table lacks.
- `revisions(meta={...})` returns the revisions whose `meta` contains every pair given, as one query over the table.
- `claims()` caches nothing, since a pending layer can still commit.

Rows are only ever added, and `cleanup` deletes only pending layers, so it never invalidates a row. `Files` gains `delete(prefix)` for the cleanup, and its `Version` precondition goes.

## Rationale

Each writer checks compatibility against its parent's manifest, which never changes, so no race remains. A full schema gives a revision's schema from one file, and costs little beside a layer.

The `files` list saves a remote read its listing per layer, so only `revisions()` lists `layers/`. A retried job's partial files do no harm, since no manifest lists them.

The claim makes a pending revision visible to a listing by `meta`, and to the cleanup, which must not delete a running job. A kept claim makes "an id is claimed once" one create-only write, and no lost delete leaves the store between two states. A job may run for days, so no timeout is safe.

Root and child were already one path below the API, so a keyword `parent` makes the difference an argument. A `WorkingRecord` has exactly one valid parent, so it supplies it.

`schema.meta` describes the record for a framework, such as PyPSA's network attributes, and a child inherits it. A revision's `meta` describes how one layer was made, such as a run id, and a child must not inherit it.

It costs:

- datarecord cannot read an existing store, which has no layer manifests, so it is re-exported.
- `store.commit(w)` writes `w`'s edits, where `rev.child(w)` wrote its whole record. A whole record goes to `ws.write`.
- A call that names the store and a parent names the store twice, so `store.commit` must refuse a mismatch, which `rev.child` could not make.
- A new attribute reaches only a revision whose ancestry declares it.
- Sibling branches may declare one name differently, which matters where their records are [combined](dep-0014-combining-records.md).
- A cold listing reads one manifest per revision, each larger by its schema.
- A failed local write leaves pending files for the cleanup, as a remote one does.

## Usage

```python
rev = store.commit(source, parent=None, meta={"run_id": "base"})
rev.meta  # {"run_id": "base"}
rev.manifest.schema  # the schema rev was written under
claim = store.claim(parent=rev, meta={"run_id": "2026-10-29-c"})
submit(job, claim)  # the claim pickles; days later, in the job:
claim.commit(result, meta={"status": "optimal"})

store.revisions(meta={"run_id": "2026-10-29-c"})
store.claims()  # pending layers, with their meta
```

## Implementation notes

1. `Manifest` and `claim.json`, and the write path through them, replacing `store.root`, `rev.child`, the staging rename and `reconcile_schema`.
2. One read path from the manifest for `ws.at` and `store.layer`, and the cache.
3. `Claim`, `claims()`, `revisions(meta=...)`, `cleanup` and `Files.delete`.

Once built, no file in a store is written twice, `resolved/` aside, and a commit that narrows its parent's schema leaves its layer pending.

## Rejected ideas

- A schema delta per layer: reading a schema would fold the ancestry, and need a compatibility rule per delta.
- Claiming an id by creating its directory: atomic only on a local filesystem, and S3 has no directories.
- Dropping the claim after the commit: a reused id would fail days later at its manifest write. A failed delete would leave a committed layer that looks pending.
- A heartbeat in the claim: a file a job rewrites for days, for a timeout no long job can live with.
- `rev.child`, with the parent as receiver: it splits one write into two calls, and a pending claim has no receiver.
- A commit that keeps the claim's `meta` exactly: a job learns its outcome at its end, and would have to keep it outside the store.
- Removals in the `files` list: a second way to remove, which the fold would also have to obey.

## Open issues

- Combining records across branches that declare one name differently needs a schema merge or a refusal ([DEP 14](dep-0014-combining-records.md)).
- Materialising every K layers waits until owner-map size and fold time are measured on a large record.
- Writing in steps, `claim.write(...)` before `claim.commit()`, for a job that stores results as they come. The writer's checks against the final axes would move to the commit.
- A moving pointer, such as a named head or one child per parent, needs a mutable file with a compare-and-swap, and is left out.
