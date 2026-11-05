<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 30: a manifest lists its revision's axes

| DEP      | 30                                                                                                                                                                                                                                                                                                                                                                         |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a manifest lists its revision's axes                                                                                                                                                                                                                                                                                                                                       |
| Author   | Bryn Pickering                                                                                                                                                                                                                                                                                                                                                             |
| Status   | Draft (2026-11-02)                                                                                                                                                                                                                                                                                                                                                         |
| Requires | [DEP 9](dep-0009-store.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), [DEP 24](dep-0024-categories-are-enum-dimensions.md), [DEP 25](dep-0025-a-layer-carries-its-manifest.md), [DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md), [DEP 33](dep-0033-the-fold-is-a-fixed-plan.md), [DEP 35](dep-0035-a-record-holds-the-tables-it-reads.md) |
| Created  | 2026-11-02                                                                                                                                                                                                                                                                                                                                                                 |

## Abstract

When it commits a revision, datarecord writes the revision's resolved axes into the layer's `manifest.json`, beside its schema. For each dim with a label, the entry lists the number of labels and, per property, the number of labels with a stored value. A reader then learns which dims and properties a revision has from the one small JSON file that already holds its schema, with no DuckDB and no fold. `Revision.axes` returns the same entry in Python.

## Motivation

A service that runs work on a revision needs to check, before the work starts, that the revision has what the work needs. Manta, a web platform that runs modelling playbooks over a store, validates each playbook this way. A step names the dims it needs, such as `period`. It also names the properties it needs, such as a `snapshot.<name>` time aggregation that an earlier step wrote. Manta's backend has no DuckDB and cannot import datarecord. It can read small JSON files from the store's bucket.

No file in a store answers the question:

- A layer's `manifest.json` holds its id, its parent, the full schema it was written under, the files it wrote and its `meta` (`Manifest` in `store/manifest.py`). The schema declares every dim a tool may write, whether or not the revision has labels for it. For a single-country PyPSA-Eur build it declares `scenario`, `line_type` and `transformer_type`, and no layer has labels for any of them. `files` lists the files of one layer, not of its ancestry.
- A revision's axis is the deepest `dims/<dim>.parquet` on its path from the root (`Coords.resolve` and `_fold_axis` in `resolve/coords.py`). A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) takes its axis from the schema instead (`one_of_axis`). In a store holding that build and its solved results, each result layer writes `dims/sub_network.parquet` alone, so every other axis of a result revision comes from the root layer.
- An axis file may be empty. A record written whole writes one for every dim its schema declares, since `record.dims` lists each ([DEP 33](dep-0033-the-fold-is-a-fixed-plan.md)), so a file says nothing about whether the dim has labels.

A reader without datarecord would walk the parent chain through each manifest, take each dim's deepest file from the `files` lists, apply these rules, and open the parquet to count labels and property values. The rules are datarecord's, and they change. [DEP 29](https://github.com/energy-models/datarecord/blob/dep/full-write-keeps-base-properties/docs/design/proposals/dep-0029-a-layer-keeps-what-it-omits.md) proposes that a layer which omits a property column keeps its parent's values, which changes which properties a revision has. A copy of the rules in another service goes stale without notice.

The [open question](../open-questions.md#what-else-a-record-should-answer-about-its-labels-without-handing-over-a-frame) on what a `Record` should answer about its labels asks for the same kind of metadata. This DEP settles the part of it that is per dim.

## Specification

### The `axes` entry

A layer's `Manifest` gains one field, `axes`, written at commit. It maps each dim that has a label at the revision to two entries:

| key          | value                                                                                                 |
| ------------ | ----------------------------------------------------------------------------------------------------- |
| `labels`     | the number of labels in the axis, or of tuples for a [multi-dim](../model/multi-dims.md)              |
| `properties` | each property of the dim with at least one stored value, mapped to the number of labels that have one |

The root manifest of a single-country PyPSA-Eur build, with three investment periods, holds beside its id, parent, schema, files and `meta`:

```json
{
  "axes": {
    "snapshot": { "labels": 2190, "properties": { "period": 2190, "timestep": 2190 } },
    "timestep": { "labels": 730, "properties": {} },
    "period": { "labels": 3, "properties": {} },
    "entity": { "labels": 132, "properties": { "ctype": 132, "carrier": 129, "sign": 93, "type": 132 } },
    "ctype": { "labels": 10, "properties": {} },
    "bus": { "labels": 10, "properties": { "carrier": 10, "type": 10, "x": 10, "y": 10, "unit": 10, "location": 10, "shape_onshore": 5, "shape_offshore": 1, "country": 10 } },
    "carrier": { "labels": 18, "properties": { "color": 18, "nice_name": 18 } },
    "sub_network": { "labels": 1, "properties": { "carrier": 1 } },
    "role": { "labels": 3, "properties": {} },
    "connection": { "labels": 165, "properties": { "role": 165 } },
    "country": { "labels": 1, "properties": { "shape_onshore": 1, "shape_offshore": 1 } }
  }
}
```

The revision has three periods, and every one of its 2,190 snapshots maps to a period. `scenario`, `line_type` and `transformer_type` are missing, so the revision has no labels for them although its schema declares them. A child revision holding only solved results resolves to the same axes.

The entry follows four rules:

- A dim is listed exactly when its axis at the revision holds a label, as `revision.record.dims` returns it. That is every dim with `one_of`, and any other dim whose deepest axis file on the path holds a row. `record.dims` lists every declared dim, empty where no layer holds one, and a dim is missing from the entry whether an empty file stands for it or none does. The map never lists `labels: 0`.
- A property counts the values stored for it, as `record.properties` returns them, so a `default` is not counted (`RecordLike.properties` in `protocols.py`). A property that is declared and never written reads as a typed NULL column (`with_properties` in `schema/categories.py`), and is missing from the map. A missing property and a count of 0 mean the same, so the map never lists 0.
- The value is a count, not a flag, so a reader can tell full coverage from partial. `entity.sign` has a value for 93 of 132 entities.
- The entry describes the revision under the schema in the same manifest, and is never rewritten, the manifest being create-only. A read of the revision resolves under that schema ([DEP 26](dep-0026-the-fold-reads-each-layer-under-its-own-schema.md)), so `axes` and a later read agree by construction.

### Writing it

`Claim.commit` writes the layer's data with `write_layer` while the parent's record is open, then writes the manifest create-only, last. Before the parent's record closes, it resolves the new revision's axes: `Coords.resolve` over the parent's sources and the new layer, read under its own schema and files. That folds the layer's axis files onto the parent's axes, which the open parent record holds as tables in `_fold.<parent>` ([DEP 35](dep-0035-a-record-holds-the-tables-it-reads.md)), so the count starts from [the nearest fold at hand](../layers/resolution.md#a-fold-starts-from-the-nearest-fold-at-hand) and folds that one layer onto it. It runs one aggregate per axis, `count(*)` beside `count(<property>)` for each declared property, and puts the result in the manifest. `Workspace.write` does the same for a standalone record, one layer with no parent.

No attribute file is opened and no owner map is folded. Each axis is a file the layer just wrote, a table of the parent's fold, or the schema's list. `write_layer` already resolves the axes where the layer writes one, to check the layer's tuples and references against them (`refuse_unheld`, `_require_living_references`), so the count adds the axes that the layer does not touch. The manifest is still written last. A failed count therefore leaves the layer pending, as a failed layer write does, and the [cleanup](../storage/store.md#cleanup) reaches it.

### Reading it

`Revision.axes` returns the entry as `dict[str, Axis]`, where `Axis` is a frozen dataclass of `labels: int` and `properties: dict[str, int]`, beside `LayerFiles`. It reads the revision's manifest, as `Revision.meta` and `Revision.parent` do, and is a property, not a pydantic field, so a serialised `Revision` is still its location and id.

The workspace caches each manifest whole, so its `revisions` table gains an `axes JSON` column, which `Store._cache` writes and `lineage` and `Revision.manifest` read back. A manifest is create-only, so the cached row can still be incomplete but never stale.

### A manifest written before this DEP

`Manifest.axes` is `None` where the file has none, and `Revision.axes` raises for such a revision, naming it and asking for the store to be exported again. datarecord does not compute the entry on read. The manifest is create-only, so no later call can add the entry to it.

Everything else reads such a manifest as before. A release from before this DEP refuses a manifest with `axes`, since `Manifest` forbids a key it does not declare. The manifest gains no `version` key: the presence of `axes` is the signal.

### A property declared on a `WorkingRecord`

[DEP 28](https://github.com/energy-models/datarecord/blob/dep/declare-a-property/docs/design/proposals/dep-0028-a-working-record-declares-a-property.md) declares a property on a `WorkingRecord`. A property declared and set that way is listed in the child's `axes`. Whether a later full export keeps it is DEP 29's rule, and `axes` reports the outcome under whichever rule applies.

## Rationale

**At commit, not at read.** datarecord has the parent's fold open at commit, and resolves part of the axes already. A revision is committed once and read many times, by readers that cannot fold.

**In the manifest, beside the schema.** The axes belong to one revision, and its manifest is the one file that makes it exist. One GET then returns both what the revision declares and what it holds, and no committed revision lacks the entry. A file of its own would be a second write after the commit, which a reader could find missing.

**Stored values, counted.** One number per property answers both "does the revision have it" and "do all labels have it". Leaving out properties with no value, and dims with no label, keeps the entry independent of what the schema declares, which the same file states.

It costs:

- Every commit reads each axis of the new revision once more. An axis is small next to the attributes, the parent's axes are tables of its fold, and no attribute file is read.
- The entry for the build above is 868 bytes, compact. A manifest already holds the full schema: the root manifest of PyPSA's `ac_dc_meshed` example is about 42 kB. A schema with more dims or properties grows the entry in step.
- `Revision.axes` raises on every revision committed before this DEP, until the store is exported again.

## Usage

```python
rev = store.revision(rid)
rev.axes["period"].labels  # 3
rev.axes["snapshot"].properties  # {"period": 2190, "timestep": 2190}
"scenario" in rev.axes  # False: declared, no labels
```

A reader without datarecord reads the same entry over HTTP:

```python
import json
from urllib.request import urlopen

axes = json.load(urlopen(f"{base}/layers/{rid}/manifest.json"))["axes"]
snapshot = axes["snapshot"]
snapshot["properties"].get("agg_3h", 0) == snapshot["labels"]  # every snapshot mapped
```

For the build above, the last line is `False`: no revision of it has `snapshot.agg_3h`.

## Implementation notes

1. `Axis` beside `LayerFiles` in `store/manifest.py`, and `Manifest.axes: dict[str, Axis] | None = None`.
2. A function that counts the labels and the stored property values of each axis in a `Coords`, one aggregate per axis, leaving out an empty axis and a property with no value.
3. `Claim.commit` resolves the new revision's `Coords` before the parent's record closes, seeded by the parent's through `Coords.resolve`'s `split`, and `Workspace.write` resolves its one layer's; both pass the counts to the `Manifest`.
4. The `revisions` table gains `axes`, which `Store._cache`, `_MANIFESTS` and `_manifests` carry; `Revision.axes` raises where it is `None`.
5. Pages: [store](../storage/store.md#the-layers) for the field and the table, [reading](../use/reading.md) for `Revision.axes`, and the [open question](../open-questions.md#what-else-a-record-should-answer-about-its-labels-without-handing-over-a-frame) on labels narrows to categories.

Checks that hold once built:

- On every revision of the test trees, materialised or not, `rev.axes` equals the counts taken from `rev.record.dims` and `rev.record.properties`.
- A dim declared and never written is missing from `axes`, and so is one whose deepest axis file is empty, and a property declared and never set.
- `ws.write(rev.record, uri)` gives a manifest with the same `axes` as `rev`'s.
- `rev.axes` on a warm workspace opens no file, and on a cold one reads only the manifest.
- A manifest without `axes` still reads, and `rev.axes` on it raises.

## Rejected Ideas

- The reader walks the manifests and parquet files with pyarrow: a second codebase copies the fold rules and opens a file per dim. The copy goes stale when a rule changes, as DEP 29 would change one.
- The service that runs playbooks writes the entry beside the store: its file is not part of the commit. A revision committed any other way has none.
- The axes of every revision in one store-wide file: every commit would then race on one mutable file, which DEP 25 removed from the store.
- Reading a materialised revision's `resolved/dim_owners.parquet`: it exists only where someone materialised, it is still parquet, and it names each axis's owning layer, whose file still needs a scan for a property's coverage.
- Reading the [registered views](../storage/views.md) of [DEP 34](dep-0034-a-record-registers-as-views.md) over a DuckDB server: a server would hold every revision open, and a reader that can only GET from the bucket has none.
- Computing the entry on read where the file has none: the Python reader would answer where an HTTP reader cannot. One store would pass one check and fail the other.
- Property names without counts: coverage would need a second read.
- A dim or property listed with 0 where it is declared and has nothing: it repeats what the schema beside it declares, and a dim's empty file would list it where another writer's missing file would not.
- A `version` key in the manifest: it would repeat what the presence of `axes` already states.

## Open Issues

- Attributes: whether the entry lists the attributes with rows, and their [`flags`](../use/reading.md#flags). Every commit would then fold the owner map, which a commit does not do today.
- Categories: whether the entry lists the labels per category of a categorized dim, such as entities per `ctype`. The open question on labels asks the same.
