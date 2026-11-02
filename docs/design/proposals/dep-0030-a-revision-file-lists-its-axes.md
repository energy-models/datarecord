<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 30: a revision file lists its axes

| DEP      | 30                                                                                                                                     |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a revision file lists its axes                                                                                                         |
| Author   | Bryn Pickering                                                                                                                         |
| Status   | Draft (2026-11-02)                                                                                                                     |
| Requires | [DEP 9](dep-0009-store.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), [DEP 24](dep-0024-categories-are-enum-dimensions.md) |
| Created  | 2026-11-02                                                                                                                             |

## Abstract

When it commits a revision, datarecord writes the revision's resolved axes into the revision file. For each dim with an axis, the entry lists the number of labels and, per property, the number of labels with a stored value. A reader then learns which dims and properties a revision has from one small JSON file, with no DuckDB and no fold. `Revision.axes` returns the same entry in Python.

## Motivation

A service that runs work on a revision needs to check, before the work starts, that the revision has what the work needs. Manta, a web platform that runs modelling playbooks over a store, validates each playbook this way. A step names the dims it needs, such as `period`. It also names the properties it needs, such as a `snapshot.<name>` time aggregation that an earlier step wrote. Manta's backend has no DuckDB and cannot import datarecord. It can read small JSON files from the store's bucket.

No file in a store answers the question:

- A revision file is `{"id": ..., "parent": ...}` and nothing more (`store/store.py:371`).
- `manifest.json` is one schema for the whole store (`store/manifest.py:26-39`). It declares every dim a tool may write, whether or not a revision has labels for it. For a single-country PyPSA-Eur build it declares `scenario`, `line_type` and `transformer_type`, and no layer has an axis file for any of them.
- A revision's axis is the deepest `dims/<dim>.parquet` on its path from the root (`resolve/coords.py:30-53`). A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) takes its axis from the schema instead (`resolve/coords.py:379-381`). In a store holding that build and its solved results, each result layer writes `dims/sub_network.parquet` alone, so every other axis of a result revision comes from the root layer.

A reader without datarecord would walk the parent chain, probe a parquet file per dim per layer, apply these rules and count the property columns. The rules are datarecord's, and they change. [DEP 29](https://github.com/energy-models/datarecord/blob/dep/full-write-keeps-base-properties/docs/design/proposals/dep-0029-a-layer-keeps-what-it-omits.md) proposes that a layer which omits a property column keeps its parent's values, which changes which properties a revision has. A copy of the rules in another service goes stale without notice.

The [open question](../open-questions.md#what-else-a-record-should-answer-about-its-labels-without-handing-over-a-frame) on what a `Record` should answer about its labels asks for the same kind of metadata. This DEP settles the part of it that is per dim.

## Specification

### The `axes` entry

A revision file gains one key, `axes`. It maps each dim that has an axis at the revision to two entries:

| key          | value                                                                                                 |
| ------------ | ----------------------------------------------------------------------------------------------------- |
| `labels`     | the number of labels in the axis, or of tuples for a [multi-dim](../model/multi-dims.md)              |
| `properties` | each property of the dim with at least one stored value, mapped to the number of labels that have one |

The root revision of a single-country PyPSA-Eur build, with three investment periods, would be written as:

```json
{
  "id": "<root id>",
  "parent": null,
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

The revision has three periods, and every one of its 2,190 snapshots maps to a period. `scenario`, `line_type` and `transformer_type` are missing, so the revision has no labels for them although the schema declares them. A child revision holding only solved results resolves to the same axes.

The entry follows four rules:

- A dim is listed exactly when `revision.record.dims` has it (`resolve/record.py:350-352`, `resolve/coords.py:378-391`). That is every dim with `one_of`, and any other dim for which a layer on the path wrote an axis file. A dim whose deepest file is empty is listed with `labels: 0`.
- A property counts the values stored for it, as `record.properties` returns them, so a `default` is not counted (`protocols.py:161-168`). A property that is declared and never written reads as a typed NULL column (`schema/categories.py:113-126`), and is missing from the map. A missing property and a count of 0 mean the same, so the map never lists 0.
- The value is a count, not a flag, so a reader can tell full coverage from partial. `entity.sign` has a value for 93 of 132 entities.
- The entry describes the revision under the schema it was committed under, and is never rewritten.

### Writing it

`Store._write` writes the layer, then reconciles the manifest, then writes the revision file create-only (`store/store.py:355-374`). Between the second and the third step, datarecord resolves the new revision's axes under the reconciled schema. It folds the parents' sources and the new layer, as `resolve_coords` does. It runs one aggregate per axis, `count(*)` beside `count(<property>)` for each declared property, and writes the result with the id and the parent.

No attribute file is opened and no owner map is folded. Each axis is one file, a materialised copy, or the schema's list. `write_layer` already resolves the axes it checks a layer against (`store/write.py:161-166`), so the count adds the axes that the layer does not touch. The revision file is still written last. A failed count therefore leaves the layer unreachable, as a failed layer write does.

### Reading it

`Revision.axes` returns the entry as `dict[str, Axis]`, where `Axis` is a frozen dataclass of `labels: int` and `properties: dict[str, int]`. It is a property, not a pydantic field, so a serialised `Revision` is still its location and id (`store/store.py:539-556`).

`Store.revision` and `Store.revisions` read the entry with the id and the parent. The workspace's `revisions` table (`store/store.py:50-57`) gains an `axes JSON` column. A revision file is write-once, so the cached row can still be incomplete but never stale.

### A revision written before this DEP

`Revision.axes` raises for a revision file without `axes`, naming the revision and asking for the store to be exported again. datarecord does not compute the entry on read. The file is create-only, so no later call can add the entry to it.

Everything else reads as before. `_read_revision` reads only `id` and `parent` (`store/store.py:402-408`), so a datarecord release from before this DEP reads a new revision file too. The file gains no `version` key: the presence of `axes` is the signal.

### DEP 25 and DEP 26

[DEP 25](https://github.com/energy-models/datarecord/blob/dep-25-layer-manifests/docs/design/proposals/dep-0025-a-layer-carries-its-manifest.md) replaces `revisions/<id>.json` with `layers/<id>/manifest.json`, which lists the layer's id, parent, full schema and files. That manifest does not list which dims have labels at a revision. `files` lists the files of one layer, not of its ancestry, and the schema declares dims whether or not they have labels. Under DEP 25, `axes` becomes a field of `Manifest`, written by the commit and cached in the `revisions` table with the rest. One GET then returns both the schema and the axes of a revision. Whichever of the two DEPs lands second moves the entry, which keeps its shape.

[DEP 26](https://github.com/energy-models/datarecord/blob/dep-26-per-layer-fold/docs/design/proposals/dep-0026-the-fold-reads-each-layer-under-its-own-schema.md) reads each layer under its own schema, and a node's read resolves under the node's schema. With both DEPs, the revision's manifest fixes the schema at commit. `axes` and a later read of the revision then agree by construction.

[DEP 28](https://github.com/energy-models/datarecord/blob/dep/declare-a-property/docs/design/proposals/dep-0028-a-working-record-declares-a-property.md) declares a property on a `WorkingRecord`. A property declared and set that way is listed in the child's `axes`. Whether a later full export keeps it is DEP 29's rule, and `axes` reports the outcome under whichever rule applies.

## Rationale

**At commit, not at read.** datarecord has the parents' sources open at commit, and resolves part of the axes already. A revision is committed once and read many times, by readers that cannot fold.

**In the revision file, not the store manifest.** The axes belong to one revision. The store manifest is one mutable file behind a [compare-and-swap](../storage/concurrency.md#the-manifests-compare-and-swap). A map per revision in it would make every commit race on it, not only a commit that widens the schema.

**Stored values, counted.** One number per property answers both "does the revision have it" and "do all labels have it". Leaving out properties with no value keeps the answer independent of what the schema declares.

It costs:

- Every commit reads each axis of the new revision once more. An axis is small next to the attributes, and no attribute file is read.
- The root revision file of the build above grows from 62 bytes to 868 bytes, compact. A schema with more dims or properties grows it in step.
- `Revision.axes` raises on every store written before this DEP, until the store is exported again.

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

axes = json.load(urlopen(f"{base}/revisions/{rid}.json"))["axes"]
snapshot = axes["snapshot"]
snapshot["properties"].get("agg_3h", 0) == snapshot["labels"]  # every snapshot mapped
```

For the build above, the last line is `False`: no revision of it has `snapshot.agg_3h`.

## Implementation notes

1. A function that counts the labels and the stored property values of each axis in a `Coords`, one aggregate per axis.
2. `Store._write` resolves the new revision's axes after `reconcile_schema` and passes the counts to `_create`, which writes them beside `id` and `parent`.
3. `_read_revision`, the `revisions` table and `Revision` gain `axes`, and `Revision.axes` raises where the file has none.
4. Pages: [store](../storage/store.md#the-tree-beside-the-layers) for the file, [reading](../use/reading.md) for `Revision.axes`, and the [open question](../open-questions.md#what-else-a-record-should-answer-about-its-labels-without-handing-over-a-frame) on labels narrows to categories.

Checks that hold once built:

- On every revision of the test trees, materialised or not, `rev.axes` equals the counts taken from `rev.record.dims` and `rev.record.properties`.
- A dim declared and never written is missing from `axes`, and so is a property declared and never set.
- `rev.axes` on a warm workspace opens no file, and on a cold one reads only the revision file.
- A revision file without `axes` still reads, and `rev.axes` on it raises.

## Rejected Ideas

- The reader walks the parquet files with pyarrow: a second codebase copies the fold rules and probes a file per dim per layer. The copy goes stale when a rule changes, as DEP 29 would change one.
- The service that runs playbooks writes the entry beside the store: its file is not part of the commit. A revision committed any other way has none.
- The axes of every revision in the store manifest: every commit would then race on one mutable file, which DEP 25 removes.
- Reading `resolved/dims/` of a materialised revision: it exists only where someone materialised, it is still parquet, and coverage of a property needs a scan.
- Computing the entry on read where the file has none: the Python reader would answer where an HTTP reader cannot. One store would pass one check and fail the other.
- Property names without counts: coverage would need a second read.
- A property listed with 0 where it is declared and never set: the entry would depend on the schema of the moment. DEP 25 fixes the schema per revision, and `main` does not.
- A `version` key in the revision file: it would repeat what the presence of `axes` already states.

## Open Issues

- Attributes: whether the entry lists the attributes with rows, and their [`flags`](../use/reading.md#flags). Every commit would then fold the owner map, which a commit does not do today.
- A standalone record: whether `ws.write` puts `axes` into its `manifest.json`. Under DEP 25 it is a layer manifest and gets the entry with no further rule.
- Categories: whether the entry lists the labels per category of a categorized dim, such as entities per `ctype`. The open question on labels asks the same.
