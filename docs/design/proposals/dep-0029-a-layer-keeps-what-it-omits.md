<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 29: a layer keeps what it omits

| DEP      | 29                                                                                                         |
| -------- | ---------------------------------------------------------------------------------------------------------- |
| Title    | a layer keeps what it omits                                                                                |
| Author   | Bryn Pickering                                                                                             |
| Status   | Draft (2026-11-02)                                                                                         |
| Requires | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md)                                                       |
| Created  | 2026-11-02                                                                                                 |
| Version  | Unreleased                                                                                                 |
| Replaces | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (an axis file restating every property of its labels) |

## Abstract

A child layer keeps every name its parent declares and it does not, with the parent's declaration. Where an axis file lacks the column of a declared property, the child keeps the parent's values of that property, by label. A layer still restates the labels and order of each axis whole. A downstream step can add a property that a tool does not model. The tool's full export can then be written as a child, and the property survives it.

## Motivation

A downstream step can widen a record's schema with a property that PyPSA does not model. An example is `snapshot.agg_3h`, the three-hour block that each snapshot falls in. A later step builds the network, edits it in PyPSA, for example by clustering, and writes it back whole:

```python
agg = rev.child(SnapshotAxis(widened_schema, snapshots_with_agg_3h))
n = PyPSA.build(agg.record)
cluster(n)
agg.child(PyPSA.to_datarecord(n, ws.con))
```

```text
ValueError: this layer's schema is incompatible with the record's:
property 'agg_3h' of 'snapshot' removed (.../design/model/schema/#versioning)
```

`to_datarecord` returns a source under the PyPSA tool's own schema, which does not declare `agg_3h`. `Store._write` (`store/store.py:355`) passes that schema to `reconcile_schema` (`store/manifest.py:42`). `reconcile_schema` refuses it at `:79-85`, because `Schema.compatible_with` (`schema/schema.py:1089-1152`) reports as removed every name the record declares and the layer does not, here at `:1137`. A dim or an attribute that the tool does not model fails the same way, as `dim ... removed` or `attribute ... removed`. A results write passes, since `_ResultsSource.schema` returns `base.schema` (`tools/pypsa.py:2736-2739`).

Widening the schema of the source is not enough. Take a child whose schema declares `agg_3h` but whose `dims/snapshot.parquet` lacks the column. Since the deepest axis file is the whole axis ([restating an axis](../layers/removal.md#restating-an-axis)), the child reads `agg_3h` as NULL on every snapshot. Both outcomes were reproduced on `main` with a three-component network.

So every caller has to keep the property itself. manta's `write_network` wraps the source in a `RecordLike` that widens its schema by the extra `snapshot` properties of the base. The wrapper then left-joins their values back onto `properties["snapshot"]` by label. Every full export of every tool, and every downstream property, needs the same wrapper.

## Specification

### The schema of a child layer

datarecord writes a child under its parent's schema widened by the source's. That schema declares every name that either declares, with the source's declaration where both do. `compatible_with` still holds between the result and the parent, so datarecord refuses a name that both declare differently, as today. A name that only the parent declares is kept as the parent declares it.

| change                                          | today        | under this DEP |
| ----------------------------------------------- | ------------ | -------------- |
| the source declares a name the parent lacks     | widens       | widens         |
| the source omits a name the parent declares     | refused      | kept           |
| the source declares a name otherwise (dtype...) | refused      | refused        |
| a root, or `ws.write` of a standalone directory | the source's | the source's   |

Removing a declaration stays an incompatible change ([versioning](../model/schema.md#versioning)). As today, it needs the record flattened under the new schema. No layer removes a name by omitting it.

### Property columns of an axis file

A layer that writes `dims/<dim>.parquet` restates the labels of the axis and their order whole, as in DEP 20. The layer also restates whole, for the axis, each property column that the file contains. Where the file lacks the column of a declared property, the fold carries the property from the parent's resolved axis, as a carried property:

- A label on the parent's axis keeps its value.
- A label missing from the parent's axis, because it is new or re-added after a removal, reads NULL.
- A removed label's value goes with the label.

Per property, a resolved axis is thus the deepest file that contains the column, joined by label onto the deepest layer's labels. To clear a property, a layer writes its column as NULL. The file `resolved/dims/<dim>.parquet` of a materialised node contains every column, so the fold stops carrying there.

### What the writer checks

A carried property is part of the layer's final axis, and the [writer checks it](../layers/removal.md#what-the-writer-checks) as it checks a written one. Where a carried property refers to a dim, the writer refuses it if one of its values is a label the layer removes. Take a clustered network that drops snapshot `01:00`, over a parent whose `agg_3h` has `01:00` as a value. The write fails with the error that a written column gives today:

```text
snapshot ['2030-01-01 02:00:00'] name a removed snapshot in 'agg_3h'; removing
a label removes the tuples naming it and leaves no property holding it, so
restate those multi-dims without the tuples and clear those properties (...)
```

The writer must then state `agg_3h` itself, since only the writer knows what the clustering makes of the blocks.

### Subjects the rule covers

The store applies the rule, so it holds for every dim, including `bus`, `carrier`, `entity` and the multi-dim `connection`. It also holds for every tool and for every hand-written layer. Attributes need no new rule: once the schema keeps them, the fold resolves them through the owner map.

### Per-layer manifests

[DEP 25](https://github.com/energy-models/datarecord/pull/119) moves the schema into each layer's manifest and checks a child's schema against its parent's. [DEP 26](https://github.com/energy-models/datarecord/pull/120) folds each layer under its own schema. Neither changes the problem: the schema of a full PyPSA export still lacks `agg_3h`, and the commit in DEP 25 refuses it against the parent's.

With DEP 25 and DEP 26 in place, `store.commit` writes the widening above into the child's manifest. That manifest stays the full schema that DEP 25 asks for. A child's schema never loses a name, so DEP 26 can still rely on schemas only growing down an ancestry. With today's single manifest, `reconcile_schema` writes the widening.

[DEP 27](https://github.com/energy-models/datarecord/blob/dep/pypsa-results-entry-point/docs/design/proposals/dep-0027-results-have-their-own-entry-point.md) makes `to_datarecord(model, con)` the full export, with no `base`. This DEP keeps it that way, because a full export needs no `base` to be written as a child.

## Rationale

### What an omission means

An omission already means unchanged everywhere else in the fold. A layer that omits an axis leaves its parent's axis in place. A layer that omits an attribute, or a key of an attribute, keeps its parent's rows under the [owner map](../layers/resolution.md#owner-map), less those of removed labels. Only the schema and the property columns of a restated axis read an omission as a removal. This DEP makes them follow the rest, which removes the one exception. A tool then no longer has to know that it must restate what it never modelled.

### Labels and order

Membership stays whole. DEP 20's case holds for labels and order, because only a whole file states them. A property is one value per label, which [DEP 22](dep-0022-a-layer-restates-membership-whole.md) already argues needs no whole file. This DEP applies that argument per column, so it needs no file per property and no mask to tell NULL from unset. The file's own columns show whether a column is missing or written as NULL.

### Where the label join lives

The store, not the tool, keeps the omitted values. The fold already does the same for attributes. It joins a parent's values by label, drops them with a removed label and starts a new label empty. Done by each tool, the join is written once per tool and once per caller who forgets it. It would also return a `base` to the full export, which DEP 27 removes.

### The refusals that stay

Each refusal that stays is one a writer can act on. The refusal this DEP drops prevents nothing a writer could mean, since no layer can remove a name. Two refusals stay: a name declared differently, and a carried value that is a removed label.

### What it costs

- A hand-written axis file that contains labels alone no longer clears its properties. To clear them, it writes their columns as NULL.
- A tool that drops a property by mistake no longer fails. The property keeps its old values, which are stale where the tool changed what they describe.
- An axis read joins one relation per carried column, where today it reads one file. A full export over a record with few downstream properties carries few.
- A layer alone no longer states which properties it set, because a carried column comes from the fold.

## Usage

Before this DEP, a full write over a widened record needs the caller's wrapper:

```python
n = PyPSA.build(agg.record)
cluster(n)
source = PyPSA.to_datarecord(n, ws.con)
extra = {
    k: v
    for k, v in agg.record.schema.properties["snapshot"].items()
    if k not in source.schema.properties["snapshot"]
}
kept = agg.record.properties["snapshot"].df()[["snapshot", *extra]]
frame = source.properties["snapshot"].df().merge(kept, on="snapshot", how="left")
agg.child(CarriedSnapshotProperties(source, widened(source.schema, extra), frame))
```

With this DEP, the export is written as it is:

```python
child = agg.child(PyPSA.to_datarecord(n, ws.con))
child.record.schema.properties["snapshot"]  # period, timestep, agg_3h
child.record.properties["snapshot"]  # agg_3h kept on the snapshots n holds
```

The child's schema declares `agg_3h` beside `period` and `timestep`, and each snapshot that `n` has keeps its `agg_3h` value.

A label that the export adds reads NULL, and a label that it drops takes its value with it:

```python
# agg: snapshots s0..s5, agg_3h = s0, s0, s0, s3, s3, s3
# n keeps s0 and s3 and adds s6
child.record.properties["snapshot"]  # s0 -> s0, s3 -> s3, s6 -> NULL
```

The kept snapshots `s0` and `s3` keep their blocks, the new `s6` has none, and the dropped snapshots are gone with their values.

To clear a property, write its column:

```python
w = WorkingRecord(child.record)
w.set("snapshot.agg_3h", None)
child.child(w.edits)
```

The new child's axis file contains `agg_3h` as NULL, so `agg_3h` reads NULL on every snapshot.

## Implementation notes

Paths are under `src/datarecord/`.

1. `schema/schema.py`: `Schema.over(parent)`, `parent` with every name `self` declares, `self`'s declaration winning. The store checks that the result is `compatible_with` the parent, which then reports no removal.
2. `store/store.py`: `_write` widens `data.schema` over the parent's before `write_layer`, so the layer is validated and checked under the schema it is read under. `reconcile_schema` writes the widened schema.
3. `resolve/coords.py`: an axis's fold carries each declared property column that its deepest file lacks from the parent's resolved axis, joined by label.
4. `store/write.py`: `_require_living_references` reads the carried columns of a restated subject, as it reads written ones.
5. `mutable.py`: optionally, an axis edit may stage only the columns it changes, where it now carries their siblings.
6. Pages: [restating an axis](../layers/removal.md#restating-an-axis), [one fold for every axis](../layers/resolution.md#one-fold-for-every-axis), [versioning](../model/schema.md#versioning), [a layer from elsewhere](../layers/removal.md#a-layer-from-elsewhere), [writing](../use/writing.md), `CHANGELOG.md`.

Checks that hold once built:

- A full PyPSA export over a record with `snapshot.agg_3h` writes as a child, and keeps `agg_3h` by label, NULL on added snapshots.
- A child whose axis file lacks a property column reads the parent's values, whether the parent is materialised or not.
- The writer refuses a carried value that is a removed label.
- The store refuses a source that declares a kept name differently, as today.
- manta's `write_network` no longer needs its wrapper.

## Rejected Ideas

- The tool keeps them: `to_datarecord(n, con, base=record)` keeps the properties and attributes of `base` that it does not model. Every tool then repeats the fold's label join, and a caller who writes a source by hand gets nothing. A full export also takes `base` again, which DEP 27 removes. [PR #112](https://github.com/energy-models/datarecord/pull/112) takes `base` on a full export for multi-period checks, not for this.
- The status quo with a helper, `carry(source, base)` in core, writes manta's wrapper once. Every full write still has to call it, and a widened schema over an omitted column still reads as NULL with no error.
- An explicit removal operation for a declared name has nothing to make explicit, because a layer never removes a name. A layer clears a value by writing it as NULL.
- A layer's manifest could contain the source's narrower schema, read under DEP 26 as "not known here". A schema would then be folded down the ancestry as a delta, which DEP 25 rules out.

## Open Issues

- PyPSA's `build` puts a `bus`, `carrier` or `entity` property that it does not model onto the component frames. `to_datarecord` writes it back as a [custom column](../tools/pypsa.md#custom-columns), an attribute over the dim of the type. Under this DEP the property is kept and the attribute is added beside it. The tool should leave out a column that `build` took from a property.
- Whether a carried value that the tool's edit made stale should be detectable, such as a block of snapshots that a clustering merged. Detecting it needs to know which layer wrote a column, which the fold has but does not expose.
- DEP 28, being drafted, gives `WorkingRecord` a way to declare the property that this DEP keeps. The two DEPs are independent.
