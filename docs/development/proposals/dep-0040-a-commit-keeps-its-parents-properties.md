<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 40: a commit keeps its parent's properties

| DEP      | 40                                                                                     |
| -------- | -------------------------------------------------------------------------------------- |
| Title    | a commit keeps its parent's properties                                                 |
| Author   | Bryn Pickering                                                                         |
| Status   | Draft (2026-11-19)                                                                     |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) |
| Created  | 2026-11-19                                                                             |

## Abstract

A commit under a parent fills each property column that an axis file the layer writes lacks with the parent's values, by label. The source's schema still declares every property of the parent, as `compatible_with` requires. The PyPSA tool's full export over `base` declares the properties of `base` the network lacks, so a commit of it keeps them.

## Motivation

A modelling workflow widens a record with properties that a tool does not model. One step adds `snapshot.agg_3h`, the three-hour block of each snapshot, so that a later step can aggregate the time series. A later step then exports the full network again, such as `PyPSA.to_datarecord(n, con)` after clustering, and commits it as a child. The tool's schema has no `agg_3h`, so `compatible_with` reports the property as removed and the commit refuses the child:

```text
ValueError: this layer's schema is incompatible with its parent's: property 'agg_3h' of 'snapshot' removed (https://energy-models.github.io/datarecord/reference/schema/#compatibility)
```

Today a caller wraps the source. The wrapper widens its schema with the parent's properties and joins the parent's values back onto each axis by label. Every workflow that mixes a tool's export with its own properties writes that wrapper again.

## Specification

The store: `Store.commit` and `Claim.commit` take no new argument. With a parent:

1. The child's manifest schema is the source's schema. `compatible_with` against the parent runs on it as for any commit, so a source that omits or redeclares a property of the parent is refused.
2. Take an axis the layer writes without the column of a property the source's schema declares and the parent's axis has. The writer left-joins the parent's resolved values onto it by label, and writes them into the child's axis file in the layer's row order. A label new to the child reads NULL. A label the child removes has no row, so its value goes with it.
3. To set a property to NULL, the source writes its column with NULLs.
4. An axis the layer does not write has no file, and the fold inherits the parent's axis, its properties included.
5. The writer checks the taken columns as it checks any written column. A property that refers to a dimension is refused while its value names a label the layer removes, with the message for [removing labels](../../reference/layers.md#removing-labels).

A root commit, `parent=None`, has nothing to take. A `WorkingRecord`, committed whole or as its `StagedSource` edits, writes every property column of each axis it restates, so the commit joins nothing for it.

The PyPSA tool: the schema of `PyPSA.to_datarecord(n, con, base=record)` declares each property that `base` declares on a dimension of the export and the export does not, with `base`'s spec. The network has no column for it, so the commit fills it by point 2. A property the export declares keeps the export's spec. A results layer, `results_only=True`, already takes `base`'s schema.

Nothing changes on disk. The taken values are ordinary columns of the child's axis files, and the manifest is an ordinary manifest.

## Rationale

The planned design moves properties into one or more properties files of their own, which fold like attribute files. There, a layer that lacks a property's values reads them from the layer below, as a layer that writes no `p_nom` file reads the parent's `p_nom`. Filling every missing column from the parent gives that behaviour today, with the axis file as it is. A workflow written against it keeps working once properties fold like attributes, because the read returns the same rows.

A missing column therefore means "unchanged", as a missing attribute file already does. A NULL is a value, so a column of NULLs blanks the property.

The store stays strict on declarations. A schema is a statement of what the layer contains, and the store does not guess one: a source that omits a property of its parent is refused, as before. The tool that knows its `base` widens its own schema, because it is the one that read `base` and chose what to write over it. A hand-written source declares the properties it means to keep, and the store fills their columns.

The values are taken at commit time rather than at read time. The taken values are in the written file, so a read does no extra join and a later layer folds them like any other column. The writer also checks them before the manifest exists. This follows [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md): a layer's axis file contains the whole axis at the new revision, properties included.

The cost is one join per written axis that lacks a declared column of the parent's axis, at commit time. A commit whose axis files have every declared column runs as before.

## Usage

```python
clustered = PyPSA.to_datarecord(n, con, base=widened.record)
child = store.commit(clustered, parent=widened)
child.record.properties["snapshot"]  # agg_3h taken by label, NULL on a new snapshot
```

## Implementation notes

1. `_Carrying` wraps the `LayerData`. For each written axis, it left-joins the parent's resolved properties that the schema declares and the axis lacks, ordered by the layer's rows.
2. `Claim._commit` applies it whenever the claim has a parent, after `compatible_with` and before `write_layer`. A `WorkingRecord`'s edits have every property column, so the join adds nothing.
3. `_NetworkSource.schema` widens the export's properties by those of `base` last, after the network's shapes and custom columns, so the export's own declarations apply.

Take a child that removes bus `south` while it takes `entity.bus = south` for `wind` from its parent. The writer refuses it as in point 5, whether or not the child restates the `entity` axis.

## Rejected ideas

- Widen the source's schema by the parent's in the store, so a source may omit a property its parent declares. The store would then guess a schema the source did not state, and a source could never be refused for dropping a property by mistake. The tool that wrote over `base` knows what it read, so it widens.
- Keep the parent's properties only with `keep_properties=True` on the commit. An omitted column would then mean "NULL" by default, while an omitted attribute file means "unchanged", and the flag would become dead weight once properties fold like attributes.
- Take the values in the fold at read time. Every read would join one relation per taken column, and the writer could not check the taken values.
- Let the PyPSA tool also join the values of `base` itself. Every tool would then repeat the label join that the store does once, and a hand-written source would get nothing.
- Leave it to a caller-side wrapper or helper. That is what users write today, once per workflow.

## Open issues

- A taken value that a tool's edit made stale is not detectable. If clustering merges snapshots, the taken `agg_3h` is the parent's value for the label kept, whatever the merge meant.
