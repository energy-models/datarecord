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

A commit under a parent widens the source's schema by the parent's, so a source may omit a property the parent declares. Each axis file the layer writes without a property's column takes the parent's values in that column, by label, whether or not the source declares the property. Where `compatible_with` used to refuse a source omitting a property, the commit now writes it.

## Motivation

A modelling workflow widens a record with properties that a tool does not model. One step adds `snapshot.agg_3h`, the three-hour block of each snapshot, so that a later step can aggregate the time series. A later step then exports the full network again, such as `PyPSA.to_datarecord(n, con)` after clustering, and commits it as a child. The tool's schema has no `agg_3h`, so `compatible_with` reports the property as removed and the commit refuses the child:

```text
ValueError: this layer's schema is incompatible with its parent's: property 'agg_3h' of 'snapshot' removed (https://energy-models.github.io/datarecord/reference/schema/#compatibility)
```

Today a caller wraps the source. The wrapper widens its schema with the parent's properties and joins the parent's values back onto each axis by label. Every workflow that mixes a tool's export with its own properties writes that wrapper again.

## Specification

`Store.commit` and `Claim.commit` take no new argument. With a parent:

1. The child's manifest schema is the parent's schema widened by the source's: the source's schema plus each property the parent declares and the source does not. Where both declare a property, the source's declaration applies.
2. `compatible_with` against the parent runs on that schema, as for any commit. So a property the source declares differently from the parent is still refused, and so is a dimension or an attribute the source omits.
3. Take an axis the layer writes without the column of a property the parent's axis has. The writer left-joins the parent's resolved values onto it by label, and writes them into the child's axis file in the layer's row order. A label new to the child reads NULL. A label the child removes has no row, so its value goes with it.
4. Point 3 holds whether or not the source's schema declares the property. To set a property to NULL, the source writes its column with NULLs.
5. An axis the layer does not write has no file, and the fold inherits the parent's axis, its properties included.
6. The writer checks the taken columns as it checks any written column. A property that refers to a dimension is refused while its value names a label the layer removes, with the message for [removing labels](../../reference/layers.md#removing-labels).

A root commit, `parent=None`, has nothing to take. A `WorkingRecord`, committed whole or as its `StagedSource` edits, writes every property column of each axis it restates, so the commit joins nothing for it.

Nothing changes on disk. The taken values are ordinary columns of the child's axis files, and the manifest is an ordinary manifest.

## Rationale

The planned design moves properties into one or more properties files of their own, which fold like attribute files. There, a layer that lacks a property's values reads them from the layer below, as a layer that writes no `p_nom` file reads the parent's `p_nom`. Taking the parent's values for every missing column gives that behaviour today, with the axis file as it is. A workflow written against it keeps working once properties fold like attributes, because the read returns the same rows.

A missing column therefore means "unchanged", as a missing attribute file already does. The rule turns on the column rather than the declaration, because that is what a properties file that folds like an attribute would see: a layer with no values for a property. A source that declares a property but writes no column for it states no values, so it takes the parent's. A NULL is a value, so a column of NULLs blanks the property. A rule that holds for attributes on every commit and for properties only behind a flag would give one omission two meanings.

The values are taken at commit time rather than at read time. The taken values are in the written file, so a read does no extra join and a later layer folds them like any other column. The writer also checks them before the manifest exists. This follows [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md): a layer's axis file contains the whole axis at the new revision, properties included.

Only properties are taken from the parent. A property describes a label, so the parent's value stays right for a label the child keeps. A source whose schema omits an attribute or a dimension is still refused, because keeping one would need a rule per shape, which this DEP does not argue for.

The cost is one join per written axis that lacks a column of the parent's axis, at commit time. A commit whose axis files have every column of the parent's runs as before.

## Usage

```python
clustered = PyPSA.to_datarecord(n, con)
child = store.commit(clustered, parent=widened)
child.record.properties["snapshot"]  # agg_3h taken by label, NULL on a new snapshot
```

## Implementation notes

1. `_with_parents_properties` widens the source's schema with the parent's properties. It skips a dimension the source does not declare, so that `compatible_with` reports the dimension as removed rather than schema validation failing first.
2. `_Carrying` wraps the `LayerData`. For each written axis, it left-joins the parent's resolved properties that the axis lacks, ordered by the layer's rows.
3. `Claim._commit` applies both whenever the claim has a parent, before `compatible_with` and `write_layer` run. A `WorkingRecord`'s edits have every property column, so the join adds nothing.

Take a child that removes bus `south` while it takes `entity.bus = south` for `wind` from its parent. The writer refuses it as in point 6, whether or not the child restates the `entity` axis.

## Rejected ideas

- Take the parent's values only with `keep_properties=True` on the commit, and refuse the source otherwise. The writer would state the intent, and a tool that drops a property by mistake would be refused. But an omitted property would then mean "removed" by default, while an omitted attribute means "unchanged", and the flag would become dead weight once properties fold like attributes.
- Take the parent's values only for a property the source's schema omits, and leave a declared property without a column NULL. A source would then blank a property by declaring it, and the outcome would depend on the schema rather than on the values the layer writes, which a properties file folding like an attribute does not see.
- Take the values in the fold at read time. Every read would join one relation per taken column, and the writer could not check the taken values.
- Let each tool carry the values itself. The PyPSA tool's `to_datarecord` already takes a `base` and leaves out values that repeat it. It could also carry the properties of `base`. Every tool would then repeat the label join, and a hand-written source would get nothing.
- Leave it to a caller-side wrapper or helper. That is what users write today, once per workflow.

## Open issues

- A source cannot remove a property its parent declares, because the commit widens its schema. Removing one was already refused by `compatible_with`, so no workflow loses a capability.
- A taken value that a tool's edit made stale is not detectable. If clustering merges snapshots, the taken `agg_3h` is the parent's value for the label kept, whatever the merge meant.
