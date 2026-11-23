<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 40: a commit may keep its parent's properties

| DEP      | 40                                                                                     |
| -------- | -------------------------------------------------------------------------------------- |
| Title    | a commit may keep its parent's properties                                              |
| Author   | Bryn Pickering                                                                         |
| Status   | Draft (2026-11-19)                                                                     |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) |
| Created  | 2026-11-19                                                                             |
| Version  | Unreleased                                                                             |

## Abstract

`Store.commit` and `Claim.commit` take `keep_properties=True`, which keeps each property the parent declares and the source's schema omits. The child's manifest declares the kept properties, and the commit writes the parent's values into each axis file the layer writes, by label. Without the flag, a commit refuses such a source as it does today.

## Motivation

A modelling workflow widens a record with properties that a tool does not model. One step adds `snapshot.agg_3h`, the three-hour block of each snapshot, so that a later step can aggregate the time series. A later step then exports the full network again, such as `PyPSA.to_datarecord(n, con)` after clustering, and commits it as a child. The tool's schema has no `agg_3h`, so `compatible_with` reports the property as removed and the commit refuses the child:

```text
ValueError: this layer's schema is incompatible with its parent's: property 'agg_3h' of 'snapshot' removed (https://energy-models.github.io/datarecord/reference/schema/#compatibility)
```

Today a caller wraps the source. The wrapper widens its schema with the parent's properties and joins the parent's values back onto each axis by label. Every workflow that mixes a tool's export with its own properties writes that wrapper again.

## Specification

`Store.commit(source, *, parent, meta=None, keep_properties=False)` and `Claim.commit(source, *, meta=None, credentials=None, keep_properties=False)` take the flag. `Store.commit` passes it to `Claim.commit`.

With `keep_properties=True` and a parent:

1. The kept properties are those the parent's manifest schema declares on a dimension the source's schema declares, and the source's schema does not.
2. The child's manifest schema is the source's schema plus the kept properties. Where both declare a property, the source's declaration applies.
3. `compatible_with` against the parent runs on that schema, as for any commit. So a property the source declares differently from the parent is still refused, and so is a dimension or an attribute the source omits.
4. Take an axis the layer writes without the column of a kept property. The writer left-joins the parent's resolved values onto it by label, and writes them into the child's axis file in the layer's row order. A label new to the child reads NULL. A label the child removes has no row, so its value goes with it.
5. An axis the layer does not write has no file, and the fold inherits the parent's axis, its properties included.
6. The writer checks the carried columns as it checks any written column. A carried property that refers to a dimension is refused while its value names a label the layer removes, with the message for [removing labels](../../reference/layers.md#removing-labels).
7. The commit logs each kept property at `INFO`: `revision <id> keeps property <dim>.<name> of its parent`.

A root commit, `parent=None`, ignores the flag. A `WorkingRecord` with the flag set raises `TypeError`, whether committed whole or as its `StagedSource` edits. Its layer already keeps every property it does not edit.

Nothing changes on disk. The carried values are ordinary columns of the child's axis files, and the manifest is an ordinary manifest.

## Rationale

The flag is per commit, so the writer states that an omitted property means "unchanged". A tool that drops a property by mistake is still refused by default.

The values are carried at commit time rather than at read time. The carried values are in the written file, so a read does no extra join and a later layer folds them like any other column. The writer also checks them before the manifest exists. This follows [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md): a layer's axis file contains the whole axis at the new revision, properties included.

Only properties are kept. A property describes a label, so the parent's value stays right for a label the child keeps. An attribute or a dimension is data the tool restates. Keeping one that the tool omits would need a rule per shape, which this DEP does not argue for.

The cost is one join per written axis that lacks a kept property, at commit time. A commit without the flag runs as before.

## Usage

```python
clustered = PyPSA.to_datarecord(n, con)
child = store.commit(clustered, parent=widened, keep_properties=True)
child.record.properties["snapshot"]  # agg_3h carried by label, NULL on a new snapshot
```

A commit without the flag refuses `clustered` as before.

## Implementation notes

1. `_keeping_properties` widens the source's schema with the parent's properties on the dimensions the source declares.
2. `_kept` lists the kept properties for the `INFO` log.
3. `_Carrying` wraps the `LayerData` and left-joins each written axis with the parent's resolved properties, ordered by the layer's rows.
4. `Claim._commit` applies both when `keep_properties` is set and the claim has a parent, before `compatible_with` and `write_layer` run.

Take a child that removes bus `south` while it carries `entity.bus = south` for `wind`. The writer refuses it as in point 6, whether or not the child restates the `entity` axis.

## Rejected ideas

- Always keep the parent's properties, so an omission always means "unchanged". A tool that drops a property by mistake would then keep stale values silently. Opt-in makes the writer state the intent.
- Carry the values in the fold at read time. Every read would join one relation per carried column, and the writer could not check the carried values.
- Let each tool carry the values itself. The PyPSA tool's `to_datarecord` already takes a `base` and leaves out values that repeat it. It could also carry the properties of `base`. Every tool would then repeat the label join, and a hand-written source would get nothing.
- Leave it to a caller-side wrapper or helper. That is what users write today, once per workflow.

## Open issues

- Dimensions and attributes that the parent declares and the source omits are not kept.
- A carried value that a tool's edit made stale is not detectable. If clustering merges snapshots, the carried `agg_3h` is the parent's value for the label kept, whatever the merge meant.
