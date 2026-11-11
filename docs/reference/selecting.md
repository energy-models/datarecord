<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Selecting

A **selection** is the `Selection` that `select(record, **labels)` returns: a record at the labels named.
`select` reads the labels as xarray's `sel` does.
One label squeezes its dim out of the selection, and a list, a slice or a partial date string keeps it.

## The call

```python
from datarecord import select

view = select(record, snapshot=1)
view.labels
```

```text
{'snapshot': 1}
```

`select` takes any [`RecordLike`](record.md), a framework's own object included, and `record.select(**labels)` calls it on a `Record`.
A `Selection` is itself a `RecordLike`, with three more members:

| Member   | Value                                                                                   |
| -------- | --------------------------------------------------------------------------------------- |
| `base`   | the record selected from, never a `Selection`                                           |
| `labels` | the keywords asked for, normalised: one label for a squeezed dim, else a list of labels |
| `schema` | the base's schema without the squeezed dims and what refers to them                     |

`labels` has what you asked for, and the labels that a selection drops through references show only in `view.dimensions`.
`view.select(**labels)` merges its keywords into `labels`, a later keyword replacing one of the same name, and selects `base` once.

## Keywords

A keyword is the name of one of three things:

- A dim, such as `snapshot=1` or `bus=["north"]`.
- A field of a multi-dim that is no dim, such as `from=` of a corridor, which narrows the multi-dim's tuples.
- A property by its plain name, which narrows its subject to the labels whose value, its default applied, is selected. A label whose value is NULL goes.

A field or property keyword never squeezes, so `labels` has its value as a list.
A tuple is selected through its fields, never by the multi-dim's own name.

## Values

| Value                                     | Selects                                             | The dim           |
| ----------------------------------------- | --------------------------------------------------- | ----------------- |
| one label                                 | that label, as pandas' `.loc` matches it            | squeezed          |
| a partial date string, on a datetime axis | every label in its span                             | kept, even at one |
| a list                                    | each label exactly, read as the dim's dtype         | kept, even at one |
| `slice(start, stop)`                      | the labels between, both ends included, `None` open | kept, even at one |

A date string is partial where it is coarser than the axis, so `"2013-02"` on a daily axis selects February and `"2013-02-01"` selects one label.
In a list, a date string is one instant.
A slice takes an axis of numbers or datetimes and compares its ends with the labels sorted, so `snapshot=slice(1, None)` gives `[1, 2]`.
The labels a selection keeps are in axis order.

`select` raises `ValueError` at the call, naming the keyword, for:

- a name that the schema declares as no dim, field or property, or as a multi-dim
- a property name that several subjects declare, or a field name that several multi-dims declare
- a label the axis lacks, or an empty list
- a slice with a step, of an axis of text, with an end the axis cannot compare, or covering no label
- a squeezed label that another keyword's selection drops
- squeezing every dim of a record that declares attributes, because attribute data over no dim is no record
- a squeeze that would change a default, as [Squeezing](#squeezing) describes

## Squeezing

**To squeeze** is to select one label of a dim, which drops the dim from the selection's schema.
A list of one keeps the dim.
Squeezing `snapshot` at `1` keeps the rows at `1` and the rows NULL on `snapshot`, settles them along `snapshot` alone, and drops the column:

<figure class="diagram">
--8<-- "docs/static/selecting-squeeze.svg"
</figure>

`wind` has the value it had at snapshot 1, and the NULL row stays NULL on `entity`.

Squeezing a dim `d` at a label `l` changes the schema and the rows:

- `d` leaves the schema with its axis and its properties, and so does every property that refers to `d`.
- A `default_by` keyed on a property that refers to `d` applies its entry for `l` as the default. A `default_by` keyed on a property of `d` applies the entry for the category that `l` has.
- A multi-dim that draws on `d` loses that field. One left with no field is squeezed too.
- An attribute over squeezed dims alone becomes one value over no dim.
- `partial` and every `Flags` lose `d`.

`record.select(ctype="Generator")` squeezes `ctype`, so `entity.ctype` leaves the schema and the default of `p_max_pu` is `1.0`, the `Generator` entry being absent.
Where a `default_by` lists `l` and a kept label is NULL on the property, `select` refuses the squeeze.
That label would take the default for `l` in the selection and not in the base.
A list of one, such as `ctype=["Load"]`, keeps the property and every default.

## What goes with a dropped label

A selection keeps every axis closed under reference, as removing a label does ([layers](layers.md)):

- A label whose property, its default applied, is a dropped label goes. A NULL is not a label and stays.
- A tuple that contains a dropped label goes, and its other labels stay.
- An attribute row that has a dropped label, in its column or in a field of a struct, goes. A row NULL there stays and applies to the kept labels.
- This repeats until nothing more goes.

<figure class="diagram">
--8<-- "docs/static/selecting-propagate.svg"
</figure>

`select(bus=["north"])` drops the bus `south` and the two connections that contain it.

Nothing flows the other way.
A dim keeps a label that no kept label refers to, such as a carrier that no entity has.
`record.select(ctype="Generator")` drops `dc` through `entity.ctype` and keeps the bus `south`.

## Laziness and layers

`select` reads the axes and properties it needs when you call it, and every attribute stays a plan on the base's connection.
A selection needs neither the data each revision adds (its [layers](layers.md)) nor `partial`.
`view.flags()` returns the base's flags without the squeezed dims, which overstate the selection's rows.

A selection writes nothing, and `WorkingRecord` refuses one.
Over a `WorkingRecord`, a narrowed axis keeps the labels it had at the call, and an attribute reads later edits at those labels.

## `lift`

**To lift** is to put the squeezed labels back on rows written over a selection, with `Selection.lift`.
`view.lift(attribute, rel)` adds a column at its label for each squeezed dim of the attribute, and puts a squeezed field back into its struct:

<figure class="diagram">
--8<-- "docs/static/selecting-lift.svg"
</figure>

The rows of `p_max_pu` over `select(record, snapshot=1)` come back with `snapshot` at `1`.

`lift` reads only `labels` and the base's schema.
It returns an attribute that the base declares over no squeezed dim unchanged, so `lift("p_nom", rel)` returns `rel`.
The fields come back in the base's field order.
A row NULL on the squeezed dim in the base was settled to the label, so it comes back at the label.
`lift` raises `KeyError` when the base declares no such attribute.

!!! info "See also"

    - [The record](record.md) describes the members a selection shares with every record.
    - [The data model](data-model.md) defines the broadcast rule that a squeeze applies.
    - [Select part of a record](../howto/select-a-view.md) shows the steps.
    - The [record API](api/record.md) lists `select` and `Selection`.
