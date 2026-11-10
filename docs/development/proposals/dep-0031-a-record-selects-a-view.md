<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 31: `select` returns a view of a record's labels

| DEP      | 31                                                                                                                                                                             |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Title    | `select` returns a view of a record's labels                                                                                                                                   |
| Author   | Jonas Hörsch                                                                                                                                                                   |
| Status   | Implemented (2026-11-04)                                                                                                                                                       |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), [DEP 23](dep-0023-multi-dims.md), [DEP 24](dep-0024-categories-are-enum-dimensions.md) |
| Created  | 2026-11-02                                                                                                                                                                     |
| Version  | Unreleased                                                                                                                                                                     |

The design landed in [selecting](../../reference/selecting.md), [select a view](../../howto/select-a-view.md) and [open questions](../../about/open-questions.md). Where they and this DEP differ, follow those pages.

## Abstract

`record.select(**labels)` returns a `Selection`: a `RecordLike` at the labels named, which it takes as xarray's `sel` does. One label squeezes its dim out of the schema, and a list, a slice or a partial date string keeps it. A label that refers to a dropped label goes too, as in a removal. `lift` puts the squeezed labels back on rows computed over the selection.

## Motivation

[PR 112](https://github.com/energy-models/datarecord/pull/112) gives the PyPSA tool a `select=` argument on `build` and `verify`. Most of what it does follows from the schema alone: matching labels, squeezing a dim and dropping what refers to a dropped label. A frontend, a plot and a second tool would each write it again.

## Specification

### The call

```python
def select(record: RecordLike, /, **labels: Any) -> Selection: ...


class Selection:  # a RecordLike: schema, dims, properties, attributes, flags
    base: RecordLike  # the record selected from, never a Selection
    labels: Mapping[str, Any | list[Any]]  # as asked, normalised

    def select(self, **labels: Any) -> Selection: ...
    def lift(self, attribute: str, rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
```

`select` takes any `RecordLike`, and `Record.select` calls it. The protocol does not grow.

`labels` contains the request, normalised. One label stays a label of the dim's dtype. A list stays a list, and a slice or a partial date string becomes the list of labels it matched. A dim's label is never a list, so `isinstance(value, list)` tells a squeeze from a kept dim. What propagation narrows shows only in the selection's axes.

`view.base.select(**view.labels)` is the same selection. `Selection.select` merges its keywords into `labels`, a later keyword replacing an earlier one of its dim, and selects `base` once.

### Keywords and values

A keyword is a dim's name, as in `set`. A keyword for a multi-dim's coordinate that is no dim, such as `from=`, narrows the tuples. Any other keyword is a property's name, which narrows its subject to the labels whose value, default applied, is selected. A NULL value is never selected. Coordinate and property keywords never squeeze, so their values are stored as lists. `select` refuses a property or coordinate name that two subjects declare, and a multi-dim's own name.

| Value                                     | Selects                                              | The dim           |
| ----------------------------------------- | ---------------------------------------------------- | ----------------- |
| one label                                 | the label, as pandas' `.loc` matches it              | squeezed          |
| a partial date string, on a datetime axis | every label in its span                              | kept, even at one |
| a list                                    | each label exactly, read as the dim's dtype          | kept, even at one |
| `slice(start, stop)`                      | the labels between, both ends inclusive, `None` open | kept, even at one |

A date string is partial where it is coarser than the axis, so `"2013-02"` on a daily axis selects February. In a list, a date string is one instant. A slice is pandas' label slice on the sorted labels of a number or datetime dim, and a partial date string at an end covers its span. Kept labels are in axis order, and every relation keeps the base's row order.

`select` refuses, naming the keyword:

- a name that is no dim, coordinate or property, a label the axis lacks, and an empty list
- a slice with a step, over a text axis, with an end the axis cannot compare, or covering no label
- a squeezed label that another keyword's selection drops
- squeezing every dim of a record that declares attributes

### Squeezing a dim

To squeeze `d` at the label `l`, `select` keeps `l` alone, so everything that refers to another label of `d` goes. Then:

- `d` leaves the schema, with its axis and its properties.
- A property that refers to `d` goes. A `default_by` keyed on it applies its entry for `l` as the default, and the keys before it still override.
- Where a `default_by` lists `l` and a kept label is NULL on the property, `select` refuses the squeeze, because the label's value would change. The caller writes `[l]`.
- A multi-dim with a coordinate on `d` loses that coordinate, and one left with no coordinate is squeezed with it.
- An attribute over `d` keeps its rows at `l` and those NULL on `d`, settled along `d` alone, without the column. One over squeezed dims alone is one value over no dim.
- `partial` and every `Flags` lose `d`.

Any dim squeezes, and a reader that needs one refuses a selection without it.

### What goes with a dropped label

A selection keeps every axis closed under reference, as the writer keeps a layer's:

- A label whose property, default applied, is a dropped label goes. A NULL stays.
- A tuple that contains a dropped label goes, and its other labels stay.
- This repeats until nothing goes, so dropping a `country` drops a bus through `bus.state` and `state.country`.
- An attribute row that contains a dropped label, in a column or a struct field, goes. A row NULL there stays.

Propagation never runs the other way: a dim keeps a label even where no kept label refers to it. A removal follows the same rule, except that a removal refuses where a selection drops the referring label.

### Laziness and layers

`select` reads only the axes and properties it needs, and raises its refusals at the call. Every attribute stays a plan: a semi-join against the kept labels and a `settle` along each squeezed dim. A selection needs neither the layers nor `partial`.

A selection writes nothing, and `WorkingRecord` refuses it. Over a `WorkingRecord`, `select` resolves the narrowed axes at the call, so a label added later is outside them. An axis nothing narrows reads as the base's, and an attribute reads the base's later edits at the kept labels.

### Lifting rows back to the base

`view.lift(attribute, rel)` adds the squeezed labels back to rows computed over a selection, from `labels` and `base.schema` alone. It adds a column per squeezed dim of the attribute, and puts a squeezed coordinate back into its struct. An attribute the base declares over no squeezed dim comes back unchanged. With `record.select(scenario="high")`, `price` rows over `bus, snapshot` come back with `scenario = high`.

`lift` inverts a squeeze one way only. A base row NULL on the squeezed dim comes back at the label, where a write-back wants it.

## Rationale

Selection belongs to the record because the schema sets its rules: what refers to what, which tuples contain a label and how a NULL broadcasts. A tool that restated them would duplicate rules that the format already sets.

One label squeezes its dim for every reader, as in xarray. If it squeezed for one reader and not another, one call would give two selections.

`labels` records the request, and the request alone rebuilds the selection, so datarecord derives propagation and never stores it.

Propagation never runs back from a dropped label. Dropping a carrier nobody has would drop labels nobody selected.

`lift` uses only the labels asked for and the base's schema, not what a value means. The reader decides which results a subset may write.

## Usage

```python
record.select(snapshot=slice("2013-02", "2013-03"))  # February and March
record.select(period=2030).select(carrier=["AC"]).labels
# {"period": 2030, "carrier": ["AC"]}
```

## Implementation notes

1. `datarecord.select` and `Selection`, exported beside `settle`, taken mostly from PR 112, with `Record.select` calling it. A tool adopts `Selection` in its own change.
2. Once built, `view.base.select(**view.labels)` equals `view` relation by relation. `view.lift` of every attribute is a subset of the base's rows at the selected labels.

## Rejected ideas

- `select=` on each tool's `build` and `verify`: each tool repeats the schema's rules.
- A narrowing hook passed to `select`: it couples the selection to a tool, where a further `select` does the same.
- A dim that one label does not squeeze, declared in the schema or by a reader: one call would give two selections.
- Recording propagated labels: two records of one selection could disagree.
- Dropping a label nobody refers to any more: it drops labels nobody selected.
- A selection as a child layer: it writes, and a squeeze restates nothing.
- Dropping a kept label NULL on a property instead of refusing the squeeze: one label and a list of it would keep different labels.

## Open issues

- Naming a label by a property's value, such as a timestamp for `snapshot=`.
- Whether a multi-dim left with one coordinate reads as that dim.
- Editing through a selection, staging at the base's coordinates through `lift`.
- A selection whose narrowed axes follow a `WorkingRecord`'s edits, and registering a selection as DuckDB views.
- Whether propagation applies a default that is not a live label ([open questions](../../about/open-questions.md)).
