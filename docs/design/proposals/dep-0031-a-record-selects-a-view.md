<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 31: a record selects a view of its labels

| DEP      | 31                                                                                                                                                                             |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Title    | a record selects a view of its labels                                                                                                                                          |
| Author   | Jonas Hörsch                                                                                                                                                                   |
| Status   | Draft (2026-11-03)                                                                                                                                                             |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md), [DEP 23](dep-0023-multi-dims.md), [DEP 24](dep-0024-categories-are-enum-dimensions.md) |
| Created  | 2026-11-02                                                                                                                                                                     |
| Version  | Unreleased                                                                                                                                                                     |

## Abstract

`record.select(**labels)` returns a `Selection`, a `RecordLike` holding the record at the labels named, as xarray's `sel` takes them. One label squeezes its dim out of the schema, and a list, a slice or a partial date string keeps it. Every label referring to a dropped one goes with it, through properties and multi-dim tuples, as a removal cascades. The selection keeps the labels it was asked for, so a consumer narrows it by selecting again, and `lift` puts the squeezed labels back on rows written over the unselected record.

## Motivation

[PR 112](https://github.com/energy-models/datarecord/pull/112) gives the PyPSA tool a `select=` argument on `build` and `verify`. Most of what it does is not PyPSA's: matching labels as pandas does, squeezing a dim by the [broadcast rule](../model/attributes.md#the-broadcast-rule), dropping a label whose property names a dropped label, dropping a tuple naming a dropped label. Those follow from the schema alone. A frontend showing one scenario, a plot of one period and a second tool need the same view, and would each write it again.

The [open question](../open-questions.md#filtering-a-record-before-build) on filtering a record before `build` has no design yet.

## Specification

### The call

```python
def select(record: RecordLike, /, **labels: Any) -> Selection: ...


class Selection:  # a RecordLike: schema, dims, properties, attributes, flags
    base: RecordLike  # the record selected from, never a Selection
    labels: Mapping[str, Any | list[Any]]  # as asked, normalized

    def select(self, **labels: Any) -> Selection: ...
    def lift(self, attribute: str, rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
```

`select` is a function of any `RecordLike`, exported beside [`settle`](../use/reading.md#settling-rows), and `Record.select` calls it. The protocol does not grow, so a framework's own `RecordLike` is selected without implementing anything.

`labels` holds what the caller asked for, normalized. One label stays a label, of the dim's dtype, and its dim is squeezed. A list stays a list, and a slice or a partial date string becomes the list of labels it matched; the dim is kept. `isinstance(value, list)` tells the two apart, since a dim's label is never a list. What propagation narrows is not in `labels`: it shows in the view's axes, `view.dims[d]`.

`view.base.select(**view.labels)` is the same view. `Selection.select` merges its keywords into `labels`, a later keyword replacing an earlier one of its dim, and selects `base` once.

### Keywords and values

A keyword names a dim, as a coordinate keyword of [`set`](../use/editing.md#set) does. One naming a multi-dim's coordinate that is no dim, such as `from=` of a corridor, narrows the multi-dim's tuples. One naming neither names a property by its bare name, as `set`'s property keyword does, and narrows its subject to the labels whose value, its default applied, is selected; a label whose value is NULL is not. These two never squeeze, so their values are stored as lists. A property name several subjects declare is refused, and so is a coordinate several multi-dims declare. A multi-dim's own name is refused, since a tuple is selected through its coordinates.

| value                                     | names                                                | the dim           |
| ----------------------------------------- | ---------------------------------------------------- | ----------------- |
| one label                                 | the label, as pandas' `.loc` matches it              | squeezed          |
| a partial date string, on a datetime axis | every label in its span                              | kept, even at one |
| a list                                    | each label exactly, read as the dim's dtype          | kept, even at one |
| `slice(start, stop)`                      | the labels between, both ends inclusive, `None` open | kept, even at one |

A date string is partial where it is coarser than the axis, as pandas reads it: `"2013-02"` on a daily axis names February, and `"2013-02-01"` names one label. In a list, a date string is one instant. A slice is pandas' label slice on the labels sorted, so a partial date string at an end covers its span. It takes a dim whose dtype is a number or a datetime. Labels kept are in axis order, and every relation keeps the base's row order.

`select` refuses, naming the keyword, a name the schema declares as no dim, coordinate or property, a label the axis lacks, an empty list, a slice with a step, a slice of an axis of text, an end the axis cannot compare and a slice covering no label. It refuses a squeezed label that another keyword's selection drops, since the dim would be squeezed at a label the view does not hold. It refuses squeezing every dim of a record that declares attributes, since attribute data over no axis is no record.

### Squeezing a dim

Squeezing `d` at the label `l` first keeps `l` alone, so everything referring to another label of `d` goes ([below](#what-goes-with-a-dropped-label)). Then:

- `d` leaves the schema, with its axis and its properties.
- A property referring to `d` goes, every value left being `l` or NULL. A `default_by` keyed on it applies its entry for `l` as the default, the keys before it still overriding.
- A kept label NULL on such a property matches no `default_by` key, so it would take the entry for `l` in the view but not in the base. Where a `default_by` lists `l` and a kept label is NULL there, the squeeze is refused, naming the keyword; the caller writes `l` as a list of one. The label is not dropped instead, since a NULL names no label and one label would keep a different set of labels than a list of it.
- A multi-dim drawing on `d` loses that coordinate, its tuples projected on the others. One left with no coordinate is squeezed with it.
- An attribute over `d` keeps its rows at `l` and those NULL on `d`, settled by the broadcast rule along `d` alone, without the column. One over squeezed dims alone is one value over no dim.
- `partial` and every `Flags` lose `d`.

Any dim squeezes. A consumer that needs one refuses a view in which it is squeezed, and the caller writes it as a list of one.

### What goes with a dropped label

A selection keeps every axis closed under reference, as the writer keeps a layer's ([removing labels](../layers/removal.md#what-a-removal-reaches)):

- A label whose property, its default applied, names a dropped label goes. A NULL names no label and stays.
- A tuple naming a dropped label goes, the labels it names staying.
- This repeats until nothing goes, so selecting `country` reaches a bus through `bus.state` and `state.country`.
- An attribute row naming a dropped label, in its column or in a field of a struct, goes. A row NULL there stays and reaches the kept labels alone.

Nothing flows the other way. A dim keeps a label no kept label refers to: a period no label refers to, a carrier no entity holds, an entity whose connections all went. The rule is a removal's, except that a property naming a dropped label drops its subject rather than blocking the removal.

### Laziness and layers

`select` reads the axes and properties it matches labels on and propagates through, which are small, and raises its refusals at the call. Every attribute stays a plan on the base's connection: a semi-join against the kept labels and a `settle` along each squeezed dim. A selection reads the folded record and needs neither its layers nor `partial`.

A selection is no layer and writes nothing. `WorkingRecord` refuses it as it refuses any other `RecordLike`, so an edit goes to the base. The axes it narrows are resolved at the call, so over a `WorkingRecord` a label added later is outside them. An axis nothing narrows reads as the base's, and an attribute reads the base's later edits at the labels kept.

### Lifting rows back to the base

A consumer that computes rows over a selection and writes them over `base` needs the squeezed labels back. `view.lift(attribute, rel)` adds them, from `labels` and `base.schema` alone. It adds the column of each squeezed dim the base declares the attribute over, at its label, and a squeezed coordinate back into its struct. An attribute declared without a squeezed dim is returned as it came; whether to write it is the consumer's decision.

With `view = record.select(scenario="high", period=2030)`:

| attribute                                             | base declares over            | rows over the view          | `view.lift`                               |
| ----------------------------------------------------- | ----------------------------- | --------------------------- | ----------------------------------------- |
| `price`                                               | `bus`, `snapshot`, `scenario` | `bus, snapshot, value`      | `bus, snapshot, scenario = high, value`   |
| `capacity`                                            | `entity`                      | `entity, value`             | unchanged                                 |
| `p`, `snapshot` a multi-dim over `(period, timestep)` | `entity`, `snapshot`          | `snapshot = {timestep: t0}` | `snapshot = {period: 2030, timestep: t0}` |

`lift` inverts a squeeze but one way: a row the base held NULL on the squeezed dim was settled to the label, and comes back at the label. That is what a write-back wants, a value at the label it was computed at.

## Rationale

Selection belongs to the record because its rules are the schema's: what refers to what, which tuples name a label, how a NULL broadcasts. They are the rules a removal follows, and a tool restating them restates the format.

One label squeezes, as in xarray, whatever the consumer. Whether a dim may go is the consumer's to judge, and a consumer that cannot lose one says so as it says a dim is missing. A rule that squeezed for one reader and not another would make one call two views.

`labels` records the request and the axes record the result. The request alone rebuilds the view, so it is what a consumer reads, merges and passes on, and propagation stays derived.

Propagation runs from a selected label to what refers to it and never back. Dropping a carrier nobody holds would drop labels nobody selected, and a consumer that needs that says so in one more `select`.

`lift` is mechanical. It knows the labels asked for and the base's schema, not what a value means, so a consumer's rule on which results a subset may write stays the consumer's.

## Usage

```python
record.select(period=2030)  # period squeezed; properties referring to it gone
record.select(period=[2030, 2040])  # kept at two labels
record.select(period=slice(2030, None))  # 2030 onwards, kept
record.select(snapshot=slice("2013-02", "2013-03"))  # February and March
record.select(scenario="high")  # squeezed
record.select(bus=["north", "south"])  # connections to other buses go
record.select(carrier=["AC", "wind"])  # AC buses, wind entities, NULL carriers stay
record.select(entity=["wind1"])  # its connections stay, every bus too
record.select(period=2030).select(carrier=["AC"]).labels
# {"period": 2030, "carrier": ["AC"]}

record.select(bus="Atlantis")  # refused: a label `bus`'s axis lacks
record.select(carrier=[])  # refused: no label
record.select(bus=slice("A", "Z"))  # refused: an axis of text
```

## Implementation notes

1. `datarecord.select` and `Selection` in core: label matching, squeezing, propagation and `lift`, exported; `Record.select` calls it.
2. Pages: a design page on selecting beside [reading](../use/reading.md), the usage pages, and the open question moved to settled.

PR 112's `_Selection`, `_select`, `_labels`, `_keyed_axis`, `_is_scalar`, `_axis_labels`, `_rebuilt` and the propagation through properties and tuples in `_propagated` carry most of the core code. How a tool adopts `Selection` is that tool's change.

Checks that hold once built:

- `view.base.select(**view.labels)` equals `view`, relation by relation.
- `view.lift` of every attribute read from `view` is a subset of the base's rows at the selected labels.

## Rejected Ideas

- `select=` on each tool's `build` and `verify`. Each tool repeats the schema's rules.
- A narrowing hook passed to `select`. It couples the view to a tool, where a further `select` does the same.
- A dim a scalar does not squeeze, declared in the schema or asked for by a consumer. One call would give two views.
- Recording propagated labels in the selection. They follow from the request, and two records of one view could disagree.
- Dropping a label nobody refers to any more. It drops labels nobody selected.
- A selection as a child layer restating the axes without the dropped labels. It writes, and a squeeze is no restatement.

## Open Issues

- Naming a label by a property's value as the dim's own keyword, such as `snapshot=` taking a timestamp over position labels.
- Whether a multi-dim left with one coordinate reads as that dim.
- Editing through a selection: a `WorkingRecord` over a `Selection`, staging at the base's coordinates through `lift`.
- A selection whose narrowed axes follow a `WorkingRecord`'s edits, and registering a selection as DuckDB views.
- Whether propagation applies a default naming no live label ([open questions](../open-questions.md#whether-a-default-must-name-a-live-label)).
