<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Selecting

`select(record, **labels)` returns a `Selection`, a `RecordLike` holding the record at the labels named, as xarray's `sel` takes them.
One label squeezes its dim out of the schema, and a list, a slice or a partial date string keeps it.
Every label referring to a dropped one goes with it, through properties and multi-dim tuples, as a [removal](../layers/removal.md#what-a-removal-reaches) cascades.
The selection keeps the labels it was asked for, so a consumer narrows it by selecting again, and `lift` puts the squeezed labels back on rows written over the unselected record.

Selection belongs to the record because its rules are the schema's: what refers to what, which tuples name a label, how a NULL broadcasts.
A frontend showing one scenario, a plot of one period and a tool building one network need the same view, and a tool restating those rules would restate the format.

## The call

```python
def select(record: RecordLike, /, **labels: Any) -> Selection: ...


class Selection:  # a RecordLike: schema, con, dims, properties, attributes, flags
    base: RecordLike  # the record selected from, never a Selection
    labels: Mapping[str, Any]  # as asked, normalized

    def select(self, **labels: Any) -> Selection: ...
    def lift(self, attribute: str, rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
```

`select` is a function of any `RecordLike`, exported beside [`settle`](reading.md#settling-rows), and `Record.select` calls it.
The [protocol](reading.md) does not grow, so a framework's own `RecordLike` is selected without implementing anything.

`labels` holds what the caller asked for, normalized.
One label stays a label, of the dim's dtype, and its dim is squeezed.
A list stays a list, and a slice or a partial date string becomes the list of labels it matched; the dim is kept.
`isinstance(value, list)` tells the two apart, since a dim's label is never a list.
What propagation narrows is not in `labels`: it shows in the view's axes, `view.dims[d]`.
The request alone rebuilds the view, so it is what a consumer reads, merges and passes on, and propagation stays derived.

`view.base.select(**view.labels)` is the same view.
`Selection.select` merges its keywords into `labels`, a later keyword replacing an earlier one of the same name, and selects `base` once.

## Keywords and values

A keyword names a dim, as a coordinate keyword of [`set`](editing.md#set) does.
One naming a multi-dim's coordinate that is no dim, such as `from=` of a corridor, narrows the multi-dim's tuples.
One naming neither names a property by its bare name, as `set`'s property keyword does, and narrows its subject to the labels whose value, its default applied, is selected; a label whose value is NULL is not.
These two never squeeze, so their values are stored as lists.
A property name several subjects declare is refused, and so is a coordinate several multi-dims declare.
A multi-dim's own name is refused, since a tuple is selected through its coordinates.

| value                                     | names                                                | the dim           |
| ----------------------------------------- | ---------------------------------------------------- | ----------------- |
| one label                                 | the label, as pandas' `.loc` matches it              | squeezed          |
| a partial date string, on a datetime axis | every label in its span                              | kept, even at one |
| a list                                    | each label exactly, read as the dim's dtype          | kept, even at one |
| `slice(start, stop)`                      | the labels between, both ends inclusive, `None` open | kept, even at one |

A date string is partial where it is coarser than the axis, as pandas reads it: `"2013-02"` on a daily axis names February, and `"2013-02-01"` names one label.
In a list, a date string is one instant.
A slice is pandas' label slice on the labels sorted, so a partial date string at an end covers its span.
It takes a dim whose dtype is a number or a datetime.
A property keyword matches against the axis of the dim the property refers to, else against the values its labels hold.
Labels kept are in axis order, and `dims` and `properties` keep the base's row order.

`select` refuses, naming the keyword, a name the schema declares as no dim, coordinate or property, a label the axis lacks, an empty list, a slice with a step, a slice of an axis of text, an end the axis cannot compare and a slice covering no label.
It refuses a squeezed label that another keyword's selection drops, since the dim would be squeezed at a label the view does not hold.
It refuses squeezing every dim of a record that declares attributes, since attribute data over no axis is [no record](../model/dims.md), naming the keywords that squeeze.

## Squeezing a dim

Squeezing `d` at the label `l` first keeps `l` alone, so everything referring to another label of `d` goes ([below](#what-goes-with-a-dropped-label)).
Then:

- `d` leaves the schema, with its axis and its properties.
- A property referring to `d` goes, every value left being `l` or NULL. A [`default_by`](../model/properties.md#default_by-a-default-per-category) keyed on it applies its entry for `l` as the default, the keys before it still overriding. A `default_by` keyed on a property of `d` itself applies the entry for the category `l` holds.
- A kept label NULL on such a property has no category there and [matches no key](../model/properties.md#default_by-a-default-per-category), so it would take the entry for `l` in the view but not in the base. Where a `default_by` lists `l` and a kept label is NULL there, the squeeze is refused, naming the keyword; the caller writes `l` as a list of one, which keeps the property and the label's default. The label is not dropped instead, since a NULL [names no label](#what-goes-with-a-dropped-label) and one label would keep a different set of labels than a list of it.
- A [multi-dim](../model/multi-dims.md) drawing on `d` loses that coordinate, its tuples projected on the others. One left with no coordinate is squeezed with it, at the tuple of the squeezed labels.
- An attribute over `d` keeps its rows at `l` and those NULL on `d`, settled by [the broadcast rule](../model/attributes.md#the-broadcast-rule) along `d` alone, without the column. One over squeezed dims alone is one value over no dim.
- `partial` and every `Flags` lose `d`.

Any dim squeezes, as in xarray, whatever the consumer.
A consumer that needs a dim refuses a view in which it is squeezed, as it refuses one missing the dim, and the caller writes it as a list of one.
A rule that squeezed for one reader and not another would make one call two views.

## What goes with a dropped label

A selection keeps every axis closed under reference, as the writer keeps a layer's ([removing labels](../layers/removal.md#what-a-removal-reaches)):

- A label whose [property](../model/properties.md#referring-to-a-dim), its default applied, names a dropped label goes. A NULL names no label and stays.
- A tuple naming a dropped label goes, the labels it names staying.
- This repeats until nothing goes, so selecting `country` reaches a bus through `bus.state` and `state.country`.
- An attribute row naming a dropped label, in its column or in a field of a struct, goes. A row NULL there stays and reaches the kept labels alone.

Nothing flows the other way.
A dim keeps a label no kept label refers to: a period no label refers to, a carrier no entity holds, an entity whose connections all went.
Dropping a carrier nobody holds would drop labels nobody selected, and a consumer that needs that says so in one more `select`.
The rule is a removal's, except that a property naming a dropped label drops its subject rather than blocking the removal.

## Laziness and layers

`select` reads the axes and properties it matches labels on and propagates through, which are small, and raises its refusals at the call.
Every attribute stays a plan on the base's connection: a join against the kept labels and a [`settle`](reading.md#settling-rows) along each squeezed dim.
A selection reads the folded record and needs neither its layers nor `partial`.

`flags` answers the base's flags without the squeezed dims, which overstate the view's rows, as `settle` takes them.

A selection is no layer and writes nothing.
`WorkingRecord` refuses it as it refuses any other `RecordLike`, so an edit goes to the base.
The axes it narrows are resolved at the call, so over a `WorkingRecord` a label added later is outside them; an axis nothing narrows reads as the base's.
An attribute reads the base's later edits at the labels kept.

## Lifting rows back to the base

A consumer that computes rows over a selection and writes them over `base` needs the squeezed labels back.
`view.lift(attribute, rel)` adds them, from `labels` and `base.schema` alone.
It adds the column of each squeezed dim the base declares the attribute over, at its label, and a squeezed coordinate back into its struct.
An attribute declared without a squeezed dim is returned as it came; whether to write it is the consumer's decision.

With `view = record.select(scenario="high", period=2030)`:

| attribute                                             | base declares over            | rows over the view          | `view.lift`                               |
| ----------------------------------------------------- | ----------------------------- | --------------------------- | ----------------------------------------- |
| `price`                                               | `bus`, `snapshot`, `scenario` | `bus, snapshot, value`      | `bus, snapshot, scenario = high, value`   |
| `capacity`                                            | `entity`                      | `entity, value`             | unchanged                                 |
| `p`, `snapshot` a multi-dim over `(period, timestep)` | `entity`, `snapshot`          | `snapshot = {timestep: t0}` | `snapshot = {period: 2030, timestep: t0}` |

The coordinates come back in the base's column order.
`lift` inverts a squeeze but one way: a row the base held NULL on the squeezed dim was settled to the label, and comes back at the label.
That is what a write-back wants, a value at the label it was computed at.
`lift` is mechanical: it knows the labels asked for and the base's schema, not what a value means, so a consumer's rule on which results a subset may write stays the consumer's.
