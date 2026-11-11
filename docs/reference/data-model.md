<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The data model

This page describes what a record contains: dims and their labels, multi-dims, properties and categories, attributes, and the broadcast rule for an attribute's rows.
A **record** is the object you hold: dims, properties and attributes under one [schema](schema.md), exposed as DuckDB relations through [`RecordLike`](record.md).
`Record`, `WorkingRecord` and `Selection` are records.
The reasons for each choice below are on the [about page](../about/data-model.md).

The examples read one record of the running example, whose schema the [schema reference](schema.md) declares.
It has the entities `wind`, `gas` and `dc`, the buses `north` and `south`, and the snapshots 0, 1 and 2.

## Dims and labels

A **dim** is a set of unique labels that values vary along, declared by a `Dimension` under `dimensions`.
A **label** is one value of a dim, unique within it, typed by the dim's `dtype`.
An **axis** is the labels one dim has in one record, in order, which `record.dims[dim]` returns:

```python
record.dims["entity"].fetchall()
```

```text
[('wind',), ('gas',), ('dc',)]
```

The axis lists the three entities in axis order.

A label exists because its axis has a row for it, whether or not any attribute row contains it.
The schema declares no order.
A record's data is the data each of its revisions adds (a [layer](layers.md)), and [resolution](resolution.md#axes) states which layer's file sets the axis order.

### A dim with `one_of`

A dim with `one_of` has exactly the labels its list declares, in list order.
Its axis comes from the schema and from no file, so `record.dims["ctype"]` returns `Generator`, `Link` and `Load` in every record of the schema.
Its column is an `ENUM` of the labels.
A dim with `one_of` has no properties, and [`add` and `remove`](editing.md#add-and-remove) refuse it: a new label is a schema change.
Otherwise it is a dim like any other, which an attribute may range over and a property may refer to.

## Multi-dims

A **multi-dim** is a dim declared with `over`, whose labels are tuples of other dims' labels and list the combinations that exist.
`connection` over `(entity, bus)` lists which entity attaches to which bus:

```python
record.dims["connection"].fetchall()
```

```text
[({'entity': 'wind', 'bus': 'north'},), ({'entity': 'gas', 'bus': 'south'},),
 ({'entity': 'dc', 'bus': 'north'},), ({'entity': 'dc', 'bus': 'south'},)]
```

The link `dc` attaches to both buses, and each generator to one.

A multi-dim is one struct column named for it, in its axis and in every attribute row over it.
A **field** is one named part of a dim's label: a multi-dim has one per entry of `over`, such as `entity` and `bus` of `connection`, and a flat dim has one, named for itself.
The struct has one field per entry of `over`, in `over` order, each typed as the dim it draws on.
Two fields may draw on one dim, such as `from` and `to` of a corridor between two buses.
Each field draws on a dim that is no multi-dim.

The struct's value is a whole tuple or NULL.
A struct with some fields NULL, such as `{entity: dc, bus: NULL}`, matches no tuple, and `set`, `add` and `store.commit` refuse it.

`connection.bus` is the `bus` field wherever a dotted `subject.name` stands.
`Schema.resolve("connection.bus")` returns `connection` and the dim `bus` that the field draws on.
A field is part of the label and has no value of its own, so datarecord refuses `set("connection.bus", ...)`.
A tuple changes only through a `remove` of the old tuple and an `add` of the new one.

Removing a label that a tuple contains removes the tuple and its values ([layers](layers.md)).
For a multi-dim with no tuples, `record.dims` returns an empty relation.

## Properties

A **property** is a value per label of its subject dim, declared by a `PropertySpec` under `properties`.
It is stored as a column of that dim's axis file, so it has no `dims` of its own.
You name it `subject.name`, such as `entity.carrier` or `connection.role`, and a plain name always means an attribute.
`record.properties[subject]` returns the subject's column and every property declared for it:

```python
record.properties["entity"].fetchall()
```

```text
[('wind', 'Generator', 'wind'), ('gas', 'Generator', 'gas'), ('dc', 'Link', None)]
```

The columns are `entity`, `ctype` and `carrier`, and `dc` has no carrier because no layer wrote one.

A property's name belongs to its subject.
`bus.x` and an attribute `x` are two names, and `carrier` may be a property of `entity`, a property of `bus` and a dim at once.
A multi-dim's tuples are its labels, so a multi-dim may have properties too.

A property may refer to a dim with `one_of` in place of `dtype`, and its values are then that dim's labels.
A value that is not NULL must be a label the record has.
[`set` and `add`](editing.md#validation) refuse a label the axis lacks, and `remove` refuses a label that is still a property's value.
A NULL value refers to no label.
A chain such as bus to state to country is two properties, `bus.state` and `state.country`, which a reader joins.

A property has one value per label and does not vary along another dim.
A mapping that varies, such as a bus's country per scenario, is a multi-dim over `(bus, scenario)` with a property `country`.

## Categories

A **category** is a label's value of a categorising property.
A property categorises its subject when two conditions hold:

- The subject is in [`partial`](schema.md#partial), the dims along which a layer overrides one value per label, a multi-dim among them.
- The property refers to a dim with `one_of`.

`entity.ctype` refers to `ctype`, which has `one_of`, and `entity` is in `partial`, so `ctype` categorises `entity`.
`entity.carrier` refers to `carrier`, which has no `one_of`, so it categorises nothing.
`Schema.categorizing("entity")` lists `("ctype",)`, the categorising properties in declaration order.

A category selects labels and picks a default:

- `flags(ctype="Generator")`, a `set` keyword and a `select` keyword each apply to the labels with that category.
- `default_by` keyed `entity.ctype` gives a default per category, which `Schema.default_for(name, ctype="Load")` returns.

The keyword is the property's plain name, `ctype`.
A category that no label has selects no label.
When `set` changes a label's category, the label keeps its attribute values, and a read per category uses the current category.

## Attributes

An **attribute** is the values at coordinates over the dims its `AttributeSpec` lists, stored as long rows, one file per attribute.
A **coordinate** is one label on each dim of an attribute, such as `(wind, 1)`, which is where a value applies.
Each row has a column per dim of the attribute, then `attribute`, `breakpoint` and `value`.
A row's labels on those columns cover one coordinate or, with a NULL, many:

```python
record.attributes["p_max_pu"].order("snapshot").fetchall()
```

```text
[('wind', 0, 'p_max_pu', None, 0.3), ('wind', 1, 'p_max_pu', None, 0.8),
 ('wind', 2, 'p_max_pu', None, 0.5), (None, None, 'p_max_pu', None, 1.0)]
```

`wind` has a value per snapshot, and the last row, NULL on both dims, gives 1.0 at every other coordinate.

An attribute is stored with a column per entry of its `dims`, a multi-dim's fields packed in one struct column, so `p_nom` is stored with `entity`.
An attribute **is indexed over** each dim its `dims` lists and each dim that a field of one of its multi-dims draws on.
`dims` sets which dims address a value and nothing about what the value means.
A solve's results are attributes too, which a child layer inherits like any other row.

`breakpoint` is NULL in an ordinary row.
On an attribute with `breakpoints`, a curve is one row per breakpoint, with `breakpoint` the abscissa and `value` the ordinate there.
datarecord checks no property of the curve, such as convexity.

## The broadcast rule

**The broadcast rule** is the rule that a NULL in a dim's column means every label of its dim.
Where rows cover one coordinate, datarecord keeps the row that leaves no dim NULL that another row there sets.
The settled rows below take the rows of `p_max_pu` above and give one row per coordinate:

<figure class="diagram">
--8<-- "docs/static/data-model-broadcast.svg"
</figure>

`wind` keeps its three values, because a row that sets both dims is more specific than the NULL row.
`gas` and `dc` take 1.0 at every snapshot.

A NULL means every label of its dim, labels that a later layer adds included.
`set("p_max_pu", 0.9)` writes one row with `entity` NULL, which applies to every entity, loads included.
A value for some categories is one row per label: `set("p_max_pu", 0.9, ctype="Generator")` writes a row for `wind` and one for `gas`.

A row that sets a dim is more specific than one that leaves that dim NULL.
Where two rows of one layer each leave NULL a dim that the other row sets, neither is more specific.
`store.commit` refuses such a layer, naming the attribute and the coordinate.
Rows of two layers meet the same way, and [the fold](resolution.md) adds a row that sets both where they cross.

A coordinate that no row covers takes the attribute's default, from `default_by` for the label's category, else `default`.
A default is never stored.
`record.attributes` returns the rows as written, NULL included, and a reader applies the rule with [`settle`](record.md#settle) and the default with `Schema.default_for`.

### A multi-dim's column

A multi-dim's struct column broadcasts over its tuples.
A NULL `connection` means every tuple, and a whole tuple means one:

<figure class="diagram">
--8<-- "docs/static/data-model-multi-dim.svg"
</figure>

The tuple `{dc, south}` keeps 0.9, and the three other tuples take 1.0 from the NULL row.

The dims a multi-dim draws on broadcast on their own.
A field stays inside its struct, so `bus` inside `connection` is no column of its own, and an attribute over the dim `bus` has a separate `bus` column.
An attribute's `dims` may not list a multi-dim beside a dim with the name of one of its fields. A keyword such as `bus=` would then address both.

## Wide and long rows

A record returns `dims` and `properties` wide, with one row per label or tuple, and `attributes` long, with one row per value:

| Relation                       | Columns                                                  |
| ------------------------------ | -------------------------------------------------------- |
| `dims["entity"]`               | `entity`                                                 |
| `dims["connection"]`           | `connection STRUCT(entity, bus)`                         |
| `properties["entity"]`         | `entity`, then one column per property                   |
| `attributes["p_max_pu"]`       | `entity`, `snapshot`, `attribute`, `breakpoint`, `value` |
| an attribute over `connection` | `connection`, `attribute`, `breakpoint`, `value`         |

An attribute row has no property columns.
To read one category's rows, a reader joins `properties["entity"]`, filtered on the category, on `entity`.
To read a multi-dim's fields flat, a reader takes `connection.bus` in SQL or unnests the struct.

!!! info "See also"

    - [The schema](schema.md) declares every name on this page.
    - [The record](record.md) lists what each accessor returns.
    - [Layers](layers.md) describes how a layer writes and removes labels.
    - [Why the data model is shaped this way](../about/data-model.md).
