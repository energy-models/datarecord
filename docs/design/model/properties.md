<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Properties and categories

A property holds one value per label of its subject, which is a dim, a [multi-dim](multi-dims.md)'s tuples being its labels. It describes the label, so it has no `dims` of its own. A caller names a property as `subject.name`, and a plain name means an attribute. A property may refer to a dim, and then each value is a live label of that dim. If that dim has `one_of` and the subject is in `partial` and no multi-dim, the property categorizes the subject. Categories select labels and key per-category defaults.

## Properties

Properties are declared per subject:

```python
dimensions = {
    "ctype": Dimension(one_of=["Generator", "Line", "Load"]),
    "carrier": Dimension(dtype="str"),
    "country": Dimension(dtype="str"),
    ...,
}
properties = {
    "entity": {
        "ctype": PropertySpec(one_of="ctype"),
        "carrier": PropertySpec(one_of="carrier"),
        "sign": PropertySpec(
            dtype="float", default=1, default_by={"entity.ctype": {"Load": -1}}
        ),
    },
    "bus": {
        "carrier": PropertySpec(one_of="carrier", default="AC"),
        "x": PropertySpec(dtype="float", unit="°"),
        "country": PropertySpec(one_of="country"),
    },
    "carrier": {"color": PropertySpec(dtype="str", default="")},
    "connection": {"role": PropertySpec(dtype="str")},
}
```

A property is a column of its subject's own file, `dims/<dim>.parquet`, a multi-dim's included ([where a value lives](../storage/format.md#where-a-value-lives)). `ctype` is a column of `dims/entity.parquet` because it is declared under `entity`.

An attribute is data: what a model reads as part of its problem and what a layer varies. Whether a name is a property or an attribute is the author's declaration. A tool reads it from its framework's registry where it can ([PyPSA](../tools/index.md)). Each section decides the file a value is stored in, so widening an attribute's `dims` to `[entity, scenario]` is a [compatible](schema.md#versioning) change that keeps its rows where earlier layers wrote them.

### Names

A property's name belongs to its subject. An attribute name is one spec record-wide, with one file and one `value` dtype ([`AttributeSpec`](attributes.md#attributespec)). A property name is unique within its subject's file only. `x` on `bus` sits beside an attribute `x`, a line's reactance, and `carrier` is a property of `entity`, a property of `bus` and a dim. The schema refuses at load a property named like a column of its subject's key, which is the dim's column, a multi-dim's struct column. It also refuses a multi-dim's property named like one of its coordinates, since those are the keywords that address a tuple and the [names a coordinate is addressed by](multi-dims.md#coordinates). Two spellings that differ only in case are refused record-wide ([names](schema.md#names)).

A caller addresses a property as `subject.name`, such as `entity.ctype`, `bus.x` or `connection.role`. This holds wherever a caller names one: in [`set`](../use/editing.md#set), as a [`default_by`](#default_by-a-default-per-category) key, and in `default_for`. A plain name is always an attribute, however few subjects declare a property of that name. Its meaning stays the same when another subject declares the name later. `Schema.resolve(name)` returns the `AttributeSpec`, or the subject and the `PropertySpec`, or for a multi-dim's coordinate such as `connection.bus` the multi-dim and the dim the coordinate draws on. For an unknown plain name it raises an error that lists the dotted forms of the properties of that name.

A column keeps its plain name. The frames of `properties[subject]`, and the categories that [`flags`](../use/reading.md#flags) and `default_for` take, are each read against one subject already. A [selector keyword](../use/editing.md#set) is bare: it matches a coordinate of the attribute first, else any property of a dim the attribute reaches.

### Referring to a dim

A property may refer to a dim with `one_of: <dim>` in place of `dtype`. Its values are that dim's labels, typed as the dim ([dtypes](schema.md#dtypes)). Above, `entity.carrier` and `bus.carrier` refer to `carrier`, and `bus.country` to `country`. Any subject, a multi-dim or not, may declare such a property. The schema refuses at load a property that refers to an undeclared dim or to a multi-dim.

A stored value that is not NULL must be a live label of the dim. The record checks it where it checks a multi-dim's coordinate. [`set` and `add`](../use/editing.md#validation) refuse a label that the record as staged lacks. The [writer](../layers/removal.md) refuses a layer that stores a label the layer removes. [`remove`](../use/editing.md#add-remove) refuses a label that a property still holds. NULL is no label.

A property of a dim holds one value per label, so a classification of a dim's label is single-valued. A many-to-many classification is an ordinary [multi-dim](multi-dims.md). A multi-dim's property is one column of its tuple's row in the same way. It may be NULL, so a tuple may carry no label ([open questions](../open-questions.md)).

A chain of classifications is stored as a chain of properties. bus→state→country is two properties, `bus.state` and `state.country`, and the chain is a join over two axis files.

An attribute over `country` reads back keyed by country. A consumer that needs it per bus joins through the classification.

## Categories

A property categorizes its subject when both of these hold:

- The subject is a dim in [`partial`](../layers/tree.md#partial-the-granularity-of-an-override), and no multi-dim. Only there is a dim owned per label, and a default per label needs that. A [multi-dim](multi-dims.md#one-struct-column) is never categorized.
- The property refers to a dim with `one_of`. The `one_of` is a declared list, so the schema checks each category that a default or a selection names.

`entity.ctype: {one_of: ctype}` with `ctype: {one_of: [Generator, Line, ...]}` is one. Its values are the subject's categories along that property. A [`default_by`](#default_by-a-default-per-category) keys on them, and a [selector keyword](../use/editing.md#set) selects by them. `Schema.categorizing(dim)` lists a dim's categorizing properties in declaration order. A dim may have several: a `technology` property that refers to a dim `technology` with `one_of` categorizes `entity` beside `ctype`. The schema refuses a `default_by` key on a property that fails a condition, and the error names the property and the reason.

A label's categories are its values of the categorizing properties. An entity with `ctype = Generator` has the category `Generator`, which is itself a label of the dim `ctype`.

Categories do two things:

- They select labels. [`flags(ctype="Generator")`](../use/reading.md#flags), a [selector keyword](../use/editing.md#set) and a consumer's per-category frame all pick the labels with that category.
- They pick a default. `default_for(name, ctype="Generator")` gives the default of an attribute or property for such a label.

The keyword is the property's plain name, `ctype`, not `entity.ctype`. A category in the `one_of` that no label holds is valid and selects no label.

A label's category can change, because `set` patches a categorizing property like any other. The label keeps its attribute values, and a per-category read uses its current categories.

## `default_by`: a default per category

`default` is the fallback. `default_by` maps a categorizing property, written as `subject.name`, to overrides per category. Attributes and properties both take it:

```python
"p_min_pu": AttributeSpec(
    dtype="float", dims={"entity", "snapshot"}, default=0,
    default_by={"entity.ctype": {"StorageUnit": -1}},
)
```

An unset value takes the first `default_by` key, in declaration order, whose map lists the label's category. Otherwise it takes `default`.

- Each key must categorize a dim the value reaches. Such a dim is one the value names directly, such as `entity`, or one a coordinate of its multi-dim draws on, such as the `entity` coordinate of `connection`.
- The schema refuses a category that is not in the `one_of` of the dim the key refers to.
- A categorizing property takes a plain `default` and no `default_by`, since a category defaulted by a category is circular.
- Only the default varies. `dtype`, `unit` and `description` belong to the spec, which keeps one name to one column and one `attributes/` file.

A default is never stored. `record.properties` and `record.attributes` hand over stored rows, and a consumer applies a default through `default_for`. The PyPSA tool does so when it builds a component's static frame.
