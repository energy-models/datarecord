<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# The schema

Each layer's `manifest.json` carries in full the schema the layer was written under, which may be narrower than a descendant's. A schema declares dims, properties and attributes, maps each dtype to one DuckDB type, and limits which names may become columns.

```python
class Dimension(BaseModel):
    """One axis attribute data may vary over."""

    over: dict[str, str] | None = None  # a multi-dim: coordinate -> dim
    dtype: DimensionDtype | None = "str"  # float, int, str or datetime
    one_of: tuple[str, ...] | None = None  # a str dim's labels: its axis
    unit: str | None = None  # what the labels measure, if anything
    description: str | None = None  # what the axis is, in prose


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take."""

    dtype: ParameterDtype  # float, int, bool or str
    dims: frozenset[str] = frozenset()  # what addresses it: dims, multi-dims among them
    default: Any | None = None
    default_by: dict[str, dict[str, Any]] = {}  # subject.name -> category -> default
    breakpoints: bool = False  # may carry a piecewise-linear curve
    unit: str | None = None  # what the values measure
    description: str | None = None  # what the attribute is, in prose


class PropertySpec(BaseModel):
    """One value per label of its subject, describing the label."""

    dtype: ParameterDtype | None = None  # float, int, bool or str
    one_of: str | None = None  # the dim it refers to, in place of dtype
    default: Any | None = None
    default_by: dict[str, dict[str, Any]] = {}
    unit: str | None = None
    description: str | None = None


class Schema(BaseModel):
    id: UUID | None = None  # a standalone record's layer id
    version: int = 1  # bumped by any change to the declarations

    dimensions: dict[str, Dimension] = {}  # every dim, multi-dims among them
    attributes: dict[str, AttributeSpec] = {}  # one attribute, one spec
    properties: dict[str, dict[str, PropertySpec]] = {}  # subject -> name -> spec

    partial: frozenset[str] | None = None  # dims a layer may patch value by value

    meta: dict[str, Any] = {}  # stored, never interpreted
```

[Dims](dims.md), [multi-dims](multi-dims.md), [properties](properties.md) and [attributes](attributes.md) each have their own page. `partial` is the one part specific to layering, and so the one optional part. It is `None` for a record with no layers, since nothing overrides anything there ([`partial`](../layers/tree.md#partial-the-granularity-of-an-override)).

`meta` holds a framework's own top-level data, such as network attributes, a coordinate reference system or free-form metadata. The package stores it and never interprets it, since none of it describes the dimensioned data.

## Dtypes

A dtype is mathspec's: `DimensionDtype` and `ParameterDtype` from `mathspec.program`, stored as the plain string a mathspec model declares. The manifest and the model that a record feeds share one vocabulary. Each declaration maps to one DuckDB type, one constant in `schema/dtypes.py` that every typed column reads:

| declares                 | declaration        | DuckDB type                |
| ------------------------ | ------------------ | -------------------------- |
| dim, attribute, property | `dtype: float`     | `DOUBLE`                   |
| dim, attribute, property | `dtype: int`       | `BIGINT`                   |
| attribute, property      | `dtype: bool`      | `BOOLEAN`                  |
| dim, attribute, property | `dtype: str`       | `VARCHAR`                  |
| dim                      | `dtype: datetime`  | `TIMESTAMP` (no time zone) |
| dim                      | `one_of: [labels]` | `ENUM` of the labels       |
| property                 | `one_of: <dim>`    | that dim's type            |

A [dim with `one_of`](dims.md#a-dim-with-one_of) is a `str` dim whose column is an `ENUM` of its labels, and whose axis is that list.

A property declares exactly one of `dtype` and `one_of`. A property with `one_of: <dim>` refers to that dim, whether its subject is a multi-dim or not, and takes the dim's type: an `ENUM` for a dim with `one_of`, `TIMESTAMP` for a `datetime` dim ([referring to a dim](properties.md#referring-to-a-dim)). The schema refuses at load a property that declares both or neither, a list of labels in place of the dim, and a multi-dim as the dim.

A [multi-dim](multi-dims.md) declares `over` and neither `dtype` nor `one_of`; the schema refuses it beside either, and a coordinate drawing on an undeclared dim or on another multi-dim.

An attribute declares no `one_of`, as a mathspec parameter has none. No property is `datetime`. A property that holds a time refers to a `datetime` dim, such as `snapshot`.

Struct columns are derived from the declarations. A multi-dim's column is a `STRUCT` with a field per coordinate, each typed as its dim, and an owner flag is a `STRUCT` of `BOOLEAN`. The fixed columns are `attribute VARCHAR`, `breakpoint DOUBLE` and `breakpoints BOOLEAN`.

The manifest writes a dtype as its string, `"dtype": "float"`. A dim with `one_of` carries `"one_of": [...]` beside it. A multi-dim carries `"over": {...}` and no `dtype`. A property that refers to a dim carries `"one_of": "<dim>"` in place of `dtype`.

## Names

A declared dim, multi-dim coordinate, attribute or property name becomes a column, beside the columns datarecord writes itself. `Schema` refuses at load a name that cannot stand there, and the error names it and the reason:

- A name that is not a plain identifier: ASCII letters, digits and `_`, not starting with a digit. This is [mathspec](https://github.com/energy-models/mathspec)'s rule for an entry name, since a model's expressions must be able to write the name. It also lets a dim's name stand unquoted in the [shared tables](../storage/concurrency.md#a-cursor-per-thread) a fold caches. Mixed case and a keyword are fine.
- A column the format writes, compared ignoring case as DuckDB resolves a column name. These are `attribute`, `breakpoint` and `value` on [every long row](../storage/format.md#the-long-schema), `varies`, `broadcast`, `breakpoints` and `layer_uuid` in [the owner map](../layers/resolution.md#owner-map), and `order_key` on a resolved frame. A collision would be silent: a dim named `value` would give the long file two columns of one name.
- A name with a leading `_`. Every scratch column that a fold adds and drops starts with `_`, so that set can grow and this list stays the same.
- Two spellings that differ only in case. DuckDB resolves `Country` and `country` to one column, so one would read the other's values. One spelling declared twice, as when a multi-dim's coordinate is named after the dim it draws on, is a single column and is fine.

Labels are data, and none of these rules constrain them. Beyond these columns, no name is special: `entity` or `bus` is a name like any other. The properties page says [where a name may appear twice](properties.md#names).

## One schema per record

A record is read under its last layer's schema, the one that layer's [manifest](../storage/store.md#the-layers) carries. Each layer's `manifest.json` holds the full schema the layer was written under, never a delta, so one file answers a revision's schema without walking its ancestry. Each layer of the ancestry folds under the schema in its own manifest, while the revision's owner map and reads use the revision's schema, to which the fold [extends the map](../layers/resolution.md#extending-the-map-to-a-wider-schema). A layer's data files hold no schema, which keeps each a plain parquet file that a tool knowing nothing about layering can read.

The schema is not layered data, and nothing folds it. A child's schema must pass `compatible_with` against its parent's, and a root's is free, so a layer cannot change what `p_nom` means, its dtype or the dims it varies over ([versioning](#versioning)). A child may widen its parent's schema. A new attribute then reaches that child and its descendants, and no revision written before, so one branch's experiment leaves its siblings as they were, and two sibling branches may declare one name differently. `schema.meta` is part of the schema and a child inherits it, unlike the revision's own `meta` in the manifest.

A standalone record carries its own `manifest.json`, with no parent. `Workspace.at(uri)` reads that file, so the record answers the same through any workspace, and `at` refuses a directory without one. Its id, a UUID that `ws.write` mints, is the manifest's and not the schema's. The id is the record's layer id, which keys the fold tables a workspace shares. An id derived from the URI would serve stale folds to a record deleted and written again at that URI, or to a relative path read from another working directory. A layer of a store is read alone through its [store](../storage/store.md#the-store), `store.layer(id)`, under its own manifest, and `at` refuses a manifest that names a parent.

An edit changes no schema. A [`WorkingRecord`](../use/editing.md) is one more layer over its base, and a layer declares nothing until it is committed, so a working record reads under its base's schema, standalone or not, and its edits are committed under it.

## Versioning

A child may declare a wider schema than its parent, so a read meets layers written under an older schema than its own. `version` records which schema a layer was written against. What matters is which changes existing layers survive.

A compatible change keeps old layers readable. `version` bumps and nothing else happens:

- adding an attribute or a property, since neither changes the key of [the owner map](../layers/resolution.md#owner-map), which only `partial` widens
- adding a dim that no existing attribute varies over, a multi-dim among them; until a layer writes a new multi-dim, it has no tuples
- widening an `AttributeSpec.dims`, since an unset dim is NULL and NULL means every label ([the broadcast rule](attributes.md#the-broadcast-rule))
- adding to `partial`, which makes ownership finer from the layer that adds it on, while an existing layer reads as it was written; the fold [extends](../layers/resolution.md#extending-the-map-to-a-wider-schema) the map folded so far, [materialised](../layers/resolution.md#materialised-node-caches) or not, to the finer key
- changing a [`unit` or `description`](#unit-and-description), which decides how no row decodes

An incompatible change makes existing rows decode differently, or not at all:

- narrowing `dims`, since a row that sets a dim no longer declared has no valid reading
- changing a `dtype` or `one_of`, which includes dropping a label from a dim's `one_of`
- moving a name between `properties` and `attributes`, or between subjects, which moves the [file](../storage/format.md#where-a-value-lives) its value is read from
- removing from `partial`, after which a layer that patched one value along that axis is a partial override of an axis owned whole, which [`partial`](../layers/tree.md#partial-the-granularity-of-an-override) forbids

The compatible changes are those where NULL already means what the new schema needs, so [the broadcast rule](attributes.md#the-broadcast-rule) absorbs them and no row changes. `schema.compatible_with(other)` returns one reason per incompatibility. A commit checks the child's schema against its parent's manifest this way before it writes any data, and refuses an incompatible one, leaving the layer [pending](../storage/store.md#a-revision-exists-only-by-its-file).

An incompatible change needs the layers rewritten. For a layered record that means flattening it to a [standalone directory](../storage/concurrency.md#a-standalone-directory) under the new schema. A reader that meets a `version` it was not written for should refuse, since every failure above is silent.

## `unit` and `description`

A `Dimension`, an `AttributeSpec` and a `PropertySpec` may each carry a `unit` and a `description`. The package stores them, reads them back and hands them to whatever displays or documents the record. It interprets neither: there is no unit conversion, no dimensional analysis and no check on adding `MW` to `kW`.

They describe the dimensioned data, so they belong in the schema. `meta` holds what does not describe it. A `unit` belongs to an attribute as its `dtype` does, and a consumer finds what `p_nom` is and what it is measured in from the schema alone.

`None` means undeclared. A quantity that has no unit is `""`. A renderer uses the difference to choose between showing nothing and showing an empty unit, and a check can use it to find what is still undocumented.

A dimension's `unit` describes what its labels measure. A `vintage` axis labelled in years or a `distance` axis in km has one. `scenario` and `snapshot` have none, since a timestamp is no quantity. `description` applies to any axis.

Neither field changes how a row decodes, so adding or editing one is a [compatible change](#versioning).
