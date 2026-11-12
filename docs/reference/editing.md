<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Editing

A `WorkingRecord` is a record that stages edits over a base `Record` and reads back with the edits applied.
An **edit** is a `set`, `add` or `remove` on a `WorkingRecord`, staged until `store.commit` writes the edits as a child [layer](layers.md).
Nothing is written until then.
`declare` widens the schema the edits are staged under.
The examples edit `rev`, the revision of the running example's record that the [data model](data-model.md) reads.

```python
from datarecord import WorkingRecord

w = WorkingRecord(rev.record)
rel = w.attributes["p_nom"]
w.set("p_nom", 150.0, entity="wind")
rel.order("entity").fetchall()
```

```text
[('gas', 'p_nom', None, 50.0), ('wind', 'p_nom', None, 150.0)]
```

`wind` has 150.0 before any write, and the relation taken before the edit returns it too.

`WorkingRecord` is a subclass of `Record`, so every read member of [the record](record.md) applies, with the edits as one more layer on the base.
The base is a `Record`, and `WorkingRecord` raises `TypeError` for any other `RecordLike`, a `Selection` included:

```text
a `WorkingRecord` folds its staged rows over a `Record`'s layers, and a Selection has none: write it first, `WorkingRecord(ws.write(source, uri))`
```

## The shape of an edit

Each edit stages rows of one file of the [format](format.md):

| Edit                      | Stages                                                                               | Keyed by                    |
| ------------------------- | ------------------------------------------------------------------------------------ | --------------------------- |
| `set` an attribute        | rows of `attributes/<attr>.parquet`                                                  | the attribute's coordinates |
| `set` a property          | the subject's axis, with one column of the labels set changed                        | the subject's label         |
| `add` labels or tuples    | the dim's axis, with the rows added                                                  | the dim's label             |
| `remove` labels or tuples | the dim's axis without them, and each multi-dim without the tuples that contain them | the dim's label             |

A layer restates each axis it changes whole, whatever `partial` sets: every label the axis has, with the edits applied, in [axis order](data-model.md#dims-and-labels).

## `declare`

`declare(name, spec)` widens the schema that `w` reads and commits under, so that an edit of `name` follows in the same revision.
`declare` reads the kind from the type of `spec`:

| `spec`          | Declares                                  | `name`         |
| --------------- | ----------------------------------------- | -------------- |
| `Dimension`     | a dim, or a multi-dim where it has `over` | a plain name   |
| `AttributeSpec` | an attribute                              | a plain name   |
| `PropertySpec`  | a property of a dim or multi-dim          | `subject.name` |

```python
from datarecord import AttributeSpec, Dimension, PropertySpec

w.declare("scenario", Dimension(dtype="str"))
w.declare("weight", AttributeSpec(dtype="float", dims={"scenario"}))
w.declare("connection.note", PropertySpec(dtype="str"))
w.declare("p_nom", AttributeSpec(dtype="float", dims={"entity", "scenario"}))
```

`declare` builds the widened schema, `w.schema` with `spec` under `name`, through `Schema` validation.
So it refuses what [the schema](schema.md) refuses in a manifest.
Those include an undeclared subject or dim, a property of a dim with `one_of`, a name that cannot be a column and a malformed `default_by`.
`declare` refuses a dotted name with a `Dimension` or an `AttributeSpec`, and a plain name with a `PropertySpec`.

A name the schema declares already is redeclared.
`declare` refuses a redeclaration that [`compatible_with`](schema.md#compatibility) refuses, a `spec` equal to the declaration, and a name declared as another kind.
So an attribute's `dims` may gain a dim, and a `default`, `default_by`, `unit` or `description` may change.
A `dtype`, a `one_of`, an `over` and a dim of `dims` stay as declared.
`partial` stays as the base's, so a declared dim is outside it.

`declare` stages nothing.
After it, `w.schema` and `w.edits.schema` are the widened schema, and every read uses it.
A new dim has no labels, a new attribute has no rows, and a new property is NULL on every label.
An attribute widened over a dim reads its rows as before, broadcast along the dim.
`set`, `add` and `remove` then take the name as any other, with the same checks.
A `set` along a declared dim stages the key's other values as the record reads them, because the dim is outside `partial`.
A `declare` after an edit of the same subject keeps the staged rows, with the new column NULL on them.
`rollback()` clears the declarations with the staged rows, and a rollback that clears one is refused where `declare` is.

`store.commit(w)` writes the layer under the widened schema.
A declared name that no edit reaches is in the child's manifest and in no file.
`declare` refuses a registered record ([registered views](format.md#registered-views)), because its views were created under the schema at `register`.
Call `unregister`, `declare`, then `register` again:

```text
`declare('scenario', ...)` is refused: this record is registered as ['views'], whose views were created under its schema; `unregister` it first, and `register` it again after (https://energy-models.github.io/datarecord/reference/format/#registered-views)
```

It also refuses a closed record, as every edit does.
It refuses a record that an open `WorkingRecord` is staged over, because that record reads the schema. Declare before staging over it.

## `set`

`set(name, value, **coords)` stages values of one attribute, or of one property named `subject.name`.

```python
import pandas as pd

series = pd.Series([0.1, 0.2], index=pd.Index([0, 1], name="snapshot"))
w.set("p_nom", 150.0, entity=["wind", "gas"])  # one value at each label
w.set("p_nom", [150.0, 80.0], entity=["wind", "gas"])  # a value per label, in order
w.set("p_nom", {"wind": 150.0, "gas": 80.0})  # a value per label, keyed
w.set("p_max_pu", series, entity="gas")  # a series indexed by snapshot
w.set("p_max_pu", 0.9)  # every entity, now and later
w.set("p_max_pu", 0.9, ctype="Generator")  # every generator there is now
w.set("efficiency", 0.9, entity="dc", bus="south")  # one connection
w.set("entity.carrier", "wind", entity="dc")  # a property, by its subject
```

### Coordinates

Every coordinate is a keyword, a multi-dim's coordinates included, because the schema declares which coordinates exist.
A keyword has one label or a list of labels, and `set` stages the value at each.
A coordinate with no keyword is NULL, which by [the broadcast rule](data-model.md#the-broadcast-rule) means every label, labels added later included.

`set` packs a multi-dim's coordinates into its struct column, so `entity="dc", bus="south"` stages `connection = {entity: dc, bus: south}`.
You give every coordinate of a multi-dim or none, and none stages a NULL struct, which means every tuple.
A coordinate is named by its own name, such as `from=` and `to=` of a corridor.

### Property keywords

A keyword that is not a coordinate of the attribute is the plain name of a property of a dim the attribute is [indexed over](data-model.md#attributes).
It selects the labels whose value matches on the record as staged, and `set` stages one row per label.
`set` stores nothing of the filter, so its rows do not apply to a label added later.
Several keywords intersect: `ctype="Generator", carrier="wind"` selects the wind generators.
A property of a multi-dim selects its tuples the same way, and a coordinate keyword such as `bus="north"` narrows them.

`set` refuses a dotted keyword, a property name that several of those dims declare, and a property keyword beside its own dim's coordinate keyword.

### Values

| `value`  | Meaning                            | Keywords                                                    |
| -------- | ---------------------------------- | ----------------------------------------------------------- |
| scalar   | staged at every listed combination | an omitted coordinate broadcasts                            |
| sequence | aligned by position                | exactly one list keyword, of the same length                |
| mapping  | keys are labels of one coordinate  | the one coordinate without a keyword                        |
| series   | index is labels of one coordinate  | the index's name, else the one coordinate without a keyword |
| frame    | supplies its own keys              | single labels that complete it                              |

A frame is a DuckDB relation, or a pandas, polars or arrow frame that DuckDB scans.
Over a multi-dim, a frame has the struct column, such as `connection STRUCT(entity, bus)`, and `set` refuses one with the coordinates as flat columns.
`set` never infers a mapping's coordinate from its labels, and refuses the call where neither the index name nor the keywords settle it.

`set` casts a frame column to its declared [type](schema.md#dtypes) where the cast widens it.
It casts a smaller integer to `BIGINT`, a number to `DOUBLE`, a timestamp of any precision to `TIMESTAMP`, and text to and from an `ENUM`.
`set` refuses any other type, and a timestamp with a time zone.

### Properties

`set("entity.carrier", value, ...)` changes one column of the subject's axis rows and keeps the other columns.
Its value is a mapping or series from label to value, a scalar with keywords that select the labels, or a scalar for every label.
A categorising property is set the same way, and the label keeps its attribute values under its new category.

A multi-dim's property takes a scalar for the tuples its coordinate keywords select, or a frame with a value per tuple.
The frame has the multi-dim's coordinates as columns, or its struct column as `dims["connection"]` returns it, and the property's column:

```python
w.set("connection.note", frame)  # columns entity, bus and note, one row per connection
```

`set` patches the column on the tuples the frame lists and keeps their other properties.
It ignores a column of another property, so a frame read from `properties["connection"]` sets back after a change to one column.
It refuses a tuple the axis as staged lacks, a column the axis has no place for and a tuple listed twice.
It also refuses a keyword beside the frame, which supplies its own tuples.

`set` refuses a coordinate, such as `connection.bus`, because a coordinate is part of the key:

```text
`set('connection.bus', ...)` names a coordinate of the multi-dim 'connection', part of its key rather than a value; `remove` the tuples and `add` them with the new label instead
```

### A later `set`

A later `set` replaces each staged row of the attribute that one of its own rows covers.
A row covers another when, at every coordinate, it has the same label or is NULL.
`set("p_nom", 0.9, entity=["wind"])` followed by `set("p_nom", 0.95)` stages one row, and `wind` is `0.95`.
In the other order both rows stay, and `wind` is `0.9`, because datarecord keeps the more specific row.

Where a new row and a staged row each leave NULL a dim that the other row sets, `set` adds a row that sets both:

```python
w.rollback()
w.set("p_max_pu", 0.5, entity="gas")
w.set("p_max_pu", 0.7, snapshot=1)
w.edits.attribute("p_max_pu").order("entity, snapshot").fetchall()
```

```text
[('gas', 1, 'p_max_pu', None, 0.7), ('gas', None, 'p_max_pu', None, 0.5), (None, 1, 'p_max_pu', None, 0.7)]
```

`gas` at snapshot 1 is 0.7, the later value, so the order of two edits sets a value and never makes an edit fail.

A `set` of one value along a dim outside [`partial`](layers.md#partial) also stages the key's other values, as the record reads them.
`w.set("p_max_pu", series, entity="gas")` with a series over snapshots 0 and 1 stages snapshot 2 as well, at the 1.0 that `gas` read before.

## `add` and `remove`

`add(dim, labels, **columns)` stages labels of `dim`, each as one whole row of its axis.

```python
w.add("carrier", ["solar"])
w.add("entity", ["pv"], ctype="Generator", carrier="solar")
w.add("connection", [{"entity": "pv", "bus": "south"}])
w.remove("connection", [{"entity": "dc", "bus": "south"}])
w.remove("entity", ["gas"])
```

`columns` are the dim's properties by plain name, each a scalar for every label or a sequence aligned to `labels`.
A property with no keyword is NULL on the row, because `add` restates the row whole.
A new label goes after the axis's last, and a label of the base that this record removed goes back to its base position.
A label exists because its axis row exists, so `add` with no columns is an edit of its own.

A multi-dim's labels are tuples, which `add` and `remove` take in one of three forms:

- a frame with the struct column, as `dims["connection"]` returns it
- a frame with one column per coordinate
- a list of mappings from coordinate to label

A frame may have property columns beside the tuples, and then each property keyword is a scalar.
`add` refuses a frame that has both the struct column and a coordinate column, and a tuple with a coordinate missing or NULL.

`remove(dim, labels)` stages the axis without `labels`.
Every value keyed by a removed label goes, and so does every tuple, of any multi-dim, that contains one, with the tuple's values.
`remove("bus", ["south"])` removes every connection at `south`.
`remove` refuses a label that is still the value of a property referring to `dim`, on the record as staged:

```text
`remove('carrier', ...)` is refused: `entity.carrier` names them at [('wind', 'wind')]; `set` those to another label or None first
```

`add` and `remove` refuse a dim with `one_of`, whose labels are the schema's.
`add` refuses an attribute as a keyword, and points at `set`.

A `remove` after a `set` on the same label takes effect, because a removed label has no values.
An `add` after a `remove` brings the label back with its values, staged ones included.
The tuples that contained it stay removed until you `add` them.

## `edits`

`w.edits` is the staged layer alone: each attribute that has staged rows, and each axis that differs from the one in the base.
`store.commit(w)` writes it as a child of the revision `w` was built over, and the [fold](resolution.md) reads the rest from the parent.
`ws.write(w, uri)` writes `w` itself, the base with the edits applied, as a standalone directory.

```python
child = store.commit(w)
child.parent == rev.id
```

```text
True
```

A write clears nothing, so `w` returns the same rows until `rollback()` clears every staged row.
An `add` undone by a `remove` leaves no axis in `edits`.
`claim.commit(w)` refuses a claim whose parent is not the revision `w` was built over, because the edits are a difference from that revision.

## Validation

`set`, `add` and `remove` check each edit when it is staged, so the error points at the line that made it.
They raise on:

- an undeclared attribute, property or keyword, or a keyword the attribute does not vary over
- a plain name that only properties have, naming their dotted forms: `set("carrier", ...)` suggests `entity.carrier`
- a label that its dim's axis, as staged, lacks, for every dim
- a tuple that its multi-dim, as staged, lacks
- a tuple with a label that its coordinate's axis lacks, naming the multi-dim, the coordinate and up to three labels
- a label that a dim with `one_of` does not list
- a value of a property that refers to a dim and is no label of it, NULL excepted
- a label or frame column whose type does not fit its dtype
- a sequence whose length differs from its keyword's

```text
`set('p_nom', ...)` names 'entity' labels ['coal'], which the staged axis does not hold; `add` them first (https://energy-models.github.io/datarecord/reference/editing/#validation)
```

## Transactions and staging

Each edit runs in one transaction on the record's cursor.
A refused edit rolls back whole, so the staged rows are as they were before it.
DuckDB nests no transaction, so `WorkingRecord` and each edit raise `duckdb.TransactionException` while you hold a transaction open on the same cursor.

The staged rows are in DuckDB tables in the workspace's database, in the schema `datarecord._staged.<id>` ([the database](store.md#the-database)), never in your files.
On a workspace at `:memory:` they go when the workspace closes.
`close()` drops them, a `with` block calls it on exit, and garbage collection drops them once the record and every relation taken from it are gone, if you never call it.
After `close()`, a read, an edit or `edits` raises `ValueError`.
A `WorkingRecord` over a closed `Record` keeps working, and one over a closed `WorkingRecord` refuses its edits.

!!! info "See also"

    - [The record](record.md) lists the read members that a `WorkingRecord` shares.
    - [Layers](layers.md) describes what a committed layer may contain and which values and tuples a removal deletes.
    - [Edit a record](../howto/edit-a-record.md) shows the steps.
    - The [`WorkingRecord` API](api/mutable.md) lists every signature.
