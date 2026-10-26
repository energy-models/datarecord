<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Editing

A `WorkingRecord` stages edits over a base record and reads back as that record with the edits applied.
Each edit is already a row of the layer it will become.
`set` writes attribute rows or patches a property, and `add` and `remove` change an axis, a multi-dim's tuples included.
Each edit is checked when it is staged.
The staged rows live in DuckDB tables shaped like the files they become.
The caller writes `edits` as a child layer to keep them.

```python
class WorkingRecord(Record):
    """A `Record` whose last source is a staging area, plus the edit surface."""

    def __init__(self, base: Record) -> None: ...

    def set(
        self,
        attribute: str,  # an attribute or a property
        value: Any,  # scalar | sequence | mapping | series | relation or frame
        **coords: Any,  # coordinates, a multi-dim's included, properties of a dim reached
    ) -> None: ...

    def add(self, dim: str, labels: Any, **columns: Any) -> None: ...
    def remove(self, dim: str, labels: Any) -> None: ...

    @property
    def edits(self) -> LayerData: ...  # the staged layer alone
    def rollback(self) -> None: ...
```

`WorkingRecord(revision.record)` stages on the base's connection, `record.con`.
The base must be a `Record`.
Any other `RecordLike` is refused, since a framework object has no layer layout to fold the edits over.

`WorkingRecord` is a subclass of `Record`.
Its sources are the base's sources plus one [staged source](#staging), so it reads as a record one layer deeper than its base.
Every read member is inherited unchanged, so an edit reads back through the same code path a written layer would.
A caller can read an edit back, or hand the record to something that only reads, before anything is written.
`RecordLike` is the protocol, and `WorkingRecord` is a class, since there is one way to edit a record.

Edits accumulate and are written at once:

- An edit costs a row in a staging table.
  A hundred edits to one attribute are a hundred rows, collapsed once when written.
  The first edit of an axis copies the base's resolved one into its table.
- Nothing touches the record until [`edits`](#edits) is written as a child.
  A caller that fails halfway leaves no layer, and a caller that changes its mind calls `rollback()`.

## The shape of an edit

Each edit maps onto exactly one part of [the format](../storage/format.md):

| edit                       | writes                                                                         | key it targets                          |
| -------------------------- | ------------------------------------------------------------------------------ | --------------------------------------- |
| set an attribute           | `attributes/<attr>.parquet` rows                                               | the attribute's coordinates             |
| set a property             | the whole `dims/<dim>.parquet`, one column of a row changed                    | the dim's axis key                      |
| set a multi-dim's property | the whole `dims/<multi-dim>.parquet`, one column of a row changed              | the multi-dim's struct                  |
| add labels or tuples       | the whole `dims/<dim>.parquet`, their rows added                               | the dim's axis key                      |
| remove labels or tuples    | the whole `dims/<dim>.parquet` without them, each multi-dim without its tuples | the dim's axis key, the multi-dim's key |

`add` and `remove` take any dim, a multi-dim among them, and an attribute's [`dims`](../model/attributes.md#attributespec) decide which coordinates its rows carry.

An edit is expressed in the format's own terms.
Setting `p_nom` on twenty entities stages twenty `attributes/p_nom.parquet` rows, which is what a patch layer holds.
A staged edit is already the row it will be written as, so writing `edits` is a concatenation.

## `set`

```python
w.set("p_nom", 150.0, entity=["wind1", "wind2"])  # broadcast
w.set("p_nom", [150.0, 80.0], entity=["wind1", "wind2"])  # per label
w.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per label, keyed
w.set("p_max_pu", frame, entity=["wind1"])  # long frame
w.set("p_max_pu", series.rename_axis("snapshot"), entity=["wind1"])  # a series
w.set("p_max_pu", 0.9)  # every entity, loads included, now and later
w.set("p_max_pu", 0.8, ctype="Generator", carrier="wind")  # selected now
w.set("efficiency", 0.9, entity="dc", bus="north")  # a connection
w.set("p_nom", 200.0, entity="wind1", scenario="high")  # scoped
w.set("entity.carrier", "solar", entity="g2")  # a property, by its subject
w.set("bus.x", 10.2, bus="north")  # a property; `x` alone names an attribute
```

Every coordinate is a keyword, a [multi-dim](../model/multi-dims.md)'s coordinates included: `entity=`, `snapshot=`, `bus="north"` for one connection, `from=` and `to=` for one corridor.
The schema declares which coordinates exist, so none has a parameter of its own.
A keyword holds one label, which fixes that coordinate, or a list of labels, at each of which the value is staged.

A coordinate that no keyword names is NULL.
By [the broadcast rule](../model/attributes.md#the-broadcast-rule), NULL is every label of the coordinate.
So `set("p_max_pu", 0.9)` is one row that reaches every entity, loads and entities added in a later layer included.
`set("p_max_pu", 0.9, ctype="Generator")` is one row per generator that exists now.

A later `set` replaces what it covers.
It drops each row of the attribute that an earlier call staged and that one of its own rows covers.
A new row covers an old one when, at every coordinate, it names the same label or is NULL, counting labels selected by a property keyword or a multi-dim's coordinates.
So `set("p_nom", 0.9, entity=["a"])` followed by `set("p_nom", 0.95)` stages one row, and `a` reads 0.95.
The other order keeps both rows, and `a` reads 0.9, since within one layer [the more specific row wins](../model/attributes.md#the-broadcast-rule).
A default set first keeps the exceptions set after it.

`set` packs a multi-dim's coordinates into its [struct column](../model/multi-dims.md#one-struct-column), so `entity="dc", bus="north"` stages `connection = {entity: dc, bus: north}`.
A multi-dim's value is [a whole tuple or NULL](../model/attributes.md#the-broadcast-rule), so every coordinate must be given or none.
Omitting `bus` alone is refused, since `{entity: dc, bus: NULL}` names no tuple.
Naming no coordinate stages the NULL struct, which is every tuple.
A coordinate is named by its own name: a contract `(from, to)` over `entity` is addressed as `from="plant", to="mill"`.
`entity=` beside those coordinates is refused, since it could name either party.

A property as a keyword selects labels when `set` is called.
A keyword is bare.
It names a coordinate of the attribute first, else any property of a dim the attribute reaches, categorizing or not.
So `bus="north"` on a connection attribute is the coordinate even where `entity` has a property `bus`.
`carrier="gas"` on an attribute over the `carrier` dim is that dim, and on `p_max_pu` it is `entity.carrier`.
A dotted keyword is refused, and so is a name that several dims the attribute reaches declare a property of.
`ctype="Generator", carrier="wind"` resolves to the labels that match every keyword on the record as staged, and `set` writes one row per label.
A property of a multi-dim the attribute is over selects its tuples the same way: `role="output"` on an attribute over `connection` writes one row per output connection, and a keyword naming one of its coordinates, such as `bus="north"`, narrows those tuples.
The filter itself is not stored, so a label added later is not reached.
A plain NULL and [`default_by`](../model/properties.md#default_by-a-default-per-category) are the two ways a value reaches labels not yet added.
The property's dim must be one the attribute reaches through exactly one coordinate, and the keyword is refused beside that coordinate's own keyword.

`set` writes a property too, a category included, when `attribute` names one as [`subject.name`](../model/properties.md#properties).
A plain name is an attribute.
Setting a property patches one column of its subject's axis rows and keeps the other columns.
Its value is a mapping or series from label to value, a scalar with a `dim=` keyword naming the labels, or a scalar for every label the axis has.
A multi-dim's property takes a scalar, with its coordinates as keywords selecting the tuples, none selecting every tuple: `set("connection.role", "input", entity="g1", bus="north")`.
A [coordinate](../model/multi-dims.md#coordinates) is part of the key, not a property, so `set("connection.bus", ...)` is refused; the caller `remove`s the tuples and `add`s them again.
Changing a category is such a patch: `set("entity.ctype", "Load", entity="g1")`.
Every value `g1` holds stays stored and read, and the per-category reads of `ctype="Load"` now include those values.

`value` takes five forms, since assigning one value to many labels and a different value to each are equally ordinary:

| `value`  | meaning                            | keywords                                                       |
| -------- | ---------------------------------- | -------------------------------------------------------------- |
| scalar   | staged at every listed combination | an omitted coordinate broadcasts                               |
| sequence | aligned positionally               | exactly one list keyword, same length                          |
| mapping  | keys are labels of one coordinate  | the one coordinate no keyword names                            |
| series   | index is labels of one coordinate  | the index's own name, else the one coordinate no keyword names |
| frame    | supplies its own keys              | single labels completing it                                    |

A frame is a DuckDB relation, or a pandas, polars or arrow frame.
It reaches DuckDB through DuckDB's own scanner, and no scanner package is a dependency, so a frame of a library never imported is no frame.
A relation on another connection is [refused](../storage/concurrency.md#a-relation-from-another-connection-is-rejected).

Each column is checked against the DuckDB type its dtype maps to ([dtypes](../model/schema.md#dtypes)).
A widening is cast: a smaller integer to `BIGINT`, a number to `DOUBLE`, a timestamp of any precision to `TIMESTAMP`, and text to the `ENUM` of a [dim with `one_of`](../model/dims.md#a-dim-with-one_of) and back.
The cast keeps the kind of value, though an integer past 2^53 rounds and a nanosecond truncates to the microsecond.
Any other type is refused, naming the declared type, unless the column holds only NULLs.
A column of timestamps with a time zone is refused.

A mapping's keys and a series' index may hold labels of any of the attribute's coordinates, since an axis label is a string like any other.
The caller says which coordinate, by naming the series' index after it or by naming every other coordinate as a keyword.
Where neither settles it, the call is refused.
`set` never infers the coordinate from the labels.
A test against the axis would make one call mean different things in two records: a scenario labelled `wind1` would capture a series meant per entity, and a partial overlap would pick a reading silently.

A frame over a multi-dim carries it as the struct column, `connection STRUCT(entity, bus)`.
A frame that carries a coordinate flat, `entity` or `bus`, is refused with the struct to pass.
So is a struct whose fields differ from the multi-dim's coordinates, which a cast would silently re-key.
A struct with some fields NULL is refused too.
A NULL struct broadcasts over every tuple.

The first four forms become a long frame before staging, so there is one staging path.
A sequence whose length differs from its keyword's is an error at the call.

Every form is checked against [the labels the record resolves](#validation), the frame included.
A frame decides where the labels come from, and those labels must still exist.

## `add` / `remove`

```python
w.add("carrier", ["wind", "H2"])
w.add("bus", ["north", "south"])
w.add("bus", ["north H2"], carrier="H2", x=10.1)  # bare: the dim's own
w.add("entity", ["g1", "g2"], ctype="Generator", carrier="wind")
w.remove("entity", ["old_coal"])
w.remove("carrier", ["gas"])  # refused while entity.carrier holds it
w.add("connection", [{"entity": "g1", "bus": "north"}], role="output")
w.add("connection", other.dims["connection"])  # the struct column `connection`
w.add("connection", frame)  # an `entity` and a `bus` column, `role` beside them
w.remove("connection", [{"entity": "g1", "bus": "north"}])
```

`add(dim, labels, **columns)` stages whole axis rows.
`columns` are the other columns of `dims/<dim>.parquet`: the dim's properties, bare since the subject is `dim`.
Each is a scalar for every label or a sequence aligned to `labels`.
A column no keyword names is NULL on these rows, since `add` restates a label's row whole.
A new label goes after the axis's last, and a removed label of the base goes back to its base position.
A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) is refused by `add` and `remove` alike, since its axis is the schema's list; a new label is a schema change.
A property value that refers to such a dim must be one of the labels it lists, and is refused as any unknown label is.
An attribute keyword is refused with a pointer to `set`, since an attribute's values are long rows whatever its `dims`.
A column the schema does not declare is refused.
A [staging table is shaped like the file it becomes](#staging), so it has no dtype for such a column.
A tool that grows a column declares it first, which [schema versioning](../model/schema.md#versioning) accepts as a widening.

A label exists by virtue of its axis row, so `add` is an edit of its own.
[Validation](#validation) refuses a value for a label no layer declares.
Adding a bus with no properties is an edit with nothing to `set`.
One axis row holds each label, so an `add` of a label the record already resolves restates its row.

A [multi-dim](../model/multi-dims.md)'s labels are tuples, which `add` and `remove` take as a frame or a list.
A frame, anything DuckDB scans, holds the multi-dim's [struct column](../model/multi-dims.md#one-struct-column), `connection STRUCT(entity, bus)`, which is what `dims["connection"]` reads, so a read goes back in as it stands.
A frame may instead carry one column per coordinate, `entity` and `bus`.
A frame carrying both the struct column and a coordinate column is refused, since it gives each tuple twice.
Either frame may carry property columns beside the tuples, and beside a frame a keyword property must be a scalar.
A list of mappings from coordinate to label, such as `{"entity": "g1", "bus": "north"}`, takes its properties as keywords, each a scalar or a sequence aligned to the list.
The list is made into a frame with a column per coordinate up front, and the struct column is unpacked to the same, so every form takes the same path and the same type checks.
The coordinates are packed into the multi-dim's [struct column](../model/multi-dims.md#one-struct-column), and each tuple is staged as one row of `dims/<multi-dim>.parquet`, placed as any label is.
A tuple with a coordinate missing or NULL is refused, since it names no tuple.
A tuple naming a label that its coordinate's axis, as staged, lacks is refused ([validation](#validation)).
A multi-dim's attributes are long rows, set through [`set`](#set) with its coordinates as keywords.

Adding a component with its connections and varying values is `add` of the entity, then `add` of its connections, then a `set` per attribute.

`remove(dim, labels)` deletes each label's row from the staged axis.
It need not list what it deletes.
Where `dim` is `partial`, [the fold](../layers/removal.md) drops every value keyed by the label, and a multi-dim attribute's value through the label inside its struct ([the owner map](../layers/resolution.md#owner-map)).
`remove` also deletes every tuple, of any multi-dim, with a coordinate drawing on `dim` that names one of `labels`.
It reads those tuples from the record as it resolves with pending edits, so [the writer](../layers/removal.md#what-the-writer-checks) never meets a dangling tuple.
An `add` of the same label brings back the label alone, and its tuples come back through an `add` of each multi-dim.

`remove` is refused while a [property that refers to](../model/properties.md#properties) `dim` stores one of the labels, read from the record as staged.
A tuple goes with its label, and a property's row belongs to its subject and outlives the label.
The caller `set`s such a property to another label or None first.

## Edits

A `WorkingRecord` does not write.
It holds two things that the [writers](writing.md) take:

```python
w = WorkingRecord(rev.record)
w.set("p_nom", 100.0, entity=["gen1"])
edited = rev.child(w.edits)  # the delta, as a child layer
ws.write(w, "out/")  # base plus edits, flattened
w.rollback()
```

- `w.edits` is the staged layer alone, the [`LayerData`](writing.md#layerdata) it already is.
  Written as a child of the revision `w` was built over, it holds only the edits: the attribute rows it set, and each axis it changed, a multi-dim's included, restated whole.
  The fold resolves the rest from the parent.
  The layer lands in the child, so the revision that `child` returns reads the edits back.
- `w` itself is what is staged plus what the record already reads: the whole resolved record.
  It is written as a standalone directory, where there is no parent to resolve against.

The writer has one contract for both: enumerate what the source holds and hand each part over.
The caller chooses the rows to write, `w.edits` or `w`.
The caller names the parent, which is the revision `w` was built from.
`rollback()` clears the staging area.
A write clears nothing, so a `WorkingRecord` that still holds its edits reads exactly what it did before.

`other.child(w.edits)`, with `w` built over a different revision, would write a delta computed against the wrong parent, since an axis edit restates its base's whole axis.
`child` therefore compares the edits' base revision with its own id and raises on a mismatch.
Edits over a directory have no revision as their base, so they are a child of none.
`rev.child(w)` writes the whole resolved record as a child layer.
That layer reads back correctly and is larger than the delta, so `edits` is the name to use.

An edited axis, a multi-dim's included, is restated whole, whatever [`partial`](../layers/tree.md#partial-the-granularity-of-an-override) says.
The layer holds every label the axis has with the edits applied, with its properties, in [axis order](../model/dims.md#axis-order).
A layer's axis file then says what the axis is there, and reads without its parent ([removing labels](../layers/removal.md)).
A standalone write of `w` writes the resolved axis whole as well, and an axis nothing touched is written by neither.

An edit replaces the rows it names.
It deletes the rows at the coordinate it writes and inserts the new ones, so a staging table holds one row per coordinate and reads without a fold.
The coordinate it replaces on is the one a read would have collapsed, so the delete removes at least what a last-writer-wins fold would have discarded.
An attribute's `set` also deletes every earlier row it [covers](#set).
A property of an axis, a multi-dim's included, differs in mechanism and has the same effect.
A row's columns are independently editable, so a property `set` patches its one column in place (`UPDATE`), which keeps a sibling property that another `set` wrote.

An edit replaces per coordinate.
The ownership key leaves out the dims an attribute is [not owned per](../layers/tree.md#partial-the-granularity-of-an-override), so replacing on it would collapse a whole staged series to one row.
Two edits at different snapshots are two coordinates.
The same distinction governs [the read overlay](#reading-with-pending-edits) and the fill below, and it is the easiest thing here to get wrong.

Three interactions go beyond replacing by coordinate:

- `remove` after `set` on the same label: the removal wins, since a removed label has no values.
  A removal reaches another file's rows, so it stays an anti-join at read and at write.
  Attribute rows are keyed by coordinate, and the axis by label.
  A layer's removals apply only to its parents in [the fold](../layers/resolution.md#owner-map), so the staged collapse drops its own rows itself.
  It drops a row that names a label the staged axis removed, and a row whose multi-dim column names a tuple the staged multi-dim removed, by `remove` of the tuple or by the cascade of `remove` on a label.
  `set` refuses a label the staged axis lacks ([validation](#validation)), so a removal after the `set` is the only way a staged row comes to name one.
- `add` after `remove` of the same label: the label exists again, at its base position.
  The staged axis then holds it as the base does, so the parent's values for it come back.
  Its tuples stay removed.
- `set` on a label this record also added: the attribute rows live in `attributes/`, and a property patches the staged row.

Along an axis outside `partial`, a layer must carry a label's whole extent.
So a `set` of one value along such an axis reads the resolved series for that key and stages the untouched coordinates beside the edit.
That is the one read of parent data an edit makes.
It happens when the rows are staged, so the staging table already holds what the layer will hold, and a write of `edits` has no completion step.

The fill covers the key the edit named.
A label no edit mentioned keeps its rows in the parent, since a layer that carried them would claim an extent it was never given.
A multi-dim outside `partial` is owned whole the same way.
A `set` on one connection's `efficiency` stages every other connection's resolved value beside it.
A connection attribute's key names no label of a `partial` dim, so the key the edit named is `(attribute)` alone.
A staged row that leaves the axis NULL needs no fill, since [the broadcast rule](../model/attributes.md#the-broadcast-rule) already makes it cover every label, and a carried row beside it would overlap.
Where a base row names a label of that axis and the staged row reaches it, the staged row is also written out along the base row's labels there, with the staged value.
The two rows would otherwise cross on a dim the owner map has no column for, which [no overlap row settles](../layers/resolution.md#a-dim-outside-partial).
Carried rows go in only where no edit holds the coordinate, so a later `set` on a carried coordinate replaces the fill.
The anti-join that keeps a fill off an occupied coordinate is the whole of what orders the two.

## Validation

[The writer](writing.md) validates structure, so writing `edits` inherits its checks.
Editing adds checks at the edit, caught when the edit is staged, so a caller learns about a typo at the line that typed it:

- an undeclared attribute, property or keyword, or a keyword the attribute does not vary over; a plain name that only properties carry is refused naming their dotted forms, so `set("carrier", …)` suggests `bus.carrier` and `entity.carrier`;
- a label that its dim's axis, as staged, lacks, for every dim, `partial` or not; `add` the label first;
- a tuple that its multi-dim, as staged, lacks, in `set`; `add` the tuple first;
- a tuple that names a label which its coordinate's axis, as staged, lacks, in `add` or `set`, naming the multi-dim, the coordinate and up to three such labels; `set` checks this before the tuple itself, so a `set` at a connection with an unknown bus names the bus;
- a label that a [dim with `one_of`](../model/dims.md#a-dim-with-one_of) does not list, in `add` or `set`;
- a frame column whose type does not fit its dtype, timestamps with a time zone among them; by the same rule a label given to `set`, `add` or `remove`, or a value of a property that refers to a dim, so a string is not parsed into a `datetime` dim's label;
- a value of a [property that refers to a dim](../model/properties.md#properties) that is no label of the dim as staged, in `add` or `set`, such as `set("entity.carrier", "coal", entity="g1")` with no `carrier` label `coal`; NULL names no label and passes;
- an `add` that names an attribute.

Each label and tuple check is one DuckDB query that anti-joins the staged rows against the axis as staged, for every checked column at once.

## Staging

Staged rows live in DuckDB tables on the record's own connection:

```sql
CREATE TABLE staged_attributes_<attr digest>_<id> (<that attribute's long columns>);
CREATE TABLE staged_axis_<dim digest>_<id>        (<the axis key>, <the dim's properties>, <position>);
```

`<id>` is the staged layer's own id, so two `WorkingRecord`s on one connection share no table.
An attribute or dim name is a caller's string, so the table name carries a digest of it.

Every table is shaped like the file it becomes.
An axis table adds only a position column, since a DuckDB table keeps no row order.
No table unions columns that the format keeps apart, so a column the schema does not declare has no table to go in, and [`add`](#add-remove) refuses it.

There is one table per staged attribute, since that is the file it becomes.
Its columns are [the attribute's own coordinates](../storage/format.md#the-long-schema), and `value` has the attribute's declared type.
A shared table would have to widen `value` to text and carry every declared dim.
The value would then need casting back on the way out.
A NULL in a dim column would also be ambiguous between an attribute without that axis and [the broadcast rule](../model/attributes.md#the-broadcast-rule)'s every value.
Per attribute, the table's shape answers both questions.

There is one table per staged axis, a [multi-dim](../model/multi-dims.md)'s keyed by its struct column, holding it whole as a layer restates it: its key, its properties and a position column.
The first edit copies the base's resolved axis into it in base order.
`add` appends a new label at the next position and puts a label of the base back at its base position.
`remove` deletes the label's row, and a property `set` patches one column in place.
The writer and the reads take each table in position order.

These tables are the only place a staged row exists, and [the reads](#reading-with-pending-edits) fold them directly.

DuckDB holds the staged rows for three reasons.
The reads are already a fold, and staging elsewhere would marshal every edit into a relation on every read.
A large edit is a bulk insert.
A write of `edits` hands each table to the writer as the file it already is.

The tables live in the workspace's database, like the folded caches, so on `:memory:` they vanish with the workspace and never reach disk.
A record whose edits must survive a process boundary is written as a child.

An edit replaces what it names, so a table holds one row per key and reads as a scan.
An attribute's table has no ordering column, and edit order is which edit ran last.

## Reading with pending edits

The read members reflect the edits, so a `set` followed by a read returns the new value.

A set of pending edits is a layer that is not yet written.
The reads compose it the same way: the staged rows are the last layer, resolved over whatever the record read before.

```text
resolved = fold(parent layers..., staged rows)
```

This is one more fold step over the same [owner-map machinery](../layers/resolution.md#owner-map), with the staging tables in place of a layer directory.
A broadcast of the base reaches a label staged here, since the fold keeps it NULL ([what a row reaches](../layers/resolution.md#what-a-row-reaches)).
A staged layer has met no writer, so the read refuses two staged rows that cross with no staged row naming both, as the write of `edits` would.
It works over a layered base and a plain directory alike, since a directory is a layer laid out like any other.
It costs what one more layer costs, on every read.
A written layer is folded once and cached, and the staged layer cannot be, since it is the only one that can still change.
So the fold is materialised up to the last layer that cannot change under the reader, and the staged step on top stays a relation.
An edit then needs no invalidation, since nothing that depends on it is cached.

`flags` come from the map rows the staged layer folds to, as for any layer.
A staged row that sets a dim adds it to `varies`, one that leaves it NULL adds it to `broadcast`, and a staged curve sets `breakpoints`.
Properties are read from `properties`, staged edits included.

A staged axis is read as its table stands, in position order.
The table already holds the base's rows with the edits applied, so a read composes nothing.
Two `set` calls for two properties of one label compose, each patching its own column of the row.
