<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# `WorkingRecord`

[`Record`](record.md) is read-only, and [`write_record`](writing.md) writes a whole record from a source that already knows everything it will contain.
Neither covers editing: adding components, removing them, setting an attribute on a relation.

```python
class WorkingRecord:
    """A `Record` that accepts edits and materialises them on commit."""

    def __init__(self, base: Record, con: DuckDBPyConnection) -> None: ...

    def set(
        self,
        attribute: str,
        value: Any,  # scalar | sequence | mapping | series | frame | nw.Expr
        *,
        entity: Sequence[str] | None = None,
        indexed_by: str | None = None,  # what a series' index holds
        **dims: Any,
    ) -> None: ...

    def add(self, frame: IntoFrame) -> None: ...
    def remove(self, dim: str, labels: Sequence[Any]) -> None: ...

    def add_relation(self, relation: str, frame: IntoFrame) -> None: ...
    def remove_relation(
        self, relation: str, keys: Sequence[tuple[Any, ...]]
    ) -> None: ...

    def commit(self, target: Target) -> Any: ...  # the new child, for NewChild
    def rollback(self) -> None: ...
```

Built over a base `Record` and a DuckDB connection: `WorkingRecord(revision.record, con)`.

A **class, not a protocol**.
`Record` is a protocol because several things satisfy it — two backings, a framework object presenting itself as one, the two readings commit writes — and structural typing is what lets a consumer satisfy it without depending on this package.
There is one way to edit a record, so a second name for it would be an interface over its only implementation.
Where [the staged rows live](#staging) is this class's own business, which is why the name says what it is rather than how.

It **satisfies** `Record`, which is the load-bearing decision: a mutable record reads as a record, and what it reads is the data _with its pending edits applied_.
So an edit can be read back, or the record handed to something that only knows `Record`, without committing.
Structurally, not by inheritance — the read members are implemented here over [base-plus-staged](#reading-with-pending-edits).

Two properties follow from accumulate-then-commit, and both are the point:

- An edit costs a row in a staging table, not a rewrite.
  A hundred edits to one attribute are a hundred rows, collapsed once at commit.
- Nothing touches the record until `commit()`.
  A caller that fails halfway leaves no layer; one that changes its mind calls `rollback()`.

## The shape of an edit

Each edit maps onto exactly one part of the format:

| edit                           | writes                                                                                                 | key it targets                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| set an attribute on a relation | `attributes/<attr>.parquet` rows                                                                       | `(*partial dims, attribute)`       |
| add components                 | `dims/entity.parquet` rows, `attributes/` rows for varying attributes, and rows of the relations named | `entity`                           |
| remove labels                  | a `deleted = true` tombstone on the dim's axis                                                         | the dim's label                    |
| add_relation / remove_relation | `relations/<relation>.parquet` rows and tombstones                                                     | the relation's own key coordinates |

`add_relation` names no component type: a relation's rows are keyed by its columns and the type is not one of them, so there is nothing for it to scope ([where the rows live](format.md#where-a-value-lives)).
Nor is there a `connect`/`disconnect` pair beside it — `connection` is one relation among however many the schema declares, and a call naming it would be the record layer holding one framework's vocabulary.

The key of `attributes/` is [schema-derived](schema.md#partial-the-granularity-of-an-override) rather than spelled: the fold key contains `entity` and every [relation](schema.md#relations) key coordinate, since neither broadcasts.
Every key is `entity`-based, because `entity` is [what identifies a component](format.md#the-entity-axis).
No edit names a type. An `add` gives an entity its type through an `entity_type` column, which stages a row of the `entity_type` [relation](schema.md#types) keyed by `entity`, so one name has one type.

The crucial property: **an edit is expressed in the format's own terms.** Setting a constant `p_max_pu` on twenty components _is_ twenty broadcast rows of `attributes/p_max_pu.parquet`, which is what a patch layer would hold anyway.
So a staged edit is already the row it will be written as, and `commit()` is a concatenation rather than a translation.

## `set`

```python
record.set("p_nom", 150.0, entity=["wind1", "wind2"])  # broadcast
record.set("p_nom", [150.0, 80.0], entity=["wind1", "wind2"])  # per name
record.set("p_nom", {"wind1": 150.0, "wind2": 80.0})  # per name, keyed
record.set("p_max_pu", frame, entity=["wind1"])  # long frame
record.set("p_max_pu", series, entity=["wind1"], indexed_by="snapshot")  # a series
record.set("icon", {"Generator": "turbine"})  # keyed by entity_type labels
record.set("efficiency", 0.9, entity=["dc"], bus="north")  # a connection
record.set("p_max_pu", 0.5, entity=["wind1"], scenario="high")  # scoped
record.set("p_max_pu", nw.col("value") * 1.1, entity=["wind1"])  # derived
```

**There is no `entity_type` keyword.** An attribute is not narrowed to [types](schema.md#types): every declared attribute can be set on any entity its `dims` address.
So `set` checks that each name is on the entity axis and that the attribute is declared over the dims the call names ([validation](#validation)), and one call may span types: `set("p_nom", {"wind1": 150.0, "link_dc": 80.0})` stages both.

`entity=None` means every entity on the axis. `set("p_max_pu", 0.9)` sets it on every entity, buses included; name the entities to scope it.

**Every coordinate but `entity` goes through `**dims`**, a [relation](schema.md#relations)'s included: `bus="north"` addresses one connection, `from=`/`to=` one corridor.
None has a parameter of its own, because which coordinates exist is declared rather than fixed — a `bus=` keyword would spell one relation's coordinate and be unable to name a two-coordinate relation at all.

A plain dim keyword scopes the edit and its absence means "every value" by the NULL broadcast rule, so `scenario="high"` patches one scenario.
A relation key coordinate does not broadcast that way: omitting `bus` means "every connection of this entity" — [the relation's rows](record.md#the-broadcast-rule), not the bus axis.

**An attribute addressed by one axis alone is keyed by that axis's labels**, not by `entity`: `set("icon", {"Generator": "turbine"})` states one type's icon, and `set("co2_budget", 3.0)` reaches every country the axis has.
The edit stages a row of [that axis's own file](format.md#where-a-value-lives) rather than a long row, so `entity=` is refused — an icon belongs to no component — and a sequence is refused too, there being no name list to align against.
A label the axis does not have is refused rather than introduced: an axis row is a label's existence, which an axis file states. Where the axis's dtype is an `Enum` the vocabulary is the schema's, so an undeclared label is rejected without reading the axis at all.

`value` takes six forms, because assigning one value to many targets and assigning a different value to each are equally ordinary and neither should require building a frame:

| `value`   | meaning                              | `entity`                                                                     |
| --------- | ------------------------------------ | ---------------------------------------------------------------------------- |
| scalar    | broadcast to every name              | required unless `None` means all                                             |
| sequence  | aligned positionally to `entity`     | required, same length                                                        |
| mapping   | keys are names                       | ignored if given, else the keys are the names                                |
| series    | index is names, or one axis's labels | names unless `indexed_by=` or the index's own name says otherwise            |
| frame     | supplies its own keys                | redundant                                                                    |
| `nw.Expr` | a function of the current value      | selects what to [derive from](#an-nwexpr-value-derived-from-the-current-one) |

The first four normalise to a long frame before staging, so there is one staging path.
A length mismatch between a sequence and `entity` is an error at the call, not a silently truncated edit.

Every form is checked against [the components the record resolves](#validation), the frame form included: "supplies its own keys" decides where the names come from, not whether they have to exist.

A one-dimensional labelled series is ambiguous: its index may hold names or axis labels, and neither its dtype nor its values settle it, an axis label being a string like a name.
**The caller says which** — `indexed_by="snapshot"`, or the series' own `index.name` where it names a coordinate of the attribute, a caller who built the series from a named index having said it already.
An index that says neither holds names.

Never inferred from the labels themselves: testing them against the axis would make one call mean different things in two records — a scenario labelled `wind1` would silently capture a series meant per component — and a partial overlap would pick a reading without saying so.
An unnamed index of timestamps is therefore read as names and fails the [member check](#validation), which is the loud version of the same mistake.

`entity=None` means every entity the record currently resolves, which is a read, so it includes earlier pending edits.

## An `nw.Expr` value — derived from the current one

```python
record.set("p_max_pu", nw.col("value") * 1.1)  # scale up every p_max_pu
record.set("p_max_pu", nw.col("value").clip(upper=0.9), entity=["wind1"])
```

A fifth `value` form rather than a second method.
Nothing else a caller passes is an `nw.Expr`, so the dispatch is unambiguous — unlike the series-versus-mapping tie, which [`set`](#set) has the caller settle rather than guessing at.

What it does differently is read before it stages:

- What it derives from is the resolved value **including earlier pending edits** ([reading with pending edits](#reading-with-pending-edits)), so two such calls compose.
- Where the other forms stage without touching parent data, this one must resolve the keys it targets first.
  On a layered record that is a fold, so a broad derived edit is the one edit whose cost scales with the ancestry rather than with the rows written.
- What is staged is the _result_, not the expression.
  So a committed layer holds ordinary rows, and nothing in the format records that a value was derived — replaying an edit sequence is not a thing the record supports.

The expression is evaluated by narwhals against the resolved long frame, so it names `value` rather than the attribute: the frame is long, and one attribute per call means the column is always `value`.

**A named target must resolve to a row.**
If the caller names `entity`, a relation key coordinate or any dim scope, every one of those targets must produce a row to derive from, or the call raises.
The caller asked for those rows to take a new value and there is nothing to compute one from, which is a failed change rather than a no-op — the same class of error as [naming a component no layer declares](#validation), and it was silently staging zero rows before.

With `entity=None` and no scope the instruction is "whatever resolves", so an empty result is an answer rather than a failure.
That asymmetry is the whole of the rule: a broad derived edit where only some entities hold a value is ordinary, while a targeted one that hits nothing is a typo.

## Answers

A solve's answers are stored as a record of their own, whose schema the producer defines.
That record is linked to the input record by the revision id of the input node ([tables by declared name](sources.md#answers)).

## Accessors — **not implemented**

`set` is the whole of the edit API.
This section is the intended spelling for an accessor over it, not something the package provides.

```python
record["Generator"]["p_nom"] = 150.0  # every generator
record["Generator"]["p_nom", ["wind1", "wind2"]] = [150.0, 80.0]
record["Generator"]["p_max_pu", "wind1"] = series
record["Link", "north"]["efficiency", "dc"] = 0.9  # a connection
record["Generator", {"scenario": "high"}]["p_max_pu", "wind1"] = 0.5
```

The component type in the subscript is a **scope**, not part of the key it writes: it selects the entities of that type from the `entity_type` [relation](schema.md#types), then `set` [addresses the names it produced](format.md#the-entity-axis).
So `record["Generator"]["p_nom"] = 150.0` is "every Generator", which `set("p_nom", 150.0)` alone cannot say.

Sugar with **no added capability** otherwise: `__setitem__` normalises its key into `(attribute, entity)` and its extra arguments into dims, then calls `set`.
Keeping the method as the protocol member and any accessor on top is deliberate — `set` is what an implementation provides and other code calls, so a spelling over it can change, or not exist, without touching an implementation.

It reads as well as writes, since a `WorkingRecord` is a `Record`: `record["Generator"]["p_nom"]` returns that type's resolved frame, so getter and setter are symmetric and the accessor is a component-type view rather than a write-only handle.
The read must be scoped by both the component type and the names — an accessor whose getter ignores either is not the view this describes.

It deliberately does not reproduce a dataframe library's full indexing grammar — no boolean masks, no slices — because a record is not a dataframe and a partial imitation invites the assumption that the rest works.
Omitting `entity` is how "all" is spelled.

## `add` / `remove`

```python
record.add("entity", frame)  # wide: entity, entity_type, attribute columns
record.remove("entity", ["old_coal"])
```

`add` takes a wide frame keyed by `entity` and splits it by the schema, per [where a value lives](format.md#where-a-value-lives):

- **An attribute over `entity` alone is a column of the entity axis.**
- **A varying attribute becomes `attributes/` rows.** A constant value of one is a broadcast row, its other dims NULL.
- **A relation keyed by `entity` gets a row where the frame carries every other column of it**, with any attribute over that relation. An `entity_type` column gives each entity its type; a `bus` column gives it a connection.

Which is which comes from the schema, so `add` needs no framework registry.
A column the schema does not name is **rejected**: a [staging table is shaped like the file it becomes](#staging), so there is no dtype to give such a column and no reader that would know what it means. A caller that grows a column declares it first, which [schema versioning](schema.md#versioning) accepts as a widening.

An `add` of a name the record already holds replaces its row on the entity axis, and its row of each relation the frame names, so the name keeps one row and one type.

It is **not** a sequence of `set` calls, even though the varying columns it stages take the same path a `set` would.
A component exists by virtue of its row on the entity axis, and `set` refuses a name no layer declares ([validation](#validation)).
Adding a bus with no attributes makes the point — nothing to `set`, yet the bus must exist.
Membership is not reducible to attribute values.

`remove(dim, labels)` stages a tombstone per label on that dim's axis.
`dim` may be any dim in the fold key (`Schema.partial_dims`): `entity`, a relation key coordinate such as `bus`, or a dim declared `partial`.
A dim outside the fold key is refused, with an error that names `partial`: a layer owns such a dim whole, so a tombstone has no key to remove. A dim `within` another is refused too.

It need not enumerate what it deletes: [the fold](layers.md#deletion) applies it to every attribute row and every relation row keyed on the label, so a removed component takes its connection rows and its `entity_type` row with it.
A tombstone on the [entity axis](format.md#the-entity-axis) has no dim scope: a component [exists or it does not](schema.md#existence-does-not-vary-along-a-dim).

## `add_relation` / `remove_relation`

```python
record.add_relation("connection", frame)  # the relation's own columns
record.remove_relation("connection", [("dc", "north")])
```

The one staging path every declared [relation](schema.md#relations) writes through. There is no `connect`/`disconnect` beside it: `connection` is one relation among however many a schema declares, and a call naming it would put one framework's vocabulary in the record layer.

`frame` carries the own columns of `relation` (`entity` and `bus` for `connection`, `from` and `to` for a `corridor`) plus whatever else the relation's file holds — an attribute [addressed by the relation](format.md#where-a-value-lives), such as PyPSA's `role`. `keys` is a tuple per row in the order of the relation's [key](schema.md#values-a-relation-that-classifies).

[`add`](#add-remove) calls `add_relation` too, but only for a relation whose **key** includes `entity` — the case where the row belongs to the component being added (`bus` for `connection`, `entity_type` for the type relation).
A relation like `corridor`, relating two entities neither of which is "the" one being added, has no such row to derive from a single component's wide frame and is staged through `add_relation` directly.

## Committing

```python
Target = NewChild | Directory
```

- **`NewChild(record=None)`** — create a child of `record` and write the staged rows as its layer.
  The patch-layer path: read a parent, edit, commit a child.
  [Any node may be a parent](layers.md#a-layers-data-is-write-once), so this needs no preparation of the one being branched from.

  `record` defaults to the node the `WorkingRecord` was built over, since branching from the thing you read is what a caller means every time; naming one is for re-parenting the edits elsewhere.
  A base that is no node in the tree — a directory, a framework object — has nothing to default to and must supply one.
  The layer lands in the **child**, never in the node branched from, so it is `commit`'s return value that reads the edits back.

- **`Directory(uri)`** — write a standalone record.
  What is staged _plus what the record already reads_, flattened into one layer.

The two write different things.
A `NewChild` writes **only the edits** — that is what a patch layer is, and the fold resolves the rest from the parent.
A `Directory` writes **the resolved result**, since there is no parent to resolve against.
Both go through [`write_record`](writing.md), which takes a [`LayerData`](record.md#layerdata): a `NewChild` hands it the staged layer's own source, a `Directory` the resolver that folds base and staged into one — the two objects a `WorkingRecord` already holds, one meaning "my layer's rows" and the other "everything folded to here", answering the same interface.
The writer cannot tell which it was handed, which is the point: "enumerate what I hold, hand each over" is one contract whether "what I hold" is a single layer or a whole fold.

An edited axis follows [`partial`](schema.md#partial-the-granularity-of-an-override), exactly as an attribute's rows do.
A `partial` axis is patched label by label: the layer holds the labels the edit touched, and the fold resolves the rest from the parent, last-writer-wins per [axis key](record.md#axis-order).
An axis **outside** `partial` is owned whole once touched, so the layer restates every label with the static attributes attached to them — one rule for what non-partial means, rather than an axis-shaped exception to it.
The fold would resolve the narrower form correctly, since it keys per label and an omitted one keeps its parent's row; what ownership buys is that a layer's axis file says what the axis _is_ there, rather than being readable only against its parent.
A `Directory` writes the resolved axis whole either way, and an axis nothing touched is written by neither.

Restating on edit is what an axis outside `partial` costs, and it is [the cheaper side of that trade](schema.md#partial-the-granularity-of-an-override) — which is the reason not to reach for `partial` when an axis merely gains an attribute.

An edit **replaces the rows it names** rather than appending beside them: it deletes the rows at the coordinate it writes and inserts the new ones, so a staging table holds one row per coordinate and no fold is needed to read it.
The key it replaces on is the coordinate — the same one a read would have collapsed — so the delete removes exactly what a last-writer-wins fold would have discarded.
An axis is the exception in mechanism, not in effect: an axis row's columns are independently editable, so a `set` there patches its one column in place (`UPDATE`) rather than replacing the row, which is what keeps a sibling attribute a different `set` wrote.

Per **coordinate**, not per ownership key: the ownership key excludes the dims an attribute is [not owned per](schema.md#partial-the-granularity-of-an-override), so replacing on it would drop a whole staged series to one row — two edits at different snapshots are two coordinates, not two writes to the same place.
The same distinction governs [the read overlay](#reading-with-pending-edits) and the restate below, and it is the one thing easy to get wrong here.

Three interactions need stating, because each is where replacing by coordinate alone is not the whole story:

- **`remove` after `set`** on the same component: the tombstone wins, since a deleted component has no attributes.
  A component tombstone reaches another file's rows, so it stays an anti-join at read and commit rather than a replace — the attribute rows are keyed by coordinate, the tombstone by name.
- **`add` after `remove`** of the same name: the component exists again.
  The entity axis replaces on `entity` alone, so the `add` row displaces the tombstone by construction — no axis row and tombstone both.
- **`set` on a component this record also added**: correct as-is, since the two live in different files.

The [non-`partial` rule](schema.md#partial-the-granularity-of-an-override) is the subtle one.
Overwriting one value along a non-partial axis means the layer must carry that component's _whole_ extent along it, so such a `set` reads the resolved series for that key and stages the untouched coordinates alongside the edit.
That is the one read of parent data an edit makes, and it happens as the rows are staged rather than at commit: the staging table then already holds what the layer will write, so commit collapses and writes it without a completion step of its own.

The scope is the key the edit named, never the attribute: a component no edit mentioned keeps its rows in the parent, and a layer carrying them would claim an extent it was never given.
A staged row that leaves the axis NULL is the exception, since [the broadcast rule](record.md#the-broadcast-rule) already makes it cover every label — there is nothing left to carry, and a carried row beside it would overlap.
Carried rows go in only where no edit already holds the coordinate, so a later `set` on a carried coordinate replaces the fill rather than tying with it — the anti-join that keeps a fill off an occupied coordinate is the whole of what orders the two.

## Validation

[`write_record`](writing.md) validates structurally, so commit inherits that.
What editing adds is edit-level: an `add` whose frame lacks `entity` or carries an undeclared column, a `set` naming a component the record does not resolve, a `set` of an attribute not declared over the dims it names, a dim keyword the schema does not declare.
These are caught when the edit is **staged**, not at commit — a caller should learn about a typo'd attribute at the line that typed it, not fifty edits later.

A `set` refuses a name that is not on the entity axis, and the error says to `add` it first.

## Staging

Staged rows live in DuckDB tables on the record's own connection:

```sql
CREATE TABLE staged_attributes_<attr>_<id>   (<that attribute's long columns>);
CREATE TABLE staged_axis_<dim>_<id>          (<the axis key>, ..., deleted BOOLEAN);
CREATE TABLE staged_<relation>_<id>          (<relation columns>, ..., deleted BOOLEAN);
```

**Every table is shaped like the file it becomes**, which is the rule the rest of this section is consequences of. So there is no table whose columns are a union over things the format keeps apart, and a column the schema does not declare has nowhere to go — [`add`](#add-remove) rejects one rather than widening a table to fit it, there being no declared dtype to give it.

**One table per staged attribute**, because that is the file it becomes: its columns are [the attribute's own coordinates](format.md#the-long-schema) and `value` has the attribute's declared type.
A shared table would have to widen `value` to text and carry every declared dim, which costs twice: the value needs casting back on the way out, and a NULL in a dim column becomes ambiguous between "this attribute has no such axis" and [the broadcast rule](record.md#the-broadcast-rule)'s "every value of it".
Per attribute both questions are answered by the table's shape, so neither is asked.

One staging table per declared [relation](schema.md#relations), mirroring [the maps the fold builds](read-path.md#owner-map): `connection` is one instance, so a record declaring a second relation stages it through the same path rather than a second method.

The **entity axis is staged as an axis**, `staged_axis_entity_<id>` like any other dim, and reaches [the fold](read-path.md) as `axis("entity")` with no special case. It holds membership, tombstones and the columns of attributes over `entity` alone, as [`dims/entity.parquet`](format.md#the-entity-axis) does.
What differs is only how an edit keys it: an ordinary axis patches a column in place, so two `set` calls on one label commute; `add` and `remove` replace on `entity` alone, so a `remove` followed by an `add` of one name resolves to one row.
An entity's type is staged in the table of the `entity_type` relation, which replaces on `entity` too.

These tables are the **only** place a staged row exists: [the reads](#reading-with-pending-edits) read them rather than holding a copy.

DuckDB rather than in-memory objects, for three reasons that all matter: the reads are already a fold, so staging elsewhere would mean marshalling every edit into a DuckDB relation on every read; a large edit is a bulk insert rather than ten thousand Python objects; and commit hands each table to `write_record` as the file it already is, no collapse in between.

Connection-scoped, like the owner-map cache, so they vanish with the connection and never appear on disk.
A record whose edits must survive a process boundary should commit.

An edit **replaces** what it names rather than appending, so a table holds one row per key and reading it is a scan — no ordering column, and edit order is just which edit ran last.

## Reading with pending edits

The inherited `Record` members must reflect the edits; otherwise `set` then read gives the old value, which no caller would expect.

A set of pending edits **is** a layer — an unwritten one.
So the reads compose the same way: the staged rows are the last layer, resolved over whatever the record was reading before.

```text
resolved = fold(parent layers..., staged rows)
```

This is exactly one more fold step over the same [owner-map machinery](read-path.md#owner-map), with the staging tables standing in for a layer directory — over a layered base or a plain directory alike, a directory being a layer laid out like any other.
It costs what one more layer costs, **per read**: a written layer is folded once and cached forever, and the staged one cannot be, being the only layer that can still change.
So the fold is materialised up to the last layer that cannot change under the reader, and the staged step on top of it stays a DuckDB relation — which is also why an edit needs no invalidation, there being nothing cached to invalidate.

`flags` follows for free, computed in the fold's own ownership `GROUP BY` as it is for any layer: a staged row setting a dim adds it to `varies`, one leaving it NULL adds it to `broadcast`, and a staged curve sets `breakpoints`.
It says nothing about an attribute addressed by one axis alone, which has no rows in the owner map; `dims` is where that value is read from, staged edits included, and [`Schema.attributes_on`](format.md#where-a-value-lives) is what names the columns an axis frame carries.

`dims` overlays per column rather than per row: a staged label's edited columns win, and a label the edit did not name keeps the base's whole row.
So two `set` calls for two attributes on one axis compose instead of the later one blanking the earlier's column.
