<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Open questions

This page lists what the design of datarecord leaves open, and below it the questions it has settled.

## Open

### Whether `partial` should ever be per attribute

`partial` sits on the dim because it holds for every attribute over that dim ([layers](layers.md)). A counter-example would be an attribute whose series a reader can take in pieces while others cannot, and none is known. Allowing it would make the fold's key vary per attribute, where the owner map has one fixed key.

### Whether results should be invalidated by a later input edit

A result is an attribute and is inherited, so a child that edits an input keeps results computed from its parent's inputs ([the data model](data-model.md)). Dropping them on the next input edit was rejected, because it discards work the caller may want. A record that guesses which to keep is worse than one that keeps both. Coherence is the caller's. If this causes trouble in practice, a warning is the cheap next step.

### Detecting intermediate results that no longer fit their inputs

A results layer of the PyPSA tool replaces the SubNetworks of its base. A child that edits the topology keeps its parent's until the next solve ([tools](../reference/tools.md)). Marking them stale needs a record of which inputs each came from.

### Naming a label by a property's value as the dim's own keyword

A multi-period record labels its snapshots by position, and `select` matches `snapshot=` against those labels ([selecting](../reference/selecting.md)). Taking a timestamp there, matched against `snapshot.timestep`, needs a rule for when a keyword reads a property rather than the label.

### Whether a multi-dim left with one coordinate reads as that dim

Squeezing a coordinate's dim projects a multi-dim's tuples on the others ([selecting](../reference/selecting.md)). A multi-dim left with one coordinate stays a struct of one field, rather than reading as the dim it draws on.

### Editing through a selection

A `WorkingRecord` over a `Selection` would stage at the base's coordinates through `lift` ([selecting](../reference/selecting.md)). Today an edit goes to the base.

### A selection that follows a `WorkingRecord`'s edits

A selection resolves the axes it narrows when you call `select`, so a label a `WorkingRecord` adds later is outside them ([selecting](../reference/selecting.md)). Following the edits, and registering a selection as views, needs the narrowing as a plan rather than as labels.

### Whether a `WorkingRecord` stages against a snapshot of its base

A `WorkingRecord` may sit over another `WorkingRecord`, and it reads that base's later edits. Staging against a snapshot taken at construction is simpler and arguably more correct, since an edit sequence should not see another writer's changes mid-flight. The cost is a record that can go stale.

### What else a `Record` should answer about its labels without handing over a frame

`flags` sets the shape: cheap derived metadata a reader plans with before opening a file, per attribute ([the record](../reference/record.md)). `entity -> ctype` is a column of the resolved axis, one frame across all categories ([the format](../reference/format.md)). "Which categories have live labels" and "how many labels a category has" are still frames to collect. The open part is the granularity, since a protocol that grows one method per question is worse than the frames it replaces. Any answer has to come from the fold, where a record's labels are settled.

### Whether a property referring to a dim without `one_of` may categorise a `partial` dim

`entity.carrier` refers to `carrier`, and a `default_by` keyed on it would make a vocabulary of data: a category that a layer adds and removes. Today a category is a label of a dim with `one_of` ([the schema](../reference/schema.md)).

### Whether a dim with `one_of` may grow in a layer

Its axis is the schema's list, so a custom PyPSA component type is a schema change and not a data edit ([the schema](../reference/schema.md)). That is the cost of checking a category at load. The alternative is an axis that a layer writes and the list bounds. A tool or layer could then choose which labels exist, and the dim could have properties. It costs a file that restates the schema and a category that may match no live label ([DEP 24](../development/proposals/dep-0024-categories-are-enum-dimensions.md)).

### Whether a multi-dim may be `over` another multi-dim

Each coordinate of a multi-dim draws on a dim that is not a multi-dim, and datarecord refuses one that does ([the schema](../reference/schema.md)). Nothing needs a tuple of tuples yet, and the refusal stands until something does.

### Whether a functional mapping over two coordinates justifies a dim

A zone per plant and period is a multi-dim `plant_period` with the property `zone` ([the data model](../reference/data-model.md)). The multi-dim exists only for `zone`, and you add its tuples before you can set the property.

### How the published package depends on mathspec

Core reads its dtypes from `mathspec.program`. The environment pins mathspec at a commit of a pull request branch, which lasts only while the commit exists on GitHub. Before a release the pin moves to the merged commit. A PyPI release cannot declare a git pin as a dependency, so the package metadata names none until mathspec is published.

### How a property that refers to a dim maps onto mathspec

mathspec spells it as a relation, `carrier_of: { key: generator, values: carrier }`, declared beside the dims. datarecord declares `one_of: carrier` on a property of `generator`. Unifying the two is left for later.

### Whether a default must name a live label

`bus.carrier` defaults to `AC` in PyPSA's schema, and a default is never stored, so nothing checks that `AC` is a label of `carrier` ([the schema](../reference/schema.md)). A selection applies the default when it drops labels. A bus whose default carrier the selection drops goes, and one whose default is on no axis stays ([selecting](../reference/selecting.md)).

### Whether `bus` earns its place in the fold key

PyPSA's `partial` includes `bus` so that a layer can patch one bus's `v_nom`, and every read pays for every `partial` dim ([layers](layers.md)). Measuring it on a record the size of PyPSA-Eur would settle it.

### Whether a custom bus column can be a property

It is an attribute over `bus` today. A custom `Bus.country` beside a custom `Generator.country` is then one attribute over two dims, which the PyPSA tool refuses ([tools](../reference/tools.md)). Placing it as `bus.country` would remove the last custom-column clash, but needs a rule that tells a custom property from a custom attribute.

### Whether selector keywords need a qualified form

`set` refuses a bare selector keyword that names a property of two dims one attribute reaches ([editing](../reference/editing.md)). The only way to say which is meant is to set per dim.

### Whether `Schema` should declare a dim with its mapping property in one call

A snapshot mapped to its period is a dim plus a property that refers to the period. A helper could make the two declarations one ([the schema](../reference/schema.md)).

### Whether a `WorkingRecord` may delete one value along a dim outside `partial`

A layer that restates a key along such a dim and leaves out a label its parent had there deletes that value ([layers](../reference/layers.md)). A `WorkingRecord` stages the label's whole extent, so today only a hand-written layer can.

### Whether a layer may remove a dim from `partial`

Schema versioning refuses it ([the schema](../reference/schema.md)). To allow it, the layer would restate as resolved each key of its `partial` where a parent's map row sets a label of the dim. The fold would then project the dim out of the parent's map, dropping every row that set it. It would refuse a layer that leaves one uncovered ([resolution](../reference/resolution.md)). `compatible_with` would accept the smaller `partial`, and a helper would build such a layer from a revision.

### What PyPSA#1474 lands as

Upstream making buses and carriers indices may turn an unlisted carrier into an error, or stop `""` meaning none. The PyPSA tool adds the first and stores the second as NULL today ([tools](../reference/tools.md)).

### Whether the checks at `set` and at write can take the fold's fixed plan

The fold carries the set of dims a row sets as data, so its joins do not depend on the rows ([resolution](../reference/resolution.md)). `set` and the writer refuse two crossing rows by looping over the pairs of such sets, fetching once per pair ([editing](../reference/editing.md)).

### Whether `set` can stage a scoped or crossing broadcast without spreading it

`set` stages a broadcast scoped to a scenario, or one that crosses another row, as one row per entity and snapshot it reaches ([editing](../reference/editing.md)). A large edit then stages rows in proportion to the labels it reaches, not to the rows it sets.

### When a deep ancestry materialises its map

A cold fold in a fresh workspace grows linearly with depth, about 0.1 s per layer ([resolution](../reference/resolution.md)). Materialising every N layers, or at a branch point, would bound it, and no policy is chosen.

### Whether an edit may join a caller's open transaction

An edit runs in its own transaction, and DuckDB nests none. datarecord therefore refuses an edit while the caller has a transaction open on the same cursor ([editing](../reference/editing.md)). Joining the caller's would lose the edit's own rollback on refusal.

### What a schema change on a `WorkingRecord` does to its registered views

A `WorkingRecord` has no method that changes its schema, which changes only at commit. Such a method would refuse a change that alters a column or the fold of a registered view, or replace the views it reaches. A client's query would then see them change ([the format](../reference/format.md)). Widening an attribute's `dims`, adding a property and adding a dim to `partial` are such changes. Adding an attribute or a dim would add its view ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### How a SQL client settles rows

Registered views are raw, so a client applies the broadcast rule itself ([the format](../reference/format.md)). `settle` ported to the frontend, and a page on the raw format for SQL clients, are still to write ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### Who drops an abandoned registration

A server that crashes or forgets leaves its views and the folds they read. The `registered` table lists every registration with its `created` time, but nothing unregisters by any rule yet ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)). Unregistering frees the staging tables the registration reads, so the rule also decides how long they stay ([the store](../reference/store.md)). A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which no record reads. Those that no registration's `held` column lists could be swept, and nothing does so yet ([DEP 35](../development/proposals/dep-0035-a-record-holds-the-tables-it-reads.md)).

### A public `create_view` taking a qualified name

`register` calls DuckDB's private `_render` and `_create_view`, because `DuckDBPyRelation.create_view` takes one identifier and no nested path ([storage](storage.md)). That caps DuckDB at the newest release the tests passed on until a public method takes a qualified name, which is to be requested upstream.

### A reader seeing part of a large committed transaction

DuckDB 2.0 can show a reader on another cursor part of a committed transaction of about 300,000 rows or more. A reader sees such a part in 0.1 to 1% of reads. No edit can rule it out until DuckDB does, so a reader sees each edit whole only where the edit is smaller ([the format](../reference/format.md)). It is to be reported upstream ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### Who pays the cold fold of a registered revision

`register`, or the first read of a view, pays the cold fold of a deep revision, once per lineage in a workspace ([resolution](../reference/resolution.md)). It grows linearly with depth until the revision's map is [materialised](#when-a-deep-ancestry-materialises-its-map).

## Settled

### Filtering a record before `build`

`select(record, period=2030)` returns a selection of the record at those labels, which any reader reads as a `RecordLike` ([selecting](../reference/selecting.md)). One label squeezes its dim, and what refers to a dropped label goes with it.

### What a pointwise `set` leaving one of two dims outside `partial` NULL reaches

Every label of the dim it leaves NULL, at the label it names. The rest of the series keeps the parent's values, whether staged, committed or over a materialised parent ([resolution](../reference/resolution.md)).

### Whether a broadcast survives materialisation

It does. The owner map contains keys as written and the fold reads no axis. A NULL therefore reaches every label the axis has at the revision read. That includes a label added below a materialised revision or staged in a `WorkingRecord` ([layers](layers.md)). A map materialised before `partial` widened is extended to the wider key, as a map folded live is ([resolution](../reference/resolution.md)).

### Whether `flags` needs a record-level counterpart

It is one. `flags()` answers per record, every attribute included, and `flags(**categories)` per category, restricted to the rows that reach its labels ([the record](../reference/record.md)). An attribute over an axis alone, or over a multi-dim outside `partial`, is reachable through the first.

### What a NULL multi-dim struct reaches

Every tuple, as a NULL on a dim reaches every label ([the data model](../reference/data-model.md)). A constant `efficiency` lands on generators' connections too, and one for links alone is `set("efficiency", 0.7, ctype="Link")`.

### What `build` does with a value its type does not take

It leaves the value out and reports it in `Requirements.ignored`, as it reports an unknown type, and does not raise ([tools](../reference/tools.md)).

### Whether a multi-dim in `partial` may have categories

It may, by the rule a dim follows. `connection.role: {one_of: role}` categorises `connection` in `partial`, so a `default_by` keyed `connection.role`, `flags(role=...)` and the keyword `role=` work as they do for `entity.ctype` ([the data model](data-model.md)).

### What a connection value staged without its `bus` should mean

Nothing: `set` refuses the call ([editing](../reference/editing.md)). A multi-dim value is a whole tuple or NULL, so `{entity: dc, bus: NULL}` is no value, and datarecord refuses it wherever a row comes in. The alternative, "every connection of this entity", would broadcast over part of a tuple, where a multi-dim's column broadcasts only whole.

### Whether a record's relations can be named views

They can, in the workspace's own database, one schema per registration with a nested schema per kind ([storage](storage.md)). Over a `WorkingRecord` the views are created once and follow every edit, since a relation's plan never reads a staged row.

### When a server drops the folds it opened

When the last open record that reads a fold is closed, unless a registration still reads it. A record collected unclosed leaves its folds to the workspace for the workspace's life, so a server closes each record it opens ([the store](../reference/store.md)).

### When a `WorkingRecord`'s staging tables are dropped

When it is closed or collected, unless a registration of it still reads them, and then on that registration's `unregister`. Its tables sit in a schema of their own, which the workspace drops whole ([the store](../reference/store.md)).
