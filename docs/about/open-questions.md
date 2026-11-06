<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Open questions

This page lists what the design of datarecord leaves open, and below it the questions it has settled.

## Open

### Should `partial` ever be per attribute?

`partial` sits on the dim because it applies to every attribute over that dim ([layers](layers.md)). A counter-example would be an attribute whose series a reader can take in pieces while others cannot, and none is known. Allowing it would make the fold's key vary per attribute, where the owner map has one fixed key.

### Should a later input edit invalidate results?

A result is an attribute and is inherited, so a child that edits an input keeps results computed from its parent's inputs ([the data model](data-model.md)). Dropping them on the next input edit was rejected, because it discards work the caller may want. datarecord does not guess which results to keep, so it keeps both. You decide whether a record's results still fit its inputs. If this causes trouble in practice, the next step is a warning, which costs little to add.

### How can datarecord detect intermediate results that no longer fit their inputs?

A results layer of the PyPSA tool replaces the SubNetworks of its base. A child that edits the topology keeps its parent's until the next solve ([tools](../reference/tools.md)). Marking them stale needs a list of the inputs that each came from.

### Can a property's value name a label as the dim's own keyword?

A record may label a dim by position and keep what each label means as a property, such as a snapshot's timestamp as `snapshot.timestep`. `select` matches `snapshot=` against the positions ([selecting](../reference/selecting.md)). Taking a timestamp there needs a rule for when `select` matches a keyword against a property rather than the label. PyPSA's multi-period snapshots need no such rule, because they are a [multi-dim](../reference/data-model.md#multi-dims) whose `timestep` is a coordinate keyword.

### Does a multi-dim left with one coordinate read as that dim?

Squeezing a coordinate's dim projects a multi-dim's tuples on the others ([selecting](../reference/selecting.md)). A multi-dim left with one coordinate stays a struct of one field, rather than reading as the dim it draws on.

### Can a `WorkingRecord` edit through a selection?

A `WorkingRecord` over a `Selection` would stage at the base's coordinates through `lift` ([selecting](../reference/selecting.md)). Today an edit goes to the base.

### Can a selection follow a `WorkingRecord`'s edits?

`select` resolves the axes it narrows when you call it, so a label a `WorkingRecord` adds later is outside them ([selecting](../reference/selecting.md)). Following the edits, and registering a selection as views, needs the narrowing as a plan rather than as labels.

### Should a `WorkingRecord` stage against a snapshot of its base?

A `WorkingRecord` may sit over another `WorkingRecord`, and its reads include that base's later edits. Staging against a snapshot taken at construction is simpler and arguably more correct. A sequence of edits should not pick up another writer's changes mid-way. The cost is a record that can go stale.

### What else should a `Record` answer about its labels without handing over a frame?

`flags` is the model for this: cheap derived metadata a reader plans with before opening a file, per attribute ([the record](../reference/record.md)). `entity -> ctype` is a column of the resolved axis, one frame across all categories ([the format](../reference/format.md)). "Which categories have live labels" and "how many labels a category has" are still frames to collect. The open part is how much one method should answer. `flags` answers every attribute in one call, and one method per question would add more to `RecordLike` than collecting the frames costs a reader. Any answer has to come from the fold, where a record's labels are settled.

### May a property referring to a dim without `one_of` categorise a `partial` dim?

`entity.carrier` refers to `carrier`, and a `default_by` keyed on it would make a vocabulary of data: a category that a layer adds and removes. Today a category is a label of a dim with `one_of` ([the schema](../reference/schema.md)).

### May a dim with `one_of` grow in a layer?

Its axis is the schema's list, so a custom PyPSA component type is a schema change and not a data edit ([the schema](../reference/schema.md)). That is the cost of checking a category at load. The alternative is an axis that a layer writes and the list bounds. A tool or a layer could then set which labels exist, and the dim could have properties. It costs a file that restates the schema and a category that may match no live label ([DEP 24](../development/proposals/dep-0024-categories-are-enum-dimensions.md)).

### May a multi-dim be `over` another multi-dim?

Each coordinate of a multi-dim draws on a dim that is not a multi-dim, and datarecord refuses one that does ([the schema](../reference/schema.md)). Nothing needs a tuple of tuples yet, and the refusal stands until something does.

### Does a functional mapping over two coordinates justify a dim?

A zone per plant and period is a multi-dim `plant_period` with the property `zone` ([the data model](../reference/data-model.md)). The multi-dim exists only for `zone`, and you add its tuples before you can set the property.

### How does the published package depend on mathspec?

Core reads its dtypes from `mathspec.program`. The environment pins mathspec at a commit of a pull request branch, which lasts only while the commit exists on GitHub. Before a release the pin moves to the merged commit. `pyproject.toml` declares that pin as a direct git dependency, which PyPI refuses in an upload, so a PyPI release waits until mathspec is published.

### How does a property that refers to a dim map onto mathspec?

mathspec spells it as a relation, `carrier_of: { key: generator, values: carrier }`, declared beside the dims. datarecord declares `one_of: carrier` on a property of `generator`. Unifying the two is left for later.

### Must a default name a live label?

`bus.carrier` defaults to `AC` in PyPSA's schema. datarecord never stores a default, so it does not check that `AC` is a label of `carrier` ([the schema](../reference/schema.md)). A selection applies the default when it drops labels. `select` drops a bus whose default carrier it drops, and keeps one whose default is on no axis ([selecting](../reference/selecting.md)).

### Does `bus` earn its place in the fold key?

PyPSA's `partial` includes `bus` so that a layer can patch one bus's `v_nom`, and every read scans every `partial` dim in the fold key ([layers](layers.md)). Measuring it on a record the size of PyPSA-Eur would settle it.

### May `partial` name a coordinate of a multi-dim?

`partial` may name a multi-dim whole. A layer overrides an attribute one value per label along each dim in both its `dims` and `partial` ([layers](../reference/layers.md#partial)). So `period` in `partial` does not apply to a series over `snapshot`, the multi-dim over `(period, timestep)`. A per-period, myopic re-solve then restates the whole series, every other period's values included ([tools](../reference/tools.md#writing-results)). With a coordinate in `partial`, `p` would be keyed by `(attribute, connection, period)`, and the re-solve would restate its own period's series alone. The cost is a wider fold key on every read of every attribute over `snapshot`.

### Do selector keywords need a qualified form?

`set` refuses a bare selector keyword that is the name of a property of two dims that one attribute is indexed over ([editing](../reference/editing.md)). The only way to say which is meant is to set per dim.

### Should `Schema` declare a dim with its mapping property in one call?

A snapshot mapped to its period is a dim plus a property that refers to the period. A helper could make the two declarations one ([the schema](../reference/schema.md)).

### May a `WorkingRecord` delete one value along a dim outside `partial`?

A layer that restates a key along such a dim and leaves out a label its parent had there deletes that value ([layers](../reference/layers.md)). A `WorkingRecord` stages the label's whole extent, so today only a hand-written layer can.

### May a layer remove a dim from `partial`?

Schema versioning refuses it ([the schema](../reference/schema.md)). To allow it, the layer would restate as resolved each key of its `partial` where a parent's map row sets a label of the dim. The fold would then project the dim out of the parent's map, dropping every row that set it. It would refuse a layer that leaves one uncovered ([resolution](../reference/resolution.md)). `compatible_with` would accept the smaller `partial`, and a helper would build such a layer from a revision.

### What does PyPSA#1474 land as?

Upstream making buses and carriers indices may turn an unlisted carrier into an error, or stop `""` meaning none. The PyPSA tool adds the first and stores the second as NULL today ([tools](../reference/tools.md)).

### Can the checks at `set` and at write take the fold's fixed plan?

The fold carries the set of dims a row sets as data, so its joins do not depend on the rows ([resolution](../reference/resolution.md)). `set` and the writer refuse two crossing rows by looping over the pairs of such sets, fetching once per pair ([editing](../reference/editing.md)).

### Can `set` stage a scoped or crossing broadcast without spreading it?

`set` stages a broadcast scoped to a scenario, or one that crosses another row, as one row per entity and snapshot it applies to ([editing](../reference/editing.md)). A large edit then stages rows in proportion to the labels it applies to, not to the rows it sets.

### When does a deep ancestry materialise its map?

A cold fold in a fresh workspace grows linearly with depth, about 0.1 s per layer ([resolution](../reference/resolution.md)). Materialising every N layers, or at a branch point, would bound it, and no policy is chosen.

### May an edit join a caller's open transaction?

An edit runs in its own transaction, and DuckDB nests none. datarecord therefore refuses an edit while the caller has a transaction open on the same cursor ([editing](../reference/editing.md)). Joining the caller's would lose the edit's own rollback on refusal.

### What does a schema change on a `WorkingRecord` do to its registered views?

A `WorkingRecord` has no method that changes its schema, which changes only at commit. Such a method would refuse a change that alters a column or the fold of a registered view, or replace the views it affects. A client would then see them change ([the format](../reference/format.md)). Widening an attribute's `dims`, adding a property and adding a dim to `partial` are such changes. Adding an attribute or a dim would add its view ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### How does a SQL client settle rows?

Registered views are raw, so a client applies the broadcast rule itself ([the format](../reference/format.md)). `settle` ported to the frontend, and a page on the raw format for SQL clients, are still to write ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### Who drops an abandoned registration?

A server that crashes or forgets leaves its views and the folds they read. The `registered` table lists every registration with its `created` time, but datarecord does not yet unregister one by any rule ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)). Unregistering frees the staging tables that the registration uses, so the rule also sets how long they stay ([the store](../reference/store.md)). A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which no record uses. Those that no registration's `held` column lists could be swept, but datarecord does not sweep them yet ([DEP 35](../development/proposals/dep-0035-a-record-holds-the-tables-it-reads.md)).

### Will DuckDB's `create_view` take a qualified name?

`register` calls DuckDB's private `_render` and `_create_view`, because `DuckDBPyRelation.create_view` takes one identifier and no nested path ([storage](storage.md)). That caps DuckDB at the newest release the tests passed on until a public method takes a qualified name, which is to be requested upstream.

### When will DuckDB stop a read seeing part of a committed transaction?

DuckDB 2.0 can show a reader on another cursor part of a committed transaction of about 300,000 rows or more. A reader sees such a part in 0.1 to 1% of reads. No edit can rule it out until DuckDB does, so a reader sees each edit whole only where the edit is smaller ([the format](../reference/format.md)). It is to be reported upstream ([DEP 34](../development/proposals/dep-0034-a-record-registers-as-views.md)).

### Who pays the cold fold of a registered revision?

`register`, or the first read of a view, runs the cold fold of a deep revision, once per lineage in a workspace ([resolution](../reference/resolution.md)). It grows linearly with depth until the revision's map is [materialised](#when-does-a-deep-ancestry-materialise-its-map).

## Settled

### Can a record be filtered before `build`?

`select(record, period=2030)` returns a selection of the record at those labels, which any reader reads as a `RecordLike` ([selecting](../reference/selecting.md)). One label squeezes its dim, and what refers to a dropped label goes with it.

### What does a pointwise `set` cover when it leaves one of two dims outside `partial` NULL?

Every label of the dim it leaves NULL, at the label it names. The rest of the series keeps the parent's values, whether staged, committed or over a materialised parent ([resolution](../reference/resolution.md)).

### Does a broadcast survive materialisation?

It does. The owner map contains keys as written and the fold reads no axis. A NULL therefore means every label the axis has at the revision you read. That includes a label added below a materialised revision or staged in a `WorkingRecord` ([layers](layers.md)). A map materialised before `partial` widened is extended to the wider key, as a map folded live is ([resolution](../reference/resolution.md)).

### Does `flags` need a record-level counterpart?

It is one. `flags()` returns flags per record, every attribute included, and `flags(**categories)` per category, restricted to the rows that apply to its labels ([the record](../reference/record.md)). An attribute over an axis alone, or over a multi-dim outside `partial`, appears only in the first.

### What does a NULL multi-dim struct cover?

Every tuple, as a NULL on a dim means every label ([the data model](../reference/data-model.md)). A constant `efficiency` applies to generators' connections too, and one for links alone is `set("efficiency", 0.7, ctype="Link")`.

### What does `build` do with a value its type does not take?

It leaves the value out and reports it in `Requirements.ignored`, as it reports an unknown type, and does not raise ([tools](../reference/tools.md)).

### May a multi-dim in `partial` have categories?

It may, by the rule a dim follows. `connection.role: {one_of: role}` categorises `connection` in `partial`, so a `default_by` keyed `connection.role`, `flags(role=...)` and the keyword `role=` work as they do for `entity.ctype` ([the data model](data-model.md)).

### What should a connection value staged without its `bus` mean?

Nothing: `set` refuses the call ([editing](../reference/editing.md)). A multi-dim value is a whole tuple or NULL, so `{entity: dc, bus: NULL}` is no value, and datarecord refuses it wherever a row comes in. The alternative, "every connection of this entity", would broadcast over part of a tuple, where a multi-dim's column broadcasts only whole.

### Can a record's relations be named views?

They can, in the workspace's own database, one schema per registration with a nested schema per kind ([storage](storage.md)). Over a `WorkingRecord` the views are created once and follow every edit, since a relation's plan never reads a staged row.

### When does a server drop the folds it opened?

When the last open record that reads a fold is closed, unless a registration still reads it. A record collected unclosed leaves its folds to the workspace for the workspace's life, so a server closes each record it opens ([the store](../reference/store.md)).

### When are a `WorkingRecord`'s staging tables dropped?

When it is closed or collected, unless a registration of it still reads them, and then on that registration's `unregister`. Its tables sit in a schema of their own, which the workspace drops whole ([the store](../reference/store.md)).
