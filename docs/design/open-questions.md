<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Open questions

## Open

### Whether `partial` should ever be per attribute

[The schema](layers/tree.md#partial-the-granularity-of-an-override) puts it on the axis because it is true of every attribute varying over that axis.
A counter-example would be an attribute whose series a consumer _can_ accept in pieces while others cannot. None is known. Permitting it would make the fold's key vary per attribute, and the owner-map key is fixed.

### Whether results should be invalidated by a later input edit

A result is [an attribute](model/attributes.md) and inherits, so a child that edits an input holds results computed from its parent's inputs, describing a record that no longer exists.
Dropping them on the next input edit was considered and rejected: it silently discards work the caller may have wanted, and a record that guesses which of the two the caller meant to keep is worse than one that keeps both and says so.
Coherence is the caller's business.
If this bites in practice, a `pending`-level warning is the cheap next step.

### Detecting intermediate results that no longer fit their inputs

A [results layer](tools/pypsa.md#writing-results) replaces the SubNetworks of its base, but a child editing the topology keeps its parent's until the next solve. Telling them stale needs provenance of which inputs each came from.

### Naming a label by a property's value as the dim's own keyword

A record may label a dim by position and carry what a label means as a property, such as a snapshot's timestamp as `snapshot.timestep`, and [`select`](use/selecting.md#keywords-and-values) matches `snapshot=` against the positions. Taking a timestamp there would need a rule for when a keyword reads a property rather than the label. PyPSA's multi-period snapshots need none: they are a [multi-dim](model/multi-dims.md) whose `timestep` is a coordinate keyword.

### Whether a multi-dim left with one coordinate reads as that dim

[Squeezing](use/selecting.md#squeezing-a-dim) a coordinate's dim projects a multi-dim's tuples on the others. One left with a single coordinate stays a struct of one field rather than reading as the dim it draws on.

### Editing through a selection

A `WorkingRecord` over a `Selection` would stage at the base's coordinates through [`lift`](use/selecting.md#lifting-rows-back-to-the-base). Today an edit goes to the base.

### A selection that follows a `WorkingRecord`'s edits

A selection resolves the axes it narrows at the call ([laziness](use/selecting.md#laziness-and-layers)), so a label a `WorkingRecord` adds later is outside them. Following the edits, and registering a selection as DuckDB [views](storage/views.md), would need the narrowing as a plan rather than as labels.

### Whether a `WorkingRecord` over an open record stages against a snapshot

Writing into an open record invalidates its owner-map cache.
A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
The second is simpler and arguably more correct, since an edit sequence should not see another writer's changes mid-flight. It means a record can go stale.

### What else a `Record` should answer about its labels without handing over a frame

[`flags`](use/reading.md#flags) sets the shape: cheap derived metadata a consumer plans against before opening a file. It answers only per attribute.
`entity -> ctype` is a column of the [resolved axis](storage/format.md#axis-files), so it is one frame across all categories. "Which categories have live labels" and "how many labels a category has" are still frames to collect.
What is open is the granularity: those are the same kind of question, and a protocol growing one method per question is worse than the frames it replaces.
Whatever is chosen, it has to be answerable off the fold, which is where a record's membership already lives.

### Whether a property referring to a dim without `one_of` may categorize a `partial` dim

`entity.carrier` refers to `carrier`, and a `default_by` keyed on it would make a vocabulary that is data: a category that a layer adds and removes. Today a category is a label of [a dim with `one_of`](model/properties.md#categories).

### Whether a dim with `one_of` may grow in a layer

[Its axis is the schema's list](model/dims.md#a-dim-with-one_of), so a custom PyPSA component type is a schema change, not a data edit. That is the cost of checking a category at load. The alternative, the axis a layer writes and the list bounds, lets a tool or layer choose which of its labels exist and lets the dim carry properties, at the cost of a file restating the schema and a category that may name no live label ([DEP 24](proposals/dep-0024-categories-are-enum-dimensions.md#open-issues)).

### Whether a multi-dim may be `over` another multi-dim

Each coordinate of [a multi-dim](model/multi-dims.md) draws on a dim that is no multi-dim, and the schema refuses one that does. Nothing needs a tuple of tuples yet; the refusal stands until something does.

### Whether a functional mapping over two coordinates justifies a dim

A zone per plant and period is [a multi-dim](model/multi-dims.md#one-struct-column) `plant_period` with the property `zone`. The multi-dim exists only to carry `zone`, and its tuples must be added before the property can be set.

### How the published package depends on mathspec

Core reads its dtypes from `mathspec.program`, and the environment pins mathspec at a commit of a pull request branch, which lasts only while the commit exists on GitHub. Before a release the pin moves to the merged commit. A PyPI release cannot declare a git pin as a dependency, so the package metadata names none until mathspec is published.

### How a property that refers to a dim maps onto mathspec

mathspec spells it as a mathspec relation, `carrier_of: { key: generator, values: carrier }`, a declaration of its own beside the dims. datarecord declares `one_of: carrier` on a property of `generator`. Unifying the two is left for later.

### Whether a default must name a live label

`bus.carrier` defaults to `AC` in PyPSA's schema, and a default is [never stored](model/properties.md#default_by-a-default-per-category), so nothing checks that `AC` is a label of `carrier`.
A [selection](use/selecting.md#what-goes-with-a-dropped-label) applies the default when it propagates, so a bus defaulting to a carrier the selection drops goes, and one defaulting to a label no axis holds stays.

### Whether `bus` earns its place in the fold key

PyPSA's `partial` gains `bus` so a layer patches one bus's `v_nom`, and every `partial` coordinate is paid for by every read ([keep it small](layers/tree.md#partial-the-granularity-of-an-override)). Measuring it on a pypsa-eur-sized record would settle it.

### Whether `partial` may name a coordinate of a multi-dim

`partial` may name [a multi-dim](model/multi-dims.md#attributes-over-a-multi-dim) whole, and a layer owns an attribute along `attribute.dims ∩ partial` ([partial](layers/tree.md#partial-the-granularity-of-an-override)). So `period` in `partial` does not reach a series over `snapshot`, the multi-dim over `(period, timestep)`, and a per-period, myopic re-solve [restates the whole series](tools/pypsa.md#writing-results), every other period's values included. Allowing a coordinate would key `p` by `(attribute, connection, period)`, so the re-solve restates its own period's series only. The cost is a wider fold key on every read of every attribute over `snapshot`.

### Whether selector keywords need a qualified form

A bare [selector](use/editing.md#set) naming a property of two dims one attribute reaches is refused. The only way to say which is meant is setting per dim.

### Whether `Schema` should declare a dim with its mapping property in one call

A snapshot mapped to its period is [a dim and a property that refers to the period](model/dims.md#mapping-labels-to-another-dim), two declarations a helper could make one.

### Whether a `WorkingRecord` may delete one value along a dim outside `partial`

A layer that restates a key along such a dim and omits a label its parent held there [deletes that value](use/editing.md#edits). A `WorkingRecord`'s fill carries the label's whole extent, so today only a hand-written layer can.

### Whether a layer may remove a dim from `partial`

[Versioning](model/schema.md#versioning) refuses it. Allowing it, the layer would restate, as resolved, every key of its own `partial` at which a parent map row names a label of the dim; the fold would then project the dim out of the parent's map, [step 3 of the fold](layers/resolution.md#crossing-rows) dropping every row that named it, and refuse a layer leaving one uncovered. `compatible_with` would accept the smaller `partial`, and a helper would build such a layer from a revision.

### What PyPSA#1474 lands as

Upstream making buses and carriers indices may turn an unlisted carrier into an error, or stop `""` meaning none. The tool adds the one and stores the other as NULL today ([PyPSA](tools/pypsa.md#buses-and-carriers-are-dims)).

### Whether the checks at `set` and at write can take the fold's fixed plan

[The fold](layers/resolution.md#one-plan-for-every-layer) carries a row's named set as data, so its joins do not depend on the rows. The refusal of two crossing rows in [`set`](use/editing.md#set) and in [the writer](use/writing.md#shape) loops over the pairs of named sets the rows hold, fetching per pair.

### Whether `set` can stage a scoped or crossing broadcast without spreading it

[`set`](use/editing.md#set) stages a scenario-scoped broadcast, or one that crosses another row, as one row per entity and snapshot it reaches. A large edit then stages rows in proportion to the labels it reaches, not to the rows it names.

### When a deep ancestry materialises its map

A cold fold in a fresh workspace stays linear in depth, about 0.1 s per layer ([the nearest fold at hand](layers/resolution.md#a-fold-starts-from-the-nearest-fold-at-hand)). Materialising every N layers or at a branch point would bound it, and no policy is chosen.

### Whether an edit may join a caller's open transaction

An edit runs in [its own transaction](use/editing.md#edits), and DuckDB nests none, so an edit is refused while the caller holds one open on the same cursor. Joining the caller's would lose the edit's own rollback on refusal.

### What a schema change on a `WorkingRecord` does to its registered views

A `WorkingRecord` has no method that changes its schema; the schema changes only at commit.
One that does would either refuse a change that alters a column or the fold of an existing [registered view](storage/views.md) — widening an attribute's `dims`, adding a property, adding a dim to `partial` — or replace the views it reaches, which a client's query would then see change under it. Adding an attribute or a dim would add its view ([DEP 34](proposals/dep-0034-a-record-registers-as-views.md#schema-changes)).

### How a SQL client settles rows

[Registered views](storage/views.md) are raw, so a client applies the broadcast rule itself. [`settle`](use/reading.md#settling-rows) ported to the frontend, and a page on the raw format for SQL clients, are both still to write ([DEP 34](proposals/dep-0034-a-record-registers-as-views.md#open-issues)).

### Who drops a forgotten registration

A server that crashes or forgets leaves its views and the folds they read. The `registered` table lists every registration with when it was `created`, but no one unregisters by any rule yet ([DEP 34](proposals/dep-0034-a-record-registers-as-views.md#open-issues)). Unregistering frees the staging tables the registration [holds](storage/store.md#the-databases-schemas), so the rule also decides how long they stay. A workspace reopened on a file keeps an earlier run's `_staged` and `_fold` schemas, which no record holds; those no registration's `held` column lists could be swept, and nothing does yet ([DEP 35](proposals/dep-0035-a-record-holds-the-tables-it-reads.md#open-issues)).

### A public `create_view` taking a qualified name

`register` calls DuckDB's private `_render` and `_create_view`, since `DuckDBPyRelation.create_view` takes one identifier and no nested path ([building the views](storage/views.md#building-the-views)). That caps DuckDB at the newest release the suite passed on until a public method takes a qualified name, which is to be requested upstream.

### A reader seeing part of a large committed transaction

DuckDB 2.0 lets a reader on another cursor see part of a committed transaction once it writes about 300,000 rows or more, in 0.1–1% of reads. No edit can rule it out until DuckDB does, so the [consistent states](storage/views.md#consistent-states) hold for smaller edits only. It is to be reported upstream ([DEP 34](proposals/dep-0034-a-record-registers-as-views.md#open-issues)).

### Who pays the cold fold of a registered revision

`register`, or the first read of a view, pays the [cold fold](layers/resolution.md#a-fold-starts-from-the-nearest-fold-at-hand) of a deep revision, once per lineage in a workspace. It stays linear in depth until a revision's map is [materialised](#when-a-deep-ancestry-materialises-its-map).

## Settled

### Filtering a record before `build`

A record selects a view of its labels, `record.select(period=2030)`, which any consumer reads as a `RecordLike` ([selecting](use/selecting.md)). One label squeezes its dim, and what refers to a dropped label goes with it.

### What a pointwise `set` leaving one of two dims outside `partial` NULL reaches

Every label of the dim it leaves NULL, at the label it names. The rest of the series keeps the parent's values, staged, committed or over a materialised parent ([a dim outside `partial`](layers/resolution.md#a-dim-outside-partial)).

### Whether a broadcast survives materialisation

It does. The owner map holds keys as written and the fold reads no axis, so a NULL reaches every label the axis holds at the node read: one added below a materialised revision and one staged in a `WorkingRecord` alike ([what a row reaches](layers/resolution.md#what-a-row-reaches)). A map materialised before `partial` widened is [extended](layers/resolution.md#extending-the-map-to-a-wider-schema) to the wider key like a map folded live.

### Whether `flags` needs a record-level counterpart

It is one. `flags()` answers per record, every attribute included, and `flags(**categories)` per category, restricted to the rows reaching its labels ([flags](use/reading.md#flags)). An attribute over an axis alone or a multi-dim outside `partial` is reachable through the first.

### What a NULL multi-dim struct reaches

Every tuple, as a NULL on the dim itself reaches every label ([the broadcast rule](model/attributes.md#the-broadcast-rule)). A constant `efficiency` lands on generators' connections too. One for links alone is `set("efficiency", 0.7, ctype="Link")`.

### What `build` does with a value its type does not take

It leaves the value out and reports it in `Requirements.ignored`, as an unknown type is reported. `build` does not raise ([tools](tools/pypsa.md#a-value-its-type-does-not-take)).

### Whether a multi-dim in `partial` may have categories

It may, by the rule a dim follows: `connection.role: {one_of: role}` categorizes `connection` in `partial`, so a `default_by` keyed `connection.role`, `flags(role=...)` and the keyword `role=` work as `entity.ctype` does ([categories of a multi-dim](model/multi-dims.md#categories-of-a-multi-dim)).

### What a connection value staged without its `bus` should mean

Nothing. The call is [rejected](use/editing.md#set).
`{entity: dc, bus: NULL}` is [no multi-dim value](model/attributes.md#the-broadcast-rule), a multi-dim value being a whole tuple or NULL, so it is refused wherever a row comes in.
The alternative, "every connection of this entity", would broadcast over part of a tuple. A multi-dim's column broadcasts only whole, and a NULL struct means every tuple.

### Whether a record's relations can be named views

They can, in the workspace's own database, one schema per registration with a nested schema per kind ([registered views](storage/views.md)). Over a `WorkingRecord` the views are created once and follow every edit, since a relation's plan never reads a staged row.

### When a server drops the folds it opened

When the last record reading one is closed, or collected with every relation taken from it, unless a registration still holds it. A server closes each record it opens, so it frees them at a point it chooses rather than the collector's ([the database's schemas](storage/store.md#the-databases-schemas)).

### When a `WorkingRecord`'s staging tables are dropped

When it is closed or collected, unless a registration of it still holds them; then on that registration's `unregister`. Its tables sit in a schema of their own, which the workspace drops whole ([the database's schemas](storage/store.md#the-databases-schemas)).
