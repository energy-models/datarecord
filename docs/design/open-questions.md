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

### Filtering a record before `build`

Selecting one period or chosen outputs is a separate feature. It has no design yet.

### Whether a `WorkingRecord` over an open record stages against a snapshot

Writing into an open record invalidates its owner-map cache.
A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
The second is simpler and arguably more correct, since an edit sequence should not see another writer's changes mid-flight. It means a record can go stale.

### Registering a record's relations as named views

A frontend issuing ad-hoc SQL needs names in a catalog, which `CREATE VIEW` against a file-backed catalog provides. Each view's definition is the resolved overlay and materialises nothing.
Creating a view binds its schema, so registering N attributes costs N footer reads. Catalog reopen cost is linear in view count, which argues for one catalog per record.

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

### Whether `bus` earns its place in the fold key

PyPSA's `partial` gains `bus` so a layer patches one bus's `v_nom`, and every `partial` coordinate is paid for by every read ([keep it small](layers/tree.md#partial-the-granularity-of-an-override)). Measuring it on a pypsa-eur-sized record would settle it.

### Whether selector keywords need a qualified form

A bare [selector](use/editing.md#set) naming a property of two dims one attribute reaches is refused. The only way to say which is meant is setting per dim.

### Whether `Schema` should declare a dim with its mapping property in one call

A snapshot mapped to its period is [a dim and a property that refers to the period](model/dims.md#mapping-labels-to-another-dim), two declarations a helper could make one.

### What PyPSA#1474 lands as

Upstream making buses and carriers indices may turn an unlisted carrier into an error, or stop `""` meaning none. The tool adds the one and stores the other as NULL today ([PyPSA](tools/pypsa.md#buses-and-carriers-are-dims)).

## Settled

### What a pointwise `set` leaving one of two dims outside `partial` NULL reaches

Every label of the dim it leaves NULL, at the label it names. The rest of the series keeps the parent's values, staged, committed or over a materialised parent ([a dim outside `partial`](layers/resolution.md#a-dim-outside-partial)).

### Whether a broadcast survives materialisation

It does. The owner map holds keys as written and the fold reads no axis, so a NULL reaches every label the axis holds at the node read: one added below a materialised revision and one staged in a `WorkingRecord` alike ([what a row reaches](layers/resolution.md#what-a-row-reaches)). A map materialised before `partial` changed is no cache, and the read folds from the layers again ([materialised node caches](layers/resolution.md#materialised-node-caches)).

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
