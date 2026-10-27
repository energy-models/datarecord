<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Open questions

- **May an entity's existence depend on a dim?** `Dimension.keys` said yes — a generator present in scenario `high` and absent from `low` — and is [now deleted](schema.md#existence-does-not-vary-along-a-dim), with nothing in its place. An entity exists or it does not.

  What it cost to keep was a second question with no good answer: what an entity tombstone means for a connection keyed by fewer dims. Deleting an entity in one scenario removed a connection that was not scenario-scoped, even though the entity survived elsewhere, and no projection recovers the difference — the connection row has no scenario column to write it into. The conservative reading was implemented and pinned by an `xfail`.

  What it cost to drop is narrower than it looks. A **value** may still vary along any axis; it is the **thing** that may not. A stochastic network with a different `capital_cost` per scenario is an attribute over `{entity, scenario}`, which [the file split](format.md#where-a-value-lives) already places in `attributes/`. Only membership itself is unrepresentable.

  Reopening it means deciding all three: whether it is needed at all, whether an entity table and a group may disagree about which dims scope them, and what the coarser one's rows mean if they may.

- **Whether `partial` should ever be per attribute.** [The schema](schema.md#partial-the-granularity-of-an-override) puts it on the axis because it is true of every attribute varying over that axis.
  A counter-example would be an attribute whose series a consumer _can_ accept in pieces while others cannot — none known, and permitting it would make the fold's key vary per attribute, which the fixed inputs key assumes it does not.

- **Whether a broadcast survives materialisation.** A materialised ancestor's owner map stores a NULL row already expanded to the labels that existed then, and seeds a child's fold without re-expanding.
  So `set("p_max_pu", 0.9)` reaches an entity added in a child only while the ancestor is unmaterialised.
  One level down, a `WorkingRecord` folds its committed prefix once, so a label staged there is not reached by a parent's NULL even while the parent is unmaterialised.

- **Whether results should be invalidated by a later input edit.** A result is [an attribute](schema.md#a-result-is-an-attribute) and inherits, so a child that edits an input holds results computed from its parent's inputs, describing a record that no longer exists.
  Dropping them on the next input edit was considered and rejected: it silently discards work the caller may have wanted, and a record that guesses which of the two the caller meant to keep is worse than one that keeps both and says so.
  Coherence is the caller's business.
  If this bites in practice, a `pending`-level warning is the cheap next step rather than a silent truncation.

- **Detecting intermediate results that no longer fit their inputs.** A [results layer](tools.md#writing-results) replaces the SubNetworks of its base, but a child editing the topology keeps its parent's until the next solve; telling them stale needs provenance of which inputs each came from.

- **Filtering a record before `build`.** Selecting one period or chosen outputs is a separate feature, not designed.

- **Whether a `WorkingRecord` over an open record stages against a snapshot.** Writing into an open record invalidates its owner-map cache.
  A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
  The second is simpler and arguably more correct — an edit sequence should not see another writer's changes mid-flight — but it means a record can go stale.

- **Registering a record's relations as named views.** A frontend issuing ad-hoc SQL needs names in a catalog rather than Python objects, which `CREATE VIEW` against a file-backed catalog provides — each view's definition being the resolved overlay, materialising nothing.
  Creating a view binds its schema, so registering N attributes costs N footer reads; and catalog reopen cost is linear in view count, which argues for one catalog per record rather than one shared.

- **What else a `Record` should answer about its labels without handing over a frame.** [`flags`](record.md#flags) sets the shape — cheap derived metadata a consumer plans against without opening a file — but answers only per attribute.
  `entity -> ctype` is now a column of the resolved axis, [names being unique record-wide](format.md#entity-is-unique-across-types), so it is one frame rather than one per type; "which categories have live labels" and "how many labels a category has" are still frames to collect.
  What is open is the granularity: those are the same kind of question, and a protocol growing one method per question is worse than the frames it replaces.
  Whatever is chosen, it has to be answerable off the fold, which is where a record's membership already lives.

- **Whether a property [`into`](schema.md#properties) a `partial` dim may categorize it.** `entity.carrier` names labels of `carrier`, and a `default_by` keyed on it would make a vocabulary that is data: a category a layer adds and removes rather than an `Enum` the schema declares.

- **Whether a default must name a live label.** `bus.carrier` defaults to `AC` in PyPSA's schema, and a default is [never stored](schema.md#default_by-a-default-per-category), so nothing checks that `AC` is a label of `carrier`.

- **Whether `bus` earns its place in the fold key.** PyPSA's `partial` gains `bus` so a layer patches one bus's `v_nom`, and every `partial` coordinate is paid for by every read ([keep it small](schema.md#partial-the-granularity-of-an-override)). Measuring it on a pypsa-eur-sized record would settle it.

- **Whether a custom bus column can be a property.** It is an attribute over `bus` today, so custom `Bus.country` beside custom `Generator.country` is [one attribute over two dims](tools.md) and refused. Placing it as `bus.country` would dissolve the one remaining custom-column clash, but needs a rule telling a custom property from a custom attribute.

- **Whether selector keywords need a qualified form.** A bare [selector](working-record.md#set) naming a property of two dims one attribute reaches is refused, with no way to say which is meant short of setting per dim.

- **Whether `Schema` should declare a dim with its parent property in one call.** A label scoped per parent is [a flat label and a property `into` the parent](schema.md#a-dim-is-flat), two declarations a helper could make one.

- **A pointwise `set` leaving one of two whole-owned dims NULL drops the rest of the series.** It is a fold bug with any two whole-owned dims on one series; [a dim being flat](schema.md#a-dim-is-flat) removed the routine case, `period` beside `snapshot`.

- **What PyPSA#1474 lands as.** Upstream making buses and carriers indices may turn an unlisted carrier into an error, or stop `""` meaning none; the tool adds the one and stores the other as NULL today ([tools](tools.md)).

## Settled

- **Whether [`flags`](record.md#flags) needs a record-level counterpart** — it is one. `flags()` answers per record, every attribute included, and `flags(**categories)` per category, restricted to the rows reaching its labels ([flags](record.md#flags)). An attribute over an axis alone or a group outside `partial` is reachable through the first.

- **What a NULL group struct reaches across entity types** — every tuple, as a NULL on the dim itself reaches every label ([the schema](schema.md#an-attribute-reaches-every-label-it-addresses)). A constant `efficiency` lands on generators' connections too; one for links alone is `set("efficiency", 0.7, ctype="Link")`.

- **What `build` does with a value its type does not take** — leaves it out and reports it in `Requirements.ignored`, as an unknown type is reported, without raising ([tools](tools.md#a-value-its-type-does-not-take)).

- **Whether a category still needs `partial`** — it does: `default_by` per label needs a dim owned per label ([categories](schema.md#categories-an-enum-property-of-a-partial-dim)).

- **Whether an attribute reaching every label grows the owner map** — only by what a broadcast is asked to reach. An exported PyPSA network stores no broadcast, so `ac_dc_meshed`'s map is unchanged; `set("p_max_pu", 0.9)` on it grows the map by one row per label it now reaches, loads and lines included, where it reached the types carrying it before — 14 more rows on `ac_dc_meshed`. A value meant for generators is `set(..., ctype="Generator")` ([the schema](schema.md#an-attribute-reaches-every-label-it-addresses)).

- **What switches a bundle of attributes per entity, as `committable` does a unit-commitment one** — the consumer, reading the `committable` attribute; the schema declares no switch ([the schema](schema.md#an-attribute-reaches-every-label-it-addresses)).

- **What a connection value staged without its `bus` should mean** — nothing; the call is [rejected](working-record.md#set).
  `{entity: dc, bus: NULL}` is [no group value](record.md#the-broadcast-rule), a group value being a whole tuple or NULL, so it is refused wherever a row comes in.
  The alternative, "every connection of this entity", would broadcast over part of a tuple — a group column broadcasts only whole, a NULL struct meaning every tuple.
