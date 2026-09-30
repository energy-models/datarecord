<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Open questions

- **May an entity's existence depend on a dim?** `Dimension.keys` said yes — a generator present in scenario `high` and absent from `low` — and is [now deleted](schema.md#existence-does-not-vary-along-a-dim), with nothing in its place. A component exists or it does not.

  What it cost to keep was a second question with no good answer: what a component tombstone means for a connection keyed by fewer dims. Deleting a component in one scenario removed a connection that was not scenario-scoped, even though the component survived elsewhere, and no projection recovers the difference — the connection row has no scenario column to write it into. The conservative reading was implemented and pinned by an `xfail`.

  What it cost to drop is narrower than it looks. A **value** may still vary along any axis; it is the **thing** that may not. A stochastic network with a different `capital_cost` per scenario is an attribute over `{entity, scenario}`, which [the file split](format.md#where-a-value-lives) already places in `attributes/`. Only membership itself is unrepresentable.

  Reopening it means deciding all three: whether it is needed at all, whether an entity table and a relation may disagree about which dims scope them, and what the coarser one's rows mean if they may.

- **Whether `partial` should ever be per attribute.** [The schema](schema.md#partial-the-granularity-of-an-override) puts it on the axis because it is true of every attribute varying over that axis.
  A counter-example would be an attribute whose series a consumer _can_ accept in pieces while others cannot — none known, and permitting it would make the fold's key vary per attribute, which the fixed key of `attributes/` assumes it does not.

- **Whether a `WorkingRecord` over an open record stages against a snapshot.** Writing into an open record invalidates its owner-map cache.
  A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
  The second is simpler and arguably more correct — an edit sequence should not see another writer's changes mid-flight — but it means a record can go stale.

- **Registering a record's resolved data as named views.** A frontend issuing ad-hoc SQL needs names in a catalog rather than Python objects, which `CREATE VIEW` against a file-backed catalog provides — each view's definition being the resolved overlay, materialising nothing.
  Creating a view binds its schema, so registering N attributes costs N footer reads; and catalog reopen cost is linear in view count, which argues for one catalog per record rather than one shared.

- **What else a `Record` should answer about its entities without handing over a frame.** [`flags`](record.md#flags) sets the shape — cheap derived metadata a consumer plans against without opening a file — but answers only per attribute.
  The entity-level case is `entity -> entity_type`, which a record answers with a read of the `entity_type` [relation](schema.md#types).
  What is open is the granularity: "which types have live rows" and "how many members a type has" are the same kind of question, and a protocol growing one method per question is worse than the frames it replaces.
  Whatever is chosen, it has to be answerable off the resolved axes and relations, which is where a record's membership lives.

- **Whether [`flags`](record.md#flags) needs a counterpart for an attribute stored as a column.** `flags` reads the owner map, so an attribute over one axis alone, which is a column of that axis's file, is not reachable through it.

  A second method keyed by attribute and scoped record-wide would answer it: which attributes have values at all, and which coordinates they use.
  What is unsettled is whether that replaces `flags` or sits beside it.

## Settled

- **Whether `within` should subsume `bus`** — no; [relations](schema.md#relations) do it.
  `bus` is an ordinary dim, and a NULL `bus` [broadcasts](record.md#the-broadcast-rule) like a NULL in any other dim.
  Data on a component's attachments to buses goes over a dim of its own, related to `entity` and `bus` ([data on a relation's rows](schema.md#data-on-a-relations-rows)).
