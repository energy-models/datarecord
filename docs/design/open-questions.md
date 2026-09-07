# Open questions

- **May an entity's existence depend on a dim?** `Dimension.keys` said yes — a generator present in scenario `high` and absent from `low` — and is [now deleted](schema.md#existence-does-not-vary-along-a-dim), with nothing in its place. A component exists or it does not.

  What it cost to keep was a second question with no good answer: what a component tombstone means for a connection keyed by fewer dims. Deleting a component in one scenario removed a connection that was not scenario-scoped, even though the component survived elsewhere, and no projection recovers the difference — the connection row has no scenario column to write it into. The conservative reading was implemented and pinned by an `xfail`.

  What it cost to drop is narrower than it looks. A **value** may still vary along any axis; it is the **thing** that may not. A stochastic network with a different `capital_cost` per scenario is an attribute over `{entity, scenario}`, which [the file split](format.md#where-a-value-lives) already places in `inputs/`. Only membership itself is unrepresentable.

  Reopening it means deciding all three: whether it is needed at all, whether an entity table and a group may disagree about which dims scope them, and what the coarser one's rows mean if they may.

- **Whether `partial` should ever be per attribute.** [The schema](schema.md#partial-the-granularity-of-an-override) puts it on the axis because it is true of every attribute varying over that axis.
  A counter-example would be an attribute whose series a consumer _can_ accept in pieces while others cannot — none known, and permitting it would make the fold's key vary per attribute, which the fixed inputs key assumes it does not.

- **Whether staged results should be invalidated by a later input edit.** Results attached through [`set(..., kind="outputs")`](working-record.md#results-through-kindoutputs) were computed from the inputs pending at that moment, so editing an input afterwards leaves them describing a record that no longer exists.
  Dropping them on the next input edit was considered and rejected: it silently discards work the caller may have wanted, and a record that guesses which of the two the caller meant to keep is worse than one that keeps both and says so.
  Coherence is the caller's business, and a commit writes whatever is staged.
  If this bites in practice, a `pending`-level warning is the cheap next step rather than a silent truncation.

- **Whether a `WorkingRecord` over an open record stages against a snapshot.** Writing into an open record invalidates its owner-map cache.
  A mutable record would need the same invalidation per edit, or to stage against a snapshot taken at construction.
  The second is simpler and arguably more correct — an edit sequence should not see another writer's changes mid-flight — but it means a record can go stale.

- **Registering a record's relations as named views.** A frontend issuing ad-hoc SQL needs names in a catalog rather than Python objects, which `CREATE VIEW` against a file-backed catalog provides — each view's definition being the resolved overlay, materialising nothing.
  Creating a view binds its schema, so registering N attributes costs N footer reads; and catalog reopen cost is linear in view count, which argues for one catalog per record rather than one shared.

- **What else a `Record` should answer about its entities without handing over a frame.** [`flags`](record.md#flags) sets the shape — cheap derived metadata a consumer plans against without opening a file — but answers only per attribute, per type.
  The entity-level case is `entity -> entity_type`: a question a record can answer, [names being unique record-wide](format.md#entity-is-unique-across-types), and one the layered `components` owner map is keyed by before it opens a file.
  Asking it through the protocol costs a frame per type instead, which `WorkingRecord` pays on every [`set`](working-record.md#validation).
  What is open is the granularity: "which types have live rows" and "how many members a type has" are the same kind of question, and a protocol growing one method per question is worse than the frames it replaces.
  Whatever is chosen, a `DirectoryRecord` must answer it without a fold, or it is fast for one implementation and a rename of the slow path for the other.

- **Whether [`flags(ctype)`](record.md#flags) needs a record-level counterpart.** It takes a component type, which two kinds of attribute do not have: one addressed by an axis alone, and one addressed by a [group](schema.md#groups)'s coordinates. Neither has a `ctype` to ask about, so neither is reachable through it.

  A second method keyed by attribute and scoped record-wide would answer it — which attributes have rows at all, which coordinates they use, which types they touch — and the [owner map](read-path.md#owner-map) already computes the material, so it costs a projection rather than a scan.
  What is unsettled is whether that replaces `flags` or sits beside it. `flags` is per type _by construction_, its union deliberately stopping at the type boundary, and a record-level answer filtered by type would have to reproduce that.

## Settled

- **Whether `within` should subsume `bus`** — no; [groups](schema.md#groups) do it.
  The recorded blocker was that `bus` inverts the rule NULL follows for a dim: a NULL declared dim means "all values" and the fold expands it against the axis, while a NULL `bus` is compared NULL-safely and never expanded.
  That premise did not survive. Expansion is governed by whether a coordinate is in the fold's key set, and a non-`partial` dim is never expanded either — so the behaviour that "makes a dim a dim" was never uniform, and `bus` was not an exception to it.
  A group states the rule instead of carrying an exception: a group coordinate addresses a sparse subset with no axis to expand against, which is [the broadcast rule](record.md#the-broadcast-rule) rather than a special case in it.
