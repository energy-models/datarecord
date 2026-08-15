# Open questions

- **What a component tombstone means for a connection keyed by fewer dims.** When `component` keys `scenario` and `connection` does not, deleting a component in one scenario removes a connection that is not scenario-scoped, even though the component survives elsewhere.
  The conservative reading — project the tombstone down to the shared dims — is implemented; deciding it properly needs the folded components map, which the connections fold cannot reach.
  Low priority while no framework allows scenario-varying connections.

- **Whether `within` should subsume `bus`.** `bus` and a nested dim express the same relation.
  A `timestep` label identifies a point only within a `period`; a `bus` label identifies a connection only within a component — `"north"` alone names nothing, since every component may attach to `north`, while `(link_dc, north)` names one connection.
  Written as a dim that would be `Dimension(dtype="str", within={"name"})`, and `bus` would stop being a hardcoded key column.
  [Name uniqueness](format.md#name-is-unique-across-types) strengthens the analogy rather than weakening it: `name` is now a single global axis rather than one qualified by `component_type`, so `within={"name"}` names something well-defined where `within={"component_type", "name"}` would have been the awkward spelling.

  What blocks it is that `bus` inverts the rule NULL follows for a dim.
  A NULL declared dim means "all values", and [the fold](read-path.md#resolving-a-relation) expands it against the axis; a NULL `bus` means "this attribute belongs to the component rather than to any connection", and is compared NULL-safely, never expanded.
  So `bus` would be a dim carrying an explicit exception to the one behaviour that makes a dim a dim.
  With one instance of each relation in hand there is nothing to generalise against, and unifying them would touch every key and every NULL comparison.

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
  The entity-level case is `name -> component_type`: a question a record can answer, [names being unique record-wide](format.md#name-is-unique-across-types), and one the layered `components` owner map is keyed by before it opens a file.
  Asking it through the protocol costs a frame per type instead, which `WorkingRecord` pays on every [`set`](working-record.md#validation).
  What is open is the granularity: "which types have live rows" and "how many members a type has" are the same kind of question, and a protocol growing one method per question is worse than the frames it replaces.
  Whatever is chosen, a `DirectoryRecord` must answer it without a fold, or it is fast for one implementation and a rename of the slow path for the other.
