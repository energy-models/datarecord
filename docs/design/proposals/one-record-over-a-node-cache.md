# Proposal: one `Record`, over a `NodeCache`

Status: **Draft** · Drafted 2026-09-01

Stacked on [staging as a layer](staging-as-a-layer.md), which made a staging area a source of one fold. This is the same observation one level up: a plain directory is a source too, so `DirectoryRecord` is a second reading of what the fold already does.

## What starts it

[read-path](../read-path.md#what-differs-between-the-implementations) says two implementations differ in four ways. Measured on one directory — `DirectoryRecord(uri)` against a `NodeCache` over `[DirectorySource(uri)]` — **all four are the same answer**:

| what was compared                                         | result                     |
| --------------------------------------------------------- | -------------------------- |
| `dims`, `entity_types`, `groups`, `attributes`, `outputs` | identical                  |
| `flags`, every declared type                              | identical                  |
| member order, every declared type                         | identical                  |
| resolved rows per attribute                               | identical                  |
| `flags`, first call                                       | 6.6 ms scanned, 2.9 ms folded |

The table was not describing a design property. It was describing an implementation that predates the fold being able to read anything but a layer tree, and each row of it dissolves on contact:

- **Resolution.** Over one source the fold degenerates to a scan. There is one layer, so every key is owned by it and the anti-join has nothing to evict.
- **`flags`.** The fold computes them _in_ that scan's ownership `GROUP BY`, where `DirectoryRecord` scans a second time to get them. Which is why the fold is the faster of the two, not the more expensive one — the opposite of what the table implies.
- **Member order.** `order_key` over one source is `(0, file order)`. Which is file order.
- **`schema.partial`.** It is the granularity of a patch, and one layer patches nothing — so it is inert either way rather than absent from one side.

Two fallback arguments for keeping it also fail, and both were mine:

- **"Folding leaves an owner map on the connection."** It does not. `_frozen_table` keys its cached fold on `revision_id`, and a `DirectorySource`'s id is derived from its URI, so nothing is `.create()`d. Measured: five tables on the connection before, five after.
- **"A foreign directory needs no layering machinery."** A plain parquet directory copied out of any tree, with no revision and no `revisions` row, folds and answers identically. Being a node in a tree is not what the fold needs; being a layer layout is, and that is what a directory _is_.

## The construct

**`NodeCache` does not grow.** It is already the module the design calls relational algebra of real complexity, and the fix here is not to add the `Record` surface to it. It stays what it is: the DuckDB-shaped view over a list of sources, answering in relations.

**`Record` becomes the narwhals interface over one.** One class, holding a `NodeCache` and adapting it — relations to `LazyFrames`, `order_key` sorting, the key sets. That is exactly what `LayeredRecord` already is, so this proposal is mostly *deleting `DirectoryRecord` and renaming what remains*:

```python
@dataclass(frozen=True)
class Record:
    """A record's resolved view, as narwhals frames."""

    node_cache: NodeCache

    @classmethod
    def over(cls, *sources: LayerSource) -> Record: ...

    @classmethod
    def at(cls, uri: str, con: DuckDBPyConnection) -> Record:
        """A plain parquet directory, read as the one layer it is."""
        return cls.over(DirectorySource(uri, con))
```

The two views stay separable, which is the property worth keeping: `tools/` builds against narwhals frames and [says so explicitly](https://github.com/energy-models/datarecord/blob/main/datarecord/tools/base.py) — `to_relation` exists so a tool needing DuckDB's own SQL unwraps a frame "rather than reaching past the record to a `NodeCache`". Collapsing the two would take that seam away.

### What happens to the protocol

`Record` is a `Protocol` today because several things satisfy it. That stays true and stays load-bearing — a framework object presenting itself as a record is the case [`tools`](../tools.md) is built on, and structural typing is what lets it do so without depending on this package.

So the name has to split. Two options, and this proposal does not pick between them:

- **`Record` the protocol, `ResolvedRecord` the class.** Keeps the protocol's name where every consumer already reads it, at the cost of the concrete class getting the longer name despite being the only one in core.
- **`Record` the class, `RecordLike` the protocol.** The reverse trade. Reads better at a construction site and worse at a `tools/` signature.

**This is the one thing to settle before writing code**, since it touches every module and both public exports.

## What this deletes

- **`directory.py` entirely** — `DirectoryRecord`, its `_read`/`_require`/`_keyed_by`, and its `flags` aggregate, which is [the second of the two the last proposal left](staging-as-a-layer.md#6-what-directoryrecordflags-keeps-and-the-aggregate-it-shares). With one caller the question that proposal deferred answers itself: there is nothing to share the fold's aggregate _with_, so the parameterised helper it declined to write is not needed at all.
- **`LayeredRecord`**, as a distinct name — it becomes the one class.
- **The read-path table**, and the paragraph under it about a `WorkingRecord` over a directory building a map. Both describe a distinction that is not there.

## What this does not change

- **`WorkingRecord` stays a separate class.** It has an edit surface — `set`, `add`, `remove`, `commit` — that a read-only record must not grow. What it wraps is its own question, listed under [what it opens](#what-it-opens).
- **`Frames` and `LazyFrames`** are untouched: the laziness contract is the interface's, not the fold's.
- **No format change.** Nothing on disk moves, and no file is read differently. A directory that reads today reads identically after.

## What it costs

**A design property is withdrawn, not just an implementation.** The read-path table has said since it was written that a directory is read without resolution. That was true of the code and is now false, and this proposal's whole claim is that it was never a property worth having — a plain directory is a one-layer record, and reading it as one is both simpler and faster. If that argument is wrong, it is wrong here rather than in the code.

**`tests/test_records.py`'s `both` fixture loses its subject.** It exists to assert two backings agree, and with one backing it asserts nothing. What replaces it has content and is arguably the better test: one directory read _as a directory_ against the same rows read _as a revision's layer_ — the same files reached by two source constructions, which is the invariant that survives.

**A one-source fold is not free where a scan was cached.** `DirectoryRecord.flags` memoises per type; the fold answers from a relation each time. First call favours the fold (2.9 ms against 6.6 ms) and the tenth favours the cache. Bounded by the map rather than by anything a caller controls, and the fold's answer is composable where the cached dict is not — but it is a real difference, and `attributes_of`'s `fetchall()` is where it lands.

## How to know it worked

**The rows, keys, flags and member order are already asserted equal** for the two implementations by `test_backings_agree_on_*`. Rewriting those to compare two _source constructions_ of one directory keeps every assertion and changes only what produces the right-hand side — so the evidence for this change is the evidence that already exists.

**A foreign directory is the case to add.** A parquet directory with no `revisions` row and no tree — which is what the format promises a non-blocks reader can consume — must read through the fold. It does today; nothing asserts it.

**`tools/` must not notice.** It builds from narwhals frames and the PyPSA round-trip is the end-to-end check, so `test_tools.py` passing unchanged is what says the seam held.

## What it opens

**Whether `Revision` still needs `record`.** If a record is a `NodeCache` plus an adapter, `Revision.record` and `Revision.node_cache` are one construction apart, and the second is what `materialise` already uses.

**Whether `WorkingRecord` wraps a `NodeCache` rather than a `Record`.** Every use it makes of its base is either the schema, the parent's rows for one kind, or the revision to branch from — all `NodeCache` members. That is a smaller change than this one and does not depend on it, so it should land first.
