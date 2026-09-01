# Proposal: `staged_only()` is the staged source

Status: **Draft** · Drafted 2026-09-02

The last half of the collapse [staging as a layer](staging-as-a-layer.md#what-it-opens) predicted. That page said `staged_only()` and `flattened()` "build two `Record`s out of one staging area", and that both should become projections of one object. `flattened()` [already did](one-record-over-a-node-cache.md) — once `WorkingRecord` became a `Record`, it was `self` in every field and `commit` now passes `self`. This is the other one.

## What starts it

`staged_only()` answers "what would this layer be written as". So does `StagedSource`, in as many words — it is the [protocol's](../read-path.md) stated contract:

> "The layer as it would be written", not "the rows as stored": a source hands over what `write_record` would persist, so a staging area's `_seq` collapsing happens behind it and the fold never learns about it.

Two answers to one question, and they are not merely similar. Each of the four builders calls the same thing the matching source member calls:

| `staged_only()` builds with | `StagedSource` answers with | shared call     |
| --------------------------- | --------------------------- | --------------- |
| `_staged_axes`              | `axis(dim)`                 | `_axis_layer`   |
| `_staged_entities`          | `entity_type(name)`         | `_collapsed_entities` |
| `_staged_groups`            | `group(name)`               | `_collapsed_group` |
| `_staged_attributes`        | `attribute(name)`           | `_collapsed_inputs` |

Measured on a `WorkingRecord` with a `set`, an `add` and an axis edit staged, comparing each builder's frame against its source member's relation:

| member                    | rows equal | columns                            |
| ------------------------- | ---------- | ---------------------------------- |
| `dims["entity_type"]`     | identical  | identical                          |
| `attributes["p_nom"]`     | identical  | identical                          |
| `groups`                  | identical  | identical                          |
| `entity_types["Generator"]` | identical  | builder carries `entity_type` too |

The one difference is a leftover rather than a distinction. `_collapsed_entities` keeps `entity_type` because it filters on it, and the builder never projects it away — but [a per-type member file no longer carries that column](../format.md#where-a-value-lives), so the writer strips it. `StagedSource.entity_type` already projects it out, which is the shape the file wants. The source is the more correct of the two.

## The construct

One adapter, turning any `LayerSource` into the `Frames` mapping `write_record` consumes:

```python
def _as_written(source: LayerSource, schema: Schema) -> _Written:
    """A layer's own rows, as the `RecordLike` `write_record` persists."""
```

`staged_only()` then becomes `_as_written(StagedSource(self, self._layer_id), self.schema)`, and the four `_staged_*` builders go. `_axis_layer`, `_collapsed_entities`, `_collapsed_group` and `_collapsed_inputs` all stay — they are what the source is built from, and were never the duplication.

### What the protocol is missing

A `Frames` is a mapping, so the adapter needs to **list** keys as well as fetch them. `LayerSource` can fetch all four and list only one:

| kind         | fetch                 | list           |
| ------------ | --------------------- | -------------- |
| dims         | `axis(dim)`           | `axes()`       |
| entity types | `entity_type(name)`   | **missing**    |
| groups       | `group(name)`         | **missing**    |
| attributes   | `attribute(name)`     | **missing**    |

That asymmetry is not an oversight in the source protocol — it is what the *fold* needs, and the fold never lists. It resolves a key set from the owner map, then asks a source for the rows behind keys it already has; only `axes()` exists because `resolve_dims` [would otherwise probe every declared dim in every layer](staging-as-a-layer.md#the-construct).

So this proposal adds three listing members:

```python
def entity_types(self) -> set[str]: ...
def groups(self) -> set[str]: ...
def attributes(self, kind: Kind = "inputs") -> set[str]: ...
```

Each is cheap for both implementations, and for the same reason `axes()` is:

- **`StagedSource`** reads its `_staged` map — a table exists exactly where rows were staged, so this is not a query at all.
- **`_FileLayer`** globs one directory with `parquet_names`, which already exists and is what `axes()` uses. One listing per kind rather than a probe per candidate name, which is the difference between one `LIST` and forty `HEAD`s against a remote record.

The entity-type listing is the one that is not a bare glob: `dims/entity_type/` holds one file per type, so the type *is* the filename, which is the same derivation [`_write_entity_axis` already does](../format.md#the-entity-axis).

## What this buys

**The duplication goes, and with it a class of drift.** Four builders and four source members answering one question is the shape [staging as a layer](staging-as-a-layer.md#what-starts-it) existed to delete, arriving one level up: "each pair was written from the same design paragraph and drifted anyway" is exactly what the `entity_type` column above is.

**`write_record` gains a second caller shape it already supports.** It consumes a `RecordLike`; a layer read as one is a `RecordLike`. Nothing in the writer changes.

**The `_Written` dataclass may go too.** With `flattened()` collapsed and `staged_only()` an adapter's return value, the only thing it holds is what `_as_written` builds — so it becomes that function's own return shape rather than a named type two methods share. Worth deciding when the code is in front of us, not now.

## What it opens rather than settles

**Whether a `ParquetLayer` should be readable this way.** The adapter takes a `LayerSource`, so `_as_written(ParquetLayer(id), schema)` would be "one layer's own rows as a record" — which is what `Record.at(layer_dir(id))` almost is, differing in that the latter folds. Two readings of one directory again, and the reason to keep them apart is the same as before: one is the layer, the other is the record it resolves to. Naming the first would be useful for a copy or a diff, and this proposal does not do it.

**Whether the listing members belong on `LayerSource` or beside it.** They are for writing, and the protocol's docstring says the fold is what it serves. A separate `WritableLayer` protocol would keep that clean at the cost of a second protocol every source satisfies — probably the wrong trade for three methods, but worth stating that it is a trade.

## What it costs

**Three members on a protocol whose selling point is being small.** `LayerSource` is deliberately rows-only, and every addition is a thing an implementation outside this package must answer. The defence is that these are the same *kind* of member as `axes()`, which is already there for the same reason: a listing that replaces a probe-per-candidate.

**A staged listing and a staged fetch could disagree** where today one method does both. `_staged_attributes` derives its keys and its frames from the same `_staged` map in one place; splitting them into `attributes()` and `attribute(name)` means a key that lists but does not fetch is expressible. It is not reachable — both read the same map — but it is newly expressible, and the adapter should skip a `None` rather than assume.

## How to know it worked

**The commit tests do not change.** `staged_only()` is what `NewChild` writes, so `test_a_child_layer_holds_only_the_edits`, `test_a_partial_axis_stays_a_patch` and `test_the_restated_series_is_in_the_layer_itself` already pin what a patch layer contains, from the outside. If they pass, the adapter agrees with the builders it replaces.

**The one behavioural change is the `entity_type` column**, which is a fix rather than a regression: the writer strips it today, so nothing on disk moves. Worth asserting directly that a staged member frame no longer carries it, since "the writer would have stripped it anyway" is exactly the kind of reasoning that stops being true.

**A round trip through both targets.** `NewChild` writes `staged_only()` and `Directory` writes `self`; committing the same edits both ways and reading each back is what says the adapter did not quietly become the flattened reading.
