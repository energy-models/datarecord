<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Writing

There are two destinations: a store, written by `store.commit`, or by `store.claim` and `claim.commit`, and a standalone directory, written by `ws.write`.
Each call writes one layer from a `LayerData` source and returns what it made.
The writer casts every column to its declared type.
It refuses rows that do not fit the schema or that name labels the layer's axes lack, and it refuses an axis whose order differs from its parent's.
A store write puts the layer's manifest last, so a layer becomes reachable only once it is complete.
A standalone directory gets its manifest last for the same reason.

```python
store.claim(*, parent: Revision | None, meta: Meta | None = None) -> Claim
claim.commit(source, *, meta: Meta | None = None) -> Revision
store.commit(source, *, parent: Revision | None, meta: Meta | None = None) -> Revision
store.commit(w, *, meta: Meta | None = None) -> Revision  # w a WorkingRecord
ws.write(source, uri, *, meta: Meta | None = None) -> Record  # a standalone directory, read back
```

`store.commit` is `store.claim(...).commit(source)` in one call.
`parent` is a required keyword, so a new root is written out as `parent=None`, and a root and a child differ by an argument.

A claim hands out the id before any data is written.
A job submitted with a claim commits it at its end, days later if need be, and until then `store.claims()` lists the layer as pending, with its `meta`, and no [cleanup](../storage/store.md#cleanup) by age reaches it while it is young.

A `WorkingRecord` brings its parent.
`store.commit(w)` writes `w`'s [edits](editing.md#edits), a delta, as a child of the revision `w` was built over.

`meta` is the revision's own, as its [manifest](../storage/store.md#the-layers) holds it, and `store.revisions(meta={...})` finds the revisions whose `meta` holds every pair given.
What a commit checks before it writes, its parent, its `meta` and its schema, is in [a revision exists only by its file](../storage/store.md#a-revision-exists-only-by-its-file).

## A write makes a new layer

Each call writes `source` as a new layer, under an id that is claimed once, so a write never half-replaces what a record holds.
`ws.write` refuses anything that already exists at `uri`: locally the path, even an empty directory, and remotely any key under it.

The writer looks up keys one at a time and writes each file before it builds the next.
A lazily-building source therefore does one read per file written.
Data goes straight into the layer directory, locally as remotely, and nothing is staged and renamed.
A frame that fails validation leaves the files written before it and no manifest, so the layer stays [pending](../storage/store.md#a-revision-exists-only-by-its-file) and no read reaches it.
A failed `ws.write` leaves its files at `uri` the same way, and they block a write there until the caller deletes them.

## Types

The writer casts every column that the schema declares a type for, so a record's files carry the schema's types and a reader can trust them.
It holds each column to its dtype as [`set`](editing.md#set) holds a frame's.
A wider type of the same kind is cast.
A column of only NULLs passes, whatever type its dataframe library guessed.
Any other type is refused, naming the attribute, property or dim and its dtype.

## Shape

Each attribute file is checked against its [`AttributeSpec`](../model/attributes.md#attributespec).
An attribute that the schema does not declare is refused.
Its `dims` say which columns its file carries, so an undeclared attribute has no shape to write it in.
A frame must carry [its attribute's own coordinates](../storage/format.md#the-long-schema), and the writer refuses one that lacks any of them.
It also refuses a frame that carries a coordinate the attribute is not addressed by.
The read path projects only the attribute's own coordinates, so such a column would be written and never read.
A source that emits one disagrees with the schema about the attribute, and the error reports that disagreement.
A [multi-dim](../model/multi-dims.md)'s column must be its struct, with exactly its coordinates as fields.
Two rows of one attribute that cover one coordinate are refused when both name the same dims, or when each names a dim the other leaves NULL and no row there names both, naming the attribute and the coordinate.
A record holding rows of several layers, written whole by `ws.write(w, uri)`, passes: wherever two of its rows cross, it holds the row naming both ([crossing rows](../layers/resolution.md#crossing-rows)).
A child layer that leaves a dim outside `partial` NULL over a parent row naming it, with no row naming the dim there, is refused by the fold rather than the writer ([a dim outside `partial`](../layers/resolution.md#a-dim-outside-partial)).

A `dims/` file of a dim that the schema does not declare is refused, since a descendant declaring the dim would read its labels.
A `dims/` file of a declared dim must carry the dim's key column, the struct column named for it for a multi-dim.

## Axes and labels

A `dims/` file is the whole axis at the new revision, a multi-dim's tuples included, in order ([removing labels](../layers/removal.md)).
The writer refuses a file that holds a key twice, or that carries a column other than its key and its subject's declared properties.
It refuses a file that orders its parent's labels differently from the parent, naming the first label out of place, since a consumer numbers ports by [that order](../model/dims.md#axis-order).
A [dim with `one_of`](../model/dims.md#a-dim-with-one_of) has no file: a `LayerData` handing one over is refused, and a `RecordLike`'s axis of it is dropped where it is the list, the same labels in the same order, and refused otherwise, naming the dim and the difference.

Every row must name labels and tuples that the layer keeps:

- an attribute row that names a label which the layer's final axis of its dim lacks is refused, naming the dim and up to three such labels;
- an attribute row at a tuple which the layer's final multi-dim lacks is refused the same way, naming the multi-dim;
- a multi-dim's tuple that names a label which the layer's final axis of its coordinate's dim lacks is refused, naming the multi-dim, the coordinate and up to three such labels.

The final axis is the layer's own file, else its parent's resolved one.
One DuckDB query per file anti-joins its rows against the final axis of every column it names.
A layer written by hand or by a tool therefore holds values only at labels and tuples some axis holds, as [`set`](editing.md#validation) holds a staged one.
The checks on properties that refer to a dim, and on labels a layer removes, are on [removing labels](../layers/removal.md#what-the-writer-checks).

## `LayerData`

The writer reads one protocol, `LayerData`: the rows of one layer, enumerated and read.

```python
@runtime_checkable
class LayerData(Protocol):
    @property
    def schema(self) -> Schema: ...

    def axes(self) -> set[str]: ...
    def axis(self, dim: str) -> DuckDBPyRelation | None: ...
    def attributes(self) -> set[str]: ...
    def attribute(self, name: str) -> DuckDBPyRelation | None: ...
```

Each pair is an enumerator of the keys of one kind and a read for one key.
An `axis` is the rows of its file: every label the axis has at the layer's revision, in order, with the dim's properties.
A multi-dim's `axis` is every tuple it has there, keyed by its struct column.

`schema` decides which of the other pairs are populated: `axes` and `axis` answer only for the dims it declares.
An enumerator answers the empty set where the schema declares nothing of that kind.
Rows are `DuckDBPyRelation`s, as a `RecordLike`'s are, so the write path runs on one engine throughout.

A [`LayerSource`](../layers/resolution.md#owner-map) answers `axis` and `attribute` for its own layer's rows.
A staged layer's own rows, `w.edits`, are such a source, so a [`WorkingRecord`](editing.md#edits) hands its edits to the writer as they are.

A [`RecordLike`](reading.md) is no `LayerData`.
This holds for a `Record`, a `WorkingRecord` among them, and for a framework object such as [`Tool.to_datarecord`](../tools/index.md) returns.
The writer wraps a `RecordLike` in a thin adapter that reads its `Relations` mappings through the same enumerate-and-read pairs, so the writer has one code path.
The adapter takes each value as [`set`](editing.md#set) does: a relation on the writer's connection, or a pandas, polars or arrow frame that DuckDB scans.
