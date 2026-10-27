<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Writing

There are three ways to write: `store.root`, `rev.child` and `ws.write`.
Each one writes one layer from a `LayerData` source and returns what it made.
The writer casts every column to its declared type.
It refuses rows that do not fit the schema or that name labels the layer's axes lack, and it refuses an axis whose order differs from its parent's.
A tree write puts the revision file last, so a layer becomes reachable only once it is complete.
A standalone directory gets its manifest last for the same reason.

```python
store.root(source) -> Revision  # a new root with this layer
rev.child(source) -> Revision  # a new child of rev with this layer
ws.write(source, uri) -> Record  # a standalone directory, read back
```

There is one call per destination, and each is named for what it returns.
A tree write needs the location and the tree, a directory write needs only a connection, and the two return different things.
`child` lives on the parent, so the call names the store once.
Its name says that it makes something new, since layers are [write-once](../layers/tree.md#a-layers-data-is-write-once) and `rev` itself never changes.

## A write makes a new layer

Each call writes `source` as a new layer.
An existing layer directory is an error, so a write never half-replaces what a record holds.
`ws.write` refuses anything that already exists at `uri`: locally the path, even an empty directory, and remotely any key under it.

The writer looks up keys one at a time and writes each file before it builds the next.
A lazily-building source therefore does one read per file written.
Locally, the layer is staged into a sibling directory and renamed on success, so a frame that fails validation leaves no layer.
Remotely, a failed write can leave part of a layer, which nothing reaches without the [revision file](../storage/store.md#a-revision-exists-only-by-its-file) or the manifest written after it.

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
A `WorkingRecord` is read through a pinned copy that folds its staged edits once for the write.
