<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Consuming a record

A **tool** is the framework-specific translation target built from a record.
The call runs from the tool inward:

```python
class Tool(Protocol):
    name: str
    schema: Schema  # attribute mapping

    def requires(self, record: Record) -> Requirements: ...
    def verify(self, record: Record) -> Requirements: ...  # falsy when usable
    def build(self, record: Record) -> Any: ...
    def to_datarecord(self, model: Any, con: DuckDBPyConnection) -> Record: ...
    def results(self, model: Any, con: DuckDBPyConnection) -> Frames: ...
```

`to_datarecord` is the inverse of `build`. It and `results` take the connection their frames are planned on, required; in practice `ws.con` of the [workspace](layers.md#the-store) that writes them, or the `con` of the record they are `set` on.
A tool is handed a connection, not a workspace or a store: it plans relations and has no business with directories, locations or the tree.
A frame planned on another connection is [rejected at the boundary](read-path.md#a-relation-from-another-connection-is-rejected), so a tool that picked its own would fail on the first write.
`build` takes none: it reads `record.con`.

```python
root = store.root(PyPSA.to_datarecord(n, ws.con))
w = WorkingRecord(root.record)
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame, kind="outputs")
```

`results` returns [`Frames`](record.md#frames) — the same type `Record.outputs` presents, keyed by attribute, each frame in [the long schema](format.md#the-long-schema).
So a tool's results go straight to [a write](writing.md), or one at a time to [`set(attr, frame, kind="outputs")`](working-record.md#results-through-kindoutputs) with no key to unpack.

A framework holds its results per entity type, so reaching this shape means concatenating each attribute's types into one frame.
That is free: the frames are lazy, so the union is a plan rather than a copy, and nothing materialises until a caller collects.
The concatenation needs no `entity_type` column to distinguish the arms, since [`entity` is unique across them](format.md#entity-is-unique-across-types) — which is what makes the union a plain one rather than a tagged one.

Lazy is what the protocol asks for rather than what any implementation must do.
A tool reshaping a solved model's in-memory containers has nothing to defer and wraps its eager frames with `.lazy()`; one that could fetch a result attribute from a solver on demand is free to, and a caller wanting three of forty then pays for three.
That is [the `Frames` argument](record.md#frames), on the write side.

A record is the input to a translation, not the owner of one, so there is no registry and no name dispatch: a tool is a module-level singleton reached by importing it, `build` returns the framework's own type, and nothing in the record layer imports a tool.

`build` takes a [`Record`](record.md) rather than a record, so a tool builds from a directory as readily as from an overlay and has no reason to know layering exists.

A tool's `verify` catches what the record layer cannot: an entity type the framework has no registry entry for, a connection `role` it cannot place, a `partial` set that breaks the framework's constant-versus-varying split.
It is also where a framework scoping names **per type** meets [a record scoping them record-wide](format.md#entity-is-unique-across-types).
PyPSA permits a `Bus` and a `Generator` both called `north`, so `to_datarecord` reports such a network as unbuildable rather than writing a record whose two entities share one key.
Reported rather than repaired: renaming to `Generator:north` would hand back a network whose components the framework can no longer find by their own names, and the record layer does not own a framework's vocabulary.
PyPSA is itself moving to record-wide unique names, so this is a constraint that resolves rather than one to design around.
It is also where bus-keyed connections are collapsed back to a framework's positional encoding, ordered by [`order_key`](read-path.md#owner-map), and where a curve is either translated or reported unbuildable.

The tool's own `Schema` reconciles vocabularies: per entity type, which record attribute a tool's attribute is renamed from, or which several it is computed from.
Since [the schema](schema.md) makes a record's attribute names _declared_ rather than conventional, this maps one declared vocabulary to another.

Frames are built and handed over one entity type at a time, so peak memory is one type's frames rather than the whole model.
