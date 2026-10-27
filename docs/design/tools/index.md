<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tools

A tool translates between a record and one modelling framework, in both directions.
`build` reads any record.
`to_datarecord` presents a framework object as relations on a connection the caller provides.
`verify` reports what the framework cannot take.
A tool holds its framework's vocabulary, including which attributes are results and which attributes each type takes.

[The PyPSA tool](pypsa.md) is the one tool today, and its page covers [results on connections](pypsa.md#results-on-connections), [writing results](pypsa.md#writing-results), [a value its type does not take](pypsa.md#a-value-its-type-does-not-take) and [geometry](pypsa.md#geometry).

```python
class Tool(Protocol):
    name: str
    schema: Schema  # attribute mapping

    def requires(self, record: RecordLike) -> Requirements: ...
    def verify(self, record: RecordLike) -> Requirements: ...  # falsy when usable
    def build(self, record: RecordLike) -> Any: ...
    def to_datarecord(
        self,
        model: Any,
        con: DuckDBPyConnection,
        *,
        results_only: bool = False,
        base: RecordLike | None = None,
    ) -> RecordLike: ...
```

A record is the input to a translation and owns none.
A tool is a module-level singleton reached by importing it, with no registry and no name dispatch.
`build` returns the framework's own type, and nothing in the record layer imports a tool ([the package](../package.md)).
`build` takes a [`RecordLike`](../use/reading.md), so a tool builds from a standalone directory as readily as from a tree of layers and needs no knowledge of layering.
It reads `record.con` for its queries.

`verify` reports what the framework cannot take and the record layer cannot check, such as an entity type the framework does not know.
It returns a `Requirements`, falsy when the record is usable.
`build` raises `UnsupportedRecordError` for a record that `verify` finds unusable.

## Presenting a framework object

`to_datarecord` is the inverse of `build`.
It requires the connection to plan its relations on, and in practice that is `ws.con` of the [workspace](../storage/store.md#the-store) that writes them.
A tool plans relations and has no business with directories, locations or the tree, so it receives a connection alone.
A relation planned on another connection is [rejected at the boundary](../storage/concurrency.md#a-relation-from-another-connection-is-rejected), so a tool that picked its own connection would fail on the first write.

```python
root = store.root(PyPSA.to_datarecord(n, ws.con))
```

The result presents [`Relations`](../use/reading.md#relations) keyed by attribute, each in [the long schema](../storage/format.md#the-long-schema), so they go straight to [a write](../use/writing.md).
The PyPSA tool scans its pandas frames on `con` with `con.from_df`.

A framework holds its values per entity type, so each attribute's relation concatenates its types.
The relations are lazy, so the concatenation is a plan, and nothing materialises until a caller fetches.
The tool builds and hands over relations one entity type at a time, so peak memory is one type's frames.

The protocol asks for lazy relations, and an implementation may be eager.
A tool that reshapes a solved model's in-memory containers has nothing to defer and scans its eager frames.
A tool that can fetch an attribute from a solver on demand may do so, and a caller who wants three attributes of forty then pays for three.
This is [the `Relations` argument](../use/reading.md#relations) on the write side.

A framework's per-port results are [stored on connections](pypsa.md#results-on-connections), like its per-port inputs.
A solve's results are written as a [thin layer](pypsa.md#writing-results).

## The tool's schema

[The schema](../model/schema.md) declares a record's attribute names, so a tool maps one declared vocabulary to another.
The tool's own `Schema` holds that mapping per entity type.
An entry names the record attribute that a tool attribute is renamed from, or the several attributes it is computed from.
An attribute without an entry is read under its own name.
