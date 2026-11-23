<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tools

This page describes the `Tool` protocol, which every tool implements.
A tool is a module under `datarecord.tools` that translates between a [record](record.md) and one modelling framework.
For the rules of the PyPSA tool, see [the PyPSA tool reference](../pypsa/reference.md).

## The `Tool` protocol

```python
class Tool(Protocol):
    name: str
    schema: Schema

    def requires(self, record: RecordLike) -> Requirements: ...
    def verify(self, record: RecordLike) -> Requirements: ...
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

| Member          | Does                                                                                    |
| --------------- | --------------------------------------------------------------------------------------- |
| `name`          | is the framework's name, which error messages quote                                     |
| `schema`        | is the map from the tool's attribute names to the record's ([below](#the-tools-schema)) |
| `requires`      | returns what the tool needs from this record: dims, categories and attributes           |
| `verify`        | returns what the record fails to supply, falsy when the tool can build it               |
| `build`         | returns the framework's own object, built from any `RecordLike`                         |
| `to_datarecord` | presents a framework object as a `RecordLike` whose relations are planned on `con`      |

Each tool's module defines the tool as a module-level object, such as `PyPSA`, and nothing outside `datarecord.tools` imports a tool.
`build` reads `record.con` for its queries, so it builds from a standalone directory as it does from a revision.
`build` raises `UnsupportedRecordError` for a record that `verify` finds unusable.

`to_datarecord` plans its relations on the connection it receives, which is `ws.con` of the [workspace](store.md) that writes them.
Its result goes straight to `store.commit` or `ws.write`.
`base` is the record the layer is committed over as a child, and the record the model was built from.
With `results_only=True` it presents only what a solve produced, as a layer over `base`.
It raises `ValueError` where `results_only` comes without `base`.

### `Requirements`

`Requirements` is what `requires` and `verify` return, with one set per kind of fault.

| Field                | Contains                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `dims`               | dims the tool needs and the schema does not declare                                                               |
| `categories`         | categories the framework has no counterpart for                                                                   |
| `attributes`         | `(category, attribute)` pairs with no value, named in the record's words                                          |
| `unsupported_keys`   | `(key, dim)` pairs of a `partial` the framework cannot take                                                       |
| `unsupported_dims`   | dims the record declares that the framework's model has no place for                                              |
| `unsupported_values` | `(category, attribute)` pairs stored as a curve, or varying along a dim the framework holds the attribute without |
| `names`              | labels that two of the framework's categories share                                                               |
| `ignored`            | `(category, attribute)` pairs that the record stores and the category does not take                               |

A `Requirements` is truthy when any field but `ignored` has an entry.
`a | b` returns a `Requirements` with the union of each field.
`describe()` returns a one-line summary, which `UnsupportedRecordError` quotes.

### The tool's schema

A tool's `Schema` lists, per category, the tool attributes that are renamed or computed, each an `Attr` that lists its source attributes.
An attribute with no entry keeps its name, so an empty `Schema` maps every name to itself.
`Attr.resolve` [settles](record.md) each source attribute before it renames or computes, so a computed attribute combines one row per point of each source.

!!! info "See also"

    [Present a framework object as a record](../howto/present-a-framework-object.md) shows a tool author's `to_datarecord`.
    [The PyPSA tool](../pypsa/reference.md) describes what the PyPSA tool reads from a record and writes back.
    [The API reference](api/tools.md) lists every signature.
