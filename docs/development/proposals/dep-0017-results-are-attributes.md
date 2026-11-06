<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 17: results are attributes

| DEP           | 17                                                                                                       |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| Title         | results are attributes                                                                                   |
| Author        | Jonas Hoersch                                                                                            |
| Status        | Implemented (2026-10-20)                                                                                 |
| Requires      | [DEP 16](dep-0016-remove-traits.md)                                                                      |
| Created       | 2026-10-20                                                                                               |
| Version       | Unreleased                                                                                               |
| Superseded-By | [DEP 20](dep-0020-a-layer-restates-an-axis-whole.md) (its SubNetwork as an `entity`, and its tombstones) |

The design landed in [the schema](../../reference/schema.md), [the record format](../../reference/format.md), [the record](../../reference/record.md), [the fold](../../reference/resolution.md) and [the tools reference](../../reference/tools.md). Where they and this DEP differ, follow those pages.

## Abstract

A result becomes an ordinary attribute. A layer has one `attributes/` directory, and `inputs/` and `outputs/` go, with `Schema.results`, `RecordLike.outputs`, `Kind` and every `kind="outputs"`. Only a tool knows which attributes it computes, and `Tool.results` gives way to `to_datarecord(model, con, results_only=True, base=record)`.

## Motivation

A record has two vocabularies of one shape. `attributes` and `results` are both mappings of `AttributeSpec`, and each lands in its own directory, `inputs/` or `outputs/`. The split touches every part of the code:

- `Schema.results` is a second mapping, and the name check, `spec`, the column check and compatibility each take the union of the two.
- `RecordLike.outputs` sits beside `attributes`, and `LayerData.attributes(kind)` and `attribute(name, kind)` take a `Kind`.
- On the read path, `outputs/` does not overlay. A revision reads its own layer only, so a child loses its parent's results.
- `set(..., kind="outputs")` checks the name against `results`, skips the label check and keeps staged results apart.
- The writer checks a result's shape only for `attribute`, `breakpoint` and `value`.
- The owner map is keyed by a kind that is always `inputs`.

A framework draws the distinction. PyPSA marks an attribute "Output" in its registry, and datarecord stores, validates and resolves that mark for a vocabulary it never defines. To read a solved `marginal_price`, you must also know which of two mappings has it.

## Specification

### What goes

| Before                                              | Under this DEP                                                                           |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `inputs/<attr>.parquet`, `outputs/<attr>.parquet`   | `attributes/<attr>.parquet`, one directory per layer                                     |
| `Schema.results`                                    | removed: `Schema.attributes` declares a result                                           |
| the name check across attributes and results        | one name, one entry in `attributes`                                                      |
| `Schema.spec` reading `attributes`, then `results`  | `attributes` alone                                                                       |
| `results` in compatibility                          | removed: `attributes` versions as before                                                 |
| `RecordLike.outputs`, `Record.outputs`              | removed: `record.attributes[name]`                                                       |
| `Kind`, `attributes(kind)`, `attribute(name, kind)` | `attributes()`, `attribute(name)`                                                        |
| the read path's outputs                             | removed: a result overlays like any attribute, and a read returns the latest layer's row |
| owner maps keyed by kind                            | one owner map: `owner_map_uri(id)`, `owner_map_<id>`                                     |
| `set(..., kind="outputs")`                          | `set(name, frame)`, with the attribute check of `set`                                    |
| the label exemption for a result                    | removed: every label must resolve                                                        |
| `staged_outputs_<attr>_<id>`                        | the staged attribute table                                                               |
| the writer's shape exemption for a result           | removed: a result's file has its declared coordinates                                    |
| `outputs/` written only when non-empty              | removed: `attributes/` contains what the source has                                      |
| `Tool.results(model, con)`                          | `to_datarecord(model, con, results_only=True, base=record)`                              |

### Loading an old layer

When a source opens a layer with an `inputs/` or `outputs/` directory, datarecord refuses it:

```text
layer <id> has an `inputs/` directory; a layer contains every attribute in
`attributes/`, results included. Re-export the record.
```

### The tool interface

`Tool.results` goes, and `to_datarecord` takes `results_only` and `base`:

```python
def to_datarecord(
    self,
    model: Any,
    con: DuckDBPyConnection,
    *,
    results_only: bool = False,
    base: RecordLike | None = None,
) -> RecordLike: ...
```

`base` is the record the model was built from. It is required with `results_only=True` and refused otherwise.

With `results_only=True`, `to_datarecord` presents only what the solve produced: the result attributes and the intermediate results they need. The tool writes this thin layer as a child of the record the model was built from, `rev.child(layer)`, without staging it through a `WorkingRecord`. Where inputs were edited first, their edits are committed as a child, and the thin layer goes on top of that child.

### Intermediate results

A solve derives structure as well as values, such as PyPSA's SubNetworks or a cycle matrix. Each is an ordinary dim, entity or attribute that the tool writes and its schema declares. A SubNetwork is an `entity` with `ctype="SubNetwork"`, and a line's `sub_network` is an attribute whose value is one such entity. A cycle matrix is an attribute over a `cycle` dim and the lines.

On every `results_only` write the tool replaces them. The model does not record what the base had, so the tool reads the intermediate entities and labels in `base`, such as its SubNetwork entities. The thin layer tombstones those it does not write again, then adds the new ones. A re-solve that numbers SubNetworks differently leaves no stale label behind.

### The PyPSA tool

- `_output_attributes(c)` still reads PyPSA's registry status "Output", and `_result_attributes` still maps each output to its record name.
- `_registry_schema` adds the outputs to `attributes`. A name that is an input on another type stays dropped.
- `build` copies every attribute the record has into the network, so a solved bus's `marginal_price` arrives as any attribute does.
- The tool reads "Output" only on import, to decide what `results_only` presents. It never drops data from `build` or from a full export.
- `to_datarecord` writes outputs as attributes, so a solved network round-trips its results through `attributes/`.

## Rationale

A result already has a dtype, dims and a description, and its file already has the long schema. Keeping it apart needs a branch in every reader, and gives only the overlay rule in return.

A child inherits its parent's results. A child that changes nothing a solve reads keeps its parent's solution. A child that changes inputs has results computed from its parent, which is the staleness a staged result already has after an input edit.

Only the tool deals with "output". PyPSA's registry already lists which attributes a solve fills, and only the tool's import reads that. This follows the [package layout](../package.md), under which framework knowledge stays out of core.

A full export and a results layer differ only in what they present. With one `to_datarecord`, the rule for each result sits in one place.

Every row of a record sets only declared labels, so a reader that joins a result to `dims["entity"]` never finds an orphan.

The costs: every record is re-exported, as [breaking changes are free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). A child of a solved revision shows results that its inputs no longer support. datarecord validates a results layer in full, so it must contain the labels that its results set.

## Usage

Before:

```python
n = PyPSA.build(w)
n.optimize()
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame, kind="outputs")
solved = rev.child(w.edits)  # inputs and results in one layer
solved.record.outputs["marginal_price"]  # a child of solved has none
```

After:

```python
inputs = rev.child(w.edits)
n = PyPSA.build(inputs.record)
n.optimize()
solved = inputs.child(
    PyPSA.to_datarecord(n, w.con, results_only=True, base=inputs.record)
)
solved.record.attributes["marginal_price"]  # a child of solved reads it too
PyPSA.build(solved.record)  # marginal_price is in the network
```

## Implementation notes

1. `schema.py`: `results` and its name, column and compatibility branches go, and `spec` reads `attributes`.
2. `record.py`: `Kind` and `outputs` go, and `attributes()` and `attribute(name)` lose `kind`.
3. `layered/sources.py` reads `attributes/` and refuses a layer with `inputs/` or `outputs/`.
4. `layered/resolve.py`: `_outputs` goes, and `map_kinds` becomes one fold without a `kind` parameter.
5. `store.py`: `owner_map_uri(id)` drops `kind`.
6. `layered/record.py`: `outputs` goes.
7. `layered/write.py` writes `attributes/`, and the `outputs` branch and the result exemptions go.
8. `mutable.py`: `kind` on `set`, `_validate_result`, `outputs` and the staged outputs tables go.
9. `tools/base.py`: `results` goes, and `to_datarecord` takes `results_only` and `base`.
10. `tools/pypsa.py`: `results` folds into `to_datarecord`, which replaces the intermediates in `base`.
11. Tests of `kind="outputs"` and `outputs` are rewritten or deleted, and `fixtures.py` declares results as attributes.
12. Pages, usage pages and `CHANGELOG.md`.

Checks: no `outputs`, `Kind` or `results` field in `src/`. An old layer is refused with `attributes/` named. A solved PyPSA network round-trips with its results, and a child reads its parent's results. A re-solve leaves no stale SubNetwork. `results_only` without `base`, or `base` without it, is refused.

## Rejected ideas

- A `result: bool` flag on `AttributeSpec`: the split again under another name, and still a framework's vocabulary.
- `outputs/` kept beside `attributes/`: two directories for one shape, and every reader still branching.
- `build` skipping outputs: it would drop data the record has, and a solved record would build unsolved.
- A `PyPSA.attach(n, w)` that stages results: a thin layer from `to_datarecord` writes them without a `WorkingRecord`.

## Open issues

- Stale results after an input edit. Results now inherit, so the open question on staged results widens. The recommendation is that the caller, not datarecord, keeps results consistent with their inputs.
- Detecting intermediates that no longer fit their inputs. This needs provenance of which inputs each came from, and is deferred.
- Filtering a record before `build`, such as one period or chosen outputs. This is a separate feature, not designed here.
