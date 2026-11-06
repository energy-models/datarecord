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

## Abstract

A result becomes an ordinary attribute. A layer has one `attributes/` directory, and `inputs/` and `outputs/` go. `Schema.results`, `RecordLike.outputs`, `Kind` and every `kind="outputs"` go with them. Only a tool knows which attributes it computes. `Tool.results` gives way to `to_datarecord(model, con, results_only=True, base=record)`.

## Motivation

Today a record keeps two vocabularies of one shape. `attributes` and `results` are both mappings of `AttributeSpec`. Each lands in its own directory, `inputs/` or `outputs/`. The split reaches every layer of the code:

- **The schema.** `Schema.results` is a second mapping. Name checks, `spec`, the column check and compatibility each union the two.
- **The protocol.** `RecordLike.outputs` sits beside `attributes`. `LayerData.attributes(kind)` and `attribute(name, kind)` take a `Kind`.
- **The read path.** `outputs/` does not overlay. A node reads its own layer only, so a child loses its parent's results.
- **The working record.** `set(..., kind="outputs")` checks the name against `results`. It skips the label check and keeps staged results apart.
- **The write path.** A result's shape is unchecked beyond `attribute`, `breakpoint` and `value`.
- **The owner map.** It is keyed by kind (`owner_map_uri(id, "inputs")`, `owner_map_<kind>_<id>`), for a kind that is always `inputs`.

The distinction belongs to a framework. PyPSA marks an attribute "Output" in its registry. The record layer stores, validates and resolves that distinction, for a vocabulary it never defines. A reader wanting a solved `marginal_price` must also know which of two mappings holds it.

## Specification

### What goes

| today                                               | becomes                                                          |
| --------------------------------------------------- | ---------------------------------------------------------------- |
| `inputs/<attr>.parquet`, `outputs/<attr>.parquet`   | `attributes/<attr>.parquet`, one directory per layer             |
| `Schema.results`                                    | gone; a result is declared in `Schema.attributes`                |
| the name check across attributes and results        | one name, one entry in `attributes`                              |
| `Schema.spec` reading `attributes` then `results`   | `attributes` alone                                               |
| `results` in compatibility                          | gone; `attributes` versions as before                            |
| `RecordLike.outputs`, `Record.outputs`              | gone; `record.attributes[name]`                                  |
| `Kind`, `attributes(kind)`, `attribute(name, kind)` | `attributes()`, `attribute(name)`                                |
| the read path's [outputs](../layers/resolution.md)  | gone; a result overlays like any attribute, latest layer winning |
| owner maps keyed by kind                            | one owner map: `owner_map_uri(id)`, `owner_map_<id>`             |
| `set(..., kind="outputs")`                          | `set(name, frame)`                                               |
| the name check of a result against `results`        | the attribute check of `set`                                     |
| the label exemption for a result                    | gone; every label must resolve                                   |
| `staged_outputs_<attr>_<id>`                        | the staged attribute table                                       |
| the write path's shape exemption for a result       | gone; a result's file carries its declared coordinates           |
| `outputs/` written only when non-empty              | gone; `attributes/` holds whatever the source has                |
| `Tool.results(model, con)`                          | `to_datarecord(model, con, results_only=True, base=record)`      |

### Loading an old layer

A layer with an `inputs/` or `outputs/` directory is refused when its source is opened:

```text
layer <id> has an `inputs/` directory; a layer holds every attribute in
`attributes/`, results included. Re-export the record.
```

### The tool interface

`Tool.results` goes. `to_datarecord` takes `results_only` and `base`:

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

With `results_only=True` it presents only what the solve produced. That is the result attributes and the [intermediate results](#intermediate-results) they need. It is a thin layer, written as a child of the record the model was built from: `rev.child(layer)`. Nothing is staged through a `WorkingRecord`. Where inputs were edited first, their edits are committed as a child, and the thin layer goes on top of that child.

### Intermediate results

A solve derives structure as well as values: PyPSA's SubNetworks, a cycle matrix. Each is an ordinary dim, entity or attribute the tool writes. A SubNetwork is an `entity` with `ctype="SubNetwork"`, and a line's `sub_network` an attribute naming one. A cycle matrix is an attribute over a `cycle` dim and the lines. The tool's schema declares them like any other name.

On every `results_only` write the tool replaces them. The model does not say what the base held, so the tool reads the intermediate entities and labels in `base`, such as its SubNetwork entities. The thin layer tombstones those it does not write again, then adds the new ones. A re-solve that numbers SubNetworks differently leaves no stale label behind.

### The PyPSA tool

- **It knows its outputs.** `_output_attributes(c)` still reads PyPSA's registry status "Output". `_result_attributes` still maps each to its record name.
- **The schema declares them as attributes.** `_registry_schema` adds them to `attributes`. A name that is an input on another type stays dropped, as today.
- **`build` incorporates every attribute the record holds.** A solved bus's `marginal_price` is copied into the network, as any attribute is.
- **"Output" is read only on import.** It decides what `results_only` presents. It never drops data from `build` or a full export.
- **`to_datarecord` writes outputs as attributes.** A solved network round-trips its results through `attributes/`.

## Rationale

**One mapping, one directory.** A result already has a dtype, dims and a description. Its file already has the long schema. Keeping it apart costs a branch at every reader and buys only the overlay rule.

**Results inherit.** A child that changes nothing a solve reads keeps its parent's solution. A child that changes inputs holds results computed from its parent. That is the same staleness a staged result has today after an input edit.

**The tool owns "output".** PyPSA's registry already says which attributes a solve fills. Only the tool's import reads that. This matches [the module layout](../package.md), which keeps framework knowledge out of core.

**One import, two scopes.** A full export and a results layer differ only in what they present. One `to_datarecord` keeps the rule for each result in one place.

**No label exemption.** Every row of a record names a declared label. A reader joining a result to `dims["entity"]` never meets an orphan.

**Costs.** Every record is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). A child of a solved node shows results its inputs no longer support. A results layer meets full validation, so it carries the labels its results name.

## Usage

Before:

```python
n = PyPSA.build(w)
n.optimize()
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame, kind="outputs")
solved = rev.child(w.edits)  # inputs and results in one layer
solved.record.outputs["marginal_price"]  # a child of solved has none
schema.results["p"]
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
schema.attributes["p"]
```

## Implementation notes

1. `schema.py`: `results`, its name, column and compatibility arms go; `spec` reads `attributes`.
2. `record.py`: `Kind` and `outputs` go; `attributes()` and `attribute(name)` lose `kind`.
3. `layered/sources.py`: reads `attributes/`; a layer with `inputs/` or `outputs/` is refused.
4. `layered/resolve.py`: `_outputs` goes; `map_kinds` becomes one fold; every `kind` parameter goes.
5. `store.py`: `owner_map_uri(id)` drops `kind`.
6. `layered/record.py`: `outputs` goes.
7. `layered/write.py`: writes `attributes/`; the `outputs` arm and the result exemptions go.
8. `mutable.py`: `kind` on `set`, `_validate_result`, `outputs` and the staged outputs tables go.
9. `tools/base.py`: `results` goes; `to_datarecord` takes `results_only` and `base`.
10. `tools/pypsa.py`: as [above](#the-pypsa-tool); `results` folds into `to_datarecord`; intermediates in `base` are replaced as [above](#intermediate-results).
11. Tests: `kind="outputs"` and `outputs` tests in `test_mutable.py`, `test_records.py`, `test_tools.py`, `test_overlay.py`, `test_write.py`, `test_properties.py`, `test_owner_map.py` are rewritten or deleted; `fixtures.py` declares results as attributes.
12. Pages: [format](../storage/format.md), [read path](../layers/resolution.md), [schema](../model/schema.md), [working record](../use/editing.md), [writing](../use/writing.md), [tools](../tools/index.md), [record](../use/reading.md), [layers](../layers/tree.md), [open questions](../open-questions.md), the usage pages, `CHANGELOG.md`.

Checks: no `outputs`, `Kind` or `results` field in `src/`; an old layer is refused naming `attributes/`; a solved PyPSA network round-trips with its results; a child reads its parent's results; a re-solve leaves no stale SubNetwork; `results_only` without `base`, or `base` without it, is refused.

## Rejected Ideas

- **A `result: bool` flag on `AttributeSpec`**: the split again under another name, and still a framework's vocabulary.
- **Keeping `outputs/` beside `attributes/`**: two directories for one shape, and every reader still branching.
- **`build` skipping outputs**: it would drop data the record holds, and a solved record would build unsolved.
- **A `PyPSA.attach(n, w)` staging results**: a thin layer from `to_datarecord` writes them without a `WorkingRecord`.

## Open Issues

- **Stale results after an input edit.** They now inherit, so the open question on staged results widens. Recommend leaving coherence to the caller, as today.
- **Detecting intermediates that no longer fit their inputs.** It needs provenance of which inputs each came from. Deferred.
- **Filtering a record before `build`.** Selecting one period or chosen outputs is a separate future feature, not designed here.
