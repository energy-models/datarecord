<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 17: results are attributes

| DEP      | 17                                  |
| -------- | ----------------------------------- |
| Title    | results are attributes              |
| Author   | Jonas Hoersch                       |
| Status   | Draft (2026-10-20)                  |
| Requires | [DEP 16](dep-0016-remove-traits.md) |
| Created  | 2026-10-20                          |
| Version  | Unreleased                          |

## Abstract

A result becomes an ordinary attribute. A layer has one `attributes/` directory, and `inputs/` and `outputs/` go. `Schema.results`, `RecordLike.outputs`, `Kind` and every `kind="outputs"` go with them. Only a tool knows which attributes it computes.

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
| the read path's [outputs](../read-path.md#outputs)  | gone; a result overlays like any attribute, latest layer winning |
| owner maps keyed by kind                            | one owner map: `owner_map_uri(id)`, `owner_map_<id>`             |
| `set(..., kind="outputs")`                          | `set(name, frame)`                                               |
| the name check of a result against `results`        | the attribute check of `set`                                     |
| the label exemption for a result                    | gone; every label must resolve                                   |
| `staged_outputs_<attr>_<id>`                        | the staged attribute table                                       |
| the write path's shape exemption for a result       | gone; a result's file carries its declared coordinates           |
| `outputs/` written only when non-empty              | gone; `attributes/` holds whatever the source has                |

### Loading an old layer

A layer with an `inputs/` or `outputs/` directory is refused when its source is opened:

```text
layer <id> has an `inputs/` directory; a layer holds every attribute in
`attributes/`, results included. Re-export the record.
```

### The PyPSA tool

- **It knows its outputs.** `_output_attributes(c)` still reads PyPSA's registry status "Output". `_result_attributes` still maps each to its record name.
- **The schema declares them as attributes.** `_registry_schema` adds them to `attributes`. A name that is an input on another type stays dropped, as today.
- **`build` skips outputs.** It reads no attribute that some type registers as an output. `_add_axis_type` no longer restores `marginal_price`.
- **`to_datarecord` writes outputs as attributes.** A solved network round-trips its results through `attributes/`.
- **`results` returns attributes.** Its frames go to `set(name, frame)` with no `kind`.
- **The tool declares its labels.** A SubNetwork is an `entity` with `ctype="SubNetwork"`. The tool adds those labels before it sets the results that name them.

## Rationale

**One mapping, one directory.** A result already has a dtype, dims and a description. Its file already has the long schema. Keeping it apart costs a branch at every reader and buys only the overlay rule.

**Results inherit.** A child that changes nothing a solve reads keeps its parent's solution. A child that changes inputs holds results computed from its parent. That is the same staleness a staged result has today after an input edit.

**The tool owns "output".** PyPSA's registry already says which attributes a solve fills. `build` asks the tool, not the record. This matches [the module layout](../module-layout.md), which keeps framework knowledge out of core.

**No label exemption.** Every row of a record names a declared label. A reader joining a result to `dims["entity"]` never meets an orphan.

**Costs.** Every record is re-exported, [breaking changes being free](https://github.com/energy-models/datarecord/blob/main/AGENTS.md). A child of a solved node shows results its inputs no longer support. `build` of a solved record yields an unsolved network. A results write meets the full `set` validation, so a tool must add labels first.

## Usage

Before:

```python
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame, kind="outputs")
rev.child(w.edits)
record.outputs["marginal_price"]
schema.results["p"]
```

After:

```python
w.add("entity", sub_networks)  # SubNetwork labels, ctype="SubNetwork"
for attr, frame in PyPSA.results(n, w.con).items():
    w.set(attr, frame)
rev.child(w.edits)
record.attributes["marginal_price"]  # a child of rev reads it too
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
9. `tools/base.py`: `results` documents `set(name, frame)`.
10. `tools/pypsa.py`: as [above](#the-pypsa-tool).
11. Tests: `kind="outputs"` and `outputs` tests in `test_mutable.py`, `test_records.py`, `test_tools.py`, `test_overlay.py`, `test_write.py`, `test_properties.py`, `test_owner_map.py` are rewritten or deleted; `fixtures.py` declares results as attributes.
12. Pages: [format](../format.md), [read path](../read-path.md), [schema](../schema.md), [working record](../working-record.md), [writing](../writing.md), [tools](../tools.md), [record](../record.md), [layers](../layers.md), [open questions](../open-questions.md), the usage pages, `CHANGELOG.md`.

Checks: no `outputs`, `Kind` or `results` field in `src/`; an old layer is refused naming `attributes/`; a solved PyPSA network round-trips; a child reads its parent's results.

## Rejected Ideas

- **A `result: bool` flag on `AttributeSpec`**: the split again under another name, and still a framework's vocabulary.
- **Keeping `outputs/` beside `attributes/`**: two directories for one shape, and every reader still branching.

## Open Issues

- **How the PyPSA tool hands back SubNetwork labels.** `results` returns attribute frames only. Recommend a `PyPSA.attach(n, w)` that adds the labels and sets every result.
- **Stale SubNetwork labels.** A re-solve may number SubNetworks differently, leaving old labels inherited. Recommend the tool deletes the previous SubNetwork entities before adding new ones.
- **Stale results after an input edit.** They now inherit, so the open question on staged results widens. Recommend leaving coherence to the caller, as today.
- **Whether `Tool.results` stays apart from `to_datarecord`.** Recommend keeping it: a results-only layer patches a record without restating its inputs.
