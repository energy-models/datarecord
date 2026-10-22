<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 20: a schema is built from a mathspec

| DEP      | 20                                                                                                                                                                                                             |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a schema is built from a mathspec                                                                                                                                                                              |
| Author   | Bryn Pickering                                                                                                                                                                                                 |
| Status   | Draft                                                                                                                                                                                                          |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 15](dep-0015-multi-dims.md), [DEP 16](dep-0016-categories-are-enum-dimensions.md), [DEP 17](dep-0017-one-default.md) |
| Created  | 2026-10-22                                                                                                                                                                                                     |
| Replaces | [DEP 18](dep-0018-a-manifest-is-a-mathspec.md)                                                                                                                                                                 |

## Abstract

A datarecord schema is a [mathspec](https://github.com/energy-models/mathspec) plus what datarecord alone reads. `Schema.from_mathspec(spec, partial=..., traits=..., within=...)` builds one, `schema.to_mathspec()` returns the spec unchanged, and a record stores the spec it was built from. Every dimension, attribute, property and result is read off the spec; the arguments carry what a spec cannot say and need not. Mathspec holds nothing datarecord alone reads. The DEP lists the [changes each side needs](#changes-in-mathspec) for the spec to say everything else.

## Motivation

**A framework declares its data twice.** A mathspec says which dimensions exist, which parameters a model reads over which dims with what dtype and fill, which variables it solves for, and how labels relate. A datarecord manifest says the same under other names. Two declarations of one fact drift: a parameter added to the spec and not the manifest cannot be written, and one renamed in the manifest and not the spec is written and never read.

**A record cannot say what model it is data for.** A consumer handed a record knows its attributes but not the math that reads them.

**The spec already says almost everything a schema does.** What it does not say splits in two: what datarecord alone needs, which the caller passes, and what both need and only datarecord can say today, which mathspec gains.

## Specification

### A schema is a spec and its arguments

```python
schema = Schema.from_mathspec(
    spec,
    partial=["entity", "bus", "scenario", "connection"],
    traits={"dispatchable": {"attributes": ["p_max_pu"], "on": {"by": "entity.component", "cases": ["Generator", "Link"]}}},
    within={"snapshot": ["period"]},
)
schema.to_mathspec() == spec  # True
schema.partial  # frozenset({"entity", "bus", "scenario", "connection"})
```

- **`from_mathspec` takes what datarecord alone reads at call time**: `partial`, [`traits`](../schema.md#traits) in [DEP 17](dep-0017-one-default.md)'s form, and [`within`](../schema.md#within-an-axis-inside-an-axis) per dim. Each names only dims, attributes and properties of the spec. Everything else is read from the spec.
- **`to_mathspec` is lossless**: it returns the spec `from_mathspec` was given.
- **`manifest.json` holds the spec and the arguments.** `Schema.math` is the `mathspec.Spec`; its math sections (`constraints`, `objective`, `expressions`, `conditions`, `piecewise`, …) are stored and not read, beyond what the table below infers.
- **A spec with a non-empty `given` is refused.** Fragments are combined with `mathspec.merge` first.
- **mathspec is a core dependency.** It needs only `pydantic`, `pyparsing` and `pyyaml`.

### What a schema reads off the spec

| schema view                          | read from the spec                                                                            |
| ------------------------------------ | --------------------------------------------------------------------------------------------- |
| `Schema.dimensions`                  | `dimensions`, multi-dims included                                                             |
| `Schema.attributes`                  | `parameters`: `dims`, `dtype`, `missing`, `unit`, `description`                               |
| `Schema.properties`                  | each value column of a relation, keyed by the relation's subject                              |
| `Schema.results`                     | `variables` and `expressions`                                                                 |
| a breakpoints attribute              | a parameter over the dim a `piecewise` block names as `over`, stored as a `breakpoint` column |
| a category, a `missing` per category | a dimension's `enum`, and a relation `into` it                                                |

- **A relation is a table of properties.** `link_carrier: {key: link, values: {carrier: carrier}}` is the property `carrier` of `link`, `into` `carrier`. A relation keyed by a multi-dim gives that multi-dim's properties.
- **A dtype is mathspec's**: `float`, `int`, `str`, `bool`, `datetime`. Datarecord stores `datetime` at microsecond precision.
- **A name follows mathspec's grammar**, and is still refused where [datarecord refuses it](../schema.md#names): a leading `_`, or a column datarecord writes.

### Changes in mathspec

Each is needed for the spec to say what a schema reads. Status as of mathspec `main` on 2026-10-22.

| change                                                                                                                                                                  | datarecord side                                             | status                                                                     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------- |
| **`enum` on a `str` dimension**, a label compared to it checked at load                                                                                                 | [DEP 16](dep-0016-categories-are-enum-dimensions.md)        | [mathspec#915](https://github.com/energy-models/mathspec/pull/915), open   |
| **`unit`** on dimensions, parameters, variables, relation values and expressions                                                                                        | as today                                                    | [mathspec#836](https://github.com/energy-models/mathspec/issues/836), open |
| **Multi-dims in `dimensions`**: `over` a list or coordinate→dim map, no `dtype`; named in any `dims`; its coordinates read as a relation's columns                      | [DEP 15](dep-0015-multi-dims.md)                            | not proposed                                                               |
| **A `missing` per category**: `{by, cases, otherwise}` on parameters and relation values; `by` a relation column `into` an `enum` dimension, `cases` checked against it | [DEP 17](dep-0017-one-default.md), in mathspec's addressing | not proposed                                                               |
| **`missing` on a relation value**, a fill or `absent` per column rather than per relation                                                                               | a property's `missing`                                      | not proposed                                                               |

- **Without multi-dims in mathspec**, a spec spells `connection` as a dim with one relation per coordinate, as its PyPSA spec spells `link_output`. That dim is a plain dim to `from_mathspec`, and datarecord cannot cascade a removed bus through it, broadcast a NULL over it, or refuse a tuple naming a missing label.
- **`neutral`** on a parameter, removed by the open [mathspec#894](https://github.com/energy-models/mathspec/pull/894), is read as no fill until then.

Not needed, and each would shorten what a spec author writes: `subject.name` addressing in place of `relation[column]`, and `properties` as the section name in place of `relations`.

### What changes in datarecord

- **The manifest is a spec and the `from_mathspec` arguments.** `Schema.dimensions`, `.attributes`, `.properties` and `.results` are read off them.
- **`default` becomes mathspec's `missing`**: a fill, `refused` or `absent`, or [DEP 17](dep-0017-one-default.md)'s per-category form.
- **The PyPSA tool builds its schema from a spec** rather than from PyPSA's registry; see [PyPSA](#pypsa).
- **CONTRIBUTING's list of core dependencies** gains mathspec.

## Rationale

**The spec as the source, not a second language beside it.** A schema derived from the spec cannot drift from it, and a record carries the math it is data for. Datarecord's manifest keys were a second spelling of what the spec says.

**Datarecord's own keys passed, not stored in mathspec.** Which dims a layer patches label by label, which attributes a trait switches off, and which dims identify a label only within another are decisions mathspec has no use for. Asking mathspec to carry them, or a slot for them, would put datarecord's vocabulary into every spec.

**Results inferred, not marked.** A variable's value and an expression's value are what a solved model hands back; nothing else in the spec is.

**What breaks:**

- Every manifest is rewritten as a spec, and every record re-exported.
- `default` is respelled `missing`.
- Core gains a dependency.

## Usage

```python
spec = mathspec.merge(["pypsa/network.yaml", "pypsa/generator.yaml", "pypsa/link.yaml"])
schema = Schema.from_mathspec(spec, partial=["entity", "bus", "scenario", "connection"])
ws.write(root(schema), uri)

record = Workspace.at(uri).record
record.schema.to_mathspec() == spec  # True
record.attributes["p_nom"]  # parameters["p_nom"]'s data
```

## Implementation notes

1. **Raise the [mathspec changes](#changes-in-mathspec)** not yet proposed, and follow #915 and #836.
2. **Land [DEP 15](dep-0015-multi-dims.md), [DEP 16](dep-0016-categories-are-enum-dimensions.md) and [DEP 17](dep-0017-one-default.md)**, spelled as mathspec takes them.
3. **Build `Schema` from a spec**, with the views derived and the manifest keys gone.
4. **Move the PyPSA tool onto a spec.**

Checks that hold once built:

- **`Schema.from_mathspec(spec, **args).to_mathspec() == spec`**, over mathspec's PyPSA spec merged.
- **A schema round-trips through `manifest.json`.**
- **Removing a parameter from the spec removes the attribute.**

## Rejected Ideas

**One file read by both tools**, with `partial`, `traits` and `within` as keys mathspec carries and ignores, as [DEP 18](dep-0018-a-manifest-is-a-mathspec.md) proposes. Puts datarecord's vocabulary into every spec, for no use mathspec has.

**A `meta` slot in mathspec** for whatever datarecord alone reads. The caller passes it instead.

**Expanding a multi-dim into its coordinates**, `[connection, snapshot]` as `[entity, bus, snapshot]`. mathspec refuses a dim named twice in one `dims`, as `corridor` over `from: bus, to: bus` needs. A variable over the expanded dims is dense over `entity × bus`, which changes the model.

## Open Issues

### Properties with a dtype

**A relation value with a dtype**, `values: {x: float}` beside `values: {carrier: carrier}`, is what a property with `dtype` rather than `into` reads from. mathspec should allow it, and it is not agreed there. Without it, `bus.x` is a parameter over `[bus]` and is read as an attribute.

### PyPSA

**mathspec's PyPSA spec has a dim per component; datarecord's tool has one `entity` dim** categorized by `entity.component`. A schema built from that spec is not what the tool writes. The spec moving to one `entity` dim ([mathspec#843](https://github.com/energy-models/mathspec/issues/843) discusses it) is the largest change in the list: about 4600 lines, every name touched.

### Order

**datarecord orders every dim by row order; mathspec shifts only along `ordered: true` dims.** Whether datarecord reads `ordered` for anything is open.
