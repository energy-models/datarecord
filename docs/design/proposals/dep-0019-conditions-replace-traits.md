<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 19: conditions replace traits

| DEP      | 19                                                                                      |
| -------- | --------------------------------------------------------------------------------------- |
| Title    | conditions replace traits                                                               |
| Author   | Bryn Pickering                                                                          |
| Status   | Draft                                                                                   |
| Requires | [DEP 17](dep-0017-one-default.md), [DEP 18](dep-0018-a-manifest-is-a-mathspec.md)       |
| Created  | 2026-10-21                                                                              |
| Replaces | [DEP 3](dep-0003-trait-switches.md)                                                     |

## Abstract

Traits go. Which labels may carry a parameter becomes a `where` on the parameter, in mathspec's condition grammar, and a rule over values becomes one of mathspec's `assumptions`. Datarecord checks both, a `where` when a value is staged and an assumption when a layer is written, so removing traits removes no refusal. This settles [#60][i60].

## Motivation

**A manifest that is a mathspec ([DEP 18](dep-0018-a-manifest-is-a-mathspec.md)) says which labels carry what twice.** `traits` is a key only datarecord reads; a constraint's `where` on `entity.component` says the same thing to the math. A trait renamed in one and not the other drifts.

**A trait cannot state a rule over values.** `p_max_pu` within `[0, 1]` is checked nowhere. mathspec's `assumptions` state it, and leave the check to the consumer attaching data.

**A switch cannot vary.** [DEP 3](dep-0003-trait-switches.md#what-it-opens) left open a switch that differs by period. A condition reading a parameter varies as the parameter does.

## Specification

### What a trait said is a condition

**A trait narrows: `p_max_pu` is carried by generators and links only. That is a `where` on the parameter**, in mathspec's condition grammar. A rule over values rather than labels is an assumption.

| trait after DEP 17                                                               | becomes                                                                              |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `on: {by: entity.component, cases: [Generator, Link]}`, `attributes: [p_max_pu]` | `p_max_pu: {where: "entity.component == 'Generator' OR entity.component == 'Link'"}` |
| an attribute no trait claims, carried by every label                             | a parameter with no `where`                                                          |
| `switch: p_nom_extendable` ([DEP 3](dep-0003-trait-switches.md))                 | `p_nom_max: {where: "p_nom_extendable"}`, a condition on a boolean parameter         |
| `Schema.attributes_for(dim, **categories)`                                       | the same call, evaluating each `where` against the given categories                  |
| `set` refusing an attribute the category does not carry                          | `set` refusing a label its `where` excludes                                          |

```yaml
parameters:
  p_max_pu:
    dims: [entity, snapshot]
    missing: 1
    where: "entity.component == 'Generator' OR entity.component == 'Link'"

assumptions:
  p_max_pu_is_a_fraction: "p_max_pu >= 0 AND p_max_pu <= 1"
```

A parameter's `where` is not on mathspec `main`, where only variables and constraints take one ([changes in mathspec](#changes-in-mathspec)).

### When each is checked

**A `where` is checked per label, when a value is staged.** `set`, `add` and the writer evaluate it against the label's properties and parameters as staged, and refuse a value for a label it excludes, naming the condition. A NULL broadcast reaches only the labels the `where` admits.

**An assumption is checked per write, against the folded record.** It may relate several values a caller stages one at a time, so it is evaluated when a layer is written, not on each `set`. A failing entry refuses the write with `mathspec.program.assumption_message`, the message mathspec defines for it. The assumptions mathspec adds for a `piecewise` block are on `Program.assumptions` beside the file's, and are checked the same way.

**Datarecord evaluates the condition mathspec parses.** mathspec exposes each parsed condition on the program as a typed tree (`program.Mask`, `Assumption.holds`); datarecord compiles it to a narwhals expression over the folded frames. A condition using a construct datarecord cannot compile is refused at schema load, not skipped.

**A category in a condition is checked at load** once [mathspec#915](https://github.com/energy-models/mathspec/pull/915), open, merges and reaches properties ([DEP 18, change 4](dep-0018-a-manifest-is-a-mathspec.md#changes-in-mathspec)): `where: "entity.component == 'Genrator'"` is then refused. Until then a misspelt category excludes every label.

### What goes

`traits`, `Trait`, `Schema.traits`, `switch` and `_carried` go, and `traits` leaves [DEP 18's list of keys](dep-0018-a-manifest-is-a-mathspec.md#one-file) only datarecord reads.

### Changes in mathspec

Status is as of mathspec `main` on 2026-10-21.

1. **`where` on parameters.** A parameter's `where` says which labels may hold a value, in the condition grammar, checked by the consumer that attaches data as assumptions are. On the program it is a `Mask`, as a variable's is. Not proposed.
2. **Conditions as public trees.** On `main`: `program.Mask` and `program.Assumption` are public, and `assumption_message` with them. Nothing to change.

## Rationale

**One mechanism, mathspec's.** A trait and a `where` both say which labels carry a value; one keyed on PyPSA's component types and read only by datarecord, the other in a grammar the math already reads. Keeping the second removes the first.

**Staged versus written.** A `where` reads one label's own properties and parameters, which a `set` has in hand. An assumption may relate values staged by separate calls, so only the written layer can be checked.

**What breaks:**

- Every trait in a manifest is rewritten as a `where`.
- A write can now fail on an assumption over values, which no trait could state. A record written before this DEP may not pass them.

## Usage

```python
w.set(
    "p_max_pu", 0.8, entity="load1"
)  # refused: `entity.component == 'Generator' OR ...` excludes it
schema.attributes_for("entity", component="Store")  # evaluates each `where`
```

## Implementation notes

1. Compile mathspec's condition trees to narwhals.
2. Check a parameter's `where` in `set`, `add` and the writer, and assumptions on write.
3. Rebuild `attributes_for` on `where`.
4. Port each PyPSA trait to a `where`; then drop `Trait`.

Checks that hold once built:

- **Every refusal a trait made, a `where` makes**: the trait tests pass with each trait rewritten as a `where`.
- **A failing assumption refuses the write**, with mathspec's message, and a passing record writes unchanged.

## Rejected Ideas

**Keeping traits beside assumptions.** Two mechanisms for which labels carry which values, one keyed on PyPSA's component types.

## Open Issues

- **What an assumption check costs on a large record.** It is evaluated against the folded record on each write. Scoping it to the values a layer touches would be cheaper and needs the fold to say which those are.
- **Which conditions datarecord compiles.** The grammar has comparisons, `AND`, `OR`, `NOT` and presence; anything a later mathspec adds must be compiled or refused at load.
- **mathspec may not take a parameter's `where`.** If it declines, `where` on a parameter joins `traits` as a key mathspec carries unread, and the math does not see it. If it takes another shape, this DEP follows that spelling.
- **mathspec#915 is open.** If it does not merge, or does not reach properties, a misspelt category in a `where` is not caught at load, and datarecord checks it against the `enum` itself.

[i60]: https://github.com/energy-models/datarecord/issues/60
