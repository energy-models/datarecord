<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 17: one default per property

| DEP      | 17                                                                                         |
| -------- | ------------------------------------------------------------------------------------------ |
| Title    | one default per property                                                                   |
| Author   | Bryn Pickering                                                                             |
| Status   | Draft                                                                                      |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 16](dep-0016-categories-are-enum-dimensions.md)     |
| Created  | 2026-10-21                                                                                 |

## Abstract

`default` and `default_by` become one `default`, which takes a value or `{by, cases, otherwise}` to differ per category. A trait's `on` takes the same `{by, cases}` shape. The categorizing property moves from a YAML key to a value, so no YAML key holds a `.`.

## Motivation

**A dotted name stands as a YAML key.** `default_by: {entity.component: {Load: -1}}` and `on: {entity.component: [Generator]}` put an address where a name belongs. A YAML reader, mathspec included, takes the key as an opaque string; `entity.component` in an expression is parsed. The same address should be spelled one way.

**Two fills for one value.** `default` and `default_by` are read in order, and `default_by` may list several categorizing properties, the first match winning. Nothing in PyPSA uses more than one.

## Specification

### One `default`, per category

**`default` takes a value, or a mapping with three keys**; `default_by` goes:

```yaml
properties:
  entity:
    sign:
      dtype: Float64
      default:
        by: entity.component # a categorizing property
        cases: { Load: -1, ShuntImpedance: -1 } # category: fill
        otherwise: 1 # every other label
```

- **`by` names one categorizing property**, as a value and not a key. It must categorize a dim the value reaches: `entity` directly, or through `connection.entity` for a value over `connection`.
- **`cases` keys are categories of `by`**, checked against its dimension's `enum` at load, as [DEP 16](dep-0016-categories-are-enum-dimensions.md#a-categorizing-property-is-a-property-into-an-enum-dimension) checks `default_by` keys.
- **`otherwise` is required.** It is what `default` held beside `default_by`.
- **One `by`.** `default_by` allowed several, the first match winning; a fill depending on two categorizing properties is not expressible.
- **Never on a categorizing property**, as before: a category defaulted by a category is circular.
- The same form applies to attributes and properties alike, and a default is still never stored: a reader fills it through `default_for`.

### A trait's `on` takes `by`

Traits stay, and their `on` loses its dotted key:

```yaml
traits:
  dispatchable:
    attributes: [p_min_pu, p_max_pu, p_set]
    on: { by: entity.component, cases: [Generator, Link] }
```

`by` is one categorizing property, as `on` already allowed; `cases` lists its categories, checked against the `enum`. [A trait narrows; it does not grant](../schema.md#traits) holds unchanged.

### PyPSA

| PyPSA tool today                                                | becomes                                                                   |
| --------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `default: 0, default_by: {entity.component: {StorageUnit: -1}}` | `default: {by: entity.component, cases: {StorageUnit: -1}, otherwise: 0}` |
| `on: {entity.component: [Generator]}`                           | `on: {by: entity.component, cases: [Generator]}`                          |

## Rationale

**`by` as a value.** An address belongs where an expression parser reads it. `by` is the keyword mathspec's `sum` and `at` use for "grouped by this", as in `sum(x, over=link, by=Link_carrier[carrier])` on mathspec `main`; `cases` and `otherwise` are the keys of its cased expressions. The dotted `by=link.carrier` is not on mathspec `main`: [DEP 18](dep-0018-a-manifest-is-a-mathspec.md#changes-in-mathspec) proposes it.

**`otherwise`, not `default` beside `cases`.** One key holds the fill, whatever shape it takes, so a reader asks one field and a writer cannot set two that disagree.

**What breaks:**

- Every `default_by` and every trait's `on` is rewritten.
- A fill by two categorizing properties is lost.

## Usage

```python
schema.default_for("entity.sign", component="Load")  # -1
schema.default_for("entity.sign", component="Generator")  # 1
```

## Implementation notes

1. `default` takes `{by, cases, otherwise}`; `default_by` refused at load, naming the form; `default_for` reads it.
2. A trait's `on` takes `{by, cases}`; the dotted key refused at load, naming the form.
3. The PyPSA tool writes the new forms.
4. Pages, usage, changelog; fixtures re-exported.

Checks that hold once built:

- **No YAML key holds a `.`** in any manifest the tool writes.
- **A misspelt category fails at load**, in a `default`'s `cases` and a trait's `on`.
- **Every PyPSA default round-trips**: `default_for` agrees with PyPSA's registry per type.
- **Every trait test passes** with `on` rewritten.

## Rejected Ideas

**A list of `{by, cases}`**, keeping `default_by`'s several properties. Nothing needs it, and the first-match order is a rule a reader has to know.

**`default_by` kept beside a plain `default`**, with its key moved to a value. Two fields for one fill, where `otherwise` says what `default` did.

## Open Issues

- **Whether one `by` is enough.** A list of `{by, cases}` would add `default_by`'s several properties back if something needs them.
- **mathspec has no per-category fill.** Nothing here needs one: datarecord fills a default itself. [DEP 18](dep-0018-a-manifest-is-a-mathspec.md#changes-in-mathspec) proposes the same shape for mathspec's `missing`; if mathspec takes another shape, DEP 18 adopts it and this form is respelled there.
