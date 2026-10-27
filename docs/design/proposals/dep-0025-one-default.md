<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 25: one default per value

| DEP      | 25                                                                                                                       |
| -------- | ------------------------------------------------------------------------------------------------------------------------ |
| Title    | one default per value                                                                                                    |
| Author   | Bryn Pickering                                                                                                           |
| Status   | On hold (2026-10-27)                                                                                                     |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 23](dep-0023-multi-dims.md), [DEP 24](dep-0024-categories-are-enum-dimensions.md) |
| Created  | 2026-10-21                                                                                                               |

## Abstract

`default` and `default_by` become one `default`, which takes a value or `{by, cases, otherwise}` to differ per category. The categorizing property moves from a mapping key to a value, so no manifest key holds a `.`. [mathspec#940](https://github.com/energy-models/mathspec/issues/940) proposes the same form for mathspec's `missing`, and this DEP lands with the shape it settles on.

## Motivation

A dotted name stands as a key: `default_by: {entity.ctype: {Load: -1}}` puts an address where a name belongs. A reader of the manifest, mathspec's among them, takes a key as an opaque string, while it parses `entity.ctype` in an expression. One address should be spelled one way. Since [DEP 16](dep-0016-remove-traits.md) removed traits and their `on`, `default_by` is the only key left that holds one.

A datarecord `default` is a mathspec `missing`, and should be spelled as mathspec spells it. A per-category fill written one way in datarecord and another in mathspec would be translated at every boundary between them.

One fill takes two fields: `default` and `default_by` are read in order, and `default_by` may list several categorizing properties, the first match winning. Nothing in PyPSA uses more than one: the tool keys every `default_by` on `entity.ctype`.

## Specification

`default` takes a value, or a mapping with three keys; `default_by` goes:

```yaml
properties:
  entity:
    sign:
      dtype: float
      default:
        by: entity.ctype # a categorizing property
        cases: { Load: -1, ShuntImpedance: -1 } # category: fill
        otherwise: 1 # every other label
```

- `by` names one categorizing property as `subject.name`, as a value and not a key. It must [categorize](../model/properties.md#categories) a dim the value reaches: one its `dims` name, such as `entity`, or one a coordinate of its multi-dim draws on. `connection.role` categorizes `connection` itself, as [DEP 24](dep-0024-categories-are-enum-dimensions.md#a-categorizing-property-of-a-multi-dim) lets it.
- `cases` keys are categories of `by`, checked at load against the `one_of` of the dim `by` refers to.
- `otherwise` is required, and may be `null` for no default. It is what `default` held beside `default_by`.
- A categorizing property takes no per-category `default`, as before: a category defaulted by a category is circular.
- A mapping is always the per-category form. No dtype has a mapping as a value, so `default` is never ambiguous.
- Attributes and properties take it alike. A default is still never stored: a consumer applies it through `default_for`.

In Python, `default` holds the value or a `ByCategory(by, cases, otherwise)` model, which a mapping validates into. A non-finite fill in `cases` or `otherwise` is encoded in `manifest.json` as a plain `default` is.

### PyPSA

| PyPSA tool today                                            | becomes                                                               |
| ----------------------------------------------------------- | --------------------------------------------------------------------- |
| `default: 0, default_by: {entity.ctype: {StorageUnit: -1}}` | `default: {by: entity.ctype, cases: {StorageUnit: -1}, otherwise: 0}` |

## Rationale

`by` is a value because an address belongs where an expression parser reads it. `by` is the keyword mathspec's `sum` uses for "grouped by this", and [mathspec#939](https://github.com/energy-models/mathspec/issues/939) would let mathspec address `entity.ctype` as datarecord does.

One `by` is enough. Every per-category default in PyPSA depends on `ctype` alone, and a fill by two categories reads as a rule of the model rather than a fill of the data.

`otherwise` replaces a `default` beside `cases`, so one field holds the fill, whatever shape it takes, so a reader asks one field and a writer cannot set two that disagree.

What breaks:

- Every `default_by` is rewritten, and every stored manifest that holds one is refused.
- A fill by two categorizing properties is lost.

## Usage

```python
AttributeSpec(
    dtype="float",
    dims={"entity", "snapshot"},
    default={"by": "entity.ctype", "cases": {"StorageUnit": -1}, "otherwise": 0},
)
schema.default_for("p_min_pu", ctype="StorageUnit")  # -1
schema.default_for("p_min_pu", ctype="Generator")  # 0
schema.default_for("p_min_pu")  # 0, a category left out matches no case
```

## Implementation notes

1. `_ValueSpec.default` takes a value or `ByCategory`; `default_by` goes, so pydantic refuses it as an unknown key.
2. The schema checks `by` and `cases` where it checks `default_by` today, and refuses a `default` per category on a categorizing property.
3. `default_for` reads `cases[categories[by's name]]`, else `otherwise`.
4. The PyPSA tool writes the new form.
5. Design and usage pages, changelog; fixtures re-exported.

Checks that hold once built:

- No manifest key holds a `.` in any manifest the tool writes.
- A misspelt category in `cases` fails at load.
- Every PyPSA default round-trips: `default_for` agrees with PyPSA's registry per type.

## Rejected Ideas

A list of `{by, cases}` would keep `default_by`'s several properties. Nothing needs it, and the first-match order is a rule a reader has to know.

`default_by` could stay beside a plain `default`, with its key moved to a value. That is two fields for one fill, where `otherwise` says what `default` did.

## Open Issues

- The DEP is on hold until [mathspec#940](https://github.com/energy-models/mathspec/issues/940) is decided; a different shape there is adopted here.
- mathspec#940 lets a case be `refused` or `absent`, as in `cases: {Line: refused, Generator: absent}`. datarecord's `default` is a value or `null` and has no readings, so a case holding one needs a meaning here first.
- On a mathspec named expression, `cases` maps a case name to `{when, expression}`; here it maps a category to a fill. A reader of both meets one key with two meanings unless mathspec#940 picks another.
