<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 16: categories are enum dimensions

| DEP      | 16                                                                                                                    |
| -------- | --------------------------------------------------------------------------------------------------------------------- |
| Title    | categories are enum dimensions                                                                                        |
| Author   | Bryn Pickering                                                                                                        |
| Status   | Draft                                                                                                                 |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 15](dep-0015-multi-dims.md) |
| Created  | 2026-10-21                                                                                                            |

## Abstract

A `String` dimension may list its labels with `enum`, as [mathspec#915](https://github.com/energy-models/mathspec/pull/915) proposes for a mathspec dimension. A category is then a label of such a dimension, and a categorizing property is a property `into` it, so datarecord's `Enum` dtype goes.

## Motivation

**A closed set of values is two constructs.** `{Enum: [...]}` is a dtype on a property, and `{into: carrier}` a reference to a dim; both say "one of these labels", and [DEP 13](dep-0013-buses-and-carriers-are-dims.md#a-property-into-a-dim) had to state that one categorizes and the other does not. mathspec has no closed set on `main`; #915, open, adds one as a dimension that lists its labels. A schema that declares `component` as an `Enum` property has no `component` dim for a parameter to range over, a condition to compare against, or a reader to iterate.

## Specification

### An `enum` dimension

```yaml
dimensions:
  entity: { dtype: String }
  component:
    dtype: String
    enum: [Generator, Line, Link, Load, Process, ShuntImpedance, StorageUnit, Store, SubNetwork, Transformer]
  role: { dtype: String, enum: [input, output, attached] }
```

**`enum` lists a `String` dimension's labels**, sibling to its `dtype`, as #915 carries it on `program.Dimension.enum`. It is refused on any other dtype and on a [multi-dim](dep-0015-multi-dims.md).

**An `enum` dimension's labels are the schema's**, not data:

- **Its axis is derived from the list**, in the listed order.
- **`add` and `remove` on it are refused**, naming the schema.
- **No layer writes `dims/component.parquet`.** The writer refuses a layer that does.

Everything else is a dim's: an attribute may range over it, a property may be `into` it, and a value naming a label it does not list is refused where a dangling label is.

### A categorizing property is a property `into` an `enum` dimension

**A property `into` an `enum` dimension, of a dim listed in `partial`, categorizes its subject.** This replaces [DEP 12's rule](dep-0012-properties.md#an-enum-property-of-a-partial-dim-categorizes-its-subject) that an `Enum`-typed property did:

```yaml
properties:
  entity:
    component: { into: component }
    carrier: { into: carrier }
  connection:
    role: { into: role }
```

- **`entity.component` categorizes `entity`**, and `connection.role` categorizes `connection`: a multi-dim is a dim since [DEP 15](dep-0015-multi-dims.md), and a group's property could not.
- **`entity.carrier` does not**: `carrier` lists no labels, so its labels are data. The condition is the same as DEP 12's, an `Enum`, now spelled as a dimension.
- **Its categories are checked at load where they are named**: a key under a `default_by` map, a trait's `on` list or a selector keyword, against the dimension's `enum`. A stored value is checked as any property `into` is.
- `Schema.categorizing(dim)` lists these properties; `attributes_for`, `properties_for` and `default_for` key on them by plain name, as today.

`default_by` and a trait's `on` keep their form; only what their category labels are checked against changes:

```yaml
properties:
  entity:
    sign:
      dtype: Float64
      default: 1
      default_by: { entity.component: { Load: -1, ShuntImpedance: -1 } } # keys: labels of `component`
```

[DEP 17](dep-0017-one-default.md) replaces both forms.

### The `Enum` dtype goes

**A closed set of values is always a dimension**, which is #915's rule for mathspec too. `{Enum: [...]}` is refused at load, naming `enum` on a dimension. An `Enum` dim becomes an `enum` dimension; an `Enum` property becomes a property `into` one.

### PyPSA

| PyPSA tool today                                              | becomes                                                            |
| ------------------------------------------------------------- | ------------------------------------------------------------------ |
| `entity.component: {dtype: {Enum: [...]}}`                    | dim `component` with `enum`; `entity.component: {into: component}` |
| `connection.role: {dtype: {Enum: [input, output, attached]}}` | dim `role` with `enum`; `connection.role: {into: role}`            |

`component` and `role` are not in `partial`: their labels are the schema's.

## Rationale

**A dimension, not a dtype.** A category a model compares against is a label of an index set in mathspec; giving datarecord the same construct is what lets one manifest serve both ([DEP 18](dep-0018-a-manifest-is-a-mathspec.md)). It also leaves one way to say "one of these labels": `into`.

**The schema's labels, not data.** A category is checked at load where it is named. That needs the vocabulary in the schema, as an `Enum` had it; a dimension a layer could grow would make a category in a `default_by` checkable only against the folded record.

**What breaks:**

- Every `Enum` in a manifest is rewritten, and every record re-exported; `dims/` gains no file for an `enum` dimension.
- Adding a PyPSA component type is a schema change, as it was with an `Enum`.

## Usage

```python
record.dims["component"]  # component: the listed labels, in order
record.properties["entity"]  # entity | component | carrier | sign | type
schema.default_for("entity.sign", component="Load")  # -1
```

```python
w.set("p_max_pu", 0.8, component="Generator")  # selector: the category
w.set("p_max_pu", 0.8, component="Genrator")  # refused: `component` lists no such label
w.add(
    "component", ["Hydrogen"]
)  # refused: an `enum` dimension's labels are the schema's
```

## Implementation notes

1. `Dimension.enum`, on `String` dims only; its axis derived; `add`, `remove` and the writer refuse it.
2. `Schema.categorizing` keyed on `into` an `enum` dimension; `Enum` refused at load; `default_by` keys and trait `on` lists checked against the `enum`.
3. The PyPSA tool declares `component` and `role` as `enum` dimensions.
4. Pages, usage, changelog; fixtures re-exported.

Checks that hold once built:

- **A misspelt category fails at load**, in a `default_by`, a trait's `on` and a selector.
- **Every PyPSA default round-trips**: `default_for` agrees with PyPSA's registry per type.
- **Every trait test passes** with `component` an `enum` dimension.

## Rejected Ideas

**Keeping `Enum` beside `enum` dimensions.** Two ways to say "one of these labels", one of them categorizing and one not, by dtype.

**An `enum` dimension a layer may grow.** A category named in a `default_by` could then be checked only against the folded record, not at load.

## Open Issues

- **Whether an `enum` dimension may grow in a layer.** Its labels are the schema's, so a custom PyPSA component is a schema change, not a data edit. That is the cost of checking categories at load.
- **[mathspec#915](https://github.com/energy-models/mathspec/pull/915) is open, not merged.** Nothing here needs mathspec: datarecord can land `enum` on its own. If #915 merges under another key or shape, this DEP follows its spelling so [DEP 18](dep-0018-a-manifest-is-a-mathspec.md) stays possible; if it does not merge, `enum` stays datarecord's and DEP 18 has to propose it upstream itself.
