<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 18: a manifest is a mathspec

| DEP      | 18                                                                                                                                                                                                             |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Title    | a manifest is a mathspec                                                                                                                                                                                       |
| Author   | Bryn Pickering                                                                                                                                                                                                 |
| Status   | Rejected 2026-10-22                                                                                                                                                                                                          |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md), [DEP 15](dep-0015-multi-dims.md), [DEP 16](dep-0016-categories-are-enum-dimensions.md), [DEP 17](dep-0017-one-default.md) |
| Created  | 2026-10-21                                                                                                                                                                                                     |

## Abstract

A datarecord manifest and a [mathspec](https://github.com/energy-models/mathspec) become one file in one language. Datarecord reads its dims, properties, inputs and results from the keys mathspec already has, and mathspec accepts the few keys only datarecord gives meaning to. [DEP 15](dep-0015-multi-dims.md)'s multi-dims, [DEP 16](dep-0016-categories-are-enum-dimensions.md)'s `enum` dimensions and [DEP 17](dep-0017-one-default.md)'s single `default` bring datarecord's constructs to mathspec's; this DEP makes every mathspec relation a property and adopts mathspec's spellings. Traits stay a key only datarecord reads; [DEP 19](dep-0019-conditions-replace-traits.md) replaces them with mathspec's conditions. The [changes mathspec needs](#changes-in-mathspec) are listed, most of them not yet proposed there; this DEP lands once they do ([issue #61](https://github.com/energy-models/datarecord/issues/61)).

## Motivation

**A framework declares its data twice.** A mathspec says which dimensions exist, which parameters a model reads over which dims with what dtype and fill, which variables it solves for, and how labels relate. A datarecord manifest says the same under other names:

| datarecord after DEP 17                        | mathspec today                                       |
| ---------------------------------------------- | ---------------------------------------------------- |
| `dimensions[d].dtype`                          | `dimensions[d].dtype`                                |
| `attributes[a].dims`, `.dtype`                 | `parameters[a].dims`, `.dtype`                       |
| `attributes[a].default`                        | `parameters[a].missing`, when a value                |
| `results[r]`                                   | `variables[r]`; constraint duals                     |
| `properties.bus.carrier: {into: carrier}`      | `relations.bus_carrier: {key: bus, values: carrier}` |
| `dimensions.connection: {over: [entity, bus]}` | `relations.connection: {key: [entity, bus]}`         |
| `attributes[a].breakpoints`                    | the `over` dim of a `piecewise` block                |

Two declarations of one fact drift. A parameter added to the spec and not the manifest cannot be written; one renamed in the manifest and not the spec is written and never read. The PyPSA tool builds its schema by hand from PyPSA's registry, and nothing checks it against the spec that consumes the record.

**A record cannot say what model it is data for.** A consumer handed a record knows its attributes but not the math that reads them, and nothing confirms the two agree.

**The two languages are one construct apart.** After DEP 15 and DEP 16, datarecord has multi-dims and `enum` dimensions; mathspec has neither on `main`, and `enum` only in the open [mathspec#915](https://github.com/energy-models/mathspec/pull/915). What is left is that mathspec spells a value per label as a `relation` where datarecord spells it as a property, and that the two name the same things differently.

## Specification

### One file

```yaml
version: 1

dimensions:
  entity: { dtype: str }
  bus: { dtype: str }
  carrier: { dtype: str }
  period: { dtype: int, ordered: true }
  snapshot: { dtype: datetime, ordered: true }
  scenario: { dtype: str }
  component: { dtype: str, enum: [Generator, Line, Link, Load, Store, StorageUnit] }
  role: { dtype: str, enum: [input, output, attached] }
  connection: # a multi-dim: its labels are (entity, bus) tuples
    over: { entity: entity, bus: bus }
    description: An entity's attachment to one bus.

properties:
  entity:
    component: { into: component }
    carrier: { into: carrier }
    sign:
      dtype: float
      missing: { by: entity.component, cases: { Load: -1, ShuntImpedance: -1 }, otherwise: 1 }
  bus:
    carrier: { into: carrier, missing: AC }
    x: { dtype: float, unit: "°" }
  snapshot:
    period: { into: period }
  connection:
    role: { into: role }

parameters:
  p_nom: { dims: [entity], missing: 0, unit: MW }
  p_max_pu: { dims: [entity, snapshot], missing: 1 }
  efficiency: { dims: [connection, snapshot], missing: 1 }
  v_nom: { dims: [bus], missing: 1, unit: kV }

variables:
  p: { dims: [connection, snapshot], unit: MW }

constraints:
  nodal_balance:
    dims: [bus, snapshot]
    expression: sum(p, over=connection, by=connection.bus) == 0
    result: true # its dual is stored, as PyPSA's marginal_price

assumptions:
  p_max_pu_is_a_fraction: "p_max_pu >= 0 AND p_max_pu <= 1"

traits:
  dispatchable:
    attributes: [p_max_pu]
    on: { by: entity.component, cases: [Generator, Link] }

partial: [entity, bus, scenario, connection]
meta: {}
```

**Every key has one meaning, and both tools know every key.** mathspec stays strict: a key neither tool declares is refused. A key only one tool acts on is still validated for shape by both, so a typo in `partial` fails in mathspec and a typo in `constraints` fails in datarecord.

| key                                                                    | datarecord reads it as                                               | mathspec reads it as                       |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------ |
| `dimensions`                                                           | the axes, the multi-dims and the `enum` dimensions                   | the index sets                             |
| `properties`                                                           | [properties](../schema.md#properties), stored on the subject's table | values per label, addressed `subject.name` |
| `parameters`                                                           | the inputs, in `inputs/`                                             | the data a model reads                     |
| `variables`                                                            | the results, in `outputs/`                                           | the decision variables                     |
| `constraints`, `expressions` with `result: true`                       | results too                                                          | as today                                   |
| `objective`, `conditions`, `macros`, `piecewise`, `sos`, `assumptions` | nothing; stored with the manifest                                    | as today                                   |
| `partial`, `meta`, `within`, `traits`                                  | as today                                                             | nothing                                    |
| `unit`, `description`                                                  | stored, not interpreted                                              | stored, not interpreted                    |

`traits` keeps [DEP 17](dep-0017-one-default.md#a-traits-on-takes-by)'s form until [DEP 19](dep-0019-conditions-replace-traits.md) replaces it. `attributes` and `results` go as manifest keys; `Schema.attributes` and `Schema.results` stay as the read API, derived from the table above.

### Properties of a dimension

**`properties` is a top-level section keyed by subject**, as [DEP 12](dep-0012-properties.md) declares it. A subject is a dimension or a multi-dim, and under it each entry names one property:

```yaml
properties:
  <subject>: # a key of `dimensions`
    <name>: # unique within the subject; addressed `<subject>.<name>`
      into: <dim> # its values are labels of <dim>; or
      dtype: <dtype> # a plain value: float, int, str, bool, date, datetime
      missing: <fill> # a value, refused or absent; or {by, cases, otherwise}
      unit: <str>
      description: <str>
```

- **Exactly one of `into` and `dtype`.** `into` makes the property a reference: each value must be a live label of the named dim, checked as [DEP 13](dep-0013-buses-and-carriers-are-dims.md#a-property-into-a-dim) checks it. `dtype` makes it a plain value.
- **No `dims`.** A property is one value per label of its subject and never varies along another axis. A value that does is a parameter.
- **`missing: refused`** says every label must have a value; `absent` that a label may have none. A value is the fill, as `default` was.
- **`missing: {by, cases, otherwise}`** is [DEP 17's per-category `default`](dep-0017-one-default.md#one-default-per-category), renamed; any fill, `refused` or `absent` included, may stand in `cases` or `otherwise`. The same form applies to a parameter's `missing`.
- **A coordinate of a multi-dim is not declared here.** `connection.entity` and `connection.bus` come from `over`, as [DEP 15](dep-0015-multi-dims.md#coordinates-are-addressed-as-properties-are) declares.

Datarecord stores a property as a column of its subject's table, `dims/<subject>.parquet`, and hands it over through `record.properties[subject]`, as today. mathspec reads it as a value per label of its subject:

| property             | in a condition, `where` or `holds` | in an expression                                                |
| -------------------- | ---------------------------------- | --------------------------------------------------------------- |
| `into` (`link.bus0`) | `link.bus0 != link.bus1`           | `by=link.bus0`, `at(x, by=link.bus0)`, `within=snapshot.period` |
| `dtype` (`bus.x`)    | `bus.x > 0`                        | a parameter over `[bus]`                                        |

**A category is a label of an `enum` dimension**, as [DEP 16](dep-0016-categories-are-enum-dimensions.md) declares, and #915, once merged, refuses at load a condition comparing a reference to that dimension with any other label. So a typo in a category fails at load in both tools: a constraint's `where: "entity.component == 'Genrator'"` is refused by mathspec, and `cases: {Genrator: 1}` under `missing` by both.

### Properties replace relations

A mathspec relation is a property of its key, or of a multi-dim naming its key:

| mathspec today                                      | becomes                                                   |
| --------------------------------------------------- | --------------------------------------------------------- |
| `Link_carrier: {key: link, values: carrier}`        | `properties.link.carrier: {into: carrier}`                |
| `ends: {key: line, values: {bus0: bus, bus1: bus}}` | `properties.line.bus0: {into: bus}`, `.bus1: {into: bus}` |
| `zone_of: {key: [plant, period], values: zone}`     | multi-dim `plant_period`, property `zone: {into: zone}`   |
| `connection: {key: [entity, bus]}` (bare)           | multi-dim `connection: {over: [entity, bus]}`             |
| `missing: refused \| absent` on a relation          | `missing` on the property                                 |
| `sum(x, over=link, by=Link_carrier[carrier])`       | `sum(x, over=link, by=link.carrier)`                      |
| `ends[bus0] != ends[bus1]` in a `where`             | `line.bus0 != line.bus1`                                  |

A property that is not `into` a dim, such as `bus.x`, is a value per label of its subject. In math it reads as a parameter over `[bus]`, addressed `bus.x`. Whether a value is a property or a parameter stays [the author's declaration](../schema.md#properties): it decides where datarecord stores it, not what the math can do with it.

### Shared spellings

Where the two differ after DEP 17, one spelling is picked:

| concept                 | datarecord after DEP 17                                           | mathspec today                                         | both                                              |
| ----------------------- | ----------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------- |
| dtypes                  | `Float64`, `Int64`, `String`, `Boolean`, `Date`, `{Datetime: us}` | `float`, `int`, `str`, `bool`, `datetime`              | `float`, `int`, `str`, `bool`, `date`, `datetime` |
| fill for an unset value | `default`                                                         | `missing` (a value, or `refused`, `absent`, `neutral`) | `missing`                                         |
| fill per category       | `default: {by, cases, otherwise}`                                 | none                                                   | `missing: {by, cases, otherwise}`                 |
| inputs                  | `attributes`                                                      | `parameters`                                           | `parameters`                                      |
| outputs                 | `results`                                                         | `variables`                                            | `variables`, and `result: true`                   |
| a multi-coordinate set  | `dimensions`, `over`                                              | `relations`, `key`                                     | `dimensions`, `over`                              |
| a value per label       | `properties`, `into`                                              | `relations`, `values`                                  | `properties`, `into`                              |
| a curve's axis          | `AttributeSpec.breakpoints`                                       | `piecewise.over`                                       | `breakpoints: true` on the dimension              |

`neutral` is on mathspec `main`; the open [mathspec#894](https://github.com/energy-models/mathspec/pull/894) removes it, and nothing here needs it.

Datarecord stores a breakpoint dim as the `breakpoint` column of a long row, not as a dim column, so a parameter over a `breakpoints` dim is the [`breakpoints=True` attribute](../schema.md#attributespec) of today, read with that dim dropped from its `dims`.

### Traits stay datarecord's

**`traits` is a key only datarecord reads**, as `partial` is; mathspec validates its shape and carries it. [DEP 19](dep-0019-conditions-replace-traits.md) replaces it with mathspec's conditions.

### Who owns the language

**mathspec owns the schema of the file; datarecord depends on it.** `mathspec.to_spec` parses and validates a manifest, and `Schema` wraps the resulting `mathspec.Spec` and derives its views from it. Two pydantic models of one file would be the drift this DEP removes, one level down. mathspec needs only `pydantic`, `pyparsing` and `pyyaml`.

`Schema.spec` is the `mathspec.Spec`. A framework building a model from a record takes the spec from it rather than being handed one, and the record's inputs are by construction the spec's parameters.

A manifest is self-contained: it is one resolved file, fragments being combined with `mathspec.merge` before writing, and a spec with a non-empty `given` is refused.

### Changes in mathspec

Each of these is an issue to raise on mathspec, unless it names a pull request. Datarecord implements this DEP once they are released. Status is as of mathspec `main` on 2026-10-21. They assume [mathspec#915](https://github.com/energy-models/mathspec/pull/915), open, is merged as it stands: `enum` on a `str` dimension, carried on `program.Dimension.enum`, and a label compared to it checked at load.

1. **Multi-dims in `dimensions`.** A dimension with `over` (a list, or a coordinate→dim map) and no `dtype`. It may appear in any `dims`, is iterated as tuples, and its coordinates are addressed `connection.bus` wherever a property may be. The datarecord side is [DEP 15](dep-0015-multi-dims.md). Not proposed.
2. **`properties` replace `relations`.** A per-subject section, where the subject is a dimension or multi-dim. An entry is `into: <dim>` (its values are that dim's labels) or a `dtype`, with `missing`, `unit`, `description`. `relations` is removed. Not proposed.
3. **Dotted addressing.** `subject.name` reads a property or coordinate in `by=`, `at`, `within=`, `where` strings and expressions. `.` becomes reserved in names. The PyPSA example's prefixes (`Link_bus0`, `Link_carrier`) become `link.bus0`, `link.carrier`. Not proposed: [mathspec#516](https://github.com/energy-models/mathspec/issues/516) compared relation spellings, and its dotted proposal, #509, was closed.
4. **`#915`'s label check reaches properties.** It checks a `RelationComparison` in #915; `entity.component == 'Generator'` is the same comparison once the relation is a property `into` an `enum` dimension, as [DEP 16](dep-0016-categories-are-enum-dimensions.md) has it on the datarecord side. Also a `date` dtype. Builds on #915; the rest not proposed.
5. **A per-category `missing`.** `missing` takes a value, a keyword, or `{by, cases, otherwise}` on parameters and properties. `by` is a property `into` an `enum` dimension of a dim the entry reaches; `cases` keys are checked against that `enum` at load; a reader fills an unset value from the label's case, else `otherwise`. The datarecord side is [DEP 17](dep-0017-one-default.md). Not proposed.
6. **`breakpoints: true` on a dimension.** A `piecewise` block's `over` must name one; an ordered numeric dtype is implied. Not proposed.
7. **`result: true` on constraints and expressions.** Marks a dual or value a consumer stores. mathspec gives it no meaning. Not proposed.
8. **Keys with no math meaning.** `partial`, `meta` and `traits` at the top level, `within` on a dimension, `unit` on dimensions, parameters, variables, properties and expressions. Validated for shape, carried, otherwise ignored. `unit` is asked for in [mathspec#836](https://github.com/energy-models/mathspec/issues/836), open, with no pull request; the rest not proposed.
9. **A public model of the file.** `Spec` is what datarecord parses with, so its fields, the ones above included, are part of mathspec's public API. On `main`: `mathspec.spec` is public since [mathspec#878](https://github.com/energy-models/mathspec/pull/878); the new fields join it.
10. **A version bump.** `version: 1`, since `relations` goes and dtype spellings change. Not proposed.
11. **The PyPSA spec over one `entity` dim.** See [one entity dim](#pypsa-one-entity-dim). Not proposed; [mathspec#843](https://github.com/energy-models/mathspec/issues/843), open, discusses deriving datarecord's schema from mathspec's PyPSA files.

### PyPSA: one entity dim

**PyPSA's components are labels of one `entity` dim, categorized by `entity.component`**, as datarecord's PyPSA tool has them since [DEP 13](dep-0013-buses-and-carriers-are-dims.md). mathspec's PyPSA spec moves to that shape; the tool does not move to mathspec's.

| mathspec's PyPSA spec today                    | becomes                                                                             |
| ---------------------------------------------- | ----------------------------------------------------------------------------------- |
| dims `generator`, `link`, `line`, `store`, …   | one dim `entity`; `component: {enum: [...]}`; `entity.component`                    |
| `Generator_p_nom`, `Link_p_nom`                | `p_nom: {dims: [entity]}`                                                           |
| `Link_bus0`, `Link_output_bus`, `Line_bus0`, … | the multi-dim `connection` over `(entity, bus)`, with `connection.role`             |
| `link_output` and its two relations            | `connection`, `role: output`                                                        |
| `Link_efficiency` over `link_output`           | `efficiency: {dims: [connection]}`                                                  |
| a per-type sum, `sum(Link_p, over=link, …)`    | `sum(p, over=entity, …)` in a constraint with `where: "entity.component == 'Link'"` |
| a name one type has, `Store_e_nom`             | `e_nom: {dims: [entity]}`, carried by stores only through a trait                   |

- **A name shared by types is one parameter.** Its fill may differ per type through a per-category `missing`; which types may carry it is a trait, and a `where` after [DEP 19](dep-0019-conditions-replace-traits.md).
- **A per-type constraint is filtered, not indexed.** Each constraint over a type's dim becomes one over `entity` with a `where` on `entity.component`. The math is the same; the index set is wider and filtered.
- **This is the largest change in the list.** The PyPSA spec is about 4600 lines, and every name, dim and relation in it is touched.

### What changes in datarecord

- `attributes` and `results` as manifest keys are replaced by `parameters`, `variables` and `result: true`.
- `default` becomes `missing`, in [DEP 17](dep-0017-one-default.md)'s shape, gaining `refused` and `absent`; dtypes take the shared spellings.
- `breakpoints` moves from the attribute to the dimension.
- `Schema` is built from `mathspec.to_spec`, and mathspec becomes a core dependency.
- `version` goes to 2.

## Rationale

**One file, not a spec embedded in a manifest.** Embedding the spec under a `math` key with datarecord's annotations beside it needs nothing from mathspec, and leaves two places that name every parameter. Aligning the languages costs a list of upstream changes once and removes the second place for good.

**Properties, not relations.** After [DEP 13](dep-0013-buses-and-carriers-are-dims.md) the single-coordinate relation is a property in datarecord, stored as one column of its subject's row. mathspec spelling it as a separate table with a prefixed name (`Link_carrier`) is the namespace spelled into the name that DEP 13 rejected.

**mathspec's words where they differ.** `parameters`, `variables` and `missing` are what a modeller reads; `missing` also carries `refused` and `absent`, which `default` cannot say. Datarecord is pre-1.0 and its manifests are rewritten either way.

**What breaks:**

- Every manifest is rewritten, and every record re-exported: `attributes`/`results` to `parameters`/`variables`, `default` to `missing`, dtypes respelled.
- Core gains a dependency on mathspec, agreed for this DEP; CONTRIBUTING.md's list of core dependencies changes with it.

## Usage

Building a record's schema from the PyPSA spec fragments:

```python
spec = mathspec.merge(["pypsa/network.yaml", "pypsa/generator.yaml", "pypsa/link.yaml"])
ws.write(root(Schema(spec)), uri)
```

Reading the spec back from a record:

```python
record = Workspace.at(uri).record
record.schema.spec.program.parameters["p_nom"].dims  # ("entity",)
record.attributes["p_nom"]  # its data
```

## Implementation notes

1. **Raise the mathspec issues** for each item in [Changes in mathspec](#changes-in-mathspec), and agree the shared spellings there.
2. **Land [DEP 15](dep-0015-multi-dims.md), [DEP 16](dep-0016-categories-are-enum-dimensions.md) and [DEP 17](dep-0017-one-default.md)**, which need nothing from mathspec.
3. **Parse with mathspec.** `Schema` wraps `mathspec.Spec`; `attributes`, `results`, `properties`, `dimensions` derived; shared spellings; `version` 2.
4. **The PyPSA tool reads mathspec's PyPSA spec** once it is [over one `entity` dim](#pypsa-one-entity-dim), instead of building a schema from PyPSA's registry.

Checks that hold once built:

- **One file validates in both**: `mathspec.to_spec(path)` and `Schema(mathspec.to_spec(path))` both accept the PyPSA manifest.
- **A schema round-trips** through `manifest.json`, and its `spec` equals what it was built from.
- **Removing a parameter from the spec removes the attribute.**
- **Every trait test passes** with traits read from the mathspec-parsed manifest.

## Rejected Ideas

**The spec embedded under a `math` key**, with datarecord's annotations in sibling sections. Needs no mathspec change, and keeps two places naming every parameter. The first draft of this DEP proposed it.

**An extension slot in mathspec** (`x-datarecord:` per block). Two vocabularies in one file, one of them opaque to the other.

**Properties nested in their dimension's entry** (`bus: {dtype: str, properties: {x: …}}`). Keeps a dim self-contained, but makes `dimensions` hold two kinds of entry body and departs from the per-subject section [DEP 12](dep-0012-properties.md) settled; a top-level `properties` sits beside `parameters` as the other kind of value.

**One dim per PyPSA component**, as mathspec's PyPSA spec has it. Every name is prefixed with its type, a name two types share is two parameters, and a record could not add a component type without a schema per type. Datarecord's tool already has one `entity` dim.

## Open Issues

### Properties in the math

**Whether a non-`into` property is readable in expressions**, or only in `where` and `by=`. `bus.x` as a parameter over `[bus]` blurs the property/attribute line for the math, while datarecord keeps it for storage.

### Results

**Which constraints and expressions are results.** `result: true` is explicit. Every constraint's dual being one would be shorter and store data nobody reads.

### Upstream

- **mathspec#915 is open.** If it merges under another key or shape, [DEP 16](dep-0016-categories-are-enum-dimensions.md) and this DEP take its spelling. If it does not merge, change 4 grows to propose `enum` itself.
- **Most of the [changes in mathspec](#changes-in-mathspec) are not proposed yet.** Each one mathspec declines leaves its key datarecord's alone: listed beside `partial` for mathspec to carry unread, or kept out of the shared file. One declined broadly (properties, dotted addressing) leaves this DEP as the embedded spec it rejects.
