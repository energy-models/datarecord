<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 14: combining a record with an extension

| DEP      | 14                                                                                  |
| -------- | ----------------------------------------------------------------------------------- |
| Title    | combining a record with an extension                                                |
| Author   | Jonas Hörsch, Bryn Pickering                                                        |
| Status   | Rejected (2026-10-20)                                                               |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md) |
| Created  | 2026-10-14                                                                          |

## Abstract

`combine(base, extension, con)` presents a tool's record and the data a reader derives beside it as one lazy `RecordLike` under one united schema. You read it directly or write it as one record. The extension adds names and never redefines one. The idea and its first prototype are Bryn Pickering's, in [PR #56](https://github.com/energy-models/datarecord/pull/56), and this DEP redesigns it against the current `RecordLike`.

## Motivation

A PyPSA-Eur workflow exports its network with `PyPSA.to_datarecord(n, ws.con)`, under the tool's schema. Beside it, the workflow has data that PyPSA does not model, keyed by the same entities, buses and periods. Examples are custom constraints, a country per bus and an emission intensity per generator. There is no correct way to put both in one record:

- A layer declares nothing. A record has [one schema](../../reference/schema.md), so datarecord refuses `w.set("co2_intensity", …)` as undeclared.
- A hand-written union is unchecked. A child layer under a wider schema widens the manifest, but datarecord checks only compatibility. That check lets the union change a base `default` or `unit`, or widen a base `Enum`.
- A layer cannot add a property on a shared subject. Per label, [the fold](../../reference/resolution.md) keeps the deepest axis row whole, so a child that adds `bus.country` must restate every base property of each bus.
- A [standalone directory](../../reference/store.md) is one source with one schema.

## Specification

### Uniting the schemas

`base.schema.extended_by(extension.schema) -> Schema` returns the united schema, or raises `ValueError` naming the entry and the rule:

| Entry                   | The extension may add                       | A shared entry must | Refused                                                   |
| ----------------------- | ------------------------------------------- | ------------------- | --------------------------------------------------------- |
| dim                     | a new dim, `within` any dim                 | be identical        | a differing re-declaration                                |
| group                   | a new group, `over` and `into` any dims     | be identical        | a differing re-declaration                                |
| property `subject.name` | a new name, on a new or a shared subject    | be identical        | a differing re-declaration                                |
| attribute, result       | a new name, over any dims and groups        | be identical        | a differing re-declaration                                |
| trait                   | a new trait, `on` any categorising property | be identical        | `attributes` or `switch` listing a name the base declares |
| `partial`               | its new dims and groups                     | not applicable      | a shared dim or group the base's `partial` lacks          |
| `meta` key              | a new key                                   | have an equal value | a differing value                                         |

Identical means equal as declared, `unit`, `default` and `default_by` included. Re-declaring keeps an extension's schema valid alone: a trait `on` `entity.component` needs the `entity` dim, the `entity.component` property and `entity` in `partial`. `version` is the base's and is not compared. The united schema then passes `Schema`'s own validator.

Every base name keeps its spec and every base category has what it had, so `extended_by(...).compatible_with(base)` is always empty.

### The combined record

`combine` computes the united schema. It refuses an extension with rows of an attribute or result the base declares, both at the call and from the mapping keys alone. Every frame is planned on `con` through `as_relation`, so datarecord refuses a relation from another connection and collects nothing.

- `dims` and `groups` are a union by key: the base's keys in its order, then the extension's new keys in its order.
- `properties[s]` is joined per subject over the united keys. A base-declared column comes from the base for its keys and from the extension for keys only the extension has. An extension-added column comes from the extension, and anything else is NULL.
- `attributes` and `outputs` are taken by name, each from one side unchanged.
- `flags(**categories)` is computed from the combined frames against the united schema, by the protocol's definition.

`combine` is a read-time view. It folds nothing and removes nothing, and takes any `RecordLike` on either side: a tool's record, a `Record`, a `WorkingRecord` or another combination. Written, it becomes an ordinary record, by `ws.write(combine(...), uri)`, `store.root(...)` or `rev.child(combine(rev.record, ext, con))`. Because of the invariant, `compatible_with` accepts the manifest widening of the last. A combination is not a `Record`, so you edit it after writing it.

`FrameRecord(schema, dims=, properties=, groups=, attributes=, outputs=)` is an extension's shape: native frames of any backend narwhals reads. A subject with properties and no `dims` frame takes its labels from the properties' key, and a declared column that a frame lacks is NULL.

`combine` and `FrameRecord` live in core as `datarecord/combine.py`, and `extended_by` lives in `schema.py` beside `compatible_with`.

## Rationale

Under this rule, datarecord refuses any change to the base, not only a clash with its names. A new trait that claims `p_nom` for generators collides with nothing, yet takes it from every other entity, because [a trait narrows](dep-0016-remove-traits.md). Identity on shared entries, with new traits that claim only new names, makes the invariant hold. A tool then reads a combined record's base names as it reads its own record.

`combine` divides subjects by key and attributes by name. An axis or group row is keyed by one label, so the two sides meet on it. Attribute rows overlap through the broadcast rule, and dividing them between sources is the fold, which is a layer's job. For the same reason, `combine` ignores an extension's value for a base column on a base key. Changing a base value is a layer's job.

The costs: `flags` is a scan per call, where a `Record`'s is free. An extension may add labels to a shared axis, so a mistyped label becomes a new one. An extension that disagrees with the base on a base key goes unnoticed. Re-declaring shared entries makes an extension verbose.

## Usage

```python
base = PyPSA.to_datarecord(n, ws.con)
s = base.schema
schema = Schema(
    dimensions={
        "entity": s.dimensions["entity"],
        "bus": s.dimensions["bus"],
        "period": s.dimensions["period"],
        "limit": Dimension(dtype=nw.String()),
    },
    properties={
        "entity": {"component": s.properties["entity"]["component"]},
        "bus": {"country": PropertySpec(dtype=nw.String())},
    },
    attributes={
        "limit_constant": AttributeSpec(dtype=nw.Float64(), dims={"limit", "period"}),
        "co2_intensity": AttributeSpec(dtype=nw.Float64(), dims={"entity"}),
    },
    traits={
        "emitting": Trait(
            attributes={"co2_intensity"}, on={"entity.component": {"Generator", "Link"}}
        )
    },
    partial={"entity"},
)
extension = FrameRecord(
    schema,
    properties={"bus": countries},  # bus | country
    attributes={"limit_constant": limits, "co2_intensity": intensities},
)
record = combine(base, extension, ws.con)
record.flags(component="Generator")["co2_intensity"]
ws.write(record, "resources/networks/base_s_37.record")
```

## Implementation notes

1. `Schema.extended_by`, a parametrised test per refused row, and a test that `compatible_with(base)` is empty.
2. `combine`'s members over `con`, ordered by numbering each side's rows and sorting outermost.
3. The frames-based `flags`, sharing `categorized_dim`, `selected` and `attributes_among` with the fold's.
4. `FrameRecord`, exported with `combine` from `datarecord`.
5. Pages, usage pages and changelog.

Checks: reading every member of a combination over frames that raise on collect runs no query. Projected to base names and keys, every combined frame equals the base's. `flags(component="Bus")` lacks `co2_intensity`.

## Rejected ideas

- An extension layer, which declares its own schema additions or is written under a hand-united schema. The first is the layered schema that datarecord rejects by having one schema per record. Both need a tree, cannot be read before they are written, and restate the base properties of every shared subject.
- The prototype's eager widening, which collected both sides to pyarrow and broke lazy `Frames`.
- Refusing only colliding trait names, which lets a new trait narrow base attributes.
- Taking `flags` from the sides. The extension has no category data, and the PyPSA tool's record returns `{}`.
- Merging two sides' rows of one attribute. Dividing overlapping rows is a fold.

## Open issues

- Data checks on shared subjects: whether datarecord should refuse extension-only labels on a shared axis, or disagreement on a base key. Either check reads both frames, perhaps in the writer.
- Relaxed identity: whether the extension may widen a base `Enum`, or add a trait without `on` that bundles base names.
- Adding an extension to an existing tree without restating the base, which needs axis rows folded per column.
