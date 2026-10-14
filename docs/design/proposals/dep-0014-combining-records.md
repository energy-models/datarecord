<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 14: combining a record with an extension

| DEP      | 14                                                                                  |
| -------- | ----------------------------------------------------------------------------------- |
| Title    | combining a record with an extension                                                |
| Author   | Jonas Hörsch, Bryn Pickering                                                        |
| Status   | Draft 2026-10-14                                                                    |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 13](dep-0013-buses-and-carriers-are-dims.md) |
| Created  | 2026-10-14                                                                          |
| Version  | Unreleased                                                                          |

## Abstract

`combine(base, extension, con)` presents a tool's record and the data a consumer derives beside it as one lazy `RecordLike` under one united schema, to read directly or write as one record. The extension adds names and never redefines one. The idea and its first prototype are Bryn Pickering's ([PR #56](https://github.com/energy-models/datarecord/pull/56)); this redesigns it against the current `RecordLike`.

## Motivation

A PyPSA-Eur workflow exports its network with `PyPSA.to_datarecord(n, ws.con)`, under the tool's schema. Beside it, the workflow holds data PyPSA does not model, keyed by the same entities, buses and periods: custom constraints, a country per bus, an emission intensity per generator. There is no correct way to put both in one record:

- **A layer declares nothing.** There is [one schema per record](../schema.md#one-schema-per-record), so `w.set("co2_intensity", …)` is refused as undeclared.
- **A hand-written union is unchecked.** A child layer under a wider schema widens the manifest, but the check is [compatibility](../schema.md#versioning), which lets the union change a base attribute's `default` or `unit` or widen a base `Enum`.
- **A property on a shared subject cannot be added by a layer.** An axis row wins whole per label ([one fold for every axis](../read-path.md#one-fold-for-every-axis)), so a child adding `bus.country` must restate every base property of each bus.
- **A [standalone directory](../writing.md#a-standalone-directory) is one source with one schema.**

## Specification

### Uniting the schemas

`base.schema.extended_by(extension.schema) -> Schema` returns the united schema or raises `ValueError` naming the entry and the rule:

| entry                   | the extension may add                       | a shared one must   | refused                                                    |
| ----------------------- | ------------------------------------------- | ------------------- | ---------------------------------------------------------- |
| dim                     | a new dim, `within` any dim                 | be identical        | a differing re-declaration                                 |
| group                   | a new group, `over` and `into` any dims     | be identical        | a differing re-declaration                                 |
| property `subject.name` | a new name, on a new or a shared subject    | be identical        | a differing re-declaration                                 |
| attribute, result       | a new name, over any dims and groups        | be identical        | a differing re-declaration                                 |
| trait                   | a new trait, `on` any categorizing property | be identical        | `attributes` or `switch` naming anything the base declares |
| `partial`               | its new dims and groups                     | —                   | a shared dim or group the base's `partial` lacks           |
| `meta` key              | a new key                                   | hold an equal value | a differing value                                          |

**Identical is equal as declared**, `unit`, `default` and `default_by` included. Re-declaring is how an extension's schema stays valid alone: a trait `on` `entity.component` needs the `entity` dim, the `entity.component` property and `entity` in `partial` declared. `version` is the base's and not compared. The united schema then passes `Schema`'s own validator.

**Invariant:** every base name keeps its spec and every base category carries what it did, so `extended_by(...).compatible_with(base)` is always empty.

### The combined record

`combine` computes the united schema and refuses an extension holding rows of an attribute or result the base declares, both at the call and from the mapping keys alone ([`Frames`](../record.md#frames)). Every frame is planned on `con` through `as_relation`, so a relation from another connection is [refused](../read-path.md#a-relation-from-another-connection-is-rejected) and nothing is collected.

- **`dims`, `groups`**: union by key, the base's keys in its order, then the extension's new keys in its order ([axis order](../record.md#axis-order)).
- **`properties[s]`**: joined per subject over the united keys. A base-declared column comes from the base for its keys and from the extension for keys only the extension holds; an extension-added column comes from the extension; anything else reads NULL.
- **`attributes`, `outputs`**: by name, each one side's frame unchanged.
- **`flags(**categories)`**: computed from the combined frames against the united schema, by [the protocol's definition](../record.md#flags).

**`combine` is a read-time view.** It folds nothing and removes nothing, takes any `RecordLike` on either side (a tool's record, a `Record`, a `WorkingRecord`, another combination), and becomes an ordinary record when written: `ws.write(combine(...), uri)`, `store.root(...)`, or `rev.child(combine(rev.record, ext, con))`, whose manifest widening the invariant guarantees. It is not a `Record`, so it is edited after being written.

**`FrameRecord(schema, dims=, properties=, groups=, attributes=, outputs=)`** is an extension's shape: native frames of any backend narwhals reads. A subject with properties and no `dims` frame takes its labels from the properties' key, and a declared column a frame lacks reads NULL.

`combine` and `FrameRecord` live in core as `datarecord/combine.py`, and `extended_by` in `schema.py` beside `compatible_with` ([module layout](../module-layout.md)).

## Rationale

**The rule protects the base, not only its names.** A new trait claiming `p_nom` for generators collides with nothing yet takes it from every other entity, because [a trait narrows](../schema.md#traits). Identity on shared entries plus new traits claiming only new names is what makes the invariant hold, so a tool reads a combined record's base names as it reads its own record.

**Subjects split by key, attributes by name.** An axis or group row is keyed by one label, so the two sides meet on it. Attribute rows overlap through [broadcast](../record.md#the-broadcast-rule), and dividing them between sources is [the fold](../read-path.md#owner-map), which is a layer's job. For the same reason, an extension's value for a base-declared column on a base-held key is not read: changing a base value is a layer's job.

**What it costs.** `flags` is a scan per call where a `Record`'s is free. An extension may add labels to a shared axis, so a mistyped label becomes a new one. An extension disagreeing with the base on a base-held key goes unnoticed. Re-declaring shared entries makes an extension verbose.

## Usage

```python
base = PyPSA.to_datarecord(n, ws.con)
s = base.schema
schema = Schema(
    dimensions={"entity": s.dimensions["entity"], "bus": s.dimensions["bus"],
                "period": s.dimensions["period"], "limit": Dimension(dtype=nw.String())},
    properties={"entity": {"component": s.properties["entity"]["component"]},
                "bus": {"country": PropertySpec(dtype=nw.String())}},
    attributes={"limit_constant": AttributeSpec(dtype=nw.Float64(), dims={"limit", "period"}),
                "co2_intensity": AttributeSpec(dtype=nw.Float64(), dims={"entity"})},
    traits={"emitting": Trait(attributes={"co2_intensity"},
                              on={"entity.component": {"Generator", "Link"}})},
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

1. `Schema.extended_by`, a parametrized test per refused row, and a test that `compatible_with(base)` is empty.
2. `combine`'s members over `con`, ordered by numbering each side's rows and sorting outermost.
3. The frames-based `flags`, sharing `categorized_dim`, `selected` and `attributes_among` with the fold's.
4. `FrameRecord`, exported with `combine` from `datarecord`.
5. Pages: a "Combining records" section on [writing](../writing.md), `extended_by` under [one schema per record](../schema.md#one-schema-per-record), [module layout](../module-layout.md), usage, changelog.

Checks: reading every member of a combination over frames that raise on collect runs no query; projected to base names and keys, every combined frame equals the base's; `flags(component="Bus")` lacks `co2_intensity`.

## Rejected Ideas

- **An extension layer**, whether declaring its own schema additions or written under a hand-united schema. The first is the layered schema [one schema per record](../schema.md#one-schema-per-record) rejects. Both need a tree, cannot be read before written, and restate the base's properties of every shared subject.
- **The prototype's eager widening**, which collected both sides to pyarrow and broke [lazy `Frames`](../record.md#frames).
- **Refusing only colliding trait names**, which lets a new trait narrow base attributes.
- **Taking `flags` from the sides.** The extension has no category data, and the PyPSA tool's record answers `{}`.
- **Merging two sides' rows of one attribute.** Dividing overlapping rows is a fold.

## Open Issues

- **Data checks on shared subjects.** Should extension-only labels on a shared axis, or disagreement on a base-held key, be refused? Either check needs both frames read, perhaps by the writer.
- **Relaxing identity.** Should the extension be allowed to widen a base `Enum`, or add a trait without `on` that bundles base names?
- **Adding an extension to an existing tree without restating the base.** This needs axis rows folded per column.
