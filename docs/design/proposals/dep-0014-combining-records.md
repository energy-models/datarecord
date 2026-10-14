<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 14: combining a record with an extension

| DEP      | 14                                   |
| -------- | ------------------------------------ |
| Title    | combining a record with an extension |
| Author   | Jonas Hörsch, Bryn Pickering         |
| Status   | Draft 2026-10-14                     |
| Requires | [DEP 12](dep-0012-properties.md)     |
| Created  | 2026-10-14                           |
| Version  | Unreleased                           |

## Abstract

`combine(base, extension, con)` presents a tool's record and the data a consumer derives beside it as one lazy `RecordLike` under one united schema. It can be read as it is, or written as one record with `ws.write`, `store.root` or `rev.child`. The extension may add names but never redefine one: a name both sides declare must be declared identically, and an extension trait may narrow only names the extension adds. Every frame of the combined record is a DuckDB plan on `con`. Nothing is collected and nothing is folded.

The idea and its first prototype come from Bryn Pickering's [PR #56](https://github.com/energy-models/datarecord/pull/56). This proposal redesigns that prototype against the current `RecordLike`.

## Motivation

A PyPSA-Eur workflow exports its network with `PyPSA.to_datarecord(n, ws.con)`. The record's schema is the tool's, read off PyPSA's registry. The workflow also holds data that PyPSA does not model:

- custom constraints, with their own axis of labels, a sense and a right-hand side per period;
- a country for each bus;
- an emission intensity for each generator and link;
- a region for each entity.

All of it is keyed by the same entities, buses and periods. It belongs in the same record, so that a solve, a scenario layer or a downstream model reads it beside the network.

**Today there is no correct way to get it there.**

- **A layer cannot carry it.** There is [one schema per record](../schema.md#one-schema-per-record), and a layer declares nothing. So `w.set("co2_intensity", …)` on a `WorkingRecord` over the tool's record is refused: the name is undeclared, and a staging area that could declare it would be [the layered-schema case](../schema.md#one-schema-per-record) arriving by another door.
- **A hand-written union is unchecked.** The workaround is to write a `Schema` that is both, by hand, and write the extension's rows as a child layer under it. The tree's manifest widens through [its compare-and-swap](../writing.md#the-manifests-compare-and-swap), since the widening is [compatible](../schema.md#versioning). But compatibility is the wrong test. It lets the union change a base attribute's `default`, `default_by` or `unit`, or add a category to a base `Enum`, and none of those is incompatible. Nothing checks that the base still means what the tool wrote.
- **A property on a shared subject breaks.** A child layer's axis row wins whole, per label ([one fold for every axis](../read-path.md#one-fold-for-every-axis)). So a child that adds `country` to `dims/bus.parquet` must restate every property the base holds for each bus, or erase them.
- **A standalone record cannot hold both at all.** `ws.write(source, uri)` writes one source with one schema, and a [standalone directory](../writing.md#a-standalone-directory) is one layer.

What is missing is a rule for uniting two schemas, and a record that answers both sides' frames under it.

## Specification

### `Schema.extended_by`

```python
class Schema(BaseModel):
    def extended_by(self, extension: Schema) -> Schema: ...
```

`base.extended_by(extension)` is the united schema, or a `ValueError`. Each error names the offending name, its section and the rule it breaks. The rule is one sentence: **an extension adds names, it never redefines one.** In detail:

| section                 | the extension may add                                                                                    | a name both declare                     |
| ----------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `dimensions`            | a new dim, `within` a shared or a new one                                                                | must be identical: `dtype` and `within` |
| `groups`                | a new group, `over` and `into` shared or new dims                                                        | must be identical                       |
| `properties`            | a new property, of a new subject or a shared one; an `Enum` one of a `partial` dim categorizes it        | must be identical, on the same subject  |
| `attributes`, `results` | a new name, over any declared dims and groups                                                            | must be identical                       |
| `traits`                | a new trait, `on` any categorizing property of the united schema, claiming only names the extension adds | must be identical                       |
| `partial`               | dims and groups the extension adds                                                                       | must be in the base's `partial` too     |
| `meta`                  | a new key                                                                                                | must hold an equal value                |

**Identical means equal as declared.** That covers `unit`, `description`, `default` and `default_by`, not only the fields that decide how a row decodes. Re-declaring a name is how an extension's schema stays valid on its own. A `default_by` keyed on `entity_type`, or a trait `on` it, needs `entity_type` declared, and [a categorizing property needs its dim in `partial`](../schema.md#categories-an-enum-property-of-a-partial-dim). So the extension re-declares the base's `entity` dim, its `entity_type` property and `entity` in `partial`. A re-declaration that differs in anything would be a second spec for one name, and is refused.

**A new trait claims only new names.** Its `attributes` and `switch` must be names the extension adds and the base does not declare. A trait claiming a base attribute would change which base labels carry it, because [a trait narrows](../schema.md#traits). For example, `p_nom` claimed for generators would leave every bus without it. Base traits name only base names, so they never reach an extension name. An extension name no trait claims is carried by every label its `dims` address, as any unclaimed name is.

**`partial` is the union.** The extension's own `partial` may name a shared dim or group only where the base's `partial` also names it. Otherwise the extension would change how a later layer patches base attributes along that axis. A base whose `partial` is `None` takes the extension's new dims and groups.

**`version` is the base's.** The united schema continues the base's lineage, and that lineage is what a tree's manifest is checked against. The extension's `version` numbers its own lineage, and is not compared.

**The united schema is then validated as a whole**, through `Schema`'s own validator. That catches what neither side can see alone. For example, an extension property named like a base attribute breaks [one name, one spec, across every section](../schema.md#properties). Two spellings that differ only in case break [names](../schema.md#names).

**Invariant.** Every base category carries exactly what it carried before, and every base name keeps its spec. So `extended_by(...).compatible_with(base)` is always empty, and widening a base tree's manifest to the united schema always succeeds.

### `combine`

```python
def combine(
    base: RecordLike, extension: RecordLike, con: DuckDBPyConnection
) -> RecordLike: ...
```

`combine` checks two things when it is called, and reads no data for either:

- it computes the schema, `base.schema.extended_by(extension.schema)`;
- it refuses an extension that holds rows of an attribute or result the base declares. It finds those by enumerating the keys of `extension.attributes` and `extension.outputs`, which [costs no read](../record.md#frames).

Every frame is planned on `con` and passed through `as_relation`. A DuckDB relation passes through as a plan. A frame of another backend, such as an extension's pandas frames, is landed. A relation from another connection is [refused](../read-path.md#a-relation-from-another-connection-is-rejected). `con` is a parameter for the same reason a tool's `to_datarecord` takes one: the two sides must meet in one engine to be joined. Records from two stores on one workspace already do.

Each member of the combined record is a `LazyFrames`. Its keys are listed from the two sides' keys, and each frame is planned only when it is read:

- **`schema`**: the united schema.
- **`dims[d]`**: the **union of labels**. First the base's labels in the base's order, then the extension's labels that the base lacks, in the extension's order. That is [axis order](../record.md#axis-order) as first introduced, base first. A dim only one side holds takes that side's frame.
- **`groups[g]`**: the same union by key, on the group's struct column. A key the base holds takes the base's `into` label. A key only the extension holds takes the extension's. Properties are joined as below.
- **`properties[s]`**: **joined per subject**, over the labels of the united `dims[s]` or `groups[s]`. Its keys are the subjects either side holds a properties frame for, and its columns follow the united schema's declaration order, the base's properties first:
  - a property the base declares is read from the base's frame for the keys the base holds, and from the extension's frame for keys only the extension holds;
  - a property the extension adds is read from the extension's frame;
  - a key a side has no row for, or a column a side's frame lacks, reads NULL.
- **`attributes[a]`, `outputs[a]`**: **by name**. Each attribute is one side's frame, unchanged, and no attribute has frames on both sides.
- **`flags(**categories)`**: computed from the combined frames against the united schema, never taken from a side. The definition is [the protocol's](../record.md#flags). Per attribute and per coordinate, `varies` holds a coordinate some row sets and `broadcast` one some row leaves NULL. With `categories`, only rows that reach a matching label in the united `properties[dim]` count, through the dim's column or through a group's single field over it. Only attributes those labels carry are present.

**Subjects split by key, attributes by name.** An axis or group row is keyed by one label, so two sides' rows of one subject meet on that key and nothing else. Attribute rows overlap through [broadcast](../record.md#the-broadcast-rule): a NULL `entity` row and a per-entity row cover the same coordinate. Dividing them between two sides is what [the fold](../read-path.md#owner-map) does, and that is a layer, not a combination.

**Values of base names on base keys come from the base.** An extension's row for a label the base holds contributes only the columns the extension adds. The values that row holds for base-declared columns are not read. Changing a base value is what a layer is for: `rev.child`, or a `WorkingRecord` over the written record.

### Layers and tombstones

**`combine` is a read-time view.** It folds nothing, holds no state, and writes nothing. Either side may be any `RecordLike`:

- a tool's record;
- a `Record` over a tree or a directory, whose frames are already resolved;
- a `WorkingRecord`, read with its pending edits;
- another combined record.

`combine(combine(a, b), c)` extends `a` by `b`, then that by `c`.

**Neither side hands over a tombstone.** A `RecordLike`'s frames hold live rows, so a combined record removes nothing. A base label the extension wants gone is a `remove` in a layer.

**Writing makes it an ordinary record.**

- `ws.write(combine(...), uri)` writes a standalone directory whose `manifest.json` is the united schema.
- `store.root(combine(...))` starts a tree under that schema, and every later layer reads and patches the extension's names like the base's.
- `rev.child(combine(rev.record, ext, con))` widens an existing tree's manifest through its compare-and-swap, which the invariant above guarantees succeeds. The child restates the base's rows whole.

**A combined record is not a `Record`.** It has no resolver, so a `WorkingRecord` cannot stage over it. To edit it, write it and edit the `Record` read back.

### `FrameRecord`

```python
@dataclass(frozen=True)
class FrameRecord:
    schema: Schema
    dims: Mapping[str, IntoFrame] = {}
    properties: Mapping[str, IntoFrame] = {}
    groups: Mapping[str, IntoFrame] = {}
    attributes: Mapping[str, IntoFrame] = {}
    outputs: Mapping[str, IntoFrame] = {}
```

The shape an extension takes: a schema and ready-made frames of any backend narwhals reads, served as `LazyFrames`. Two conveniences keep an extension short:

- a subject with a `properties` frame and no `dims` frame takes its labels from the properties frame's key columns;
- a declared property column a frame lacks reads NULL.

Its `flags` is the same frames-based computation `combine` uses.

### Where it lives

`datarecord/combine.py`, in core, holds `combine`, `FrameRecord` and the frames-based `flags`. `Schema.extended_by` sits in `schema.py` beside `compatible_with`, since it is a rule about schemas that a caller may need without a record. Neither names a modelling framework, and neither imports a tool ([module layout](../module-layout.md)). An extension built for PyPSA is a consumer's code.

## Rationale

**Two checks, before any read.** Every refusal comes from the schemas, or from the keys of the frame mappings. So a combination that would redefine a name fails at the `combine` call, before a frame is planned. The rows meet the writer's [validation](../writing.md) when they are written, as any source's do.

**Lazy throughout.** Union by key and join per subject are relational operations on DuckDB plans. A consumer reading three of forty attributes from a combined record over a remote tree pays for those three ([`Frames`](../record.md#frames)).

**The rule protects the base, not only the names.** Refusing a colliding name is not enough, because a trait claiming a base attribute redefines who carries it without colliding with anything. Identity on shared names, and new traits claiming only new names, are what make the invariant hold: the base's categories carry what they did. A tool's `verify` and `build` therefore read a combined record's base names as they read the tool's own record.

**What it costs.**

- **`flags` is a scan, not free.** A `Record` folds its flags into the owner map. A combined record aggregates its attribute frames for each `flags` call. That is one query per call, over every attribute, with a semi-join against `properties[dim]` when categories are given. A combined record read repeatedly is better written once and read back.
- **The extension is verbose.** Re-declaring each shared dim, and each categorizing property its traits key on, is the price of each side's schema being valid on its own.
- **Base values on base keys are silently the base's.** An extension's value of a base-declared property, on a label the base holds, is not read. Finding a disagreement would mean reading both frames.
- **An extension can add labels to a shared axis.** A typo in a label lands as a new label rather than an error. A new label on a categorized dim has a NULL category and carries only what no trait narrows, as [the broadcast rule](../record.md#the-broadcast-rule) already says of such a label.

## Usage

A PyPSA-Eur extension adds a constraint axis, a property on a shared subject without base properties (`bus`), one on a shared subject with them (`entity`), and an attribute narrowed by a trait keyed on the base's `entity_type`:

```python
import narwhals as nw
import pandas as pd

from datarecord import (
    AttributeSpec,
    Dimension,
    FrameRecord,
    PropertySpec,
    Schema,
    Trait,
    Workspace,
    combine,
)
from datarecord.tools.pypsa import PyPSA

ws = Workspace()
base = PyPSA.to_datarecord(n, ws.con)
shared = base.schema

schema = Schema(
    dimensions={
        "entity": shared.dimensions["entity"],
        "bus": shared.dimensions["bus"],
        "period": shared.dimensions["period"],
        "limit": Dimension(dtype=nw.String()),
    },
    properties={
        "entity": {
            "entity_type": shared.properties["entity"]["entity_type"],
            "region": PropertySpec(dtype=nw.String()),
        },
        "bus": {"country": PropertySpec(dtype=nw.String())},
        "limit": {"sense": PropertySpec(dtype=nw.Enum(["<=", ">=", "=="]))},
    },
    attributes={
        "limit_constant": AttributeSpec(dtype=nw.Float64(), dims={"limit", "period"}),
        "co2_intensity": AttributeSpec(
            dtype=nw.Float64(), dims={"entity"}, default=0.0, unit="t/MWh"
        ),
    },
    traits={
        "emitting": Trait(
            attributes={"co2_intensity"}, on={"entity_type": {"Generator", "Link"}}
        )
    },
    partial={"entity"},
)
extension = FrameRecord(
    schema,
    properties={
        "entity": pd.DataFrame({"entity": ["DE0 0 gas"], "region": ["DE11"]}),
        "bus": pd.DataFrame({"bus": ["DE0 0", "FR0 0"], "country": ["DE", "FR"]}),
        "limit": pd.DataFrame({"limit": ["co2_cap"], "sense": ["<="]}),
    },
    attributes={
        "limit_constant": pd.DataFrame(
            {
                "limit": "co2_cap",
                "period": [2030, 2040],
                "attribute": "limit_constant",
                "breakpoint": None,
                "value": [100.0, 50.0],
            }
        ),
        "co2_intensity": pd.DataFrame(
            {
                "entity": ["DE0 0 gas"],
                "attribute": "co2_intensity",
                "breakpoint": None,
                "value": [0.2],
            }
        ),
    },
)

record = combine(base, extension, ws.con)
```

Read directly:

```python
record.properties["entity"]  # entity | entity_type | carrier | sign | type | region
record.properties["bus"]  # bus | country, over every bus of the base
record.flags(entity_type="Generator")["co2_intensity"]
```

Written as one record:

```python
from datarecord import WorkingRecord

ws.write(record, "resources/networks/base_s_37.record")  # a standalone directory
rev = ws.store("s3://bucket/runs/base_s_37").root(record)  # or a tree's root
w = WorkingRecord(rev.record)
w.set("co2_intensity", 0.0, entity="DE0 0 gas")  # an extension name, patched like any
```

Refused when called, before any frame is read:

```python
combine(
    base,
    FrameRecord(
        Schema(
            dimensions={"entity": shared.dimensions["entity"]},
            attributes={"p_nom": AttributeSpec(dtype=nw.Float32(), dims={"entity"})},
        )
    ),
    ws.con,
)
# ValueError: the extension declares attribute 'p_nom' differently from the base
# (dtype Float32, not Float64); an extension adds names, it never redefines one

combine(
    base,
    FrameRecord(
        Schema(
            dimensions={"entity": shared.dimensions["entity"]},
            properties={
                "entity": {"entity_type": shared.properties["entity"]["entity_type"]}
            },
            attributes={"p_nom": shared.attributes["p_nom"]},
            traits={
                "built": Trait(attributes={"p_nom"}, on={"entity_type": {"Generator"}})
            },
            partial={"entity"},
        )
    ),
    ws.con,
)
# ValueError: the extension's trait 'built' claims 'p_nom', which the base declares;
# a trait would narrow which base labels carry it
```

## Implementation notes

1. **`Schema.extended_by`**, with one parametrized test per refusal in the table above, and a test that `compatible_with(base)` is empty for every accepted pair, which pins the invariant.
2. **`combine`'s members over `con`.** Each side's rows are numbered on its own frame, and the order is the outermost clause, since a join or a union defines no order. That is how the writer's adapter already orders `dims[dim]` against `properties[dim]`. A side that is a `Record` past a staging area is read through its pinned resolver, as the writer pins one, so reading many keys folds the staging tail once.
3. **The frames-based `flags`**, sharing `categorized_dim`, `selected` and `attributes_among` with the fold's `flags`, so the two agree on what a category reaches.
4. **`FrameRecord`**, exported from `datarecord` with `combine`.
5. **Pages.** A "Combining records" section on [writing](../writing.md), a sentence under [one schema per record](../schema.md#one-schema-per-record) naming `extended_by` as how two schemas become one, `combine.py` in [module layout](../module-layout.md), a usage example, and the changelog.

The checks that hold once it is built:

- **Laziness.** Calling `combine` over a base whose frames raise when collected, and reading every key of every member, runs no query.
- **The base is untouched.** Every frame of the combined record, projected to the base's names and keys, equals the base's own frame, and the base's categories carry the same names.
- **Round trip.** `ws.write(combine(PyPSA.to_datarecord(n, ws.con), ext, ws.con), uri)` reads back with `properties["bus"]` carrying `country` for every bus, and the base's property values intact on every entity.
- **Flags against the united schema.** `flags(entity_type="Bus")` holds no `co2_intensity`. `flags(entity_type="Generator")` does, although the extension's own schema has no category data to answer it.

## Rejected Ideas

**An extension layer that declares its own schema additions.** A layer would carry a schema fragment, and the record's schema would be the fold of the fragments along the ancestry. That is the layered schema that [one schema per record](../schema.md#one-schema-per-record) rejects:

- the schema becomes unknowable without walking the ancestry;
- sibling branches declare different names;
- nothing structural stops a deeper fragment redefining what a shallower one declared.

It would also not solve the shared-subject case. An axis row wins whole per label, so an extension layer adding `country` to buses must restate every bus property or erase it. Making it not do so means a per-column fold of axis rows, which changes what a NULL property in a layer means for every record.

**A child layer under a hand-united schema**, which works today for new dims, groups and attributes. It is not rejected outright, because `rev.child` under `extended_by`'s schema is a sound spelling of the same rule. But it is not enough. It needs a tree, so it cannot give a standalone directory. It cannot add a property to a shared subject without restating the base's. And it cannot be read before it is written. `combine` covers all of those, and its `rev.child` use is the same layer, built for the caller.

**The prototype's eager widening.** It collected both sides to pyarrow to join an extension's columns onto a base axis, which breaks the [lazy `Frames`](../record.md#frames) contract on the largest frames a record has.

**Refusing only colliding trait names.** A new trait with a fresh name may still claim base attributes and narrow them. The prototype's check let that through, and the invariant above does not hold without the stricter rule.

**Refusing extension labels the base lacks.** The prototype raised when a frame was read. Checking it needs both sides' labels, so the check is either eager or a raise inside a lazy plan, where it surfaces wherever the plan is first collected. The union keeps the plan lazy, and the new label is visible in `dims` where a consumer can see it. Whether to refuse instead is open below.

**Taking `flags` from the sides.** A side's flags are computed against that side's schema. The extension cannot answer `flags(entity_type="Generator")`, because the categories are the base's rows. The PyPSA tool's record answers `{}`, because the writer never asks it. A `dict` union of two wrong answers is the prototype's.

**Merging two sides' rows of one attribute.** Rows overlap by broadcast, so dividing them is ownership, which is the fold. A record that combines and also folds would be a second `Resolver`.

**Requiring equal `version`s.** The prototype did. Two schemas from different lineages have no reason to share a number, and a caller would set one to satisfy the check.

**A symmetric union.** Without a base, nothing decides axis order, nobody wins a key both sides hold, and no side's categories are protected. The asymmetry is the point: the extension is written against the base.

**Placing it under `tools/`.** It names no framework, and a consumer extending a hand-built record needs it as much as one extending PyPSA's.

## Open Issues

- **Refuse or keep extension-only labels on a shared axis.** The union keeps them. Refusing catches typos, at the cost of a data check.
- **Detect disagreement on a base-held key.** An extension restating a base property differently, for a label the base holds, is ignored. Reporting it needs both frames read, perhaps by the writer, which reads them anyway.
- **Let an extension widen a base `Enum`.** Adding a carrier the base's `Enum` lacks is [compatible](../schema.md#versioning) as a schema change, but it changes a base name's spec. It is refused as drafted.
- **Let a trait without `on` bundle base names.** Such a trait narrows nothing, so it would leave the invariant intact while letting a consumer dispatch on a bundle that spans both sides. It is refused as drafted, for one simple rule.
- **Write an extension into an existing tree without restating the base.** A child holding only the extension's rows works except for properties of shared subjects. Supporting those needs axis rows folded per column.
- **Share the frames-based `flags` with the PyPSA tool.** The tool's record could answer `flags` honestly instead of `{}`.
- **Let a `WorkingRecord` stage over a combined record.** That would need a staging area over a `RecordLike`, not over a resolver.
