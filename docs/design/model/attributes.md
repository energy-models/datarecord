<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Attributes and the broadcast rule

An attribute is data: long rows of coordinates, a breakpoint and a value, one file per attribute. Its `dims` name the dims and groups that address it. A NULL coordinate means every label of that dim ([the broadcast rule](#the-broadcast-rule)). A solve's results are attributes like any other. A child layer inherits its parent's results, and after an input edit the caller keeps them coherent ([open questions](../open-questions.md#whether-results-should-be-invalidated-by-a-later-input-edit)). Which attributes a framework computes is its tool's knowledge ([PyPSA](../tools/pypsa.md#writing-results)).

## `AttributeSpec`

An `AttributeSpec` declares what one attribute may do over the record's axes:

```python
attributes = {
    # a capacity to build: one decision, evaluated against every scenario
    "p_nom": AttributeSpec(dtype="float", dims={"entity"}),
    # an availability profile: varies over time, and per scenario
    "p_max_pu": AttributeSpec(dtype="float", dims={"entity", "scenario", "snapshot"}),
    "marginal_cost": AttributeSpec(
        dtype="float", dims={"entity", "scenario"}, breakpoints=True
    ),
    # addressed by a group
    "efficiency": AttributeSpec(dtype="float", dims={"connection", "snapshot"}),
    # addressed by an axis alone: a weighting belongs to no entity
    "objective_weighting": AttributeSpec(dtype="float", dims={"snapshot"}),
}
```

One attribute is one spec, one file and one `value` dtype.

`dims` is the only addressing mechanism. It names dims and [groups](groups.md) alike, resolved by [one rule](groups.md#attributes-over-a-group). Whether a coordinate is a dim or a group changes where its labels come from. The declaration and the storage of the attribute stay the same.

`dims` also makes a scenario-varying `p_nom` a schema violation. A capacity is a first-stage decision, one value taken before the scenario is known, and stochastic scenarios differ only in dispatch.

`dims` says which coordinates address a value. It says nothing about what kind of value it is. `p_nom` over `entity` alone is data an optimiser decides, and `carrier` describes the entity, so `carrier` is a [property](properties.md#properties).

`breakpoints` declares whether the attribute may carry a piecewise-linear curve. The write refuses a curve on an attribute that takes one value ([wide and long rows](#wide-and-long-rows)).

Every attribute is long rows in `attributes/<attr>.parquet`, whatever its `dims`. The section a name is declared in decides [where a value lives](../storage/format.md#where-a-value-lives). Widening `dims` adds a column to the same file.

`default` and [`default_by`](properties.md#default_by-a-default-per-category) give the value for a coordinate that no row covers.

## The broadcast rule

A row's `value` applies to every combination of its NULL dim columns, enumerated from the axis frames in `dims`. A NULL dim means all labels of that dim. A constant `p_max_pu` is one row with `snapshot = NULL`, and a varying one is a row per snapshot.

`set("p_max_pu", 0.9)` is one row with `entity = NULL` that reaches every entity, loads and entities added in a later layer included. A tool that does not take the value for a load [leaves it out](../tools/pypsa.md#a-value-its-type-does-not-take). A value for some categories is written per label, `set("p_max_pu", 0.9, ctype="Generator")`.

A [group](groups.md#attributes-over-a-group)'s column broadcasts too, with the resolved group as its axis. A NULL `connection` means every connection, expanded against `groups/connection.parquet` as a NULL `snapshot` is expanded against `dims/snapshot.parquet`. The dims a group draws on broadcast on their own: a connection's `bus` is a field of the `connection` column, so a per-bus `marginal_price` with `bus = NULL` holds one value for every bus.

Within one layer, the more specific row wins. Two rows of one layer may cover one coordinate, such as a `p_nom` row with `entity = NULL` beside one that names `a`. A row that names a label of a dim is more specific than one that leaves that dim NULL. Where one row names every dim the other names and more, it wins at every coordinate both cover. Two rows may cover one coordinate where each names a dim the other leaves NULL, or where both name the same dims at one breakpoint. Neither row is then more specific. The write refuses such a layer, and a read of a layer written another way fails. Both errors name the attribute and the coordinate. Rows of two layers meet at a coordinate too, and the rule decides between them the same way. Where a deeper layer's row crosses a shallower one, [the fold](../layers/resolution.md#crossing-rows) adds a row naming both, owned by the deeper layer, so the rule finds one winner wherever a row reaches.

A coordinate that no row covers takes the attribute's default from [the schema](#attributespec). That includes every coordinate of an attribute with no rows. The default is the `default_by` entry for the label's category where one exists, else `default`.

The read preserves broadcast form. A value held once is answered once, so a consumer can tell a constant from a varying value by the shape it gets back. The read returns the rows as written, NULL included, along every dim, and the consumer applies the rule over the full coordinates. It needs neither the layers nor the [`partial`](../layers/tree.md#partial-the-granularity-of-an-override) dims to do so.

## Wide and long rows

A record hands over its data in two shapes.

`dims`, `groups` and `properties` are wide, one row per label or tuple:

```text
dims["entity"]                     entity                              key columns only
groups["connection"]               connection STRUCT(entity, bus) | role | ...
properties["entity"]               entity | ctype | carrier | ... stored values
```

`dims[dim]` is the axis's key, the label, in [axis order](dims.md#axis-order). [Reading](../use/reading.md#properties-and-categories) covers properties and categories. `groups` holds one relation per group, keyed by the one struct column named for it with its properties beside it ([where a value lives](../storage/format.md#where-a-value-lives)).

`attributes` are long, one row per value, keyed by the attribute's name:

```text
attributes["p_nom"]        entity | attribute | breakpoint | value
attributes["p_max_pu"]     entity | <one column per coordinate> | attribute | breakpoint | value
attributes["efficiency"]   connection STRUCT(entity, bus) | <...> | attribute | breakpoint | value
```

A row names what the value belongs to, the coordinate it sits at, and the value there.

The coordinate columns are the attribute's own, which its [`dims`](#attributespec) declare, with a [group](groups.md) carried as one struct column named for it. `efficiency` over the `connection` group carries `connection`, `flow` over a `corridor` carries `corridor`, and `objective_weighting` over `snapshot` alone carries no entity column. A consumer that wants a group's coordinates flat takes `connection.bus` in SQL, or unnests. A numpy-backed pandas frame has no struct dtype, so a pandas consumer unnests on the DuckDB side first. `union_by_name` lets the fold union files of differing shape, and it supplies NULL for a coordinate that a file does not carry.

An attribute row holds no property columns, so `attributes["p_max_pu"]` holds every entity's `p_max_pu` together. A consumer that wants one category's rows joins `properties["entity"]`, filtered on the category, on `entity`.

`breakpoint` is NULL in the ordinary case. It carries the abscissa of a piecewise-linear value: a curve is one row per breakpoint, with `value` the ordinate at each. The record never checks or records convexity, which is a framework's judgement.

`properties` and `attributes` answer stored values only and apply no default. A NULL property is one that no layer wrote, and a coordinate that no row covers is absent. The consumer applies a default, a property's or an attribute's, through `Schema.default_for`.
