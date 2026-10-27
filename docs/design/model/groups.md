<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Groups

A group declares which tuples over several dims exist, which is a sparse subset of their product. A record stores a group as one struct column named for it, in its own file and in every attribute row over it. The value of a group column is a whole tuple or NULL. A group's property may refer to a dim, such as a bus's country per scenario.

```python
groups = {
    "connection": Group(over={"entity": "entity", "bus": "bus"}),
    "corridor": Group(over={"from": "bus", "to": "bus"}),
    "bus_country": Group(over=["bus", "scenario"]),
}
```

A dim declares an axis of labels. A group declares which combinations of labels exist, which no axis can say because the product is sparse: an entity attaches to two buses out of a thousand. A group has its own order ([axis order](dims.md#axis-order)).

`over` maps each coordinate name to the dim it draws its labels from. Two coordinates may draw on the same axis: a corridor between two buses is `(from, to)`. A list is short for the dict with identical keys and values, so `over=["bus"]` is `over={"bus": "bus"}`.

## One struct column

An attribute over a group carries the group as one struct column named for the group, with a field per coordinate in `over` order:

```text
attributes/efficiency.parquet       connection STRUCT(entity, bus) | timestep | attribute | breakpoint | value
attributes/flow.parquet             corridor STRUCT("from", "to")  | timestep | attribute | breakpoint | value
attributes/marginal_price.parquet   bus | timestep | attribute | breakpoint | value
```

The column's name says which group constrains the row, so two groups may share a set of coordinates. A group's coordinate stays inside its struct: `bus` inside `connection` is a field, and the dim `bus` is a separate column beside it.

A group file is keyed by the same column. `groups/connection.parquet` is `connection STRUCT(entity, bus) | role`, as `dims/bus.parquet` is keyed by `bus`, so a row and the group tuple it names meet on one column.

The other columns of a group file are the group's [properties](properties.md#properties), declared under the group's name. PyPSA's `role` on a connection is `properties: {connection: {role: ...}}`, which [the tool](../tools/index.md) declares. The `Group` itself declares no columns. A group has no categories. An attribute over the group is long rows in `attributes/` keyed by the struct ([where a value lives](../storage/format.md#where-a-value-lives)).

A group's property may refer to a dim, as a dim's property may. A mapping that varies along another dim needs a group: a bus's country per scenario is the group `bus_country` over `[bus, scenario]` with the property `bus_country.country: {one_of: country}`, stored as `groups/bus_country.parquet` with columns `bus_country STRUCT(bus, scenario) | country`. A mapping that does not vary is a property of the dim, `bus.country`.

## Attributes over a group

An attribute names a group in its `dims` as it names a dim. `X` is the dim `X` if one is declared, and otherwise the group `X`. Naming a group in `dims` is the only way to declare an attribute over it.

```python
"efficiency": AttributeSpec(dims={"connection", "timestep"}),  # connection | timestep
"co2_budget": AttributeSpec(dims={"country"}),  # country: the dim
```

The attribute has one coordinate for the group: the group's struct column. The group's tuples are that coordinate's labels, as an axis's labels are a dim's. A NULL struct means every tuple of the group ([the broadcast rule](attributes.md#the-broadcast-rule)). Any other value is a whole tuple. A struct with some fields NULL, such as `{entity: dc, bus: NULL}`, names no tuple, and `set`, `add_group` and the writer refuse it. The dims a group draws on broadcast on their own, so a per-bus `marginal_price` with `bus = NULL` holds one value for every bus.

Removing a label that a tuple names removes the tuple and its values ([removing labels](../layers/removal.md)). A declared group with no rows reads back as an empty relation.

`partial` may name a group as it names a dim, and then a layer overrides one tuple's value at a time. Otherwise a layer restates the attribute's whole extent along the group. The fold's key is the same for every attribute: [`partial_dims`](../layers/tree.md#partial-the-granularity-of-an-override) is one fixed tuple of exactly the dims and groups that `partial` names. `Schema.coordinates_of` lists an attribute's groups first, then its dims in the schema's declaration order.

A group may not share a dim's name, since both would be carried as a column of that name. An attribute may not name a group beside a dim that shares a name with one of the group's coordinates, since a keyword such as `bus=` in [`set`](../use/editing.md#set) would address both. `dims={"connection", "bus"}` is refused for that reason. A group whose coordinate draws on `bus` under another name, such as `port`, may sit beside `bus`.
