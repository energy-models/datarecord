<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Multi-dims

A multi-dim is a dim whose labels are tuples of other dims' labels, which is a sparse subset of their product. It is declared under `dimensions` with `over` and has its own table in `dims/`, as any dim does. A record carries a multi-dim as one struct column named for it, in its table and in every attribute row over it. The value of that column is a whole tuple or NULL. A multi-dim's property may refer to a dim, such as a bus's country per scenario.

```python
dimensions = {
    "entity": Dimension(dtype="str"),
    "bus": Dimension(dtype="str"),
    "scenario": Dimension(dtype="str"),
    "connection": Dimension(over={"entity": "entity", "bus": "bus"}),
    "corridor": Dimension(over={"from": "bus", "to": "bus"}),
    "bus_country": Dimension(over=["bus", "scenario"]),
}
```

A dim without `over` declares an axis of labels. A multi-dim declares which combinations of labels exist, which no single axis can say because the product is sparse: an entity attaches to two buses out of a thousand. A multi-dim has its own order, as any axis has ([axis order](dims.md#axis-order)).

`over` maps each coordinate name to the dim it draws its labels from. Two coordinates may draw on the same dim: a corridor between two buses is `(from, to)`. A list is short for the dict with identical keys and values, so `over=["bus"]` is `over={"bus": "bus"}`. Each coordinate draws on a declared dim that is no multi-dim. A multi-dim has no `dtype` and no `one_of`, and the schema refuses one declaring `over` beside either. `Schema.dimensions` holds every dim, multi or not, and `Dimension.over` holds the map.

## One struct column

An attribute over a multi-dim carries it as one struct column named for it, with a field per coordinate in `over` order:

```text
attributes/efficiency.parquet       connection STRUCT(entity, bus) | timestep | attribute | breakpoint | value
attributes/flow.parquet             corridor STRUCT("from", "to")  | timestep | attribute | breakpoint | value
attributes/marginal_price.parquet   bus | timestep | attribute | breakpoint | value
```

The column's name says which multi-dim constrains the row, so two multi-dims may share a set of coordinates. A coordinate stays inside its struct: `bus` inside `connection` is a field, and the dim `bus` is a separate column beside it.

A multi-dim's table is keyed by the same column. `dims/connection.parquet` is `connection STRUCT(entity, bus) | role`, as `dims/bus.parquet` is keyed by `bus`, so a row and the tuple it names meet on one column. A layer that changes its tuples or their properties writes it whole and in order, as it writes any axis ([one fold for every axis](../layers/resolution.md#one-fold-for-every-axis)).

The other columns of the table are the multi-dim's [properties](properties.md#properties), declared under its name. PyPSA's `role` on a connection is `properties: {connection: {role: ...}}`, which [the tool](../tools/index.md) declares. An attribute over the multi-dim is long rows in `attributes/` keyed by the struct ([where a value lives](../storage/format.md#where-a-value-lives)).

A multi-dim's property may refer to a dim, as any dim's property may. One value per tuple is what a property is. A mapping that varies along another dim needs a multi-dim: a bus's country per scenario is the multi-dim `bus_country` over `[bus, scenario]` with the property `bus_country.country: {one_of: country}`, stored as `dims/bus_country.parquet` with columns `bus_country STRUCT(bus, scenario) | country`. A mapping that does not vary is a property of the dim, `bus.country`. A property refers to a dim that is no multi-dim.

## Categories of a multi-dim

A property of a multi-dim in `partial` that refers to a dim with `one_of` categorizes the multi-dim, by the rule a dim follows ([categories](properties.md#categories)):

```yaml
dimensions:
  role: { one_of: [input, output, attached] }
  connection: { over: { entity: entity, bus: bus } }
properties:
  connection:
    role: { one_of: role }
partial: [entity, bus, connection]
```

`Schema.categorizing("connection")` lists `role`. A [`default_by`](properties.md#default_by-a-default-per-category) keyed `connection.role` applies to a value that reaches `connection`: an attribute over it, or a property of it. `default_for(name, role="output")` and [`flags(role="output")`](../use/reading.md#flags) key on it by plain name, and the [selector keyword](../use/editing.md#set) `role="output"` on an attribute over `connection`, or on a property of it, picks the tuples with that role, narrowed by any keyword naming one of its coordinates. A multi-dim outside `partial` has no categories.

## Coordinates

`connection.bus` names the `bus` of each tuple, wherever a dotted `subject.name` stands, as a property is named. `Schema.resolve("connection.bus")` answers the multi-dim and the dim the coordinate draws on. A coordinate is part of the key, not a value, so [`set`](../use/editing.md#set) refuses `set("connection.bus", ...)` and points at [`remove` and `add`](../use/editing.md#add-remove), and a [`default_by`](properties.md#default_by-a-default-per-category) keyed on a coordinate is refused, a coordinate being no category. A property of a multi-dim named like one of its coordinates is refused at load ([names](schema.md#names)).

A selector keyword stays bare: `bus="north"` on an attribute over `connection` selects the tuples whose `bus` is `north`.

## Attributes over a multi-dim

An attribute names a multi-dim in its `dims` as it names any dim:

```python
"efficiency": AttributeSpec(dims={"connection", "timestep"}),  # connection | timestep
"co2_budget": AttributeSpec(dims={"country"}),  # country
```

The attribute has one coordinate for the multi-dim: its struct column. The tuples are that coordinate's labels, as an axis's labels are a dim's. A NULL struct means every tuple ([the broadcast rule](attributes.md#the-broadcast-rule)). Any other value is a whole tuple. A struct with some fields NULL, such as `{entity: dc, bus: NULL}`, names no tuple, and `set`, `add` and the writer refuse it. The dims a multi-dim draws on broadcast on their own, so a per-bus `marginal_price` with `bus = NULL` holds one value for every bus.

Removing a label that a tuple names removes the tuple and its values ([removing labels](../layers/removal.md)). A declared multi-dim with no rows reads back as an empty relation.

`partial` may name a multi-dim as it names a dim, and then a layer overrides one tuple's value at a time. Otherwise a layer restates the attribute's whole extent along it. The fold's key is the same for every attribute: [`partial_dims`](../layers/tree.md#partial-the-granularity-of-an-override) is one fixed tuple of exactly the dims that `partial` names. `Schema.coordinates_of` lists an attribute's multi-dims first, then its other dims in the schema's declaration order.

An attribute may not name a multi-dim beside a dim that shares a name with one of its coordinates, since a keyword such as `bus=` in [`set`](../use/editing.md#set) would address both. `dims={"connection", "bus"}` is refused for that reason. A multi-dim whose coordinate draws on `bus` under another name, such as `port`, may sit beside `bus`.
