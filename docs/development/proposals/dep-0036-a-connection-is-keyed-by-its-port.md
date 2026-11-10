<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 36: a connection is keyed by its port

| DEP      | 36                                                                                                              |
| -------- | --------------------------------------------------------------------------------------------------------------- |
| Title    | a connection is keyed by its port                                                                               |
| Author   | Fabian Hofmann, Jonas Hörsch                                                                                    |
| Status   | Implemented (2026-11-10)                                                                                        |
| Requires | [DEP 12](dep-0012-properties.md), [DEP 23](dep-0023-multi-dims.md)                                              |
| Created  | 2026-11-10                                                                                                      |
| Version  | Unreleased                                                                                                      |
| Replaces | the PyPSA tool's `connection` over `(entity, bus)` and its `role` in [DEP 23](dep-0023-multi-dims.md) (in part) |

The design landed in [the tools](../../reference/tools.md#ports) and [layers and `partial`](../../about/layers.md). Where they and this DEP differ, follow those pages.

## Abstract

The PyPSA tool keys its `connection` multi-dim by `(entity, port)`, where the port is PyPSA's number in `bus0`, `bus1`, … and a one-port component has port 0. The bus becomes the property `connection.bus`, which refers to the dim `bus`. The dim `role` and the property `connection.role` go.

## Motivation

A component that attaches to one bus at two ports loses a port on the round trip. For a Link with `bus0="north"`, `bus1="north"`, `bus2="south"`, `efficiency=3.0` and `efficiency2=-1.0`, `to_datarecord` writes two connection rows with the key `(heat_pump, north)`. The fold keeps one of them, so `build` returns a Link with `bus0="north"`, `bus1="south"` and `efficiency=-1.0`. No step raises.

The key `(entity, bus)` has one tuple for two ports at one bus. [DEP 23](dep-0023-multi-dims.md) carried the group of [DEP 2](dep-0002-groups.md) and [DEP 10](dep-0010-a-group-is-a-struct-column.md) over as a multi-dim over `(entity, bus)`, with `role` telling the ports apart. The design identified a connection by the bus it attaches to, never by its position. `build` then numbered the ports itself: the `input` first, the others in axis order.

## Specification

| Name             | Is                                                                          |
| ---------------- | --------------------------------------------------------------------------- |
| `port`           | a dim of dtype `int`, in `partial`                                          |
| `connection`     | a multi-dim over `{entity: entity, port: port}`, in `partial`               |
| `connection.bus` | a property with `one_of: bus`, the bus the port attaches to                 |
| `role`           | gone, as a dim and as the property `connection.role`                        |
| per-port values  | `efficiency`, `delay`, `rate`, `p`, `q` and the rest stay over `connection` |

- The port is the number in PyPSA's column name. A Link's `bus2` is port 2, and a Generator's `bus` is port 0.
- `to_datarecord` writes the `port` axis in ascending order: ports 0 and 1, which PyPSA's registry declares, and each further port a connection has.
- `build` names a port's columns by the component type. A type whose registry declares `bus0` gets `bus0`, `p0`, …, and any other type gets `bus` and `p` at port 0. PyPSA's suffix rule holds as before: `efficiency` at port 1 of a Link, and `efficiency2` at port 2.
- A stochastic network's connections are one row per `(entity, port)` across its scenarios.
- `verify` and a selection find an entity attached to a dropped bus through `connection.bus`.

## Rationale

The port is the key PyPSA has. Every per-port column in PyPSA is a stem and a port number, so `efficiency2` maps onto `(entity, 2)` one to one. The tool already undoes PyPSA's quirk that leaves port 1 unsuffixed, so a number costs nothing at `build`, and `build` no longer derives an order.

The bus is a value of the port, so it is a property. A property with `one_of: bus` keeps every check a coordinate on `bus` had: the writer refuses a bus that is no label, and a selection that drops a bus drops the connections at it.

`role` stated less than the port. `input` is port 0 and `output` any other, and a one-port component's `attached` follows from its type, which `entity.ctype` states.

What it costs:

- A record the PyPSA tool wrote before is unreadable by this version. You export the network again.
- `remove("bus", ...)` refuses a bus while a connection is at it, because datarecord refuses to remove a label that a property has as a value. Before, removing the bus removed the tuples that contained it. You remove those connections first.
- `port` has an axis. A record whose network has no component with a port still lists ports 0 and 1.

## Usage

`port=` names one port. `bus=` selects connections through the property, every port of every type at that bus, and `entity=` narrows them:

```python
w.add("connection", [{"entity": "wind1", "port": 0}], bus="north")
w.set("efficiency", 0.9, entity="link1", port=1)
w.set("efficiency", 0.7, entity="out_south", bus="south")

at_north = w.properties["connection"].filter("bus = 'north'")
w.remove("connection", at_north.project("connection"))
w.remove("bus", ["north"])
```

## Implementation notes

1. Declare `port`, key `connection` on it, and replace `connection.role` with `connection.bus`.
2. Export each port with its number, and key per-port inputs and results on `(entity, port)`.
3. Name the columns in `build` from the port and the component type.

Checks: a Link with two ports at one bus round-trips with both efficiencies, and every existing round trip of the PyPSA tool passes.

## Rejected ideas

- PyPSA's string index, `""`, `"0"`, `"1"`, as the label: a one-port component's `""` is no number, and the record would carry PyPSA's spelling rather than the port.
- `port` with `one_of`: datarecord takes `one_of` on a `str` dim alone, and a fixed list would cap how many ports a Link has.
- Keeping `(entity, bus)` and adding the port as a property: the key would still hold one row for two ports at one bus.

## Open issues

- Whether removing a bus should cascade through `connection.bus`, so one call removes the connections at it.
