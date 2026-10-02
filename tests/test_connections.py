# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Ports: a dim of attachments, related to their entity and bus, keying per-port values.

Notes
-----
- [connections](https://energy-models.github.io/datarecord/design/record/#connections)
"""

from datarecord.duck import layer_dir
from datarecord.layered.revision import Revision
from tests.fixtures import (
    port,
    relation,
    schema,
    tombstone,
    write_attribute,
    write_axis,
    write_connections,
    write_entity_type,
    write_ports,
    write_schema,
)

PROCESS = "Process"
H2, ORE, DRI = (port("steel_dri", p) for p in ("0", "1", "2"))


def _port_buses(revision):
    """The resolved `port_bus` relation, asserted non-`None` for tests where a row must exist."""
    frame = revision.resolver.relation_frame("port_bus")
    assert frame is not None
    return frame.df()


def _root(con) -> Revision:
    """A record whose layer has one Process with three ports."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_ports(
        layer,
        [
            {"entity": "steel_dri", "port": "0", "bus": "h2_north", "role": "input"},
            {"entity": "steel_dri", "port": "1", "bus": "iron_ore", "role": "input"},
            {"entity": "steel_dri", "port": "2", "bus": "dri", "role": "output"},
        ],
    )
    write_attribute(
        layer,
        "efficiency",
        [{"port": p, "value": v} for p, v in ((H2, 2.1), (ORE, 1.6), (DRI, 1.0))],
    )
    return revision


def _efficiencies(revision) -> dict[str, float]:
    df = relation(revision, "efficiency").df()
    return dict(zip(df["port"], df["value"], strict=True))


def test_ports_resolve_in_order(con, base_uri):
    """A component's ports come back in first-introduced order, each with its role."""
    revision = _root(con)
    assert list(_port_buses(revision)["bus"]) == ["h2_north", "iron_ore", "dri"], (
        "`port_bus` rows in the order the layer wrote them"
    )
    axis = revision.resolver.dims.axes["port"].df()
    assert dict(zip(axis["port"], axis["role"], strict=True)) == {
        H2: "input",
        ORE: "input",
        DRI: "output",
    }, "`role` is a column of the `port` axis, one per port"


def test_patch_overrides_one_port_only(con, base_uri):
    """The sibling-clobbering case: `port` in `input_key` scopes ownership per port."""
    root = _root(con)
    root.materialise()

    child = root.child()
    write_attribute(layer_dir(child.id), "efficiency", [{"port": H2, "value": 9.9}])

    assert _efficiencies(child) == {H2: 9.9, ORE: 1.6, DRI: 1.0}, (
        "the patched port takes the child's value; its siblings keep the root's"
    )


def test_patch_hits_the_port_it_named(con, base_uri):
    """An intermediate layer adding a port does not redirect a later patch."""
    root = _root(con)
    root.materialise()

    middle = root.child()
    write_ports(
        layer_dir(middle.id),
        [{"entity": "steel_dri", "port": "3", "bus": "elec_north", "role": "input"}],
    )
    elec = port("steel_dri", "3")
    write_attribute(layer_dir(middle.id), "efficiency", [{"port": elec, "value": 0.4}])
    middle.materialise()

    leaf = middle.child()
    write_attribute(layer_dir(leaf.id), "efficiency", [{"port": DRI, "value": 7.7}])

    assert _efficiencies(leaf) == {H2: 2.1, ORE: 1.6, DRI: 7.7, elec: 0.4}, (
        "the leaf's patch lands on the port it named, and the middle's port stays"
    )


def test_component_level_attribute_is_unaffected(con, base_uri):
    """A component attribute carries no `port` column at all, and resolves as ever.

    `port` is a dim like any other, so it is on the files of the attributes over
    it and on no others.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    root = _root(con)
    write_attribute(
        layer_dir(root.id), "p_nom", [{"entity": "steel_dri", "value": 100.0}]
    )
    root.materialise()

    child = root.child()
    write_attribute(
        layer_dir(child.id), "p_nom", [{"entity": "steel_dri", "value": 250.0}]
    )

    df = relation(child, "p_nom").df()
    assert list(df["value"]) == [250.0]
    assert "port" not in df.columns, "`p_nom` is not over `port`"


def test_per_port_attribute_varies_by_snapshot(con, base_uri):
    """`port` extends the key; it does not displace the dims.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_ports(
        layer,
        [
            {"entity": "steel_dri", "port": "0", "bus": "h2_north"},
            {"entity": "steel_dri", "port": "1", "bus": "dri"},
        ],
    )
    write_attribute(
        layer,
        "efficiency",
        [
            {"port": H2, "value": 2.0},
            {"port": ORE, "snapshot": "2030-01-01", "value": 2.5},
            {"port": ORE, "snapshot": "2030-01-02", "value": 2.7},
        ],
    )

    flags = revision.record.flags(port=[H2, ORE])["efficiency"]
    assert "snapshot" in flags.varies, "one port's efficiency is per-snapshot"
    assert "snapshot" in flags.broadcast, "the other's is a single broadcast row"
    assert not flags.breakpoints
    assert len(relation(revision, "efficiency").df()) == 3, (
        "one broadcast row and a two-snapshot series"
    )


def test_port_tombstone_removes_one_port(con, base_uri):
    """A port tombstone drops its relation rows and its `attributes/` rows."""
    root = _root(con)
    root.materialise()

    child = root.child()
    write_axis(layer_dir(child.id), "port", [{"port": ORE, "deleted": True}])

    assert set(_port_buses(child)["bus"]) == {"h2_north", "dri"}, (
        "the removed port's `port_bus` row goes with its label"
    )
    assert _efficiencies(child) == {H2: 2.1, DRI: 1.0}, (
        "and so do its efficiency rows, `port` being in `input_key`"
    )


def test_component_tombstone_removes_its_connections(con, base_uri):
    """Deleting a component removes its connections with it.

    A component tombstone removes the `entity` label, and every relation row keyed
    on that label goes with it - the `connection` rows included.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    root = _root(con)
    write_connections(
        layer_dir(root.id),
        [{"entity": "steel_dri", "bus": b} for b in ("h2_north", "iron_ore", "dri")],
    )
    root.materialise()

    child = root.child()
    tombstone(layer_dir(child.id), PROCESS, ["steel_dri"])
    assert child.resolver.relation_frame("connection") is None, (
        "a removed component leaves no connection rows behind"
    )
