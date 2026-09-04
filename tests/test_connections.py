"""Connections as bus-keyed rows, and `bus` in the inputs key.

Notes
-----
- [connections](https://energy-models.github.io/datarecord/design/record/#connections)
"""

from datarecord.duck import layer_dir
from datarecord.layered.revision import Revision
from tests.fixtures import (
    relation,
    schema,
    tombstone,
    tombstone_connection,
    write_connections,
    write_entity_type,
    write_input,
    write_schema,
)

PROCESS = "Process"


def _connections(revision):
    """`group_frame`, asserted non-`None` for tests where a row must exist.

    No type: one `groups/connection.parquet` holds every type's rows, and these
    tests declare a single one.
    """
    frame = revision.resolver.group_frame("connection")
    assert frame is not None
    return frame


def _entity_type_frame(revision, ctype=PROCESS):
    """`entity_type_frame`, asserted non-`None` for tests where a row must exist."""
    frame = revision.resolver.entity_type_frame(ctype)
    assert frame is not None
    return frame


def _root(con) -> Revision:
    """A record whose layer has one Process with three connections."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_connections(
        layer,
        [
            {"entity": "steel_dri", "bus": "h2_north", "role": "input"},
            {"entity": "steel_dri", "bus": "iron_ore", "role": "input"},
            {"entity": "steel_dri", "bus": "dri", "role": "output"},
        ],
    )
    write_input(
        layer,
        "efficiency",
        [
            {"entity_type": PROCESS, "entity": "steel_dri", "bus": b, "value": v}
            for b, v in (("h2_north", 2.1), ("iron_ore", 1.6), ("dri", 1.0))
        ],
    )
    return revision


def _efficiencies(revision) -> dict[str, float]:
    df = relation(revision, "efficiency").df()
    return dict(zip(df["bus"], df["value"], strict=True))


def test_connections_resolve_in_order(con, base_uri):
    """A component's connections come back in first-introduced order."""
    revision = _root(con)
    frame = _connections(revision).df()
    assert list(frame["bus"]) == ["h2_north", "iron_ore", "dri"]
    # `role` describes the connection rather than keying it, so it rides along
    # from the owning layer's file (https://energy-models.github.io/datarecord/design/record/#connections).
    assert list(frame["role"]) == ["input", "input", "output"]


def test_patch_overrides_one_connection_only(con, base_uri):
    """The sibling-clobbering case: `bus` in `input_key` scopes ownership per connection."""
    root = _root(con)
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "efficiency",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "h2_north",
                "value": 9.9,
            }
        ],
    )

    # The patched connection takes the child's value; its siblings keep the
    # root's rather than vanishing.
    assert _efficiencies(child) == {"h2_north": 9.9, "iron_ore": 1.6, "dri": 1.0}


def test_patch_hits_the_bus_it_named_not_a_position(con, base_uri):
    """An intermediate layer inserting a connection does not redirect a later patch."""
    root = _root(con)
    root.materialise()

    # A middle layer prepends a connection, which under a positional encoding
    # would shift every later index by one.
    middle = root.child()
    write_connections(
        layer_dir(middle.id),
        [{"entity": "steel_dri", "bus": "elec_north", "role": "input"}],
    )
    write_input(
        layer_dir(middle.id),
        "efficiency",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "elec_north",
                "value": 0.4,
            }
        ],
    )
    middle.materialise()

    leaf = middle.child()
    write_input(
        layer_dir(leaf.id),
        "efficiency",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "dri",
                "value": 7.7,
            }
        ],
    )

    assert _efficiencies(leaf) == {
        "h2_north": 2.1,
        "iron_ore": 1.6,
        "dri": 7.7,  # the bus the patch named
        "elec_north": 0.4,
    }


def test_component_level_attribute_is_unaffected(con, base_uri):
    """A component attribute carries no `bus` column at all, and resolves as ever.

    `bus` is the `connection` group's coordinate, so it is on the files of the
    attributes addressed by that group and on no others - where before every
    long file carried it, all-NULL, whether or not the attribute could use it.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    root = _root(con)
    write_input(
        layer_dir(root.id),
        "p_nom",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 100.0}],
    )
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "p_nom",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 250.0}],
    )

    df = relation(child, "p_nom").df()
    assert list(df["value"]) == [250.0]
    assert "bus" not in df.columns, "`p_nom` is not addressed by the connection group"


def test_per_connection_attribute_varies_by_snapshot_and_scenario(con, base_uri):
    """`bus` extends the key; it does not displace the dims.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_connections(
        layer, [{"entity": "steel_dri", "bus": "h2_north", "role": "input"}]
    )
    write_input(
        layer,
        "efficiency",
        [
            # one static row, and a two-snapshot series for the same connection
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "h2_north",
                "value": 2.0,
            },
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "h2_north",
                "snapshot": "2030-01-01",
                "value": 2.5,
            },
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "bus": "h2_north",
                "snapshot": "2030-01-02",
                "value": 2.7,
            },
        ],
    )

    flags = revision.record.flags(PROCESS)["efficiency"]
    # Both sets hold `snapshot`: one connection's efficiency is per-snapshot,
    # another's is a single broadcast row, and the union over the type's names
    # reports both - which is what tells a consumer one container will not do
    # (https://energy-models.github.io/datarecord/design/record/#flags). A per-connection attribute needs no special case for this.
    assert "snapshot" in flags.varies
    assert "snapshot" in flags.broadcast
    assert not flags.breakpoints
    assert len(relation(revision, "efficiency").df()) == 3


def test_connection_tombstone_removes_one_connection(con, base_uri):
    """A connection tombstone drops its connection row and its `inputs/` rows."""
    root = _root(con)
    root.materialise()

    child = root.child()
    tombstone_connection(layer_dir(child.id), [("steel_dri", "iron_ore")])

    frame = _connections(child).df()
    assert set(frame["bus"]) == {"h2_north", "dri"}
    # ... and the attribute rows go with it, which the map can scope per
    # connection only because `bus` is in `input_key`.
    assert _efficiencies(child) == {"h2_north": 2.1, "dri": 1.0}


def test_component_tombstone_does_not_cascade_to_its_connections(con, base_uri):
    """Deleting a component does not auto-remove its connections; the author does.

    A component tombstone drops the component from the entity axis, but its
    connection rows stay in `groups/connection.parquet` — deletion is not
    cascaded across membership relations, so the two must be kept consistent by
    hand. Removing the component *and* its connections is what clears both.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    root = _root(con)
    root.materialise()

    # The component alone: its connections survive, dangling.
    child = root.child()
    tombstone(layer_dir(child.id), PROCESS, ["steel_dri"])
    assert child.resolver.group_frame("connection") is not None

    # The connections too: now both are gone.
    both = root.child()
    tombstone(layer_dir(both.id), PROCESS, ["steel_dri"])
    tombstone_connection(
        layer_dir(both.id),
        [("steel_dri", "h2_north"), ("steel_dri", "iron_ore"), ("steel_dri", "dri")],
    )
    assert both.resolver.group_frame("connection") is None
