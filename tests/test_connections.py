"""Connections as bus-keyed rows, and `bus` in the inputs key (design doc §6)."""

import pytest
from pydantic import ValidationError

from datarecord.duck import layer_dir
from datarecord.layered.record import DataRecord
from tests.fixtures import (
    schema,
    tombstone,
    tombstone_connection,
    write_components,
    write_connections,
    write_input,
    write_schema,
)

PROCESS = "Process"


def _connections(record, ctype=PROCESS):
    """`connection_frame`, asserted non-`None` for tests where a row must exist."""
    frame = record.node_cache.connection_frame(ctype)
    assert frame is not None
    return frame


def _components(record, ctype=PROCESS):
    """`component_frame`, asserted non-`None` for tests where a row must exist."""
    frame = record.node_cache.component_frame(ctype)
    assert frame is not None
    return frame


def _root(con) -> DataRecord:
    """A record whose layer has one Process with three connections."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_connections(
        layer,
        PROCESS,
        [
            {"name": "steel_dri", "bus": "h2_north", "role": "input"},
            {"name": "steel_dri", "bus": "iron_ore", "role": "input"},
            {"name": "steel_dri", "bus": "dri", "role": "output"},
        ],
    )
    write_input(
        layer,
        "efficiency",
        [
            {"component_type": PROCESS, "name": "steel_dri", "bus": b, "value": v}
            for b, v in (("h2_north", 2.1), ("iron_ore", 1.6), ("dri", 1.0))
        ],
    )
    return record


def _efficiencies(record) -> dict[str, float]:
    df = record.relation("efficiency").df()
    return dict(zip(df["bus"], df["value"], strict=True))


def test_connections_resolve_in_order(con, base_uri):
    """A component's connections come back in first-introduced order."""
    record = _root(con)
    frame = _connections(record).order("order_key").df()
    assert list(frame["bus"]) == ["h2_north", "iron_ore", "dri"]
    # `role` describes the connection rather than keying it, so it rides along
    # from the owning layer's file (§6).
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
                "component_type": PROCESS,
                "name": "steel_dri",
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
        PROCESS,
        [{"name": "steel_dri", "bus": "elec_north", "role": "input"}],
    )
    write_input(
        layer_dir(middle.id),
        "efficiency",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
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
        [{"component_type": PROCESS, "name": "steel_dri", "bus": "dri", "value": 7.7}],
    )

    assert _efficiencies(leaf) == {
        "h2_north": 2.1,
        "iron_ore": 1.6,
        "dri": 7.7,  # the bus the patch named
        "elec_north": 0.4,
    }


def test_component_level_attribute_is_unaffected(con, base_uri):
    """A NULL `bus` keys against the map's NULL, exactly as before connections existed."""
    root = _root(con)
    write_input(
        layer_dir(root.id),
        "p_nom",
        [{"component_type": PROCESS, "name": "steel_dri", "value": 100.0}],
    )
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "p_nom",
        [{"component_type": PROCESS, "name": "steel_dri", "value": 250.0}],
    )

    df = child.relation("p_nom").df()
    assert list(df["value"]) == [250.0]
    assert df["bus"].isna().all()


def test_per_connection_attribute_varies_by_snapshot_and_scenario(con, base_uri):
    """`bus` extends the key; it does not displace the dims (§6)."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_connections(
        layer, PROCESS, [{"name": "steel_dri", "bus": "h2_north", "role": "input"}]
    )
    write_input(
        layer,
        "efficiency",
        [
            # one static row, and a two-snapshot series for the same connection
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "bus": "h2_north",
                "value": 2.0,
            },
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "bus": "h2_north",
                "snapshot": "2030-01-01",
                "value": 2.5,
            },
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "bus": "h2_north",
                "snapshot": "2030-01-02",
                "value": 2.7,
            },
        ],
    )

    flags = record.store.flags(PROCESS)["efficiency"]
    # Both sets hold `snapshot`: one connection's efficiency is per-snapshot,
    # another's is a single broadcast row, and the union over the type's names
    # reports both - which is what tells a consumer one container will not do
    # (§8.1). A per-connection attribute needs no special case for this.
    assert "snapshot" in flags.varies
    assert "snapshot" in flags.broadcast
    assert not flags.breakpoints
    assert len(record.relation("efficiency").df()) == 3


def test_connection_tombstone_removes_one_connection(con, base_uri):
    """A connection tombstone drops its connection row and its `inputs/` rows."""
    root = _root(con)
    root.materialise()

    child = root.child()
    tombstone_connection(layer_dir(child.id), PROCESS, [("steel_dri", "iron_ore")])

    frame = _connections(child).df()
    assert set(frame["bus"]) == {"h2_north", "dri"}
    # ... and the attribute rows go with it, which the map can scope per
    # connection only because `bus` is in `input_key`.
    assert _efficiencies(child) == {"h2_north": 2.1, "dri": 1.0}


def test_component_tombstone_removes_every_connection(con, base_uri):
    """Deleting the component takes its connections and all their rows (§8.3)."""
    root = _root(con)
    root.materialise()

    child = root.child()
    tombstone(layer_dir(child.id), PROCESS, ["steel_dri"])

    assert child.node_cache.connection_frame(PROCESS) is None
    assert _efficiencies(child) == {}


def test_connection_exists_per_scenario(con, base_uri):
    """`scenario` keys connections here, so a tombstone can scope to one (§5.3)."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(
        layer, PROCESS, [{"name": "steel_dri", "scenario": s} for s in ("low", "high")]
    )
    write_connections(
        layer,
        PROCESS,
        [
            {"name": "steel_dri", "bus": "co2", "role": "output", "scenario": s}
            for s in ("low", "high")
        ],
    )
    record.materialise()

    child = record.child()
    tombstone_connection(
        layer_dir(child.id), PROCESS, [("steel_dri", "co2")], scenario="high"
    )

    frame = _connections(child).df()
    assert list(zip(frame["bus"], frame["scenario"], strict=True)) == [("co2", "low")]


def test_a_connection_key_must_be_partial(con, base_uri):
    """The one rule the format fixes, applied to the third key too (§6).

    A connection exists per value of a keying dim, so a tombstone selects by
    it - which needs the dim to be one a layer patches value by value (§5.3).
    """
    with pytest.raises(ValidationError, match="not `partial`"):
        schema(partial=set(), keys={"scenario": {"connection"}})


@pytest.mark.xfail(
    reason="Open question, design doc §14, deliberately unresolved: a component "
    "tombstone scoped to one scenario removes a connection that is not "
    "scenario-scoped, even though the component survives in another scenario. "
    "Deciding it needs the folded components map, which `fold_connections` cannot "
    "reach - `_fold_map` folds each kind independently. Low priority because PyPSA "
    "does not let connections differ between scenarios, so no record built for it "
    "reaches this case.",
    strict=True,
)
def test_narrower_connection_key_than_component_key(con, base_uri):
    """`component_dims` may exceed `connection_dims`; that is a model, not an error (§6).

    Components are deleted per scenario while connection existence does not
    vary by scenario at all, so a component tombstone in one scenario should
    leave the connection to the scenarios the component still has.
    """
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema(keys={"scenario": {"component"}}))
    write_components(
        layer, PROCESS, [{"name": "steel_dri", "scenario": s} for s in ("low", "high")]
    )
    write_connections(
        layer, PROCESS, [{"name": "steel_dri", "bus": "co2", "role": "output"}]
    )
    record.materialise()

    # `scenario` does not key connections, so that map carries no such column.
    assert record.node_cache.schema.connection_dims == ()
    assert "scenario" not in record.node_cache.connections.columns

    child = record.child()
    tombstone(layer_dir(child.id), PROCESS, ["steel_dri"], scenario="high")

    # The component survives in `low`, and so the connection does - the
    # widened match drops it only when no owning component row remains.
    assert list(_components(child).df()["scenario"]) == ["low"]
    assert list(_connections(child).df()["bus"]) == ["co2"]
