# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Connections as bus-keyed rows, and `bus` in the inputs key.

Notes
-----
- [connections](https://energy-models.github.io/datarecord/design/record/#connections)
"""

from pathlib import Path
from typing import Any

import duckdb
import narwhals as nw
import pytest

from datarecord.layered.revision import Revision
from datarecord.mutable import WorkingRecord
from datarecord.schema import AttributeSpec
from tests.fixtures import (
    empty_child,
    empty_root,
    relation,
    schema,
    tombstone,
    tombstone_connection,
    unpacked,
    write_axis,
    write_connections,
    write_entity_type,
    write_group,
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
    """`entity_type`, asserted non-`None` for tests where a row must exist."""
    frame = revision.resolver.entity_type(ctype)
    assert frame is not None
    return frame


def _root(store) -> Revision:
    """A record whose layer has one Process with three connections."""
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(schema(), store)
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
    df = relation(revision, "efficiency").project("connection.bus AS bus, value").df()
    return dict(zip(df["bus"], df["value"], strict=True))


def test_connections_resolve_in_order(store):
    """A component's connections come back in first-introduced order."""
    revision = _root(store)
    frame = unpacked(_connections(revision))
    assert list(frame["bus"]) == ["h2_north", "iron_ore", "dri"]
    # `role` describes the connection rather than keying it, so it rides along
    # from the owning layer's file (https://energy-models.github.io/datarecord/design/record/#connections).
    assert list(frame["role"]) == ["input", "input", "output"]


def test_patch_overrides_one_connection_only(store):
    """The sibling-clobbering case: `bus` in `input_key` scopes ownership per connection."""
    root = _root(store)
    root.materialise()

    child = empty_child(root)
    write_input(
        store.layer_dir(child.id),
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


def test_patch_hits_the_bus_it_named_not_a_position(store):
    """An intermediate layer inserting a connection does not redirect a later patch."""
    root = _root(store)
    root.materialise()

    # A middle layer prepends a connection, which under a positional encoding
    # would shift every later index by one.
    middle = empty_child(root)
    write_connections(
        store.layer_dir(middle.id),
        [{"entity": "steel_dri", "bus": "elec_north", "role": "input"}],
    )
    write_input(
        store.layer_dir(middle.id),
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

    leaf = empty_child(middle)
    write_input(
        store.layer_dir(leaf.id),
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


def test_component_level_attribute_is_unaffected(store):
    """A component attribute carries no `bus` column at all, and resolves as ever.

    `bus` is the `connection` group's coordinate, so it is on the files of the
    attributes addressed by that group and on no others - where before every
    long file carried it, all-NULL, whether or not the attribute could use it.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    root = _root(store)
    write_input(
        store.layer_dir(root.id),
        "p_nom",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 100.0}],
    )
    root.materialise()

    child = empty_child(root)
    write_input(
        store.layer_dir(child.id),
        "p_nom",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 250.0}],
    )

    df = relation(child, "p_nom").df()
    assert list(df["value"]) == [250.0]
    assert "bus" not in df.columns, "`p_nom` is not addressed by the connection group"


def test_per_connection_attribute_varies_by_snapshot_and_scenario(store):
    """`bus` extends the key; it does not displace the dims.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_schema(schema(), store)
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


def test_connection_tombstone_removes_one_connection(store):
    """A connection tombstone drops its connection row and its `inputs/` rows."""
    root = _root(store)
    root.materialise()

    child = empty_child(root)
    tombstone_connection(store.layer_dir(child.id), [("steel_dri", "iron_ore")])

    frame = unpacked(_connections(child))
    assert set(frame["bus"]) == {"h2_north", "dri"}
    # ... and the attribute rows go with it, which the map can scope per
    # connection only because `bus` is in `input_key`.
    assert _efficiencies(child) == {"h2_north": 2.1, "dri": 1.0}


def test_component_tombstone_does_not_cascade_to_its_connections(store):
    """Deleting a component does not auto-remove its connections; the author does.

    A component tombstone drops the component from the entity axis, but its
    connection rows stay in `groups/connection.parquet` — deletion is not
    cascaded across membership relations, so the two must be kept consistent by
    hand. Removing the component *and* its connections is what clears both.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    root = _root(store)
    root.materialise()

    # The component alone: its connections survive, dangling, but not their
    # values - the entity tombstone reaches the entity inside `connection`.
    child = empty_child(root)
    tombstone(store.layer_dir(child.id), PROCESS, ["steel_dri"])
    frame = child.resolver.group_frame("connection")
    assert frame is not None
    assert frame.count("*").fetchone() == (3,), "the tuples are not cascaded to"
    assert _efficiencies(child) == {}, "the entity's tombstone drops their values"

    # The connections too: now both are gone.
    both = empty_child(root)
    tombstone(store.layer_dir(both.id), PROCESS, ["steel_dri"])
    tombstone_connection(
        store.layer_dir(both.id),
        [("steel_dri", "h2_north"), ("steel_dri", "iron_ore"), ("steel_dri", "dri")],
    )
    frame = both.resolver.group_frame("connection")
    assert frame is not None, "the group was written, so it is present"
    assert frame.count("*").fetchone() == (0,), "every connection row is tombstoned"
    assert _efficiencies(both) == {}, "and their values go with them"


def test_a_dim_a_group_draws_on_still_broadcasts(store):
    """A NULL `bus` on a per-bus attribute is every bus, beside a `connection` group.

    The group carries its `bus` inside its own struct column, so the dim keeps
    its axis to expand against.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    price = AttributeSpec(dtype=nw.Float64(), dims={"bus", "snapshot"})
    write_schema(
        schema(attributes={"Bus": {"marginal_price": price}}, partial={"bus"}), store
    )
    root = empty_root(store)
    layer = store.layer_dir(root.id)
    write_axis(layer, "bus", [{"bus": "north"}, {"bus": "south"}])
    write_input(layer, "marginal_price", [{"value": 5.0}], connection=False)
    root.materialise()

    child = empty_child(root)
    write_input(
        store.layer_dir(child.id),
        "marginal_price",
        [{"bus": "south", "value": 7.0}],
        connection=False,
    )

    def prices(revision):
        df = relation(revision, "marginal_price").df()
        return dict(zip(df["bus"], df["value"], strict=True))

    assert prices(root) == {"north": 5.0, "south": 5.0}, "one row, every bus"
    assert prices(child) == {"north": 5.0, "south": 7.0}, "a patch hits its bus"
    assert "bus" in root.resolver.fold.flags()["marginal_price"].broadcast


def test_a_null_connection_broadcasts_to_every_connection(store):
    """A NULL struct is every tuple of the group, as a NULL dim is every label.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    root = _root(store)
    child = empty_child(root)
    target = Path(store.layer_dir(child.id), "inputs")
    target.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        """
        SELECT NULL::STRUCT(entity VARCHAR, bus VARCHAR) AS connection,
               'efficiency' AS attribute, NULL::DOUBLE AS breakpoint, 0.5 AS value
        """
    ).to_parquet(str(target / "efficiency.parquet"))
    assert _efficiencies(child) == {"h2_north": 0.5, "iron_ore": 0.5, "dri": 0.5}


def test_a_group_outside_partial_is_owned_whole(store):
    """Not `partial`, a layer touching one connection owns them all.

    A removed entity's values still go, though no owner key names it.

    Notes
    -----
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    root = _root(store)
    write_schema(schema(partial={"scenario"}), store)
    child = empty_child(root)
    write_input(
        store.layer_dir(child.id),
        "efficiency",
        [{"entity": "steel_dri", "bus": "h2_north", "value": 9.9}],
    )
    assert _efficiencies(child) == {"h2_north": 9.9}, "the child restates the attribute"

    gone = empty_child(root)
    tombstone(store.layer_dir(gone.id), PROCESS, ["steel_dri"])
    assert _efficiencies(gone) == {}


# -- a group drawing twice on `entity` --------------------------------------

SELLER, BUYER = "Generator", "Load"


def _contract_root(store) -> Revision:
    """Two contracts `(from, to)`, from one seller to two buyers.

    `price` is over `{contract, snapshot}`, so no top-level `entity` names
    either party: both ride inside the `contract` struct.
    """
    price = AttributeSpec(dtype=nw.Float64(), dims={"contract", "snapshot"})
    write_schema(
        schema(
            attributes={SELLER: {"price": price}},
            partial={"scenario", "contract"},
            groups={
                "connection": {"entity": "entity", "bus": "bus"},
                "contract": {"from": "entity", "to": "entity"},
            },
        ),
        store,
    )
    revision = empty_root(store)
    layer = store.layer_dir(revision.id)
    write_entity_type(layer, SELLER, [{"entity": "plant"}])
    write_entity_type(layer, BUYER, [{"entity": "mill"}, {"entity": "smelter"}])
    write_group(
        layer,
        "contract",
        [
            {"contract": {"from": "plant", "to": "mill"}},
            {"contract": {"from": "plant", "to": "smelter"}},
        ],
    )
    Path(layer, "inputs").mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        """
        SELECT {'from': 'plant', 'to': c}::STRUCT("from" VARCHAR, "to" VARCHAR)
                   AS contract,
               NULL::TIMESTAMP AS snapshot, 'price' AS attribute, v AS value
        FROM (VALUES ('mill', 10.0), ('smelter', 20.0)) t(c, v)
        """
    ).to_parquet(str(Path(layer, "inputs", "price.parquet")))
    return revision


def _prices(record) -> dict[str, float]:
    df = record.attributes["price"].collect().to_native().to_pandas()
    return {c["to"]: v for c, v in zip(df["contract"], df["value"], strict=True)}


def test_an_entity_tombstone_reaches_a_groups_second_entity(store):
    """Removing the buyer drops its contract's values, as removing the seller would.

    Every key coordinate over `entity` names an owner, so a row dies when any of
    them is tombstoned; the contract tuple stands, as a connection's does.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    root = _contract_root(store)
    assert _prices(root.record) == {"mill": 10.0, "smelter": 20.0}

    child = empty_child(root)
    tombstone(store.layer_dir(child.id), BUYER, ["mill"])
    assert _prices(child.record) == {"smelter": 20.0}, "the `to` entity's row is gone"
    contracts = child.record.groups["contract"].collect().to_native()
    assert len(contracts) == 2, "the tuples are not cascaded to"


def test_remove_reaches_a_groups_second_entity_before_and_after_commit(store):
    """Staging drops the parent's and its own rows for the `to` entity, as the fold.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
    """
    root = _contract_root(store)
    staged = WorkingRecord(root.record)
    contract: dict[str, Any] = {"from": "plant", "to": "mill"}
    staged.set("price", 30.0, **contract)
    assert _prices(staged)["mill"] == 30.0, "the staged value reads before removal"
    staged.remove(BUYER, ["mill"])
    assert _prices(staged) == {"smelter": 20.0}, "neither the base's nor the staged"

    child = root.child(staged.edits)
    assert _prices(child.record) == {"smelter": 20.0}


def test_set_names_a_contracts_parties_by_keyword_only(store):
    """No field is called `entity`, so `entity=` has nothing to fill; one row per call.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    staged = WorkingRecord(_contract_root(store).record)
    contract: dict[str, Any] = {"from": "plant", "to": "mill"}
    staged.set("price", 30.0, **contract)
    assert len(staged.attributes["price"].collect()) == 2, "one row per contract"
    assert _prices(staged) == {"mill": 30.0, "smelter": 20.0}

    with pytest.raises(ValueError, match="no `entity` coordinate"):
        staged.set("price", 50.0, entity=["plant"], to="smelter")
    with pytest.raises(ValueError, match="no `entity` coordinate"):
        staged.set("price", 50.0, to="smelter")


def test_flags_scope_a_two_entity_row_to_the_type_of_each(store):
    """A contract across two types counts towards both; a third sees nothing.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    record = _contract_root(store).record
    assert "price" in record.flags(SELLER), "the `from` entity's type"
    assert "price" in record.flags(BUYER), "the `to` entity's type too"
    assert "price" not in record.flags("Store"), "no party is of this type"
