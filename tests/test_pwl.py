# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Piecewise-linear values as breakpoint rows.

Notes
-----
- [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
"""

from datarecord.duck import layer_dir
from datarecord.layered.revision import Revision
from datarecord.record import Flags
from tests.fixtures import (
    names,
    port,
    relation,
    schema,
    write_attribute,
    write_entity_type,
    write_ports,
    write_schema,
)

PROCESS = "Process"


def _curve(revision, attribute: str) -> list[tuple[float, float]]:
    """`(breakpoint, value)` pairs, in curve order - a sort on `breakpoint`."""
    df = relation(revision, attribute).order("breakpoint").df()
    return list(zip(df["breakpoint"], df["value"], strict=True))


def _flags(revision, ctype: str, attribute: str) -> Flags:
    record = revision.record
    flags = record.flags(entity=names(record, ctype))
    if attribute not in flags:
        raise AssertionError(f"{attribute} not in the owner map")
    return flags[attribute]


def _root_with_curve(con) -> Revision:
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_attribute(
        layer,
        "marginal_cost",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 20.0), (50.0, 35.0), (80.0, 60.0))
        ],
    )
    return revision


def test_curve_resolves_as_breakpoint_rows(con, base_uri):
    """A curve is N rows of one key, ordered by sorting on `breakpoint`."""
    revision = _root_with_curve(con)
    assert _curve(revision, "marginal_cost") == [
        (0.0, 20.0),
        (50.0, 35.0),
        (80.0, 60.0),
    ]


def test_breakpoints_distinguishes_curve_from_scalar(con, base_uri):
    """The owner map says which keys are curves, without opening the file.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    revision = _root_with_curve(con)
    write_attribute(
        layer_dir(revision.id),
        "p_nom",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 100.0}],
    )

    curve = _flags(revision, PROCESS, "marginal_cost")
    scalar = _flags(revision, PROCESS, "p_nom")
    assert curve.broadcast == scalar.broadcast, "both rows leave every value dim NULL"
    assert "snapshot" in curve.broadcast
    assert curve.varies == scalar.varies == frozenset({"entity"}), (
        "both rows name their entity and nothing else, so only `breakpoints` "
        "separates a curve from a scalar"
    )
    assert curve.breakpoints
    assert not scalar.breakpoints


def test_patch_replaces_the_whole_curve(con, base_uri):
    """`breakpoint` is not in `input_key`: one layer owns every breakpoint of a key."""
    root = _root_with_curve(con)
    root.materialise()

    child = root.child()
    write_attribute(
        layer_dir(child.id),
        "marginal_cost",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 25.0), (90.0, 70.0))
        ],
    )

    # The child's curve replaces the parent's entirely - never a mix of the
    # two, which is the hole-in-the-curve resolution the key shape rules out.
    assert _curve(child, "marginal_cost") == [(0.0, 25.0), (90.0, 70.0)]


def test_curve_on_a_port(con, base_uri):
    """`port` and `breakpoint` compose: one keys, the other does not.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_ports(
        layer,
        [
            {"entity": "steel_dri", "port": "0", "bus": "h2_north", "role": "input"},
            {"entity": "steel_dri", "port": "1", "bus": "dri", "role": "output"},
        ],
    )
    h2, dri = port("steel_dri", "0"), port("steel_dri", "1")
    write_attribute(
        layer,
        "efficiency",
        [
            {"port": p, "breakpoint": x, "value": v}
            for p, x, v in ((h2, 0.0, 2.0), (h2, 50.0, 2.4), (dri, 0.0, 1.0))
        ],
    )
    revision.materialise()

    child = revision.child()
    write_attribute(
        layer_dir(child.id),
        "efficiency",
        [
            {"port": h2, "breakpoint": x, "value": v}
            for x, v in ((0.0, 3.0), (50.0, 3.5), (99.0, 4.0))
        ],
    )

    df = relation(child, "efficiency").order("port, breakpoint").df()
    rows = list(zip(df["port"], df["breakpoint"], df["value"], strict=True))
    assert rows == [
        (h2, 0.0, 3.0),
        (h2, 50.0, 3.5),
        (h2, 99.0, 4.0),
        (dri, 0.0, 1.0),
    ], "each port owns its own curve, so a patch to one leaves the other"


def test_curve_varying_by_snapshot(con, base_uri):
    """A curve per snapshot: `breakpoint` multiplies by the dims like anything else."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_attribute(
        layer,
        "marginal_cost",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "snapshot": snap,
                "breakpoint": x,
                "value": v,
            }
            for snap, x, v in (
                ("2030-01-01", 0.0, 20.0),
                ("2030-01-01", 50.0, 30.0),
                ("2030-01-02", 0.0, 22.0),
                ("2030-01-02", 50.0, 33.0),
            )
        ],
    )

    flags = _flags(revision, PROCESS, "marginal_cost")
    # A curve that varies over snapshots: on the snapshot axis and a curve too.
    assert "snapshot" in flags.varies
    assert "snapshot" not in flags.broadcast
    assert flags.breakpoints
    assert len(relation(revision, "marginal_cost").df()) == 4


def test_scalar_replaced_by_a_curve(con, base_uri):
    """A child may turn a scalar into a curve; it is one key either way."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_attribute(
        layer,
        "marginal_cost",
        [{"entity_type": PROCESS, "entity": "steel_dri", "value": 20.0}],
    )
    revision.materialise()

    child = revision.child()
    write_attribute(
        layer_dir(child.id),
        "marginal_cost",
        [
            {
                "entity_type": PROCESS,
                "entity": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 18.0), (40.0, 26.0))
        ],
    )

    assert _curve(child, "marginal_cost") == [(0.0, 18.0), (40.0, 26.0)]
    assert _flags(child, PROCESS, "marginal_cost").breakpoints


def test_a_default_curve_beside_its_exception_is_kept_or_dropped_whole(con, base_uri):
    """One layer holds a curve for every port and another for one port.

    At the named port the exception wins with all its breakpoints, and none of
    the default's: the read picks per coordinate, not per breakpoint.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_entity_type(layer, PROCESS, [{"entity": "steel_dri"}])
    write_ports(
        layer,
        [
            {"entity": "steel_dri", "port": "0", "bus": "h2_north", "role": "input"},
            {"entity": "steel_dri", "port": "1", "bus": "dri", "role": "output"},
        ],
    )
    h2, dri = port("steel_dri", "0"), port("steel_dri", "1")
    write_attribute(
        layer,
        "efficiency",
        [
            {"port": p, "breakpoint": x, "value": v}
            for p, x, v in (
                (None, 0.0, 2.0),
                (None, 50.0, 2.4),
                (None, 99.0, 2.8),
                (dri, 0.0, 1.0),
                (dri, 50.0, 1.5),
            )
        ],
    )

    df = relation(revision, "efficiency").order("port, breakpoint").df()
    rows = list(zip(df["port"], df["breakpoint"], df["value"], strict=True))
    assert rows == [
        (h2, 0.0, 2.0),
        (h2, 50.0, 2.4),
        (h2, 99.0, 2.8),
        (dri, 0.0, 1.0),
        (dri, 50.0, 1.5),
    ], "the named port takes its own two-point curve, the other the default's three"
