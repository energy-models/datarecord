"""Piecewise-linear values as breakpoint rows (design doc §3.1)."""

from datarecord.duck import layer_dir
from datarecord.layered.revision import Revision
from datarecord.record import Flags
from tests.fixtures import (
    relation,
    schema,
    write_components,
    write_connections,
    write_input,
    write_schema,
)

PROCESS = "Process"


def _curve(revision, attribute: str) -> list[tuple[float, float]]:
    """`(breakpoint, value)` pairs, in curve order - a sort on `breakpoint`."""
    df = relation(revision, attribute).order("breakpoint").df()
    return list(zip(df["breakpoint"], df["value"], strict=True))


def _flags(revision, ctype: str, attribute: str) -> Flags:
    flags = revision.record.flags(ctype)
    if attribute not in flags:
        raise AssertionError(f"{attribute} not in the owner map")
    return flags[attribute]


def _root_with_curve(con) -> Revision:
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_input(
        layer,
        "marginal_cost",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
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
    """The owner map says which keys are curves, without opening the file (§7.1)."""
    revision = _root_with_curve(con)
    write_input(
        layer_dir(revision.id),
        "p_nom",
        [{"component_type": PROCESS, "name": "steel_dri", "value": 100.0}],
    )

    curve = _flags(revision, PROCESS, "marginal_cost")
    scalar = _flags(revision, PROCESS, "p_nom")
    # A curve and a scalar are shaped alike - both rows leave every dim NULL,
    # so all three are in `broadcast` - and `breakpoints` is what separates
    # them, without opening the file.
    assert curve.broadcast == scalar.broadcast
    assert "snapshot" in curve.broadcast
    assert not curve.varies
    assert curve.breakpoints
    assert not scalar.breakpoints


def test_patch_replaces_the_whole_curve(con, base_uri):
    """`breakpoint` is not in `input_key`: one layer owns every breakpoint of a key."""
    root = _root_with_curve(con)
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "marginal_cost",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 25.0), (90.0, 70.0))
        ],
    )

    # The child's curve replaces the parent's entirely - never a mix of the
    # two, which is the hole-in-the-curve resolution the key shape rules out.
    assert _curve(child, "marginal_cost") == [(0.0, 25.0), (90.0, 70.0)]


def test_curve_on_a_connection(con, base_uri):
    """`bus` and `breakpoint` compose: one keys, the other does not (§3.1)."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_connections(
        layer,
        PROCESS,
        [
            {"name": "steel_dri", "bus": "h2_north", "role": "input"},
            {"name": "steel_dri", "bus": "dri", "role": "output"},
        ],
    )
    write_input(
        layer,
        "efficiency",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "bus": bus,
                "breakpoint": x,
                "value": v,
            }
            for bus, x, v in (
                ("h2_north", 0.0, 2.0),
                ("h2_north", 50.0, 2.4),
                ("dri", 0.0, 1.0),
            )
        ],
    )
    revision.materialise()

    # Each connection owns its own curve, so a patch to one leaves the other.
    child = revision.child()
    write_input(
        layer_dir(child.id),
        "efficiency",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "bus": "h2_north",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 3.0), (50.0, 3.5), (99.0, 4.0))
        ],
    )

    df = relation(child, "efficiency").order("bus, breakpoint").df()
    rows = list(zip(df["bus"], df["breakpoint"], df["value"], strict=True))
    assert rows == [
        ("dri", 0.0, 1.0),
        ("h2_north", 0.0, 3.0),
        ("h2_north", 50.0, 3.5),
        ("h2_north", 99.0, 4.0),
    ]


def test_curve_varying_by_snapshot(con, base_uri):
    """A curve per snapshot: `breakpoint` multiplies by the dims like anything else."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_input(
        layer,
        "marginal_cost",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
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
    write_components(layer, PROCESS, [{"name": "steel_dri"}])
    write_input(
        layer,
        "marginal_cost",
        [{"component_type": PROCESS, "name": "steel_dri", "value": 20.0}],
    )
    revision.materialise()

    child = revision.child()
    write_input(
        layer_dir(child.id),
        "marginal_cost",
        [
            {
                "component_type": PROCESS,
                "name": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 18.0), (40.0, 26.0))
        ],
    )

    assert _curve(child, "marginal_cost") == [(0.0, 18.0), (40.0, 26.0)]
    assert _flags(child, PROCESS, "marginal_cost").breakpoints
