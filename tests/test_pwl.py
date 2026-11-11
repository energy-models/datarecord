# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Piecewise-linear values as breakpoint rows.

Notes
-----
- [wide and long rows](https://energy-models.github.io/datarecord/reference/data-model/#wide-and-long-rows)
"""

from datarecord.protocols import Flags
from tests.fixtures import (
    HandBuilt,
    empty_child,
    empty_root,
    relation,
    schema,
    write_attribute,
    write_component,
)

PROCESS = "Process"


def _curve(revision, attribute: str) -> list[tuple[float, float]]:
    """`(breakpoint, value)` pairs, in curve order - a sort on `breakpoint`."""
    df = relation(revision, attribute).order("breakpoint").df()
    return list(zip(df["breakpoint"], df["value"], strict=True))


def _flags(revision, attribute: str) -> Flags:
    flags = revision.record.flags()
    if attribute not in flags:
        raise AssertionError(f"{attribute} not in the owner map")
    return flags[attribute]


def _root_with_curve(store) -> HandBuilt:
    """A root whose `steel_dri` has a three-point `marginal_cost`."""
    layer = empty_root(store, schema())
    write_component(layer, PROCESS, [{"entity": "steel_dri"}])
    rows = [
        {"ctype": PROCESS, "entity": "steel_dri", "breakpoint": x, "value": v}
        for x, v in ((0.0, 20.0), (50.0, 35.0), (80.0, 60.0))
    ]
    write_attribute(layer, "marginal_cost", rows)
    return layer


def test_a_child_curve_replaces_the_parents_value_whole(store):
    """`breakpoint` is not in `owner_map_index`: one layer owns every breakpoint of a map row.

    So a child's curve replaces a parent's curve entirely - never a mix of the
    two.
    """
    root = _root_with_curve(store).commit()
    root.materialise()

    child_layer = empty_child(root)
    write_attribute(
        child_layer,
        "marginal_cost",
        [
            {
                "ctype": PROCESS,
                "entity": "steel_dri",
                "breakpoint": x,
                "value": v,
            }
            for x, v in ((0.0, 25.0), (90.0, 70.0))
        ],
    )
    child = child_layer.commit()

    assert _curve(child, "marginal_cost") == [(0.0, 25.0), (90.0, 70.0)], (
        "the child's breakpoints only"
    )
    assert _flags(child, "marginal_cost").breakpoints


def test_breakpoints_distinguishes_curve_from_scalar(store):
    """The owner map says which map rows are curves, without opening the file.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    layer = _root_with_curve(store)
    write_attribute(
        layer,
        "p_nom",
        [{"ctype": PROCESS, "entity": "steel_dri", "value": 100.0}],
    )
    revision = layer.commit()

    curve = _flags(revision, "marginal_cost")
    scalar = _flags(revision, "p_nom")
    # A curve and a scalar are shaped alike - both rows name the entity and
    # leave every other dim NULL - and `breakpoints` is what separates them,
    # without opening the file.
    assert curve.broadcast == scalar.broadcast
    assert curve.varies == scalar.varies
    assert "snapshot" in curve.broadcast
    assert curve.varies == {"entity"}, "only the entity is named"
    assert curve.breakpoints
    assert not scalar.breakpoints
    assert _curve(revision, "marginal_cost") == [
        (0.0, 20.0),
        (50.0, 35.0),
        (80.0, 60.0),
    ], "a curve is N rows of one coordinate, ordered by sorting on `breakpoint`"
