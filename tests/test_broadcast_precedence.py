# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A default and its exceptions in one layer: the row naming more dims wins.

Notes
-----
- [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
"""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.layered.resolve import write_schema
from datarecord.layered.write import write_record
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema
from datarecord.sources import from_sources

LABELS = {
    "entity": ["wind", "gas"],
    "port": ["dc_out", "ac_in"],
    "snapshot": ["s0", "s1"],
    "scenario": ["high", "low"],
}


def _schema(partial: set[str]) -> Schema:
    return Schema(
        dimensions={d: Dimension(dtype=nw.String()) for d in LABELS},
        attributes={
            "efficiency": AttributeSpec(dtype=nw.Float64(), dims={"port", "snapshot"}),
            "p_max_pu": AttributeSpec(
                dtype=nw.Float64(), dims={"entity", "snapshot", "scenario"}
            ),
        },
        partial=frozenset(partial),
    )


def _staged(con, base_uri, partial: set[str]) -> tuple[Revision, WorkingRecord]:
    write_schema(_schema(partial), base_uri)
    root = Revision.create(con)
    staged = WorkingRecord(root.record, con)
    for dim, labels in LABELS.items():
        staged.add(dim, pd.DataFrame({dim: labels}))
    return root, staged


def _values(record, attribute: str) -> list[tuple]:
    df = record.attributes[attribute].collect().to_native().to_pandas()
    coordinates = [c for c in df.columns if c in LABELS]
    return sorted((*(r[c] for c in coordinates), r["value"]) for _, r in df.iterrows())


@pytest.mark.parametrize(
    ("partial", "edits", "attribute", "expected"),
    [
        pytest.param(
            {"entity", "port"},
            [({"port": ["dc_out"]}, 0.9), ({}, 0.95)],
            "efficiency",
            [("ac_in", None, 0.95), ("dc_out", None, 0.9)],
            id="a-port-default-beside-its-exception",
        ),
        pytest.param(
            {"entity", "snapshot"},
            [
                ({"entity": ["wind"]}, 0.5),
                ({"entity": ["wind"], "snapshot": "s1"}, 0.7),
            ],
            "p_max_pu",
            [
                ("wind", "s0", None, 0.5),
                ("wind", "s1", None, 0.7),
            ],
            id="a-static-value-beside-one-snapshot",
        ),
        pytest.param(
            {"entity", "snapshot"},
            [
                ({}, 1.0),
                ({"entity": ["wind"]}, 0.5),
                ({"entity": ["wind"], "snapshot": "s1"}, 0.7),
            ],
            "p_max_pu",
            [
                ("gas", "s0", None, 1.0),
                ("gas", "s1", None, 1.0),
                ("wind", "s0", None, 0.5),
                ("wind", "s1", None, 0.7),
            ],
            id="three-rows-nested-at-one-coordinate",
        ),
    ],
)
def test_one_value_per_coordinate_from_one_layer(
    con, base_uri, partial, edits, attribute, expected
):
    """Before the fix a read gave both the default and the exception at a coordinate.

    `to_sources` settled it on its own; the fold read handed back every row the
    owner map matched, and one layer owns the coordinate for both.
    """
    root, staged = _staged(con, base_uri, partial)
    for dims, value in edits:
        staged.set(attribute, value, **dims)
    child = staged.commit(NewChild(root))
    got = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in _values(child.record, attribute)
    ]
    assert got == expected, "one row per coordinate, the one naming more dims"


@pytest.mark.parametrize(
    ("edits", "nulls"),
    [
        pytest.param(
            [({"entity": ["wind"]}, 0.5), ({"snapshot": "s1"}, 0.7)],
            ("['entity', 'scenario']", "['scenario', 'snapshot']"),
            id="entity-against-snapshot-overlap-at-wind-s1",
        ),
        pytest.param(
            [
                ({"entity": ["wind"], "scenario": "high"}, 0.5),
                ({"snapshot": "s1", "scenario": "high"}, 0.7),
            ],
            ("['entity']", "['snapshot']"),
            id="overlap-on-the-dim-both-name",
        ),
        pytest.param(
            [
                ({"entity": ["wind"], "scenario": "high"}, 0.5),
                ({"snapshot": "s1", "scenario": "low"}, 0.7),
            ],
            None,
            id="disjoint-on-the-dim-both-name",
        ),
        pytest.param(
            [
                ({"entity": ["wind"]}, 0.5),
                ({"entity": ["wind"], "snapshot": "s1"}, 0.7),
            ],
            None,
            id="nested-is-no-tie",
        ),
    ],
)
def test_a_tie_between_two_broadcast_rows_is_refused(con, base_uri, edits, nulls):
    """Neither row names a superset of the other's dims, so no rule picks one.

    Before the fix the commit wrote both, and a read returned two values at
    `(wind, s1)`. `nulls` is the two rows' NULL patterns, `None` where the
    commit is accepted.
    """
    root, staged = _staged(con, base_uri, {"entity", "snapshot", "scenario"})
    for dims, value in edits:
        staged.set("p_max_pu", value, **dims)
    if nulls is None:
        staged.commit(NewChild(root))
        return
    with pytest.raises(ValueError, match=r"p_max_pu.*in a row of its own") as info:
        staged.commit(NewChild(root))
    message = str(info.value)
    for pattern in nulls:
        assert f"leaving {pattern} NULL" in message, f"the message names {pattern}"
    assert "entity='wind'" in message, "and the coordinate both rows cover"
    assert "snapshot='s1'" in message, "along every dim either names"


@pytest.mark.parametrize(
    ("edits", "labels", "varies", "broadcast"),
    [
        pytest.param(
            [({"port": ["dc_out"]}, 0.9), ({}, 0.95)],
            {"port": ["dc_out"]},
            {"port"},
            {"snapshot"},
            id="the-named-port-outranks-the-default",
        ),
        pytest.param(
            [({"port": ["dc_out"]}, 0.9), ({}, 0.95)],
            {"port": ["ac_in"]},
            set(),
            {"port", "snapshot"},
            id="the-default-still-covers-the-other-port",
        ),
        pytest.param(
            [({"port": ["dc_out"]}, 0.9), ({}, 0.95)],
            {},
            {"port"},
            {"port", "snapshot"},
            id="the-whole-record-sees-both",
        ),
        pytest.param(
            [({"port": ["dc_out", "ac_in"]}, 0.9), ({}, 0.95)],
            {},
            {"port"},
            {"snapshot"},
            id="a-default-shadowed-at-every-port",
        ),
    ],
)
def test_flags_describe_the_rows_that_win(
    con, base_uri, edits, labels, varies, broadcast
):
    """Before the fix `broadcast` held `port` wherever the port-NULL row matched.

    The owner map aggregated every row of the layer at a key, the shadowed
    default among them, so `flags(port=["dc_out"])` said some row leaves the
    port NULL where the read returns only the row naming it.
    """
    root, staged = _staged(con, base_uri, {"entity", "port"})
    for dims, value in edits:
        staged.set("efficiency", value, **dims)
    child = staged.commit(NewChild(root))
    flags = child.record.flags(**labels)["efficiency"]
    assert flags.varies == varies, f"the winning rows name {sorted(varies)}"
    assert flags.broadcast == broadcast, (
        f"the winning rows leave {sorted(broadcast)} NULL"
    )


@pytest.mark.parametrize(
    ("rows", "coordinate"),
    [
        pytest.param(
            {"entity": ["wind", "wind"], "value": [0.5, 0.7]},
            "entity='wind'",
            id="one-entity-twice",
        ),
        pytest.param(
            {"entity": ["wind", "wind"], "snapshot": ["s1", "s1"], "value": [0.5, 0.7]},
            "entity='wind', snapshot='s1'",
            id="one-entity-and-snapshot-twice",
        ),
        pytest.param(
            {"value": [0.5, 0.7]},
            "every coordinate",
            id="two-defaults",
        ),
        pytest.param(
            {"entity": ["wind", "wind"], "breakpoint": [0.0, 0.0], "value": [0.5, 0.7]},
            "entity='wind'",
            id="a-curve-repeating-a-breakpoint",
        ),
    ],
)
def test_a_duplicate_row_is_refused(con, base_uri, rows, coordinate):
    """Two rows with one NULL pattern at one coordinate: nothing orders them.

    `set` replaces what it staged at a coordinate, so a duplicate comes from a
    source handed to `write_record`. Before the fix it wrote both, and a read
    returned both values.
    """
    schema = _schema({"entity", "snapshot", "scenario"})
    write_schema(schema, base_uri)
    revision = Revision.create(con)
    tables = {"p_max_pu": pd.DataFrame(rows)}
    with pytest.raises(ValueError, match=r"p_max_pu.*Keep one row") as info:
        write_record(revision.id, from_sources(schema, tables), con)
    assert coordinate in str(info.value), "the message names the coordinate"


@pytest.mark.parametrize(
    ("rows", "coordinate"),
    [
        pytest.param(
            {
                "entity": ["wind", "wind", "wind"],
                "breakpoint": [None, 0.0, 50.0],
                "value": [0.5, 0.2, 0.4],
            },
            "entity='wind'",
            id="a-scalar-beside-a-curve",
        ),
        pytest.param(
            {"breakpoint": [None, 0.0], "value": [0.5, 0.2]},
            "every coordinate",
            id="a-scalar-default-beside-a-curve-default",
        ),
        pytest.param(
            {
                "entity": ["wind", "wind", "gas"],
                "breakpoint": [0.0, 50.0, None],
                "value": [0.2, 0.4, 0.5],
            },
            None,
            id="a-curve-at-one-entity-a-scalar-at-another",
        ),
        pytest.param(
            {
                "entity": ["wind", "wind", None],
                "breakpoint": [0.0, 50.0, None],
                "value": [0.2, 0.4, 0.5],
            },
            None,
            id="a-curve-beside-a-scalar-default-it-outranks",
        ),
    ],
)
def test_a_scalar_beside_a_curve_is_refused(con, base_uri, rows, coordinate):
    """A scalar and a curve with one NULL pattern at one coordinate: neither wins.

    Before the fix the duplicate check grouped by `breakpoint`, so the scalar's
    NULL breakpoint and the curve's points fell in different groups and both
    were written. `coordinate` is `None` where the write is accepted.
    """
    schema = _schema({"entity", "snapshot", "scenario"})
    write_schema(schema, base_uri)
    revision = Revision.create(con)
    tables = {"p_max_pu": pd.DataFrame(rows)}
    if coordinate is None:
        write_record(revision.id, from_sources(schema, tables), con)
        return
    with pytest.raises(ValueError, match=r"p_max_pu.*scalar or the curve") as info:
        write_record(revision.id, from_sources(schema, tables), con)
    assert coordinate in str(info.value), "the message names the coordinate"
