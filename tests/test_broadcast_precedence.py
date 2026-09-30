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
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import AttributeSpec, Dimension, Schema

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
