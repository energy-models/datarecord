# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Settling long rows by the broadcast rule.

Notes
-----
- [settling rows](https://energy-models.github.io/datarecord/design/use/reading/#settling-rows)
"""

import duckdb
import pandas as pd
import pytest

from datarecord import Flags, settle


@pytest.mark.parametrize(
    "rows",
    [
        [("a", "x", 1.0), ("b", "x", 2.0), ("a", "y", 3.0)],
        [(None, "x", 1.0), (None, "y", 2.0)],
        [(None, None, 1.0)],
        [(None, None, 1.0), ("a", None, 2.0), ("a", "x", 3.0)],
        [("a", None, 1.0), (None, "x", 2.0), ("a", "x", 3.0)],
        [("c", "x", 1.0), (None, "x", 2.0), ("a", "z", 3.0)],
    ],
    ids=[
        "named",
        "entity-broadcast",
        "full-broadcast",
        "nested",
        "crossing",
        "foreign",
    ],
)
@pytest.mark.parametrize("filtered", [True, False], ids=["filter", "no-filter"])
def test_settle_reads_the_same_whatever_flags_overstate(rows, filtered):
    """Flags only let `settle` skip work: over-reporting them changes no value.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
    """
    con = duckdb.connect()
    frame = pd.DataFrame(rows, columns=["entity", "scenario", "value"])
    rel = con.from_df(frame.astype({"entity": object, "scenario": object}))
    labels = {
        "entity": con.from_df(pd.DataFrame({"entity": ["a", "b"]})),
        "scenario": con.from_df(pd.DataFrame({"scenario": ["x", "y"]})),
    }
    dims = ("entity", "scenario")
    exact = Flags(
        frozenset(d for d in dims if frame[d].notna().any()),
        frozenset(d for d in dims if frame[d].isna().any()),
    )
    every = Flags(frozenset(dims), frozenset(dims))
    settled = [
        sorted(settle(rel, labels, f, filter=filtered).fetchall())
        for f in (exact, every)
    ]
    assert settled[0] == settled[1], "the flags of the rows read as every dim mixed"
    if not filtered:
        return
    reached = {
        (e, s)
        for named_e, named_s, _ in rows
        for e in ("a", "b")
        for s in ("x", "y")
        if named_e in (None, e) and named_s in (None, s)
    }
    assert {(e, s) for e, s, _ in settled[0]} == reached, (
        "a NULL reaches each passed label, a label not passed is dropped"
    )
