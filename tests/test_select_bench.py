# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`select` at half of 17520 snapshots, read off an attribute of about 17.5 million rows.

The kept labels are joined to the attribute's rows, and DuckDB has to know
there are thousands of them to build its hash join on them rather than on the
attribute.

Notes
-----
- [selecting](https://energy-models.github.io/datarecord/design/use/selecting/)
"""

from datetime import datetime, timedelta
from time import perf_counter

import pytest

from datarecord import AttributeSpec, WorkingRecord
from tests.fixtures import empty_root, schema

ENTITIES = 1000
SNAPSHOTS = [datetime(2013, 1, 1) + timedelta(hours=h) for h in range(2 * 8760)]


@pytest.fixture(scope="module")
def record(module_store):
    """`p` over every connection, one per entity, and every snapshot."""
    over = {"p": AttributeSpec(dtype="float", dims={"connection", "snapshot"})}
    declared = schema(dims={"snapshot": "datetime"}, attributes={"Link": over})
    root = empty_root(module_store, declared).commit()
    connection = "{'entity': 'e' || i, 'bus': 'b' || (i % 100)} AS connection"
    with WorkingRecord(root.record) as w:
        w.add("bus", [f"b{i}" for i in range(100)])
        w.add("entity", [f"e{i}" for i in range(ENTITIES)], ctype="Link")
        w.add(
            "connection", w.con.sql(f"SELECT {connection} FROM range({ENTITIES}) r(i)")
        )
        w.add("snapshot", SNAPSHOTS)
        w.set(
            "p",
            w.con.sql(
                f"SELECT {connection}, TIMESTAMP '2013-01-01' + to_hours(h) AS snapshot,"
                f" (i * h % 7)::DOUBLE AS value"
                f" FROM range({ENTITIES}) e(i), range({len(SNAPSHOTS)}) s(h)"
            ),
        )
        return module_store.commit(w).record


@pytest.mark.benchmark
def test_selecting_half_the_snapshots_takes_at_most_150_ms(record):
    kept = SNAPSHOTS[::2]
    timings = []
    for _ in range(3):
        start = perf_counter()
        ((rows,),) = (
            record.select(snapshot=kept)
            .attributes["p"]
            .aggregate("count(*)")
            .fetchall()
        )
        timings.append(perf_counter() - start)

    print(f"\nselect and count {rows} rows: {', '.join(f'{t:.3f}s' for t in timings)}")
    assert rows == ENTITIES * len(kept), (
        "the selection keeps every connection at each kept snapshot"
    )
    assert min(timings) <= 0.15, (
        f"selecting {len(kept)} of {len(SNAPSHOTS)} snapshots of {ENTITIES * len(SNAPSHOTS)}"
        f" attribute rows builds its hash join on the kept labels, and so counts them"
        f" in at most 150 ms, took {min(timings):.3f}s"
    )
