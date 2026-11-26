# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Settling long rows by the broadcast rule, case by case from `settle_cases.json`.

Notes
-----
- [settling rows](https://energy-models.github.io/datarecord/reference/record/#settle)
"""

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from datarecord import Flags, settle

CASES: list[dict[str, Any]] = json.loads(
    (Path(__file__).parent / "settle_cases.json").read_text()
)


def _table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    columns: list[dict[str, str]],
    rows: list[list[Any]],
) -> duckdb.DuckDBPyRelation:
    typed = ", ".join(f'"{c["name"]}" {c["type"]}' for c in columns)
    con.execute(f"CREATE TABLE {name} ({typed})")
    if rows:
        con.executemany(
            f"INSERT INTO {name} VALUES ({', '.join('?' * len(columns))})", rows
        )
    return con.table(name)


def _flags(case: dict[str, Any], every: bool) -> Flags:
    names = [c["name"] for c in case["columns"]]
    dims = [d for d in case["labels"] if d in names]
    if every:
        return Flags(frozenset(dims), frozenset(dims))
    values = [{d: row[names.index(d)] for d in dims} for row in case["rows"]]
    return Flags(
        frozenset(d for d in dims if any(v[d] is not None for v in values)),
        frozenset(d for d in dims if any(v[d] is None for v in values)),
    )


@pytest.mark.parametrize("every", [False, True], ids=["exact-flags", "every-dim-mixed"])
@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_settle_case(case: dict[str, Any], every: bool) -> None:
    """`settle` returns a case's expected rows, or fails, whatever flags overstate.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """
    con = duckdb.connect()
    column_of = {c["name"]: c for c in case["columns"]}
    rel = _table(con, "rows", case["columns"], case["rows"])
    labels = {
        d: _table(con, f"labels_{d}", [column_of[d]], [[label] for label in held])
        for d, held in case["labels"].items()
        if d in column_of
    }
    settled = settle(rel, labels, _flags(case, every), filter=case["filter"])
    if "error" in case:
        with pytest.raises(duckdb.Error):
            settled.fetchall()
        return
    expected = _table(con, "expected", case["columns"], case["expected"])
    assert sorted(settled.fetchall(), key=repr) == sorted(
        expected.fetchall(), key=repr
    ), case["doc"]
