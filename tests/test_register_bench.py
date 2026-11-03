# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The baseline for DEP 34's `register` on a PyPSA record of about 168 views.

`register` does not exist yet, so this times what it will do first: build
every relation of a fresh `WorkingRecord` and render each to view SQL.
"""

from time import perf_counter

import pytest
from duckdb import _relation

from datarecord import WorkingRecord


@pytest.mark.benchmark
def test_register_baseline(ac_dc_root):
    start = perf_counter()
    record = WorkingRecord(ac_dc_root.record)
    opened = perf_counter()
    kinds = (record.dims, record.properties, record.attributes)
    relations = [kind[name] for kind in kinds for name in kind]
    built = perf_counter()
    for rel in relations:
        _relation._render(rel, literals=True, wrap_text=True)[0]
    rendered = perf_counter()

    print(
        f"\n{len(relations)} views: open {opened - start:.2f}s,"
        f" build {built - opened:.2f}s, render {rendered - built:.2f}s"
    )
    assert rendered - opened <= 10, (
        f"DEP 34 registers {len(relations)} views in at most 10 s,"
        f" took {rendered - opened:.2f}s"
    )
