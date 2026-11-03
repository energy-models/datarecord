# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""DEP 34's `register` on a fresh `WorkingRecord` over a PyPSA record of about 168 views."""

from time import perf_counter

import pytest

from datarecord import WorkingRecord


@pytest.mark.benchmark
def test_register_takes_at_most_ten_seconds(ac_dc_root):
    start = perf_counter()
    record = WorkingRecord(ac_dc_root.record)
    opened = perf_counter()
    record.register("bench")
    registered = perf_counter()
    record.workspace.unregister("bench")

    print(f"\nopen {opened - start:.2f}s, register {registered - opened:.2f}s")
    assert registered - opened <= 10, (
        f"DEP 34 registers a fresh WorkingRecord in at most 10 s,"
        f" took {registered - opened:.2f}s"
    )
