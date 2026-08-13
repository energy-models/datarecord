"""Memory-bounded per-type build on a large network (design doc §12, §12).

Marked slow: `carbon_management` is ~2164 buses / 6830 links / 168 snapshots,
so the record write alone dominates the suite runtime.
"""

import tracemalloc

import pytest

from datarecord import Revision
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import export_network

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def carbon():
    import pypsa

    return pypsa.examples.carbon_management()


def test_to_model_peaks_near_one_component_type(con, base_uri, carbon):
    """Peak memory stays near the largest single type, not the whole network."""
    revision = Revision.create(con)
    export_network(carbon, revision, con)

    tracemalloc.start()
    n = PyPSA.build(revision.record)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(n.c["Link"].static) == len(carbon.c["Link"].static)
    assert len(n.snapshots) == len(carbon.snapshots)

    # The widest single frame is Link's dynamic p_max_pu-shaped block; allow a
    # generous multiple of it, but far below materialising every type at once.
    cell = len(carbon.snapshots) * len(carbon.c["Link"].static) * 8
    assert peak < 20 * cell, f"peak {peak / 1e6:.0f} MB vs one-type {cell / 1e6:.0f} MB"
