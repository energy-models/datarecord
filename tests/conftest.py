# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

import os

import pytest

from datarecord import Workspace


def pytest_sessionstart(session: pytest.Session) -> None:
    """Cache PyPSA's example networks before xdist starts its workers.

    PyPSA downloads an example straight to its cache path, so a worker that
    finds the file while another is still writing it reads a truncated netCDF.
    """
    if "PYTEST_XDIST_WORKER" in os.environ:
        return
    import pypsa

    from tests.fixtures import example

    for load in (
        pypsa.examples.ac_dc_meshed,
        pypsa.examples.stochastic_network,
        pypsa.examples.carbon_management,
    ):
        example(load)


@pytest.fixture
def workspace():
    ws = Workspace()
    yield ws
    ws.close()


@pytest.fixture
def store(workspace, tmp_path):
    return workspace.store(str(tmp_path))


@pytest.fixture(scope="session")
def ac_dc():
    """PyPSA's `ac_dc_meshed`, with its carriers' own cost columns dropped.

    Its loads keep their buses' names, a bus being a label of `bus` rather
    than an entity. Its carriers' own `efficiency`, `marginal_cost` and
    `capital_cost` columns have nowhere to go: a custom carrier column is an
    attribute over `carrier`, and PyPSA declares all three names over
    another dim. `test_tools.py` pins the rejection itself.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    import pypsa

    from tests.fixtures import example

    n = example(pypsa.examples.ac_dc_meshed)
    n.c.Carrier.static = n.c.Carrier.static.drop(
        columns=["efficiency", "marginal_cost", "capital_cost"]
    )
    return n
