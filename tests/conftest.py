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


@pytest.fixture(scope="module")
def module_store(tmp_path_factory):
    """A store shared by a module, for fixtures its tests only branch from."""
    ws = Workspace()
    yield ws.store(str(tmp_path_factory.mktemp("module")))
    ws.close()


@pytest.fixture(scope="session")
def ac_dc_root(tmp_path_factory, ac_dc):
    """`ac_dc` exported and materialised once per session, to branch from.

    Its store is shared, so a test only ever reads it or commits a child of it.
    """
    from tests.fixtures import export_network

    ws = Workspace()
    revision = export_network(ac_dc, ws.store(str(tmp_path_factory.mktemp("ac_dc"))))
    revision.materialise()
    yield revision
    ws.close()


@pytest.fixture(scope="session")
def ac_dc():
    """PyPSA's `ac_dc_meshed`.

    Its loads keep their buses' names, a bus being a label of `bus` rather
    than an entity. Its own `Bus.country` and carriers' `efficiency`,
    `marginal_cost` and `capital_cost` are properties of `bus` and `carrier`.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
    - [custom columns](https://energy-models.github.io/datarecord/design/tools/pypsa/#custom-columns)
    """
    import pypsa

    from tests.fixtures import example

    return example(pypsa.examples.ac_dc_meshed)
