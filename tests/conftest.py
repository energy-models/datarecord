# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

import pytest

from datarecord import Workspace


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
