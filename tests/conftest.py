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
    """PyPSA's `ac_dc_meshed`, with its carriers' own cost columns dropped.

    Its loads keep their buses' names, a bus being a label of `bus` rather
    than an entity. Its carriers' own `efficiency`, `marginal_cost` and
    `capital_cost` columns have nowhere to go: a custom carrier column is an
    attribute over `carrier`, and PyPSA declares all three names over
    another dim. `test_tools.py` pins the rejection itself.

    Notes
    -----
    - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    import pypsa

    from tests.fixtures import example

    n = example(pypsa.examples.ac_dc_meshed)
    n.c.Carrier.static = n.c.Carrier.static.drop(
        columns=["efficiency", "marginal_cost", "capital_cost"]
    )
    return n
