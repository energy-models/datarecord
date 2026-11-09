# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The scenario axis and per-scenario overlay.

Notes
-----
- [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

import pytest

from datarecord import Workspace
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    example,
    export_network,
    rename_components,
)
from tests.test_roundtrip import assert_networks_equal


@pytest.fixture(scope="session")
def stochastic():
    """PyPSA's `stochastic_network`, with its generators renamed off their carriers.

    The example names each `Generator` after its `Carrier`, which collides in a
    record. The generators move rather than the carriers, since the
    `carrier` attribute *values* reference the carrier names.

    Notes
    -----
    - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
    """
    import pypsa

    n = example(pypsa.examples.stochastic_network)
    rename_components(n, "Generator", " Gen")
    return n


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    """One store for the module: every test branches its own child off `parent`."""
    workspace = Workspace()
    yield workspace.store(str(tmp_path_factory.mktemp("stochastic")))
    workspace.close()


@pytest.fixture(scope="module")
def parent(store, stochastic):
    revision = export_network(stochastic, store)
    revision.materialise()
    return revision


def test_scenario_roundtrip(store, parent, stochastic):
    """A stochastic record round-trips through our reader."""
    n = PyPSA.build(parent.record)
    assert list(n.scenarios) == list(stochastic.scenarios)
    assert_networks_equal(n, stochastic)
