"""The scenario axis and per-scenario overlay (design doc §5.5, §10)."""

from pathlib import Path

import pandas as pd
import pytest

from datarecord import Revision
from datarecord.duck import layer_dir, resolved_dir
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    export_network,
    relation,
    rename_components,
    tombstone,
    write_input,
    write_scenarios,
)
from tests.test_roundtrip import assert_networks_equal


@pytest.fixture(scope="session")
def stochastic():
    """PyPSA's `stochastic_network`, with its generators renamed off their carriers.

    The example names each `Generator` after its `Carrier`, which collides in a
    record (§4.3). The generators move rather than the carriers, since the
    `carrier` attribute *values* reference the carrier names.
    """
    import pypsa

    n = pypsa.examples.stochastic_network()
    rename_components(n, "Generator", " Gen")
    return n


@pytest.fixture
def parent(con, base_uri, stochastic):
    revision = Revision.create(con)
    export_network(stochastic, revision, con)
    revision.materialise()
    return revision


def test_scenario_roundtrip(con, parent, stochastic):
    """A stochastic record round-trips through our reader."""
    n = PyPSA.build(parent.record)
    assert list(n.scenarios) == list(stochastic.scenarios)
    assert_networks_equal(n, stochastic)


def test_map_is_scenario_expanded(con, parent, stochastic):
    """A NULL-scenario row becomes one map entry per scenario (§5.5)."""
    df = parent.node_cache.inputs.df()
    assert set(df["scenario"]) == set(stochastic.scenarios)

    solar = df[
        (df["name"] == "solar Gen") & (df["attribute"].astype(str) == "p_max_pu")
    ]
    assert set(solar["scenario"]) == set(stochastic.scenarios)


def test_partial_scenario_override(con, parent, stochastic):
    """A child may replace one scenario and leave the others to the parent."""
    scenario = stochastic.scenarios[0]
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "name": "solar Gen",
                "scenario": scenario,
                "value": 0.77,
            }
        ],
    )

    df = child.node_cache.inputs.df()
    solar = df[
        (df["name"] == "solar Gen") & (df["attribute"].astype(str) == "p_max_pu")
    ]
    owners = dict(zip(solar["scenario"], solar["layer_uuid"], strict=True))
    assert owners[scenario] == child.id
    for other in stochastic.scenarios[1:]:
        assert owners[other] == parent.id

    # The resolved relation carries the child's value for that scenario only.
    rel = relation(child, "p_max_pu").df()
    solar_rows = rel[rel["name"] == "solar Gen"]
    overridden = solar_rows[solar_rows["scenario"] == scenario]
    assert set(overridden["value"]) == {0.77}
    assert set(solar_rows[solar_rows["scenario"] != scenario]["value"]) != {0.77}


def test_per_scenario_tombstone(con, parent, stochastic):
    """A tombstone in one scenario leaves the component in the others (§6.3)."""
    scenario = stochastic.scenarios[0]
    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["solar Gen"], scenario=scenario)

    df = child.node_cache.inputs.df()
    # No type scoping needed: the generator is "solar Gen" and the carrier
    # "solar", names being unique across types (§4.3) - which is exactly what
    # used to require filtering on `component_type` here.
    solar = df[df["name"] == "solar Gen"]
    assert scenario not in set(solar["scenario"])
    assert set(solar["scenario"]) == set(stochastic.scenarios[1:])


def test_child_adds_new_scenario(con, parent, stochastic):
    """A child may add a scenario the root never declared (§6, §6.2)."""
    child = parent.child()
    write_scenarios(
        layer_dir(child.id),
        [
            {"scenario": "high", "weight": 0.27},
            {"scenario": "low", "weight": 0.36},
            {"scenario": "med", "weight": 0.27},
            {"scenario": "extra", "weight": 0.1},
        ],
    )
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [
            {
                "name": "solar Gen",
                "scenario": "extra",
                "value": 0.33,
            }
        ],
    )

    n = PyPSA.build(child.record)
    assert set(n.scenarios) == set(stochastic.scenarios) | {"extra"}

    rel = relation(child, "p_max_pu").df()
    extra_solar = rel[(rel["name"] == "solar Gen") & (rel["scenario"] == "extra")]
    assert set(extra_solar["value"]) == {0.33}


def test_scenario_order_survives_a_chain_of_closed_layers(con, parent, stochastic):
    """Axis row order stays root-first, then each layer's own append order (§6.2, §7.1).

    `fold_axis` tags each layer's row order (`_row`) before the cross-layer
    `UNION ALL` in `union_all_by_name`, not after: a bare `row_number()` on
    the unioned relation would have no defined order to draw from and could
    silently permute which scenario counts as "first introduced". A single
    child is too small a union to expose this: chain several closed layers,
    each adding one new scenario, to stress the fold across many arms.
    """
    rec = parent
    added = []
    for i in range(5):
        rec = rec.child()
        name = f"extra{i}"
        write_scenarios(layer_dir(rec.id), [{"scenario": name, "weight": 0.01}])
        rec.materialise()
        added.append(name)

    scenarios = rec.node_cache.dims.axes["scenario"].df()["scenario"].tolist()
    assert scenarios == list(stochastic.scenarios) + added


def test_resolved_dims_are_node_scoped(con, parent, stochastic):
    """Closing writes the resolved scenario axis to the node cache, not the layer (§6.2)."""
    middle = parent.child()
    middle.materialise()

    resolved = Path(resolved_dir(middle.id), "dims", "scenarios.parquet")
    assert resolved.exists()
    assert set(pd.read_parquet(resolved)["scenario"]) == set(stochastic.scenarios)

    # `middle` wrote nothing itself, so its layer directory has no dims at
    # all - the resolved axis must not have been written there.
    assert not Path(layer_dir(middle.id), "dims", "scenarios.parquet").exists()


def test_scenario_axis_survives_closed_grandchild(con, parent, stochastic):
    """The scenario axis still comes from the root when a closed node sits in between.

    `middle` is closed but writes nothing scenario-related, so its persisted
    map carries no fresh expansion; the live fold at `grandchild` must expand
    this NULL-scenario row itself, which requires the axis from the true
    root, not just from the nearest closed ancestor.
    """
    middle = parent.child()
    middle.materialise()
    grandchild = middle.child()
    write_input(
        layer_dir(grandchild.id),
        "p_max_pu",
        [{"name": "solar Gen", "value": 0.88}],
    )

    df = grandchild.node_cache.inputs.df()
    solar = df[
        (df["name"] == "solar Gen") & (df["attribute"].astype(str) == "p_max_pu")
    ]
    assert set(solar["scenario"]) == set(stochastic.scenarios)
    assert set(solar["layer_uuid"]) == {grandchild.id}


def test_scenario_null_row_broadcasts(con, parent, stochastic):
    """A child's NULL-scenario row replaces the component in every scenario."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"name": "solar Gen", "value": 0.55}],
    )

    df = child.node_cache.inputs.df()
    solar = df[
        (df["name"] == "solar Gen") & (df["attribute"].astype(str) == "p_max_pu")
    ]
    assert set(solar["layer_uuid"]) == {child.id}

    rel = relation(child, "p_max_pu").df()
    assert set(rel[rel["name"] == "solar Gen"]["value"]) == {0.55}
