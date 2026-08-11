"""The scenario axis and per-scenario overlay (design doc §5.5, §12)."""

from pathlib import Path

import pandas as pd
import pytest

from datarecord import DataRecord
from datarecord.duck import layer_dir, node_dir
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import export_network, tombstone, write_input, write_scenarios
from tests.test_roundtrip import assert_networks_equal


@pytest.fixture(scope="session")
def stochastic():
    import pypsa

    return pypsa.examples.stochastic_network()


@pytest.fixture
def parent(con, base_uri, stochastic):
    record = DataRecord.create(con)
    export_network(stochastic, record, con)
    record.materialise()
    return record


def test_scenario_roundtrip(con, parent, stochastic):
    """A stochastic store round-trips through our reader."""
    n = PyPSA.build(parent)
    assert list(n.scenarios) == list(stochastic.scenarios)
    assert_networks_equal(n, stochastic)


def test_map_is_scenario_expanded(con, parent, stochastic):
    """A NULL-scenario row becomes one map entry per scenario (§5.5)."""
    df = parent.node_cache.inputs.df()
    assert set(df["scenario"]) == set(stochastic.scenarios)

    solar = df[(df["name"] == "solar") & (df["attribute"].astype(str) == "p_max_pu")]
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
                "component_type": "Generator",
                "name": "solar",
                "scenario": scenario,
                "value": 0.77,
            }
        ],
    )

    df = child.node_cache.inputs.df()
    solar = df[(df["name"] == "solar") & (df["attribute"].astype(str) == "p_max_pu")]
    owners = dict(zip(solar["scenario"], solar["layer_uuid"], strict=True))
    assert owners[scenario] == child.id
    for other in stochastic.scenarios[1:]:
        assert owners[other] == parent.id

    # The resolved relation carries the child's value for that scenario only.
    rel = child.relation("p_max_pu").df()
    solar_rows = rel[rel["name"] == "solar"]
    overridden = solar_rows[solar_rows["scenario"] == scenario]
    assert set(overridden["value"]) == {0.77}
    assert set(solar_rows[solar_rows["scenario"] != scenario]["value"]) != {0.77}


def test_per_scenario_tombstone(con, parent, stochastic):
    """A tombstone in one scenario leaves the component in the others (§8.3)."""
    scenario = stochastic.scenarios[0]
    child = parent.child()
    tombstone(layer_dir(child.id), "Generator", ["solar"], scenario=scenario)

    df = child.node_cache.inputs.df()
    # `Carrier` "solar" also exists in this fixture and is untouched by the
    # tombstone, so scope by component_type too, not just name.
    solar = df[
        (df["name"] == "solar") & (df["component_type"].astype(str) == "Generator")
    ]
    assert scenario not in set(solar["scenario"])
    assert set(solar["scenario"]) == set(stochastic.scenarios[1:])


def test_child_adds_new_scenario(con, parent, stochastic):
    """A child may add a scenario the root never declared (§8, §8.2)."""
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
                "component_type": "Generator",
                "name": "solar",
                "scenario": "extra",
                "value": 0.33,
            }
        ],
    )

    n = PyPSA.build(child)
    assert set(n.scenarios) == set(stochastic.scenarios) | {"extra"}

    rel = child.relation("p_max_pu").df()
    extra_solar = rel[(rel["name"] == "solar") & (rel["scenario"] == "extra")]
    assert set(extra_solar["value"]) == {0.33}


def test_scenario_order_survives_a_chain_of_closed_layers(con, parent, stochastic):
    """Axis row order stays root-first, then each layer's own append order (§8.2, §9.1).

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
    """Closing writes the resolved scenario axis to the node cache, not the layer (§13)."""
    middle = parent.child()
    middle.materialise()

    resolved = Path(node_dir(middle.id), "dims", "scenarios.parquet")
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
        [{"component_type": "Generator", "name": "solar", "value": 0.88}],
    )

    df = grandchild.node_cache.inputs.df()
    solar = df[(df["name"] == "solar") & (df["attribute"].astype(str) == "p_max_pu")]
    assert set(solar["scenario"]) == set(stochastic.scenarios)
    assert set(solar["layer_uuid"]) == {grandchild.id}


def test_scenario_null_row_broadcasts(con, parent, stochastic):
    """A child's NULL-scenario row replaces the component in every scenario."""
    child = parent.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"component_type": "Generator", "name": "solar", "value": 0.55}],
    )

    df = child.node_cache.inputs.df()
    solar = df[(df["name"] == "solar") & (df["attribute"].astype(str) == "p_max_pu")]
    assert set(solar["layer_uuid"]) == {child.id}

    rel = child.relation("p_max_pu").df()
    assert set(rel[rel["name"] == "solar"]["value"]) == {0.55}
