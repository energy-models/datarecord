# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A collection of five weather years of 8760 h and 200 generators, written with `extend` and relabelled to one year.

Notes
-----
- [a collection](https://energy-models.github.io/datarecord/pypsa/reference/#a-collection)
"""

from time import perf_counter

import numpy as np
import pandas as pd
import pypsa
import pytest

from datarecord.tools.pypsa import PyPSACollection

YEARS = {"wy2010": 2010, "wy2012": 2012, "wy2013": 2013, "wy2019": 2019, "wy2020": 2020}
GENERATORS = 200


def _network(year: int, seed: int) -> pypsa.Network:
    """One weather year's network: 8760 h of `year`, 29 February dropped, and a `p_max_pu` series per generator."""
    hours = pd.date_range(f"{year}-01-01", periods=8784, freq="h")
    hours = hours[~((hours.month == 2) & (hours.day == 29))][:8760]
    n = pypsa.Network()
    n.set_snapshots(hours)
    n.add("Bus", "b")
    n.add("Load", "d", bus="b", p_set=1.0)
    names = [f"g{i}" for i in range(GENERATORS)]
    rng = np.random.default_rng(seed)
    p_max_pu = pd.DataFrame(rng.random((len(hours), GENERATORS)), hours, names)
    n.add("Generator", names, bus="b", p_nom=1.0, p_max_pu=p_max_pu)
    return n


def _written(store, members: dict, **kwargs) -> float:
    """Seconds to write `members` as a collection over `weather_year` and commit it."""
    collection = pypsa.NetworkCollection(pd.Series(members).rename_axis("weather_year"))
    start = perf_counter()
    layer = PyPSACollection.to_datarecord(collection, store.workspace.con, **kwargs)
    store.commit(layer, parent=None)
    return perf_counter() - start


@pytest.mark.benchmark
def test_extending_snapshot_is_at_most_20_percent_slower_than_relabelling(
    workspace, tmp_path
):
    members = {
        label: _network(y, seed) for seed, (label, y) in enumerate(YEARS.items())
    }
    relabelled = {label: _network(2013, seed) for seed, label in enumerate(YEARS)}
    timings: dict[str, list[float]] = {"extend": [], "relabel": []}
    for i in range(3):
        timings["extend"].append(
            _written(
                workspace.store(str(tmp_path / f"e{i}")), members, extend=["snapshot"]
            )
        )
        timings["relabel"].append(
            _written(workspace.store(str(tmp_path / f"r{i}")), relabelled)
        )
    best = {k: min(v) for k, v in timings.items()}
    print(f"\nextend {timings['extend']}, relabel {timings['relabel']}")
    assert best["extend"] <= 1.2 * best["relabel"], (
        f"writing {len(YEARS)} weather years with `extend` takes at most 20 % longer "
        f"than relabelling them to one year: {best['extend']:.2f}s against "
        f"{best['relabel']:.2f}s"
    )
