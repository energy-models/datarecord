# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pytest

from datarecord import duck


@pytest.fixture
def base_uri(tmp_path, monkeypatch):
    """Point layer_dir at a temporary directory for the whole test."""
    monkeypatch.setattr(duck, "DEFAULT_BASE_URI", str(tmp_path))
    return str(tmp_path)


@pytest.fixture
def con(base_uri):
    connection = duck.connect(base_uri=base_uri)
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def ac_dc():
    """PyPSA's `ac_dc_meshed`, with its loads renamed off their buses.

    The example names each `Load` after the `Bus` it sits on, which a record
    cannot represent: names are unique across component types,
    and `PyPSA.to_datarecord` rejects such a network rather than renaming it. Renaming here is the test suite standing in for the caller that has
    to reconcile the two vocabularies; `test_tools.py` pins the rejection
    itself.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    import pypsa

    from tests.fixtures import rename_components

    n = pypsa.examples.ac_dc_meshed()
    rename_components(n, "Load", " Load")
    return n


@pytest.fixture(scope="session")
def multiperiod(ac_dc):
    """`ac_dc` as a perfect-foresight network shaped like a pypsa-eur export.

    Three investment periods with weightings, assets with a `build_year` and
    `lifetime`, a storage unit and a store, and the custom columns pypsa-eur writes: `technology_group`,
    `asset_status`, `resource_class` and WKT `geometry` on the generators,
    `geometry` and `busmap_*` on the buses, and bus regions (plus a country
    shape, which the record drops) in `n.shapes`.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    import shapely

    n = ac_dc.copy()
    n.set_investment_periods([2030, 2040, 2050])
    n.investment_period_weightings = pd.DataFrame(
        {"objective": [1.0, 0.6, 0.35], "years": [10.0, 10.0, 10.0]},
        index=n.investment_periods,
    )
    n.add(
        "StorageUnit",
        "Manchester Battery",
        bus="Manchester",
        carrier="battery",
        p_nom=100.0,
        max_hours=4.0,
        build_year=2030,
        lifetime=15.0,
    )
    n.add("Store", "Norway Store", bus="Norway", carrier="battery", e_nom=500.0)
    gens = n.c["Generator"].static
    gens["build_year"] = [2030, 2030, 2040, 2030, 2040, 2030]
    gens["lifetime"] = [30.0, 20.0, 30.0, 20.0, 30.0, 40.0]
    gens["technology_group"] = gens["carrier"].map({"wind": "onwind", "gas": "OCGT"})
    gens["resource_class"] = gens["carrier"].map({"wind": 1.0})
    gens["asset_status"] = ["existing", "candidate", "estimate"] * 2
    gens["geometry"] = [shapely.Point(i, i).wkt for i in range(len(gens))]
    buses = n.c["Bus"].static
    buses["geometry"] = [shapely.Point(x, y).wkt for x, y in zip(buses.x, buses.y)]
    for level in ("all", "5", "10"):
        buses[f"busmap_{level}"] = buses.index.str.split().str[0] + f"-{level}"
    ac = buses.index[buses.carrier == "AC"]
    n.add(
        "Shape",
        [str(i) for i in range(len(ac) + 2)],
        geometry=[
            *(shapely.Point(x, y).buffer(1) for x, y in zip(buses.x[ac], buses.y[ac])),
            shapely.Point(10, 60).buffer(2),
            shapely.Point(0, 50).buffer(5),
        ],
        idx=[*ac, "Norway", "UK"],
        component=[*["Bus"] * (len(ac) + 1), ""],
        type=[*["onshore"] * len(ac), "offshore", "country"],
    )
    return n
