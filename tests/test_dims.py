# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Layer keys beyond `scenario`, resolved through a real record.

What `partial` *means* as a declaration is pinned in `test_schema.py`; here it
is written into a layer and the fold is asked whether it keyed by it.

Notes
-----
- [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
"""

from pathlib import Path

import pandas as pd

from tests.fixtures import (
    empty_child,
    empty_root,
    relation,
    schema,
    write_attribute,
    write_component,
    write_periods,
    write_schema,
)


def test_deleting_a_dim_coordinate_drops_the_attribute_rows_keyed_on_it(store):
    """A dim tombstone removes the attribute rows over that coordinate, not others.

    The fold anti-joins every membership's tombstones into the owner map, dims
    included: deleting `period=2030` drops a `p_max_pu` row keyed on 2030 while
    the row over 2020 survives. Not a cascade - the attribute honours the dim's
    own tombstone because that coordinate is part of its key.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    revision = empty_root(store)
    write_schema(schema(partial={"period"}), store)
    write_component(
        store.layer_dir(revision.id), "Generator", [{"entity": "Manchester Wind"}]
    )
    write_periods(store.layer_dir(revision.id), [{"period": 2020}, {"period": 2030}])
    write_attribute(
        store.layer_dir(revision.id),
        "p_max_pu",
        [
            {"entity": "Manchester Wind", "period": 2020, "value": 0.2},
            {"entity": "Manchester Wind", "period": 2030, "value": 0.3},
        ],
    )
    revision.materialise()

    child = empty_child(revision)
    write_periods(store.layer_dir(child.id), [{"period": 2030, "deleted": True}])

    rel = relation(child, "p_max_pu").df()
    wind = rel[rel["entity"] == "Manchester Wind"]
    assert set(wind["period"]) == {2020}, "the deleted coordinate's row is gone"

    keys = child.record._fold.owner_map.df()
    keyed = keys[
        (keys["entity"] == "Manchester Wind") & (keys["attribute"] == "p_max_pu")
    ]
    assert set(keyed["period"]) == {2020}, "and the map no longer owns its key"


def test_the_fold_unions_maps_by_name(store):
    """A child's own map is unioned with the parent's by name, not by position.

    One schema serves the whole tree, so the dim *order* is fixed - but
    the parent's map is read from a persisted parquet file whose column order
    is its own, so the union must still be `UNION ALL BY NAME`. Positional
    would swap `scenario` and `period` here, which the values below would show.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    revision = empty_root(store)
    write_schema(schema(partial={"scenario", "period"}), store)
    write_component(
        store.layer_dir(revision.id), "Generator", [{"entity": "Manchester Wind"}]
    )
    write_attribute(
        store.layer_dir(revision.id),
        "p_max_pu",
        [
            {
                "entity": "Manchester Wind",
                "scenario": "base",
                "period": 2020,
                "value": 0.1,
            }
        ],
    )
    revision.materialise()

    child = empty_child(revision)
    write_attribute(
        store.layer_dir(child.id),
        "p_max_pu",
        [
            {
                "entity": "Manchester Wind",
                "scenario": "high",
                "period": 2030,
                "value": 0.5,
            }
        ],
    )

    df = child.record._fold.owner_map.df()
    wind = df[(df["entity"] == "Manchester Wind") & (df["layer_uuid"] == child.id)]
    assert wind["scenario"].tolist() == ["high"]
    assert wind["period"].tolist() == [2030]


def test_a_dim_names_its_own_file(store):
    """`dims/<dim>.parquet`, whatever the dim is called.

    A dim named `bus` is the case that matters: pluralising by concatenation
    would look for `buss.parquet` and find nothing, so the axis would read as
    absent rather than wrong.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    revision = empty_root(store)
    write_schema(schema(dims={"bus": "str"}, partial=set()), store)
    target = Path(store.layer_dir(revision.id), "dims")
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"bus": "north"}, {"bus": "south"}]).to_parquet(
        target / "bus.parquet", index=False
    )

    axis = revision.record.dims["bus"].df()
    assert sorted(axis["bus"].tolist()) == ["north", "south"]
