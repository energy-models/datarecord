# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A child commit over an axis of a million labels, keeping its parent's three properties.

The child removes every hundredth label and appends one per cent new ones.

Notes
-----
- [property columns a layer omits](https://energy-models.github.io/datarecord/reference/layers/#property-columns-a-layer-omits)
"""

from time import perf_counter

import numpy as np
import pandas as pd
import pytest

from datarecord import Dimension, PropertySpec, Schema
from tests.test_write import _Source

LABELS = 1_000_000
PROPERTIES = {
    "region": PropertySpec(dtype="str"),
    "weight": PropertySpec(dtype="float"),
    "agg": PropertySpec(dtype="int"),
}


def _schema(properties: dict[str, PropertySpec]) -> Schema:
    return Schema(
        dimensions={"entity": Dimension(dtype="str")},
        properties={"entity": properties} if properties else {},
        attributes={},
        partial=frozenset({"entity"}),
    )


def _entities(ids: np.ndarray, schema: Schema) -> _Source:
    """A source whose `entity` axis is `e<id>`, with each property `schema` declares."""
    frame = pd.DataFrame(
        {
            "entity": "e" + pd.Series(ids).astype(str),
            "region": "r" + pd.Series(ids % 50).astype(str),
            "weight": ids * 0.5,
            "agg": ids // 3,
        }
    )
    columns = ["entity", *schema.properties.get("entity", {})]
    return _Source(
        schema,
        dims={"entity": frame[["entity"]]},
        properties={"entity": frame[columns]},
    )


@pytest.mark.benchmark
def test_keeping_properties_takes_at_most_twice_stating_them(store):
    declaring, omitting = _schema(PROPERTIES), _schema({})
    root = store.commit(_entities(np.arange(LABELS), declaring), parent=None)
    ids = np.arange(LABELS)
    child = np.concatenate([ids[ids % 100 != 0], LABELS + np.arange(LABELS // 100)])

    timings: dict[str, list[float]] = {"own": [], "keep": []}
    for _ in range(3):
        for way, schema in (("own", declaring), ("keep", omitting)):
            source = _entities(child, schema)
            start = perf_counter()
            kept = store.commit(source, parent=root)
            timings[way].append(perf_counter() - start)

    own, keep = min(timings["own"]), min(timings["keep"])
    print(f"\nstating {own:.2f}s, keeping {keep:.2f}s, at {LABELS} labels")
    ((rows, agg),) = (
        kept.record.properties["entity"].aggregate("count(*), count(agg)").fetchall()
    )
    assert (rows, agg) == (len(child), len(child) - LABELS // 100), (
        "each kept label has the parent's value, each new one NULL"
    )
    assert keep <= 2 * own, (
        f"keeping three properties over {len(child)} labels joins them by label and"
        f" so takes at most twice the commit stating them, took {keep:.2f}s"
        f" against {own:.2f}s"
    )
