# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Layer keys beyond `scenario`, resolved through a real record.

What `partial` *means* as a declaration is pinned in `test_schema.py`; here it
is written into a layer and the fold is asked whether it keyed by it.

Notes
-----
- [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
"""

from pathlib import Path

import pandas as pd

from tests.fixtures import (
    empty_root,
    schema,
)


def test_a_dim_names_its_own_file(store):
    """`dims/<dim>.parquet`, whatever the dim is called.

    A dim named `bus` is the case that matters: pluralising by concatenation
    would look for `buss.parquet` and find nothing, so the axis would read as
    absent rather than wrong.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
    """
    layer = empty_root(store, schema(dims={"bus": "str"}, partial=set()))
    target = Path(layer.dir, "dims")
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"bus": "north"}, {"bus": "south"}]).to_parquet(
        target / "bus.parquet", index=False
    )

    axis = layer.commit().record.dims["bus"].df()
    assert sorted(axis["bus"].tolist()) == ["north", "south"]
