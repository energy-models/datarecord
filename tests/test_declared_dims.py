# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record whose schema is mathspec's declarations, with one dim per kind.

The declarations are a slice of mathspec's `examples/pypsa.yaml`: `generator`,
`bus` and `carrier` are dims of their own, and nothing is called `entity`. Every
edit and read below goes through the same core a PyPSA-shaped record does, so a
pass here says the core names no dim.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
"""

import pandas as pd
import pytest

from datarecord import Revision
from datarecord.layered.resolve import write_schema
from datarecord.mutable import NewChild, WorkingRecord
from datarecord.schema import Schema

DECLARATIONS = {
    "dimensions": {
        "scenario": {"description": "the futures dispatch is chosen in"},
        "snapshot": {"description": "dispatch periods", "dtype": "datetime"},
        "bus": {"description": "network nodes"},
        "carrier": {"description": "energy carriers"},
        "generator": {"description": "generating units, each on one bus"},
    },
    "relations": {
        "Generator_bus": {"key": "generator", "values": "bus"},
        "Generator_carrier": {"key": "generator", "values": "carrier"},
    },
    "parameters": {
        "Generator_p_nom": {"dims": ["scenario", "generator"]},
        "Generator_p_max_pu": {"dims": ["scenario", "snapshot", "generator"]},
        "Generator_marginal_cost": {"dims": ["generator"]},
    },
}
STORAGE = {"partial": {"generator", "scenario"}}
SNAPSHOTS = pd.date_range("2030-01-01", periods=2, freq="h")


@pytest.fixture
def root(con, base_uri):
    """Two generators on one bus, their relations and a constant each."""
    write_schema(Schema.from_declarations(DECLARATIONS, storage=STORAGE), base_uri)
    revision = Revision.create(con)
    staged = WorkingRecord(revision.record, con)
    staged.add(
        "generator",
        pd.DataFrame(
            {
                "generator": ["wind", "gas"],
                "bus": ["north", "north"],
                "carrier": ["wind", "gas"],
                "Generator_marginal_cost": [0.0, 50.0],
            }
        ),
    )
    child = staged.commit(NewChild(revision))
    child.materialise()
    return child


def _long(record, attribute: str) -> pd.DataFrame:
    return record.attributes[attribute].collect().to_native().to_pandas()


def test_the_schema_names_no_entity(root):
    """Every dim the record uses is one the declarations name."""
    schema = root.record.schema
    assert "entity" not in schema.dims, "no dim is added behind the declarations"
    assert schema.partial_dims == ("scenario", "generator"), (
        "the fold key is exactly `partial`, in declaration order"
    )


def test_a_relation_row_is_staged_by_add(root):
    """`add("generator", ...)` writes the generator's rows of both relations."""
    groups = root.record.groups
    bus = groups["Generator_bus"].collect().to_native().to_pandas()
    assert dict(zip(bus["generator"], bus["bus"], strict=True)) == {
        "wind": "north",
        "gas": "north",
    }, "each generator's bus is a row of the relation"


def test_an_unnamed_generator_broadcasts_to_every_generator(root, con):
    """A scalar with no `generator=` is one NULL row: every generator, later ones too.

    The row is stored once, and a generator added by a later layer picks it up
    without being named.
    """
    staged = WorkingRecord(root.record, con)
    staged.set("Generator_p_nom", 100.0)
    first = staged.commit(NewChild(root))
    staged = WorkingRecord(first.record, con)
    staged.add(
        "generator",
        pd.DataFrame({"generator": ["solar"], "bus": ["north"], "carrier": ["sun"]}),
    )
    second = staged.commit(NewChild(first))

    rows = _long(second.record, "Generator_p_nom")
    assert dict(zip(rows["generator"], rows["value"], strict=True)) == {
        "wind": 100.0,
        "gas": 100.0,
        "solar": 100.0,
    }, "the NULL row reaches a generator added after it was written"


def test_a_scoped_value_overrides_one_generator_in_one_scenario(root, con):
    """A named generator and scenario outrank the broadcast row there, and only there."""
    staged = WorkingRecord(root.record, con)
    staged.set("Generator_p_nom", 100.0, scenario="low")
    staged.set("Generator_p_nom", 150.0, scenario="high")
    base = staged.commit(NewChild(root))
    staged = WorkingRecord(base.record, con)
    staged.set("Generator_p_nom", 200.0, generator=["wind"], scenario="high")
    child = staged.commit(NewChild(base))

    rows = _long(child.record, "Generator_p_nom")
    got = {
        (g, s): v
        for g, s, v in zip(
            rows["generator"], rows["scenario"], rows["value"], strict=True
        )
    }
    assert got == {
        ("wind", "low"): 100.0,
        ("gas", "low"): 100.0,
        ("wind", "high"): 200.0,
        ("gas", "high"): 150.0,
    }, "only (wind, high) takes the child's value"


def test_removing_a_generator_takes_its_values_and_relation_rows(root, con):
    """`remove("generator", ...)` is the same operation `remove("entity", ...)` was."""
    staged = WorkingRecord(root.record, con)
    staged.remove("generator", ["gas"])
    child = staged.commit(NewChild(root))

    record = child.record
    axis = record.dims["generator"].collect().to_native().to_pandas()
    assert list(axis["generator"]) == ["wind"], "gas is off the generator axis"
    bus = record.groups["Generator_bus"].collect().to_native().to_pandas()
    assert list(bus["generator"]) == ["wind"], "gas's relation row went with it"


def test_flags_narrow_by_the_declared_dim(root, con):
    """`flags(generator=[...])` narrows as `flags(entity=[...])` did."""
    staged = WorkingRecord(root.record, con)
    staged.set(
        "Generator_p_max_pu",
        pd.Series([0.2, 0.9], index=pd.Index(SNAPSHOTS, name="snapshot")),
        generator=["wind"],
    )
    staged.set("Generator_p_max_pu", 1.0, generator=["gas"])
    child = staged.commit(NewChild(root))

    wind = child.record.flags(generator=["wind"])["Generator_p_max_pu"]
    gas = child.record.flags(generator=["gas"])["Generator_p_max_pu"]
    assert "snapshot" in wind.varies, "wind's rows set the snapshot"
    assert "snapshot" in gas.broadcast, "gas's one row leaves the snapshot NULL"
