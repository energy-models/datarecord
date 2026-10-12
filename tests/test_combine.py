# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A record from a tool's record and an extension that adds names to it.

Notes
-----
- [combining records](https://energy-models.github.io/datarecord/design/writing/#combining-records)
"""

import pandas as pd
import pytest

from datarecord import (
    AttributeSpec,
    Dimension,
    FrameRecord,
    Group,
    Record,
    Schema,
    combine,
    write_record,
)
from datarecord.tools.pypsa import PyPSA


def _extension(base: Schema, **overrides) -> FrameRecord:
    """Constraint data a consumer keeps beside a PyPSA record.

    A new axis with an axis attribute, a long attribute over it and a shared
    axis, a column on a shared axis and on the entities, a pair of entities and
    a value over no axis at all.
    """
    shared = {d: base.dimensions[d] for d in ("entity", "period", "carrier")}
    schema = Schema(
        dimensions=shared | {"limit": Dimension(dtype="String")},
        attributes={
            "limit_sense": AttributeSpec(dtype="String", dims={"limit"}),
            "limit_constant": AttributeSpec(dtype="Float64", dims={"limit", "period"}),
            "carrier_floor": AttributeSpec(dtype="Float64", dims={"carrier"}),
            "partner_ratio": AttributeSpec(dtype="Float64", dims={"entity"}),
            "share": AttributeSpec(dtype="Float64"),
        },
        groups={"pair": Group(over={"entity": "entity", "partner": "entity"})},
    )
    frames = {
        "dim_frames": {
            "limit": pd.DataFrame({"limit": ["co2"], "limit_sense": ["<="]}),
            "carrier": pd.DataFrame({"carrier": ["gas"], "carrier_floor": [5.0]}),
        },
        "entity_type_frames": {
            "Link": pd.DataFrame({"entity": ["DC link"], "partner_ratio": [0.9]})
        },
        "group_frames": {
            "pair": pd.DataFrame(
                {"entity": ["DC link"], "partner": ["Norwich Converter"]}
            )
        },
        "attribute_frames": {
            "limit_constant": pd.DataFrame(
                {
                    "limit": ["co2", "co2"],
                    "period": [2030, 2040],
                    "attribute": "limit_constant",
                    "breakpoint": None,
                    "value": [10.0, 5.0],
                }
            ),
            "share": pd.DataFrame(
                {"attribute": ["share"], "breakpoint": [None], "value": [0.7]}
            ),
        },
    }
    frames.update(overrides)
    return FrameRecord(schema, **frames)


def test_combined_record_writes_and_reads_back(multiperiod, con, tmp_path):
    base = PyPSA.to_datarecord(multiperiod)
    combined = combine(base, _extension(base.schema))
    write_record(None, combined, con, uri=str(tmp_path / "record"))

    record = Record.at(str(tmp_path / "record"), con)
    assert {"limit_sense", "limit_constant", "partner_ratio", "share"} <= set(
        record.schema.attributes
    )
    assert "pair" in record.schema.groups
    carriers = record.dims["carrier"].collect().to_pandas().set_index("carrier")
    assert carriers.loc["gas", "carrier_floor"] == 5.0
    assert (
        carriers.loc["gas", "co2_emissions"]
        == multiperiod.carriers.co2_emissions["gas"]
    )
    links = record.entity_types["Link"].collect().to_pandas().set_index("entity")
    assert links.loc["DC link", "partner_ratio"] == 0.9
    assert links["partner_ratio"].isna().sum() == len(links) - 1
    constants = record.attributes["limit_constant"].collect().to_pandas()
    assert constants.set_index("period").value.to_dict() == {2030: 10.0, 2040: 5.0}


@pytest.mark.parametrize(
    ("change", "match"),
    [
        (
            {"attributes": {"capacity_fixed": AttributeSpec(dtype="Float64", dims={"entity"})}},
            "attributes \\['capacity_fixed'\\]",
        ),
        (
            {
                "groups": {
                    "connection": Group(over={"entity": "entity", "bus": "entity"})
                }
            },
            "groups \\['connection'\\]",
        ),
        (
            {"dimensions": {"period": Dimension(dtype="String")}},
            "shared dims \\['period'\\]",
        ),
    ],
    ids=["attribute", "group", "dim"],
)
def test_a_colliding_name_is_refused(multiperiod, change, match):
    base = PyPSA.to_datarecord(multiperiod)
    declared = base.schema.model_dump()
    extension = Schema(
        dimensions={"entity": declared["dimensions"]["entity"]}
        | change.get("dimensions", {}),
        attributes=change.get("attributes", {}),
        groups=change.get("groups", {}),
    )
    with pytest.raises(ValueError, match=match):
        combine(base, FrameRecord(extension))


def test_a_value_on_a_label_the_base_lacks_is_refused(multiperiod):
    base = PyPSA.to_datarecord(multiperiod)
    unknown = {"carrier": pd.DataFrame({"carrier": ["fusion"], "carrier_floor": [1.0]})}
    combined = combine(base, _extension(base.schema, dim_frames=unknown))
    with pytest.raises(ValueError, match="fusion"):
        combined.dims["carrier"]
