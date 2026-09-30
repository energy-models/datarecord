# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A schema from a mathspec spec, behind the `mathspec` extra.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
"""

import subprocess
import sys

import narwhals as nw
import pytest

from datarecord.schema import AttributeSpec, Dimension, Relation, Schema
from tests.test_declared_dims import DECLARATIONS, STORAGE

DISPATCH = DECLARATIONS | {
    "variables": {
        "Generator_p": {
            "dims": ["scenario", "snapshot", "generator"],
            "bounds": {"lower": 0},
        }
    },
    "constraints": {
        "capacity": {
            "dims": ["scenario", "snapshot", "generator"],
            "expression": "Generator_p <= Generator_p_nom * Generator_p_max_pu",
        }
    },
    "objective": {
        "sense": "minimize",
        "expression": "sum(Generator_p * Generator_marginal_cost)",
    },
}


def test_the_core_imports_without_mathspec():
    """`mathspec` is an extra, so importing `datarecord` never reaches it."""
    code = (
        "import sys, datarecord, datarecord.sources; print('mathspec' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "False", "the core imports no mathspec module"


def test_a_missing_extra_names_itself(monkeypatch):
    monkeypatch.setitem(sys.modules, "mathspec", None)
    with pytest.raises(ImportError, match=r"pip install 'datarecord\[mathspec\]'"):
        Schema.from_mathspec(DECLARATIONS, storage=STORAGE)


def test_a_full_spec_gives_its_data_declarations(con):
    """A spec that also declares math reads as its dims, relations and parameters.

    The spec a solver solves is the one a record is built from, so the math is
    not refused, and it does not reach the schema.
    """
    full = Schema.from_mathspec(DISPATCH, storage=STORAGE)
    data = Schema.from_mathspec(DECLARATIONS, storage=STORAGE)
    assert full == data, "variables, constraints and the objective add nothing"


def test_the_declarations_become_dims_relations_and_attributes():
    """Each mathspec declaration maps to its `Schema` field, `ordered` included."""
    want = Schema(
        dimensions={
            "scenario": Dimension(
                dtype=nw.String(), description="the futures dispatch is chosen in"
            ),
            "snapshot": Dimension(
                dtype=nw.Datetime(), ordered=True, description="dispatch periods"
            ),
            "bus": Dimension(dtype=nw.String(), description="network nodes"),
            "carrier": Dimension(dtype=nw.String(), description="energy carriers"),
            "generator": Dimension(
                dtype=nw.String(), description="generating units, each on one bus"
            ),
        },
        relations={
            "Generator_bus": Relation(key=["generator"], values="bus"),
            "Generator_carrier": Relation(key=["generator"], values="carrier"),
        },
        attributes={
            "Generator_p_nom": AttributeSpec(
                dtype=nw.Float64(), dims=frozenset({"scenario", "generator"})
            ),
            "Generator_p_max_pu": AttributeSpec(
                dtype=nw.Float64(),
                dims=frozenset({"scenario", "snapshot", "generator"}),
            ),
            "Generator_marginal_cost": AttributeSpec(
                dtype=nw.Float64(), dims=frozenset({"generator"})
            ),
        },
        partial=frozenset({"generator", "scenario"}),
    )
    assert Schema.from_mathspec(DECLARATIONS, storage=STORAGE) == want, (
        "a dim, a relation and a float parameter each, under the storage block's `partial`"
    )


def test_partial_defaults_to_every_dim_not_declared_ordered():
    """A layer patches one generator or one scenario, and restates a series along `snapshot` whole."""
    schema = Schema.from_mathspec(DECLARATIONS)
    assert schema.partial == {"scenario", "bus", "carrier", "generator"}, (
        "`snapshot` is the one dim declared ordered"
    )
    assert schema.dimensions["snapshot"].ordered, "the flag is kept on the dim"


def test_storage_partial_replaces_the_default():
    schema = Schema.from_mathspec(DECLARATIONS, storage=STORAGE)
    assert schema.partial == {"generator", "scenario"}, "storage names partial exactly"
