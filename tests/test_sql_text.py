# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`sql_text`: a read built once over stand-ins, filled with each relation's text."""

import duckdb
import pytest
from duckdb import ColumnExpression as col
from duckdb import DuckDBPyRelation

from datarecord.resolve.sql_text import template


def _joined(left: DuckDBPyRelation, right: DuckDBPyRelation) -> DuckDBPyRelation:
    return (
        left.set_alias("l")
        .join(right.set_alias("r"), col("l", "k") == col("r", "k"))
        .project(col("l", "k"), col("l", "v"), col("r", "w"))
        .order("k")
    )


def test_a_filled_template_reads_as_the_read_built_over_its_parts():
    """A template built over one pair of relations fills with another pair of the same types."""
    con = duckdb.connect()
    left = con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(k, v)")
    right = con.sql("SELECT * FROM (VALUES (2, 2.5), (3, 3.5)) t(k, w)")
    made = template(con, _joined, left=left.limit(0), right=right)
    filled = con.sql(made.filled(left=left, right=right)).fetchall()
    assert filled == _joined(left, right).fetchall() == [(2, "b", 2.5)]


def test_a_template_refuses_parts_for_other_slots():
    """`filled` names the slots it has where the parts name others."""
    con = duckdb.connect()
    left = con.sql("SELECT 1 AS k, 'a' AS v")
    made = template(con, _joined, left=left, right=con.sql("SELECT 1 AS k, 2.5 AS w"))
    with pytest.raises(ValueError, match=r"slots \['left', 'right'\]"):
        made.filled(left=left)
