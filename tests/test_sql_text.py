# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`sql_text`: a read built once over its first call's relations, filled with each later call's text."""

from collections.abc import Callable

import duckdb
import pytest
from duckdb import ColumnExpression as col
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from datarecord.resolve.sql_text import filled, template


def _joined(left: DuckDBPyRelation, right: DuckDBPyRelation) -> DuckDBPyRelation:
    return (
        left.set_alias("l")
        .join(right.set_alias("r"), col("l", "k") == col("r", "k"))
        .project(col("l", "k"), col("l", "v"), col("r", "w"))
        .order("k")
    )


def _within(con: DuckDBPyConnection, left: DuckDBPyRelation) -> DuckDBPyRelation:
    """A relation whose text holds `left`'s as a filled template writes it."""
    made = template(con, lambda left: left.filter(col("k") > 0), left=left)
    return con.sql(made.filled(left=left))


def test_a_filled_template_reads_as_the_read_built_over_its_parts():
    """A template built over one pair of relations fills with another pair of the same types."""
    con = duckdb.connect()
    left = con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(k, v)")
    right = con.sql("SELECT * FROM (VALUES (2, 2.5), (3, 3.5)) t(k, w)")
    templates: dict = {}
    filled(templates, "joined", con, _joined, left=left.limit(0), right=right)
    statement = filled(templates, "joined", con, _joined, left=left, right=right)
    assert len(templates) == 1, "one template serves both calls"
    assert con.sql(statement).fetchall() == _joined(left, right).fetchall()
    assert con.sql(statement).fetchall() == [(2, "b", 2.5)]


def test_a_template_refuses_parts_for_other_slots():
    """`filled` names the slots it has where the parts name others."""
    con = duckdb.connect()
    left = con.sql("SELECT 1 AS k, 'a' AS v")
    made = template(con, _joined, left=left, right=con.sql("SELECT 1 AS k, 2.5 AS w"))
    with pytest.raises(ValueError, match=r"slots \['left', 'right'\]"):
        made.filled(left=left)


@pytest.mark.parametrize(
    "build",
    [lambda left, right: left, lambda left, right: right],
    ids=["unread", "read only inside another"],
)
def test_a_template_refuses_a_slot_read_nowhere_but_inside_another(
    build: Callable[..., DuckDBPyRelation],
):
    """A slot's text cannot be cut out where the statement reads it only inside another slot's, or not at all."""
    con = duckdb.connect()
    left = con.sql("SELECT 1 AS k, 'a' AS v")
    with pytest.raises(RuntimeError, match="nowhere in the template but inside"):
        template(con, build, left=left, right=_within(con, left))
