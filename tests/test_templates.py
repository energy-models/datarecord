# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`templates`: a plan's read built once over its first call's relations, filled with each later call's text."""

from dataclasses import dataclass

import pytest
from duckdb import ColumnExpression as col
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from datarecord import Workspace


@dataclass(frozen=True)
class Joined:
    key: str

    def build(
        self, con: DuckDBPyConnection, left: DuckDBPyRelation, right: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        return (
            left.set_alias("l")
            .join(right.set_alias("r"), col("l", self.key) == col("r", self.key))
            .project(col("l", self.key), col("l", "v"), col("r", "w"))
            .order(self.key)
        )


@dataclass(frozen=True)
class Positive:
    def build(
        self, con: DuckDBPyConnection, **slots: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        return slots["left"].filter(col("k") > 0)


@dataclass(frozen=True)
class Only:
    slot: str

    def build(
        self, con: DuckDBPyConnection, **slots: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        return slots[self.slot]


def test_one_template_serves_two_calls_of_the_same_types(workspace: Workspace):
    """A template built over one pair of relations fills with another pair of the same types, passed by position or by name."""
    con = workspace.con
    left = con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(k, v)")
    right = con.sql("SELECT * FROM (VALUES (2, 2.5), (3, 3.5)) t(k, w)")
    read = workspace.templates.memoize(Joined("k"))
    read(left.limit(0), right)
    statement = read.text(left=left, right=right)
    assert len(workspace.templates._kept) == 1, "one template serves both calls"
    assert read(left, right).fetchall() == [(2, "b", 2.5)]
    assert read.bind(left)(right).fetchall() == [(2, "b", 2.5)], (
        "bound slots come first"
    )
    assert (
        con.sql(statement).fetchall() == Joined("k").build(con, left, right).fetchall()
    )


def test_a_read_refuses_slots_its_build_does_not_take(workspace: Workspace):
    """The slots bind to `build`'s arguments, so one missing fails before any template is built."""
    left = workspace.con.sql("SELECT 1 AS k, 'a' AS v")
    with pytest.raises(TypeError, match="right"):
        workspace.templates.memoize(Joined("k")).text(left)


@pytest.mark.parametrize(
    "slot", ["left", "right"], ids=["unread", "read only inside another"]
)
def test_a_template_refuses_a_slot_read_nowhere_but_inside_another(
    workspace: Workspace, slot: str
):
    """A slot's text cannot be cut out where the statement reads it only inside another slot's, or not at all."""
    con = workspace.con
    left = con.sql("SELECT 1 AS k, 'a' AS v")
    within = workspace.templates.memoize(Positive())(left=left)
    read = workspace.templates.memoize(Only(slot))
    with pytest.raises(RuntimeError, match="nowhere in the template but inside"):
        read(left=left, right=within)
