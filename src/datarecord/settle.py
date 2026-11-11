# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Settle a record's long rows by the broadcast rule, and check what they cover.

Notes
-----
- [settling rows](https://energy-models.github.io/datarecord/reference/record/#settle)
"""

from __future__ import annotations

import operator
from collections.abc import Iterator, Sequence, Set
from functools import reduce
from itertools import combinations
from typing import TYPE_CHECKING

from duckdb import CaseExpression, DuckDBPyRelation, Expression, count_all, sqltypes
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import StarExpression as star

from datarecord.duck import ex_all, fn, join_on, null_pattern, null_safe

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datarecord.protocols import Flags


def settle(
    rel: DuckDBPyRelation,
    labels: Mapping[str, DuckDBPyRelation],
    flags: Flags,
    *,
    filter: bool = True,
) -> DuckDBPyRelation:
    """Settle the long rows of `rel` by the broadcast rule, one row per coordinate.

    A row NULL on a dimension in `labels` takes each label there, and a row
    naming a label outside `labels`, or covering none, is dropped. At each
    coordinate the row naming every dimension in `labels` that the other rows
    there name is kept. A dimension outside `labels` is kept as written, a
    NULL there being one more label, and `breakpoint` is no dimension, so a
    curve is kept or dropped whole.

    Parameters
    ----------
    labels
        Per dimension, a relation holding its labels, or its tuples for a
        multi-dimension, in a column of the dimension's name.
    flags
        The attribute's `Flags`, or flags overstating them; they only spare
        work, so overstating changes no value.
    filter
        False keeps a row naming a label outside `labels` as written, for a
        caller whose own joins on the labels drop it anyway.

    Raises
    ------
    duckdb.Error
        On fetch, where no row names every dimension the rows covering a coordinate name.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [settling rows](https://energy-models.github.io/datarecord/reference/record/#settle)
    """
    over = [d for d in labels if d in rel.columns and (filter or d in flags.broadcast)]
    mixed = [d for d in over if d in flags.broadcast and d in flags.varies]
    if mixed:
        rel = rel.project(star(), null_pattern(mixed).alias("_named"))
    for d in over:
        rows = rel.set_alias("_r")
        if d not in flags.broadcast:
            axis = labels[d].project(col(d)).set_alias("_ax")
            rel = rows.join(axis, col("_r", d) == col("_ax", d), how="semi")
        elif filter:
            rel = _broadcast_onto(rows, labels[d], d)
        else:
            rel = _broadcast_beside(rows, labels[d], d)
    if not mixed:
        return rel
    dims = [c for c in rel.columns if c not in ("_named", "breakpoint", "value")]
    coordinate = fn.concat_ws(
        lit(", "),
        *(
            fn.concat(
                lit(f"{c}="), coalesce(col(c).cast(sqltypes.VARCHAR), lit("NULL"))
            )
            for c in dims
        ),
    )
    crossed = fn.error(
        fn.concat(
            lit("two rows cross at "),
            coordinate,
            lit(
                " and no row reaching it names both, so neither is the more specific "
                "there (https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)"
            ),
        )
    )
    return most_specific(rel, dims, crossed)


def most_specific(
    rel: DuckDBPyRelation,
    dims: Sequence[str],
    crossed: Expression,
    twins: Expression | None = None,
) -> DuckDBPyRelation:
    """Keep, at each coordinate, the row of `rel` naming every dimension the other rows there name.

    `rel` carries each row's pattern as a `null_pattern` in `_named`, which the
    result drops. `breakpoint` is no dimension, so a curve is kept or
    dropped whole.

    Parameters
    ----------
    crossed
        The value at a coordinate where no row names every dimension the rows there
        name; an `error` there fails the query on fetch.
    twins
        The value where two rows at one coordinate and breakpoint both name
        every such dimension; None keeps both.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    by = [col(c) for c in dims]
    windows = {
        "_all": col("_named").bit_or().over(partition_by=by),
        "_most": fn.bit_count(col("_named")).max().over(partition_by=by),
    }
    keep = CaseExpression(fn.bit_count(col("_all")) != col("_most"), crossed)
    if twins is not None:
        windows["_twins"] = count_all().over(
            partition_by=[*by, col("breakpoint"), col("_named")]
        )
        keep = keep.when(
            (col("_twins") > lit(1)) & (col("_named") == col("_all")), twins
        )
    ranked = rel.project(star(), *(e.alias(n) for n, e in windows.items()))
    return ranked.filter(keep.otherwise(col("_named") == col("_all"))).project(
        star(exclude=("_named", *windows))
    )


def _broadcast_onto(
    rows: DuckDBPyRelation, labels: DuckDBPyRelation, d: str
) -> DuckDBPyRelation:
    """`rows` with a NULL `d` taking each of `labels`, and a label not in them dropped.

    One hash join against `labels` keyed by itself and by NULL, so expanding
    and filtering cost a single pass.
    """
    named_to = labels.project(col(d), col(d).alias("_label"))
    null_to = labels.project(lit(None).alias(d), col(d).alias("_label"))
    return rows.join(
        named_to.union(null_to).set_alias("_ax"), null_safe("_r", "_ax", [d])
    ).project(
        *(
            col("_ax", "_label").alias(c) if c == d else col("_r", c)
            for c in rows.columns
        )
    )


def _broadcast_beside(
    rows: DuckDBPyRelation, labels: DuckDBPyRelation, d: str
) -> DuckDBPyRelation:
    """`rows` with a NULL `d` taking each of `labels`, a named row kept as written."""
    # `IS NULL = true` hashes; the one-sided `IS NULL` is a nested loop.
    axis = labels.project(col(d), lit(True).alias("_broadcast"))  # noqa: FBT003
    return (
        rows.join(
            axis.set_alias("_ax"),
            col("_r", d).isnull() == col("_ax", "_broadcast"),
            how="left",
        )
        .project(
            *(
                coalesce(col("_r", d), col("_ax", d)).alias(c)
                if c == d
                else col("_r", c)
                for c in rows.columns
            )
        )
        .filter(col(d).isnotnull())
    )


def null_patterns(rel: DuckDBPyRelation, dims: Sequence[str]) -> list[frozenset[str]]:
    """The distinct sets of `dims` that rows of `rel` set."""
    if not dims:
        return [frozenset()]
    rows = rel.project(*(col(d).isnotnull() for d in dims)).distinct().fetchall()
    return [frozenset(d for d, n in zip(dims, row, strict=True) if n) for row in rows]


def has_pattern(dims: Sequence[str], pattern: Set[str]) -> Expression:
    """Whether a row sets exactly the `pattern` of `dims`."""
    return reduce(
        operator.and_,
        (col(d).isnotnull() if d in pattern else col(d).isnull() for d in dims),
        lit(True),  # noqa: FBT003
    )


def uncovered(
    rel: DuckDBPyRelation, staged: DuckDBPyRelation, columns: Sequence[str]
) -> DuckDBPyRelation:
    """`rel` without the rows a row of `staged` reaches, equal or NULL on each of `columns`.

    Split by which `columns` `staged` leaves NULL, each part an equi-join over
    the columns it names: a match on "NULL or equal" cannot hash.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    for pattern in null_patterns(staged, columns):
        part = staged.filter(has_pattern(columns, pattern))
        rel = rel.set_alias("r").join(
            part.set_alias("s"),
            null_safe("r", "s", [c for c in columns if c in pattern]),
            how="anti",
        )
    return rel


def unsettled(
    rel: DuckDBPyRelation, dims: Sequence[str]
) -> Iterator[tuple[frozenset[str], frozenset[str], DuckDBPyRelation]]:
    """Each pair of patterns of `rel` neither holds, with the coordinates its rows meet at that no row names both at.

    Two rows meet where they agree on every one of `dims` both name; a
    coordinate where they meet takes the labels either names. There the
    broadcast rule finds no row more specific than both. The relation is lazy
    and yields one column per dimension.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    patterns = null_patterns(rel, dims)

    def exactly(pattern: frozenset[str]) -> DuckDBPyRelation:
        return rel.filter(has_pattern(dims, pattern)).project(*map(col, dims))

    for a, b in combinations(patterns, 2):
        if a <= b or b <= a:
            continue
        on = [col("a", c) == col("b", c) for c in dims if c in a & b]
        meets = join_on(
            exactly(a).distinct().set_alias("a"),
            exactly(b).distinct().set_alias("b"),
            on,
        ).project(*(coalesce(col("a", c), col("b", c)).alias(c) for c in dims))
        if a | b in patterns:
            settled = ex_all(col("m", c) == col("u", c) for c in a | b)
            meets = meets.set_alias("m").join(
                exactly(a | b).set_alias("u"), settled, how="anti"
            )
        yield a, b, meets
