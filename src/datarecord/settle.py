# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Settle a record's long rows by the broadcast rule.

Notes
-----
- [settling rows](https://energy-models.github.io/datarecord/design/use/reading/#settling-rows)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from duckdb import CaseExpression, DuckDBPyRelation, Expression, sqltypes
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import fn, named_mask, null_safe

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datarecord.protocols import Flags


def settle(
    rel: DuckDBPyRelation,
    labels: Mapping[str, DuckDBPyRelation],
    flags: Flags,
    *,
    filter: bool = True,
) -> DuckDBPyRelation:
    """Settle the long rows of `rel` by the broadcast rule, one row per coordinate.

    A row NULL on a coordinate in `labels` takes each label there, and a row
    naming a label outside `labels`, or reaching none, is dropped. At each
    coordinate the row naming every coordinate in `labels` that the other rows
    there name is kept. A coordinate outside `labels` is kept as written, a
    NULL there being one more label, and `breakpoint` is no coordinate, so a
    curve is kept or dropped whole.

    Parameters
    ----------
    labels
        Per coordinate, a relation holding its labels, or its tuples for a
        multi-dim, in a column of the coordinate's name.
    flags
        The attribute's `Flags`, or flags overstating them; they only spare
        work, so overstating changes no value.
    filter
        False keeps a row naming a label outside `labels` as written, for a
        caller whose own joins on the labels drop it anyway.

    Raises
    ------
    duckdb.Error
        On fetch, where no row names every coordinate the rows reaching it name.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [settling rows](https://energy-models.github.io/datarecord/design/use/reading/#settling-rows)
    """
    over = [d for d in labels if d in rel.columns and (filter or d in flags.broadcast)]
    mixed = [d for d in over if d in flags.broadcast and d in flags.varies]
    if mixed:
        rel = rel.project(star(), named_mask(mixed).alias("_named"))
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
    coordinates = [c for c in rel.columns if c not in ("_named", "breakpoint", "value")]
    coordinate = fn.concat_ws(
        lit(", "),
        *(
            fn.concat(
                lit(f"{c}="), coalesce(col(c).cast(sqltypes.VARCHAR), lit("NULL"))
            )
            for c in coordinates
        ),
    )
    crossed = fn.error(
        fn.concat(
            lit("two rows cross at "),
            coordinate,
            lit(
                " and no row reaching it names both, so neither is the more specific "
                "there (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)"
            ),
        )
    )
    return most_specific(rel, coordinates, crossed)


def most_specific(
    rel: DuckDBPyRelation,
    coordinates: Sequence[str],
    crossed: Expression,
    twins: Expression | None = None,
) -> DuckDBPyRelation:
    """Keep, at each coordinate, the row of `rel` naming every dim the other rows there name.

    `rel` carries each row's named dims as a `named_mask` in `_named`, which the
    result drops. `breakpoint` is no coordinate, so a curve is kept or
    dropped whole.

    Parameters
    ----------
    crossed
        The value at a coordinate where no row names every dim the rows there
        name; an `error` there fails the query on fetch.
    twins
        The value where two rows at one coordinate and breakpoint both name
        every such dim; None keeps both.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    by = ", ".join(str(col(c)) for c in coordinates)
    over = f"OVER (PARTITION BY {by})"
    # A window stays SQL text: DuckDB's expression API has no window construct.
    windows = {
        "_all": sql(f"bit_or(_named) {over}"),
        "_most": sql(f"max(bit_count(_named)) {over}"),
    }
    keep = CaseExpression(fn.bit_count(col("_all")) != col("_most"), crossed)
    if twins is not None:
        windows["_twins"] = sql(
            f"count(*) OVER (PARTITION BY {by}, breakpoint, _named)"
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
