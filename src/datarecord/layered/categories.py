# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Selecting labels of a categorized dim, and the defaults its properties read.

Every default is the schema's (`Schema.default_for`), asked once per category
combination a relation holds.

Notes
-----
- [categories](https://energy-models.github.io/datarecord/design/schema/#categories-labels-of-an-enum-dtype-dim)
- [default_by](https://energy-models.github.io/datarecord/design/schema/#default_by-a-default-per-category)
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import reduce
from typing import Any

from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb import StarExpression as star

from datarecord.duck import ex_all, null_safe
from datarecord.schema import Schema

Categories = Mapping[str, "str | Sequence[str]"]
"""Per categorizing property, the one category or the categories to select."""


def categorized_dim(schema: Schema, categories: Categories) -> str:
    """The one dim every key of `categories` categorizes.

    Raises
    ------
    ValueError
        If a key categorizes no dim, or the keys categorize different dims.
    """
    dims = {d for d in schema.dims for k in categories if k in schema.categorizing(d)}
    unknown = sorted(
        k for k in categories if not any(k in schema.categorizing(d) for d in dims)
    )
    if unknown:
        msg = f"{unknown} categorize no dim; a categorizing property is a property of a `partial` dim naming an enum-dtype dim"
        raise ValueError(msg)
    if len(dims) != 1:
        msg = f"{sorted(categories)} categorize several dims {sorted(dims)}; name one dim's"
        raise ValueError(msg)
    return dims.pop()


def selected(
    schema: Schema, dim: str, categories: Categories
) -> dict[str, tuple[str, ...]]:
    """`categories` checked against `dim`'s, each as a tuple of categories.

    Raises
    ------
    ValueError
        If a key does not categorize `dim`, or a value is none of its categories.
    """
    schema.require_categorizing(dim, categories)
    chosen = {
        k: (v,) if isinstance(v, str) else tuple(v) for k, v in categories.items()
    }
    schema.require_listed(dim, chosen, "the selection")
    return chosen


def matching(
    axis: DuckDBPyRelation, chosen: Mapping[str, Sequence[Any]]
) -> DuckDBPyRelation:
    """The rows of `axis` whose stored values are among `chosen`; none for an absent column."""
    if any(k not in axis.columns for k in chosen):
        return axis.filter(lit(False))  # noqa: FBT003
    if not chosen:
        return axis
    return axis.filter(
        ex_all(col(k).isin(*(lit(v) for v in vs)) for k, vs in chosen.items())
    )


def with_defaults(
    schema: Schema,
    subject: str,
    rel: DuckDBPyRelation,
    names: Sequence[str],
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """`rel` projected to `subject` and `names`, an unset name read as its default."""
    if not names:
        return rel.project(col(subject))

    def columns(cats: dict[str, str]) -> list[Expression]:
        return [
            lit(schema.default_for(f"{subject}.{n}", **cats))
            .cast(schema.duck_type(schema.properties[subject][n].dtype))
            .alias(f"_default_{n}")
            for n in names
        ]

    return _per_combination(schema, subject, rel, columns, con).project(
        col("r", subject),
        *(coalesce(col("r", n), col("t", f"_default_{n}")).alias(n) for n in names),
    )


def with_properties(
    schema: Schema, rel: DuckDBPyRelation, subject: str
) -> DuckDBPyRelation:
    """`rel` with each property of `subject` it lacks added as a typed NULL."""
    missing = [n for n in schema.properties.get(subject, {}) if n not in rel.columns]
    if not missing:
        return rel
    return rel.project(
        star(),
        *(
            lit(None)
            .cast(schema.duck_type(schema.properties[subject][n].dtype))
            .alias(n)
            for n in missing
        ),
    )


def _combinations(
    schema: Schema, subject: str, rel: DuckDBPyRelation
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """`subject`'s categorizing columns in `rel`, and their distinct value combinations."""
    keys = [k for k in schema.categorizing(subject) if k in rel.columns]
    if not keys:
        return keys, [()]
    return keys, rel.project(*(col(k) for k in keys)).distinct().fetchall()


def _categories_of(keys: Sequence[str], combo: Sequence[Any]) -> dict[str, str]:
    """`combo` by key, a NULL category left out, so it matches no `default_by` keyed on it."""
    return {k: c for k, c in zip(keys, combo, strict=True) if c is not None}


def _per_combination(
    schema: Schema,
    subject: str,
    rel: DuckDBPyRelation,
    columns: Callable[[dict[str, str]], list[Expression]],
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """`rel` as `r`, joined on its categories to `t`, `columns` of them.

    `t` is built eagerly, one row per category combination `rel` holds when
    called, so the default rules stay the schema's.
    """
    keys, combos = _combinations(schema, subject, rel)
    types = dict(zip(rel.columns, rel.types, strict=True))

    def row(combo: Sequence[Any]) -> DuckDBPyRelation:
        return con.values(
            (
                *(
                    lit(c).cast(types[k]).alias(k)
                    for k, c in zip(keys, combo, strict=True)
                ),
                *columns(_categories_of(keys, combo)),
            )  # type: ignore[arg-type]
        )

    table = (
        reduce(DuckDBPyRelation.union, map(row, combos))
        if combos
        else row((None,) * len(keys)).limit(0)
    ).set_alias("t")
    if not keys:
        return rel.set_alias("r").cross(table)
    return rel.set_alias("r").join(table, null_safe("r", "t", keys))
