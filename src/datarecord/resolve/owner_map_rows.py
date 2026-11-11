# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""One layer's owner-map rows, as it wrote them, before any fold.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
- [flags](https://energy-models.github.io/datarecord/reference/resolution/#flags)
"""

from __future__ import annotations

from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression

from datarecord.duck import fn
from datarecord.schema.schema import (
    LAYER_COLUMN,
    LAYER_UUID_TYPE,
    Schema,
    cast_declared,
    per_schema,
    with_columns,
)


def owner_map_rows(
    rel: DuckDBPyRelation,
    schema: Schema,
    layer_id: UUID,
    attributes: tuple[str, ...],
) -> DuckDBPyRelation:
    """One layer's owner-map rows, as it wrote them: one per distinct index of its attribute rows `rel`, under its `schema`.

    `attributes` are those `rel` holds rows of.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    tagged = _tagged(schema, rel, layer_id)
    return tagged.aggregate(
        [
            *(col(c) for c in (*schema.owner_map_index, LAYER_COLUMN)),
            *_flags(schema, attributes),
        ]
    )


def _tagged(schema: Schema, rel: DuckDBPyRelation, layer: UUID) -> DuckDBPyRelation:
    """Layer `layer`'s attribute rows `rel`, padded to `schema`'s dims and cast, as `_flags` reads them."""
    rel = cast_declared(
        schema, with_columns(schema, rel, "breakpoint", *schema.dimensions)
    )
    return rel.project(
        *(col(d) for d in schema.dimensions),
        col("attribute"),
        lit(str(layer)).cast(LAYER_UUID_TYPE).alias(LAYER_COLUMN),
        col("breakpoint"),
    )


@per_schema
def _flags(schema: Schema, attributes: tuple[str, ...]) -> tuple[Expression, ...]:
    """The aggregates giving one map row's `varies`, `broadcast` and `breakpoints` from its `_tagged` rows.

    `attributes` are those the rows are of.

    Notes
    -----
    - [flags](https://energy-models.github.io/datarecord/reference/resolution/#flags)
    """
    broadcast = tuple(schema.dimensions)
    over = {d: _over(schema, d, attributes) for d in broadcast}
    return (
        fn.struct_pack(
            **{d: fn.bool_or(col(d).isnotnull() & over[d]) for d in broadcast}
        ).alias("varies"),
        fn.struct_pack(
            **{d: fn.bool_or(col(d).isnull() & over[d]) for d in broadcast}
        ).alias("broadcast"),
        fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
    )


def _over(schema: Schema, dim: str, attributes: tuple[str, ...]) -> Expression:
    """Whether a row's attribute, one of `attributes`, is over `dim`.

    An attribute not over `dim` holds NULL there for want of the column, which
    is neither a label nor a broadcast. An undeclared attribute counts as over
    every dim.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """
    lacking = [
        a for a in attributes if a in schema.attributes and dim not in schema.dims_of(a)
    ]
    if not lacking:
        return lit(True)  # noqa: FBT003
    if len(lacking) == len(attributes):
        return lit(False)  # noqa: FBT003
    return ~col("attribute").isin(*(lit(a) for a in lacking))
