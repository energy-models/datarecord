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

from collections.abc import Iterable
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

type Shape = frozenset[str]

SHAPE_COLUMN = "_shape"
"""The column of a map kept as one table or file: each row's `shape_mask` under the schema of the node it belongs to.

What splits a map arriving as one relation by shape. A caller of the map
never sees it.

Notes
-----
- [`resolved/`](https://energy-models.github.io/datarecord/reference/format/#resolved)
"""


def shape_of(schema: Schema, attribute: str) -> Shape:
    """The dimensions `attribute` is over under `schema`, every one where it does not declare it.

    The unit of the fold: attributes of one shape fold together, their flags
    constant per dimension.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """
    if attribute not in schema.attributes:
        return frozenset(schema.dimensions)
    return frozenset(schema.dimensions_of(attribute))


def shape_mask(schema: Schema, shape: Shape) -> int:
    """`shape` as an integer, bit `i` set where it holds `schema.dimensions[i]`."""
    return sum(1 << i for i, d in enumerate(schema.dimensions) if d in shape)


def shapes(schema: Schema, attributes: Iterable[str]) -> dict[Shape, list[str]]:
    """`attributes`, sorted, grouped by their `shape_of` under `schema`."""
    grouped: dict[Shape, list[str]] = {}
    for a in sorted(attributes):
        grouped.setdefault(shape_of(schema, a), []).append(a)
    return grouped


def owner_map_rows(
    rel: DuckDBPyRelation, schema: Schema, layer_id: UUID, shape: Shape
) -> DuckDBPyRelation:
    """One layer's owner-map rows, as it wrote them: one per distinct index of its attribute rows `rel`, under its `schema`.

    `rel` holds rows of attributes of one `shape`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    tagged = _tagged(schema, rel, layer_id)
    return tagged.aggregate(
        [
            *(col(c) for c in (*schema.owner_map_index, LAYER_COLUMN)),
            *_flags(schema, shape),
        ]
    )


def _tagged(schema: Schema, rel: DuckDBPyRelation, layer: UUID) -> DuckDBPyRelation:
    """Layer `layer`'s attribute rows `rel`, padded to `schema`'s dimensions and cast, as `_flags` reads them."""
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
def _flags(schema: Schema, shape: Shape) -> tuple[Expression, ...]:
    """The aggregates giving one map row's `varies`, `broadcast` and `breakpoints` from its `_tagged` rows.

    The rows are of attributes of one `shape`. An attribute not over a
    dimension holds NULL there for want of the column, which is neither a
    label nor a broadcast.

    Notes
    -----
    - [flags](https://energy-models.github.io/datarecord/reference/resolution/#flags)
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """
    over = {d: lit(d in shape) for d in schema.dimensions}
    return (
        fn.struct_pack(
            **{d: fn.bool_or(col(d).isnotnull() & over[d]) for d in over}
        ).alias("varies"),
        fn.struct_pack(
            **{d: fn.bool_or(col(d).isnull() & over[d]) for d in over}
        ).alias("broadcast"),
        fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
    )
