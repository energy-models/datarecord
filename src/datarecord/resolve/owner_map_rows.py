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

import threading
from collections.abc import Callable
from functools import wraps
from typing import Concatenate
from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import StarExpression as star

from datarecord.duck import empty_relation, fn
from datarecord.schema.schema import LAYER_COLUMN, LAYER_UUID_TYPE, Schema


def cast_declared(
    schema: Schema,
    rel: DuckDBPyRelation,
    subject: str | None = None,
    attribute: str | None = None,
) -> DuckDBPyRelation:
    """Return `rel` with each column the schema declares a type for cast to that type.

    Other columns are left as they are. `subject` and `attribute` are as in
    `Schema.declared`.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    cols = [
        col(c).cast(found[1]).alias(c)
        if (found := schema.declared(c, subject, attribute))
        else col(c)
        for c in rel.columns
    ]
    return rel.project(*cols)


def _empty_relation(
    schema: Schema, con: DuckDBPyConnection, *columns: str
) -> DuckDBPyRelation:
    """A zero-row relation with `columns`, cast to their declared type, `VARCHAR` if none.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    return empty_relation(
        con, **{c: schema.column_type(c) or sqltypes.VARCHAR for c in columns}
    )


def with_columns(
    schema: Schema, rel: DuckDBPyRelation, *columns: str
) -> DuckDBPyRelation:
    """`rel` with any of `columns` it lacks added as typed NULLs.

    One file carries a column per dim of one attribute, so a column another
    attribute uses is simply absent - and a dim an attribute is not addressed by
    means the value applies across every value of it, which is what NULL means.
    Materialising it here lets every path downstream project the column
    unconditionally instead of branching on its presence.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    missing = [c for c in columns if c not in rel.columns]
    if not missing:
        return rel
    added = [
        lit(None).cast(schema.column_type(c) or sqltypes.VARCHAR).alias(c)
        for c in missing
    ]
    return rel.project(star(), *added)


def _per_schema[**P, T](
    build: Callable[Concatenate[Schema, P], T],
) -> Callable[Concatenate[Schema, P], T]:
    """`build` memoised on a `Schema` and further hashable arguments, for the last 64 keys.

    A schema is unhashable, and every layer holds its own equal copy, so it is
    keyed by its `digest`.
    """
    held: dict[tuple[object, ...], T] = {}
    lock = threading.Lock()

    @wraps(build)
    def memo(schema: Schema, *args: P.args, **kwargs: P.kwargs) -> T:
        key = (schema.digest, *args, *sorted(kwargs.items()))
        with lock:
            found = held.get(key)
        if found is not None:
            return found
        value = build(schema, *args, **kwargs)
        with lock:
            held[key] = value
            if len(held) > 64:
                del held[next(iter(held))]
        return value

    return memo


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
    rel = cast_declared(schema, with_columns(schema, rel, "breakpoint", *schema.dims))
    return rel.project(
        *(col(d) for d in schema.dims),
        col("attribute"),
        lit(str(layer)).cast(LAYER_UUID_TYPE).alias(LAYER_COLUMN),
        col("breakpoint"),
    )


@_per_schema
def _flags(schema: Schema, attributes: tuple[str, ...]) -> tuple[Expression, ...]:
    """The aggregates giving one map row's `varies`, `broadcast` and `breakpoints` from its `_tagged` rows.

    `attributes` are those the rows are of.

    Notes
    -----
    - [flags](https://energy-models.github.io/datarecord/reference/resolution/#flags)
    """
    broadcast = schema.dims
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
