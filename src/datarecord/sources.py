# SPDX-FileCopyrightText: Contributors to datarecord <https://github.com/energy-models/datarecord>
#
# SPDX-License-Identifier: MIT

"""A record as tables keyed by the names its declarations give, and back.

The seam between a record and whatever reads or writes it: a solver takes
`to_sources(record)`, an importer hands `from_sources(schema, tables)` to
`write_record`. Both speak the declarations' vocabulary - a dimension's labels
under its name, a relation's rows under its name, a parameter's `(dims...,
value)` rows under its name - so nothing framework-specific sits between them.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import narwhals as nw

from datarecord.record import EMPTY, Flags, Frames, LazyFrames, RecordLike
from datarecord.schema import Schema


def to_sources(record: RecordLike) -> dict[str, nw.LazyFrame]:
    """Every dimension, relation and parameter `record` holds, as tables by name.

    A dimension is a table of its live labels, one column named after it. A
    relation is its rows, one column per coordinate. A parameter is one row per
    coordinate it has a value at, its coordinates and `value`: a row stored
    NULL along a dim it broadcasts over is expanded to every label of that dim,
    a row that names the label outranking it there, and a NULL value is left
    out, since a coordinate with no value has no row.

    A parameter no layer wrote is absent rather than filled with its `default`.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    schema = record.schema
    dims = record.dims
    out: dict[str, nw.LazyFrame] = {}
    for dim in schema.dims:
        if dim not in dims:
            continue
        axis = dims[dim]
        out[dim] = axis.select(dim)
        for attribute in schema.attributes_on(dim):
            if attribute in axis.collect_schema().names():
                out[attribute] = (
                    axis.select(dim, nw.col(attribute).alias("value"))
                    .filter(~nw.col("value").is_null())
                )
    for group in record.groups:
        out[group] = record.groups[group].select(*schema.group_coordinates(group))
    for attribute in record.attributes:
        out[attribute] = _expanded(schema, record.attributes[attribute], attribute, out)
    return out


def _expanded(
    schema: Schema, frame: nw.LazyFrame, attribute: str, labels: Mapping[str, nw.LazyFrame]
) -> nw.LazyFrame:
    """One long attribute with its broadcast NULLs replaced by every label.

    A consumer of the declarations' contract reads a NULL coordinate as a label
    that matches nothing, so the broadcast the fold keeps implicit has to become
    rows here. Where expansion gives one coordinate two rows, the row that named
    more of the broadcast dims wins: a layer may state a default and its
    exceptions side by side. Counted before expanding, because a join cannot
    match on a coordinate both rows leave NULL.
    """
    coordinates = list(schema.coordinates_of(attribute)) or [
        c for c in frame.collect_schema().names() if c in schema.dimensions
    ]
    spread_over = [d for d in schema.broadcasts_over(attribute) if d in labels]
    frame = (
        frame.select(*coordinates, "value")
        .filter(~nw.col("value").is_null())
        .with_columns(
            nw.sum_horizontal(
                *(nw.col(d).is_null().cast(nw.Int64()) for d in spread_over), nw.lit(0)
            ).alias("_unnamed")
        )
    )
    for dim in spread_over:
        spread = (
            frame.filter(nw.col(dim).is_null())
            .drop(dim)
            .join(labels[dim], how="cross")
            .select(*coordinates, "value", "_unnamed")
        )
        frame = nw.concat([frame.filter(~nw.col(dim).is_null()), spread], how="vertical")
    return frame.filter(
        nw.col("_unnamed") == nw.col("_unnamed").min().over(*coordinates)
    ).drop("_unnamed")


def from_sources(schema: Schema, sources: Mapping[str, Any]) -> RecordLike:
    """A record over `sources`, tables keyed by the names `schema` declares.

    What `to_sources` returns, read the other way, and what `write_record`
    writes. Each value is anything `narwhals.from_native` takes. A dimension's
    table carries its labels; a relation's its rows; a parameter's its
    coordinates and `value`. A parameter table may leave out a coordinate it
    broadcasts over - a constant over every snapshot has no `snapshot` column -
    and the column is written NULL. A result's table goes to `outputs/`.

    Raises
    ------
    KeyError
        If a name is none the schema declares.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    tables = {name: nw.from_native(table).lazy() for name, table in sources.items()}
    declared = {
        *schema.dimensions,
        *schema.groups,
        *schema.attributes,
        *schema.results,
    }
    unknown = sorted(set(tables) - declared)
    if unknown:
        msg = (
            f"the schema declares no {unknown}; a table is keyed by the name of a "
            f"dimension, relation, parameter or result"
        )
        raise KeyError(msg)
    return _Tables(schema, tables)


class _Tables:
    """`RecordLike` over declared-name tables, built on read.

    The shaping `write_record` needs and a declared-name table lacks - an
    axis attribute joined onto its dim's labels, the `attribute` and
    `breakpoint` columns of a long row, a NULL for a coordinate left out - is
    done here, so the writer's own checks see what they would see from any
    other source.
    """

    def __init__(self, schema: Schema, tables: Mapping[str, nw.LazyFrame]):
        self._schema = schema
        self._tables = tables

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def dims(self) -> Frames:
        present = tuple(d for d in self._schema.dims if d in self._tables)
        return LazyFrames(present, self._axis) if present else EMPTY

    def _axis(self, dim: str) -> nw.LazyFrame:
        axis = self._tables[dim]
        for attribute in self._schema.attributes_on(dim):
            if attribute in self._tables:
                values = self._tables[attribute].select(
                    dim, nw.col("value").alias(attribute)
                )
                axis = axis.join(values, on=dim, how="left")
        return axis

    @property
    def groups(self) -> Frames:
        present = tuple(g for g in self._schema.groups if g in self._tables)
        return LazyFrames(present, self._tables.__getitem__) if present else EMPTY

    @property
    def attributes(self) -> Frames:
        return self._long(self._schema.attributes)

    @property
    def outputs(self) -> Frames:
        return self._long(self._schema.results)

    def _long(self, declared: Mapping[str, Any]) -> Frames:
        axis_attributes = {
            a for d in self._schema.dims for a in self._schema.attributes_on(d)
        }
        present = tuple(
            a for a in declared if a in self._tables and a not in axis_attributes
        )
        return LazyFrames(present, self._long_frame) if present else EMPTY

    def _long_frame(self, attribute: str) -> nw.LazyFrame:
        frame = self._tables[attribute]
        names = frame.collect_schema().names()
        columns = self._schema.long_columns_for(attribute)
        return frame.select(
            *(
                nw.col(c)
                if c in names
                else nw.lit(None, self._schema.column_type(c)).alias(c)
                for c in columns
                if c not in ("attribute", "value")
            ),
            nw.lit(attribute).alias("attribute"),
            nw.col("value"),
        ).select(*columns)

    def flags(self, **labels: Sequence[str]) -> dict[str, Flags]:
        """Never consulted: `write_record` persists frames, not flags."""
        return {}
