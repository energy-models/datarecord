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

from collections.abc import Collection, Mapping, Sequence
from typing import Any

import narwhals as nw

from datarecord.record import EMPTY, Flags, Frames, LazyFrames, RecordLike
from datarecord.schema import Schema


def to_sources(
    record: RecordLike, names: Collection[str] | None = None
) -> dict[str, nw.LazyFrame]:
    """The dimensions, relations and parameters `record` holds, as tables by name.

    A dimension is a table of its live labels, one column named after it. A
    relation is its rows, one column per coordinate. A parameter is one row per
    coordinate it has a value at, its coordinates and `value`: a row stored
    NULL along a dim it broadcasts over is expanded to every label of that dim,
    a row that names the label outranking it there, and a NULL value is left
    out, since a coordinate with no value has no row. Every coordinate has the
    type the schema declares, whatever precision a layer stored it at.

    A parameter no layer wrote is absent rather than filled with its `default`.

    Parameters
    ----------
    names
        The names to return - a solver passes the ones its spec declares.
        Every name the record holds where `None`.

    Raises
    ------
    KeyError
        If a name to return is both a dimension and a relation.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    schema = record.schema
    wanted = None if names is None else set(names)
    _ambiguous(schema, set(schema.dimensions) if wanted is None else wanted)
    dims = record.dims
    labels: dict[str, nw.LazyFrame] = {}
    out: dict[str, nw.LazyFrame] = {}
    for dim in schema.dims:
        if dim not in dims:
            continue
        axis = _typed(schema, dims[dim])
        labels[dim] = axis.select(dim)
        out[dim] = labels[dim]
        for attribute in schema.attributes_on(dim):
            if attribute in axis.collect_schema().names():
                out[attribute] = axis.select(
                    dim, nw.col(attribute).alias("value")
                ).filter(~nw.col("value").is_null())
    for relation in record.relations:
        out[relation] = _typed(schema, record.relations[relation]).select(
            *schema.relation_columns(relation)
        )
    for attribute in record.attributes:
        if wanted is None or attribute in wanted:
            out[attribute] = _expanded(
                schema, record.attributes[attribute], attribute, labels
            )
    return out if wanted is None else {n: f for n, f in out.items() if n in wanted}


def _ambiguous(schema: Schema, names: Collection[str]) -> None:
    """Refuse a name that is both a dimension and a relation.

    The tables are keyed by name alone, so the two would share one key.
    mathspec keeps every declaration in one namespace and refuses the same
    collision, which is why this refuses rather than picks one.
    """
    both = sorted(set(names) & set(schema.dimensions) & set(schema.relations))
    if both:
        msg = (
            f"{both} name both a dimension and a relation; tables are keyed by one "
            f"flat namespace, so rename the relation"
        )
        raise KeyError(msg)


def _typed(schema: Schema, frame: nw.LazyFrame) -> nw.LazyFrame:
    """`frame` with each declared dim column cast to the type the schema gives it."""
    return frame.with_columns(
        *(
            nw.col(c).cast(dtype)
            for c in frame.collect_schema().names()
            if c in schema.dimensions and (dtype := schema.column_type(c)) is not None
        )
    )


def _expanded(
    schema: Schema,
    frame: nw.LazyFrame,
    attribute: str,
    labels: Mapping[str, nw.LazyFrame],
) -> nw.LazyFrame:
    """One long attribute with its broadcast NULLs replaced by every label.

    A consumer of the declarations' contract reads a NULL coordinate as a label
    that matches nothing, so the broadcast the fold keeps implicit has to become
    rows here. Where expansion gives one coordinate two rows, the row that named
    more of the broadcast dims wins: a layer may state a default and its
    exceptions side by side. A layered record's read has already applied that
    rule along the `partial` dims it fills in, but a NULL along any other dim
    reaches here unexpanded, and a `RecordLike` need not be layered at all.
    Counted before expanding, because a join cannot match on a coordinate both
    rows leave NULL.
    """
    coordinates = list(schema.coordinates_of(attribute)) or [
        c for c in frame.collect_schema().names() if c in schema.dimensions
    ]
    spread_over = [d for d in schema.broadcasts_over(attribute) if d in labels]
    frame = (
        _typed(schema, frame)
        .select(*coordinates, "value")
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
        frame = nw.concat(
            [frame.filter(~nw.col(dim).is_null()), spread], how="vertical"
        )
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
    and the column is written NULL.

    Raises
    ------
    KeyError
        If a name is none the schema declares, or both a dimension and a relation.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    tables = {name: _lazy(table) for name, table in sources.items()}
    _ambiguous(schema, tables)
    declared = {*schema.dimensions, *schema.relations, *schema.attributes}
    unknown = sorted(set(tables) - declared)
    if unknown:
        msg = (
            f"the schema declares no {unknown}; a table is keyed by the name of a "
            f"dimension, relation or parameter"
        )
        raise KeyError(msg)
    return _Tables(schema, tables)


def _lazy(table: Any) -> nw.LazyFrame:
    """`table` as a lazy frame, an eager one by way of Arrow.

    Arrow rather than the table's own backend, because the shaping below adds
    typed NULL columns and pandas has no NULL for an integer column.
    """
    frame = nw.from_native(table)
    if isinstance(frame, nw.DataFrame):
        return nw.from_native(frame.to_arrow()).lazy()
    return frame


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
    def relations(self) -> Frames:
        present = tuple(r for r in self._schema.relations if r in self._tables)
        return LazyFrames(present, self._tables.__getitem__) if present else EMPTY

    @property
    def attributes(self) -> Frames:
        axis_attributes = {
            a for d in self._schema.dims for a in self._schema.attributes_on(d)
        }
        present = tuple(
            a
            for a in self._schema.attributes
            if a in self._tables and a not in axis_attributes
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
