# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Editing a record: staged edits, written as a layer by whoever writes them.

What `Record` (read-only) and a whole-record write do not cover: an edit costs
a row in a staging table rather than a rewrite, and nothing touches the record
until `edits` is written as a child.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

from __future__ import annotations

import operator
import re
import sys
from collections.abc import Collection, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import reduce
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression, sqltypes
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star
from duckdb.sqltypes import DuckDBPyType

from datarecord.duck import (
    as_relation,
    empty_relation,
    fn,
    is_frame,
    null_safe,
    struct_of,
    union_all_by_name,
)
from datarecord.protocols import RecordLike
from datarecord.resolve.coords import first_unheld
from datarecord.resolve.fold import Fold
from datarecord.resolve.record import Record
from datarecord.schema.categories import matching
from datarecord.schema.dtypes import enum_labels, takes
from datarecord.schema.schema import LONG_TAIL, Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


# -- value normalisation (https://energy-models.github.io/datarecord/design/working-record/#set) ----------------------------------------------


def _series_index(value: Any) -> Sequence[Any] | None:
    """`value`'s labels if it is a one-dimensional labelled series, else None."""
    index = getattr(value, "index", None)
    if index is None or getattr(value, "ndim", None) != 1:
        return None
    return list(index)


def _series_index_name(value: Any) -> str | None:
    """A labelled series' index name, where it has one a caller could have meant.

    `pd.Series(...).index.name` is the caller already saying what the index
    holds. A `MultiIndex` has `names` rather than one `name` and is no
    one-dimensional index, so it answers None.
    """
    index = getattr(value, "index", None)
    name = getattr(index, "name", None)
    return name if isinstance(name, str) else None


def _is_labels(value: Any) -> bool:
    """Whether a keyword's value lists several labels rather than naming one."""
    return isinstance(value, Sequence) and not isinstance(value, str | bytes)


def normalise_value(
    value: Any, labels: Sequence[Any] | None = None
) -> tuple[list[Any] | None, list[Any]]:
    """One of `set`'s four `value` forms as labels and the values at them.

    Parameters
    ----------
    value
        Scalar, sequence, mapping, or a one-dimensional labelled series.
    labels
        What a sequence aligns to positionally.

    Returns
    -------
    keys
        The label each value belongs to, or None for a scalar.
    values
        One value per key, or the scalar alone.

    Raises
    ------
    ValueError
        If a sequence has no `labels`, or a different length.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    index = _series_index(value)
    if index is not None:
        return list(index), list(value)
    if isinstance(value, Mapping):
        return list(value), list(value.values())
    if not _is_labels(value):
        return None, [value]
    if labels is None:
        msg = "a sequence needs labels to align to; pass them as a list keyword"
        raise ValueError(msg)
    if len(value) != len(labels):
        msg = (
            f"{len(value)} values for {len(labels)} labels; a length mismatch is "
            f"an error at the call rather than a truncated edit"
        )
        raise ValueError(msg)
    return list(labels), list(value)


@dataclass(frozen=True)
class StagedSource:
    """A staging area as one layer's rows - the last source the fold reads.

    "The layer as it would be written": each member reads its staging table,
    which an edit replaces by key rather than appending to (`_insert`), so the
    table is already one row per key and what a write would persist. An axis
    or group is staged whole, and handed over in position order.

    Unfrozen, which is the whole of what distinguishes it: a `set` changes these
    rows under a reader, so the fold must stay a relation past this point rather
    than materialise. Nothing here needs invalidating in exchange - a relation
    over a staging table reads whatever the table holds when it is collected.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
    """

    record: WorkingRecord
    layer_id: UUID
    frozen: bool = False

    @property
    def schema(self) -> Schema:
        """This record's schema - the `LayerData` a write reads for one.

        A staging area is not a `LayerSource` a fold folds under a schema
        someone else supplies; it is also what `rev.child(w.edits)` writes
        directly, where it needs its own.
        """
        return self.record.schema

    @property
    def base(self) -> UUID:
        """The revision these edits are a delta against: the base's `revision_id`."""
        return self.record._base.revision_id

    def materialised(self, con: DuckDBPyConnection, schema: Schema) -> Fold | None:
        """A staging area has no `resolved/` cache: it is never a fold's base."""
        return None

    def axes(self) -> set[str]:
        """The dims staged whole - by `add`, `remove` or a `set` on an axis column."""
        return set(self.record._staged_dims())

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """One axis as this layer would write it: every label, with its properties, in order.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return self.record._restatement(f"{_AXIS_PREFIX}{dim}")

    def groups(self) -> set[str]:
        """Which declared groups are staged whole."""
        return set(self.record._staged_groups())

    def group(self, name: str) -> DuckDBPyRelation | None:
        return self.record._restatement(name)

    def attributes(self) -> set[str]:
        """Which attributes have staged rows."""
        return set(self.record._staged_attributes())

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_attribute(name)

    def all_attributes(self) -> DuckDBPyRelation | None:
        """Every staged attribute, unioned by name and unprojected.

        By name because the tables carry per-attribute column variation exactly
        as the files do - one attribute's coordinates and no others - which is
        what lets `fold_attributes` pad both the same way.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        arms = [
            rel
            for name in self.record._staged_attributes()
            if (rel := self.attribute(name)) is not None
        ]
        if not arms:
            return None
        return union_all_by_name(arms, self.record.con)


# -- the DuckDB-backed implementation (https://energy-models.github.io/datarecord/design/working-record/#staging) ---------------------------------


class WorkingRecord(Record):
    """A `Record` whose last source is a staging area, plus the edit surface.

    A `Record` in the type as well as in the fold: what it reads is the data
    *with its pending edits applied*, and it reads it by being one layer deeper
    than its base rather than by overlaying anything of its own. Every read
    member is inherited unchanged, so an edit reads back through the same code
    path a committed layer would.

    Staged rows live in connection-scoped DuckDB tables, the *only* place a
    staged row exists: the reads fold them rather than holding a copy, so what
    is staged is asked of the reads themselves.

    The source list is fixed at construction and never changes; a `set`
    changes what the last source's tables hold, not which sources there are.
    That is what lets the inherited members stay correct - they cache a key
    set only where the fold is stable, which a staged source makes it not.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
    - [staging](https://energy-models.github.io/datarecord/design/working-record/#staging)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """

    #: This record's one identity, which is the staged layer's: the fold stamps
    #: it as `layer_uuid` and dispatches a winning row back through the source
    #: carrying it. Synthetic, the staged layer having no revision until
    #: `edits` is written as one, and it names the staging tables too - so two
    #: `WorkingRecord`s on one connection collide in neither.
    _layer_id: UUID
    #: The record these edits are made over: its sources are this record's
    #: but the staged one, and its `revision_id` is what the edits are a delta
    #: against.
    _base: Record
    #: Keyed by `(kind, attribute)`, the attribute being None for an axis or a
    #: group. Attributes stage one table per attribute because that is the
    #: file it stands for: one `value` column at the attribute's own type, and
    #: its own coordinates and no others.
    _staged: dict[tuple[str, str | None], str]

    def __init__(self, base: RecordLike) -> None:
        """Stage edits over `base`, which must be a `Record`.

        Raises
        ------
        TypeError
            For any other `RecordLike`. A framework object hands over relations
            and has no layer layout behind it, so synthesising one would be
            rebuilding the format from a protocol that deliberately lacks it.
        """
        if not isinstance(base, Record):
            msg = (
                f"a `WorkingRecord` reads by folding its staged rows over the base's, "
                f"and a {type(base).__name__} has no layer layout to fold - pass a "
                f"`Record`, which `Revision.record` and `Workspace.at(uri)` both give"
            )
            raise TypeError(msg)
        # `object.__setattr__` throughout: the base is a frozen dataclass, and
        # these are set before `super().__init__` because the `StagedSource`
        # below reads them off `self`.
        object.__setattr__(self, "_layer_id", uuid4())
        object.__setattr__(self, "_staged", {})
        object.__setattr__(self, "_base", base)
        super().__init__(
            base.revision_id,
            [*base.sources, StagedSource(self, self._layer_id)],
            base.workspace,
            base.schema,
        )

    # -- staging tables -----------------------------------------------------

    def _table(self, kind: str, attribute: str | None = None) -> str:
        """A staging table's name, unique per record and per attribute.

        The attribute is hashed rather than spelled: it is a caller's string,
        and a table name is the one thing here that cannot be an expression, so
        it would be an injection and a quoting problem at once.
        """
        if kind.startswith(_AXIS_PREFIX):
            # Hashed for the same reason the attribute is: a dim name is a
            # caller's string, and this becomes an identifier.
            digest = sha256(kind[len(_AXIS_PREFIX) :].encode()).hexdigest()[:16]
            return f"staged_axis_{digest}_{self._layer_id.hex}"
        if attribute is None:
            return f"staged_{kind}_{self._layer_id.hex}"
        digest = sha256(attribute.encode()).hexdigest()[:16]
        return f"staged_{kind}_{digest}_{self._layer_id.hex}"

    def _ensure(self, kind: str, attribute: str | None = None) -> str:
        """The staging table for `kind`, created on first use.

        `kind` is `_ATTRIBUTES`, an axis (`_AXIS_PREFIX` and its dim), or a
        declared group's name - an axis or a group gets a table shaped by its
        own file.

        `_ATTRIBUTES` takes an `attribute` and gets a table per attribute, shaped
        like the file it becomes: `long_columns_for` for the columns, and the
        declared dtype for `value`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        key = (kind, attribute)
        if key in self._staged:
            return self._staged[key]
        name = self._table(kind, attribute)
        self._shape(kind, attribute).create(name)
        self._staged[key] = name
        base = None if attribute is not None else self._base_rows(kind)
        if base is not None:
            numbered = base.project(
                star(), sql("row_number() OVER ()").alias(_POSITION)
            )
            self._insert(numbered, name, {})
        return name

    def _base_rows(self, kind: str) -> DuckDBPyRelation | None:
        """The base's resolved axis or group `kind` stands for, with its properties."""
        if not kind.startswith(_AXIS_PREFIX):
            return self._base.groups.get(kind)
        dim = kind[len(_AXIS_PREFIX) :]
        rows = (
            self._base.properties if dim in self.schema.properties else self._base.dims
        )
        return rows.get(dim)

    def _restatement(self, kind: str) -> DuckDBPyRelation | None:
        """The staged axis or group `kind` stands for, in position order; `None` where unstaged."""
        name = self._staged.get((kind, None))
        if name is None:
            return None
        return self.con.sql(
            f"SELECT * EXCLUDE ({_POSITION}) FROM {name} ORDER BY {_POSITION}"
        )

    def _positioned(
        self, rel: DuckDBPyRelation, table: str, kind: str, key: str
    ) -> DuckDBPyRelation:
        """`rel` with each row's position in the staged axis or group `table`.

        A label keeps its staged position, a label of the base takes its base
        position back, and any other comes after every position either holds,
        in `rel`'s order.
        """
        staged = self.con.table(table)
        base = self._base_rows(kind)
        start = max(
            staged.aggregate(f"coalesce(max({_POSITION}), 0)").fetchall()[0][0],
            0 if base is None else base.count("*").fetchall()[0][0],
        )
        numbered = rel.project(star(), sql("row_number() OVER ()").alias("_n"))
        held = [staged.project(col(key), col(_POSITION))]
        if base is not None:
            held.append(
                base.project(col(key), sql("row_number() OVER ()").alias(_POSITION))
            )
        joined = numbered.set_alias("r")
        for i, positions in enumerate(held):
            alias = f"h{i}"
            joined = joined.join(
                positions.set_alias(alias), null_safe("r", alias, [key]), how="left"
            )
        position = coalesce(
            *(col(f"h{i}", _POSITION) for i in range(len(held))),
            lit(start) + col("r", "_n"),
        )
        return joined.project(
            *(col("r", c) for c in rel.columns), position.alias(_POSITION)
        )

    def _shape(self, kind: str, attribute: str | None) -> DuckDBPyRelation:
        """A row-less relation shaped like the file `kind` becomes.

        The one place a staging table's columns are decided, so a new kind is a
        branch here rather than a second dispatch beside it.
        """
        if attribute is not None:
            return self._empty_long(attribute)
        if kind.startswith(_AXIS_PREFIX):
            columns = _axis_columns(self.schema, kind[len(_AXIS_PREFIX) :])
        else:
            columns = _group_columns(self.schema, kind)
        return empty_relation(self.con, **columns, **{_POSITION: sqltypes.BIGINT})

    def _empty_long(self, attribute: str) -> DuckDBPyRelation:
        """A row-less relation shaped like one attribute's long file.

        What the staging table is created from, so the table's shape is a
        projection rather than assembled DDL - the same expressions the inserts
        then project, which is what keeps the two from drifting.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        """
        value = self.schema.value_type(attribute)
        shaped = {
            c: value if c == "value" else self._column_type(c)
            for c in self.schema.long_columns_for(attribute)
        }
        return empty_relation(self.con, **cast("dict[str, DuckDBPyType]", shaped))

    def _rows(self, kind: str, attribute: str | None = None) -> DuckDBPyRelation | None:
        name = self._staged.get((kind, attribute))
        return None if name is None else self.con.table(name)

    def _staged_attributes(self) -> tuple[str, ...]:
        """Which attributes have staged rows, in insertion order.

        The staging map is the answer, so this is not a query: a table exists
        exactly where rows were staged.
        """
        return tuple(a for (k, a), _ in self._staged.items() if k == _ATTRIBUTES and a)

    def _column_type(self, column: str, subject: str | None = None) -> DuckDBPyType:
        return _column_type(self.schema, column, subject)

    def _staged_coordinates(self, attribute: str) -> tuple[str, ...]:
        """The dim columns one attribute's staging table has, in table order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return tuple(
            c for c in self.schema.long_columns_for(attribute) if c not in LONG_TAIL
        )

    # -- Record, one fold deeper (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) --------------------------------

    # `schema`, `dims`, `groups`, `attributes` and `flags` are
    # inherited from `Record` unchanged, which is the property this design
    # exists to have: a staged edit is read by the same fold that reads a
    # committed layer, so there is no second overlay to keep in step.

    def _owned_whole(self, attribute: str) -> tuple[str, ...]:
        """`AttributeSpec.dims` minus the fold key - the value axes owned whole.

        The complement of `owned_per`: a dim or group the attribute varies over
        but that is not in `partial_dims` (a non-`partial` `timestep`, or
        `connection`) is owned whole, so a patch to one value restates the
        attribute's whole extent along it.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        spec = self.schema.attributes.get(attribute)
        whole = (
            frozenset() if spec is None else spec.dims - set(self.schema.partial_dims)
        )
        return tuple(
            d for d in (*self.schema.group_columns, *self.schema.dims) if d in whole
        )

    # -- edits (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/working-record/#add-remove) ----------------------------------------

    def _key_coordinate(self, attribute: str, value: Any, given: Container[str]) -> str:
        """Which coordinate a mapping's keys or a labelled series' index holds.

        The series' own `index.name` where it names a coordinate of this
        attribute, else the one coordinate no keyword names. Never read off the
        labels: one dim's label may be a string just like another's, so a
        membership test would make one call mean different things in two
        records.

        An `index.name` naming no coordinate is ignored rather than rejected; it
        may be `None`, or a pandas artefact like `"index"`.

        Raises
        ------
        ValueError
            Where the index names a coordinate also given as a keyword, or
            nothing says which of several open coordinates is meant.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        coordinates = _flat_coordinates(self.schema, attribute)
        named = _series_index_name(value)
        if named in coordinates:
            if named in given:
                msg = (
                    f"`set({attribute!r}, ...)` keys its values by {named!r} and "
                    f"names it as a keyword too; give its labels one way"
                )
                raise ValueError(msg)
            return cast("str", named)
        open_ = [c for c in coordinates if c not in given]
        if len(open_) != 1:
            msg = (
                f"`set({attribute!r}, ...)` keys its values by one of {open_}; "
                f"say which by naming the series' index, or name the others as "
                f"keywords"
            )
            raise ValueError(msg)
        return open_[0]

    def _coordinate_type(self, attribute: str, coordinate: str) -> DuckDBPyType:
        """The declared type of one flat coordinate of `attribute` - a group field's dim's."""
        dim = _flat_coordinates(self.schema, attribute).get(coordinate, coordinate)
        return self._column_type(dim)

    def _axis_values(
        self, dim: str, name: str, value: Any, coords: Mapping[str, Any]
    ) -> Any:
        """One property's edit, as a mapping from label to value or a bare scalar.

        `value` is a mapping or labelled series from label to value, a scalar or
        sequence over the labels a `dim=` keyword names, or a scalar for every
        label the axis currently has - which stays a scalar.

        Raises
        ------
        ValueError
            On a keyword other than `dim`, or a sequence with no labels to
            align to.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        unknown = sorted(set(coords) - {dim})
        if unknown:
            msg = (
                f"`set({f'{dim}.{name}'!r}, ...)` names {unknown}, but {name!r} is a "
                f"property of {dim!r} - a column of dims/{dim}.parquet, keyed "
                f"by nothing else"
            )
            raise ValueError(msg)
        if not isinstance(value, Mapping) and (
            dim in coords or _series_index(value) is not None
        ):
            given = coords.get(dim)
            labels = (
                None if given is None else list(given) if _is_labels(given) else [given]
            )
            keys, values = normalise_value(value, labels)
            if keys is None:
                keys, values = cast("list[Any]", labels), values * len(labels or ())
            value = dict(zip(keys, values, strict=True))
        elif is_frame(value) or _is_labels(value):
            msg = (
                f"`set({f'{dim}.{name}'!r}, <sequence>)` has no labels to align to; "
                f"{name!r} is a property of {dim!r}, so pass a mapping from "
                f"{dim!r} label to value, or a scalar for every label"
            )
            raise ValueError(msg)
        return value

    def _stage_axis(self, dim: str, name: str, value: Any) -> None:
        """Stage property `name` of `dim`, `value` as `_axis_values` returns it.

        A scalar reaches every label of the axis as staged, one added in this
        record included.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        if not isinstance(value, Mapping):
            value = dict.fromkeys(self._labels_matching(dim, {}), value)
            if not value:
                msg = (
                    f"`set({f'{dim}.{name}'!r}, <scalar>)` reaches every label the "
                    f"{dim!r} axis has, and it has none; `add` labels first"
                )
                raise ValueError(msg)
        if not value:
            return
        edit = self._values_relation(
            {dim: list(value), name: list(value.values())},
            {dim: self._column_type(dim), name: self._column_type(name, dim)},
        )
        self._patch(f"{_AXIS_PREFIX}{dim}", dim, name, edit)

    def _patch(self, kind: str, key: str, name: str, edit: DuckDBPyRelation) -> None:
        """Set property `name` on each label or tuple `edit` names, in the staged `kind`.

        `key` is the one column a row is keyed by: a dim, or a group's struct
        column. The first edit stages the base's axis or group whole, so the
        row is there to update, its other properties with it.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        table = self._ensure(kind)
        # Raw SQL because `UPDATE ... FROM` is a join-update, which the relational
        # `update` cannot express (https://duckdb.org/docs/stable/clients/python/relational_api).
        self.con.execute(
            f"UPDATE {table} AS t SET {col(name)} = {col('e', name)} "
            f"FROM edit e "
            f"WHERE {col('t', key)} IS NOT DISTINCT FROM {col('e', key)}"
        )

    def _validate_dims(self, dims: Mapping[str, Any]) -> None:
        """The dim vocabulary.

        A group's key coordinate is a keyword too, `from=` for a corridor,
        though it names no dim.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        keys = {c for g in self.schema.groups for c in self.schema.group_key(g)}
        unknown = sorted(set(dims) - set(self.schema.dims) - keys)
        if unknown:
            msg = f"the schema declares no dims {unknown}"
            raise KeyError(msg)

    def _validate_input(self, attribute: str, coords: Mapping[str, Any]) -> None:
        """An input's name, its keywords, and the labels they give.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        self._validate_dims(coords)
        spec = self.schema.attributes.get(attribute)
        if spec is None:
            msg = f"the schema declares no attribute {attribute!r}"
            raise KeyError(msg)
        coordinates = _flat_coordinates(self.schema, attribute)
        outside = sorted(set(coords) - spec.dims - set(coordinates))
        if outside:
            msg = (
                f"{attribute!r} does not vary over {outside}; "
                f"it varies over {sorted(spec.dims) or 'nothing'}"
            )
            raise ValueError(msg)
        for c, given in coords.items():
            if c in coordinates:
                labels = list(given) if _is_labels(given) else [given]
                self._require_typed(
                    coordinates[c], labels, f"`set({attribute!r}, ...)`"
                )

    def _require_held(self, rows: DuckDBPyRelation, attribute: str) -> None:
        """Refuse a row of `attribute` naming a label or tuple the staged axis or group lacks.

        Labels are checked before tuples, so a tuple naming an unknown label
        is refused for that label. NULL is no label and passes.

        Raises
        ------
        KeyError
            Naming the dim, the group's field or the group, and up to three
            such labels or tuples.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        dims, groups = self.dims, self.groups
        coordinates = self._staged_coordinates(attribute)
        over = {
            c: self.schema.groups[c].over
            for c in coordinates
            if c in self.schema.groups
        }
        fields = {f"{g}.{f}": (g, f, d) for g, o in over.items() for f, d in o.items()}
        named = {
            **{c: (col(c), dims.get(c), c) for c in coordinates if c not in over},
            **{
                k: (fn.struct_extract(col(g), lit(f)), dims.get(d), d)
                for k, (g, f, d) in fields.items()
            },
            **{g: (col(g), groups.get(g), g) for g in over},
        }
        unheld = first_unheld(rows, named)
        if unheld is None:
            return
        name, labels = unheld
        if name in fields:
            g, f, d = fields[name]
            msg = (
                f"`set({attribute!r}, ...)`: field {f!r} of {g!r} names {d!r} "
                f"labels {labels} its axis does not hold; `add` them first - a "
                f"tuple names labels its fields' axes hold"
            )
        elif name in over:
            msg = (
                f"no {name!r} tuple [{', '.join(labels)}]; `add_group` them "
                f"first - an attribute's value is set at tuples its groups hold"
            )
        else:
            msg = (
                f"no {name!r} label {labels}; `add` them first - an "
                f"attribute's value is set at labels its dims' axes hold"
            )
        raise KeyError(msg)

    def set(
        self,
        attribute: str,
        value: Any,
        **coords: Any,
    ) -> None:
        """Stage values of one attribute.

        Every coordinate is a keyword - `entity=`, `snapshot=`, a group's field
        such as `bus=` or a corridor's `from=` - since which exist is declared
        rather than fixed. A keyword holds one label, fixing that coordinate, or
        a list of labels the value is staged at each of. A coordinate no keyword
        names is NULL, which by the broadcast rule is every label of it. The
        call replaces every row of `attribute` staged earlier that its own
        rows cover.

        `value` takes five forms:

        - a scalar, staged at every combination of the listed labels;
        - a sequence, aligned positionally to the one list keyword;
        - a mapping or a one-dimensional labelled series, keyed by labels of
          the series' `index.name` where that names a coordinate, else of the
          one coordinate no keyword names;
        - a long frame supplying its own keys, which keywords may complete with
          single labels.

        An attribute's values are long rows, whatever its `dims`. A property -
        a category included - patches one column of existing axis rows: a
        mapping or series from label to value, a `dim=` keyword naming the
        labels, or a scalar for every label the axis has. A group's property
        takes a scalar, its fields as keywords selecting the tuples, none
        selecting every tuple.

        A property is named `subject.name` - `set("entity.carrier", ...)` -
        and a plain name is an attribute.

        A keyword naming no coordinate of `attribute` is a property of a dim it
        reaches - `ctype="Generator"`, or a list of values - and selects
        the labels of that dim matching it on the record as staged, the
        keywords combining as an intersection; `set` writes one row per label,
        and stores nothing of the filter, so a label added later is not reached.

        Raises
        ------
        KeyError
            On an undeclared attribute or keyword, a label that its dim's
            axis, as staged, does not hold, a tuple that its group, as
            staged, does not hold, a property's label no layer wrote, or a
            value of a property that takes its labels from a dim and is no
            label of it.
        ValueError
            On a keyword the attribute does not vary over, a sequence with no single list keyword to align to,
            keys whose coordinate is ambiguous, a label its dim does not
            list, a property keyword whose dim the attribute does not reach
            through exactly one coordinate, or that is given beside that
            coordinate's own keyword, a frame column the schema does not
            declare for `attribute`, or a frame column or label whose type
            does not fit its dtype, timestamps with a time zone among them.

        Notes
        -----
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        keyed = isinstance(value, Mapping) or _series_index(value) is not None
        if isinstance(self.schema.resolve(attribute), tuple):
            self._set_dotted(attribute, value, coords)
            return
        coords = self._select(attribute, coords)
        listed = {c: list(v) for c, v in coords.items() if _is_labels(v)}

        if is_frame(value) and not keyed:
            if listed:
                msg = (
                    f"`set({attribute!r}, <frame>)` supplies its own keys, so "
                    f"keywords {sorted(listed)} may give one label each, not a list"
                )
                raise ValueError(msg)
            call = f"`set({attribute!r}, <frame>)`"
            rel = as_relation(value, self.con)
            self.schema.check_group_columns(
                self.schema.groups_of(attribute), rel, carrier=call
            )
            self._validate_input(attribute, coords)
            table = self._ensure(_ATTRIBUTES, attribute)
            known = [
                *self._staged_coordinates(attribute),
                *_flat_coordinates(self.schema, attribute),
                "value",
            ]
            extra = sorted(set(rel.columns) - set(known))
            if extra:
                msg = (
                    f"{call} was given columns {extra} the schema does not "
                    f"declare for {attribute!r}; it takes {sorted(set(known))}"
                )
                raise ValueError(msg)
            rel = self.schema.require_takes(rel, call, attribute=attribute)
            self._insert_long(rel, table, attribute, set(rel.columns), coords)
            return

        self._validate_input(attribute, coords)

        if keyed:
            key = self._key_coordinate(attribute, value, coords)
            keys, values = normalise_value(value)
            dim = _flat_coordinates(self.schema, attribute)[key]
            self._require_typed(
                dim, cast("list[Any]", keys), f"`set({attribute!r}, ...)`"
            )
            column: dict[str, Sequence[Any]] = {key: cast("list[Any]", keys)}
        elif _is_labels(value):
            if len(listed) != 1:
                msg = (
                    f"`set({attribute!r}, <sequence>)` aligns to exactly one list "
                    f"keyword, and was given {sorted(listed) or 'none'}"
                )
                raise ValueError(msg)
            ((key, labels),) = listed.items()
            del listed[key]
            _, values = normalise_value(value, labels)
            column = {key: labels}
        else:
            values = [value]
            column = {}
        rel = self._values_relation(
            {**column, "value": values},
            {
                **{c: self._coordinate_type(attribute, c) for c in column},
                "value": None,
            },
        )
        for c, labels in listed.items():
            rel = rel.cross(
                self._values_relation(
                    {c: labels}, {c: self._coordinate_type(attribute, c)}
                )
            )
        fixed = {c: v for c, v in coords.items() if not _is_labels(v)}
        self._insert_long(
            rel,
            self._ensure(_ATTRIBUTES, attribute),
            attribute,
            set(rel.columns),
            fixed,
        )

    def _set_dotted(self, dotted: str, value: Any, coords: Mapping[str, Any]) -> None:
        """Stage the property `dotted` names, of a dim or a group."""
        subject, _, name = dotted.partition(".")
        if subject in self.schema.groups:
            self._set_group_property(subject, name, value, coords)
            return
        self._set_property(subject, name, value, self._select(dotted, coords))

    def _set_property(
        self, dim: str, name: str, value: Any, coords: Mapping[str, Any]
    ) -> None:
        """Stage property `name` of `dim`: a patch of one column of each label's row.

        Raises
        ------
        KeyError
            On a label the record does not resolve, or a value of a property
            that takes its labels from a dim and is no label of it.
        ValueError
            On a label its dim does not list or whose type does not fit
            its dtype, and as `_axis_values` does.
        """
        call = f"`set({f'{dim}.{name}'!r}, ...)`"
        values = self._axis_values(dim, name, value, coords)
        keyed = isinstance(values, Mapping)
        if keyed:
            self._require_typed(dim, list(values), call)
        data = {name: list(values.values()) if keyed else [values]}
        self.schema.require_listed(dim, data, call)
        self._require_live(dim, data, call)
        if keyed:
            self._require_existing(dim, list(values))
        self._stage_axis(dim, name, values)

    def _set_group_property(
        self, group: str, name: str, value: Any, coords: Mapping[str, Any]
    ) -> None:
        """Stage scalar property `name` of `group` on the tuples its field keywords select, all if none.

        Raises
        ------
        KeyError
            Where keywords are given and select no tuple, or on a value of a
            property that takes its labels from a dim and is no label of it.
        ValueError
            On a keyword naming no field of `group`, a value that is not a
            scalar, or a label its dim does not list or whose type does
            not fit its dtype.
        """
        call = f"set({f'{group}.{name}'!r}, ...)"
        key = self.schema.group_key(group)
        unknown = sorted(set(coords) - set(key))
        if unknown:
            msg = (
                f"`{call}` names {unknown}, but {name!r} is a property of the "
                f"group {group!r}, whose tuples its fields {list(key)} select"
            )
            raise ValueError(msg)
        # Not left to the cast: DuckDB would store a dict or list as its text.
        if isinstance(value, Mapping) or _is_labels(value) or is_frame(value):
            msg = (
                f"`{call}` takes one value for the tuples its keywords select, "
                f"{name!r} being a property of the group {group!r}"
            )
            raise ValueError(msg)
        self.schema.require_listed(group, {name: [value]}, f"`{call}`")
        self._require_live(group, {name: [value]}, f"`{call}`")
        tuples = self.groups.get(group)
        if tuples is None:
            msg = f"`{call}` finds no tuple of {group!r}; `add_group` them first"
            raise KeyError(msg)
        for field, given in coords.items():
            labels = list(given) if _is_labels(given) else [given]
            self._require_typed(
                self.schema.groups[group].over[field], labels, f"`{call}`"
            )
            tuples = tuples.filter(
                fn.struct_extract(col(group), lit(field)).isin(*map(lit, labels))
            )
        rel = tuples.project(col(group))
        if coords and rel.limit(1).fetchone() is None:
            scope = ", ".join(f"{c}={v!r}" for c, v in coords.items())
            msg = f"`{call}` selects no tuple of {group!r} at {scope}"
            raise KeyError(msg)
        dtype = self._column_type(name, group)
        edit = rel.project(col(group), lit(value).cast(dtype).alias(name))
        self._patch(group, group, name, edit)

    def _require_existing(
        self,
        dim: str,
        labels: Sequence[Any],
        why: str = "a value for a label no layer declares would resolve to nothing",
    ) -> None:
        """Reject labels of `dim` that its axis, as staged, does not hold, saying `why`.

        Raises
        ------
        KeyError
            Naming up to three such labels.
        """
        given = self._values_relation({dim: labels}, {dim: self._column_type(dim)})
        unheld = first_unheld(given, {dim: (col(dim), self.dims.get(dim), dim)})
        if unheld is not None:
            msg = f"no {dim!r} label {unheld[1]}; `add` them first - {why}"
            raise KeyError(msg)

    def _require_live(
        self, subject: str, data: Mapping[str, Sequence[Any]], call: str
    ) -> None:
        """Refuse a value of a property of `subject` that is no label of the dim it takes its labels from.

        The labels are those of the record as staged. NULL is no label and
        passes.

        Raises
        ------
        KeyError
            Naming the unknown labels.
        ValueError
            On a value whose type does not fit the dtype of the dim.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        named = self.schema.one_of_properties()
        for name, values in data.items():
            dim = named.get((subject, name))
            if dim is not None:
                self._require_typed(dim, values, call)
                self._require_existing(
                    dim,
                    values,
                    why=f"each value of `{subject}.{name}` must be a label of {dim!r}",
                )

    def _select(self, name: str, coords: Mapping[str, Any]) -> dict[str, Any]:
        """`coords` with each property keyword replaced by the labels it selects.

        Those of its dim storing one of the keyword's values, as staged, each
        under the one coordinate of attribute or dotted dim property `name`
        over the dim. A keyword naming a coordinate of `name` is that
        coordinate, whatever property shares its name; any other names a
        property of a dim `name` reaches, else of any dim.

        Raises
        ------
        ValueError
            Where the keyword is a property of several dims `name` reaches,
            `name` reaches the dim through no coordinate or several, the
            coordinate is also given as a keyword, a keyword is dotted, or a
            value is a label its dim does not list.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        call = f"set({name!r}, ...)"
        subject, dot, _ = name.partition(".")
        flat = {subject: subject} if dot else _flat_coordinates(self.schema, name)
        reached = set(flat.values())
        by_dim: dict[str, dict[str, list[Any]]] = {}
        rest: dict[str, Any] = {}
        for c, given in coords.items():
            if "." in c:
                msg = (
                    f"`{call}` selects by {c!r}; a keyword is bare, a "
                    f"coordinate or a property of a dim {name!r} reaches"
                )
                raise ValueError(msg)
            owners = [
                d for d in self.schema.dims if c in self.schema.properties.get(d, {})
            ]
            if c in flat or not owners:
                rest[c] = given
                continue
            among = [d for d in owners if d in reached]
            if len(among) > 1:
                msg = (
                    f"`{call}` selects by {c!r}, a property of each of {among}, "
                    f"which {name!r} all reaches; set it per dim instead"
                )
                raise ValueError(msg)
            values = list(given) if _is_labels(given) else [given]
            by_dim.setdefault((among or owners)[0], {})[c] = values
        for dim, wanted in by_dim.items():
            drawing = [c for c, d in flat.items() if d == dim]
            if len(drawing) != 1:
                msg = (
                    f"`{call}` selects by {sorted(wanted)}, properties of "
                    f"{dim!r}, but {name!r} reaches {dim!r} through "
                    f"{drawing or 'no coordinate'}, not exactly one"
                )
                raise ValueError(msg)
            (coordinate,) = drawing
            if coordinate in rest:
                msg = (
                    f"`{call}` names {coordinate}= and selects it by "
                    f"{sorted(wanted)} too; give its labels one way"
                )
                raise ValueError(msg)
            self.schema.require_listed(dim, wanted, f"`{call}`")
            rest[coordinate] = self._labels_matching(dim, wanted)
        return rest

    def _labels_matching(self, dim: str, wanted: Mapping[str, list[Any]]) -> list[Any]:
        """The labels of `dim`, as staged, storing one of `wanted`'s values of each property."""
        axis = self.properties.get(dim)
        if axis is None:
            return []
        return [r[0] for r in matching(axis, wanted).project(col(dim)).fetchall()]

    def _require_typed(self, dim: str, labels: Sequence[Any], call: str) -> None:
        """Refuse a label whose type does not fit the dtype of `dim` (`takes`).

        A label has the type DuckDB binds it as, so a string is refused for a
        `datetime` dim rather than parsed by the cast. One label per Python
        type is bound, so a list mixing types is checked once per type.

        Raises
        ------
        ValueError
            Naming `call`, the label, its type, `dim` and its dtype.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        _, declared, shown = cast(
            "tuple[str, DuckDBPyType, str]", self.schema.declared(dim)
        )
        for label in {type(n): n for n in labels if n is not None}.values():
            given = self._values_relation({dim: [label]}, {dim: None}).types[0]
            if not takes(declared, given):
                msg = (
                    f"{call}: label {label!r} has type {given}, which does not "
                    f"fit {dim!r} of dtype {shown}"
                )
                raise ValueError(msg)

    def _insert(
        self,
        rel: DuckDBPyRelation,
        table: str,
        supplied: Mapping[str, Expression],
        key: Sequence[str] | None = None,
        broadcast: Container[str] = (),
    ) -> None:
        """Project `rel` into `table`'s column order and insert it.

        `insert_into` is positional, and a staging table's order is its own -
        `ALTER TABLE` appends each extra column as `add` first sees one - so the
        projection is built from the table rather than from the caller. A column
        `supplied` does not name is taken from `rel` where it carries one, and is
        otherwise a NULL typed from the table, which is what spares the insert a
        coercion.

        `key` given, the rows `rel` names by it are deleted first, so the table
        holds one row per key and *is* the file it becomes - no fold on the way
        out (`_delete_covered`).
        `rel` must not read `table`, or the delete changes what the insert then
        reads: a caller deriving from the staged rows snapshots them first
        (`_insert_snapshot`).

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
        """
        staging = self.con.table(table)
        carried = set(rel.columns)

        def column(name: str, dtype: str) -> Expression:
            if name in supplied:
                return supplied[name].alias(name)
            if name in carried:
                return col(name)
            return lit(None).cast(dtype).alias(name)

        projected = rel.project(
            *(
                column(c, str(t))
                for c, t in zip(staging.columns, staging.types, strict=True)
            )
        )
        if key is not None:
            self._delete_covered(projected, table, key, broadcast)
        try:
            projected.insert_into(table)
        except duckdb.ConversionException as exc:
            # The column of a dim listing its labels reaches DuckDB as an
            # `ENUM`, so this is what rejects a label it does not list - as a
            # conversion to `UINT8`,
            # the enum's storage type, naming neither the dim nor its labels.
            # The source column is in the message, which is what makes it
            # restatable.
            match = re.search(r"casting from source column (\w+)", str(exc))
            if match is None:
                raise
            source = match.group(1)
            subject = next(
                (
                    k.removeprefix(_AXIS_PREFIX)
                    for (k, a), n in self._staged.items()
                    if n == table and a is None
                ),
                None,
            )
            dtype = self.schema.column_type(source, subject)
            listed = None if dtype is None else enum_labels(dtype)
            if listed is None:
                raise
            msg = (
                f"{source!r} names a label its dim does not list; "
                f"that dim lists {list(listed)}"
            )
            raise ValueError(msg) from exc

    def _insert_long(
        self,
        rel: DuckDBPyRelation,
        table: str,
        attribute: str,
        present: Container[str],
        dims: Mapping[str, Any],
    ) -> None:
        """Insert `rel` as one attribute's long rows.

        `present` names the coordinates `rel` already carries; the rest come from
        `dims` where the caller scoped them and are otherwise NULL - "every value
        of it" by the broadcast rule - typed as the schema declares them either
        way, so the insert needs no coercion. A group column `rel` lacks is
        packed from its key coordinates the same way, a field at a time.

        Only this attribute's own coordinates: the staging table is the shape of
        the file it becomes, so a dim it is not addressed by has no column to
        fill, and the projection is in that table's order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """

        def coordinate(d: str) -> Expression:
            if d in present:
                return col(d)
            if d in self.schema.groups_of(attribute):
                return _packed(self.schema, d, present, dims).alias(d)
            return lit(dims.get(d)).cast(self._column_type(d)).alias(d)

        shaped = rel.project(
            *(coordinate(d) for d in self._staged_coordinates(attribute)),
            lit(attribute).alias("attribute"),
            lit(None).cast(sqltypes.DOUBLE).alias("breakpoint"),
            col("value"),
        )
        self._require_held(shaped, attribute)
        self._insert(
            shaped,
            table,
            {},
            key=self._long_key(attribute),
            broadcast=self._staged_coordinates(attribute),
        )
        self._complete_owned_whole(attribute, table)

    def _delete_covered(
        self,
        rel: DuckDBPyRelation,
        table: str,
        key: Sequence[str],
        broadcast: Container[str],
    ) -> None:
        """Delete every row of `table` that a row of `rel` covers.

        A row covers another where each `key` column is equal, NULL-safe so a
        broadcast NULL replaces the same NULL rather than sitting beside it, or
        is a `broadcast` column the covering row leaves NULL, every label by the
        broadcast rule. `rel` is split by which `broadcast` columns it leaves
        NULL, and each part deletes on an equi-join over the columns it names: a
        match on "NULL or equal" cannot hash and would compare every pair.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        open_ = [c for c in key if c in broadcast]
        flags = ", ".join(f"{col(c)} IS NULL" for c in open_)
        patterns = (
            self.con.sql(f"SELECT DISTINCT {flags} FROM rel").fetchall()
            if open_
            else [()]
        )
        for pattern in patterns:
            nulls = {c for c, null in zip(open_, pattern, strict=True) if null}
            part = " AND ".join(
                f"({col('p', c)} IS NULL) = {c in nulls}" for c in open_
            )
            on = null_safe(table, "p", [c for c in key if c not in nulls])
            where = f"{part} AND {on}" if part else str(on)
            self.con.execute(f"DELETE FROM {table} USING rel p WHERE {where}")

    def _long_key(self, attribute: str) -> tuple[str, ...]:
        """The coordinate columns one long table is keyed on - what an edit replaces.

        The fold partitions on these too (`_collapsed_attribute` before this change),
        so replacing them per edit removes at least what it would have discarded.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        return tuple(c for c in self.schema.long_columns_for(attribute) if c != "value")

    def _complete_owned_whole(self, attribute: str, table: str) -> None:
        """Carry the base extent a non-partial axis obliges the staged rows to hold.

        Done as the rows are staged rather than at write, so the staging table
        *is* the layer: touching one snapshot of a series makes
        this layer the owner of that key's whole extent along the dim, and the
        untouched coordinates have to be carried or the written layer would
        report a loss.

        Scoped by the keys already staged, not by the attribute: the semi-join
        below reaches only the keys some edit named, so an entity this record
        never touched stays in the parent.

        Idempotent, which is what lets it run per insert instead of once: the
        anti-join drops any coordinate the table already holds, so a second edit
        to the same attribute carries only what the first did not. That
        anti-join is also the whole of what keeps a fill off an edited
        coordinate - it keys on `_long_key`, the same coordinate a later `set`
        deletes on (`_delete_covered`), so the fill it excludes and the row that
        replaces one are one key, and no ordering column is needed to rank them.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
        """
        whole = self._owned_whole(attribute)
        if not whole or attribute not in self._base.attributes:
            return

        # Intersected with the attribute's own columns: a key column its file
        # does not carry is absent from both sides rather than NULL in them,
        # and joining on it would fail to bind (`long_columns_for`).
        columns = set(self.schema.long_columns_for(attribute))
        scope = [
            c for c in self.schema.owner_map_key if c not in whole and c in columns
        ]
        present = [d for d in whole if d in columns]
        if not present:
            # No column for any whole-owned dim, so the file holds one row per
            # key and there is no extent to complete.
            return
        coordinate = self._long_key(attribute)
        staged = self.con.table(table)
        base = self._base.attributes[attribute]
        # A staged row leaving a whole-owned dim NULL already covers that dim's
        # whole extent by the broadcast rule, so its key has nothing left to
        # carry and a base row there would overlap it.
        broadcast = staged.filter(
            sql(" OR ".join(f"{col(d)} IS NULL" for d in present))
        )
        carried = (
            base.set_alias("b")
            # The keys some edit touched, minus those a broadcast already covers.
            .join(staged.set_alias("s"), null_safe("b", "s", scope), how="semi")
            .set_alias("b")
            .join(broadcast.set_alias("s"), null_safe("b", "s", scope), how="anti")
            # Then away the coordinates already staged, leaving the rest of the
            # extent the layer now owns whole and so must carry.
            .set_alias("b")
            .join(staged.set_alias("s"), null_safe("b", "s", coordinate), how="anti")
        )
        self._insert(carried, table, {})

    def _values_relation(
        self,
        columns: Mapping[str, Sequence[Any]],
        schema: Mapping[str, DuckDBPyType | None],
    ) -> DuckDBPyRelation:
        """A relation over `columns`, each an equal-length sequence of scalars.

        Column-wise, so nothing here is per-row: each column is one list
        parameter, unnested beside the others.

        A `None` in `schema` is inferred, as `set` leaves the `value` column.
        An `ENUM` is built as text and cast by the insert, which is what
        rejects a label it does not list with the column named (`_insert`).
        """
        params = [[_scalar(v) for v in values] for values in columns.values()]
        built = {
            name: sqltypes.VARCHAR if t is not None and t.id == "enum" else t
            for name, t in ((name, schema[name]) for name in columns)
        }
        unnested = ", ".join(
            f"unnest(${i}){'' if t is None else f'::{t}'} AS {col(name)}"
            for i, (name, t) in enumerate(built.items(), start=1)
        )
        return self.con.sql(f"SELECT {unnested}", params=params)

    def add(self, dim: str, labels: Sequence[Any], **columns: Any) -> None:
        """Stage labels of `dim`, each as one whole row of its axis file.

        `columns` are `dim`'s properties, each a scalar for every label or a
        sequence aligned to `labels`. A column no keyword names is NULL on
        these rows: `add` restates a label's row whole, replacing any this
        record staged for it. A label keeps its place in the axis, a label of
        the base removed here takes its base place back, and a new one is
        appended.

        Not a sequence of `set` calls: a label exists by virtue of its axis row,
        so adding one with no columns at all is an edit. An attribute's value,
        whatever its `dims`, goes through `set`, a group's row through
        `add_group`.

        Raises
        ------
        KeyError
            If the schema declares no `dim`, or on a value of a property
            that takes its labels from a dim and is no label of it.
        ValueError
            On an attribute, a column the axis file has no place for, a
            sequence of the wrong length, a label its dim does not list,
            `dim`'s own included, or a label whose type does not fit the dtype
            of `dim`.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        if dim not in self.schema.dimensions:
            msg = f"the schema declares no dim {dim!r}"
            raise KeyError(msg)
        call = f"add({dim!r}, ...)"
        kind = f"{_AXIS_PREFIX}{dim}"
        self._reject_undeclared(f"`{call}`", _axis_columns(self.schema, dim), columns)
        names = list(labels)
        self._require_typed(dim, names, f"`{call}`")
        data: dict[str, list[Any]] = {dim: names}
        for c, given in columns.items():
            data[c] = list(given) if _is_labels(given) else [given] * len(names)
            if len(data[c]) != len(names):
                msg = (
                    f"`{call}` has {len(data[c])} {c!r} values for {len(names)} labels"
                )
                raise ValueError(msg)
        self.schema.require_listed(dim, data, f"`{call}`")
        self._require_live(dim, data, f"`{call}`")
        rel = self._values_relation(data, {c: self._column_type(c, dim) for c in data})
        table = self._ensure(kind)
        self._insert_snapshot(
            self._positioned(rel, table, kind, dim), table, {}, key=(dim,)
        )

    def remove(self, dim: str, labels: Sequence[Any]) -> None:
        """Stage `dim`'s axis without `labels`.

        Need not enumerate what it deletes: the fold drops every value keyed
        by a removed label where `dim` is `partial`. No labels stage nothing.

        And every group with a field over `dim` without the tuples naming one
        of `labels`, read from the record as it currently resolves: a group
        attribute's value goes with its tuple.

        Raises
        ------
        ValueError
            On a label whose type does not fit the dtype of `dim`, or while a
            property that takes its labels from `dim` stores one of `labels` in the record
            as staged.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
        """
        if not labels:
            return
        self._require_typed(dim, labels, f"`remove({dim!r}, ...)`")
        self._require_unnamed(dim, labels)
        gone = self._values_relation({dim: list(labels)}, {dim: self._column_type(dim)})
        groups = self.groups
        for g in self.schema.group_columns:
            fields = [f for f, d in self.schema.groups[g].over.items() if d == dim]
            if not fields or g not in groups:
                continue
            naming = reduce(
                operator.or_,
                (
                    fn.struct_extract(col(g), lit(f)).isin(*map(lit, labels))
                    for f in fields
                ),
            )
            if groups[g].filter(naming).limit(1).fetchone() is None:
                continue
            table = self._ensure(g)
            for f in fields:
                self._delete(table, gone, dim, fn.struct_extract(col(table, g), lit(f)))
        table = self._ensure(f"{_AXIS_PREFIX}{dim}")
        self._delete(table, gone, dim, col(table, dim))

    def _delete(
        self, table: str, gone: DuckDBPyRelation, key: str, target: Expression
    ) -> None:
        """Delete the rows of `table` whose `target` is one of `gone`'s `key` values."""
        self.con.execute(
            f"DELETE FROM {table} USING gone g "
            f"WHERE {target} IS NOT DISTINCT FROM {col('g', key)}"
        )

    def _require_unnamed(self, dim: str, labels: Sequence[Any]) -> None:
        """Refuse removing `labels` of `dim` while a property that takes its labels from `dim` stores one of them.

        A tuple naming a label goes with it, but a property's row is its
        subject's, which outlives the label; clearing it is the caller's edit.

        Raises
        ------
        ValueError
            Naming up to three rows per such property.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        problems = []
        naming = [k for k, d in self.schema.one_of_properties().items() if d == dim]
        for subject, name in naming:
            rows = self.properties.get(subject)
            if rows is None:
                continue
            hits = (
                rows.filter(col(name).isin(*(lit(n) for n in labels)))
                .project(col(subject), col(name))
                .limit(3)
                .fetchall()
            )
            if hits:
                problems.append(f"`{subject}.{name}` names them at {hits}")
        if problems:
            msg = (
                f"`remove({dim!r}, ...)` is refused: {'; '.join(problems)}; "
                f"`set` those to another label or None first"
            )
            raise ValueError(msg)

    def _insert_snapshot(
        self,
        rel: DuckDBPyRelation,
        table: str,
        supplied: Mapping[str, Expression],
        key: Sequence[str],
    ) -> None:
        """`_insert`, for a `rel` read from `table` itself.

        Snapshotted into a table first: the replace-by-key delete would
        otherwise change what `rel` reads.
        """
        snapshot = f"{table}_snapshot"
        rel.create(snapshot)
        try:
            self._insert(self.con.table(snapshot), table, supplied, key=key)
        finally:
            self.con.execute(f"DROP TABLE {snapshot}")

    def _reject_undeclared(
        self, call: str, known: Collection[str], columns: Iterable[str]
    ) -> None:
        """Refuse a column outside `known`, the columns of the staged file, an attribute's included.

        A staging table is shaped from the schema, like the file it becomes, so
        a column outside that shape has no declared dtype; widening the table
        would have to guess it. A tool that grows a column changes its schema
        first.

        Raises
        ------
        ValueError
            Naming the columns, what the file does hold, and pointing an
            attribute at `set`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
        """
        extra = sorted(c for c in columns if c not in known)
        if not extra:
            return
        attributes = [c for c in extra if c in self.schema.attributes]
        hint = (
            f"; attributes {attributes} are long rows whatever their `dims` - "
            f"stage them with `set` once the row exists"
            if attributes
            else ""
        )
        msg = (
            f"{call} was given columns {extra} the schema does not declare "
            f"there; it holds {sorted(known)}. A column there is a property, so "
            f"declare it on its subject before writing it (https://energy-models.github.io/datarecord/design/schema/#versioning){hint}"
        )
        raise ValueError(msg)

    def add_group(self, group: str, frame: Any) -> None:
        """Stage rows of one declared group from a frame keyed by its struct column.

        The frame carries the group as one struct column named for it and
        the group's properties as plain columns - the shape of
        `groups/<group>.parquet`. The one path every group is added through, a
        record's `connection` group included.

        Raises
        ------
        KeyError
            On a tuple naming a label that its field's axis, as staged, does
            not hold.
        ValueError
            On a missing group column, an attribute column, a column whose type
            does not fit its dtype, and as `Schema.check_group_columns` does.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        call = f"`add_group({group!r}, ...)`"
        rel = as_relation(frame, self.con)
        self.schema.check_group_columns([group], rel, carrier=call)
        if group not in rel.columns:
            msg = f"{call} needs a {group!r} column"
            raise ValueError(msg)
        self._reject_undeclared(call, _group_columns(self.schema, group), rel.columns)
        rel = self.schema.require_takes(rel, call, subject=group)
        self._require_fields_held(rel, group, call)
        table = self._ensure(group)
        self._insert_snapshot(
            self._positioned(rel, table, group, group), table, {}, key=(group,)
        )

    def _require_fields_held(
        self, rel: DuckDBPyRelation, group: str, call: str
    ) -> None:
        """Refuse a tuple of `group` in `rel` naming a label the staged axis of its field's dim lacks.

        Raises
        ------
        KeyError
            Naming `call`, the field and up to three such labels.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        dims = self.dims
        over = self.schema.groups[group].over
        named = {
            f: (fn.struct_extract(col(group), lit(f)), dims.get(d), d)
            for f, d in over.items()
        }
        unheld = first_unheld(rel, named)
        if unheld is not None:
            field, labels = unheld
            msg = (
                f"{call}: field {field!r} names {over[field]!r} labels {labels} "
                f"its axis does not hold; `add` them first - a tuple names "
                f"labels its fields' axes hold"
            )
            raise KeyError(msg)

    def remove_group(self, group: str, keys: Sequence[tuple[Any, ...]]) -> None:
        """Stage `group` without the tuples `keys` name, each in `Schema.group_key` order.

        Each tuple is packed into the group's struct column.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        key = self.schema.group_key(group)
        over = self.schema.groups[group].over
        types = {c: self._column_type(d) for c, d in over.items()}
        flat = self._values_relation(
            {c: [k[i] for k in keys] for i, c in enumerate(key)}, types
        )
        gone = flat.project(_packed(self.schema, group, key, {}).alias(group))
        table = self._ensure(group)
        self._delete(table, gone, group, col(table, group))

    # -- edits / rollback (https://energy-models.github.io/datarecord/design/working-record/#edits) --------------------------

    def rollback(self) -> None:
        """Clear every staged row without writing.

        Notes
        -----
        - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
        """
        for name in self._staged.values():
            self.con.execute(f"DROP TABLE IF EXISTS {name}")
        self._staged.clear()

    def _collapsed_attribute(self, attribute: str) -> DuckDBPyRelation | None:
        """One attribute's staged rows, without those naming a label its staged axis lacks.

        The rows are already one per coordinate - an edit replaced its key rather
        than appending beside it (`_insert`) - so this is the table scan minus
        the rows naming a label, or a tuple of one of the attribute's groups,
        that the staged axis or group does not hold. `set` refuses a label the
        staged axis lacks, so a row is dropped here only where `remove` took
        its label after the `set`: the drop is that removal. The fold drops
        only the parent's rows of a label a layer removes, and a label added
        and removed in one staging area is in no restatement, so it is dead
        nowhere. A group attribute's row naming a removed label goes by its
        tuple, which `remove` dropped.

        None where nothing is staged for it, which is what says the base's rows
        stand alone.

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
        - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        rel = self._rows(_ATTRIBUTES, attribute)
        if rel is None:
            return None
        columns = rel.columns
        kinds = {
            **{d: f"{_AXIS_PREFIX}{d}" for d in self.schema.dims},
            **{g: g for g in self.schema.groups_of(attribute)},
        }
        for name, kind in kinds.items():
            held = self._rows(kind)
            if held is None or name not in columns:
                continue
            kept = held.project(col(name), lit(True).alias("_kept"))  # noqa: FBT003
            rel = (
                rel.set_alias("l")
                .join(kept.set_alias("k"), null_safe("l", "k", [name]), how="left")
                .filter(col("l", name).isnull() | col("k", "_kept").isnotnull())
                .project(*(col("l", c) for c in columns))
            )
        return rel

    # -- what edits write (https://energy-models.github.io/datarecord/design/working-record/#edits) ------------------------------------------

    def _staged_dims(self) -> tuple[str, ...]:
        """Which axes are staged whole, in declaration order.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [the schema](https://energy-models.github.io/datarecord/design/schema/)
        """
        return tuple(
            d for d in self.schema.dims if (f"{_AXIS_PREFIX}{d}", None) in self._staged
        )

    def _staged_groups(self) -> tuple[str, ...]:
        """Which groups are staged whole, in declaration order."""
        return tuple(g for g in self.schema.groups if (g, None) in self._staged)

    @property
    def edits(self) -> StagedSource:
        """The staged layer alone: the delta against the base, as a `LayerData`.

        What `rev.child(w.edits)` writes, where `rev` is the revision this
        record was built over - only the edits, the fold resolving the rest from
        the parent. The record itself is the flattened whole, base plus edits,
        which is what a standalone `ws.write(w, uri)` wants; `rev.child(w)`
        would write that whole as a child, reading back right but larger.

        Writing clears nothing: `rollback()` does, called or not.

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/design/working-record/#edits)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return cast("StagedSource", self.sources[-1])


def _column_type(
    schema: Schema, column: str, subject: str | None = None
) -> DuckDBPyType:
    """A staged column's declared type, for a typed NULL or literal.

    Every column a staging table has is a declared dim, a structural one or a
    property of `subject`, all of which the schema types - so a missing type
    is a disagreement between the table's shape and the schema, not a column
    to guess at.

    Raises
    ------
    ValueError
        If the schema declares no type for `column`.
    """
    dtype = schema.column_type(column, subject)
    if dtype is None:
        msg = (
            f"the schema declares no type for {column!r}, which a staged "
            f"row needs to fill (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
        )
        raise ValueError(msg)
    return dtype


def _group_columns(schema: Schema, group: str) -> dict[str, DuckDBPyType]:
    """Return the columns of `group`'s file: its struct column and its properties.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return {
        group: schema.group_type(group),
        **{p: _column_type(schema, p, group) for p in schema.properties.get(group, {})},
    }


def _flat_coordinates(schema: Schema, attribute: str) -> dict[str, str]:
    """`coordinates_of`, each group column spelled as its key coordinates, each to its dim.

    The names a caller addresses a row by - `bus=` for a connection attribute -
    where the row itself carries `connection`; a group field maps to the dim it
    draws on, a plain coordinate to itself.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    groups = schema.groups_of(attribute)
    flat: dict[str, str] = {}
    for coordinate in schema.coordinates_of(attribute):
        if coordinate in groups:
            over = schema.groups[coordinate].over
            flat.update({c: over[c] for c in schema.group_key(coordinate)})
        else:
            flat[coordinate] = coordinate
    return flat


def _packed(
    schema: Schema,
    group: str,
    present: Container[str],
    dims: Mapping[str, Any],
) -> Expression:
    """`group`'s struct column, packed from flat coordinates.

    A field is the column of that name where the relation carries one, else
    the caller's keyword. No field given at all is a NULL struct, every tuple.

    Raises
    ------
    ValueError
        Where some fields are given and others not: a struct NULL in some
        fields and not all matches no tuple.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    over = schema.groups[group].over
    missing = [c for c in over if c not in present and c not in dims]
    if len(missing) == len(over):
        return lit(None).cast(schema.group_type(group))
    if missing:
        msg = (
            f"{group!r} is keyed by {list(over)}, and {missing} was not given; "
            f"name it as a keyword, e.g. `{missing[0]}=...` (https://energy-models.github.io/datarecord/design/schema/#groups)"
        )
        raise ValueError(msg)
    return struct_of(
        {
            c: col(c) if c in present else lit(dims[c]).cast(_column_type(schema, d))
            for c, d in over.items()
        }
    ).cast(schema.group_type(group))


def _axis_columns(schema: Schema, dim: str) -> dict[str, DuckDBPyType]:
    """One axis's columns - the shape of `dims/{dim}.parquet`: its key and its properties.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return {
        dim: _column_type(schema, dim),
        **{p: _column_type(schema, p, dim) for p in schema.properties.get(dim, {})},
    }


def _scalar(value: Any) -> Any:
    """Return `value` as DuckDB binds a parameter, turning a numpy scalar into a Python one.

    numpy is only looked up among the imported modules, since a numpy scalar
    cannot exist without it.
    """
    numpy = sys.modules.get("numpy")
    if numpy is not None and isinstance(value, numpy.generic):
        return value.item()
    return value


_ATTRIBUTES = "attributes"
"""The staging kind of an attribute's long rows, one table per attribute."""

_POSITION = "_position"
"""The column a staged axis or group keeps its order in, which a table does not."""

# A staging kind, so it becomes part of a table name: no punctuation a SQL
# identifier would need quoting for, and no collision with a group's name,
# which `Schema` already rejects for colliding with a declared dim.
_AXIS_PREFIX = "axis_of_"
