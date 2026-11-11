# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Editing a record: staged edits, written as a layer by whoever writes them.

What `Record` (read-only) and a whole-record write do not cover: an edit costs
a row in a staging table rather than a rewrite, and nothing touches the record
until `edits` is written as a child.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/reference/editing/)
"""

from __future__ import annotations

import operator
import sys
from collections.abc import (
    Callable,
    Collection,
    Container,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from functools import cached_property, partial, reduce, wraps
from hashlib import sha256
from typing import TYPE_CHECKING, Any, ClassVar, Concatenate, cast
from uuid import UUID, uuid4

from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression, count_all, sqltypes
from duckdb import StarExpression as star
from duckdb.sqltypes import DuckDBPyType

from datarecord.duck import (
    STAGED,
    as_relation,
    create_table,
    empty_relation,
    ex_all,
    fn,
    insert_into,
    is_frame,
    join_on,
    nested,
    null_safe,
    refuse_open_transaction,
    scratch_table,
    transaction,
    union_all_by_name,
)
from datarecord.protocols import RecordLike
from datarecord.resolve.axes import refuse_unheld
from datarecord.resolve.fold import Fold
from datarecord.resolve.owner_map_rows import owner_map_rows
from datarecord.resolve.record import Record, _empty_long, _PerCursor
from datarecord.schema.categories import matching
from datarecord.schema.dtypes import takes
from datarecord.schema.schema import Schema, packed, unpacked
from datarecord.settle import has_pattern, null_patterns, uncovered, unsettled

if TYPE_CHECKING:
    from datarecord.store.store import Workspace


# -- value normalisation (https://energy-models.github.io/datarecord/reference/editing/#set) ----------------------------------------------


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


_Labels = Sequence[Any] | Sequence[Mapping[str, Any]] | DuckDBPyRelation


def _flat_labels(dim: str, labels: _Labels, call: str) -> list[Any]:
    """`labels` of the flat dim `dim` as a list.

    Raises
    ------
    TypeError
        On a frame, which gives the tuples of a multi-dim.
    """
    if is_frame(labels):
        msg = (
            f"{call} takes a sequence of labels of {dim!r}; a frame gives the "
            f"tuples of a multi-dim"
        )
        raise TypeError(msg)
    return list(cast("Sequence[Any]", labels))


def _field_in(multi: str, field: str, labels: Sequence[Any]) -> Expression:
    """Whether field `field` of the multi-dim `multi` holds one of `labels`, none matching no tuple.

    DuckDB's `isin` refuses an empty list, which a property keyword selecting
    no label yields.
    """
    if not labels:
        return lit(False)
    return col(multi)[field].isin(*map(lit, labels))


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
    labels
        The label each value belongs to, or None for a scalar.
    values
        One value per label, or the scalar alone.

    Raises
    ------
    ValueError
        If a sequence has no `labels`, or a different length.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
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
    in which an edit replaces the rows it covers rather than appending to them
    (`_insert`), so the table already holds one row per label or coordinate and
    is what a write would persist. An axis
    is staged whole, and handed over in position order.

    Unfrozen, which is the whole of what distinguishes it: a `set` changes these
    rows under a reader, so the fold must stay a relation past this point rather
    than materialise. Nothing here needs invalidating in exchange - a relation
    over a staging table reads whatever the table holds when it is collected.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """

    record: WorkingRecord
    layer_id: UUID
    frozen: bool = False

    @property
    def schema(self) -> Schema:
        """This record's schema - the `LayerData` a write reads for one.

        A staging area is not a `LayerSource` a fold folds under a schema
        someone else supplies; it is also what `store.commit(w)` writes
        directly, where it needs its own.
        """
        return self.record._base.schema

    @property
    def base(self) -> UUID:
        """The revision these edits are a delta against: the base's `revision_id`."""
        return self.record._base.revision_id

    @property
    def workspace(self) -> Workspace:
        """Its record's workspace, whose database holds the staging tables."""
        return self.record.workspace

    def materialised(self) -> Fold | None:
        """A staging area has no `resolved/` files: it is never a fold's base."""
        return None

    def axes(self) -> set[str]:
        """The dims whose staged axis differs from the base's, as rows in position order.

        What a write of `edits` restates. The fold reads every staged axis
        regardless, a staging area restating each from the start.
        """
        return set(self.record._staged_dims())

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """One axis as this layer would write it: every label, with its properties, in order.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        """
        return self.record._restatement(f"{_AXIS_PREFIX}{dim}")

    def attributes(self) -> set[str]:
        """Which attributes have staged rows, each table existing from the start."""
        return set(self.record._staged_attributes())

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_attribute(name)

    def owners(self, name: str) -> DuckDBPyRelation:
        """One attribute's owner-map rows, kept in a table each edit refreshes (`_refresh_owners`)."""
        return self.record.con.table(self.record._staged[_OWNERS, name])

    def all_attributes(self) -> DuckDBPyRelation | None:
        """Every staged attribute, unioned by name and unprojected.

        By name because each table carries a column per dim of its attribute, as
        its file does, which is what lets `fold_layer` pad both the same way.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        arms = [
            rel
            for name in self.record._staged_attributes()
            if (rel := self.attribute(name)) is not None
        ]
        if not arms:
            return None
        return union_all_by_name(arms)


class _Base(Record):
    """A `WorkingRecord`'s own copy of a `Record` base, so closing the caller's leaves its edits working.

    It holds nothing: the `WorkingRecord` holds the folds it reads, and hands
    it its own `_release`, so the copy is open exactly while the
    `WorkingRecord` is and leaves no finaliser of its own.

    Notes
    -----
    - [staging](https://energy-models.github.io/datarecord/reference/editing/#transactions-and-staging)
    """

    def __post_init__(self) -> None:
        return None


class _StagedPerCursor(_PerCursor):
    """What a `WorkingRecord` keeps per thread besides a `Record`'s."""

    def __init__(self) -> None:
        super().__init__()
        self.labels: dict[str, DuckDBPyRelation] = {}


def _atomic[**P](
    edit: Callable[Concatenate[WorkingRecord, P], None],
) -> Callable[Concatenate[WorkingRecord, P], None]:
    """Run an edit in one transaction on the record's cursor, rolled back where it raises.

    So a refused edit leaves the staging tables as they were, and a reader on
    another cursor sees each edit whole or not at all.

    Raises
    ------
    TransactionException
        Where the cursor already holds an open transaction, which DuckDB cannot
        nest and an edit could not roll back alone.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """

    @wraps(edit)
    def run(self: WorkingRecord, *args: P.args, **kwargs: P.kwargs) -> None:
        self._require_editable()
        with transaction(
            self.con,
            edit.__name__,
            "https://energy-models.github.io/datarecord/reference/editing/#edits",
        ) as on_rollback:
            on_rollback(
                partial(object.__setattr__, self, "_touched", set(self._touched))
            )
            self._dirty.clear()
            edit(self, *args, **kwargs)
            self._refresh_owners()

    return run


# -- the DuckDB-backed implementation (https://energy-models.github.io/datarecord/reference/editing/#transactions-and-staging) ---------------------------------


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

    Raises
    ------
    ValueError
        On a read, an edit or `edits` once the record is `closed`, and on an
        edit or `edits` once a `WorkingRecord` it is staged over is.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/reference/editing/)
    - [staging](https://energy-models.github.io/datarecord/reference/editing/#transactions-and-staging)
    - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
    """

    #: This record's one identity, which is the staged layer's: the fold stamps
    #: it as `layer_uuid` and dispatches a winning row back through the source
    #: carrying it. Synthetic, the staged layer having no revision until
    #: `edits` is written as one, and it names the staging schema too - so two
    #: `WorkingRecord`s on one connection collide in neither.
    _layer_id: UUID
    #: The schema `_staged."<layer id>"` of the workspace's database every
    #: staging table is kept in, quoted in full.
    _staging_schema: str
    #: The record these edits are made over: its sources are this record's
    #: but the staged one, and its `revision_id` is what the edits are a delta
    #: against. A `WorkingRecord` base is the caller's, any other a `_Base`.
    _base: Record
    #: Keyed by `(kind, attribute)`, the attribute being None for an axis.
    #: Attributes stage one table per attribute because that is the
    #: file it stands for: a column per dim of its attribute and one `value`
    #: column at the attribute's own type.
    _staged: dict[tuple[str, str | None], str]
    #: The `_staged` keys an edit wrote to since the start or `rollback()`.
    _touched: set[tuple[str, str | None]]
    #: The `_staged` keys the running edit wrote to.
    _dirty: set[tuple[str, str | None]]
    _closed_message: ClassVar[str] = (
        "this `WorkingRecord` is closed and its staging tables released; "
        "stage edits on a new one over the same base"
    )

    def __init__(self, base: RecordLike) -> None:
        """Stage edits over `base`, which must be a `Record`.

        Raises
        ------
        TypeError
            For any other `RecordLike`, naming `ws.write(source, uri)`, which
            gives one.
        duckdb.TransactionException
            Where the cursor holds an open transaction, whose staging tables no
            other cursor could see to drop.

        Notes
        -----
        - [WorkingRecord](https://energy-models.github.io/datarecord/reference/editing/)
        """
        if not isinstance(base, Record):
            msg = (
                f"a `WorkingRecord` folds its staged rows over a `Record`'s layers, "
                f"and a {type(base).__name__} has none: write it first, "
                f"`WorkingRecord(ws.write(source, uri))`"
            )
            raise TypeError(msg)
        refuse_open_transaction(
            base.con,
            "WorkingRecord",
            "https://energy-models.github.io/datarecord/reference/editing/#transactions-and-staging",
        )
        # `object.__setattr__` throughout: the base is a frozen dataclass, and
        # these are set before `super().__init__` because the `StagedSource`
        # below reads them off `self`.
        object.__setattr__(self, "_layer_id", uuid4())
        object.__setattr__(self, "_staged", {})
        object.__setattr__(self, "_touched", set())
        object.__setattr__(self, "_dirty", set())
        if not isinstance(base, WorkingRecord):
            base = _Base(base.revision_id, base.sources, base.workspace)
        object.__setattr__(self, "_base", base)
        object.__setattr__(
            self,
            "_staging_schema",
            f"{col(base.workspace.name)}.{nested(STAGED, self._layer_id)}",
        )
        base.con.execute(f"CREATE SCHEMA {self._staging_schema}")
        super().__init__(
            base.revision_id,
            [*base.sources, StagedSource(self, self._layer_id)],
            base.workspace,
        )
        if isinstance(base, _Base):
            object.__setattr__(base, "_release", self._release)
            object.__setattr__(base, "_holder", self._holder)
        for kind, attribute in self._kinds():
            name = self._table(kind, attribute)
            create_table(self.con, name, self._shape(kind, attribute))
            self._staged[kind, attribute] = name
            self._fill(kind, attribute)
        shape = self._table(_OWNERS)
        empty = empty_relation(self.con, attribute=sqltypes.VARCHAR)
        create_table(
            self.con, shape, owner_map_rows(empty, self.schema, self._layer_id, ())
        )
        for attribute in self.schema.attributes:
            name = self._table(_OWNERS, attribute)
            self.con.execute(f"CREATE TABLE {name} AS FROM {shape}")
            self._staged[_OWNERS, attribute] = name
        self.con.execute(f"DROP TABLE {shape}")
        self._frozen_owners  # noqa: B018 - built outside any edit's transaction

    def _require_editable(self) -> None:
        """Refuse an edit or a write where this record or its base is closed: both read the base.

        Raises
        ------
        ValueError
            Once `close()` ran here or on a `WorkingRecord` base.
        """
        self._require_open()
        if self._base.closed:
            msg = (
                "the `WorkingRecord` this one is staged over is closed, and an "
                "edit reads it; stage over an open one"
            )
            raise ValueError(msg)

    @property
    def _kept_schemas(self) -> frozenset[str]:
        return self._base._kept_schemas | {nested(STAGED, self._layer_id)}

    def _kinds(self) -> list[tuple[str, str | None]]:
        """Every staging table's `(kind, attribute)`: an axis per dim but those with `one_of`, a table per attribute."""
        return [
            *(
                (f"{_AXIS_PREFIX}{d}", None)
                for d in self.schema.dimensions
                if d not in self.schema.one_of_dims
            ),
            *((_ATTRIBUTES, a) for a in self.schema.attributes),
        ]

    def _fill(self, kind: str, attribute: str | None) -> None:
        """Fill a staging table as it starts: an axis with the base's, in base order; an attribute empty."""
        if attribute is not None:
            return
        numbered = self._base_rows(kind).project(
            star(), fn.row_number().over().alias(_POSITION)
        )
        self._insert(numbered, self._staged[kind, attribute], {})

    # -- staging tables -----------------------------------------------------

    def _table(self, kind: str, attribute: str | None = None) -> str:
        """A staging table's name in the record's schema, unique per attribute.

        The attribute is hashed rather than spelled: it is a caller's string,
        and a table name is the one thing here that cannot be an expression, so
        it would be an injection and a quoting problem at once.
        """
        if kind.startswith(_AXIS_PREFIX):
            # Hashed for the same reason the attribute is: a dim name is a
            # caller's string, and this becomes an identifier.
            digest = sha256(kind[len(_AXIS_PREFIX) :].encode()).hexdigest()[:16]
            return f"{self._staging_schema}.axis_{digest}"
        if attribute is None:
            return f"{self._staging_schema}.{kind}"
        digest = sha256(attribute.encode()).hexdigest()[:16]
        return f"{self._staging_schema}.{kind}_{digest}"

    def _ensure(self, kind: str, attribute: str | None = None) -> str:
        """The staging table for `kind`, marked as touched by an edit.

        `kind` is `_ATTRIBUTES` with an `attribute`, or an axis (`_AXIS_PREFIX`
        and its dim). Every table exists from the start (`_kinds`); the mark
        is what `_staged_dims` and `_staged_attributes` look at.
        """
        self._touched.add((kind, attribute))
        self._dirty.add((kind, attribute))
        return self._staged[kind, attribute]

    def _base_rows(self, kind: str) -> DuckDBPyRelation:
        """The base's resolved axis `kind` stands for, with its properties."""
        dim = kind[len(_AXIS_PREFIX) :]
        rows = (
            self._base.properties if dim in self.schema.properties else self._base.dims
        )
        return rows[dim]

    def _restatement(self, kind: str) -> DuckDBPyRelation | None:
        """The staged axis `kind` stands for, in position order; `None` for a dim with `one_of`."""
        name = self._staged.get((kind, None))
        if name is None:
            return None
        return self.con.sql(
            f"SELECT * EXCLUDE ({_POSITION}) FROM {name} ORDER BY {_POSITION}"
        )

    def _positioned(
        self, rel: DuckDBPyRelation, table: str, kind: str, dim: str
    ) -> DuckDBPyRelation:
        """`rel` with each row's position in the staged axis `table`.

        A label keeps its staged position, a label of the base takes its base
        position back, and any other comes after every position either holds,
        in `rel`'s order.
        """
        staged = self.con.table(table)
        base = self._base_rows(kind)
        start = max(
            staged.aggregate([coalesce(col(_POSITION).max(), lit(0))]).fetchall()[0][0],
            base.aggregate([count_all()]).fetchall()[0][0],
        )
        numbered = rel.project(star(), fn.row_number().over().alias("_n"))
        held = [
            staged.project(col(dim), col(_POSITION)),
            base.project(col(dim), fn.row_number().over().alias(_POSITION)),
        ]
        joined = numbered.set_alias("r")
        for i, positions in enumerate(held):
            alias = f"h{i}"
            joined = joined.join(
                positions.set_alias(alias), null_safe("r", alias, [dim]), how="left"
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
            return _empty_long(self.schema, attribute, self.con)
        columns = _axis_columns(self.schema, kind[len(_AXIS_PREFIX) :])
        return empty_relation(self.con, **columns, **{_POSITION: sqltypes.BIGINT})

    def _refresh_owners(self) -> None:
        """Recompute the owner rows of each attribute the running edit may have changed.

        An attribute's rows read through its staged axes (`_collapsed_attribute`),
        so an edit of an axis it is over changes them as well as one of its table.
        """
        axes = {k.removeprefix(_AXIS_PREFIX) for k, a in self._dirty if a is None}
        for attribute in self.schema.attributes:
            key = (_ATTRIBUTES, attribute)
            if key not in self._touched:
                continue
            columns = set(self.schema.long_columns_for(attribute))
            if key not in self._dirty and not axes & columns:
                continue
            table = self._staged[_OWNERS, attribute]
            self.con.execute(f"DELETE FROM {table}")
            rows = self._collapsed_attribute(attribute)
            insert_into(
                self.con,
                table,
                owner_map_rows(rows, self.schema, self._layer_id, (attribute,)),
            )

    def _rows(self, kind: str, attribute: str | None = None) -> DuckDBPyRelation | None:
        name = self._staged.get((kind, attribute))
        return None if name is None else self.con.table(name)

    def _staged_attributes(self) -> tuple[str, ...]:
        """Which attributes have staged rows that survive a `remove`, in declaration order.

        Only a table an edit touched can hold one.
        """
        return tuple(
            a
            for a in self.schema.attributes
            if (_ATTRIBUTES, a) in self._touched
            and self._collapsed_attribute(a).limit(1).fetchall()
        )

    def _column_type(self, column: str, subject: str | None = None) -> DuckDBPyType:
        return _column_type(self.schema, column, subject)

    # -- Record, one fold deeper (https://energy-models.github.io/datarecord/reference/editing/) --------------------------------

    # `schema`, `dims`, `properties`, `attributes` and `flags` are
    # inherited from `Record` unchanged, which is the property this design
    # exists to have: a staged edit is read by the same fold that reads a
    # committed layer, so there is no second overlay to keep in step.

    def _outside_partial(self, attribute: str) -> tuple[str, ...]:
        """`AttributeSpec.dims` minus the owner map's index - the value axes outside `partial`.

        The complement of `owned_per`: along a dim the attribute varies over
        but that is not in `partial_dims` (a `timestep` or `connection`
        outside `partial`), a patch to one value restates the attribute's
        whole extent along it.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
        """
        spec = self.schema.attributes.get(attribute)
        outside = (
            frozenset() if spec is None else spec.dims - set(self.schema.partial_dims)
        )
        return tuple(d for d in self.schema.dimensions if d in outside)

    # -- edits (https://energy-models.github.io/datarecord/reference/editing/#set, https://energy-models.github.io/datarecord/reference/editing/#add-and-remove) ----------------------------------------

    def _labelled_field(
        self, attribute: str, labelled: Any, given: Container[str]
    ) -> str:
        """The field of `attribute` whose labels a mapping's keys or a series' index are.

        The series' own `index.name` where it names a field of this attribute,
        else the one field the call gives no labels for. Never read off the labels:
        one dim's label may be a string just like another's, so a membership
        test would make one call mean different things in two records.

        An `index.name` naming no field is ignored rather than rejected; it may
        be `None`, or a pandas artefact like `"index"`.

        Parameters
        ----------
        given
            The fields the call already gives labels for, by keyword or
            through the tuples a property keyword selects.

        Raises
        ------
        ValueError
            Where the index names a field the call also gives labels for, or nothing says
            which of several open fields is meant.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        fields = self.schema.fields_of(attribute)
        named = _series_index_name(labelled)
        if named in fields:
            if named in given:
                msg = (
                    f"`set({attribute!r}, ...)` got a series indexed by {named!r} "
                    f"and the keyword {named}= too; give its labels one way"
                )
                raise ValueError(msg)
            return cast("str", named)
        open_ = [f for f in fields if f not in given]
        if len(open_) != 1:
            msg = (
                f"`set({attribute!r}, ...)` cannot tell which of {open_} the "
                f"mapping's keys or the series' index are labels of; name the "
                f"series' index after one, or give the others as keywords"
            )
            raise ValueError(msg)
        return open_[0]

    def _label_type(self, attribute: str, field: str) -> DuckDBPyType:
        """The declared type of the labels of one field of `attribute`: its dim's type."""
        dim = self.schema.fields_of(attribute).get(field, field)
        return self._column_type(dim)

    def _axis_values(
        self, dim: str, name: str, value: Any, labels: Mapping[str, Any]
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
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        unknown = sorted(set(labels) - {dim})
        if unknown:
            msg = (
                f"`set({f'{dim}.{name}'!r}, ...)` names {unknown}, but {name!r} is a "
                f"property of {dim!r} - a column of dims/{dim}.parquet, set per label "
                f"of {dim!r} alone"
            )
            raise ValueError(msg)
        if not isinstance(value, Mapping) and (
            dim in labels or _series_index(value) is not None
        ):
            given = labels.get(dim)
            picked = (
                None if given is None else list(given) if _is_labels(given) else [given]
            )
            at, values = normalise_value(value, picked)
            if at is None:
                at, values = cast("list[Any]", picked), values * len(picked or ())
            value = dict(zip(at, values, strict=True))
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
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
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
        edit = self._columns_relation(
            {dim: list(value), name: list(value.values())},
            {dim: self._column_type(dim), name: self._column_type(name, dim)},
        )
        self._patch(f"{_AXIS_PREFIX}{dim}", dim, name, edit)

    def _patch(self, kind: str, column: str, name: str, edit: DuckDBPyRelation) -> None:
        """Set property `name` on each label or tuple `edit` names, in the staged `kind`.

        `column` is the dim's column a row is matched on: a flat dim's, or a
        multi-dim's struct column. The first edit stages the base's axis whole, so the
        row is there to update, its other properties with it.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        table = self._ensure(kind)
        # Raw SQL because `UPDATE ... FROM` is a join-update, which the relational
        # `update` cannot express (https://duckdb.org/docs/stable/clients/python/relational_api).
        _dr_edit = edit
        self.con.execute(
            f"UPDATE {table} AS t SET {col(name)} = {col('e', name)} "
            f"FROM _dr_edit e "
            f"WHERE {col('t', column)} IS NOT DISTINCT FROM {col('e', column)}"
        )

    def _validate_dims(self, dims: Mapping[str, Any]) -> None:
        """The dim vocabulary.

        A multi-dim's field is a keyword too, `from=` for a corridor,
        though it names no dim.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
        """
        multi = sorted(set(dims) & set(self.schema.multi_dims))
        if multi:
            over = list(self.schema.over(multi[0]))
            msg = (
                f"{multi} are multi-dims, addressed by their fields as "
                f"keywords, e.g. {over} for {multi[0]!r}; `add` and `remove` "
                f"change their tuples"
            )
            raise KeyError(msg)
        fields = {c for m in self.schema.multi_dims for c in self.schema.over(m)}
        unknown = sorted(set(dims) - set(self.schema.flat_dims) - fields)
        if unknown:
            msg = f"the schema declares no dims {unknown}"
            raise KeyError(msg)

    def _validate_input(self, attribute: str, labels: Mapping[str, Any]) -> None:
        """An input's name, its keywords, and the labels they give.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
        """
        self._validate_dims(labels)
        spec = self.schema.attributes.get(attribute)
        if spec is None:
            msg = f"the schema declares no attribute {attribute!r}"
            raise KeyError(msg)
        fields = self.schema.fields_of(attribute)
        outside = sorted(set(labels) - spec.dims - set(fields))
        if outside:
            msg = (
                f"{attribute!r} does not vary over {outside}; "
                f"it varies over {sorted(spec.dims) or 'nothing'}"
            )
            raise ValueError(msg)
        for c, given in labels.items():
            if c in fields:
                picked = list(given) if _is_labels(given) else [given]
                self._require_typed(fields[c], picked, f"`set({attribute!r}, ...)`")

    def _held(self, name: str) -> DuckDBPyRelation | None:
        """Return the staged axis `name`, `None` where there is none."""
        return self.dims.get(name)

    @_atomic
    def set(
        self,
        attribute: str,
        value: Any,
        **labels: Any,
    ) -> None:
        """Stage values of one attribute.

        Each flat dim and each field of a multi-dim is a keyword, such as
        `entity=`, `snapshot=`, `bus=` or a corridor's `from=`, since which
        exist is declared rather than fixed. A keyword holds one label, fixing
        that dim, or a list of labels the value is staged at each of. A dim no
        keyword names is NULL, which by the broadcast rule is every label of
        it. The call replaces every row of `attribute` staged earlier that its
        own rows cover.

        `value` takes five forms:

        - a scalar, staged at every combination of the listed labels;
        - a sequence, aligned positionally to the one list keyword;
        - a mapping or a one-dimensional labelled series, keyed by labels of
          the series' `index.name` where that names a keyword, else of the
          one keyword not given;
        - a long frame supplying its own labels, which keywords may complete with
          single labels.

        An attribute's values are long rows, whatever its `dims`. A property -
        a category included - patches one column of existing axis rows: a
        mapping or series from label to value, a `dim=` keyword naming the
        labels, or a scalar for every label the axis has. A multi-dim's
        property takes a scalar, its fields as keywords selecting the
        tuples, none selecting every tuple. A field is part of the label,
        which `add` and `remove` change, and is refused here.

        A property is named `subject.name` - `set("entity.carrier", ...)` -
        and a plain name is an attribute.

        A keyword naming no dim or field of `attribute` is a property of a dim it
        reaches - `ctype="Generator"`, or a list of values - and selects
        the labels of that dim matching it on the record as staged, the
        keywords combining as an intersection; `set` writes one row per label,
        and stores nothing of the filter, so a label added later is not reached.
        A property of a multi-dim `attribute` is over - `role="output"` -
        selects its tuples the same way, narrowed by any keyword naming one of
        its fields. On a multi-dim's property, the keywords select its
        tuples as on an attribute over it: `set("connection.note", v,
        role="output")` patches the output connections, and `ctype="Link"`
        the connections of links.

        Raises
        ------
        KeyError
            On an undeclared attribute or keyword, a label that its dim's
            axis, as staged, does not hold, a tuple that its multi-dim, as
            staged, does not hold, a property's label no layer wrote, or a
            value of a property that refers to a dim and is no
            label of it.
        ValueError
            On a field named as `subject.name`, a keyword the attribute
            does not vary over, a sequence with no single list keyword to align to,
            keys whose keyword is ambiguous, a label its dim does not
            list, a property keyword whose dim the attribute does not reach
            through exactly one keyword, or that is given beside that
            keyword, a frame column the schema does not
            declare for `attribute`, or a frame column or label whose type
            does not fit its dtype, timestamps with a time zone among them.

        Notes
        -----
        - [the shape of an edit](https://energy-models.github.io/datarecord/reference/editing/#the-shape-of-an-edit)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
        """
        keyed = isinstance(value, Mapping) or _series_index(value) is not None
        if isinstance(self.schema.resolve(attribute), tuple):
            self._set_dotted(attribute, value, labels)
            return
        labels, tuples = self._select(attribute, labels)
        listed = {c: list(v) for c, v in labels.items() if _is_labels(v)}

        if is_frame(value) and not keyed:
            if listed or tuples:
                msg = (
                    f"`set({attribute!r}, <frame>)` supplies its own labels, so "
                    f"keywords {sorted(listed)} may give one label each, not a "
                    f"list, and none may select the tuples of {sorted(tuples)}"
                )
                raise ValueError(msg)
            call = f"`set({attribute!r}, <frame>)`"
            rel = as_relation(value, self.con)
            multi_dims = self.schema.multi_dims_of(attribute)
            self.schema.check_multi_dim_columns(multi_dims, rel, carrier=call)
            self.schema.refuse_partial_tuples(multi_dims, rel, carrier=call)
            self._validate_input(attribute, labels)
            table = self._ensure(_ATTRIBUTES, attribute)
            known = [
                *self.schema.dims_of(attribute),
                *self.schema.fields_of(attribute),
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
            self._insert_long(rel, table, attribute, set(rel.columns), labels)
            return

        self._validate_input(attribute, labels)

        if keyed:
            selected = [c for m in tuples for c in self.schema.over(m)]
            field = self._labelled_field(attribute, value, [*labels, *selected])
            picked, values = normalise_value(value)
            dim = self.schema.fields_of(attribute)[field]
            self._require_typed(
                dim, cast("list[Any]", picked), f"`set({attribute!r}, ...)`"
            )
            column: dict[str, Sequence[Any]] = {field: cast("list[Any]", picked)}
        elif _is_labels(value):
            if len(listed) != 1:
                msg = (
                    f"`set({attribute!r}, <sequence>)` aligns to exactly one list "
                    f"keyword, and was given {sorted(listed) or 'none'}"
                )
                raise ValueError(msg)
            ((field, picked),) = listed.items()
            del listed[field]
            _, values = normalise_value(value, picked)
            column = {field: picked}
        else:
            values = [value]
            column = {}
        rel = self._columns_relation(
            {**column, "value": values},
            {
                **{c: self._label_type(attribute, c) for c in column},
                "value": None,
            },
        )
        for c, picked in listed.items():
            rel = rel.cross(
                self._columns_relation({c: picked}, {c: self._label_type(attribute, c)})
            )
        for chosen in tuples.values():
            rel = rel.cross(chosen)
        fixed = {c: v for c, v in labels.items() if not _is_labels(v)}
        self._insert_long(
            rel,
            self._ensure(_ATTRIBUTES, attribute),
            attribute,
            set(rel.columns),
            fixed,
        )

    def _set_dotted(self, dotted: str, value: Any, labels: Mapping[str, Any]) -> None:
        """Stage the property `dotted` names.

        Raises
        ------
        ValueError
            Where `dotted` names a field of a multi-dim.
        """
        subject, _, name = dotted.partition(".")
        if name in self.schema.over(subject):
            msg = (
                f"`set({dotted!r}, ...)` names a field of the multi-dim "
                f"{subject!r}, which `set` cannot change; `remove` the tuples and "
                f"`add` them with the new label instead"
            )
            raise ValueError(msg)
        rest, tuples = self._select(dotted, labels)
        if subject in self.schema.multi_dims:
            self._set_multi_dim_property(
                subject, name, value, rest, tuples.get(subject)
            )
            return
        self._set_property(subject, name, value, rest)

    def _set_property(
        self, dim: str, name: str, value: Any, labels: Mapping[str, Any]
    ) -> None:
        """Stage property `name` of `dim`: a patch of one column of each label's row.

        Raises
        ------
        KeyError
            On a label the record does not resolve, or a value of a property
            that refers to a dim and is no label of it.
        ValueError
            On a label its dim does not list or whose type does not fit
            its dtype, and as `_axis_values` does.
        """
        call = f"`set({f'{dim}.{name}'!r}, ...)`"
        values = self._axis_values(dim, name, value, labels)
        keyed = isinstance(values, Mapping)
        if keyed:
            self._require_typed(dim, list(values), call)
        data = {name: list(values.values()) if keyed else [values]}
        self.schema.require_one_of(dim, data, call)
        self._require_live(dim, data, call)
        if keyed:
            self._require_existing(dim, dim, list(values), call)
        self._stage_axis(dim, name, values)

    def _set_multi_dim_property(
        self,
        multi: str,
        name: str,
        value: Any,
        labels: Mapping[str, Any],
        chosen: DuckDBPyRelation | None,
    ) -> None:
        """Stage scalar property `name` of `multi` on the tuples its field keywords select, all if none.

        `chosen` are the tuples a category keyword selected, as `_select`
        returns them, which the field keywords narrow; None where no
        category keyword was given.

        Raises
        ------
        KeyError
            Where keywords are given and select no tuple, or on a value of a
            property that refers to a dim and is no label of it.
        ValueError
            On a keyword naming no field of `multi`, a value that is not a
            scalar, or a label its dim does not list or whose type does
            not fit its dtype.
        """
        call = f"set({f'{multi}.{name}'!r}, ...)"
        over = self.schema.over(multi)
        unknown = sorted(set(labels) - set(over))
        if unknown:
            msg = (
                f"`{call}` names {unknown}, but {name!r} is a property of the "
                f"multi-dim {multi!r}, whose tuples its fields {list(over)} "
                f"select"
            )
            raise ValueError(msg)
        # Not left to the cast: DuckDB would store a dict or list as its text.
        if isinstance(value, Mapping) or _is_labels(value) or is_frame(value):
            msg = (
                f"`{call}` takes one value for the tuples its keywords select, "
                f"{name!r} being a property of the multi-dim {multi!r}"
            )
            raise ValueError(msg)
        self.schema.require_one_of(multi, {name: [value]}, f"`{call}`")
        self._require_live(multi, {name: [value]}, f"`{call}`")
        tuples = self.dims.get(multi) if chosen is None else chosen
        if tuples is None:
            msg = f"`{call}` finds no tuple of {multi!r}; `add` them first"
            raise KeyError(msg)
        for field, given in labels.items():
            picked = list(given) if _is_labels(given) else [given]
            self._require_typed(over[field], picked, f"`{call}`")
            tuples = tuples.filter(_field_in(multi, field, picked))
        rel = tuples.project(col(multi))
        if labels and not rel.limit(1).fetchall():
            scope = ", ".join(f"{c}={v!r}" for c, v in labels.items())
            msg = f"`{call}` selects no tuple of {multi!r} at {scope}"
            raise KeyError(msg)
        dtype = self._column_type(name, multi)
        edit = rel.project(col(multi), lit(value).cast(dtype).alias(name))
        self._patch(f"{_AXIS_PREFIX}{multi}", multi, name, edit)

    def _require_existing(
        self,
        column: str,
        dim: str,
        labels: Sequence[Any],
        call: str,
        subject: str | None = None,
    ) -> None:
        """Refuse `labels` of `dim`, given in `column`, that its staged axis does not hold.

        Raises
        ------
        KeyError
            As `refuse_unheld` does.
        """
        given = self._columns_relation(
            {column: labels}, {column: self._column_type(dim)}
        )
        refuse_unheld(
            given, self.schema, [column], self._held, call, staged=True, subject=subject
        )

    def _require_live(
        self, subject: str, data: Mapping[str, Sequence[Any]], call: str
    ) -> None:
        """Refuse a value of a property of `subject` that is no label of the dim it refers to.

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
        - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
        """
        named = self.schema.referring_properties()
        for name, values in data.items():
            dim = named.get((subject, name))
            if dim is not None:
                self._require_typed(dim, values, call)
                self._require_existing(name, dim, values, call, subject)

    def _select(
        self, name: str, labels: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, DuckDBPyRelation]]:
        """`labels` with each property keyword replaced by the labels or tuples it selects.

        Those of its dim storing one of the keyword's values, as staged, each
        under the one keyword of attribute or dotted dim property `name`
        over the dim. A keyword naming a dim or field of `name` is that
        dim or field, whatever property shares its name; any other names a
        property of a dim `name` reaches, else of any dim. A property of a
        multi-dim attribute `name` is over, or dotted `name` is a property
        of, selects its tuples instead, which the keywords naming its
        fields narrow. For dotted `name` of a multi-dim, a property of
        a dim one of its fields draws on selects the tuples holding
        those labels there.

        Returns
        -------
        tuple of dict and dict
            The keywords, each property one replaced by the labels it
            selects; and per multi-dim selected by a property, its tuples.

        Raises
        ------
        ValueError
            Where the keyword is a property of several dims `name` reaches,
            `name` reaches the dim through no keyword or several, that
            keyword is also given, a keyword is dotted, a
            value is a label its dim does not list, or the keyword is a
            property of a multi-dim `name` is not over.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        call = f"set({name!r}, ...)"
        subject, dot, _ = name.partition(".")
        if dot:
            flat = self.schema.fields(subject)
            over: tuple[str, ...] = (subject,)
        else:
            flat = self.schema.fields_of(name)
            over = self.schema.multi_dims_of(name)
        reached = {*flat.values(), *over}
        by_dim: dict[str, dict[str, list[Any]]] = {}
        rest: dict[str, Any] = {}
        for c, given in labels.items():
            if "." in c:
                msg = (
                    f"`{call}` selects by {c!r}; a keyword is bare, a "
                    f"dim, a field or a property of a dim {name!r} reaches"
                )
                raise ValueError(msg)
            owners = [
                d
                for d in self.schema.dimensions
                if c in self.schema.properties.get(d, {})
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
        tuples = {
            m: self._tuples_matching(m, by_dim.pop(m), name)
            for m in self.schema.multi_dims
            if m in by_dim
        }
        drawn = []
        for dim, wanted in by_dim.items():
            drawing = [c for c, d in flat.items() if d == dim]
            if len(drawing) != 1:
                msg = (
                    f"`{call}` selects by {sorted(wanted)}, properties of "
                    f"{dim!r}, but {name!r} reaches {dim!r} through "
                    f"{drawing or 'no keyword'}, not exactly one"
                )
                raise ValueError(msg)
            (keyword,) = drawing
            if keyword in rest:
                msg = (
                    f"`{call}` names {keyword}= and selects it by "
                    f"{sorted(wanted)} too; give its labels one way"
                )
                raise ValueError(msg)
            self.schema.require_one_of(dim, wanted, f"`{call}`")
            rest[keyword] = self._labels_matching(dim, wanted)
            drawn.append(keyword)
        if dot and drawn and subject in self.schema.multi_dims:
            held = self.dims.get(subject)
            if held is not None:
                tuples.setdefault(subject, held)
        for multi, chosen in tuples.items():
            for field in self.schema.over(multi):
                if field in rest:
                    given = rest.pop(field)
                    picked = list(given) if _is_labels(given) else [given]
                    chosen = chosen.filter(_field_in(multi, field, picked))
            tuples[multi] = chosen
        return rest, tuples

    def _tuples_matching(
        self, multi: str, wanted: Mapping[str, list[Any]], name: str
    ) -> DuckDBPyRelation:
        """The tuples of `multi`, as staged, storing one of `wanted`'s values of each property.

        Raises
        ------
        ValueError
            Where `name` is neither an attribute over `multi` nor a dotted
            property of it, or a value is a label its dim does not list.
        """
        call = f"set({name!r}, ...)"
        if multi != name.partition(".")[0] and multi not in self.schema.multi_dims_of(
            name
        ):
            msg = (
                f"`{call}` selects by {sorted(wanted)}, properties of the "
                f"multi-dim {multi!r}, which {name!r} is not over"
            )
            raise ValueError(msg)
        self.schema.require_one_of(multi, wanted, f"`{call}`")
        axis = self.properties.get(multi)
        if axis is None:
            return empty_relation(self.con, **{multi: self._column_type(multi)})
        return matching(axis, wanted).project(col(multi))

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
        - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
        """
        _, declared, shown = cast(
            "tuple[str, DuckDBPyType, str]", self.schema.declared(dim)
        )
        for label in {type(n): n for n in labels if n is not None}.values():
            given = self._columns_relation({dim: [label]}, {dim: None}).types[0]
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
        on: Sequence[str] | None = None,
        broadcast: Container[str] = (),
    ) -> None:
        """Project `rel` into `table`'s column order and insert it.

        `insert_into` is positional, and a staging table's order is its own -
        `ALTER TABLE` appends each extra column as `add` first sees one - so the
        projection is built from the table rather than from the caller. A column
        `supplied` does not name is taken from `rel` where it carries one, and is
        otherwise a NULL typed from the table, which is what spares the insert a
        coercion.

        `on` given, the rows `rel` covers on those columns are deleted first, so
        the table holds one row per coordinate and *is* the file it becomes - no fold on the way
        out (`_delete_covered`).
        `rel` must not read `table`, or the delete changes what the insert then
        reads: a caller deriving from the staged rows snapshots them first
        (`_insert_snapshot`).

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
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
        if on is not None:
            self._delete_covered(projected, table, on, broadcast)
        insert_into(self.con, table, projected)

    def _insert_long(
        self,
        rel: DuckDBPyRelation,
        table: str,
        attribute: str,
        present: Container[str],
        dims: Mapping[str, Any],
    ) -> None:
        """Insert `rel` as one attribute's long rows.

        `present` names the columns `rel` already carries; the rest come from
        `dims` where the caller scoped them and are otherwise NULL - "every value
        of it" by the broadcast rule - typed as the schema declares them either
        way, so the insert needs no coercion. A multi-dim's column `rel` lacks is
        packed from its fields the same way, a field at a time.

        Only this attribute's own dims: the staging table is the shape of
        the file it becomes, so a dim it is not addressed by has no column to
        fill, and the projection is in that table's order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """

        def column(d: str) -> Expression:
            if d in present:
                return col(d)
            if d in self.schema.multi_dims_of(attribute):
                return packed(self.schema, d, present, dims).alias(d)
            return lit(dims.get(d)).cast(self._column_type(d)).alias(d)

        own_dims = self.schema.dims_of(attribute)
        shaped = rel.project(
            *(column(d) for d in own_dims),
            lit(attribute).alias("attribute"),
            lit(None).cast(sqltypes.DOUBLE).alias("breakpoint"),
            col("value"),
        )
        call = f"`set({attribute!r}, ...)`"
        refuse_unheld(shaped, self.schema, own_dims, self._held, call, staged=True)
        self._insert(
            shaped,
            table,
            {},
            on=tuple(
                c for c in self.schema.long_columns_for(attribute) if c != "value"
            ),
            broadcast=own_dims,
        )
        self._complete_outside_partial(attribute, table)
        self._overwrite_crossing(attribute, shaped, table)
        self._refuse_crossing(attribute)

    def _overwrite_crossing(
        self, attribute: str, rows: DuckDBPyRelation, table: str
    ) -> None:
        """Stage `rows`' value where one crosses an earlier staged row that no staged row names both with.

        The later `set` wins there, as it does over an earlier row it covers
        (`_delete_covered`): the row staged names every label either names,
        so it is the more specific at the coordinates the two share. `rows`
        is what the `set` staged, and an earlier row is any other of
        `attribute`'s staged rows. A staged row already naming both keeps its
        value, being the more specific.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        - [crossing rows](https://energy-models.github.io/datarecord/reference/resolution/#crossing-rows)
        """
        dims = self.schema.dims_of(attribute)
        if not dims:
            return
        staged = self._collapsed_attribute(attribute)
        earlier = (
            staged.set_alias("s")
            .join(rows.set_alias("r"), null_safe("s", "r", dims), how="anti")
            .project(*map(col, dims))
            .distinct()
        )
        parts = []
        for row_pattern in null_patterns(rows, dims):
            for earlier_pattern in null_patterns(earlier, dims):
                if row_pattern <= earlier_pattern or earlier_pattern <= row_pattern:
                    continue
                shared = row_pattern & earlier_pattern
                on = [col("r", c) == col("e", c) for c in dims if c in shared]
                met = join_on(
                    rows.filter(has_pattern(dims, row_pattern)).set_alias("r"),
                    earlier.filter(has_pattern(dims, earlier_pattern)).set_alias("e"),
                    on,
                )
                parts.append(
                    met.project(
                        *(coalesce(col("r", c), col("e", c)).alias(c) for c in dims),
                        *(col("r", c) for c in ("attribute", "breakpoint", "value")),
                    )
                )
        if not parts:
            return
        meets = (
            reduce(DuckDBPyRelation.union, parts)
            .distinct()
            .set_alias("m")
            .join(
                self.con.table(table).set_alias("t"),
                null_safe("m", "t", dims),
                how="anti",
            )
        )
        with scratch_table(self.con, meets) as snapshot:
            self._insert(snapshot, table, {})

    def _refuse_crossing(self, attribute: str) -> None:
        """Refuse two staged rows of `attribute` that cross where no staged row names both.

        The staged layer's rows are then those of a layer the writer accepts,
        so the fold settles them as it does a written layer's.

        Raises
        ------
        ValueError
            Naming the attribute, the two sets of dims and the coordinate.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        - [crossing rows](https://energy-models.github.io/datarecord/reference/resolution/#crossing-rows)
        """
        rel = self._collapsed_attribute(attribute)
        dims = self.schema.dims_of(attribute)
        for a, b, meets in unsettled(rel, dims):
            row = meets.limit(1).fetchall()
            if not row:
                continue
            at = {c: v for c, v in zip(dims, row[0], strict=True) if v is not None}
            msg = (
                f"`set({attribute!r}, ...)` would stage a row naming {sorted(a)} and "
                f"one naming {sorted(b)} that both cover {at}, where no staged row "
                f"names {sorted(a | b)}, so neither is the more specific there; stage "
                f"the rows with one naming both in one frame "
                f"(https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)"
            )
            raise ValueError(msg)

    def _delete_covered(
        self,
        rel: DuckDBPyRelation,
        table: str,
        on: Sequence[str],
        broadcast: Container[str],
    ) -> None:
        """Delete every row of `table` that a row of `rel` covers.

        A row covers another where each `on` column is equal, NULL-safe so a
        broadcast NULL replaces the same NULL rather than sitting beside it, or
        is a `broadcast` column the covering row leaves NULL, every label by the
        broadcast rule. `rel` is split by which `broadcast` columns it leaves
        NULL, and each part deletes on an equi-join over the columns it names: a
        match on "NULL or equal" cannot hash and would compare every pair.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        """
        open_ = [c for c in on if c in broadcast]
        _dr_rel = rel
        patterns = (
            rel.project(*(col(c).isnull() for c in open_)).distinct().fetchall()
            if open_
            else [()]
        )
        for pattern in patterns:
            nulls = {c for c, null in zip(open_, pattern, strict=True) if null}
            part = " AND ".join(
                f"({col('p', c)} IS NULL) = {c in nulls}" for c in open_
            )
            equal = null_safe("t", "p", [c for c in on if c not in nulls])
            where = f"{part} AND {equal}" if part else str(equal)
            self.con.execute(f"DELETE FROM {table} AS t USING _dr_rel p WHERE {where}")

    def _complete_outside_partial(self, attribute: str, table: str) -> None:
        """Carry the base extent a non-partial axis obliges the staged rows to hold.

        Done as the rows are staged rather than at write, so the staging table
        *is* the layer: touching one snapshot of a series makes
        this layer the owner of that map row's whole extent along the dim, and the
        untouched coordinates have to be carried or the written layer would
        report a loss.

        Scoped by the map rows already staged, not by the attribute: only the
        base rows reaching a map row some edit named are carried (`_reaching`), so
        an entity this record never touched stays in the parent. A map row NULL on a
        dim the base holds labels of carries nothing.

        A carried row is expanded along the dims outside `partial` it leaves NULL, and
        every coordinate a staged row covers is dropped (`uncovered`), so no
        fill sits beside a staged row neither of which is the more specific.
        A staged row leaving a dim outside `partial` NULL is itself written out along
        the labels the base rows it crosses name there (`_spread`).

        Idempotent, which is what lets it run per insert instead of once: the
        anti-join drops any coordinate the table already covers, so a second
        edit to the same attribute carries only what the first did not. That
        anti-join is also the whole of what keeps a fill off an edited
        coordinate - it covers as a later `set` deletes (`_delete_covered`),
        so the fill it excludes and the row that replaces one cover one coordinate, and
        no ordering column is needed to rank them.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        - [a dim outside `partial`](https://energy-models.github.io/datarecord/reference/resolution/#a-dim-outside-partial)
        - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
        """
        outside = self._outside_partial(attribute)
        if not outside:
            return

        # Intersected with the attribute's own columns: an index column its file
        # does not carry is absent from both sides rather than NULL in them,
        # and joining on it would fail to bind (`long_columns_for`).
        columns = set(self.schema.long_columns_for(attribute))
        scope = [
            c for c in self.schema.owner_map_index if c not in outside and c in columns
        ]
        present = [d for d in outside if d in columns]
        if not present:
            # No column for any dim outside `partial`, so the file holds one row per
            # map row and there is no extent to complete.
            return
        dims = [*self.schema.dims_of(attribute), "attribute"]
        staged = self.con.table(table)

        axes: dict[str, DuckDBPyRelation | None] = {d: self.dims[d] for d in present}
        labelled = [
            c
            for c in scope
            if c in self.schema.dimensions and self._base.dims[c].limit(1).fetchall()
        ]
        named = staged.filter(
            ex_all(col(c).isnotnull() for c in ("attribute", *labelled))
        )
        base = self._base.attributes[attribute]
        carried = uncovered(_reaching(base, named, scope, axes), staged, dims)
        self._insert(carried, table, {})
        spread = _spread(self.con.table(table), base, scope, present, dims)
        if spread is not None:
            self._insert(spread, table, {})

    def _columns_relation(
        self,
        columns: Mapping[str, Sequence[Any]],
        schema: Mapping[str, DuckDBPyType | None],
    ) -> DuckDBPyRelation:
        """A relation over `columns`, each an equal-length sequence of scalars.

        Column-wise, so nothing here is per-row: each column is one list
        parameter, unnested beside the others.

        A `None` in `schema` is inferred, as `set` leaves the `value` column.
        An `ENUM` is built as text and cast by the insert.
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
        return self.con.sql(f"SELECT {unnested}", params=params).materialize()

    @_atomic
    def add(
        self,
        dim: str,
        labels: Sequence[Any] | Sequence[Mapping[str, Any]] | DuckDBPyRelation,
        **columns: Any,
    ) -> None:
        """Stage labels of `dim`, each as one whole row of its axis file.

        `columns` are `dim`'s properties, each a scalar for every label or a
        sequence aligned to `labels`. A column no keyword names is NULL on
        these rows: `add` restates a label's row whole, replacing any this
        record staged for it. A label keeps its place in the axis, a label of
        the base removed here takes its base place back, and a new one is
        appended.

        A multi-dim's labels are tuples, given as a frame holding the struct
        column named for the multi-dim, as `dims[dim]` reads it, or one with a
        column per field instead, or as a list of mappings from
        field to label. A frame may carry property columns too and takes
        a scalar keyword only.

        Not a sequence of `set` calls: a label exists by virtue of its axis row,
        so adding one with no columns at all is an edit. An attribute's value,
        whatever its `dims`, goes through `set`.

        Raises
        ------
        KeyError
            If the schema declares no `dim`, on a value of a property
            that refers to a dim and is no label of it, or on a tuple naming
            a label its field's axis, as staged, does not hold.
        TypeError
            On a frame for a dim that is no multi-dim, or a value that is no
            frame DuckDB rescans.
        ValueError
            On a dim with `one_of`, whose axis is the schema's, an
            attribute, a column the axis file has no place for, a sequence of
            the wrong length, a property value its dim does not list, a label
            whose type does not fit the dtype of `dim`, a tuple missing a
            field or naming NULL in one, or a frame carrying both the
            struct column and a field column.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        if dim not in self.schema.dimensions:
            msg = f"the schema declares no dim {dim!r}"
            raise KeyError(msg)
        call = f"add({dim!r}, ...)"
        self._refuse_one_of(dim, f"`{call}`")
        if dim in self.schema.multi_dims:
            self._add_tuples(dim, labels, columns)
            return
        kind = f"{_AXIS_PREFIX}{dim}"
        names = _flat_labels(dim, labels, f"`{call}`")
        self._reject_undeclared(f"`{call}`", _axis_columns(self.schema, dim), columns)
        self._require_typed(dim, names, f"`{call}`")
        data: dict[str, list[Any]] = {dim: names}
        for c, given in columns.items():
            data[c] = list(given) if _is_labels(given) else [given] * len(names)
            if len(data[c]) != len(names):
                msg = (
                    f"`{call}` has {len(data[c])} {c!r} values for {len(names)} labels"
                )
                raise ValueError(msg)
        self.schema.require_one_of(dim, data, f"`{call}`")
        self._require_live(dim, data, f"`{call}`")
        rel = self._columns_relation(data, {c: self._column_type(c, dim) for c in data})
        table = self._ensure(kind)
        self._insert_snapshot(
            self._positioned(rel, table, kind, dim), table, {}, on=(dim,)
        )

    @_atomic
    def remove(
        self,
        dim: str,
        labels: Sequence[Any] | Sequence[Mapping[str, Any]] | DuckDBPyRelation,
    ) -> None:
        """Stage `dim`'s axis without `labels`.

        Need not enumerate what it deletes: the fold drops every value keyed
        by a removed label where `dim` is `partial`. No labels stage nothing.

        And every multi-dim with a field over `dim` without the tuples
        naming one of `labels`, read from the record as it currently resolves:
        a multi-dim attribute's value goes with its tuple.

        A multi-dim's labels are tuples, given as `add` takes them.

        Raises
        ------
        TypeError
            As `add` does.
        ValueError
            On a dim with `one_of`, whose axis is the schema's, a label whose
            type does not fit the dtype of `dim`, a tuple given as `add`
            refuses it, or while a property that refers to `dim` stores one of
            `labels` in the record as staged.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
        - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
        """
        call = f"`remove({dim!r}, ...)`"
        self._refuse_one_of(dim, call)
        if dim in self.schema.multi_dims:
            if not is_frame(labels) and not labels:
                return
            rel = self._tuples(dim, self._tuple_frame(dim, labels, {}, call), call)
            table = self._ensure(f"{_AXIS_PREFIX}{dim}")
            self._delete(table, rel, dim, col("t", dim))
            return
        labels = _flat_labels(dim, labels, call)
        if not labels:
            return
        self._require_typed(dim, labels, call)
        self._require_unnamed(dim, labels)
        gone = self._columns_relation(
            {dim: list(labels)}, {dim: self._column_type(dim)}
        )
        staged = self.dims
        for m in self.schema.multi_dims:
            fields = [f for f, d in self.schema.over(m).items() if d == dim]
            if not fields or m not in staged:
                continue
            naming = reduce(
                operator.or_,
                (_field_in(m, f, labels) for f in fields),
            )
            if not staged[m].filter(naming).limit(1).fetchall():
                continue
            table = self._ensure(f"{_AXIS_PREFIX}{m}")
            for f in fields:
                self._delete(table, gone, dim, col("t", m)[f])
        table = self._ensure(f"{_AXIS_PREFIX}{dim}")
        self._delete(table, gone, dim, col("t", dim))

    def _refuse_one_of(self, dim: str, call: str) -> None:
        """Refuse an edit to the axis of `dim` where it has `one_of`.

        Raises
        ------
        ValueError
            Naming `call` and the list.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
        """
        if dim not in self.schema.one_of_dims:
            return
        msg = (
            f"{call} is refused: {dim!r} lists its labels with `one_of`, "
            f"{list(self.schema.dimensions[dim].one_of or ())}, so its axis is "
            f"the schema's; change the schema instead "
            f"(https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)"
        )
        raise ValueError(msg)

    def _add_tuples(
        self, multi: str, tuples: _Labels, columns: Mapping[str, Any]
    ) -> None:
        """Stage `tuples` of the multi-dim `multi`, with its properties `columns`, as `add` does.

        Raises
        ------
        KeyError
            As `refuse_unheld` does.
        ValueError
            As `_tuple_frame` and `_tuples` do, on a column the axis file has
            no place for, or one whose type does not fit its dtype.
        """
        call = f"`add({multi!r}, ...)`"
        rel = self._tuple_frame(multi, tuples, columns, call)
        known = {*self.schema.over(multi), *self.schema.properties.get(multi, {})}
        self._reject_undeclared(call, known, rel.columns)
        rel = self.schema.require_takes(self._tuples(multi, rel, call), call, multi)
        refuse_unheld(
            rel, self.schema, rel.columns, self._held, call, staged=True, subject=multi
        )
        kind = f"{_AXIS_PREFIX}{multi}"
        table = self._ensure(kind)
        self._insert_snapshot(
            self._positioned(rel, table, kind, multi), table, {}, on=(multi,)
        )

    def _tuple_frame(
        self, multi: str, tuples: _Labels, columns: Mapping[str, Any], call: str
    ) -> DuckDBPyRelation:
        """`tuples` of `multi` as a relation with a column per field and per one of `columns`.

        `tuples` is a list of mappings from field to label, beside which
        each of `columns` is a scalar or a sequence aligned to it, or else a
        frame, beside which each is a scalar, its struct column of `multi`
        unpacked by `_unpacked`. Either way a field keeps the type its
        labels bind as, for `_tuples` to check.

        Raises
        ------
        TypeError
            As `as_relation` does.
        ValueError
            As `_unpacked` does, on a mapping whose keys are not the
            fields of `multi`, a label of a list whose type does not fit
            its field's dim, which a column mixing types would hide, a
            sequence beside a frame, or one of the wrong length.
        """
        over = self.schema.over(multi)
        if not _is_labels(tuples):
            listed = sorted(c for c, v in columns.items() if _is_labels(v))
            if listed:
                msg = (
                    f"{call} takes a frame, which carries its own rows, so "
                    f"{listed} may each be a scalar, or columns of the frame"
                )
                raise ValueError(msg)
            rel = _unpacked(self.schema, multi, as_relation(tuples, self.con), call)
            return rel.project(star(), *(lit(v).alias(c) for c, v in columns.items()))
        rows = list(cast("Sequence[Mapping[str, Any]]", tuples))
        for row in rows:
            if set(row) != set(over):
                msg = (
                    f"{call} gives the tuple {dict(row)}; a tuple of {multi!r} "
                    f"maps each of its fields {list(over)} to a label"
                )
                raise ValueError(msg)
        data: dict[str, list[Any]] = {c: [row[c] for row in rows] for c in over}
        for c, d in over.items():
            self._require_typed(d, data[c], call)
        for c, given in columns.items():
            data[c] = list(given) if _is_labels(given) else [given] * len(rows)
            if len(data[c]) != len(rows):
                msg = f"{call} has {len(data[c])} {c!r} values for {len(rows)} tuples"
                raise ValueError(msg)
        return self._columns_relation(data, dict.fromkeys(data))

    def _tuples(self, multi: str, rel: DuckDBPyRelation, call: str) -> DuckDBPyRelation:
        """`rel` with its field columns packed into the struct column of `multi`.

        Raises
        ------
        ValueError
            On a field `rel` carries no column of, whose type does not
            fit the dtype of the dim it draws on, or that is NULL in a row.
        """
        over = self.schema.over(multi)
        missing = [c for c in over if c not in rel.columns]
        if missing:
            msg = (
                f"{call} carries no column {missing}; a tuple of {multi!r} names "
                f"a label of each of its fields {list(over)}"
            )
            raise ValueError(msg)
        for c, d in over.items():
            self.schema.require_takes(rel.project(col(c).alias(d)), f"{call} in {c!r}")
        partly = reduce(operator.or_, (col(c).isnull() for c in over))
        if rel.filter(partly).limit(1).fetchall():
            msg = (
                f"{call} gives a tuple of {multi!r} with a field NULL; a "
                f"tuple names a label of each of {list(over)}"
            )
            raise ValueError(msg)
        return rel.project(
            packed(self.schema, multi, rel.columns, {}).alias(multi),
            *(col(c) for c in rel.columns if c not in over),
        )

    def _delete(
        self, table: str, gone: DuckDBPyRelation, key: str, target: Expression
    ) -> None:
        """Delete the rows of `table`, aliased `t`, whose `target` is one of `gone`'s `key` values."""
        _dr_gone = gone
        self.con.execute(
            f"DELETE FROM {table} AS t USING _dr_gone g "
            f"WHERE {target} IS NOT DISTINCT FROM {col('g', key)}"
        )

    def _require_unnamed(self, dim: str, labels: Sequence[Any]) -> None:
        """Refuse removing `labels` of `dim` while a property that refers to `dim` stores one of them.

        A tuple naming a label goes with it, but a property's row is its
        subject's, which outlives the label; clearing it is the caller's edit.

        Raises
        ------
        ValueError
            Naming up to three rows per such property.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
        """
        problems = []
        naming = [k for k, d in self.schema.referring_properties().items() if d == dim]
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
        on: Sequence[str],
    ) -> None:
        """`_insert`, for a `rel` read from `table` itself.

        Snapshotted into a table first: the delete of the rows it covers would
        otherwise change what `rel` reads.
        """
        with scratch_table(self.con, rel) as snapshot:
            self._insert(snapshot, table, supplied, on=on)

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
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
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
            f"declare it on its subject before writing it (https://energy-models.github.io/datarecord/reference/schema/#compatibility){hint}"
        )
        raise ValueError(msg)

    # -- edits / rollback (https://energy-models.github.io/datarecord/reference/editing/#edits) --------------------------

    @_atomic
    def rollback(self) -> None:
        """Clear every staged row without writing.

        Notes
        -----
        - [WorkingRecord](https://energy-models.github.io/datarecord/reference/editing/)
        """
        for kind, attribute in self._touched:
            self.con.execute(f"DELETE FROM {self._staged[kind, attribute]}")
            self._fill(kind, attribute)
            if kind == _ATTRIBUTES:
                self.con.execute(f"DELETE FROM {self._staged[_OWNERS, attribute]}")
        self._touched.clear()

    def _collapsed_attribute(self, attribute: str) -> DuckDBPyRelation:
        """One attribute's staged rows, without those naming a label its staged axis lacks.

        The rows are already one per coordinate - an edit replaced the row at its
        coordinate rather than appending beside it (`_insert`) - so this is the table scan minus
        the rows naming a label, or a tuple of one of the attribute's
        multi-dims, that the staged axis does not hold. `set` refuses a label the
        staged axis lacks, so a row is dropped here only where `remove` took
        its label after the `set`: the drop is that removal. The fold drops
        only the parent's rows of a label a layer removes, and a label added
        and removed in one staging area is in no restatement, so it is dead
        nowhere. A multi-dim attribute's row naming a removed label goes by
        its tuple, which `remove` dropped.

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
        - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        rel = self.con.table(self._staged[_ATTRIBUTES, attribute])
        columns = rel.columns
        for name in self.schema.dimensions:
            if name in self.schema.one_of_dims or name not in columns:
                continue
            rel = (
                rel.set_alias("l")
                .join(self._labels(name), null_safe("l", "k", [name]), how="left")
                .filter(col("l", name).isnull() | col("k", "_kept").isnotnull())
                .project(*(col("l", c) for c in columns))
            )
        return rel

    @cached_property
    def _per_cursor(self) -> _StagedPerCursor:
        return _StagedPerCursor()

    def _labels(self, dim: str) -> DuckDBPyRelation:
        """The labels of `dim`'s staged axis, each flagged `_kept`, aliased `k`.

        Built once per thread (`_collapsed_attribute` joins it per attribute):
        it reads the staging table by name, so it reads every later edit.
        """
        labels = self._per_cursor.labels
        if dim not in labels:
            held = self.con.table(self._staged[f"{_AXIS_PREFIX}{dim}", None])
            kept = held.project(col(dim), lit(True).alias("_kept"))  # noqa: FBT003
            labels[dim] = kept.set_alias("k")
        return labels[dim]

    # -- what edits write (https://energy-models.github.io/datarecord/reference/editing/#edits) ------------------------------------------

    def _staged_dims(self) -> tuple[str, ...]:
        """Which axes an edit left differing from the base's, in declaration order.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        - [the schema](https://energy-models.github.io/datarecord/reference/schema/)
        """
        return tuple(
            d
            for d in self.schema.dimensions
            if (kind := f"{_AXIS_PREFIX}{d}", None) in self._touched
            and self._differs(kind)
        )

    def _differs(self, kind: str) -> bool:
        """Whether the staged axis `kind` differs from the base's, as rows in position order."""
        table = self.con.table(self._staged[kind, None])
        columns = [c for c in table.columns if c != _POSITION]
        order = fn.row_number().over(order_by=[col(_POSITION)]).alias(_POSITION)
        staged = table.project(*map(col, columns), order)
        types = dict(zip(table.columns, table.types, strict=True))
        base = self._base_rows(kind).project(
            *(col(c).cast(types[c]).alias(c) for c in columns),
            fn.row_number().over().alias(_POSITION),
        )
        return bool(
            staged.except_(base).limit(1).fetchall()
            or base.except_(staged).limit(1).fetchall()
        )

    @property
    def edits(self) -> StagedSource:
        """The staged layer alone: the delta against the base, as a `LayerData`.

        What `store.commit(w)` writes, as a child of the revision this record
        was built over - only the edits, the fold resolving the rest from the
        parent. The record itself is the flattened whole, base plus edits,
        which is what a standalone `ws.write(w, uri)` wants.

        Writing clears nothing: `rollback()` does, called or not.

        Notes
        -----
        - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
        - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
        """
        self._require_editable()
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
            f"row needs to fill (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)"
        )
        raise ValueError(msg)
    return dtype


def _unpacked(
    schema: Schema, multi: str, rel: DuckDBPyRelation, call: str
) -> DuckDBPyRelation:
    """`rel` with the struct column of the multi-dim `multi`, where it carries one, spelled as a column per field (`unpacked`), checked as a caller gave it.

    Raises
    ------
    ValueError
        On a frame carrying both the struct column and a field column,
        or a struct column whose fields are not the fields of `multi`.
    """
    if multi not in rel.columns:
        return rel
    over = schema.over(multi)
    both = [c for c in over if c in rel.columns]
    if both:
        msg = (
            f"{call} gives a frame carrying both the column {multi!r} and its "
            f"fields {both}; give the tuples one way, either the struct "
            f"column or a column per field"
        )
        raise ValueError(msg)
    dtype = rel.types[rel.columns.index(multi)]
    fields = [name for name, _ in dtype.children] if dtype.id == "struct" else None
    if fields is None or set(fields) != set(over):
        msg = (
            f"{call} gives the column {multi!r} of type {dtype}; it holds the "
            f"tuples of {multi!r} as a struct of its fields {list(over)}"
        )
        raise ValueError(msg)
    return unpacked(schema, multi, rel)


def _axis_columns(schema: Schema, dim: str) -> dict[str, DuckDBPyType]:
    """One axis's columns - the shape of `dims/{dim}.parquet`: its column and its properties.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
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

_OWNERS = "owners"
"""The staging kind of an attribute's owner-map rows, one table per attribute."""

_POSITION = "_position"
"""The column a staged axis keeps its order in, which a table does not."""

# A staging kind, so it becomes part of a table name: no punctuation a SQL
# identifier would need quoting for, and no collision with `_ATTRIBUTES`.
_AXIS_PREFIX = "axis_of_"


def _reaching(
    base: DuckDBPyRelation,
    staged: DuckDBPyRelation,
    scope: Sequence[str],
    axes: Mapping[str, DuckDBPyRelation | None],
) -> DuckDBPyRelation:
    """`base`'s value at each coordinate of the `scope` labels `staged` holds, by the broadcast rule.

    A base row NULL on a `scope` column reaches every staged label there and
    takes it; NULL on a dim of `axes`, every label of that axis, and
    stays NULL where the dim holds none. Of the rows covering one coordinate
    the one naming the most of those columns is kept. A match on "NULL or equal" cannot hash, so `base` is split by which
    `scope` columns it leaves NULL and each part joins on the columns it
    names, as `_delete_covered` does.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    staged_at = staged.project(*(col(c).alias(f"_s_{c}") for c in scope)).distinct()
    named = reduce(
        operator.add,
        (col(c).isnotnull().cast(sqltypes.INTEGER) for c in (*scope, *axes)),
    )
    parts = []
    for pattern in null_patterns(base, scope):
        part = base.filter(has_pattern(scope, pattern))
        part = part.project(star(), named.alias("_named"))
        on = [col(c) == col(f"_s_{c}") for c in scope if c in pattern]
        parts.append(
            part.join(staged_at, ex_all(on)).project(
                *(
                    col(f"_s_{c}").alias(c) if c in scope else col(c)
                    for c in base.columns
                ),
                col("_named"),
            )
        )
    if not parts:
        return base
    rel = reduce(DuckDBPyRelation.union, parts)
    for d, axis in axes.items():
        if axis is None:
            continue
        label = axis.project(col(d).alias("_label"), lit(True).alias("_any"))  # noqa: FBT003
        # `... = true` rather than the bare condition: an equality hashes,
        # a one-sided condition is a nested loop over every row and label.
        rel = rel.join(label, col(d).isnull() == col("_any"), how="left").project(
            *(
                coalesce(col(c), col("_label")).alias(c) if c == d else col(c)
                for c in (*base.columns, "_named")
            )
        )
    return _most_named(rel, (*scope, *axes), base.columns)


def _spread(
    staged: DuckDBPyRelation,
    base: DuckDBPyRelation,
    scope: Sequence[str],
    outside: Sequence[str],
    columns: Sequence[str],
) -> DuckDBPyRelation | None:
    """Each staged row leaving an `outside` dim NULL, at every base row's label it reaches there.

    A base row it reaches names an `outside` dim the staged row leaves NULL and is
    not covered by it on `scope`: the staged row names a `scope` column the
    base row leaves NULL. The row written takes every label either names, and
    the staged row's value. Of the staged rows covering one coordinate the one
    naming the most `scope` columns is kept, and a coordinate `staged` already
    holds is skipped, which keeps it idempotent. `None` where no pair reaches.

    Split by which columns each side leaves NULL, each part an equi-join over
    the columns both name, as `_delete_covered` does.

    Notes
    -----
    - [a dim outside `partial`](https://energy-models.github.io/datarecord/reference/resolution/#a-dim-outside-partial)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    matched = [*scope, *outside]
    open_ = reduce(operator.or_, (col(c).isnull() for c in outside))
    named = reduce(operator.or_, (col(c).isnotnull() for c in outside))
    parts = []
    for staged_pattern in null_patterns(staged.filter(open_), matched):
        for base_pattern in null_patterns(base.filter(named), matched):
            base_only = base_pattern - staged_pattern
            if staged_pattern & set(scope) <= base_pattern or base_only.isdisjoint(
                outside
            ):
                continue
            left = staged.filter(has_pattern(matched, staged_pattern)).set_alias("s")
            right = base.filter(has_pattern(matched, base_pattern)).set_alias("b")
            shared = staged_pattern & base_pattern
            on = [col("s", c) == col("b", c) for c in matched if c in shared]
            parts.append(
                join_on(left, right, on).project(
                    *(
                        col("b" if c in base_only else "s", c).alias(c)
                        for c in staged.columns
                    ),
                    lit(len(staged_pattern & set(scope))).alias("_named"),
                )
            )
    if not parts:
        return None
    rel = _most_named(reduce(DuckDBPyRelation.union, parts), columns, staged.columns)
    return (
        rel.set_alias("r")
        .join(staged.set_alias("t"), null_safe("r", "t", columns), how="anti")
        .distinct()
    )


def _most_named(
    rel: DuckDBPyRelation, by: Sequence[str], columns: Sequence[str]
) -> DuckDBPyRelation:
    """`rel`'s `columns`, keeping per `by` the rows whose `_named` is the largest."""
    over = [col(c) for c in by]
    return (
        rel.project(star(), col("_named").max().over(partition_by=over).alias("_most"))
        .filter(col("_named") == col("_most"))
        .project(*(col(c) for c in columns))
    )
