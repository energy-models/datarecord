"""Editing a record: staged edits, materialised on commit.

What `Record` (read-only) and `write_record` (a whole record at once) do not
cover. Accumulate-then-commit: an edit costs a row in a staging table rather
than a rewrite, and nothing touches the record until `commit()`.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Container, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import uuid4

import duckdb
import narwhals as nw
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import (
    DuckTypes,
    as_relation,
    broadcast_match,
    distinct_values,
    fn,
    null_safe,
    struct_of,
    union_all_by_name,
)
from datarecord.record import (
    EMPTY,
    Flags,
    Frames,
    LazyFrames,
    Record,
    flags_from_rows,
)
from datarecord.schema import LONG_TAIL, Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


# -- commit targets (https://energy-models.github.io/datarecord/design/working-record/#committing) ---------------------------------------------------


@dataclass(frozen=True)
class NewChild:
    """Write the staged rows as a new child layer of `record`.

    Only the edits are written; the fold resolves the rest from the parent.

    `record` defaults to the node the `WorkingRecord` was built over, which is
    what a caller branching from a revision means every time. Passing one
    explicitly is for the rarer case of re-parenting the edits elsewhere; a
    `WorkingRecord` over a base that is not a layered node (a directory, a
    framework object) has nothing to default to and must supply it.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """

    record: Any = None  # a Revision; typed loosely to keep this module import-free


@dataclass(frozen=True)
class Directory:
    """Write a standalone record at `uri`: staged rows *plus* what the record
    already reads, there being no parent to resolve against.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """

    uri: str


Target = NewChild | Directory


# -- value normalisation (https://energy-models.github.io/datarecord/design/working-record/#set) ----------------------------------------------


def _incoming(frame: Any, con: DuckDBPyConnection) -> nw.LazyFrame:
    """A caller's frame as a lazy frame on `con`, whatever backend it arrived on.

    Every edit converts at this one point, so the steps behind it join and union
    in narwhals without minding where the frame came from.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    return nw.from_native(as_relation(nw.from_native(frame).lazy(), con)).lazy()


def _latest_per(rel: DuckDBPyRelation, key: Iterable[str]) -> DuckDBPyRelation:
    """`rel`'s newest row per `key`, by `_seq` - last write wins.

    The three staging tables collapse the same way and differ only in what keys
    them, so the window lives here once. `_seq` and the ranking column are
    projected away, since a collapsed relation is read as data rather than as
    staging bookkeeping.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """
    partition = ", ".join(str(col(c)) for c in key)
    ranked = rel.project(
        star(),
        sql(f"row_number() OVER (PARTITION BY {partition} ORDER BY _seq DESC)").alias(
            "_rn"
        ),
    )
    return ranked.filter(col("_rn") == lit(1)).project(star(exclude=["_rn", "_seq"]))


def _is_frame(value: Any) -> bool:
    """Whether `value` supplies its own keys rather than being a value.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    if isinstance(value, nw.DataFrame | nw.LazyFrame):
        return True
    try:
        nw.from_native(value)
    except TypeError:
        return False
    return True


def _series_index(value: Any) -> Sequence[Any] | None:
    """`value`'s labels if it is a one-dimensional labelled series, else None."""
    index = getattr(value, "index", None)
    if index is None or getattr(value, "ndim", None) != 1:
        return None
    return list(index)


def _series_index_name(value: Any) -> str | None:
    """A labelled series' index name, where it has one a caller could have meant.

    `pd.Series(...).index.name` is the caller already saying what the index
    holds, so an `indexed_by=` repeating it is noise. A `MultiIndex` has `names`
    rather than one `name` and is no one-dimensional index, so it answers None
    and the caller says it explicitly.
    """
    index = getattr(value, "index", None)
    name = getattr(index, "name", None)
    return name if isinstance(name, str) else None


def normalise_value(
    value: Any,
    names: Sequence[str] | None,
    *,
    indexed_by: str | None = None,
) -> tuple[list[str] | None, list[Any], dict[str, list[Any]]]:
    """One of `set`'s four `value` forms as per-name values.

    Parameters
    ----------
    value
        Scalar, sequence, mapping, or a one-dimensional labelled series.
    names
        The names to broadcast or align to.
    indexed_by
        Which axis a labelled series' index holds, where the caller's `entity=`
        or `**dims` said so. `None` means the index holds entity names, which is
        the only other thing it can be.

    Returns
    -------
    names
        The names each value belongs to, or None to keep the caller's.
    values
        One value per name.
    per_dim
        Dim -> label, where a labelled series indexed by an axis was given.

    Raises
    ------
    ValueError
        If a sequence's length does not match `names`.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    labels = _series_index(value)
    if labels is not None:
        # Told, never inferred: an axis label may be a string just like an entity
        # name, so testing membership would make one call mean different things
        # in two records (https://energy-models.github.io/datarecord/design/working-record/#set).
        if indexed_by is not None:
            return None, list(value), {indexed_by: list(labels)}
        return list(labels), list(value), {}

    if isinstance(value, Mapping):
        return list(value), list(value.values()), {}

    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        # A scalar: broadcast to every name.
        if names is None:
            return None, [value], {}
        return list(names), [value] * len(names), {}

    if names is None:
        msg = "a sequence needs `names` to align to"
        raise ValueError(msg)
    if len(value) != len(names):
        msg = (
            f"{len(value)} values for {len(names)} names; a length mismatch is an "
            f"error at the call rather than a truncated edit"
        )
        raise ValueError(msg)
    return list(names), list(value), {}


@dataclass(frozen=True)
class _Written:
    """One reading of a `WorkingRecord`, as the `Record` `write_record` consumes.

    Commit needs two different records out of one staging area - `NewChild` the
    edits alone, `Directory` the resolved result - so this holds whichever
    frame mappings the caller chose.

    Notes
    -----
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """

    schema: Schema
    dims: Frames
    entity_types: Frames
    groups: Frames
    attributes: Frames
    # `default_factory` because `EMPTY` is a `LazyFrames` instance, which
    # `dataclass` reads as a mutable default even though it never mutates.
    outputs: Frames = field(default_factory=lambda: EMPTY)

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Never consulted: `write_record` persists frames, not flags.

        Notes
        -----
        - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
        """
        return {}


# -- the DuckDB-backed implementation (https://energy-models.github.io/datarecord/design/working-record/#staging) ---------------------------------

_SEQ = itertools.count(1)

_CARRIED_SEQ = 0
"""`_seq` for a row carried from the base, below every edit's - `_SEQ` starts at 1.

A carried row is what the layer must hold, never what a caller stated, so an edit
naming the same coordinate has to win in `_latest_per` whenever it arrives.
"""


class WorkingRecord:
    """A `Record` that accepts edits and materialises them on commit.

    Satisfies `Record`, and what it reads is the data *with its pending edits
    applied* - so an edit reads back, or the record is handed to
    something that only knows `Record`, without committing.

    Staged rows live in three connection-scoped DuckDB tables, the *only* place
    a staged row exists: the reads fold them rather than holding a copy, so what
    is staged is asked of the reads themselves.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
    - [staging](https://energy-models.github.io/datarecord/design/working-record/#staging)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """

    def __init__(self, base: Record, con: DuckDBPyConnection) -> None:
        self.base = base
        self.con = con
        self._id = uuid4().hex
        # Keyed by `(kind, attribute)`, the attribute being None for the entity
        # kinds. A long kind stages one table per attribute because that is the
        # file it stands for: one `value` column at the attribute's own type,
        # and its own coordinates and no others.
        self._staged: dict[tuple[str, str | None], str] = {}

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
            return f"staged_axis_{digest}_{self._id}"
        if attribute is None:
            return f"staged_{kind}_{self._id}"
        digest = sha256(attribute.encode()).hexdigest()[:16]
        return f"staged_{kind}_{digest}_{self._id}"

    def _ensure(self, kind: str, attribute: str | None = None) -> str:
        """The staging table for `kind`, created on first use.

        `kind` is one of the fixed three, or a declared group's name - a group
        gets a table shaped by its own coordinates.

        A long kind takes an `attribute` and gets a table per attribute, shaped
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
        if attribute is not None:
            shape = self._empty_long(attribute)
        else:
            duck_types = DuckTypes(self.con)
            columns = _COLUMNS.get(kind)
            if columns is not None:
                shaped = columns(self.schema)
            elif kind.startswith(_AXIS_PREFIX):
                shaped = _axis_columns(self.schema, kind[len(_AXIS_PREFIX) :])
            else:
                shaped = _group_columns(self.schema, kind)
            shape = duck_types.empty_relation(**shaped)
        shape.create(name)
        self._staged[key] = name
        return name

    def _empty_long(self, attribute: str) -> DuckDBPyRelation:
        """A row-less relation shaped like one attribute's long file.

        What the staging table is created from, so the table's shape is a
        projection rather than assembled DDL - the same expressions the inserts
        then project, which is what keeps the two from drifting.

        `value` takes the attribute's declared type, results being declared
        beside inputs. A name neither vocabulary holds falls back to a string,
        which is the widest thing a value column can be.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        """
        duck_types = DuckTypes(self.con)
        value_type = self.schema.value_type(attribute) or nw.String()
        types = {
            "attribute": nw.String(),
            "breakpoint": nw.Float64(),
            "value": value_type,
        }
        shaped = {
            c: types.get(c) or self._column_type(c)
            for c in self.schema.long_columns_for(attribute)
        }
        return duck_types.empty_relation(**shaped, _seq=nw.Int64())

    def _rows(self, kind: str, attribute: str | None = None) -> DuckDBPyRelation | None:
        name = self._staged.get((kind, attribute))
        return None if name is None else self.con.table(name)

    def _staged_attributes_of(self, kind: str) -> tuple[str, ...]:
        """Which attributes `kind` has staged rows for, in insertion order.

        The staging map is the answer, so this is not a query: a table exists
        exactly where rows were staged.
        """
        return tuple(a for (k, a), _ in self._staged.items() if k == kind and a)

    def _column_type(self, column: str) -> nw.dtypes.DType:
        return _column_type(self.schema, column)

    def _staged_coordinates(self, attribute: str) -> tuple[str, ...]:
        """The dim columns one attribute's staging table has, in table order.

        `long_columns_for` minus the fixed tail, rather than `coordinates_of`:
        the two disagree for an *undeclared* attribute, where the first widens
        to every declared dim and the second answers none. The table is built
        from the first, so an insert deriving its columns from the second would
        supply too few - which is the shape a result arrives in.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return tuple(
            c for c in self.schema.long_columns_for(attribute) if c not in LONG_TAIL
        )

    # -- Record, over base plus pending (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) -----------------------------

    @property
    def schema(self) -> Schema:
        return self.base.schema

    @property
    def dims(self) -> Frames:
        staged = self._staged_dims()
        if not staged:
            return self.base.dims
        base = self.base.dims
        keys = tuple(dict.fromkeys((*base, *staged)))
        return LazyFrames(keys, self._axis_frame)

    @property
    def entity_types(self) -> Frames:
        """Base members with pending additions and tombstones applied.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return self._entity_frames("entities")

    @property
    def groups(self) -> Frames:
        """Each declared group's rows, with pending ones applied.

        Keyed by group alone, the component type being no coordinate of one
        (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        base = self.base.groups
        staged = tuple(g for g in self.schema.groups if self._rows(g) is not None)
        keys = tuple(dict.fromkeys((*base, *staged)))
        return LazyFrames(keys, self._group_frame)

    def _group_frame(self, group: str) -> nw.LazyFrame:
        """One group's rows, staged over the base ones on the group's key.

        Union by name rather than a keyed overlay, as `_entity_frame` is: a
        staged row wins on the key, and one only the base holds passes through.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        base = self.base.groups
        staged = self._collapsed_group(group)
        if staged is None:
            return base[group]
        if group not in base:
            return nw.from_native(staged.filter(~col("deleted")))
        key = self.schema.group_key(group)
        frame = base[group]
        present = set(frame.collect_schema().names())
        on = null_safe("b", "s", [c for c in key if c in present])
        return nw.from_native(
            self._group_union(as_relation(frame, self.con), staged, on)
        )

    def _entity_frames(self, kind: str) -> Frames:
        base = self.base.entity_types
        staged = self._collapsed_entities(kind)
        if staged is None:
            return base
        types = distinct_values(staged, "entity_type", order=False)
        keys = tuple(dict.fromkeys((*base, *types)))
        return LazyFrames(keys, lambda ctype: self._entity_frame(kind, ctype))

    def _entity_frame(self, kind: str, ctype: str) -> nw.LazyFrame:
        """One type's members, staged rows over the base ones.

        Union by name rather than a keyed overlay: a staged member row carries
        whatever columns the caller passed, and a name the base also holds is
        an edit to it, so the staged row wins on the entity key while a name
        only the base holds passes through.
        """
        base = self.base.entity_types
        staged = self._collapsed_entities(kind)
        assert staged is not None
        mine = staged.filter(col("entity_type") == lit(ctype))
        if ctype not in base:
            return nw.from_native(mine.filter(~col("deleted")))

        key = ("entity",)
        frame = base[ctype]
        # `collect_schema` reads names without materialising, and `_as_relation`
        # keeps a DuckDB-backed frame as the plan it already is: the long schema
        # promises a record hands over unmaterialised frames, and the pending-edit
        # overlay prices a read with
        # pending edits at what one more layer costs.
        present = set(frame.collect_schema().names())
        on = null_safe("b", "s", [c for c in key if c in present])
        return nw.from_native(
            self._entity_union(as_relation(frame, self.con), mine, on, ctype)
        )

    def _entity_union(
        self,
        base: DuckDBPyRelation,
        staged: DuckDBPyRelation,
        on: Expression,
        ctype: str,
    ) -> DuckDBPyRelation:
        """`staged` over `base` on the entity key, tombstones removed.

        `entity_type` and `deleted` are supplied whatever the base carried:
        a resolved frame drops them (the type is the key it was looked up by)
        while `write_record` needs them back, so one shape serves both.
        """
        drop = ("entity_type", "deleted")
        # Filtered against `base`'s own columns, since DuckDB rejects excluding
        # one the relation does not have: a resolved frame carries neither of
        # these where a `write_record` source carries both. `staged` always
        # carries them, having just been filtered on `entity_type`.
        b = base.project(star(exclude=[c for c in drop if c in base.columns]))
        s = staged.project(star(exclude=list(drop)), col("deleted"))
        # The staged row wins on the entity key, so the base keeps only what the
        # staging area does not restate; the two are then unioned by name, since
        # a staged member row carries whatever columns the caller passed.
        kept = b.set_alias("b").join(s.set_alias("s"), on, how="anti")
        live = s.filter(~col("deleted")).project(star(exclude=["deleted"]))
        return union_all_by_name([kept, live], self.con).project(
            star(),
            lit(ctype).alias("entity_type"),
            lit(False).alias("deleted"),  # noqa: FBT003
        )

    def _group_union(
        self, base: DuckDBPyRelation, staged: DuckDBPyRelation, on: Expression
    ) -> DuckDBPyRelation:
        """`staged` over `base` on the group's key, tombstones removed.

        `_entity_union` without the type: a group's rows carry no `entity_type`
        to restore, so `deleted` is the whole of what is supplied back.
        """
        b = base.project(star(exclude=[c for c in ("deleted",) if c in base.columns]))
        s = staged.project(star(exclude=["deleted"]), col("deleted"))
        kept = b.set_alias("b").join(s.set_alias("s"), on, how="anti")
        live = s.filter(~col("deleted")).project(star(exclude=["deleted"]))
        return union_all_by_name([kept, live], self.con).project(
            star(),
            lit(False).alias("deleted"),  # noqa: FBT003
        )

    @property
    def attributes(self) -> Frames:
        """Base attributes with pending edits applied.

        A set of pending edits *is* a layer - an unwritten one - so the reads
        compose the same way: the staged rows are the last layer, resolved over
        whatever the record was reading before.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        staged = self._staged_attribute_names()
        keys = tuple(dict.fromkeys((*self.base.attributes, *staged)))
        return LazyFrames(keys, self._attribute_frame)

    def _staged_attribute_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._staged_attributes_of("inputs")))

    def _attribute_frame(self, attribute: str) -> nw.LazyFrame:
        base = (
            self.base.attributes[attribute]
            if attribute in self.base.attributes
            else None
        )
        staged = self._collapsed_inputs(attribute)
        if staged is None:
            if base is None:
                raise KeyError(attribute)
            return base
        if base is None:
            return nw.from_native(staged)
        # The staged rows are the last layer, so they win per key (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits).
        return nw.from_native(self._overlay(attribute, base.to_native(), staged))

    def _overlay(
        self, attribute: str, base: DuckDBPyRelation, staged: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        """`staged` over `base`, last-writer-wins per coordinate.

        Per *coordinate*, not per input key: the input key excludes the dims an
        attribute is not owned per, so keying on it alone would let one
        staged snapshot displace the base's whole series on read - reporting a
        loss the staging area is careful not to make
        (`_complete_owned_whole`).

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        # NULL-safe on the input key and `breakpoint`; *broadcast* on the dims
        # an attribute is not owned per. The broadcast rule says a staged NULL dim means "all
        # values of that dim", so it must displace the base's rows at every
        # value of it - otherwise the two overlap, which the broadcast rule forbids. A staged
        # row that *does* name a coordinate displaces only that one, which is
        # what keeps the rest of the series (`_complete_owned_whole` stages it
        # alongside).
        # Both sides carry this attribute's columns and no others, so the key
        # and the broadcast set are intersected with them: a dim the attribute
        # is not addressed by is absent from the file rather than NULL in it,
        # and joining on it would fail to bind.
        columns = set(self._long_columns(attribute))
        fixed = (
            *(c for c in self.schema.input_key if c in columns),
            "breakpoint",
        )
        # The dims a NULL broadcasts over, minus those the key already fixes.
        # Read off `broadcast_dims` rather than subtracted from `schema.dims`:
        # an address coordinate is in neither, and a subtraction would put one
        # here the moment it left the key.
        key = set(self.schema.input_key)
        broadcast = tuple(
            d for d in self.schema.broadcast_dims if d not in key and d in columns
        )
        # The staged side broadcasts, so it is `broadcast_match`'s first alias.
        on = broadcast_match("s", "b", dict.fromkeys(fixed), broadcast)
        kept = base.set_alias("b").join(staged.set_alias("s"), on, how="anti")
        return union_all_by_name([kept, staged], self.con).project(
            *(col(c) for c in self._long_columns(attribute))
        )

    def _long_columns(self, attribute: str) -> tuple[str, ...]:
        return self.schema.long_columns_for(attribute)

    def _owned_whole(self, attribute: str) -> tuple[str, ...]:
        """`AttributeSpec.dims` minus `Schema.partial`, for every type declaring
        `attribute` - one `inputs/<attr>.parquet` serves them all.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        partial = self.schema.partial or frozenset()
        spec = self.schema.attributes.get(attribute)
        whole = frozenset() if spec is None else spec.dims - partial
        return tuple(d for d in self.schema.dims if d in whole)

    @property
    def outputs(self) -> Frames:
        """Staged results, keyed by attribute - what a tool handed back.

        Results reach a record through `set(..., kind="outputs")`, so a tool can
        solve against this record's pending inputs and attach what it computed
        without committing first. The base's results are *not* included: they
        were computed from inputs these edits may have changed, and results do
        not overlay, so what is staged is the whole answer.

        Keeping them coherent with the inputs is the caller's business - editing
        an input after attaching results leaves results describing a record that
        no longer exists, and nothing here silently discards them.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        names = tuple(sorted(self._staged_attributes_of("outputs")))
        if not names:
            return EMPTY

        def frame(attr: str) -> nw.LazyFrame:
            rel = cast("DuckDBPyRelation", self._rows("outputs", attr))
            # `_seq` is staging bookkeeping, not data. Results are otherwise
            # read as staged: they do not overlay, so there is nothing to
            # collapse them against (https://energy-models.github.io/datarecord/design/read-path/#outputs).
            return nw.from_native(rel.project(star(exclude=["_seq"])))

        return LazyFrames(names, frame)

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Base flags unioned with what the staged rows use.

        Scoped by the names this record resolves for the type - base members plus
        pending additions - the staged rows carrying no type.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        out = dict(self.base.flags(ctype))
        if ctype not in self.entity_types:
            return out
        members = as_relation(self.entity_types[ctype], self.con).project("entity")
        arms = [
            arm
            for attribute in self._staged_attributes_of("inputs")
            if (arm := self._flags_arm(attribute, members)) is not None
        ]
        if not arms:
            return out
        # The arms answer in the shape the fold's own flags do, so they scope the
        # same way (https://energy-models.github.io/datarecord/design/read-path/#owner-map) before being unioned into the base's.
        staged = flags_from_rows(
            self.schema,
            self.schema.broadcast_dims,
            union_all_by_name(arms, self.con).fetchall(),
        )
        for attribute, flags in staged.items():
            was = out.get(attribute) or Flags(frozenset(), frozenset(), False)  # noqa: FBT003
            out[attribute] = Flags(
                varies=was.varies | flags.varies,
                broadcast=was.broadcast | flags.broadcast,
                breakpoints=was.breakpoints or flags.breakpoints,
            )
        return out

    def _flags_arm(
        self, attribute: str, members: DuckDBPyRelation
    ) -> DuckDBPyRelation | None:
        """One attribute's flags as a single row, in a shape every arm shares.

        `(attribute, varies, broadcast, breakpoints)`, the two middle fields
        structs keyed by dim. The staging tables differ in columns where the
        answer does not, so a dim this attribute has no column for is a constant
        `false` - "no such axis" rather than "every row broadcasts over it".
        Uniform arms are what let one union answer every attribute in a single
        query instead of a round trip each.

        None where the attribute carries no `entity` column at all: `flags` is
        answered per component type, and an attribute addressed by an axis alone
        belongs to the record rather than to any type's members.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        rel = self._rows("inputs", attribute)
        if rel is None or "entity" not in rel.columns:
            return None
        present = set(rel.columns)

        def used(dim: str, *, broadcast: bool) -> Expression:
            if dim not in present:
                return lit(False)  # noqa: FBT003
            value = col(dim).isnull() if broadcast else col(dim).isnotnull()
            return fn.bool_or(value)

        dims = self.schema.broadcast_dims
        return (
            rel.set_alias("i")
            .join(members.set_alias("m"), "i.entity = m.entity", how="semi")
            .aggregate(
                [
                    lit(attribute).alias("attribute"),
                    struct_of({d: used(d, broadcast=False) for d in dims}).alias(
                        "varies"
                    ),
                    struct_of({d: used(d, broadcast=True) for d in dims}).alias(
                        "broadcast"
                    ),
                    fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
                    fn.count_star().alias("_rows"),
                ]
            )
            # An ungrouped aggregate answers one row whatever the filter matched,
            # so the count is what distinguishes "this type has no rows of it" -
            # which is absence from the mapping - from flags that are all false.
            .filter(col("_rows") > lit(0))
            .project(star(exclude=["_rows"]))
        )

    # -- edits (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one, https://energy-models.github.io/datarecord/design/working-record/#add-remove) ----------------------------------------

    def _series_axis(
        self, attribute: str, value: Any, indexed_by: str | None
    ) -> str | None:
        """Which axis a labelled series' index holds, or None for entity names.

        The caller says which - `indexed_by="snapshot"`, or the series' own
        `index.name` where it names a coordinate of this attribute. Never read
        off the labels: an axis label may be a string just like an entity name,
        so a membership test would make one call mean different things in two
        records.

        An `index.name` naming no coordinate is ignored rather than rejected; it
        may be `None`, or a pandas artefact like `"index"`.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        if _series_index(value) is None:
            if indexed_by is not None:
                msg = (
                    f"`set({attribute!r}, ..., indexed_by={indexed_by!r})` says what "
                    f"a series index holds, but the value is not a labelled series"
                )
                raise ValueError(msg)
            return None
        named = indexed_by if indexed_by is not None else _series_index_name(value)
        if named is None:
            return None
        coordinates = [
            c for c in self.schema.coordinates_of(attribute) if c != "entity"
        ]
        if named not in coordinates:
            if indexed_by is None:
                return None
            msg = (
                f"`indexed_by={named!r}` is no coordinate of {attribute!r}, "
                f"which is addressed by {coordinates or ['entity']}"
            )
            raise ValueError(msg)
        return named

    def _axis_of(self, attribute: str) -> str | None:
        """The dim whose axis file carries `attribute`, or `None`.

        A declared attribute addressed by one dim alone is a column of that
        dim's axis file rather than a long row, so an edit to it stages an axis
        row. `attributes_on` is the rule, so `entity` answers None here too: its
        sole-coordinate attributes are the component frame's columns, which
        `add` already stages. An undeclared attribute is never one either - only
        a result is undeclared, and a result is always long.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        spec = self.schema.attributes.get(attribute)
        if spec is None or spec.varying:
            return None
        (dim,) = spec.dims
        return dim if attribute in self.schema.attributes_on(dim) else None

    def _stage_axis(
        self,
        dim: str,
        attribute: str,
        value: Any,
        *,
        entity: Sequence[str] | None,
    ) -> None:
        """Stage one axis-file attribute, keyed by the axis's own labels.

        `value` is a mapping from label to value, or a scalar for every label the
        axis currently has. A mapping may name a label no layer has written yet,
        which becomes a row of this layer's axis file - the fold keys per label,
        so introducing one displaces nothing.

        One row per label carrying this attribute's column alone, so a second
        `set` on the same axis adds its own and `_collapsed_axis` folds the two
        together per label.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        if entity is not None:
            msg = (
                f"`set({attribute!r}, ..., entity=...)` names components, but "
                f"{attribute!r} is addressed by {dim!r} alone - it is a column of "
                f"dims/{dim}.parquet and belongs to no component"
            )
            raise ValueError(msg)
        if _is_frame(value) or isinstance(value, (list, tuple)):
            msg = (
                f"`set({attribute!r}, <sequence>)` has no labels to align to; "
                f"{attribute!r} is addressed by {dim!r} alone, so pass a mapping "
                f"from {dim!r} label to value, or a scalar for every label"
            )
            raise ValueError(msg)
        if len(self.schema.axis_key(dim)) > 1:
            msg = (
                f"{attribute!r} is addressed by {dim!r}, which is `within` "
                f"{sorted(self.schema.dimensions[dim].within)}; a nested axis's "
                f"labels identify a point only within its parents, which a "
                f"mapping from label alone cannot name"
            )
            raise ValueError(msg)

        seq = next(_SEQ)
        table = self._ensure(f"{_AXIS_PREFIX}{dim}", None)
        if isinstance(value, Mapping):
            if not value:
                return
            rel = self._values_relation(
                {dim: list(value), attribute: list(value.values())},
                {
                    dim: self._column_type(dim),
                    attribute: self.schema.value_type(attribute),
                },
            )
            supplied = {"_seq": lit(seq)}
        else:
            if dim not in self.base.dims:
                msg = (
                    f"`set({attribute!r}, <scalar>)` reaches every label the "
                    f"{dim!r} axis has, and it has none; name the labels as a "
                    f"mapping, or write the axis file first"
                )
                raise ValueError(msg)
            # The key alone: the axis frame may carry a sibling attribute's
            # column, and this edit states only its own - `_collapsed_axis` folds
            # the rest back per label.
            rel = as_relation(self.base.dims[dim], self.con).project(col(dim))
            supplied = {attribute: lit(value), "_seq": lit(seq)}

        self._insert(rel, table, supplied)

    def _resolved_names(self, ctype: str) -> list[str]:
        """Every name `ctype` currently resolves to, base plus staged.

        Through narwhals rather than the native frame: a backend's column
        yields its own scalar type (a `pyarrow.StringScalar`, say), which
        would compare unequal to the plain strings an edit names.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        if ctype not in self.entity_types:
            return []
        frame = self.entity_types[ctype].select("entity").collect()
        return [str(n) for n in frame["entity"].to_list()]

    def _name_types(self) -> nw.LazyFrame | None:
        """`(name, entity_type)` over everything this record resolves.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        parts = [
            self.entity_types[ctype]
            .select("entity")
            .with_columns(entity_type=nw.lit(ctype))
            for ctype in self.entity_types
        ]
        if not parts:
            return None
        return nw.concat(parts, how="vertical")

    def _require_unique(self, ctype: str, lazy: nw.LazyFrame) -> None:
        """Reject an `add` whose names another type already holds.

        Re-adding a name of the *same* type is an edit to that member, which
        `_entity_union` resolves last-writer-wins - so only a cross-type clash
        raises.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        known = self._name_types()
        if known is None:
            return
        clashing = (
            lazy.select("entity")
            .unique("entity")
            .join(
                known.filter(nw.col("entity_type") != ctype),
                on="entity",
                how="inner",
            )
            .unique(["entity", "entity_type"])
            .select("entity", "entity_type")  # the order `iter_rows` unpacks
            .collect()
        )
        if not clashing.is_empty():
            # Sorted here rather than in the query, as `collision_detail` is:
            # the message must be deterministic, and this is a handful of rows.
            detail = ", ".join(
                f"{n!r} is already a {t}" for n, t in sorted(clashing.iter_rows())
            )
            msg = (
                f"cannot add {ctype} components whose names are taken: {detail}; "
                f"names are unique across every component type (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)"
            )
            raise ValueError(msg)

    def _resolve_types(self, names: Sequence[str]) -> dict[str, str]:
        """`names` mapped to their types, rejecting any the record does not resolve.

        A value keyed to a name with no member row would resolve to nothing, so
        it is caught here rather than dropped at read time.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        wanted = list(dict.fromkeys(names))
        known = self._name_types()
        if known is None or not wanted:
            found: dict[str, str] = {}
        else:
            matched = (
                known.filter(nw.col("entity").is_in(wanted))
                .select("entity", "entity_type")
                .collect()
            )
            found = {str(n): str(t) for n, t in matched.iter_rows()}
        unknown = sorted({n for n in wanted if n not in found})
        if unknown:
            msg = (
                f"no member row for {unknown}; `add` them first - a value for a "
                f"name no layer declares would resolve to nothing"
            )
            raise KeyError(msg)
        return {n: found[n] for n in names}

    def _validate_dims(self, dims: Mapping[str, Any]) -> None:
        """The dim vocabulary, checked for either `kind`.

        Notes
        -----
        - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        unknown = sorted(set(dims) - set(self.schema.dims))
        if unknown:
            msg = f"the schema declares no dims {unknown}"
            raise KeyError(msg)

    def _validate_result(self, attribute: str, dims: Mapping[str, Any]) -> None:
        """A result's name and dims, against the schema's `results`.

        The attribute check only - not membership, which stays relaxed for a
        result: a solve may produce rows for a component type it derived rather
        than read, and rejecting those would refuse a legitimate result.

        Notes
        -----
        - [results through kind="outputs"](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        self._validate_dims(dims)
        spec = self.schema.results.get(attribute)
        if spec is None:
            known = sorted(self.schema.results)
            msg = (
                f"the schema declares no result {attribute!r}; it declares "
                f"{known or 'none'}. A result is declared like an input, so a "
                f"tool states its vocabulary before attaching what it computed"
            )
            raise KeyError(msg)
        outside = sorted(set(dims) - spec.dims)
        if outside:
            msg = (
                f"result {attribute!r} does not vary over {outside}; "
                f"it varies over {sorted(spec.dims) or 'nothing'}"
            )
            raise ValueError(msg)

    def _validate_attribute(
        self,
        ctype: str,
        attribute: str,
        dims: Mapping[str, Any],
        *,
        name: str | None = None,
    ) -> None:
        """One name's attribute checks, against the spec of *its* type.

        Inputs only: a result is declared in `results` rather than per type, so
        `_validate_result` checks its name and dims and nothing checks its
        membership - what a solve computes need not be a component the record
        declares.

        `name` is reported where known: with the type derived rather than passed, the name is what the caller can act on.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        who = f" (for {name!r})" if name is not None else ""
        # The type's existence and its vocabulary are separate questions once
        # attributes are declared record-wide: `entity_types` answers the
        # first, `attributes_for` the second, and a type carrying nothing is
        # not the same as a type the schema never declared. An empty
        # `entity_types` is no vocabulary rather than an empty one - the axis
        # is undeclared or a plain string - so there is nothing to check
        # against and any label passes.
        known = self.schema.entity_types
        if known and ctype not in known:
            msg = f"the schema declares no entity type {ctype!r}{who}"
            raise KeyError(msg)
        declared = self.schema.attributes_for(ctype)
        if attribute not in declared:
            msg = f"{ctype} does not carry {attribute!r}{who}"
            raise KeyError(msg)
        spec = declared[attribute]
        outside = sorted(set(dims) - spec.dims)
        if outside:
            msg = (
                f"{ctype}.{attribute} does not vary over {outside}{who}; "
                f"it varies over {sorted(spec.dims) or 'nothing'}"
            )
            raise ValueError(msg)

    def set(
        self,
        attribute: str,
        value: Any,
        *,
        entity: Sequence[str] | None = None,
        kind: Literal["inputs", "outputs"] = "inputs",
        indexed_by: str | None = None,
        **dims: Any,
    ) -> None:
        """Stage an attribute value for a group of components.

        A labelled series must say what its index holds: `indexed_by="snapshot"`
        names the axis, and `entity=` implies it where the attribute has exactly
        one other coordinate. Without either the index is read as entity names.
        Never inferred from the labels themselves - an axis label may be a string
        just like a name, so that would make one call mean different things in
        two records.

        `value` takes five forms: a scalar broadcast to every name, a sequence
        aligned positionally to `names`, a mapping keyed by name, a long frame
        supplying its own keys, and a narwhals expression - which is a *function
        of the current value* rather than a value, so it reads before it stages
        and two such calls compose.

        No `entity_type` parameter: the type is looked up from the entity,
        so one call may span types and each is validated against its own
        type's spec. `entity=None` means every component whose type
        declares `attribute`.

        Every other coordinate goes through `**dims`, including a group's -
        `bus="north"` for a connection attribute, `from=`/`to=` for a corridor.
        None has a parameter of its own, since which coordinates exist is
        declared rather than fixed.

        `kind` names the destination in the format's own terms:
        `"outputs"` stages into `outputs/` instead of `inputs/`, which is how a
        tool hands results back. Results use the same long schema; what differs
        is that they do not overlay.

        Two checks are skipped for `"outputs"`, both because a result is not a
        value the schema governs: the attribute need not be declared, and a
        result's `name` need not resolve to a declared member. A solve may
        produce rows for a component type it derived rather than read - PyPSA's
        `SubNetwork` is one - and rejecting those would refuse a legitimate
        result. An *input* for an undeclared name stays an error.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        is_long_frame = _is_frame(value) and _series_index(value) is None
        if is_long_frame:
            lazy = _incoming(value, self.con)
            if "entity_type" in lazy.collect_schema().names():
                msg = (
                    f"`set({attribute!r}, <frame>)` was given a `entity_type` "
                    f"column; names are unique across every type, so an attribute row "
                    f"carries no type and the column would be ignored (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)"
                )
                raise ValueError(msg)
            if kind == "inputs":
                self._validate_frame(lazy, attribute, dims)
            else:
                self._validate_result(attribute, dims)
            self._stage_long(attribute, lazy, kind, dims)
            return

        if isinstance(value, nw.Expr):
            if kind == "inputs":
                self._validate_dims(dims)
            else:
                self._validate_result(attribute, dims)
            self._stage_derived(attribute, value, entity=entity, kind=kind, **dims)
            return

        axis = self._axis_of(attribute) if kind == "inputs" else None
        if axis is not None:
            self._stage_axis(axis, attribute, value, entity=entity)
            return

        target = (
            list(entity) if entity is not None else self._names_declaring(attribute)
        )
        keys, values, per_dim = normalise_value(
            value,
            target,
            indexed_by=self._series_axis(attribute, value, indexed_by),
        )
        if keys is None:
            keys = target
            if len(values) == 1 and len(keys) > 1:
                values = values * len(keys)
        if kind == "inputs":
            self._validate_dims(dims)
            # One lookup serves both: rejects a name with no member row, and
            # returns the type whose spec is checked (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
            for name, ctype in self._resolve_types(keys).items():
                self._validate_attribute(ctype, attribute, dims, name=name)
        else:
            self._validate_result(attribute, dims)

        table = self._ensure(kind, attribute)
        self._stage_rows(attribute, table, keys, values, per_dim, dims)

    def _names_declaring(self, attribute: str) -> list[str]:
        """Every resolved name whose type declares `attribute` - `names=None`.

        The types come from the record rather than from `types_declaring` where
        the schema declares no entity-type labels: the axis is then a plain
        string and its labels are data, so what types exist is a question only
        the resolved components can answer.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        declared = self.schema.types_declaring(attribute)
        if not self.schema.entity_types:
            declared = frozenset(
                c
                for c in self.entity_types
                if attribute in self.schema.attributes_for(c)
            )
        return [
            name
            for ctype in sorted(declared)
            if ctype in self.entity_types
            for name in self._resolved_names(ctype)
        ]

    def _validate_frame(
        self, lazy: nw.LazyFrame, attribute: str, dims: Mapping[str, Any]
    ) -> None:
        """A long input frame's dims, names and per-name specs.

        The frame supplies its own names, so each is resolved to its type and
        checked against that type's spec - one frame may legitimately span
        types, since names are unique.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        self._validate_dims(dims)
        if "entity" not in lazy.collect_schema().names():
            return
        names = [
            str(n)
            for n in lazy.select("entity")
            .unique("entity")
            .collect()["entity"]
            .to_list()
        ]
        for name, ctype in self._resolve_types(names).items():
            self._validate_attribute(ctype, attribute, dims, name=name)

    def _stage_rows(
        self,
        attribute: str,
        table: str,
        keys: list[str],
        values: list[Any],
        per_dim: dict[str, list[Any]],
        dims: Mapping[str, Any],
    ) -> None:
        """Stage the scalar/sequence/mapping forms, as a relation rather than rows.

        The rows are `keys` x `per_dim`'s labels - every named entity at every
        coordinate the value covers - so a per-snapshot series over a year for a
        thousand components is millions of them. Both factors are small, and only
        their product is not, so each becomes a one-column relation and DuckDB
        joins them: the product never exists as Python objects.

        `keys` and `values` are positionally aligned where there is no `per_dim`;
        with one, `values` aligns to its labels and every key takes all of them.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        entity = {"entity": self._column_type("entity")}
        # `value` is inferred: an undeclared result has no declared dtype, and
        # the staging table already took its type from `_scalar_dtype`.
        if per_dim:
            (dim, labels) = next(iter(per_dim.items()))
            # The value belongs to the label, so it rides on that side of the
            # join and every entity picks it up.
            rel = self._values_relation({"entity": keys}, entity).cross(
                self._values_relation(
                    {dim: labels, "value": values},
                    {dim: self._column_type(dim), "value": None},
                )
            )
            present = {"entity", dim}
        else:
            rel = self._values_relation(
                {"entity": keys, "value": values}, {**entity, "value": None}
            )
            present = {"entity"}

        self._insert_long(rel, table, attribute, present, dims)

    def _insert(
        self, rel: DuckDBPyRelation, table: str, supplied: Mapping[str, Expression]
    ) -> None:
        """Project `rel` into `table`'s column order and insert it.

        `insert_into` is positional, and a staging table's order is its own -
        `ALTER TABLE` appends each extra column as `add` first sees one - so the
        projection is built from the table rather than from the caller. A column
        `supplied` does not name is taken from `rel` where it carries one, and is
        otherwise a NULL typed from the table, which is what spares the insert a
        coercion.
        """
        staging = self.con.table(table)
        carried = {c.lower() for c in rel.columns}

        def column(name: str, dtype: str) -> Expression:
            if name in supplied:
                return supplied[name].alias(name)
            if name.lower() in carried:
                return col(name)
            return lit(None).cast(dtype).alias(name)

        projected = rel.project(
            *(
                column(c, str(t))
                for c, t in zip(staging.columns, staging.types, strict=True)
            )
        )
        try:
            projected.insert_into(table)
        except duckdb.ConversionException as exc:
            # An `Enum` dim reaches DuckDB as one, so this is what rejects a
            # label its vocabulary does not hold - as a conversion to `UINT8`,
            # the enum's storage type, naming neither the dim nor what it
            # declares. The source column is in the message, which is what makes
            # it restatable.
            match = re.search(r"casting from source column (\w+)", str(exc))
            if match is None:
                raise
            source = match.group(1)
            dim = self.schema.dimensions.get(source)
            if dim is None or not isinstance(dim.dtype, nw.Enum):
                raise
            msg = (
                f"{source!r} declares no such label; its dtype is an Enum over "
                f"{sorted(dim.dtype.categories)}, which pins the vocabulary"
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
        way, so the insert needs no coercion.

        Only this attribute's own coordinates: the staging table is the shape of
        the file it becomes, so a dim it is not addressed by has no column to
        fill, and the projection is in that table's order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        duck_types = DuckTypes(rel)
        rel.project(
            *(
                col(d)
                if d in present
                else duck_types.lit(dims.get(d), self._column_type(d)).alias(d)
                for d in self._staged_coordinates(attribute)
            ),
            lit(attribute).alias("attribute"),
            duck_types.null(nw.Float64()).alias("breakpoint"),
            col("value"),
            lit(next(_SEQ)).alias("_seq"),
        ).insert_into(table)
        self._complete_owned_whole(attribute, table)

    def _complete_owned_whole(self, attribute: str, table: str) -> None:
        """Carry the base extent a non-partial axis obliges the staged rows to hold.

        Done as the rows are staged rather than at commit, so the staging table
        *is* the layer: touching one snapshot of a series makes
        this layer the owner of that key's whole extent along the dim, and the
        untouched coordinates have to be carried or the commit would report a
        loss.

        Scoped by the keys already staged, not by the attribute: the semi-join
        below reaches only the keys some edit named, so a component this record
        never touched stays in the parent. Carried rows take a `_seq` below every
        edit's, so a later `set` on a carried coordinate outranks it rather than
        tying with it.

        Idempotent, which is what lets it run per insert instead of once: the
        anti-join drops any coordinate the table already holds, so a second edit
        to the same attribute carries only what the first did not.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        whole = self._owned_whole(attribute)
        if not whole or attribute not in self.base.attributes:
            return

        # Intersected with the attribute's own columns: a key column its file
        # does not carry is absent from both sides rather than NULL in them,
        # and joining on it would fail to bind (`long_columns_for`).
        columns = set(self._long_columns(attribute))
        scope = [c for c in self.schema.input_key if c not in whole and c in columns]
        present = [d for d in whole if d in columns]
        if not present:
            # No column for any whole-owned dim, so the file holds one row per
            # key and there is no extent to complete.
            return
        coordinate = [*scope, *present, "breakpoint"]
        staged = self.con.table(table)
        base = as_relation(self.base.attributes[attribute], self.con)
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
        self._insert(carried, table, {"_seq": lit(_CARRIED_SEQ)})

    def _values_relation(
        self,
        columns: Mapping[str, Sequence[Any]],
        schema: Mapping[str, nw.dtypes.DType | None],
    ) -> DuckDBPyRelation:
        """A relation over `columns`, each an equal-length sequence of scalars.

        Column-wise, so nothing here is per-row: a caller's product of entities
        and labels never becomes Python objects.

        A `None` in `schema` is inferred, which an undeclared result's value
        column needs. An `Enum` is built as its `String` and cast by the insert -
        narwhals cannot construct an arrow enum - which is also what rejects a
        label the dtype does not declare.
        """
        buildable = {
            name: (nw.String() if isinstance(dtype, nw.Enum) else dtype)
            for name, dtype in schema.items()
        }
        return as_relation(
            nw.DataFrame.from_dict(
                dict(columns), schema=buildable, backend="pyarrow"
            ).lazy(),
            self.con,
        )

    def _stage_long(
        self, attribute: str, lazy: nw.LazyFrame, kind: str, dims: dict[str, Any]
    ) -> None:
        """Stage a long frame that supplies its own keys.

        Its keys are the entity and whatever coordinates it carries; `dims`
        supplies any the frame leaves out, so a caller may scope a whole frame
        to one connection without repeating it per row.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        table = self._ensure(kind, attribute)
        rel = as_relation(lazy, self.con)
        self._insert_long(
            rel, table, attribute, set(lazy.collect_schema().names()), dims
        )

    def _stage_derived(
        self,
        attribute: str,
        expr: nw.Expr,
        *,
        entity: Sequence[str] | None = None,
        kind: str = "inputs",
        **dims: Any,
    ) -> None:
        """Stage a value derived from the current one - the `Expr` form.

        Reads before it stages, so what it derives from is the resolved value
        *including earlier pending edits*, and two such calls compose. What is
        staged is the result, never the expression, so a committed layer holds
        ordinary rows and nothing stores that a value was derived.

        On a layered base the read is a fold, so this is the one edit whose cost
        scales with the ancestry rather than with the rows written.

        Unscoped, this derives from every row of the attribute across the types
        declaring it - there being no type keyword to narrow it.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        """
        source = self.outputs if kind == "outputs" else self.attributes
        # Once: three uses follow, and something iterating only once would be
        # exhausted by the first.
        names = None if entity is None else list(entity)
        if attribute not in source:
            frame = None
        else:
            frame = source[attribute]
            if names is not None:
                if kind == "inputs":
                    for name, ctype in self._resolve_types(names).items():
                        self._validate_attribute(ctype, attribute, dims, name=name)
                frame = frame.filter(nw.col("entity").is_in(names))
            for dim, value in dims.items():
                frame = frame.filter(nw.col(dim) == value)

        # A named target that resolves to no row is a failed change, not a
        # no-op: the caller asked for these rows to take a new value and there
        # is nothing to derive one from. With `entity=None` and no scope the
        # instruction is "whatever resolves", so an empty result is an answer.
        if names is not None or dims:
            # `head(1)`: `is_empty` is a `DataFrame` method, so the question
            # costs a collect either way - this one collects a single row.
            if frame is None or frame.select("entity").head(1).collect().is_empty():
                scope = ", ".join(
                    filter(
                        None,
                        [
                            f"entity={names}" if names is not None else "",
                            *(f"{d}={v!r}" for d, v in dims.items()),
                        ],
                    )
                )
                msg = (
                    f"no {attribute!r} rows resolve for {scope}, so there is "
                    f"no current value to derive from; `set` a value directly to "
                    f"create one (https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)"
                )
                raise KeyError(msg)
        if frame is None:
            return
        self._stage_resolved(frame.with_columns(expr.alias("value")), attribute, kind)

    def _stage_resolved(
        self, frame: nw.LazyFrame, attribute: str, kind: str = "inputs"
    ) -> None:
        """Stage an already-long frame carrying every key column.

        `value` needs no cast: the table is this attribute's own, so its column
        already has the attribute's type (`_empty_long`).
        """
        table = self._ensure(kind, attribute)
        # A coordinate the frame leaves out is one it broadcasts over, so it is
        # filled with a typed NULL rather than left to `INSERT ... BY NAME`:
        # projecting the table's full column list keeps the insert positional
        # and the types the table's own (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        present = set(frame.collect_schema().names())

        rel = as_relation(frame, self.con)
        duck_types = DuckTypes(rel)

        def column(c: str) -> Expression:
            if c in present:
                return col(c)
            if c == "attribute":
                return lit(attribute).alias(c)
            dtype = nw.Float64() if c == "breakpoint" else self._column_type(c)
            return duck_types.null(dtype).alias(c)

        rel.project(
            *(column(c) for c in self.schema.long_columns_for(attribute)),
            lit(next(_SEQ)).alias("_seq"),
        ).insert_into(table)

    def add(self, ctype: str, frame: Any) -> None:
        """Stage new components from a wide frame.

        Splits it: attributes addressed by `entity` alone stay in `dims/entity_type/`,
        varying ones become `inputs/` rows. Which is which comes from the
        schema, so this needs no framework registry.

        Not a sequence of `set` calls: a component exists by virtue of its
        member row, so staging attribute values for a name no layer declares is
        what `_validate_attribute` rejects. Adding a bus with no attributes makes
        the point - nothing to `set`, yet the bus must exist.

        `ctype` stays a parameter where `set` loses it: this is the call that
        establishes a name's type, so there is nothing yet to look it up in. It is also where uniqueness is enforced.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        if "entity" not in columns:
            msg = "`add` needs an `entity` column"
            raise ValueError(msg)
        self._require_unique(ctype, lazy)

        declared = self.schema.attributes_for(ctype)
        varying = [c for c in columns if declared.get(c) and declared[c].varying]
        # An attribute addressed by a group belongs to that group's table, not
        # the member frame - putting it there would introduce a column the
        # ancestors' files lack, which then reads as NULL for their rows. It
        # says so by naming the group among its `dims`, which is what replaced
        # a field of its own (https://energy-models.github.io/datarecord/design/record/#connections).
        # Only a group `entity` itself keys: a wide frame adding one component
        # can describe that component's own row of such a group (`bus`, for
        # `connection`), but not a `corridor`, which relates two entities
        # neither is "the" one being added here - that goes through `add_group`.
        by_group: dict[str, list[str]] = {}
        for c in columns:
            if not declared.get(c) or c in varying:
                continue
            for group in self.schema.groups_of(c):
                if "entity" in self.schema.group_key(group):
                    by_group.setdefault(group, []).append(c)
        ports = [c for cols in by_group.values() for c in cols]
        # A column the schema does not name goes to `dims/entity_type/`
        # unchanged, like any non-varying one.
        member_cols = [c for c in columns if c not in varying and c not in ports]

        rel = as_relation(lazy, self.con)
        table = self._ensure("entities")
        extra = [c for c in member_cols if c != "entity" and c not in self.schema.dims]
        self._widen(table, rel, extra)

        self._insert(
            rel,
            table,
            {
                "entity_type": lit(ctype),
                "deleted": lit(False),  # noqa: FBT003
                "_seq": lit(next(_SEQ)),
            },
        )
        for attribute in varying:
            # Always `inputs`: `add` declares components, and a component's
            # attribute values are inputs whatever a later solve produces.
            self._stage_long(
                attribute,
                lazy.select("entity", nw.col(attribute).alias("value")),
                "inputs",
                {},
            )
        # A group's coordinates name the row itself rather than being an
        # attribute of one, so they become that group's row; an attribute
        # addressed by the group rides along
        # (https://energy-models.github.io/datarecord/design/record/#connections).
        for group, group_cols in by_group.items():
            coordinates = self.schema.group_coordinates(group)
            extra = [c for c in group_cols if c not in coordinates]
            self.add_group(
                group,
                lazy.select(
                    "entity",
                    *(nw.col(c) for c in coordinates if c != "entity"),
                    *(nw.col(c) for c in extra),
                ),
            )

    def _widen(self, table: str, rel: DuckDBPyRelation, columns: Sequence[str]) -> None:
        """Add any of `columns` the staging table lacks, typed from the schema.

        A staging table starts with the key columns its `_COLUMNS` entry
        declares; what a caller passes beyond them - a component's attribute
        values, a group's own labels - is whatever their frame carries, so the
        columns are added as they are first seen rather than declared up front.

        The frame's own type where the schema declares none: `VARCHAR` would
        take a float column and store `'1234.5'`, which then reads back as text
        for every consumer of the frame.
        """
        existing = {c.lower() for c in self.con.table(table).columns}
        incoming = dict(zip(rel.columns, (str(t) for t in rel.types), strict=True))
        duck_types = DuckTypes(rel)
        for c in columns:
            if c.lower() in existing:
                continue
            spec_dtype = self.schema.value_type(c)
            dtype = duck_types(spec_dtype) if spec_dtype else incoming[c]
            self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {dtype}')
            existing.add(c.lower())

    def _stage_tombstones(
        self,
        kind: str,
        fixed: tuple[str, ...],
        keys: list[list[Any]],
    ) -> None:
        """Stage one `deleted` row per key.

        Shared by `remove` and `remove_group`, which differ only in their key
        columns - a group's key where `remove` carries the entity. One helper so
        the shape is derived from the column list rather than restated per
        caller, which is what let the two drift out of step.

        `keys` arrives row-oriented and is transposed to build the relation,
        columns being how every insert here crosses into DuckDB.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        table = self._ensure(kind)
        if not keys:
            return
        seq = next(_SEQ)
        by_column = dict(zip(fixed, zip(*keys, strict=True), strict=True))
        rel = self._values_relation(by_column, {c: self._column_type(c) for c in fixed})
        self._insert(
            rel,
            table,
            {
                "deleted": lit(True),  # noqa: FBT003
                "_seq": lit(seq),
            },
        )

    def remove(self, ctype: str, names: Sequence[str]) -> None:
        """Stage a tombstone per entity.

        Need not enumerate what it deletes: one row per key, and the fold
        applies it to every attribute. Nor scope it - a component exists or it
        does not, so a deletion removes it whole.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        self._stage_tombstones(
            "entities",
            ("entity_type", "entity"),
            [[ctype, name] for name in names],
        )

    def add_group(self, group: str, frame: Any) -> None:
        """Stage rows of one declared group from a frame carrying its coordinates.

        The one path every group is added through, a record's `connection`
        group included.

        No component type, which is no coordinate of a group
        (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        coordinates = self.schema.group_coordinates(group)
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        for required in coordinates:
            if required not in columns:
                msg = f"`add_group({group!r}, ...)` needs a {required!r} column"
                raise ValueError(msg)
        _rows = lazy.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure(group)
        extra = [c for c in columns if c not in coordinates]
        self._widen(table, as_relation(lazy, self.con), extra)
        extra_sql = "".join(f', "{c}"' for c in extra)
        coordinate_sql = ", ".join(f'"{c}"' for c in coordinates)
        self.con.execute(
            f"INSERT INTO {table} BY NAME "
            f"SELECT {coordinate_sql}{extra_sql}, "
            f"false AS deleted, {next(_SEQ)} AS _seq FROM _rows"
        )

    def remove_group(self, group: str, keys: Sequence[tuple[Any, ...]]) -> None:
        """Stage a tombstone per key, over one declared group's `group_key`.

        An `into` label is no part of a key: the tuple is removed, whatever
        label it carried.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        self._stage_tombstones(
            group, self.schema.group_key(group), [list(key) for key in keys]
        )

    # -- commit / rollback (https://energy-models.github.io/datarecord/design/working-record/#committing) -------------------------

    def rollback(self) -> None:
        """Clear every staged row without writing.

        Notes
        -----
        - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
        """
        for name in self._staged.values():
            self.con.execute(f"DROP TABLE IF EXISTS {name}")
        self._staged.clear()

    def _collapsed_inputs(self, attribute: str) -> DuckDBPyRelation | None:
        """One attribute's staged rows, last-write-wins per key, tombstones applied.

        None where nothing is staged for it, which is what says the base's rows
        stand alone.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows("inputs", attribute)
        if rel is None:
            return None
        # Per coordinate, not per input key: the input key excludes the dims an
        # attribute is not owned per (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override), so partitioning on it alone would
        # collapse a whole staged series to one row - two `set` calls at
        # different snapshots are not two writes to the same key.
        #
        # Intersected with the table's own columns: it carries this attribute's
        # coordinates and no others (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
        columns = set(self._long_columns(attribute))
        key = dict.fromkeys(
            c
            for c in (*self.schema.input_key, *self.schema.dims, "breakpoint")
            if c in columns
        )
        live = _latest_per(rel, key)
        dead = self._tombstoned()
        if dead is None:
            return live
        # A deleted component has no attributes, so the tombstone wins over a
        # staged value regardless of sequence (https://energy-models.github.io/datarecord/design/working-record/#committing).
        #
        # Matched on `name` alone: the tombstone carries a type and a staged
        # input row does not (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
        on = null_safe("l", "d", ("entity",))
        return live.set_alias("l").join(dead.set_alias("d"), on, how="anti")

    def _tombstoned(self) -> DuckDBPyRelation | None:
        """Component keys whose latest staged member row is a tombstone.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows("entities")
        if rel is None:
            return None
        cols = ("entity",)
        # An `add` after a `remove` means the component exists again, so only
        # the latest row per key counts (https://energy-models.github.io/datarecord/design/working-record/#committing).
        return (
            _latest_per(rel, cols)
            .filter(col("deleted"))
            .project(*(col(c) for c in cols))
        )

    def _collapsed_entities(self, kind: str) -> DuckDBPyRelation | None:
        """Staged member rows, last-write-wins per entity.

        The entity key, so no `entity_type` - partitioning on it too
        would keep both a tombstone and a later `add` under a different type.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows(kind)
        if rel is None:
            return None
        return _latest_per(rel, ("entity",))

    def _collapsed_group(self, group: str) -> DuckDBPyRelation | None:
        """One group's staged rows, last-write-wins per `group_key`.

        Not per coordinate: restating a tuple with a different `into` label is
        an edit to it rather than a second row.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows(group)
        if rel is None:
            return None
        return _latest_per(rel, self.schema.group_key(group))

    def _collapsed_axis(self, dim: str) -> DuckDBPyRelation | None:
        """Staged rows for one axis, last-write-wins per label *per column*.

        Not `_latest_per`, which picks a whole row: two `set` calls for two
        attributes on one axis each stage rows carrying only their own column,
        so the newest whole row would drop the other attribute's value. A
        `max_by` per column keeps each attribute's own latest, which is what
        makes the two calls commute.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows(f"{_AXIS_PREFIX}{dim}")
        if rel is None:
            return None
        staged = [a for a in self.schema.attributes_on(dim) if a in rel.columns]
        return rel.aggregate(
            [
                col(dim),
                # The FILTER is what keeps a sibling attribute's row, which
                # leaves this column NULL, from winning it.
                *(
                    sql(f"max_by({a}, _seq) FILTER (WHERE {a} IS NOT NULL)").alias(a)
                    for a in staged
                ),
            ],
            str(col(dim)),
        )

    # -- what commit writes (https://energy-models.github.io/datarecord/design/working-record/#committing) -----------------------------------------

    def _staged_dims(self) -> tuple[str, ...]:
        """Which axes have staged rows, in declaration order.

        Rows rather than tables: `_ensure` creates the table before the label
        checks run, so a rejected `set` leaves an empty one behind and a table
        that exists is not yet an edit.
        """
        staged = {
            k[len(_AXIS_PREFIX) :]
            for (k, _), _ in self._staged.items()
            if k.startswith(_AXIS_PREFIX)
        }
        return tuple(
            d
            for d in self.schema.dims
            if d in staged
            and self.con.table(self._table(f"{_AXIS_PREFIX}{d}")).limit(1).fetchone()
        )

    def _staged_axes(self) -> Frames:
        """The staged axis rows, completed from the base where the axis is owned whole.

        Only the axis files an edit touched. What each holds follows `partial`,
        as an attribute's rows do:

        - **`partial`** - the touched labels alone, the fold resolving the rest
          from the parent.
        - **not `partial`** - every label with its attributes, since a dim
          outside `partial` is one a layer owns entirely once it touches it.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        dims = self._staged_dims()
        if not dims:
            return EMPTY
        return LazyFrames(dims, lambda dim: nw.from_native(self._axis_layer(dim)))

    def _axis_layer(self, dim: str) -> DuckDBPyRelation:
        """One axis as this layer writes it, per `partial`.

        A `partial` axis contributes its edited labels; one owned whole
        contributes the resolved axis entire, which is `_axis_frame` - the same
        relation a read with pending edits answers.
        """
        if dim in self.schema.partial_dims:
            return cast("DuckDBPyRelation", self._collapsed_axis(dim))
        return as_relation(self._axis_frame(dim), self.con)

    def _axis_frame(self, dim: str) -> nw.LazyFrame:
        """One axis, staged columns over the base's row for each label.

        An outer join, since a `set` may name a label no layer has written: one
        side or the other may be missing, and both belong to the answer.
        """
        base = self.base.dims[dim]
        staged = self._collapsed_axis(dim)
        if staged is None:
            return base
        present = list(base.collect_schema().names())
        edited = [a for a in staged.columns if a != dim]
        rel = (
            as_relation(base, self.con)
            .set_alias("b")
            .join(staged.set_alias("s"), null_safe("b", "s", [dim]), how="outer")
        )
        # `coalesce` per column, not "staged row wins": an untouched label is
        # NULL on the staged side and would otherwise read as cleared. The label
        # itself coalesces the other way round, a staged-only row having no base.
        return nw.from_native(
            rel.project(
                *(
                    coalesce(col("s", dim), col("b", dim)).alias(dim)
                    if c == dim
                    else coalesce(col("s", c), col("b", c)).alias(c)
                    if c in edited
                    else col("b", c).alias(c)
                    for c in present
                ),
                *(col("s", c).alias(c) for c in edited if c not in present),
            )
        )

    def _staged_entities(self, kind: str) -> Frames:
        """The staged member rows, keyed by component type."""
        rel = self._collapsed_entities(kind)
        if rel is None:
            return EMPTY
        types = distinct_values(rel, "entity_type")
        return LazyFrames(
            types,
            lambda ctype: nw.from_native(rel.filter(col("entity_type") == lit(ctype))),
        )

    def _staged_groups(self) -> Frames:
        """The staged group rows, keyed by group - one frame each."""
        staged = {
            g: rel
            for g in self.schema.groups
            if (rel := self._collapsed_group(g)) is not None
        }
        return LazyFrames(tuple(staged), lambda group: nw.from_native(staged[group]))

    def _staged_attributes(self) -> Frames:
        """The staged rows - what a patch layer holds.

        No completion step: a dim owned whole was carried into the staging table
        when it was created (`_seed_owned_whole`), so the rows here are already
        the layer's full extent.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        names = self._staged_attribute_names()
        if not names:
            return EMPTY

        def frame(attr: str) -> nw.LazyFrame:
            return nw.from_native(
                cast("DuckDBPyRelation", self._collapsed_inputs(attr))
            )

        return LazyFrames(names, frame)

    def _writable_entity_types(self) -> Frames:
        """The resolved member frames, in the shape `write_record` persists.

        A resolved frame drops `entity_type` (the type is the key it was
        looked up by) while a layer's file carries it, so it is added
        back for any type the staging area did not already rebuild.

        Components only: a group's file carries no type, so `groups` already
        hands over the shape written.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        frames = self.entity_types

        def build(ctype: str) -> nw.LazyFrame:
            frame = frames[ctype]
            if "entity_type" in frame.collect_schema().names():
                return frame
            return frame.with_columns(entity_type=nw.lit(ctype))

        return LazyFrames(tuple(frames), build)

    def staged_only(self) -> _Written:
        """The staged rows alone - what a patch layer holds.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        return _Written(
            schema=self.schema,
            dims=self._staged_axes(),
            entity_types=self._staged_entities("entities"),
            groups=self._staged_groups(),
            attributes=self._staged_attributes(),
            # No completion counterpart: results are complete as produced, never
            # a partial override of a parent's, so there is nothing to carry
            # forward from the base (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override, https://energy-models.github.io/datarecord/design/read-path/#outputs).
            outputs=self.outputs,
        )

    def flattened(self) -> _Written:
        """The staged rows over what the record already reads.

        `attributes` is this record's own, since a `WorkingRecord` already reads
        the base with its pending edits applied - which is exactly the
        flattened result.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return _Written(
            schema=self.schema,
            dims=self.dims,
            entity_types=self._writable_entity_types(),
            groups=self.groups,
            attributes=self.attributes,
            outputs=self.outputs,
        )

    def _base_revision(self) -> Any:
        """The `Revision` this record's base resolves, for `NewChild()`'s default.

        Only a layered base has one: a `DirectoryRecord` or a framework object
        is not a node in the tree, so there is no parent to branch from and the
        caller must name one.
        """
        from datarecord.layered.revision import LayeredRecord, Revision

        if not isinstance(self.base, LayeredRecord):
            msg = (
                f"`NewChild()` needs a revision to branch from, and this "
                f"`WorkingRecord`'s base is a {type(self.base).__name__} rather than a "
                f"node in a layer tree; pass one as `NewChild(revision)`, or commit to "
                f"a standalone record with `Directory(uri)` (https://energy-models.github.io/datarecord/design/working-record/#committing)"
            )
            raise ValueError(msg)
        return Revision.get(self.base.node_cache.revision_id, self.con)

    def commit(self, target: Target) -> Any:
        """Write everything staged and clear it.

        Returns
        -------
        The new child for a `NewChild` target, so the caller can read what it
        just wrote without going back to the record table; `None` for a
        `Directory`, which belongs to no record.

        The layer lands in the *child*, never in the node that was branched
        from - layers are write-once - so it is the returned node that
        reads back the edits.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        from datarecord.layered.write import write_record  # circular at module level

        if isinstance(target, NewChild):
            parent = (
                target.record if target.record is not None else self._base_revision()
            )
            child = parent.child()
            write_record(child.id, self.staged_only(), self.con)
            self.rollback()
            return child
        write_record(None, self.flattened(), self.con, uri=target.uri)
        self.rollback()
        return None


def _column_type(schema: Schema, column: str) -> nw.dtypes.DType:
    """A staged column's declared type, for a typed NULL or literal.

    Every column a staging table has is a declared dim or a structural one,
    both of which the schema types - so a missing type is a disagreement
    between the table's shape and the schema, not a column to guess at.

    Raises
    ------
    ValueError
        If the schema declares no type for `column`.
    """
    dtype = schema.column_type(column)
    if dtype is None:
        msg = (
            f"the schema declares no type for {column!r}, which a staged "
            f"row needs to fill (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
        )
        raise ValueError(msg)
    return dtype


def _entity_columns(schema: Schema) -> dict[str, nw.dtypes.DType]:  # noqa: ARG001 - shape is fixed
    return {
        "entity_type": nw.String(),
        "entity": nw.String(),
        "deleted": nw.Boolean(),
        "_seq": nw.Int64(),
    }


def _group_columns(schema: Schema, group: str) -> dict[str, nw.dtypes.DType]:
    """One group's staged columns: its coordinates, and the fold's own.

    No `entity_type`, which is no coordinate of a group
    (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).

    The coordinates alone: a column a caller passes that is not one - an
    attribute over the group, a framework's own label - is added by `ALTER
    TABLE` as it is first seen, like a component's.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    return {
        **{c: _column_type(schema, c) for c in schema.group_coordinates(group)},
        "deleted": nw.Boolean(),
        "_seq": nw.Int64(),
    }


def _axis_columns(schema: Schema, dim: str) -> dict[str, nw.dtypes.DType]:
    """One axis's staged columns: its key, plus the attributes it carries.

    The shape of `dims/{dim}.parquet` for the attributes addressed by `dim`
    alone (`attributes_on`). No classification column: a group `into` this axis
    is its own file, which no edit stages here.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return {
        **{c: _column_type(schema, c) for c in schema.axis_key(dim)},
        **{a: schema.value_type(a) or nw.String() for a in schema.attributes_on(dim)},
        "_seq": nw.Int64(),
    }


# The entity kinds only. A long kind is staged per attribute, so its columns
# take the attribute and come from `_empty_long` instead; `outputs/` shares
# that shape with `inputs/`, differing only in not overlaying, which is a
# read-path property rather than a shape one (https://energy-models.github.io/datarecord/design/read-path/#outputs).
# An axis kind is staged per dim rather than per attribute, since one axis file
# carries every attribute addressed by that dim alone.
_COLUMNS = {
    "entities": _entity_columns,
}

# A staging kind, so it becomes part of a table name: no punctuation a SQL
# identifier would need quoting for, and no collision with a group's name,
# which `Schema` already rejects for colliding with a declared dim.
_AXIS_PREFIX = "axis_of_"
