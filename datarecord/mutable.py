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
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

import narwhals as nw
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import ex_all, fn, union_all_by_name
from datarecord.record import EMPTY, Flags, Frames, LazyFrames, Record
from datarecord.schema import Schema

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


@dataclass(frozen=True)
class Pending:
    """What a `WorkingRecord` would write, without writing it.

    A derived summary, not a second place rows live: a `GROUP BY` over the
    staging tables, computed on access and discarded.

    Notes
    -----
    - [pending](https://energy-models.github.io/datarecord/design/working-record/#pending)
    """

    attributes: Mapping[str, int] = field(default_factory=dict)
    """Staged attribute rows, per attribute name."""

    components: Mapping[str, int] = field(default_factory=dict)
    """Components staged to exist, per component type."""

    connections: Mapping[str, int] = field(default_factory=dict)
    """Connections staged to exist, per component type."""

    tombstones: Mapping[str, int] = field(default_factory=dict)
    """Deletions staged, per component type - components and connections both."""

    def __bool__(self) -> bool:
        """Whether anything is staged."""
        return bool(
            self.attributes or self.components or self.connections or self.tombstones
        )


# -- value normalisation (https://energy-models.github.io/datarecord/design/working-record/#set) ----------------------------------------------


def _as_relation(frame: nw.LazyFrame, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """One narwhals frame as a DuckDB relation, without collecting where possible.

    A DuckDB-backed frame is already a plan, so it passes straight through; any
    other backend is collected to arrow and re-registered, which is the same
    boundary `write_record` crosses.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    native = frame.to_native()
    if isinstance(native, DuckDBPyRelation):
        return native
    arrow = frame.collect(backend="pyarrow").to_native()  # noqa: F841 - by name
    return con.sql("FROM arrow")


def _incoming(frame: Any, con: DuckDBPyConnection) -> nw.LazyFrame:
    """A caller's frame as a lazy frame on `con`, whatever backend it arrived on.

    Every edit converts at this one point, so the steps behind it join and union
    in narwhals without minding where the frame came from.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    return nw.from_native(_as_relation(nw.from_native(frame).lazy(), con)).lazy()


def _null_safe_on(
    columns: Iterable[str], alias_a: str = "b", alias_b: str = "s"
) -> Expression:
    """A NULL-safe join condition over `columns`, for two aliases.

    An `Expression` rather than a SQL string: `join` takes one directly, so the
    condition composes with `&` (`_overlay`'s broadcast arms) instead of being
    spliced into a template.
    """
    return ex_all(
        sql(f"{col(alias_a, c)} IS NOT DISTINCT FROM {col(alias_b, c)}")
        for c in columns
    )


def _without(columns: Sequence[str], drop: Sequence[str]) -> list[Expression]:
    """`columns` minus `drop`, as projectable column expressions."""
    return [col(c) for c in columns if c not in drop]


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


def normalise_value(
    value: Any, names: Sequence[str] | None, axis_labels: Mapping[str, Sequence[Any]]
) -> tuple[list[str] | None, list[Any], dict[str, list[Any]]]:
    """One of `set`'s four `value` forms as per-name values.

    Parameters
    ----------
    value
        Scalar, sequence, mapping, or a one-dimensional labelled series.
    names
        The names to broadcast or align to.
    axis_labels
        Per dim, its resolved labels - used to break the labelled-series
        ambiguity by membership rather than by dtype.

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
        If a sequence's length does not match `names`, or a labelled series'
        index matches both a name set and an axis.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    labels = _series_index(value)
    if labels is not None:
        # A series is genuinely ambiguous: its index may hold names or axis
        # labels. Index dtype does not settle it, since an axis label may be a
        # string like a name, so the tie is broken by membership (https://energy-models.github.io/datarecord/design/working-record/#set).
        matches_axis = [
            dim for dim, values in axis_labels.items() if set(labels) <= set(values)
        ]
        matches_names = names is not None and set(labels) <= set(names)
        if matches_axis and matches_names:
            msg = (
                f"index {labels!r} matches both the names and the "
                f"{matches_axis[0]!r} axis; rejected rather than guessed"
            )
            raise ValueError(msg)
        if matches_axis:
            dim = matches_axis[0]
            return None, list(value), {dim: list(labels)}
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
    components: Frames
    connections: Frames
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


class WorkingRecord:
    """A `Record` that accepts edits and materialises them on commit.

    Satisfies `Record`, and what it reads is the data *with its pending edits
    applied* - so an edit reads back, or the record is handed to
    something that only knows `Record`, without committing.

    Staged rows live in three connection-scoped DuckDB tables, the *only* place
    a staged row exists: `pending` counts them and the reads fold them, neither
    holding a copy.

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
        self._staged: set[str] = set()

    # -- staging tables -----------------------------------------------------

    def _table(self, kind: str) -> str:
        return f"staged_{kind}_{self._id}"

    def _ensure(self, kind: str) -> str:
        """The staging table for `kind`, created on first use."""
        name = self._table(kind)
        if kind not in self._staged:
            self.con.execute(f"CREATE TABLE {name} ({_COLUMNS[kind](self.schema)})")
            self._staged.add(kind)
        return name

    def _rows(self, kind: str) -> DuckDBPyRelation | None:
        if kind not in self._staged:
            return None
        return self.con.table(self._table(kind))

    # -- Record, over base plus pending (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) -----------------------------

    @property
    def schema(self) -> Schema:
        return self.base.schema

    @property
    def dims(self) -> Frames:
        return self.base.dims

    @property
    def components(self) -> Frames:
        """Base members with pending additions and tombstones applied.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return self._entity_frames("components")

    @property
    def connections(self) -> Frames:
        """Base connections with pending ones applied.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return self._entity_frames("connections")

    def _entity_frames(self, kind: str) -> Frames:
        base = self.base.components if kind == "components" else self.base.connections
        staged = self._collapsed_entities(kind)
        if staged is None:
            return base
        types = tuple(
            r[0] for r in staged.project("component_type").distinct().fetchall()
        )
        keys = tuple(dict.fromkeys((*base, *types)))
        return LazyFrames(keys, lambda ctype: self._entity_frame(kind, ctype))

    def _entity_frame(self, kind: str, ctype: str) -> nw.LazyFrame:
        """One type's members or connections, staged rows over the base ones.

        Union by name rather than a keyed overlay: a staged member row carries
        whatever columns the caller passed, and a name the base also holds is
        an edit to it, so the staged row wins on the entity key while a name
        only the base holds passes through.
        """
        base = self.base.components if kind == "components" else self.base.connections
        staged = self._collapsed_entities(kind)
        assert staged is not None
        mine = staged.filter(col("component_type") == lit(ctype))
        if ctype not in base:
            return nw.from_native(mine.filter(~col("deleted")))

        key = (
            ("entity",)
            if kind == "components"
            else self.schema.group_coordinates("connection")
        )
        frame = base[ctype]
        # `collect_schema` reads names without materialising, and `_as_relation`
        # keeps a DuckDB-backed frame as the plan it already is: the long schema
        # promises a record hands over unmaterialised frames, and the pending-edit
        # overlay prices a read with
        # pending edits at what one more layer costs.
        present = set(frame.collect_schema().names())
        on = _null_safe_on([c for c in key if c in present])
        return nw.from_native(
            self._entity_union(_as_relation(frame, self.con), mine, on, ctype)
        )

    def _entity_union(
        self,
        base: DuckDBPyRelation,
        staged: DuckDBPyRelation,
        on: Expression,
        ctype: str,
    ) -> DuckDBPyRelation:
        """`staged` over `base` on the entity key, tombstones removed.

        `component_type` and `deleted` are supplied whatever the base carried:
        a resolved frame drops them (the type is the key it was looked up by)
        while `write_record` needs them back, so one shape serves both.
        """
        drop = ("component_type", "deleted")
        b = (
            base.project(*_without(base.columns, drop))
            if any(c in base.columns for c in drop)
            else base
        )
        s = staged.project(*_without(staged.columns, drop), col("deleted"))
        # The staged row wins on the entity key, so the base keeps only what the
        # staging area does not restate; the two are then unioned by name, since
        # a staged member row carries whatever columns the caller passed.
        kept = b.set_alias("b").join(s.set_alias("s"), on, how="anti")
        live = s.filter(~col("deleted")).project(star(exclude=["deleted"]))
        return union_all_by_name([kept, live], self.con).project(
            star(),
            lit(ctype).alias("component_type"),
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
        rel = self._rows("inputs")
        if rel is None:
            return ()
        rows = rel.project("attribute").distinct().order("attribute").fetchall()
        return tuple(r[0] for r in rows)

    def _attribute_frame(self, attribute: str) -> nw.LazyFrame:
        base = (
            self.base.attributes[attribute]
            if attribute in self.base.attributes
            else None
        )
        rel = self._rows("inputs")
        if rel is None:
            if base is None:
                raise KeyError(attribute)
            return base
        staged = (
            self._collapsed_inputs()
            .filter(col("attribute") == lit(attribute))
            .project(*self._typed_value(attribute))
        )
        if base is None:
            return nw.from_native(staged)
        # The staged rows are the last layer, so they win per key (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits).
        return nw.from_native(self._overlay(base.to_native(), staged))

    def _overlay(
        self, base: DuckDBPyRelation, staged: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        """`staged` over `base`, last-writer-wins per coordinate.

        Per *coordinate*, not per input key: the input key excludes the dims an
        attribute is not owned per, so keying on it alone would let one
        staged snapshot displace the base's whole series on read - reporting a
        loss the commit path is careful not to make (`_restated`).

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
        # what keeps the rest of the series (`_restated` writes it out at
        # commit).
        fixed = (*self.schema.input_key, "breakpoint")
        # The dims a NULL broadcasts over, minus those the key already fixes.
        # Read off `broadcast_dims` rather than subtracted from `schema.dims`:
        # an address coordinate is in neither, and a subtraction would put one
        # here the moment it left the key.
        key = set(self.schema.input_key)
        broadcast = tuple(d for d in self.schema.broadcast_dims if d not in key)
        on = ex_all(
            [
                _null_safe_on(dict.fromkeys(fixed)),
                *(
                    col("s", d).isnull()
                    | sql(f"{col('s', d)} IS NOT DISTINCT FROM {col('b', d)}")
                    for d in broadcast
                ),
            ]
        )
        kept = base.set_alias("b").join(staged.set_alias("s"), on, how="anti")
        return union_all_by_name([kept, staged], self.con).project(
            *(col(c) for c in self._long_columns())
        )

    def _typed_value(self, attribute: str) -> list[Expression]:
        """A projection of `_long_columns` with `value` cast to its dtype.

        `value` is staged as text because one staging table holds every
        attribute's values (`_input_columns`); here the attribute is known, so
        the declared dtype applies. One attribute is one spec, so there is one
        answer rather than a per-type agreement to hope for.

        `TRY_CAST`, not `cast`: a value that does not parse as the declared dtype
        reads as NULL rather than failing the whole relation, which is what the
        text staging column makes possible in the first place.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        dtype = self.schema.value_type(attribute) or "DOUBLE"
        return [
            sql(f'TRY_CAST("value" AS {dtype})').alias("value")
            if c == "value"
            else col(c)
            for c in self._long_columns()
        ]

    def _long_columns(self) -> tuple[str, ...]:
        # Derived, never spelled: `entity` and a group's coordinates are
        # declared dims, so naming them beside `schema.dims` would emit each
        # twice. The staging table's own order (`_input_columns`).
        return (*self.schema.dims, "attribute", "breakpoint", "value")

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

    def _restated(self, attribute: str, staged: DuckDBPyRelation) -> DuckDBPyRelation:
        """`staged` plus the base rows a non-partial axis obliges it to carry.

        The one commit-time read of parent data. Note the two
        keys below: `scope` excludes the whole-owned dims, so one touched
        snapshot pulls in that key's others; `coordinate` adds them back, so a
        base row is dropped only where the edit named that exact coordinate.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        whole = self._owned_whole(attribute)
        if not whole:
            return staged
        if attribute not in self.base.attributes:
            return staged

        scope = [c for c in self.schema.input_key if c not in whole]
        coordinate = [*scope, *whole, "breakpoint"]
        base = _as_relation(self.base.attributes[attribute], self.con)
        # Semi-join first to the keys this edit touched, then anti-join away the
        # exact coordinates it named: what survives is the rest of the extent the
        # layer now owns whole and so must carry.
        carried = (
            base.set_alias("b")
            .join(staged.set_alias("s"), _null_safe_on(scope), how="semi")
            .set_alias("b")
            .join(staged.set_alias("s"), _null_safe_on(coordinate), how="anti")
        )
        return union_all_by_name([staged, carried], self.con).project(
            *(col(c) for c in self._long_columns())
        )

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
        rel = self._rows("outputs")
        if rel is None:
            return EMPTY
        names = tuple(
            r[0]
            for r in rel.project("attribute").distinct().order("attribute").fetchall()
        )
        return LazyFrames(
            names,
            lambda attr: nw.from_native(
                rel.filter(col("attribute") == lit(attr)).project(
                    *self._typed_value(attr)
                )
            ),
        )

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
        rel = self._rows("inputs")
        if rel is None:
            return out
        names = self._resolved_names(ctype)
        if not names:
            return out
        # Only the dims a NULL broadcasts over: "did a row set this" is not a
        # question about `entity` or a group's coordinate, which address the
        # row rather than expanding against an axis.
        dims = self.schema.broadcast_dims
        rows = (
            rel.filter(col("entity").isin(*(lit(n) for n in names)))
            .aggregate(
                [
                    col("attribute"),
                    *(fn.bool_or(col(d).isnotnull()).alias(f"v_{d}") for d in dims),
                    *(fn.bool_or(col(d).isnull()).alias(f"b_{d}") for d in dims),
                    fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
                ]
            )
            .fetchall()
        )
        n = len(dims)
        for row in rows:
            attribute = row[0]
            varies = frozenset(
                d for d, on in zip(dims, row[1 : 1 + n], strict=True) if on
            )
            broadcast = frozenset(
                d for d, on in zip(dims, row[1 + n : 1 + 2 * n], strict=True) if on
            )
            was = out.get(attribute)
            out[attribute] = Flags(
                varies=(was.varies if was else frozenset()) | varies,
                broadcast=(was.broadcast if was else frozenset()) | broadcast,
                breakpoints=(was.breakpoints if was else False) or bool(row[1 + 2 * n]),
            )
        return out

    # -- edits (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one, https://energy-models.github.io/datarecord/design/working-record/#add-remove) ----------------------------------------

    def _axis_labels(self) -> dict[str, list[Any]]:
        labels: dict[str, list[Any]] = {}
        for dim in self.schema.dims:
            if dim in self.base.dims:
                frame = self.base.dims[dim].collect().to_native()
                labels[dim] = list(frame[dim]) if dim in frame.columns else []
        return labels

    def _resolved_names(self, ctype: str) -> list[str]:
        """Every name `ctype` currently resolves to, base plus staged.

        Through narwhals rather than the native frame: a backend's column
        yields its own scalar type (a `pyarrow.StringScalar`, say), which
        would compare unequal to the plain strings an edit names.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        if ctype not in self.components:
            return []
        frame = self.components[ctype].select("entity").collect()
        return [str(n) for n in frame["entity"].to_list()]

    def _name_types(self) -> nw.LazyFrame | None:
        """`(name, component_type)` over everything this record resolves.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        parts = [
            self.components[ctype]
            .select("entity")
            .with_columns(component_type=nw.lit(ctype))
            for ctype in self.components
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
                known.filter(nw.col("component_type") != ctype),
                on="entity",
                how="inner",
            )
            .unique(["entity", "component_type"])
            .select("entity", "component_type")  # the order `iter_rows` unpacks
            .collect()
        )
        if not clashing.is_empty():
            # Sorted here rather than in the query: the message must be
            # deterministic, and this is a handful of rows.
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
                .select("entity", "component_type")
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

    def _validate_attribute(
        self,
        ctype: str,
        attribute: str,
        dims: Mapping[str, Any],
        *,
        name: str | None = None,
    ) -> None:
        """One name's attribute checks, against the spec of *its* type.

        Inputs only: a result attribute is not schema-declared at all -
        `Tool.results` derives which attributes count as results from the
        framework's own registry, and `write_record` persists `outputs/` without
        consulting the schema. So an unknown attribute name is an
        error for an input and simply unknowable for a result.

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
        # attributes are declared record-wide: `component_types` answers the
        # first, `attributes_for` the second, and a type carrying nothing is
        # not the same as a type the schema never declared.
        if ctype not in self.schema.component_types:
            msg = f"the schema declares no component type {ctype!r}{who}"
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
        **dims: Any,
    ) -> None:
        """Stage an attribute value for a group of components.

        `value` takes five forms: a scalar broadcast to every name, a sequence
        aligned positionally to `names`, a mapping keyed by name, a long frame
        supplying its own keys, and a narwhals expression - which is a *function
        of the current value* rather than a value, so it reads before it stages
        and two such calls compose.

        No `component_type` parameter: the type is looked up from the entity,
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
            if "component_type" in lazy.collect_schema().names():
                msg = (
                    f"`set({attribute!r}, <frame>)` was given a `component_type` "
                    f"column; names are unique across every type, so an attribute row "
                    f"carries no type and the column would be ignored (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)"
                )
                raise ValueError(msg)
            if kind == "inputs":
                self._validate_frame(lazy, attribute, dims)
            else:
                self._validate_dims(dims)
            self._stage_long(attribute, lazy, kind, dims)
            return

        if isinstance(value, nw.Expr):
            self._validate_dims(dims)
            self._stage_derived(attribute, value, entity=entity, kind=kind, **dims)
            return

        target = (
            list(entity) if entity is not None else self._names_declaring(attribute)
        )
        keys, values, per_dim = normalise_value(value, target, self._axis_labels())
        if keys is None:
            keys = target
            if len(values) == 1 and len(keys) > 1:
                values = values * len(keys)
        self._validate_dims(dims)
        if kind == "inputs":
            # One lookup serves both: rejects a name with no member row, and
            # returns the type whose spec is checked (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
            for name, ctype in self._resolve_types(keys).items():
                self._validate_attribute(ctype, attribute, dims, name=name)

        seq = next(_SEQ)
        table = self._ensure(kind)
        # Positional, so the order must be the staging table's own
        # (`_input_columns`): every declared dim, then the fixed three.
        dim_cols = self.schema.dims
        placeholders = ", ".join(["?"] * (len(dim_cols) + 4))

        def row(name: str, val: Any, label_of: dict[str, Any]) -> list[Any]:
            # The entity comes from `entity=`, every other coordinate from
            # `dims` - which is why no coordinate is spelled here.
            return [
                name if d == "entity" else label_of.get(d, dims.get(d))
                for d in dim_cols
            ] + [attribute, None, val, seq]

        rows: list[list[Any]] = []
        if per_dim:
            (dim, labels) = next(iter(per_dim.items()))
            for label, val in zip(labels, values, strict=True):
                rows.extend(row(name, val, {dim: label}) for name in keys)
        else:
            rows.extend(
                row(name, val, {}) for name, val in zip(keys, values, strict=True)
            )
        self.con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    def _names_declaring(self, attribute: str) -> list[str]:
        """Every resolved name whose type declares `attribute` - `names=None`.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        return [
            name
            for ctype in sorted(self.schema.types_declaring(attribute))
            if ctype in self.components
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
        _in_long = lazy.to_native()  # noqa: F841 - referenced by name below
        table = self._ensure(kind)
        # A coordinate the frame does not carry comes from `dims` if the caller
        # scoped it there, and is otherwise NULL - "every value of it" (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        # Typed as the schema declares it either way, so the insert matches the
        # staging table's column type rather than relying on a VARCHAR coercion.
        present = set(lazy.collect_schema().names())
        params: list[Any] = []

        def column(d: str) -> str:
            if d in present:
                return f'"{d}"'
            if d in dims:
                params.append(dims[d])
                return f'?::{self.schema.column_type(d)} AS "{d}"'
            return f'NULL::{self.schema.column_type(d)} AS "{d}"'

        cols = ", ".join(column(d) for d in self.schema.dims)
        self.con.execute(
            f"INSERT INTO {table} SELECT {cols}, "
            f"? AS attribute, NULL::DOUBLE AS breakpoint, "
            f"value::VARCHAR AS value, ? "
            f"FROM _in_long",
            [*params, attribute, next(_SEQ)],
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
        if attribute not in source:
            frame = None
        else:
            frame = source[attribute]
            if entity is not None:
                if kind == "inputs":
                    for name, ctype in self._resolve_types(list(entity)).items():
                        self._validate_attribute(ctype, attribute, dims, name=name)
                frame = frame.filter(nw.col("entity").is_in(list(entity)))
            for dim, value in dims.items():
                frame = frame.filter(nw.col(dim) == value)

        # A named target that resolves to no row is a failed change, not a
        # no-op: the caller asked for these rows to take a new value and there
        # is nothing to derive one from. With `entity=None` and no scope the
        # instruction is "whatever resolves", so an empty result is an answer.
        if entity is not None or dims:
            if frame is None or frame.select("entity").collect().is_empty():
                scope = ", ".join(
                    filter(
                        None,
                        [
                            f"entity={list(entity)}" if entity is not None else "",
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
        self._stage_resolved(frame.with_columns(expr.alias("value")), kind)

    def _stage_resolved(self, frame: nw.LazyFrame, kind: str = "inputs") -> None:
        """Stage an already-long frame carrying every key column.

        `value` is cast to text on the way in, since the staging table holds
        every attribute's values in one column (`_input_columns`).
        """
        _upd = frame.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure(kind)
        cols = ", ".join(
            '"value"::VARCHAR AS "value"' if c == "value" else f'"{c}"'
            for c in self._long_columns()
        )
        self.con.execute(
            f"INSERT INTO {table} BY NAME SELECT {cols}, {next(_SEQ)} AS _seq FROM _upd"
        )

    def add(self, ctype: str, frame: Any) -> None:
        """Stage new components from a wide frame.

        Splits it: attributes varying over nothing stay in `dims/components/`,
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
        # A connection attribute belongs to `dims/connections/`, keyed by bus
        # (https://energy-models.github.io/datarecord/design/record/#connections) - putting it in the member frame would introduce a column the
        # ancestors' files lack, which then reads as NULL for their rows.
        ports = [
            c
            for c in columns
            if declared.get(c) and declared[c].bus == "connection" and c not in varying
        ]
        # A column the schema does not name goes to `dims/components/`
        # unchanged, like any non-varying one.
        member_cols = [
            c for c in columns if c not in varying and c not in ports and c != "role"
        ]

        _add = lazy.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure("components")
        dim_cols = ""
        extra = [c for c in member_cols if c != "entity" and c not in self.schema.dims]
        # The staging table starts with the key columns only (`_COLUMNS`); a
        # wide frame's attribute columns are whatever the caller passed, so
        # they are added as they are first seen rather than declared up front.
        existing = {c.lower() for c in self.con.table(table).columns}
        for c in extra:
            if c.lower() in existing:
                continue
            dtype = self.schema.value_type(c) or "VARCHAR"
            self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {dtype}')
            existing.add(c.lower())
        extra_sql = "".join(f', "{c}"' for c in extra)
        self.con.execute(
            f"INSERT INTO {table} BY NAME "
            f"SELECT $ctype AS component_type, entity"
            f"{', ' + dim_cols if dim_cols else ''}"
            f"{extra_sql}, false AS deleted, {next(_SEQ)} AS _seq FROM _add",
            {"ctype": ctype},
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
        # `bus` names the connection itself rather than being an attribute of
        # one, so it becomes the connection row; any other port attribute
        # rides along on it (https://energy-models.github.io/datarecord/design/record/#connections). `role` is passed through when the caller
        # supplies it and left NULL otherwise: what the roles of a type's ports
        # are is a framework's vocabulary, not this layer's to invent.
        if "bus" in ports:
            self.connect(
                ctype,
                lazy.select(
                    "entity",
                    nw.col("bus"),
                    *(nw.col(c) for c in ports if c != "bus"),
                    *([nw.col("role")] if "role" in columns else []),
                ),
            )

    def _stage_tombstones(
        self,
        kind: str,
        fixed: tuple[str, ...],
        keys: list[list[Any]],
    ) -> None:
        """Stage one `deleted` row per key.

        Shared by `remove` and `disconnect`, which differ only in their key
        columns - `disconnect` carries the group's coordinates where `remove`
        carries the entity. One helper so the placeholder count is derived
        from the column list rather than restated per caller, which is what
        let the two drift out of step.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        columns = (*fixed, "deleted", "_seq")
        quoted = ", ".join(f'"{c}"' for c in columns)
        table = self._ensure(kind)
        seq = next(_SEQ)
        self.con.executemany(
            f"INSERT INTO {table} ({quoted}) "
            f"VALUES ({', '.join(['?'] * len(columns))})",
            [[*key, True, seq] for key in keys],
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
            "components",
            ("component_type", "entity"),
            [[ctype, name] for name in names],
        )

    def connect(self, ctype: str, frame: Any) -> None:
        """Stage connection rows from a frame carrying `name` and `bus`.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        for required in ("entity", "bus"):
            if required not in columns:
                msg = f"`connect` needs a {required!r} column"
                raise ValueError(msg)
        _conn = lazy.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure("connections")
        extra = [c for c in columns if c not in ("entity", "bus")]
        extra_sql = "".join(f', "{c}"' for c in extra)
        self.con.execute(
            f"INSERT INTO {table} BY NAME "
            f"SELECT $ctype AS component_type, entity, bus{extra_sql}, "
            f"false AS deleted, {next(_SEQ)} AS _seq FROM _conn",
            {"ctype": ctype},
        )

    def disconnect(self, ctype: str, pairs: Sequence[tuple[str, str]]) -> None:
        """Stage a tombstone per `(entity, bus)`.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        self._stage_tombstones(
            "connections",
            ("component_type", *self.schema.group_coordinates("connection")),
            [[ctype, name, bus] for name, bus in pairs],
        )

    # -- pending / commit / rollback (https://energy-models.github.io/datarecord/design/working-record/#pending, https://energy-models.github.io/datarecord/design/working-record/#committing) -------------------------

    @property
    def pending(self) -> Pending:
        """Counts over the staging tables, computed on access.

        Notes
        -----
        - [pending](https://energy-models.github.io/datarecord/design/working-record/#pending)
        """

        def counts(
            kind: str, by: str, *, deleted: bool | None = None
        ) -> dict[str, int]:
            rel = self._rows(kind)
            if rel is None:
                return {}
            if deleted is not None:
                rel = rel.filter(col("deleted") if deleted else ~col("deleted"))
            rows = rel.aggregate([col(by), fn.count_star().alias("n")]).fetchall()
            return {r[0]: r[1] for r in rows}

        # `tombstones` spans both entity kinds: a `disconnect` is a deletion
        # like a `remove`, so counting only components would report a staged
        # one as nothing pending (https://energy-models.github.io/datarecord/design/working-record/#pending).
        dead = counts("components", "component_type", deleted=True)
        for ctype, n in counts("connections", "component_type", deleted=True).items():
            dead[ctype] = dead.get(ctype, 0) + n
        return Pending(
            attributes=counts("inputs", "attribute"),
            components=counts("components", "component_type", deleted=False),
            connections=counts("connections", "component_type", deleted=False),
            tombstones=dead,
        )

    def rollback(self) -> None:
        """Clear every staged row without writing.

        Notes
        -----
        - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
        """
        for kind in list(self._staged):
            self.con.execute(f"DROP TABLE IF EXISTS {self._table(kind)}")
        self._staged.clear()

    def _collapsed_inputs(self) -> DuckDBPyRelation:
        """Staged attribute rows, last-write-wins per key, tombstones applied.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows("inputs")
        assert rel is not None
        # Per coordinate, not per input key: the input key excludes the dims an
        # attribute is not owned per (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override), so partitioning on it alone would
        # collapse a whole staged series to one row - two `set` calls at
        # different snapshots are not two writes to the same key.
        key = dict.fromkeys((*self.schema.input_key, *self.schema.dims, "breakpoint"))
        live = _latest_per(rel, key)
        dead = self._tombstoned()
        if dead is None:
            return live
        # A deleted component has no attributes, so the tombstone wins over a
        # staged value regardless of sequence (https://energy-models.github.io/datarecord/design/working-record/#committing).
        #
        # Matched on `name` alone: the tombstone carries a type and a staged
        # input row does not (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
        on = _null_safe_on(("entity",), "l", "d")
        return live.set_alias("l").join(dead.set_alias("d"), on, how="anti")

    def _tombstoned(self) -> DuckDBPyRelation | None:
        """Component keys whose latest staged member row is a tombstone.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows("components")
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
        """Staged member or connection rows, last-write-wins per key.

        The entity key, so no `component_type` - partitioning on it too
        would keep both a tombstone and a later `add` under a different type.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows(kind)
        if rel is None:
            return None
        return _latest_per(
            rel,
            ("entity",)
            if kind == "components"
            else self.schema.group_coordinates("connection"),
        )

    # -- what commit writes (https://energy-models.github.io/datarecord/design/working-record/#committing) -----------------------------------------

    def _staged_entities(self, kind: str) -> Frames:
        """One kind's staged member or connection rows, keyed by component type."""
        rel = self._collapsed_entities(kind)
        if rel is None:
            return EMPTY
        types = tuple(
            r[0]
            for r in rel.project("component_type")
            .distinct()
            .order("component_type")
            .fetchall()
        )
        return LazyFrames(
            types,
            lambda ctype: nw.from_native(
                rel.filter(col("component_type") == lit(ctype))
            ),
        )

    def _staged_attributes(self) -> Frames:
        """The staged rows, plus what a non-partial axis obliges them to carry.

        A patch layer holds only the edits - except along a dim owned
        whole, where touching one value makes this layer the owner of the
        attribute's entire extent along it, so `_restated` completes it from the
        base.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        names = self._staged_attribute_names()
        if not names:
            return EMPTY
        collapsed = self._collapsed_inputs()

        def frame(attr: str) -> nw.LazyFrame:
            staged = collapsed.filter(col("attribute") == lit(attr)).project(
                *self._typed_value(attr)
            )
            return nw.from_native(self._restated(attr, staged))

        return LazyFrames(names, frame)

    def _writable_entities(self, kind: str) -> Frames:
        """The resolved entity frames, in the shape `write_record` persists.

        A resolved frame drops `component_type` (the type is the key it was
        looked up by) while a layer's file carries it, so it is added
        back for any type the staging area did not already rebuild.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        frames = self.components if kind == "components" else self.connections

        def build(ctype: str) -> nw.LazyFrame:
            frame = frames[ctype]
            if "component_type" in frame.collect_schema().names():
                return frame
            return frame.with_columns(component_type=nw.lit(ctype))

        return LazyFrames(tuple(frames), build)

    def staged_only(self) -> _Written:
        """The staged rows alone - what a patch layer holds.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        return _Written(
            schema=self.schema,
            dims=EMPTY,
            components=self._staged_entities("components"),
            connections=self._staged_entities("connections"),
            attributes=self._staged_attributes(),
            # No `_restated` counterpart: results are complete as produced, never
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
            dims=self.base.dims,
            components=self._writable_entities("components"),
            connections=self._writable_entities("connections"),
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


def _input_columns(schema: Schema) -> str:
    """DDL for the staged `inputs/` rows.

    `value` is `VARCHAR` because one staging table serves every attribute, where
    The long schema gives `value` a *per-attribute* type; `_typed_value` casts to the
    declared dtype on the way into the layer.

    No `component_type`: a staged row is the format's own row.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
    """
    # One staging table serves every attribute, so its columns are every
    # declared dim rather than any one attribute's, with a row leaving NULL
    # where its attribute has no such coordinate. Derived, never spelled:
    # `entity` and a group's coordinates are declared dims, so naming them
    # here would emit each twice.
    columns = "".join(f'"{d}" {schema.column_type(d)}, ' for d in schema.dims)
    return f"{columns}attribute VARCHAR, breakpoint DOUBLE, value VARCHAR, _seq BIGINT"


def _component_columns(schema: Schema) -> str:  # noqa: ARG001 - shape is fixed
    return "component_type VARCHAR, entity VARCHAR, deleted BOOLEAN, _seq BIGINT"


def _connection_columns(schema: Schema) -> str:
    # The `connection` group's own coordinates, which are what identifies one.
    coords = "".join(
        f'"{c}" {schema.column_type(c)}, '
        for c in schema.group_coordinates("connection")
    )
    return f"component_type VARCHAR, {coords}role VARCHAR, deleted BOOLEAN, _seq BIGINT"


_COLUMNS = {
    "inputs": _input_columns,
    # `outputs/` uses the same long schema as `inputs/`; what differs is that it
    # does not overlay, which is a read-path property rather than a shape one
    # (https://energy-models.github.io/datarecord/design/read-path/#outputs). So the staging DDL is shared.
    "outputs": _input_columns,
    "components": _component_columns,
    "connections": _connection_columns,
}
