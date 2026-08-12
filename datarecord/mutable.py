"""Editing a store: staged edits, materialised on commit (design doc §11).

What `Store` (read-only) and `write_layer` (a whole store at once) do not
cover. Accumulate-then-commit: an edit costs a row in a staging table rather
than a rewrite, and nothing touches the store until `commit()`.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import narwhals as nw
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import ex_all, fn, union_all_by_name
from datarecord.schema import Schema
from datarecord.store import EMPTY, Flags, Frames, LazyFrames, Store

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


# -- commit targets (§11.7) ---------------------------------------------------


@dataclass(frozen=True)
class NewChild:
    """Write the staged rows as a new child layer of `record` (§11.7).

    Only the edits are written; the fold resolves the rest from the parent.
    """

    record: Any  # a DataRecord; typed loosely to keep this module import-free


@dataclass(frozen=True)
class Directory:
    """Write a standalone store at `uri`: staged rows *plus* what the store
    already reads, there being no parent to resolve against (§11.7).
    """

    uri: str


Target = NewChild | Directory


@dataclass(frozen=True)
class Pending:
    """What a `MutableStore` would write, without writing it (§11.6).

    A derived summary, not a second place rows live: a `GROUP BY` over the
    staging tables, computed on access and discarded.
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


# -- value normalisation (§11.2) ----------------------------------------------


def _as_relation(frame: nw.LazyFrame, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """One narwhals frame as a DuckDB relation, without collecting where possible.

    A DuckDB-backed frame is already a plan, so it passes straight through; any
    other backend is collected to arrow and re-registered, which is the same
    boundary `write_layer` crosses (§4.2).
    """
    native = frame.to_native()
    if isinstance(native, DuckDBPyRelation):
        return native
    arrow = frame.collect(backend="pyarrow").to_native()  # noqa: F841 - by name
    return con.sql("FROM arrow")


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
    """`rel`'s newest row per `key`, by `_seq` - last write wins (§11.7).

    The three staging tables collapse the same way and differ only in what keys
    them, so the window lives here once. `_seq` and the ranking column are
    projected away, since a collapsed relation is read as data rather than as
    staging bookkeeping.
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
    """Whether `value` supplies its own keys rather than being a value (§11.2)."""
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
    """One of `set`'s four `value` forms as per-name values (§11.2).

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
    """
    labels = _series_index(value)
    if labels is not None:
        # A series is genuinely ambiguous: its index may hold names or axis
        # labels. Index dtype does not settle it, since an axis label may be a
        # string like a name, so the tie is broken by membership (§11.2).
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
    """One reading of a `MutableStore`, as the `Store` `write_layer` consumes.

    Commit needs two different stores out of one staging area - `NewChild` the
    edits alone, `Directory` the resolved result - so this holds whichever four
    frame mappings the caller chose (§11.7).
    """

    schema: Schema
    dims: Frames
    components: Frames
    connections: Frames
    attributes: Frames

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Never consulted: `write_layer` persists frames, not flags (§10)."""
        return {}


# -- the DuckDB-backed implementation (§11.9) ---------------------------------

_SEQ = itertools.count(1)


class MutableStore:
    """A `Store` that accepts edits and materialises them on commit (§11).

    Satisfies `Store`, and what it reads is the data *with its pending edits
    applied* (§11.10) - so an edit reads back, or the store is handed to
    something that only knows `Store`, without committing.

    Staged rows live in three connection-scoped DuckDB tables, the *only* place
    a staged row exists: `pending` counts them and the reads fold them, neither
    holding a copy (§11.9).
    """

    def __init__(self, base: Store, con: DuckDBPyConnection) -> None:
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

    # -- Store, over base plus pending (§11.10) -----------------------------

    @property
    def schema(self) -> Schema:
        return self.base.schema

    @property
    def dims(self) -> Frames:
        return self.base.dims

    @property
    def components(self) -> Frames:
        """Base members with pending additions and tombstones applied (§11.10)."""
        return self._entity_frames("components")

    @property
    def connections(self) -> Frames:
        """Base connections with pending ones applied (§11.10)."""
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

        dims = (
            self.schema.component_dims
            if kind == "components"
            else self.schema.connection_dims
        )
        key = ("name", *(("bus",) if kind == "connections" else ()), *dims)
        frame = base[ctype]
        # `collect_schema` reads names without materialising, and `_as_relation`
        # keeps a DuckDB-backed frame as the plan it already is: §4.2 promises a
        # store hands over unmaterialised frames, and §11.10 prices a read with
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
        while `write_layer` needs them back, so one shape serves both.
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
        """Base attributes with pending edits applied (§11.10).

        A set of pending edits *is* a layer - an unwritten one - so the reads
        compose the same way: the staged rows are the last layer, resolved over
        whatever the store was reading before.
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
        # The staged rows are the last layer, so they win per key (§11.10).
        return nw.from_native(self._overlay(base.to_native(), staged))

    def _overlay(
        self, base: DuckDBPyRelation, staged: DuckDBPyRelation
    ) -> DuckDBPyRelation:
        """`staged` over `base`, last-writer-wins per coordinate (§11.10).

        Per *coordinate*, not per input key: the input key excludes the dims an
        attribute is not owned per (§5.5), so keying on it alone would let one
        staged snapshot displace the base's whole series on read - reporting a
        loss the commit path is careful not to make (`_restated`).
        """
        # NULL-safe on the input key and `breakpoint`; *broadcast* on the dims
        # an attribute is not owned per. §3.3 says a staged NULL dim means "all
        # values of that dim", so it must displace the base's rows at every
        # value of it - otherwise the two overlap, which §3.3 forbids. A staged
        # row that *does* name a coordinate displaces only that one, which is
        # what keeps the rest of the series (`_restated` writes it out at
        # commit).
        fixed = (*self._input_key(), "breakpoint")
        broadcast = tuple(d for d in self.schema.dims if d not in self._input_key())
        on = ex_all(
            [
                _null_safe_on(dict.fromkeys(fixed)),
                *(
                    col("s", d).isnull()
                    | sql(f'{col("s", d)} IS NOT DISTINCT FROM {col("b", d)}')
                    for d in broadcast
                ),
            ]
        )
        kept = base.set_alias("b").join(staged.set_alias("s"), on, how="anti")
        return union_all_by_name([kept, staged], self.con).project(
            *(col(c) for c in self._long_columns())
        )

    def _typed_value(self, attribute: str) -> list[Expression]:
        """A projection of `_long_columns` with `value` cast to its dtype (§3.2).

        `value` is staged as text because one staging table holds every
        attribute's values (`_input_columns`); here the attribute is known, so
        the declared dtype applies. Any component type that declares it answers
        - one `inputs/<attr>.parquet` serves them all, so they must agree.

        `TRY_CAST`, not `cast`: a value that does not parse as the declared dtype
        reads as NULL rather than failing the whole relation, which is what the
        text staging column makes possible in the first place.
        """
        dtype = next(
            (
                attrs[attribute].dtype
                for attrs in self.schema.attributes.values()
                if attribute in attrs
            ),
            "DOUBLE",
        )
        return [
            sql(f'TRY_CAST("value" AS {dtype})').alias("value")
            if c == "value"
            else col(c)
            for c in self._long_columns()
        ]

    def _long_columns(self) -> tuple[str, ...]:
        return (
            "component_type",
            "name",
            "bus",
            "attribute",
            "breakpoint",
            "value",
            *self.schema.dims,
        )

    def _input_key(self) -> tuple[str, ...]:
        return ("component_type", "name", "bus", "attribute", *self.schema.input_dims)

    def _owned_whole(self, attribute: str) -> tuple[str, ...]:
        """`AttributeSpec.dims` minus `Schema.partial`, for every type declaring
        `attribute` - one `inputs/<attr>.parquet` serves them all (§5.5, §3.1).
        """
        partial = self.schema.partial or frozenset()
        whole: set[str] = set()
        for attrs in self.schema.attributes.values():
            spec = attrs.get(attribute)
            if spec is not None:
                whole |= spec.dims - partial
        return tuple(d for d in self.schema.dims if d in whole)

    def _restated(self, attribute: str, staged: DuckDBPyRelation) -> DuckDBPyRelation:
        """`staged` plus the base rows a non-partial axis obliges it to carry.

        The one commit-time read of parent data (§5.5, §11.7). Note the two
        keys below: `scope` excludes the whole-owned dims, so one touched
        snapshot pulls in that key's others; `coordinate` adds them back, so a
        base row is dropped only where the edit named that exact coordinate.
        """
        whole = self._owned_whole(attribute)
        if not whole:
            return staged
        if attribute not in self.base.attributes:
            return staged

        scope = [c for c in self._input_key() if c not in whole]
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
        """Never: an edit changes the inputs a result was computed from (§11.7)."""
        return EMPTY

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Base flags unioned with what the staged rows use (§11.10)."""
        out = dict(self.base.flags(ctype))
        rel = self._rows("inputs")
        if rel is None:
            return out
        dims = self.schema.dims
        rows = (
            rel.filter(col("component_type") == lit(ctype))
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

    # -- edits (§11.2, §11.3, §11.5) ----------------------------------------

    def _axis_labels(self) -> dict[str, list[Any]]:
        labels: dict[str, list[Any]] = {}
        for dim in self.schema.dims:
            if dim in self.base.dims:
                frame = self.base.dims[dim].collect().to_native()
                labels[dim] = list(frame[dim]) if dim in frame.columns else []
        return labels

    def _resolved_names(self, ctype: str) -> list[str]:
        """Every name `ctype` currently resolves to, base plus staged (§11.10).

        Through narwhals rather than the native frame: a backend's column
        yields its own scalar type (a `pyarrow.StringScalar`, say), which
        would compare unequal to the plain strings an edit names.
        """
        if ctype not in self.components:
            return []
        frame = self.components[ctype].select("name").collect()
        return [str(n) for n in frame["name"].to_list()]

    def _require_names(self, ctype: str, names: Sequence[str]) -> None:
        """Reject a value for a name no layer declares a member row for (§11.8).

        A component exists by virtue of its member row, so a value keyed to a
        name that has none would resolve to nothing - caught here rather than
        silently dropped at read time. `add` is how such a name comes to exist.
        """
        known = set(self._resolved_names(ctype))
        unknown = sorted({n for n in names if n not in known})
        if unknown:
            msg = (
                f"no {ctype} member row for {unknown}; `add` them first - a value "
                f"for a name no layer declares would resolve to nothing"
            )
            raise KeyError(msg)

    def _validate_edit(
        self, ctype: str, attribute: str, dims: Mapping[str, Any]
    ) -> None:
        """Edit-level checks, at stage time rather than at commit (§11.8)."""
        declared = self.schema.attributes.get(ctype)
        if declared is None:
            msg = f"the schema declares no component type {ctype!r}"
            raise KeyError(msg)
        if attribute not in declared:
            msg = f"the schema declares no {ctype}.{attribute!r}"
            raise KeyError(msg)
        unknown = sorted(set(dims) - set(self.schema.dims))
        if unknown:
            msg = f"the schema declares no dims {unknown}"
            raise KeyError(msg)
        spec = declared[attribute]
        outside = sorted(set(dims) - spec.dims)
        if outside:
            msg = (
                f"{ctype}.{attribute} does not vary over {outside}; "
                f"it varies over {sorted(spec.dims) or 'nothing'}"
            )
            raise ValueError(msg)

    def set(
        self,
        ctype: str,
        attribute: str,
        value: Any,
        *,
        names: Sequence[str] | None = None,
        bus: str | None = None,
        **dims: Any,
    ) -> None:
        """Stage an attribute value for a group of components (§11.2)."""
        self._validate_edit(ctype, attribute, dims)
        if _is_frame(value) and _series_index(value) is None:
            # A frame supplies its own keys (§11.2), but the §11.8 check applies
            # the same way: a name no layer declares resolves to nothing, and a
            # caller should learn that here rather than at read time.
            lazy = nw.from_native(value).lazy()
            if "name" in lazy.collect_schema().names():
                named = lazy.select("name").unique("name").collect()
                self._require_names(ctype, [str(n) for n in named["name"].to_list()])
            self._stage_long(ctype, attribute, value, bus)
            return

        target = list(names) if names is not None else self._resolved_names(ctype)
        keys, values, per_dim = normalise_value(value, target, self._axis_labels())
        if keys is None:
            keys = target
            if len(values) == 1 and len(keys) > 1:
                values = values * len(keys)
        self._require_names(ctype, keys)

        seq = next(_SEQ)
        table = self._ensure("inputs")
        cols = ("component_type", "name", "bus", "attribute", "breakpoint", "value")
        dim_cols = tuple(self.schema.dims)
        placeholders = ", ".join(["?"] * (len(cols) + len(dim_cols) + 1))
        rows = []
        if per_dim:
            (dim, labels) = next(iter(per_dim.items()))
            for label, val in zip(labels, values, strict=True):
                for name in keys:
                    rows.append(
                        [ctype, name, bus, attribute, None, val]
                        + [label if d == dim else dims.get(d) for d in dim_cols]
                        + [seq]
                    )
        else:
            for name, val in zip(keys, values, strict=True):
                rows.append(
                    [ctype, name, bus, attribute, None, val]
                    + [dims.get(d) for d in dim_cols]
                    + [seq]
                )
        self.con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    def _stage_long(
        self, ctype: str, attribute: str, frame: Any, bus: str | None
    ) -> None:
        """Stage a long frame that supplies its own keys (§11.2)."""
        lazy = nw.from_native(frame).lazy()
        _in_long = lazy.to_native()  # noqa: F841 - referenced by name below
        table = self._ensure("inputs")
        # A dim the frame does not carry is NULL - "every value of it" (§3.3) -
        # typed as the schema declares it, so the insert matches the staging
        # table's column type rather than relying on a VARCHAR coercion.
        present = set(lazy.collect_schema().names())
        dim_cols = ", ".join(
            f'"{d}"' if d in present else f'NULL::{self.schema.column_type(d)} AS "{d}"'
            for d in self.schema.dims
        )
        self.con.execute(
            f"INSERT INTO {table} SELECT ? AS component_type, name, "
            f"? AS bus, ? AS attribute, "
            f"NULL::DOUBLE AS breakpoint, "
            f"value::VARCHAR AS value, {dim_cols}, ? "
            f"FROM _in_long",
            [ctype, bus, attribute, next(_SEQ)],
        )

    def update(
        self,
        ctype: str,
        attribute: str,
        expr: nw.Expr,
        *,
        names: Sequence[str] | None = None,
        bus: str | None = None,
        **dims: Any,
    ) -> None:
        """Stage a value derived from the current one (§11.3).

        Reads before it stages, so what it derives from is the resolved value
        *including earlier pending edits* - two `update`s compose. What is
        staged is the result, not the expression.
        """
        self._validate_edit(ctype, attribute, dims)
        frame = self.attributes[attribute].filter(nw.col("component_type") == ctype)
        if names is not None:
            frame = frame.filter(nw.col("name").is_in(list(names)))
        for dim, value in dims.items():
            frame = frame.filter(nw.col(dim) == value)
        derived = frame.with_columns(expr.alias("value"))
        self._stage_resolved(derived)

    def _stage_resolved(self, frame: nw.LazyFrame) -> None:
        """Stage an already-long frame carrying every key column.

        `value` is cast to text on the way in, since the staging table holds
        every attribute's values in one column (`_input_columns`).
        """
        _upd = frame.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure("inputs")
        cols = ", ".join(
            '"value"::VARCHAR AS "value"' if c == "value" else f'"{c}"'
            for c in (
                "component_type",
                "name",
                "bus",
                "attribute",
                "breakpoint",
                "value",
                *self.schema.dims,
            )
        )
        self.con.execute(
            f"INSERT INTO {table} BY NAME SELECT {cols}, {next(_SEQ)} AS _seq FROM _upd"
        )

    def add(self, ctype: str, frame: Any) -> None:
        """Stage new components from a wide frame (§11.5).

        Splits it: attributes varying over nothing stay in `dims/components/`,
        varying ones become `inputs/` rows. Which is which comes from the
        schema, so this needs no framework registry.

        Not a sequence of `set` calls: a component exists by virtue of its
        member row, so staging attribute values for a name no layer declares is
        what `_validate_edit` rejects. Adding a bus with no attributes makes the
        point - nothing to `set`, yet the bus must exist.
        """
        lazy = nw.from_native(frame).lazy()
        columns = lazy.collect_schema().names()
        if "name" not in columns:
            msg = "`add` needs a `name` column"
            raise ValueError(msg)

        declared = self.schema.attributes.get(ctype, {})
        varying = [c for c in columns if declared.get(c) and declared[c].varying]
        # A connection attribute belongs to `dims/connections/`, keyed by bus
        # (§6) - putting it in the member frame would introduce a column the
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
        dim_cols = ", ".join(
            f'"{d}"'
            if d in member_cols
            else f'CAST(NULL AS {self.schema.column_type(d)}) AS "{d}"'
            for d in self.schema.component_dims
        )
        extra = [c for c in member_cols if c != "name" and c not in self.schema.dims]
        # The staging table starts with the key columns only (`_COLUMNS`); a
        # wide frame's attribute columns are whatever the caller passed, so
        # they are added as they are first seen rather than declared up front.
        existing = {c.lower() for c in self.con.table(table).columns}
        for c in extra:
            if c.lower() in existing:
                continue
            dtype = self.schema.value_type(ctype, c) or "VARCHAR"
            self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {dtype}')
            existing.add(c.lower())
        extra_sql = "".join(f', "{c}"' for c in extra)
        self.con.execute(
            f"INSERT INTO {table} BY NAME "
            f"SELECT $ctype AS component_type, name"
            f"{', ' + dim_cols if dim_cols else ''}"
            f"{extra_sql}, false AS deleted, {next(_SEQ)} AS _seq FROM _add",
            {"ctype": ctype},
        )
        for attribute in varying:
            self._stage_long(
                ctype,
                attribute,
                lazy.select("name", nw.col(attribute).alias("value")),
                None,
            )
        # `bus` names the connection itself rather than being an attribute of
        # one, so it becomes the connection row; any other port attribute
        # rides along on it (§6). `role` is passed through when the caller
        # supplies it and left NULL otherwise: what the roles of a type's ports
        # are is a framework's vocabulary, not this layer's to invent.
        if "bus" in ports:
            self.connect(
                ctype,
                lazy.select(
                    "name",
                    nw.col("bus"),
                    *(nw.col(c) for c in ports if c != "bus"),
                    *([nw.col("role")] if "role" in columns else []),
                ),
            )

    def _stage_tombstones(
        self,
        kind: str,
        fixed: tuple[str, ...],
        dim_cols: tuple[str, ...],
        keys: list[list[Any]],
        dims: Mapping[str, Any],
    ) -> None:
        """Stage one `deleted` row per key, scoped by `dim_cols` (§11.5).

        Shared by `remove` and `disconnect`, which differ only in their fixed
        key columns - `disconnect` carries `bus` too (§6). One helper so the
        placeholder count is derived from the column list rather than restated
        per caller, which is what let the two drift out of step.
        """
        columns = (*fixed, *dim_cols, "deleted", "_seq")
        quoted = ", ".join(f'"{c}"' for c in columns)
        table = self._ensure(kind)
        seq = next(_SEQ)
        self.con.executemany(
            f"INSERT INTO {table} ({quoted}) "
            f"VALUES ({', '.join(['?'] * len(columns))})",
            [[*key, *(dims.get(d) for d in dim_cols), True, seq] for key in keys],
        )

    def remove(self, ctype: str, names: Sequence[str], **dims: Any) -> None:
        """Stage a tombstone per name, scoped by the component key dims (§11.5).

        Need not enumerate what it deletes: one row per key, and the fold
        applies it to every attribute.
        """
        unknown = sorted(set(dims) - set(self.schema.component_dims))
        if unknown:
            msg = f"{unknown} do not key component membership"
            raise KeyError(msg)
        self._stage_tombstones(
            "components",
            ("component_type", "name"),
            self.schema.component_dims,
            [[ctype, name] for name in names],
            dims,
        )

    def connect(self, ctype: str, frame: Any) -> None:
        """Stage connection rows from a frame carrying `name` and `bus` (§6)."""
        lazy = nw.from_native(frame).lazy()
        columns = lazy.collect_schema().names()
        for required in ("name", "bus"):
            if required not in columns:
                msg = f"`connect` needs a {required!r} column"
                raise ValueError(msg)
        _conn = lazy.to_native()  # noqa: F841 - bound by replacement scan below
        table = self._ensure("connections")
        extra = [c for c in columns if c not in ("name", "bus")]
        extra_sql = "".join(f', "{c}"' for c in extra)
        self.con.execute(
            f"INSERT INTO {table} BY NAME "
            f"SELECT $ctype AS component_type, name, bus{extra_sql}, "
            f"false AS deleted, {next(_SEQ)} AS _seq FROM _conn",
            {"ctype": ctype},
        )

    def disconnect(
        self, ctype: str, pairs: Sequence[tuple[str, str]], **dims: Any
    ) -> None:
        """Stage a tombstone per `(name, bus)` (§6)."""
        unknown = sorted(set(dims) - set(self.schema.connection_dims))
        if unknown:
            msg = f"{unknown} do not key connection membership"
            raise KeyError(msg)
        self._stage_tombstones(
            "connections",
            ("component_type", "name", "bus"),
            self.schema.connection_dims,
            [[ctype, name, bus] for name, bus in pairs],
            dims,
        )

    # -- pending / commit / rollback (§11.6, §11.7) -------------------------

    @property
    def pending(self) -> Pending:
        """Counts over the staging tables, computed on access (§11.6)."""

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
        # one as nothing pending (§11.6).
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
        """Clear every staged row without writing (§11)."""
        for kind in list(self._staged):
            self.con.execute(f"DROP TABLE IF EXISTS {self._table(kind)}")
        self._staged.clear()

    def _collapsed_inputs(self) -> DuckDBPyRelation:
        """Staged attribute rows, last-write-wins per key, tombstones applied (§11.7)."""
        rel = self._rows("inputs")
        assert rel is not None
        # Per coordinate, not per input key: the input key excludes the dims an
        # attribute is not owned per (§5.5), so partitioning on it alone would
        # collapse a whole staged series to one row - two `set` calls at
        # different snapshots are not two writes to the same key.
        key = dict.fromkeys((*self._input_key(), *self.schema.dims, "breakpoint"))
        live = _latest_per(rel, key)
        dead = self._tombstoned()
        if dead is None:
            return live
        # A deleted component has no attributes, so the tombstone wins over a
        # staged value regardless of sequence (§11.7).
        on = _null_safe_on(
            ("component_type", "name", *self.schema.component_dims), "l", "d"
        )
        return live.set_alias("l").join(dead.set_alias("d"), on, how="anti")

    def _tombstoned(self) -> DuckDBPyRelation | None:
        """Component keys whose latest staged member row is a tombstone (§11.7)."""
        rel = self._rows("components")
        if rel is None:
            return None
        cols = ("component_type", "name", *self.schema.component_dims)
        # An `add` after a `remove` means the component exists again, so only
        # the latest row per key counts (§11.7).
        return (
            _latest_per(rel, cols)
            .filter(col("deleted"))
            .project(*(col(c) for c in cols))
        )

    def _collapsed_entities(self, kind: str) -> DuckDBPyRelation | None:
        """Staged member or connection rows, last-write-wins per key (§11.7)."""
        rel = self._rows(kind)
        if rel is None:
            return None
        dims = (
            self.schema.component_dims
            if kind == "components"
            else self.schema.connection_dims
        )
        cols = (
            "component_type",
            "name",
            *(("bus",) if kind == "connections" else ()),
            *dims,
        )
        return _latest_per(rel, cols)

    # -- what commit writes (§11.7) -----------------------------------------

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

        A patch layer holds only the edits (§11.7) - except along a dim owned
        whole, where touching one value makes this layer the owner of the
        attribute's entire extent along it, so `_restated` completes it from the
        base (§5.5).
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
        """The resolved entity frames, in the shape `write_layer` persists.

        A resolved frame drops `component_type` (the type is the key it was
        looked up by) while a layer's file carries it (§3.1), so it is added
        back for any type the staging area did not already rebuild.
        """
        frames = self.components if kind == "components" else self.connections

        def build(ctype: str) -> nw.LazyFrame:
            frame = frames[ctype]
            if "component_type" in frame.collect_schema().names():
                return frame
            return frame.with_columns(component_type=nw.lit(ctype))

        return LazyFrames(tuple(frames), build)

    def staged_only(self) -> _Written:
        """The staged rows alone - what a patch layer holds (§11.7)."""
        return _Written(
            schema=self.schema,
            dims=EMPTY,
            components=self._staged_entities("components"),
            connections=self._staged_entities("connections"),
            attributes=self._staged_attributes(),
        )

    def flattened(self) -> _Written:
        """The staged rows over what the store already reads (§11.7).

        `attributes` is this store's own, since a `MutableStore` already reads
        the base with its pending edits applied (§11.10) - which is exactly the
        flattened result.
        """
        return _Written(
            schema=self.schema,
            dims=self.base.dims,
            components=self._writable_entities("components"),
            connections=self._writable_entities("connections"),
            attributes=self.attributes,
        )

    def commit(self, target: Target) -> Any:
        """Write everything staged and clear it (§11.7).

        Returns
        -------
        The new child for a `NewChild` target, so the caller can read what it
        just wrote without going back to the record table; `None` for a
        `Directory`, which belongs to no record.
        """
        from datarecord.layered.write import write_layer  # circular at module level

        if isinstance(target, NewChild):
            child = target.record.child()
            write_layer(child.id, self.staged_only(), self.con)
            self.rollback()
            return child
        write_layer(None, self.flattened(), self.con, uri=target.uri)
        self.rollback()
        return None


def _input_columns(schema: Schema) -> str:
    """DDL for the staged `inputs/` rows.

    `value` is `VARCHAR` because one staging table serves every attribute, where
    §3.2 gives `value` a *per-attribute* type; `_typed_value` casts to the
    declared dtype on the way into the layer.
    """
    dims = "".join(f', "{d}" {schema.column_type(d)}' for d in schema.dims)
    return (
        "component_type VARCHAR, name VARCHAR, bus VARCHAR, attribute VARCHAR, "
        f"breakpoint DOUBLE, value VARCHAR{dims}, _seq BIGINT"
    )


def _component_columns(schema: Schema) -> str:
    dims = "".join(f', "{d}" {schema.column_type(d)}' for d in schema.component_dims)
    return f"component_type VARCHAR, name VARCHAR{dims}, deleted BOOLEAN, _seq BIGINT"


def _connection_columns(schema: Schema) -> str:
    dims = "".join(f', "{d}" {schema.column_type(d)}' for d in schema.connection_dims)
    return (
        f"component_type VARCHAR, name VARCHAR, bus VARCHAR, role VARCHAR{dims}, "
        "deleted BOOLEAN, _seq BIGINT"
    )


_COLUMNS = {
    "inputs": _input_columns,
    "components": _component_columns,
    "connections": _connection_columns,
}
