# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Editing a record: staged edits, materialised on commit.

What `Record` (read-only) and `write_record` (a whole record at once) do not
cover. Accumulate-then-commit: an edit costs a row in a staging table rather
than a rewrite, and nothing touches the record until `commit()`.

Notes
-----
- [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
"""

from __future__ import annotations

import re
from collections.abc import Collection, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from uuid import UUID, uuid4

import duckdb
import narwhals as nw
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql

from datarecord.duck import (
    DuckTypes,
    as_relation,
    null_safe,
    union_all_by_name,
)
from datarecord.layered.fold import Fold
from datarecord.layered.resolve import Resolver
from datarecord.layered.revision import Record, Revision
from datarecord.layered.write import write_record
from datarecord.record import (
    EMPTY,
    Frames,
    LazyFrames,
    RecordLike,
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

    record: Revision | None = None


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


def _mapping_keys(value: Any) -> list[Any]:
    """The labels a mapping or a labelled series is keyed by; none for a scalar."""
    if isinstance(value, Mapping):
        return list(value)
    return list(_series_index(value) or [])


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
    names: Sequence[Any] | None,
    *,
    indexed_by: str | None = None,
) -> tuple[list[Any] | None, list[Any], dict[str, list[Any]]]:
    """One of `set`'s four `value` forms as labels and values.

    Parameters
    ----------
    value
        Scalar, sequence, mapping, or a one-dimensional labelled series.
    names
        The labels a call listed along one dim, which a scalar reaches and a
        sequence aligns to.
    indexed_by
        The dim a mapping's keys or a series' index hold (`_series_axis`).

    Returns
    -------
    names
        The listed labels each value belongs to, or None where none were listed.
    values
        One value per name, or per `per_dim` label.
    per_dim
        Dim -> labels, where a mapping or a labelled series was given.

    Raises
    ------
    ValueError
        If a sequence's length does not match `names`, or no names were listed.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    labels = _series_index(value)
    if labels is not None:
        return _listed(names), list(value), {str(indexed_by): list(labels)}

    if isinstance(value, Mapping):
        return _listed(names), list(value.values()), {str(indexed_by): list(value)}

    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        if names is None:
            return None, [value], {}
        return list(names), [value] * len(names), {}

    if names is None:
        msg = "a sequence needs labels listed along one dim to align to"
        raise ValueError(msg)
    if len(value) != len(names):
        msg = (
            f"{len(value)} values for {len(names)} labels; a length mismatch is an "
            f"error at the call rather than a truncated edit"
        )
        raise ValueError(msg)
    return list(names), list(value), {}


def _listed(names: Sequence[Any] | None) -> list[Any] | None:
    """`names` as a list, keeping None for "no labels listed"."""
    return None if names is None else list(names)


def _split_dims(
    dims: Mapping[str, Any],
) -> tuple[str | None, list[Any], dict[str, Any]]:
    """`set`'s keywords split into the one dim given a list, its labels, and the rest.

    A list along two dims would ask for their product, which a mapping or a long
    frame states unambiguously, so it is refused rather than guessed.

    Raises
    ------
    ValueError
        If more than one dim is given a list.
    """
    many = {
        d: list(v)
        for d, v in dims.items()
        if isinstance(v, Iterable) and not isinstance(v, str | bytes)
    }
    if len(many) > 1:
        msg = (
            f"`set` takes a list of labels along one dim at a time; it was given "
            f"lists for {sorted(many)}. Pass a long frame for their product"
        )
        raise ValueError(msg)
    fixed = {d: v for d, v in dims.items() if d not in many}
    if not many:
        return None, [], fixed
    ((dim, labels),) = many.items()
    return dim, labels, fixed


@dataclass(frozen=True)
class StagedSource:
    """A staging area as one layer's rows - the last source the fold reads.

    "The layer as it would be written": each member reads its staging table,
    which an edit replaces by key rather than appending to (`_replace`), so the
    table is already one row per key and what `write_record` would persist -
    the entity axis apart, whose tombstone anti-join reaches another file.

    Unfrozen, which is the whole of what distinguishes it: a `set` changes these
    rows under a reader, so the fold must stay a relation past this point rather
    than materialise. Nothing here needs invalidating in exchange - a relation
    over a staging table reads whatever the table holds when it is collected.

    Structural rather than inheriting `LayerSource`, so `mutable.py` keeps its
    rule of importing from `layered/` only inside function bodies.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    """

    record: WorkingRecord
    layer_id: UUID
    frozen: bool = False

    @property
    def schema(self) -> Schema:
        """This record's schema - the `LayerData` `write_record` reads for one.

        A staging area is not a `LayerSource` a fold folds under a schema
        someone else supplies; it is also what `write_record` writes directly
        for a `NewChild` commit, where it needs its own.
        """
        return self.record.schema

    def materialised(self, con: DuckDBPyConnection, schema: Schema) -> Fold | None:
        """A staging area has no `resolved/` cache: it is never a fold's base."""
        return None

    def axes(self) -> set[str]:
        """The dims with staged rows, `entity` among them.

        `entity` is an axis file like any other to a reader, so an `add` or a
        `remove` contributes to it - which is what puts a staged component in
        the components map.
        """
        return set(self.record._staged_dims())

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """One axis as this layer would write it - `_axis_layer`, exactly.

        Which matters for the columns rather than the labels: the fold is
        last-writer-wins per *label*, over the whole row, so a source handing
        over only the column its `set` named would blank every sibling
        attribute on that label. A `set` on an axis carries the siblings into
        the staged row when it patches it (`_patch_axis`), so the table is
        already the row `commit` writes.

        `entity` included, staged as an axis like the rest - what differs is
        only the extent `_axis_layer` gives it.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        if self.record._rows(f"{_AXIS_PREFIX}{dim}") is None:
            return None
        return self.record._axis_layer(dim)

    def groups(self) -> set[str]:
        """Which declared groups have staged rows."""
        return set(self.record._staged_groups())

    def group(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_group(name)

    def attributes(self, kind: str = "inputs") -> set[str]:
        """Which attributes of `kind` have staged rows."""
        return set(self.record._staged_attributes_of(kind))

    def attribute(self, name: str, kind: str = "inputs") -> DuckDBPyRelation | None:
        if kind == "outputs":
            # Results do not overlay, so the table is already what is written -
            # one row per coordinate (`_replace`) and no fold to apply.
            return self.record._rows("outputs", name)
        return self.record._collapsed_inputs(name)

    def all_attributes(self, kind: str = "inputs") -> DuckDBPyRelation | None:
        """Every staged attribute of `kind`, unioned by name and unprojected.

        By name because the tables carry per-attribute column variation exactly
        as the files do - one attribute's coordinates and no others - which is
        what lets `fold_inputs` pad both the same way.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        arms = [
            rel
            for name in self.record._staged_attributes_of(kind)
            if (rel := self.attribute(name, kind)) is not None
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
    member is inherited unchanged - `outputs` alone is overridden, results not
    overlaying - so an edit reads back through the same code path a committed
    layer would.

    Staged rows live in connection-scoped DuckDB tables, the *only* place a
    staged row exists: the reads fold them rather than holding a copy, so what
    is staged is asked of the reads themselves.

    The `Resolver` is fixed at construction and the source list never changes;
    a `set` changes what the last source's tables hold, not which sources there
    are. That is what lets the inherited members stay correct - they cache a key
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
    #: `commit` writes one, and it names the staging tables too - so two
    #: `WorkingRecord`s on one connection collide in neither.
    _layer_id: UUID
    #: What the base resolves from, which every use of the base here wants: the
    #: schema, one dim's axis, one attribute's rows and the revision to branch
    #: from are all members of one.
    _base: Resolver
    #: Keyed by `(kind, attribute)`, the attribute being None for the entity
    #: kinds. A long kind stages one table per attribute because that is the
    #: file it stands for: one `value` column at the attribute's own type, and
    #: its own coordinates and no others.
    _staged: dict[tuple[str, str | None], str]

    def __init__(self, base: RecordLike, con: DuckDBPyConnection) -> None:
        # `object.__setattr__` throughout: the base is a frozen dataclass, and
        # these are set before `super().__init__` because the `StagedSource`
        # below reads them off `self`.
        object.__setattr__(self, "_layer_id", uuid4())
        object.__setattr__(self, "_staged", {})
        base_cache = _base_resolver(base, con)
        object.__setattr__(self, "_base", base_cache)
        # This record *is* the fold one layer deeper, and that layer is the
        # staging area - so the field the base class holds is that fold, whose
        # last source is the staged layer `commit` writes.
        super().__init__(base_cache.with_source(StagedSource(self, self._layer_id)))

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
        self._shape(kind, attribute).create(name)
        self._staged[key] = name
        return name

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
        return DuckTypes(self.con).empty_relation(**columns)

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
        return duck_types.empty_relation(**shaped)

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

    # -- Record, one fold deeper (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) --------------------------------

    # `schema`, `dims`, `groups`, `attributes` and `flags` are
    # inherited from `Record` unchanged, which is the property this design
    # exists to have: a staged edit is read by the same fold that reads a
    # committed layer, so there is no second overlay to keep in step. Only
    # `outputs` below differs, and only because results do not overlay.

    def _owned_whole(self, attribute: str) -> tuple[str, ...]:
        """`AttributeSpec.dims` minus the fold key - the value axes owned whole.

        The complement of `owned_per`: a dim the attribute varies over but that is
        not in `partial_dims` (a non-`partial` value axis like `timestep`) is
        owned whole, so a patch to one value restates the attribute's whole extent
        along it. Membership keys are in `partial_dims`, so never here - a layer
        patches one component's or connection's value, never restating the rest.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        spec = self.schema.attributes.get(attribute)
        whole = (
            frozenset() if spec is None else spec.dims - set(self.schema.partial_dims)
        )
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
            # The table is already the shape of the file: results do not overlay,
            # so there is nothing to collapse them against (https://energy-models.github.io/datarecord/design/read-path/#outputs).
            return nw.from_native(rel)

        return LazyFrames(names, frame)

    # -- edits (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one, https://energy-models.github.io/datarecord/design/working-record/#add-remove) ----------------------------------------

    def _series_axis(
        self,
        attribute: str,
        value: Any,
        indexed_by: str | None,
        named: Collection[str] = (),
    ) -> str | None:
        """Which dim a labelled series' index or a mapping's keys hold.

        The caller says which - `indexed_by="snapshot"`, or the series' own
        `index.name` where it names a coordinate of this attribute - or else it
        is the one coordinate the call does not name. Never read off the
        labels: one dim's label may be a string just like another's, so a
        membership test would make one call mean different things in two
        records.

        An `index.name` naming no coordinate is ignored rather than rejected; it
        may be `None`, or a pandas artefact like `"index"`.

        Raises
        ------
        ValueError
            If more than one coordinate is left unnamed and nothing says which.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        keyed = isinstance(value, Mapping) or _series_index(value) is not None
        if not keyed:
            if indexed_by is not None:
                msg = (
                    f"`set({attribute!r}, ..., indexed_by={indexed_by!r})` says what "
                    f"a mapping's keys or a series index hold, but the value has "
                    f"no labels"
                )
                raise ValueError(msg)
            return None
        coordinates = [c for c in self._staged_coordinates(attribute) if c not in named]
        told = indexed_by if indexed_by is not None else _series_index_name(value)
        if told is None or (indexed_by is None and told not in coordinates):
            if len(coordinates) == 1:
                return coordinates[0]
            msg = (
                f"`set({attribute!r}, ...)` cannot tell which of {coordinates} "
                f"the value's labels hold; say `indexed_by=`"
            )
            raise ValueError(msg)
        named_dim = told
        if named_dim not in coordinates:
            msg = (
                f"`indexed_by={named_dim!r}` is no coordinate of {attribute!r}, "
                f"which is addressed by {coordinates}"
            )
            raise ValueError(msg)
        return named_dim

    def _axis_of(self, attribute: str) -> str | None:
        """The dim whose axis file carries `attribute`, or `None`.

        A declared attribute addressed by one dim alone is a column of that
        dim's axis file rather than a long row, so an edit to it stages an axis
        row. `attributes_on` is the rule. An undeclared attribute is never one -
        only a result is undeclared, and a result is always long.

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
        labels: Sequence[Any] | None,
    ) -> None:
        """Stage one axis-file attribute, keyed by the axis's own labels.

        `value` is a mapping from label to value, or a scalar for every label the
        axis currently has. A mapping may name a label no layer has written yet,
        which becomes a row of this layer's axis file - the fold keys per label,
        so introducing one displaces nothing. A dim that keys a group is the
        exception: `set` requires its labels first (`_require_labels`).

        One *complete* row per label: this edit's column over the label's
        current row - the one already staged where there is one, else the base's
        - so a second `set` on the same axis replaces the row rather than adding
        beside it and losing the first's column.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        if labels is not None and not isinstance(value, Mapping):
            # A scalar broadcast to the named labels, or a sequence aligned to
            # them - the same two forms `set` takes for a long attribute, folded
            # to the axis's label->value mapping the rest of this method wants.
            names = list(labels)
            vals = (
                list(value)
                if isinstance(value, (list, tuple))
                else [value] * len(names)
            )
            if len(vals) != len(names):
                msg = (
                    f"`set({attribute!r}, <sequence>, {dim}=...)` has "
                    f"{len(vals)} values for {len(names)} names"
                )
                raise ValueError(msg)
            value = dict(zip(names, vals, strict=True))
        elif _is_frame(value) or isinstance(value, (list, tuple)):
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

        table = self._ensure(f"{_AXIS_PREFIX}{dim}", None)
        if isinstance(value, Mapping):
            if not value:
                return
            edit = self._values_relation(
                {dim: list(value), attribute: list(value.values())},
                {
                    dim: self._column_type(dim),
                    attribute: self.schema.value_type(attribute),
                },
            )
        else:
            base_axis = self._base.dims.axes.get(dim)
            if base_axis is None:
                msg = (
                    f"`set({attribute!r}, <scalar>)` reaches every label the "
                    f"{dim!r} axis has, and it has none; name the labels as a "
                    f"mapping, or write the axis file first"
                )
                raise ValueError(msg)
            edit = base_axis.project(col(dim), lit(value).alias(attribute))

        self._patch_axis(dim, attribute, table, edit)

    def _patch_axis(
        self, dim: str, attribute: str, table: str, edit: DuckDBPyRelation
    ) -> None:
        """Set `attribute` on each label `edit` names, in place.

        An axis row's columns are independently editable, so this patches the one
        column rather than replacing the row - a sibling a `set` did not name is
        never read and so cannot be lost. A label falls to one of two statements
        by whether a prior `set` already staged it:

        - **INSERT** a label not yet staged, taking its siblings from the base row
          where the base has one and the edited column from `edit`. Its siblings
          have to travel with it: the fold is whole-row last-writer-wins per label
          (`fold_axis`), so a staged patch carrying only its own column would win
          the label and blank the rest - the resolved row is the layer's, not one
          the read rebuilds column by column. A label the base also lacks is new,
          and the edit is the whole of its row.
        - **UPDATE** a label already staged, patching the one column in place so
          the siblings an earlier edit carried stay put.

        `partial` decides only the label *extent* the layer carries, which is
        `_axis_layer`'s business, not this.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        staged = self.con.table(table)
        base = self._base.dims.axes.get(dim)
        # Raw SQL because `UPDATE ... FROM` is a join-update, which the relational
        # `update` cannot express (https://duckdb.org/docs/stable/clients/python/relational_api).
        self.con.execute(
            f"UPDATE {table} AS t SET {col(attribute)} = {col('e', attribute)} "
            f"FROM edit e "
            f"WHERE {col('t', dim)} IS NOT DISTINCT FROM {col('e', dim)}"
        )
        fresh = edit.set_alias("e").join(
            staged.set_alias("s"), null_safe("e", "s", [dim]), how="anti"
        )
        if base is None:
            rows = fresh.set_alias("e").select(col(dim), col(attribute))
        else:
            # A left join, since a fresh label the base also lacks has no row.
            siblings = [c for c in base.columns if c not in (dim, attribute)]
            rows = (
                fresh.set_alias("e")
                .join(base.set_alias("b"), null_safe("e", "b", [dim]), how="left")
                .select(
                    col("e", dim), col("e", attribute), *(col("b", c) for c in siblings)
                )
            )
        self._insert(rows, table, {})

    def _require_labels(self, dim: str, labels: Iterable[Any]) -> None:
        """Reject a label of a group-keying dim its axis does not hold, base plus staged.

        A dim that keys a group - `entity` for `connection` - names the rows a
        layer adds and removes, so a value for a label no layer added would
        resolve to a member that does not exist. Other axes take new labels
        from `set`, which is why the check is scoped rather than universal.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        if not any(dim in g.key for g in self.schema.groups.values()):
            return
        wanted = list(dict.fromkeys(str(n) for n in labels))
        if not wanted:
            return
        found = {str(n) for n in self._labels(dim)}
        unknown = sorted(n for n in wanted if n not in found)
        if unknown:
            msg = (
                f"no {dim} {unknown}; `add` them first - a value for a label "
                f"no layer declares would resolve to nothing"
            )
            raise KeyError(msg)

    def _validate_dims(self, dims: Collection[str]) -> None:
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

    def _validate_result(self, attribute: str, dims: Collection[str]) -> None:
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

    def _validate_attribute(self, attribute: str, dims: Collection[str]) -> None:
        """An input attribute's declaration and the dims an edit names for it.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        spec = self.schema.attributes.get(attribute)
        if spec is None:
            msg = f"no attribute {attribute!r} is declared"
            raise KeyError(msg)
        coordinates = self.schema.coordinates_of(attribute)
        outside = sorted(set(dims) - set(coordinates))
        if outside:
            msg = (
                f"{attribute} does not vary over {outside}; "
                f"it varies over {list(coordinates) or 'nothing'}"
            )
            raise ValueError(msg)
        through_group = sorted(
            set(coordinates) - set(self.schema.broadcasts_over(attribute)) - set(dims)
        )
        if through_group:
            msg = (
                f"{attribute} is addressed through a group, so each row names "
                f"{through_group} too; a NULL there cannot mean every row of the group"
            )
            raise ValueError(msg)

    def set(
        self,
        attribute: str,
        value: Any,
        *,
        kind: Literal["inputs", "outputs"] = "inputs",
        indexed_by: str | None = None,
        **dims: Any,
    ) -> None:
        """Stage an attribute value.

        `**dims` scopes the edit, one keyword per coordinate: a label
        (`scenario="high"`), or a list of labels along one dim at most
        (`generator=["wind", "gas"]`). A coordinate no keyword names is
        written NULL, which the broadcast rule reads as every label of it -
        including labels a later layer adds. A coordinate the attribute reaches
        through a group - `bus` for a connection attribute - must be named.

        `value` takes five forms: a scalar for every named label, a sequence
        aligned positionally to the listed labels, a mapping or a labelled
        series keyed by one coordinate, a long frame supplying its own keys,
        and a narwhals expression - which is a *function of the current value*
        rather than a value, so it reads before it stages and two such calls
        compose.

        A mapping's keys and a series' index hold the coordinate `indexed_by`
        names, else the series' own `index.name`, else the one coordinate no
        keyword names. Never inferred from the labels themselves - one dim's
        label may be a string just like another's, so that would make one call
        mean different things in two records.

        `kind` names the destination in the format's own terms:
        `"outputs"` stages into `outputs/` instead of `inputs/`, which is how a
        tool hands results back. Results use the same long schema; what differs
        is that they do not overlay.

        Two checks are skipped for `"outputs"`, both because a result is not a
        value the schema governs: the attribute need not be declared, and its
        labels need not resolve to declared members. A solve may produce rows
        for a component it derived rather than read - PyPSA's `SubNetwork` is
        one - and rejecting those would refuse a legitimate result. An *input*
        for an undeclared label stays an error.

        Raises
        ------
        KeyError
            If the attribute is not declared, or a label of a dim that keys a
            group is on no layer's axis.
        ValueError
            If a keyword names a dim the attribute does not vary over, a group
            coordinate is left unnamed, two dims are given lists, or the dim a
            mapping or series is keyed by cannot be told.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        is_long_frame = _is_frame(value) and _series_index(value) is None
        if is_long_frame:
            lazy = _incoming(value, self.con)
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
            self._stage_derived(attribute, value, kind=kind, **dims)
            return

        listed, labels, fixed = _split_dims(dims)
        axis = self._axis_of(attribute) if kind == "inputs" else None
        if axis is not None:
            self._set_axis(axis, attribute, value, indexed_by, listed, labels, fixed)
            return

        keyed_by = self._series_axis(attribute, value, indexed_by, named=dims)
        if keyed_by is not None and listed is not None and isinstance(value, Mapping):
            msg = (
                f"`set({attribute!r}, <mapping>, {listed}=[...])` names labels "
                f"twice; key the mapping by {listed!r} or pass a scalar"
            )
            raise ValueError(msg)
        keys, values, per_dim = normalise_value(
            value, labels if listed is not None else None, indexed_by=keyed_by
        )
        named = {*dims, *per_dim}
        if kind == "inputs":
            self._validate_dims(named)
            self._validate_attribute(attribute, named)
            for dim, dim_labels in per_dim.items():
                self._require_labels(dim, dim_labels)
            if listed is not None:
                self._require_labels(listed, keys or labels)
            for dim, label in fixed.items():
                self._require_labels(dim, [label])
        else:
            self._validate_result(attribute, named)

        table = self._ensure(kind, attribute)
        self._stage_rows(attribute, table, listed, keys, values, per_dim, fixed)

    def _set_axis(
        self,
        axis: str,
        attribute: str,
        value: Any,
        indexed_by: str | None,
        listed: str | None,
        labels: list[Any],
        fixed: Mapping[str, Any],
    ) -> None:
        """`set` on an attribute stored as a column of `axis`'s file.

        The attribute is keyed by `axis` alone, so a keyword for any other dim
        has nothing to scope and is refused rather than dropped. A scalar with
        no labels reaches every label the axis resolves, staged ones included:
        an axis file has no NULL row to broadcast from.
        """
        other = sorted(d for d in {*fixed, *([listed] if listed else [])} if d != axis)
        if other:
            msg = (
                f"{attribute} does not vary over {other}; it is a column of "
                f"dims/{axis}.parquet, keyed by {axis!r} alone"
            )
            raise ValueError(msg)
        self._series_axis(attribute, value, indexed_by, named=())
        names: list[Any] | None = (
            labels if listed == axis else [fixed[axis]] if axis in fixed else None
        )
        if names is None and not _mapping_keys(value):
            names = self._labels(axis)
        self._require_labels(axis, names if names is not None else _mapping_keys(value))
        self._stage_axis(axis, attribute, value, labels=names)

    def _labels(self, dim: str) -> list[Any]:
        """Every label `dim`'s axis resolves to, staged edits included."""
        axis = self.resolver.dims.axes.get(dim)
        return [] if axis is None else [n for (n,) in axis.project(dim).fetchall()]

    def _validate_frame(
        self, lazy: nw.LazyFrame, attribute: str, dims: Mapping[str, Any]
    ) -> None:
        """A long input frame's dims, labels and the attribute's spec.

        The frame supplies its own labels, so each of a group-keying dim must
        already be on its axis (`_require_labels`).

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        self._validate_dims(dims)
        columns = lazy.collect_schema().names()
        for dim in self.schema.coordinates_of(attribute):
            if dim in columns:
                self._require_labels(
                    dim, lazy.select(dim).unique(dim).collect()[dim].to_list()
                )
        self._validate_attribute(
            attribute, {*dims, *(c for c in columns if c in self.schema.dimensions)}
        )

    def _stage_rows(
        self,
        attribute: str,
        table: str,
        listed: str | None,
        keys: list[Any] | None,
        values: list[Any],
        per_dim: dict[str, list[Any]],
        fixed: Mapping[str, Any],
    ) -> None:
        """Stage the scalar/sequence/mapping forms, as a relation rather than rows.

        The rows are `listed`'s labels x `per_dim`'s labels - every named label
        at every coordinate the value covers - so a per-snapshot series over a
        year for a thousand components is millions of them. Both factors are
        small, and only their product is not, so each becomes a one-column
        relation and DuckDB joins them: the product never exists as Python
        objects.

        `keys` and `values` are positionally aligned where there is no `per_dim`;
        with one, `values` aligns to its labels and every key takes all of them.
        With neither a listed dim nor `per_dim`, the one value is one row, NULL
        wherever `fixed` names nothing.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        if per_dim:
            ((dim, labels),) = per_dim.items()
            rel = self._values_relation(
                {dim: labels, "value": values},
                {dim: self._column_type(dim), "value": None},
            )
            present = {dim}
            if listed is not None and keys:
                rel = self._values_relation(
                    {listed: keys}, {listed: self._column_type(listed)}
                ).cross(rel)
                present.add(listed)
        elif listed is not None:
            rel = self._values_relation(
                {listed: keys or [], "value": values},
                {listed: self._column_type(listed), "value": None},
            )
            present = {listed}
        else:
            rel = self._values_relation({"value": values}, {"value": None})
            present = set()
        self._insert_long(rel, table, attribute, present, fixed)

    def _insert(
        self,
        rel: DuckDBPyRelation,
        table: str,
        supplied: Mapping[str, Expression],
        key: Sequence[str] | None = None,
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
        out. The match is `null_safe` so a broadcast coordinate's NULL replaces
        the same NULL rather than sitting beside it (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        `rel` is a whole-row edit built from the caller's frame, never from the
        staged table, so the delete cannot change what the insert then reads -
        the one path that patches columns in place (`_patch_axis`) does not pass
        a `key`.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
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
        if key is not None:
            # `null_safe`, not `IN`/`=`: those never match a broadcast row's NULL.
            on = null_safe(table, "p", key)
            self.con.execute(f"DELETE FROM {table} USING projected p WHERE {on}")
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
        shaped = rel.project(
            *(
                col(d)
                if d in present
                else duck_types.lit(dims.get(d), self._column_type(d)).alias(d)
                for d in self._staged_coordinates(attribute)
            ),
            lit(attribute).alias("attribute"),
            duck_types.null(nw.Float64()).alias("breakpoint"),
            col("value"),
        )
        self._insert(shaped, table, {}, key=self._long_key(attribute))
        self._complete_owned_whole(attribute, table)

    def _long_key(self, attribute: str) -> tuple[str, ...]:
        """The coordinate columns one long table is keyed on - what an edit replaces.

        The fold partitions on these too (`_collapsed_inputs` before this change),
        so replacing them per edit removes exactly what it would have discarded.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        columns = set(self.schema.long_columns_for(attribute))
        return tuple(
            c
            for c in (*self.schema.input_key, *self.schema.dims, "breakpoint")
            if c in columns
        )

    def _complete_owned_whole(self, attribute: str, table: str) -> None:
        """Carry the base extent a non-partial axis obliges the staged rows to hold.

        Done as the rows are staged rather than at commit, so the staging table
        *is* the layer: touching one snapshot of a series makes
        this layer the owner of that key's whole extent along the dim, and the
        untouched coordinates have to be carried or the commit would report a
        loss.

        Scoped by the keys already staged, not by the attribute: the semi-join
        below reaches only the keys some edit named, so a component this record
        never touched stays in the parent.

        Idempotent, which is what lets it run per insert instead of once: the
        anti-join drops any coordinate the table already holds, so a second edit
        to the same attribute carries only what the first did not. That
        anti-join is also the whole of what keeps a fill off an edited
        coordinate - it keys on `_long_key`, the same coordinate a later `set`
        deletes on (`_replace`), so the fill it excludes and the row that
        replaces one are one key, and no ordering column is needed to rank them.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        whole = self._owned_whole(attribute)
        if not whole or attribute not in self._base.attributes():
            return

        # Intersected with the attribute's own columns: a key column its file
        # does not carry is absent from both sides rather than NULL in them,
        # and joining on it would fail to bind (`long_columns_for`).
        columns = set(self.schema.long_columns_for(attribute))
        scope = [c for c in self.schema.input_key if c not in whole and c in columns]
        present = [d for d in whole if d in columns]
        if not present:
            # No column for any whole-owned dim, so the file holds one row per
            # key and there is no extent to complete.
            return
        coordinate = self._long_key(attribute)
        staged = self.con.table(table)
        base = self._base.attribute(attribute)
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

        Unscoped, this derives from every row of the attribute.

        Notes
        -----
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        """
        source = self.outputs if kind == "outputs" else self.attributes
        listed, labels, fixed = _split_dims(dims)
        if attribute not in source:
            frame = None
        else:
            frame = source[attribute]
            if listed is not None:
                if kind == "inputs":
                    self._require_labels(listed, labels)
                frame = frame.filter(nw.col(listed).is_in(labels))
            for dim, value in fixed.items():
                frame = frame.filter(nw.col(dim) == value)

        # A named target that resolves to no row is a failed change, not a
        # no-op: the caller asked for these rows to take a new value and there
        # is nothing to derive one from. With no scope the instruction is
        # "whatever resolves", so an empty result is an answer.
        if dims:
            # `head(1)`: `is_empty` is a `DataFrame` method, so the question
            # costs a collect either way - this one collects a single row.
            if frame is None or frame.select("value").head(1).collect().is_empty():
                scope = ", ".join(f"{d}={v!r}" for d, v in dims.items())
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

        shaped = rel.project(
            *(column(c) for c in self.schema.long_columns_for(attribute))
        )
        # Keyed the same whether input or result: a repeat coordinate is the
        # caller restating one, and replacing it is the answer folding it gave.
        # No `_complete_owned_whole`: the derived frame resolved the current
        # value, so it already carries the extent an edit would have to.
        self._insert(shaped, table, {}, key=self._long_key(attribute))

    def add(self, dim: str, frame: Any) -> None:
        """Stage new labels of `dim` from a wide frame, with the relation rows it names.

        `frame` has a `dim` column. Splits the rest by the schema: an attribute
        addressed by `dim` alone is a column of its axis, one that varies over
        more is `inputs/` rows, and the coordinates of a relation keyed on `dim`
        - `entity_type` for a type relation, `bus` for `connection` - stage that
        relation's row, with any attribute over it.

        Not a sequence of `set` calls: a label exists by its axis row, so a
        value for a label no layer declares is what `_require_labels` rejects.
        Adding a bus with no attributes makes the point - nothing to `set`, yet
        the bus must exist.

        Raises
        ------
        ValueError
            If `frame` has no `dim` column, or a column the schema has no
            place for.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        if dim not in columns:
            msg = f"`add({dim!r}, frame)` needs a {dim!r} column"
            raise ValueError(msg)
        declared = {
            a: spec
            for a, spec in self.schema.attributes.items()
            if dim in self.schema.coordinates_of(a)
        }
        varying = [c for c in columns if c in declared and declared[c].varying]
        by_group: dict[str, list[str]] = {}
        for group in self.schema.groups:
            if dim not in self.schema.group_key(group):
                continue
            coordinates = [c for c in self.schema.group_coordinates(group) if c != dim]
            riding = [
                c
                for c in columns
                if c in declared
                and c not in varying
                and group in self.schema.groups_of(c)
            ]
            if riding or (coordinates and all(c in columns for c in coordinates)):
                by_group[group] = [*coordinates, *riding]
        in_groups = {c for cols in by_group.values() for c in cols}
        axis_columns = [c for c in columns if c not in varying and c not in in_groups]

        rel = as_relation(lazy, self.con)
        axis = self._ensure(f"{_AXIS_PREFIX}{dim}")
        self._reject_undeclared(f"add({dim!r}, ...)", axis, axis_columns)
        # One row per label, replacing any this record already staged for it:
        # `add` after `remove` is one row, the tombstone deleted rather than
        # left to be outranked.
        self._insert(rel, axis, {"deleted": lit(False)}, key=(dim,))  # noqa: FBT003
        for attribute in varying:
            self._stage_long(
                attribute,
                lazy.select(dim, nw.col(attribute).alias("value")),
                "inputs",
                {},
            )
        for group, group_columns in by_group.items():
            self.add_group(
                group,
                lazy.select(dim, *(nw.col(c) for c in dict.fromkeys(group_columns))),
            )

    def _reject_undeclared(self, call: str, table: str, columns: Sequence[str]) -> None:
        """Refuse a column the staging table's file has no place for.

        A staging table is shaped from the schema, like the file it becomes, so
        a column outside that shape has no declared dtype and no reader that
        knows what it means - the same thing `_validate_frame` refuses of an
        axis file, at the edit rather than at the write. Widening the table to
        fit instead would have to guess the dtype from the caller's frame.

        A tool that grows a column changes its schema first, which
        `_reconcile_schema` accepts as a widening; there is no path here that
        needs a column the record cannot describe.

        Raises
        ------
        ValueError
            Naming the columns and what the file does hold.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
        """
        known = list(self.con.table(table).columns)
        allowed = {c.lower() for c in known}
        extra = sorted(c for c in columns if c.lower() not in allowed)
        if not extra:
            return
        msg = (
            f"`{call}` was given columns {extra} the schema does not declare "
            f"there; it holds {sorted(known)}. An attribute's `dims` are what "
            f"put it in a file, so declare it before writing it (https://energy-models.github.io/datarecord/design/schema/#versioning)"
        )
        raise ValueError(msg)

    def _stage_tombstones(
        self,
        kind: str,
        fixed: tuple[str, ...],
        keys: list[list[Any]],
        key: tuple[str, ...],
        attribute: str | None = None,
    ) -> None:
        """Replace one `deleted` row per key.

        Shared by `remove` and `remove_group`, which differ only in their
        columns - a group's key where `remove` carries the entity. One helper so
        the shape is derived from the column list rather than restated per
        caller, which is what let the two drift out of step.

        `fixed` are the columns the tombstone carries; `key` is the subset an
        edit replaces on, which is `entity` alone even where the entity axis
        also carries the type - so a tombstone deletes an `add` row whatever type
        it named.

        `keys` arrives row-oriented and is transposed to build the relation,
        columns being how every insert here crosses into DuckDB.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        """
        table = self._ensure(kind, attribute)
        if not keys:
            return
        by_column = dict(zip(fixed, zip(*keys, strict=True), strict=True))
        rel = self._values_relation(by_column, {c: self._column_type(c) for c in fixed})
        self._insert(
            rel,
            table,
            {"deleted": lit(True)},  # noqa: FBT003
            key=key,
        )

    def remove(self, dim: str, labels: Sequence[Any]) -> None:
        """Stage a tombstone per label of `dim`, which removes it from every attribute.

        Need not enumerate what it deletes: one row per label, on `dim`'s axis,
        and the fold applies it to every attribute keyed on `dim`. Nor scope it -
        a label exists or it does not.

        Raises
        ------
        ValueError
            If `dim` is not declared, or is outside the fold key. A dim outside it
            is owned whole by the layer that last wrote an attribute over it, so a
            tombstone would have no key to remove - declaring it `partial` puts it
            in the key.

        Notes
        -----
        - [add / remove](https://energy-models.github.io/datarecord/design/working-record/#add-remove)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        if dim not in self.schema.dimensions:
            msg = f"`remove({dim!r}, ...)`: no dim {dim!r} is declared"
            raise ValueError(msg)
        if dim not in self.schema.partial_dims:
            msg = (
                f"`remove({dim!r}, ...)`: {dim!r} is outside the fold key "
                f"{list(self.schema.partial_dims)}, so a layer owns it whole and a "
                f"tombstone has no key to remove; declare it `partial`"
            )
            raise ValueError(msg)
        key = self.schema.axis_key(dim)
        if key != (dim,):
            msg = f"`remove({dim!r}, ...)`: a dim `within` {key[:-1]} is not handled"
            raise NotImplementedError(msg)
        self._stage_tombstones(
            f"{_AXIS_PREFIX}{dim}", key, [[label] for label in labels], key
        )

    def add_group(self, group: str, frame: Any) -> None:
        """Stage rows of one declared group from a frame carrying its coordinates.

        The one path every group is added through, a record's `connection`
        group included.

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
        table = self._ensure(group)
        extra = [c for c in columns if c not in coordinates]
        self._reject_undeclared(f"add_group({group!r}, ...)", table, extra)
        self._insert(
            as_relation(lazy, self.con),
            table,
            {"deleted": lit(False)},  # noqa: FBT003
            key=self.schema.group_key(group),
        )

    def remove_group(self, group: str, keys: Sequence[tuple[Any, ...]]) -> None:
        """Stage a tombstone per key, over one declared group's `group_key`.

        An `into` label is no part of a key: the tuple is removed, whatever
        label it carried.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        group_key = self.schema.group_key(group)
        self._stage_tombstones(group, group_key, [list(key) for key in keys], group_key)

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
        """One attribute's staged rows, tombstones applied.

        The rows are already one per coordinate - an edit replaced its key rather
        than appending beside it (`_replace`) - so this is the table scan minus
        the coordinates a component tombstone reaches. That anti-join stays, being
        a cross-table fact rather than an ordering one: a `remove` on the entity
        axis has to clear this attribute's rows for the name too.

        None where nothing is staged for it, which is what says the base's rows
        stand alone.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows("inputs", attribute)
        if rel is None:
            return None
        coordinates = set(self.schema.coordinates_of(attribute))
        for dim in self.schema.partial_dims:
            dead = self._tombstoned(dim) if dim in coordinates else None
            if dead is not None:
                on = null_safe("l", "d", (dim,))
                rel = rel.set_alias("l").join(dead.set_alias("d"), on, how="anti")
        return rel

    def _tombstoned(self, dim: str) -> DuckDBPyRelation | None:
        """Labels whose staged axis row is a tombstone.

        One row per label in the table already (`_replace` on `dim`), so an
        `add` after a `remove` has replaced the tombstone rather than sitting
        above it - the scan is the answer.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        rel = self._rows(f"{_AXIS_PREFIX}{dim}")
        if rel is None or "deleted" not in rel.columns:
            return None
        return rel.filter(col("deleted")).project(col(dim))

    def _collapsed_group(self, group: str) -> DuckDBPyRelation | None:
        """One group's staged rows - a table scan, one per `group_key`.

        Restating a tuple replaced its row (`_replace` on `group_key`), so the
        table already holds one per key; a different `into` label is that edit,
        not a second row.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        return self._rows(group)

    # -- what commit writes (https://energy-models.github.io/datarecord/design/working-record/#committing) -----------------------------------------

    def _staged_dims(self) -> tuple[str, ...]:
        """Which axes have staged rows, in declaration order.

        Rows rather than tables: `_ensure` creates the table before the label
        checks run, so a rejected `set` leaves an empty one behind and a table
        that exists is not yet an edit.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [the schema](https://energy-models.github.io/datarecord/design/schema/)
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

    def _axis_layer(self, dim: str) -> DuckDBPyRelation:
        """One axis as this layer writes it, which `partial` decides the extent of.

        The staged table already holds one complete row per label an edit touched
        (`_patch_axis`), so there is nothing to merge on the way out - what
        `partial` decides is only how many labels are in it:

        - **`partial`** - the touched labels alone, the fold resolving the rest
          from the parent. The staged table is exactly that.
        - **not `partial`** - a dim a layer owns whole once it touches it, so the
          untouched labels are carried from the base, unioned by name so a base
          row lacking a newly-added column reads NULL there.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        staged = self._rows(f"{_AXIS_PREFIX}{dim}")
        assert staged is not None, "only a staged dim reaches an axis layer"
        base = self._base.dims.axes.get(dim)
        if base is None or dim in self.schema.partial_dims:
            return staged
        # The untouched labels the layer owns whole and so must carry, taken from
        # the base with their whole rows - unioned by name, the staged side
        # carrying any column the base lacks and the base side the reverse.
        untouched = base.set_alias("b").join(
            staged.set_alias("s"), null_safe("b", "s", [dim]), how="anti"
        )
        return union_all_by_name([staged, untouched], self.con)

    def _staged_groups(self) -> Frames:
        """The staged group rows, keyed by group - one frame each.

        A `Frames` for `StagedSource.groups()`'s key set, not a builder of the
        rows themselves - `StagedSource.group` reads those directly off
        `_collapsed_group`.
        """
        staged = {
            g: rel
            for g in self.schema.groups
            if (rel := self._collapsed_group(g)) is not None
        }
        return LazyFrames(tuple(staged), lambda group: nw.from_native(staged[group]))

    def _base_revision(self) -> Revision:
        """The `Revision` this record's base resolves, for `NewChild()`'s default.

        Only a base that is a node in the tree has one, which is asked of the
        `revisions` table rather than of the base's type: a directory's
        `revision_id` is derived from where it is, so it names no row, and that
        - not which class the base was - is what makes it unbranchable.

        A missing row and a missing table are the same answer here: `connect`
        creates the table, so a connection without one was made by hand and has
        no tree in it either.
        """
        try:
            return Revision.get(self._base.revision_id, self.con)
        except (KeyError, duckdb.Error):
            msg = (
                "`NewChild()` needs a revision to branch from, and this "
                "`WorkingRecord`'s base is no node in a layer tree; pass one as "
                "`NewChild(revision)`, or commit to a standalone record with "
                "`Directory(uri)` (https://energy-models.github.io/datarecord/design/working-record/#committing)"
            )
            raise ValueError(msg) from None

    @overload
    def commit(self, target: NewChild) -> Revision: ...

    @overload
    def commit(self, target: Directory) -> None: ...

    def commit(self, target: Target) -> Revision | None:
        """Write everything staged and clear it.

        Returns
        -------
        The new child for a `NewChild` target, so the caller can read what it
        just wrote without going back to the record table; `None` for a
        `Directory`, which belongs to no record. Overloaded on the target, so a
        caller committing to a child holds a `Revision` rather than an optional
        one - which target it passed is what decides, and it is always literal
        at the call site.

        The layer lands in the *child*, never in the node that was branched
        from - layers are write-once - so it is the returned node that
        reads back the edits.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        if isinstance(target, NewChild):
            parent = (
                target.record if target.record is not None else self._base_revision()
            )
            child = parent.child()
            # The staged layer's own rows - the fold's last source, a `LayerData`
            # like any other - so only the edits are written, the fold resolving
            # the rest from the parent (https://energy-models.github.io/datarecord/design/working-record/#committing).
            write_record(child.id, self.resolver.sources[-1], self.con)
            self.rollback()
            return child
        # The `Resolver`, not `self`: it is the same `LayerData` a fold answers
        # for everything folded in, which is exactly base-plus-staged flattened
        # (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits).
        write_record(None, self.resolver, self.con, uri=target.uri)
        self.rollback()
        return None


def _base_resolver(base: RecordLike, con: DuckDBPyConnection) -> Resolver:
    """What `base` resolves from, as a `Resolver` a staged layer can extend.

    A `Record` is one already - whether it came from a revision or from
    `Record.at(uri)`, since a directory read as its one layer is a fold like
    any other.

    Raises
    ------
    TypeError
        For anything else. A framework object hands over narwhals frames and has
        no layer layout behind it, so synthesising one would be rebuilding the
        format from a protocol that deliberately lacks it.
    ValueError
        For a `Record` on another connection, which would read its rows through
        one connection and stage them on another.
    """
    if not isinstance(base, Record):
        msg = (
            f"a `WorkingRecord` reads by folding its staged rows over the base's, "
            f"and a {type(base).__name__} has no layer layout to fold - pass a "
            f"`Record`, which `Revision.record` and `Record.at(uri)` both give"
        )
        raise TypeError(msg)
    if base.con is not con:
        msg = (
            "a `WorkingRecord` stages its rows on the connection it reads the "
            "base through, and this base reads through another one"
        )
        raise ValueError(msg)
    return base.resolver


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


def _group_columns(schema: Schema, group: str) -> dict[str, nw.dtypes.DType]:
    """One group's staged columns: its coordinates, and the fold's own.

    An attribute over the group is a column of the group's file, so it is
    declared here too. `role` on a connection reads as a framework's own label,
    but the framework declaring it is what puts it in the schema - and an
    undeclared one has no dtype to give the column.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    over = {
        name: schema.value_type(name) or nw.String()
        for name, spec in schema.attributes.items()
        if not spec.varying and group in schema.groups_of(name)
    }
    return {
        **{c: _column_type(schema, c) for c in schema.group_coordinates(group)},
        "deleted": nw.Boolean(),
        **over,
    }


def _axis_columns(schema: Schema, dim: str) -> dict[str, nw.dtypes.DType]:
    """One axis's staged columns: its key, its tombstone, the attributes it carries.

    The shape of `dims/{dim}.parquet` for the attributes addressed by `dim`
    alone (`attributes_on`), plus `deleted`, which `_validate_frame` admits on
    any axis.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return {
        **{c: _column_type(schema, c) for c in schema.axis_key(dim)},
        "deleted": nw.Boolean(),
        **{a: schema.value_type(a) or nw.String() for a in schema.attributes_on(dim)},
    }


# A staging kind, so it becomes part of a table name: no punctuation a SQL
# identifier would need quoting for, and no collision with a group's name,
# which `Schema` already rejects for colliding with a declared dim.
_AXIS_PREFIX = "axis_of_"

"""The entity axis's staging kind - an axis like any other, named like one.

`dims/entity.parquet` is a file a layer holds, so staging it as an axis is what
makes the staged layer the same shape as every other. What differs is only how an
edit keys it: membership replaces on `entity` alone, so a `remove` then an `add`
under another type resolves to one row, where an ordinary axis patches a column
in place (`_patch_axis`) and keeps its siblings.
"""
