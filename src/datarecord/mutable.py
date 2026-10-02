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
from datetime import date, datetime
from hashlib import sha256
from numbers import Integral, Real
from typing import TYPE_CHECKING, Any, overload
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
    Frames,
    LazyFrames,
    RecordLike,
)
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
    """Whether `value` is a frame, which supplies its own keys.

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


def _unresolved_targets(
    frame: nw.LazyFrame | None,
    dims: Mapping[str, Any],
    listed: str | None,
    labels: list[Any],
) -> dict[str, Any]:
    """The keywords of a derived `set` whose targets have no current value.

    A named target with no row is a failed change rather than a no-op: the
    caller asked for it to take a new value and there is nothing to derive one
    from. So every label a list names must have a row in the scoped `frame`,
    not just one of them, and the list comes back cut to the labels with none.
    A keyword with one label has a row wherever the scoped frame has any. With
    no keyword the instruction is "whatever resolves", so an empty frame is an
    answer and nothing comes back.

    Labels compare as strings, as `_require_labels` compares them.

    Notes
    -----
    - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
    """
    if not dims:
        return {}
    if frame is None:
        return dict(dims)
    if listed is None:
        return dict(dims) if frame.select("value").head(1).collect().is_empty() else {}
    present = {str(n) for n in frame.select(listed).unique().collect()[listed]}
    absent = [n for n in labels if str(n) not in present]
    if absent or not present:
        return {**dims, listed: absent or labels}
    return {}


def _split_dims(
    dims: Mapping[str, Any],
) -> tuple[str | None, list[Any], dict[str, Any]]:
    """`set`'s keywords split into the one dim given a list, its labels, and the rest.

    A list along two dims would ask for their product, which a long frame
    states unambiguously, so it is refused rather than guessed.

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

    def relations(self) -> set[str]:
        """Which declared relations have staged rows."""
        return set(self.record._staged_relations())

    def relation(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_relation(name)

    def attributes(self) -> set[str]:
        """Which attributes have staged rows."""
        return set(self.record._staged_attributes())

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_inputs(name)

    def all_attributes(self) -> DuckDBPyRelation | None:
        """Every staged attribute, unioned by name and unprojected.

        By name because the tables carry per-attribute column variation exactly
        as the files do - one attribute's coordinates and no others - which is
        what lets `fold_inputs` pad both the same way.

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
    #: Keyed by `(kind, attribute)`, the attribute being None for an axis or a
    #: relation. `attributes` stages one table per attribute because that is the
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

        `kind` is `attributes`, an axis (`_AXIS_PREFIX`), or a declared relation's
        name - a relation gets a table shaped by its own columns.

        `attributes` takes an `attribute` and gets a table per attribute, shaped
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
            columns = _relation_columns(self.schema, kind)
        return DuckTypes(self.con).empty_relation(**columns)

    def _empty_long(self, attribute: str) -> DuckDBPyRelation:
        """A row-less relation shaped like one attribute's long file.

        What the staging table is created from, so the table's shape is a
        projection rather than assembled DDL - the same expressions the inserts
        then project, which is what keeps the two from drifting.

        `value` takes the attribute's declared type.

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

    def _staged_attributes(self) -> tuple[str, ...]:
        """Which attributes have staged rows, in insertion order.

        The staging map is the answer, so this is not a query: a table exists
        exactly where rows were staged.
        """
        return tuple(a for (k, a), _ in self._staged.items() if k == "attributes" and a)

    def _column_type(self, column: str) -> nw.dtypes.DType:
        return _column_type(self.schema, column)

    # -- Record, one fold deeper (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) --------------------------------

    # `schema`, `dims`, `relations`, `attributes` and `flags` are
    # inherited from `Record` unchanged, which is the property this design
    # exists to have: a staged edit is read by the same fold that reads a
    # committed layer, so there is no second overlay to keep in step.

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

    # -- edits (https://energy-models.github.io/datarecord/design/working-record/#set, https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one, https://energy-models.github.io/datarecord/design/working-record/#add-remove) ----------------------------------------

    def _require_labels(self, dim: str, labels: Iterable[Any]) -> None:
        """Reject a label of a relation-keying dim its axis does not hold, base plus staged.

        A dim that keys a relation - `entity` for `connection` - names the rows a
        layer adds and removes, so a value for a label no layer added would
        resolve to a member that does not exist. Other axes take new labels
        from `set`, which is why the check is scoped rather than universal.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        if not any(dim in r.key for r in self.schema.relations.values()):
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
        """The dim vocabulary.

        Notes
        -----
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        unknown = sorted(set(dims) - set(self.schema.dims))
        if unknown:
            msg = f"the schema declares no dims {unknown}"
            raise KeyError(msg)

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

    def set(self, attribute: str, value: Any, **dims: Any) -> None:
        """Stage an attribute value.

        `**dims` scopes the edit, one keyword per coordinate: a label
        (`scenario="high"`), or a list of labels along one dim at most
        (`generator=["wind", "gas"]`). A coordinate no keyword names is
        written NULL, which the broadcast rule reads as every label of it -
        including labels a later layer adds.

        `value` takes three forms:

        - a scalar, the value at every coordinate the keywords name;
        - a long frame, with a column per coordinate it names and a `value`
          column, the keywords supplying the coordinates it leaves out;
        - a narwhals expression, a *function of the current value*, which reads
          before it stages, so two such calls compose.

        A different value per label is a frame:
        `set("p_nom", pd.DataFrame({"entity": ["a", "b"], "value": [1.0, 2.0]}))`.

        Raises
        ------
        KeyError
            If the attribute is not declared, or a label of a dim that keys a
            relation is on no layer's axis.
        TypeError
            If `value` is a mapping, a sequence or a series, or a keyword's
            label is not of its dim's declared dtype, such as a str for a
            `Datetime` dim. A label is never parsed into the dtype.
        ValueError
            If a keyword names a dim the attribute does not vary over, or two
            dims are given lists.

        Notes
        -----
        - [the shape of an edit](https://energy-models.github.io/datarecord/design/working-record/#the-shape-of-an-edit)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
        """
        if isinstance(value, nw.Expr):
            self._validate_dims(dims)
            self._validate_attribute(attribute, dims)
            self._stage_derived(attribute, value, **dims)
            return
        if _is_frame(value):
            lazy = _incoming(value, self.con)
            self._validate_frame(lazy, attribute, dims)
            self._stage_long(attribute, lazy, dims)
            return
        if isinstance(value, Mapping) or (
            isinstance(value, Iterable) and not isinstance(value, str | bytes)
        ):
            coordinates = list(self.schema.coordinates_of(attribute)) or ["<dim>"]
            columns = ", ".join(f"{c!r}: [...]" for c in coordinates)
            msg = (
                f"`set({attribute!r}, ...)` takes a scalar, a long frame or an "
                f"`nw.Expr`, and was given a {type(value).__name__}; pass a value "
                f"per label as a frame, `pd.DataFrame({{{columns}, 'value': [...]}})`"
            )
            raise TypeError(msg)

        listed, labels, fixed = _split_dims(dims)
        self._validate_dims(dims)
        self._validate_attribute(attribute, dims)
        if listed is not None:
            _require_label_types(self.schema, listed, labels)
            self._require_labels(listed, labels)
        for dim, label in fixed.items():
            _require_label_types(self.schema, dim, [label])
            self._require_labels(dim, [label])

        if listed is None:
            rel = self._values_relation({"value": [value]}, {"value": None})
        else:
            rel = self._values_relation(
                {listed: labels, "value": [value] * len(labels)},
                {listed: self._column_type(listed), "value": None},
            )
        table = self._ensure("attributes", attribute)
        present = set() if listed is None else {listed}
        self._insert_long(rel, table, attribute, present, fixed)

    def _labels(self, dim: str) -> list[Any]:
        """Every label `dim`'s axis resolves to, staged edits included."""
        axis = self.resolver.dims.axes.get(dim)
        return [] if axis is None else [n for (n,) in axis.project(dim).fetchall()]

    def _validate_frame(
        self, lazy: nw.LazyFrame, attribute: str, dims: Mapping[str, Any]
    ) -> None:
        """A long input frame's dims, labels and the attribute's spec.

        The frame supplies its own labels, so each of a relation-keying dim must
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
        staged table, so the delete cannot change what the insert then reads.

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
                for d in self.schema.coordinates_of(attribute)
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

        A `None` in `schema` is inferred. An `Enum` is built as its `String` and
        cast by the insert - narwhals cannot construct an arrow enum - which is
        also what rejects a label the dtype does not declare.
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
        self, attribute: str, lazy: nw.LazyFrame, dims: dict[str, Any]
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
        table = self._ensure("attributes", attribute)
        rel = as_relation(lazy, self.con)
        self._insert_long(
            rel, table, attribute, set(lazy.collect_schema().names()), dims
        )

    def _stage_derived(
        self,
        attribute: str,
        expr: nw.Expr,
        **dims: Any,
    ) -> None:
        """Stage a value derived from the current one - the `Expr` form.

        Reads before it stages, so what it derives from is the resolved value
        *including earlier pending edits*, and two such calls compose. What is
        staged is the result, never the expression, so a committed layer holds
        ordinary rows and nothing stores that a value was derived.

        On a layered base the read is a fold, so this is the one edit whose cost
        scales with the ancestry rather than with the rows written.

        The result is collected before it is staged: the read is a relation
        over the staging table the insert replaces rows of (`_insert`), so left
        lazy it would re-read the table after the delete and derive from the
        base instead.

        Unscoped, this derives from every row of the attribute.

        Notes
        -----
        - [a derived value](https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)
        """
        listed, labels, fixed = _split_dims(dims)
        frame = self.attributes[attribute] if attribute in self.attributes else None
        if frame is not None:
            if listed is not None:
                self._require_labels(listed, labels)
                frame = frame.filter(nw.col(listed).is_in(labels))
            for dim, value in fixed.items():
                frame = frame.filter(nw.col(dim) == value)

        unresolved = _unresolved_targets(frame, dims, listed, labels)
        if unresolved:
            scope = ", ".join(f"{d}={v!r}" for d, v in unresolved.items())
            msg = (
                f"no {attribute!r} rows resolve for {scope}, so there is "
                f"no current value to derive from; `set` a value directly to "
                f"create one (https://energy-models.github.io/datarecord/design/working-record/#an-nwexpr-value-derived-from-the-current-one)"
            )
            raise KeyError(msg)
        if frame is None:
            return
        derived = frame.with_columns(expr.alias("value")).collect().lazy()
        self._stage_resolved(derived, attribute)

    def _stage_resolved(self, frame: nw.LazyFrame, attribute: str) -> None:
        """Stage an already-long frame carrying every key column.

        `value` needs no cast: the table is this attribute's own, so its column
        already has the attribute's type (`_empty_long`).
        """
        table = self._ensure("attributes", attribute)
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
        # A repeat coordinate is the caller restating one, and replacing it is
        # the answer folding it gave.
        # No `_complete_owned_whole`: the derived frame resolved the current
        # value, so it already carries the extent an edit would have to.
        self._insert(shaped, table, {}, key=self._long_key(attribute))

    def add(self, dim: str, frame: Any) -> None:
        """Stage new labels of `dim` from a wide frame, with the relation rows it names.

        `frame` has a `dim` column. Splits the rest by the schema: an attribute
        over `dim` is `attributes/` rows, broadcast over its other dims, and the
        other columns of a relation keyed on `dim` - `entity_type` for a type
        relation, `bus` for `connection` - stage that relation's row where the
        frame carries every one of them.

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
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        """
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        if dim not in columns:
            msg = f"`add({dim!r}, frame)` needs a {dim!r} column"
            raise ValueError(msg)
        attributes = [c for c in columns if dim in self.schema.coordinates_of(c)]
        by_relation: dict[str, list[str]] = {}
        for relation in self.schema.relations:
            if dim not in self.schema.relation_key(relation):
                continue
            coordinates = [
                c for c in self.schema.relation_columns(relation) if c != dim
            ]
            if coordinates and all(c in columns for c in coordinates):
                by_relation[relation] = coordinates
        in_relations = {c for cols in by_relation.values() for c in cols}
        axis_columns = [
            c for c in columns if c not in attributes and c not in in_relations
        ]

        rel = as_relation(lazy, self.con)
        axis = self._ensure(f"{_AXIS_PREFIX}{dim}")
        self._reject_undeclared(f"add({dim!r}, ...)", axis, axis_columns)
        # One row per label, replacing any this record already staged for it:
        # `add` after `remove` is one row, the tombstone deleted rather than
        # left to be outranked.
        self._insert(rel, axis, {"deleted": lit(False)}, key=(dim,))  # noqa: FBT003
        for attribute in attributes:
            self._stage_long(
                attribute,
                lazy.select(dim, nw.col(attribute).alias("value")),
                {},
            )
        for relation, carried in by_relation.items():
            self.add_relation(
                relation,
                lazy.select(dim, *(nw.col(c) for c in carried)),
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

        Shared by `remove` and `remove_relation`, which differ only in their
        columns - a relation's key where `remove` carries the entity. One helper so
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
        by_column = dict(zip(fixed, zip(*keys, strict=True), strict=True))
        for column, labels in by_column.items():
            _require_label_types(self.schema, column, labels)
        table = self._ensure(kind, attribute)
        if not keys:
            return
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
        TypeError
            If a label is not of `dim`'s declared dtype.
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

    def add_relation(self, relation: str, frame: Any) -> None:
        """Stage rows of one declared relation from a frame carrying its columns.

        The one path every relation is added through, a record's `connection`
        relation included.

        Notes
        -----
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        """
        coordinates = self.schema.relation_columns(relation)
        lazy = _incoming(frame, self.con)
        columns = lazy.collect_schema().names()
        for required in coordinates:
            if required not in columns:
                msg = f"`add_relation({relation!r}, ...)` needs a {required!r} column"
                raise ValueError(msg)
        table = self._ensure(relation)
        extra = [c for c in columns if c not in coordinates]
        self._reject_undeclared(f"add_relation({relation!r}, ...)", table, extra)
        self._insert(
            as_relation(lazy, self.con),
            table,
            {"deleted": lit(False)},  # noqa: FBT003
            key=self.schema.relation_key(relation),
        )

    def remove_relation(self, relation: str, keys: Sequence[tuple[Any, ...]]) -> None:
        """Stage a tombstone per key, over one declared relation's `relation_key`.

        A `values` label is no part of a key: the tuple is removed, whatever
        label it carried.

        Raises
        ------
        TypeError
            If a key's label is not of its column's declared dtype.

        Notes
        -----
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        """
        relation_key = self.schema.relation_key(relation)
        self._stage_tombstones(
            relation, relation_key, [list(key) for key in keys], relation_key
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
        rel = self._rows("attributes", attribute)
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

    def _collapsed_relation(self, relation: str) -> DuckDBPyRelation | None:
        """One relation's staged rows - a table scan, one per `relation_key`.

        Restating a tuple replaced its row (`_replace` on `relation_key`), so the
        table already holds one per key; a different `values` label is that edit,
        not a second row.

        Notes
        -----
        - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
        """
        return self._rows(relation)

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

        The staged table holds one row per label an edit touched, so what
        `partial` decides is only how many labels are in it:

        - **`partial`** - the touched labels alone, the fold resolving the rest
          from the parent. The staged table is exactly that.
        - **not `partial`** - a dim a layer owns whole once it touches it, so the
          untouched labels are carried from the base.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        staged = self._rows(f"{_AXIS_PREFIX}{dim}")
        assert staged is not None, "only a staged dim reaches an axis layer"
        base = self._base.dims.axes.get(dim)
        if base is None or dim in self.schema.partial_dims:
            return staged
        untouched = base.set_alias("b").join(
            staged.set_alias("s"), null_safe("b", "s", [dim]), how="anti"
        )
        return union_all_by_name([staged, untouched], self.con)

    def _staged_relations(self) -> Frames:
        """The staged relation rows, keyed by relation - one frame each.

        A `Frames` for `StagedSource.relations()`'s key set, not a builder of the
        rows themselves - `StagedSource.relation` reads those directly off
        `_collapsed_relation`.
        """
        staged = {
            r: rel
            for r in self.schema.relations
            if (rel := self._collapsed_relation(r)) is not None
        }
        return LazyFrames(
            tuple(staged), lambda relation: nw.from_native(staged[relation])
        )

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


def _label_type(dtype: nw.dtypes.DType) -> tuple[tuple[type, ...], str] | None:
    """The Python types a label of `dtype` is passed as, and the call that makes one.

    Stated here rather than left to the builders, because each guesses where
    the other refuses: pyarrow reads an int as a `Datetime`'s epoch offset and
    truncates a float to an `Int64`, and DuckDB parses a str as a `Datetime`.
    A dtype outside this table is left to them.
    """
    if isinstance(dtype, nw.String | nw.Enum | nw.Categorical):
        return (str,), "str"
    if isinstance(dtype, nw.Datetime):
        return (datetime,), "pd.Timestamp"
    if isinstance(dtype, nw.Date):
        return (date,), "datetime.date.fromisoformat"
    if dtype.is_integer():
        return (Integral,), "int"
    if dtype.is_float():
        return (Real,), "float"
    return None


def _require_label_types(schema: Schema, dim: str, labels: Iterable[Any]) -> None:
    """Refuse a caller's label whose Python type is not `dim`'s declared dtype.

    Checked before any relation is built from the labels, so the call fails
    here, naming the dim, rather than in the builder. A `None` is the
    broadcast, and a `bool` is no number.

    Raises
    ------
    TypeError
        Naming the dim, its dtype, the first label of another type, and the
        rewrite.
    ValueError
        If `dim` is an `Enum` and a label is none of its categories.
    """
    dtype = _column_type(schema, dim)
    if isinstance(dtype, nw.Enum):
        unknown = sorted(
            {str(n) for n in labels if n is not None} - set(dtype.categories)
        )
        if unknown:
            msg = (
                f"{dim!r} declares no label {unknown}; its dtype is an Enum over "
                f"{sorted(dtype.categories)}, which pins the vocabulary"
            )
            raise ValueError(msg)
    expected = _label_type(dtype)
    if expected is None:
        return
    types, make = expected
    wrong = [
        n
        for n in labels
        if n is not None and (isinstance(n, bool) or not isinstance(n, types))
    ]
    if not wrong:
        return
    first = wrong[0]
    kind = type(first).__name__
    article = "an" if kind[0] in "aeiou" else "a"
    count = f" ({len(wrong)} labels)" if len(wrong) > 1 else ""
    rewrite = (
        f"{make}({first!r})" if make == "str" or isinstance(first, str) else f"a {make}"
    )
    msg = (
        f"{dim} is {type(dtype).__name__}, and {first!r} is {article} {kind}"
        f"{count}; pass {rewrite}"
    )
    raise TypeError(msg)


def _relation_columns(schema: Schema, relation: str) -> dict[str, nw.dtypes.DType]:
    """One relation's staged columns: its own, and its tombstone.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
    """
    return {
        **{c: _column_type(schema, c) for c in schema.relation_columns(relation)},
        "deleted": nw.Boolean(),
    }


def _axis_columns(schema: Schema, dim: str) -> dict[str, nw.dtypes.DType]:
    """One axis's staged columns: its key and its tombstone.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    return {
        **{c: _column_type(schema, c) for c in schema.axis_key(dim)},
        "deleted": nw.Boolean(),
    }


# A staging kind, so it becomes part of a table name: no punctuation a SQL
# identifier would need quoting for, and no collision with a relation's name,
# which `Schema` already rejects for colliding with a declared dim.
_AXIS_PREFIX = "axis_of_"
