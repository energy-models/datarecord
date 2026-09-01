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
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from uuid import UUID, uuid4

import duckdb
import narwhals as nw
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.directory import DirectoryRecord
from datarecord.duck import (
    DuckTypes,
    as_relation,
    distinct_values,
    null_safe,
    union_all_by_name,
)
from datarecord.layered.resolve import NodeCache
from datarecord.layered.revision import LayeredRecord, Revision
from datarecord.layered.sources import DirectorySource
from datarecord.layered.write import write_record
from datarecord.record import (
    EMPTY,
    Flags,
    Frames,
    LazyFrames,
    Record,
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


@dataclass(frozen=True)
class StagedSource:
    """A staging area as one layer's rows - the last source the fold reads.

    "The layer as it would be written": each member collapses its staging table
    last-write-wins and hands over what `write_record` would persist, so `_seq`
    never reaches the fold and nobody can add a raw-rows member later.

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

    def axes(self) -> set[str]:
        """The dims with staged rows, plus `entity` where a member row is staged.

        `entity` is an axis file like any other, so an `add` or a `remove`
        contributes to it - which is what puts a staged component in the
        components map.
        """
        staged = set(self.record._staged_dims())
        if self.record._rows("entities") is not None:
            staged.add("entity")
        return staged

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """One axis as this layer would write it - `_axis_layer`, exactly.

        Which matters for the columns rather than the labels: the fold is
        last-writer-wins per *label*, over the whole row, so a source handing
        over only the column its `set` named would blank every sibling
        attribute on that label. `_axis_layer` merges them per column first,
        which is the same relation `commit` writes.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        if dim == "entity":
            return self.record._collapsed_entities("entities")
        if self.record._rows(f"{_AXIS_PREFIX}{dim}") is None:
            return None
        return self.record._axis_layer(dim)

    def entity_type(self, name: str) -> DuckDBPyRelation | None:
        """One type's staged member rows, the type filtered rather than keyed.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        rel = self.record._collapsed_entities("entities")
        if rel is None:
            return None
        return rel.filter(col("entity_type") == lit(name)).project(
            star(exclude=["entity_type"])
        )

    def group(self, name: str) -> DuckDBPyRelation | None:
        return self.record._collapsed_group(name)

    def attribute(self, name: str, kind: str = "inputs") -> DuckDBPyRelation | None:
        if kind == "outputs":
            rel = self.record._rows("outputs", name)
            # Results do not overlay, so there is nothing to collapse them
            # against; `_seq` is dropped as it is from every other member.
            return None if rel is None else rel.project(star(exclude=["_seq"]))
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

    Staged rows live in connection-scoped DuckDB tables, the *only* place a
    staged row exists: the reads fold them rather than holding a copy, so what
    is staged is asked of the reads themselves.

    A set of pending edits **is** a layer, and the reads deliver that literally:
    a `StagedSource` over those tables is appended to whatever the base resolves
    from, and every read is the same fold one layer deeper. So there is no
    staged overlay of its own to keep in step with the layered one - the
    `Record` members below are `LayeredRecord`'s, over a `NodeCache` this record
    extends.

    Notes
    -----
    - [WorkingRecord](https://energy-models.github.io/datarecord/design/working-record/)
    - [staging](https://energy-models.github.io/datarecord/design/working-record/#staging)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """

    def __init__(self, base: Record, con: DuckDBPyConnection) -> None:
        self.con = con
        # The base as what every use of it here actually wants: its `NodeCache`,
        # resolved once rather than per read. The schema, one dim's axis, one
        # attribute's rows and the revision to branch from are all members of
        # one, so the record the caller passed is not kept.
        self._base = _base_node_cache(base, con)
        # This record's one identity, which is the staged layer's: the fold
        # stamps it as `layer_uuid` and dispatches a winning row back through
        # the source carrying it. Synthetic, the staged layer having no revision
        # until `commit` writes one, and it names the staging tables too - so
        # two `WorkingRecord`s on one connection collide in neither.
        self._layer_id = uuid4()
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

    # -- Record, one fold deeper (https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits) --------------------------------

    @property
    def schema(self) -> Schema:
        return self._base.schema

    @property
    def _resolved(self) -> LayeredRecord:
        """This record as a `LayeredRecord` over the base plus the staged layer.

        Rebuilt per access rather than cached, and cheap because it is: a
        `NodeCache` holds relations, and the staged source being unfrozen is
        what keeps the fold over it from being materialised - so an edit is
        picked up with no invalidation at all.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        """
        staged = StagedSource(self, self._layer_id)
        return LayeredRecord(self._base.with_source(staged))

    @property
    def dims(self) -> Frames:
        return self._resolved.dims

    @property
    def entity_types(self) -> Frames:
        """Base members with pending additions and tombstones applied.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return self._resolved.entity_types

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
        return self._resolved.groups

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
        return self._resolved.attributes

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Straight off the folded owner map, staged rows included.

        Free, as it is for any layered read: the fold computes the flags in its
        ownership `GROUP BY`, and a staged layer is one more layer.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        """
        return self._resolved.flags(ctype)

    def _staged_attribute_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._staged_attributes_of("inputs")))

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
            base_axis = self._base.dims.axes.get(dim)
            if base_axis is None:
                msg = (
                    f"`set({attribute!r}, <scalar>)` reaches every label the "
                    f"{dim!r} axis has, and it has none; name the labels as a "
                    f"mapping, or write the axis file first"
                )
                raise ValueError(msg)
            # The key alone: the axis frame may carry a sibling attribute's
            # column, and this edit states only its own - `_collapsed_axis` folds
            # the rest back per label.
            rel = base_axis.project(col(dim))
            supplied = {attribute: lit(value), "_seq": lit(seq)}

        self._insert(rel, table, supplied)

    def _resolved_names(self, ctype: str) -> list[str]:
        """Every name `ctype` currently resolves to, base plus staged.

        Off the components map rather than the member frame, for the reason
        `_name_types` gives: membership is what the map decided, and resolving
        the type's wide rows to read one column of it is the expensive way to
        ask. `str` because a backend's column yields its own scalar type (a
        `pyarrow.StringScalar`, say), which would compare unequal to the plain
        strings an edit names.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        rel = self._resolved.node_cache.entity_map
        rows = rel.filter(col("entity_type") == lit(ctype)).project("entity").fetchall()
        return [str(n) for (n,) in rows]

    def _name_types(self) -> nw.LazyFrame | None:
        """`(name, entity_type)` over everything this record resolves.

        Straight off the components map, which is keyed by entity and carries
        the type: what every validating caller here asks is "what type is this
        name", and that is the map's own column. Assembling it from the member
        frames instead would resolve each type's wide rows - a union and a join
        per type - to read two columns the fold already decided.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        rel = self._resolved.node_cache.entity_map.project("entity", "entity_type")
        if rel.limit(1).fetchone() is None:
            return None
        return nw.from_native(rel).lazy()

    def _require_unique(self, ctype: str, lazy: nw.LazyFrame) -> None:
        """Reject an `add` whose names another type already holds.

        Re-adding a name of the *same* type is an edit to that member, which
        the fold resolves last-writer-wins - so only a cross-type clash
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
        if not whole or attribute not in self._base.attribute_names():
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
        base = self._base.relation(attribute)
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
        """One axis as this layer writes it, which `partial` decides the extent of.

        Both arms merge the staged columns over the base's *per column*, because
        a label is one row whichever labels the layer carries: a `set` states one
        attribute and the row has to keep its siblings' values. What `partial`
        decides is only how many labels are in it - the touched ones, the fold
        resolving the rest from the parent, or every label, a dim outside
        `partial` being one a layer owns entirely once it touches it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        merged = as_relation(self._axis_frame(dim), self.con)
        if dim not in self.schema.partial_dims:
            return merged
        touched = cast("DuckDBPyRelation", self._collapsed_axis(dim)).project(col(dim))
        return merged.set_alias("m").join(
            touched.set_alias("t"), null_safe("m", "t", [dim]), how="semi"
        )

    def _axis_frame(self, dim: str) -> nw.LazyFrame:
        """One axis, staged columns over the base's row for each label.

        An outer join, since a `set` may name a label no layer has written: one
        side or the other may be missing, and both belong to the answer.

        Only ever reached for a dim some edit staged - `StagedSource.axis`
        answers `None` before calling this where no axis table exists - so
        `staged` is never `None` here and the base's own rows are not a case.
        """
        staged = self._collapsed_axis(dim)
        assert staged is not None, "only a staged dim reaches an axis layer"
        base = self._base.dims.axes.get(dim)
        if base is None:
            # A `set` may introduce a label for an axis no layer has written, in
            # which case the staged rows are the whole of it.
            return nw.from_native(staged)
        present = list(base.columns)
        edited = [a for a in staged.columns if a != dim]
        rel = base.set_alias("b").join(
            staged.set_alias("s"), null_safe("b", "s", [dim]), how="outer"
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
            write_record(child.id, self.staged_only(), self.con)
            self.rollback()
            return child
        write_record(None, self.flattened(), self.con, uri=target.uri)
        self.rollback()
        return None


def _base_node_cache(base: Record, con: DuckDBPyConnection) -> NodeCache:
    """What `base` resolves from, as a `NodeCache` a staged layer can extend.

    A `LayeredRecord` already has one. A `DirectoryRecord` is a parquet
    directory in the layer layout, so it becomes a one-source cache over a
    `DirectorySource` - which is what lets one fold serve both bases, with no
    conditional path and no second overlay. Resolved once at construction, so
    what a `WorkingRecord` holds is the cache rather than the record: every use
    it makes of its base is a member of one.

    Raises
    ------
    TypeError
        For a base that is neither. A framework object hands over narwhals
        frames and has no layer layout behind it: `axis("entity")` wants the
        entity axis and `Record` exposes `entity_types` keyed by type, so
        synthesising one would be rebuilding the format from a protocol that
        deliberately lacks it.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """
    if isinstance(base, LayeredRecord):
        return base.node_cache
    if isinstance(base, DirectoryRecord):
        # Its own id, distinct from the staged layer's: the two are separate
        # sources, and `source_for` dispatches a winning row by matching
        # `layer_uuid` against them - so sharing one would send every staged win
        # to the directory and read the edit back as the base's value. The
        # source derives its own id from where the directory is, so there is
        # nothing to allocate here and two readings of one directory agree on
        # which layer they read.
        source = DirectorySource(base.base, con)
        return NodeCache(source.layer_id, [source], con)
    msg = (
        f"a `WorkingRecord` reads by folding its staged rows over the base's, "
        f"and a {type(base).__name__} has no layer layout to fold - only a "
        f"`LayeredRecord` or a `DirectoryRecord` does"
    )
    raise TypeError(msg)


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
