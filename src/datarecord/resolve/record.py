# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The `Record`: a record's layers folded, and the resolved reads gated by its owner map.

The map answers which layer owns each key; `Record` exposes the reads over
it - one long relation per attribute. Tool-agnostic: turning
these into a framework's object is `datarecord.tools`.

Notes
-----
- [the DuckDB read path](https://energy-models.github.io/datarecord/design/layers/resolution/)
- [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from __future__ import annotations

import dataclasses
import operator
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import cached_property, reduce, wraps
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import (
    fn,
    null_safe,
    struct_field,
    struct_of,
    union_all_by_name,
)
from datarecord.protocols import Flags, LazyRelations
from datarecord.resolve.coords import Coords, resolve_coords
from datarecord.resolve.fold import Fold
from datarecord.resolve.owner_map import (
    _coord_table,
    _frozen_coords,
    _table,
    frozen_prefix,
    unsettled,
    with_columns,
)
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.categories import with_properties
from datarecord.schema.schema import LAYER_COLUMN, Schema

if TYPE_CHECKING:
    from datarecord.store.store import Workspace


def _living(
    rel: DuckDBPyRelation, dead: dict[str, DuckDBPyRelation], unkeyed: Sequence[str]
) -> DuckDBPyRelation:
    """`rel` without the rows naming, in one of `unkeyed`, a dead label.

    The fold drops a removed label's rows by anti-joining the owner map on its
    key. A coordinate outside `partial` is no key column, so the fold cannot
    reach the rows naming a label there: they are dropped here instead, against
    that coordinate's `dead` labels (`Coords.dead`).

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    for c in unkeyed:
        labels = dead.get(c)
        if labels is None or c not in rel.columns:
            continue
        rel = rel.set_alias("r").join(
            labels.set_alias("x"), null_safe("r", "x", (c,)), how="anti"
        )
    return rel


def _owned(dim: str) -> str:
    return f"__owned_{dim}"


def _as_stored(om: DuckDBPyRelation, dims: tuple[str, ...]) -> DuckDBPyRelation:
    """`om` with each of `dims` as its owning layer stored it, the owned label in `__owned_<dim>`.

    A raw row then finds its owned keys by NULL-safe equality, which hashes where
    `raw IS NULL OR raw IS NOT DISTINCT FROM owned` does not. A key NULL on a
    dim stays NULL, the read returning it as written. The flags then say how
    the layer stored it, and a key its layer wrote both ways appears once per
    way. A flag field NULL from `UNION ALL BY NAME`, or missing from a map
    folded before the dim was declared, is a dim no row set: `varies` false,
    `broadcast` true.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    types = dict(zip(om.columns, om.types, strict=True))
    for d in dims:
        stored = fn.list_concat(
            duckdb.CaseExpression(
                struct_field(om, "varies", d, lit(False)),  # noqa: FBT003
                fn.list_value(col(d)),
            ),
            duckdb.CaseExpression(
                struct_field(om, "broadcast", d, lit(True)),  # noqa: FBT003
                fn.list_value(lit(None).cast(types[d])),
            ),
        )
        om = om.project(
            *(col(c) for c in types if c != d),
            col(d).alias(_owned(d)),
            fn.unnest(stored).alias(d),
        )
        types = dict(zip(om.columns, om.types, strict=True))
    return om


def _owners(om: DuckDBPyRelation, schema: Schema) -> dict[str, tuple[list[UUID], bool]]:
    """Return per attribute in `om`: its owning layers, and whether one holds keys both ways.

    A layer holds keys both ways where its rows of the attribute name
    different sets of `partial` dims, read off `varies`. A flag field missing
    from a map folded before the dim was declared is unset.
    """
    dims = schema.partial_dims
    contested = "false"
    if dims:
        unset = lit(False)  # noqa: FBT003
        named = struct_of({d: struct_field(om, "varies", d, unset) for d in dims})
        ways = struct_of({"l": col(LAYER_COLUMN), "n": named})
        contested = f"count(DISTINCT {ways}) > count(DISTINCT {LAYER_COLUMN})"
    rows = om.aggregate(
        f"attribute, list(DISTINCT {LAYER_COLUMN}), {contested}", "attribute"
    ).fetchall()
    return {a: (layers, bool(c)) for a, layers, c in rows}


def _named_mask(alias: str, dims: Sequence[str]) -> Expression:
    """Return a bitmask with one bit per dim in `dims`, set where the row under `alias` names a label of that dim."""
    return reduce(
        operator.add,
        (
            col(alias, d).isnotnull().cast(sqltypes.INTEGER) * lit(1 << i)
            for i, d in enumerate(dims)
        ),
        lit(0),
    )


def _most_specific(
    rel: DuckDBPyRelation, coordinates: Sequence[str], attribute: str
) -> DuckDBPyRelation:
    """Keep, at each coordinate, the row of `rel` that names every dim the other rows there name.

    One layer may hold a row naming a label of a `partial` dim beside a row
    leaving that dim NULL, and both join the key that label names. The column
    `_named` holds the `_named_mask` of the `partial` dims, so the row kept is
    the one whose mask is the union of all masks at its coordinate. Where no
    row has that mask, two rows each name a dim the other leaves NULL; where
    two rows at one breakpoint have that mask, they name the same dims.
    `write_layer` refuses such a layer, so it was written another way, and the
    read fails at that coordinate. `breakpoint` is no coordinate, so a curve
    is kept or dropped whole.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    by = ", ".join(str(col(c)) for c in coordinates)
    over = f"OVER (PARTITION BY {by})"
    # A window stays SQL text: DuckDB's expression API has no window construct.
    ranked = rel.project(
        star(),
        sql(f"bit_or(_named) {over}").alias("_all"),
        sql(f"max(bit_count(_named)) {over}").alias("_most"),
        sql(f"count(*) OVER (PARTITION BY {by}, breakpoint, _named)").alias("_twins"),
    )
    incomparable = _refusal(attribute, coordinates, _INCOMPARABLE)
    twins = _refusal(attribute, coordinates, "both name the same dims")
    keep = sql(
        f"CASE WHEN bit_count(_all) <> _most THEN {incomparable} "
        f"WHEN _twins > 1 AND _named = _all THEN {twins} ELSE _named = _all END"
    )
    return ranked.filter(keep).project(
        star(exclude=("_named", "_all", "_most", "_twins"))
    )


_INCOMPARABLE = "each names a dim the other leaves NULL"


def _refusal(attribute: str, coordinates: Sequence[str], why: str) -> Expression:
    """An expression failing the query, naming `attribute`, the row's `coordinates` and `why`.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    coordinate = fn.concat_ws(
        lit(", "),
        *(
            fn.concat(
                lit(f"{c}="), coalesce(col(c).cast(sqltypes.VARCHAR), lit("NULL"))
            )
            for c in coordinates
        ),
    )
    return fn.error(
        fn.concat(
            lit(f"one layer holds two rows of {attribute!r} covering "),
            coordinate,
            lit(
                f", where {why}, so neither is the more specific there "
                "(https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)"
            ),
        )
    )


def _crossings(
    rel: DuckDBPyRelation, coordinates: Sequence[str], attribute: str
) -> DuckDBPyRelation | None:
    """Rows failing the read at each key two rows of `rel` cover and neither names every dim of, unless a third names both.

    `rel` is one layer's rows as written. `None` where no two named sets are
    incomparable, sparing the read the joins.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    crossed = [meets for _, _, meets in unsettled(rel, coordinates)]
    if not crossed:
        return None
    return reduce(DuckDBPyRelation.union, crossed).filter(
        _refusal(attribute, coordinates, _INCOMPARABLE).isnull()
    )


def _stable_cache(method: Callable[[Record], LazyRelations]) -> property:
    """Cache a `Record` member only while the fold behind it cannot go stale.

    Every one of these answers a *key set*, which is a question about which
    layers exist rather than about their rows - so it is cacheable exactly when
    `Record.frozen` holds. Past a staging area it is not: an `add` or a `set`
    naming a new attribute changes the answer, and a cached tuple of names would
    hide the edit that was just made.

    What is cached is the names and a lookup building each relation per access,
    so none is held and the record reads from any thread.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """
    attr = f"_cached_{method.__name__}"

    @wraps(method)
    def get(self: Record) -> LazyRelations:
        if not self.frozen:
            return method(self)
        try:
            return cast("LazyRelations", object.__getattribute__(self, attr))
        except AttributeError:
            value = method(self)
            object.__setattr__(self, attr, value)
            return value

    return property(get)


@dataclass(frozen=True)
class Record:
    """A record's layers folded last-writer-wins, read as DuckDB relations.

    One class for every backing, because a fold over one source degenerates to
    a scan of it - so a plain parquet directory is read as the one layer it is
    (`Workspace.at`) rather than by a second code path. `RecordLike` is the
    protocol a caller annotates against; this is the class a caller holds.

    Nothing up to the last frozen source can go stale, so the fold over them is
    kept in shared tables. A source after it is folded onto those tables per
    read, so a staged edit reads back with nothing to invalidate.

    Parameters
    ----------
    revision_id
        The node whose frozen layers are folded, which names the shared tables
        their fold is kept in.
    sources : list of LayerSource
        Root first, ending in the layer being resolved, truncated at the
        deepest materialised ancestor (`sources_to_read`). A `WorkingRecord`'s
        staged rows are one more source on the end.
    workspace
        Where the calling thread's cursor comes from.
    schema
        The record's one manifest, which every source carries too.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    """

    revision_id: UUID
    sources: list[LayerSource]
    workspace: Workspace
    schema: Schema

    @property
    def con(self) -> DuckDBPyConnection:
        """The cursor of the calling thread, which the relations of this record are planned on.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        """
        return self.workspace.con

    @property
    def frozen(self) -> bool:
        """Whether every source is frozen, so a fold over them cannot go stale.

        False exactly when the last source is a staging area.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
        """
        return frozen_prefix(self.sources) == len(self.sources)

    @_stable_cache
    def dims(self) -> LazyRelations:
        return LazyRelations(tuple(self._coords.axes), self._axis_frame)

    @_stable_cache
    def properties(self) -> LazyRelations:
        coords = self._coords
        subjects = tuple(s for s in self.schema.properties if s in coords.axes)
        return LazyRelations(subjects, self._property_frame)

    @_stable_cache
    def attributes(self) -> LazyRelations:
        return LazyRelations(tuple(self._fold.attributes()), self._relation)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Straight off the owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return self._fold.flags(**categories)

    # -- relations, in member order (the resolved file's row order) (https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis) --

    def _axis_frame(self, dim: str) -> DuckDBPyRelation:
        return self._coords.axes[dim].project(col(dim))

    def _property_frame(self, subject: str) -> DuckDBPyRelation:
        rel = with_properties(self.schema, self._coords.axes[subject], subject)
        names = (subject, *self.schema.properties[subject])
        return rel.project(*(col(n) for n in names))

    # -- the fold ------------------------------------------------------------

    @property
    def _coords(self) -> Coords:
        """Return every declared dim's folded relation, and the dead labels.

        The frozen prefix's fold is a set of shared tables (`_frozen_coords`),
        read on the calling thread's cursor. The sources after it are folded
        per call onto those tables, so a read with staged edits opens no file.
        A schema widened since the fold reads the tables as they are, a
        compatible widening changing no column an axis already has.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
        - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
        """
        prefix = self._prefix
        if self.frozen:
            return prefix
        above = self.sources[frozen_prefix(self.sources) :]
        return resolve_coords(self.schema, self.sources, self.con, (prefix, above))

    @property
    def _prefix(self) -> Coords:
        """Return the frozen prefix's coords, its shared tables read on the calling thread's cursor.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        """
        con = self.con
        tables = {
            (kind, name): con.table(_coord_table(kind, name, self.revision_id))
            for kind, name in self._coord_names
        }
        return Coords(
            schema=self.schema,
            axes={n: rel for (k, n), rel in tables.items() if k == "dims"},
            dead={n: rel for (k, n), rel in tables.items() if k == "dead"},
        )

    @cached_property
    def _coord_names(self) -> list[tuple[str, str]]:
        """Return which axes and dead labels the frozen prefix's fold resolved.

        Plain data, which any thread computes the same.
        """
        prefix = self.sources[: frozen_prefix(self.sources)]
        return _frozen_coords(self.revision_id, prefix, self.con, self.schema)

    @property
    def _fold(self) -> Fold:
        """Return the folded coords and the owner map as one `Fold`.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        coords = self._coords
        return Fold(
            schema=self.schema,
            axes=coords.axes,
            owner_map=_table(
                self.revision_id, self.sources, self.con, self.schema, self._prefix
            ),
            dead=coords.dead,
        )

    def _source_for(self, layer_uuid: UUID) -> LayerSource:
        """Return the source the fold stamped `layer_uuid` from.

        How a winning row is read back: the map names which layer owns a key,
        and the row itself comes from that layer's own member.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        for source in self.sources:
            if source.layer_id == layer_uuid:
                return source
        # A layer the map names but the source list does not hold: the map was
        # materialised over a longer ancestry than this node reads, so the
        # winning row is still in that layer's own directory, in the same store.
        store = next(s.store for s in self.sources if isinstance(s, ParquetLayer))
        return ParquetLayer(layer_uuid, self.schema, store)

    def _relation(self, attribute: str) -> DuckDBPyRelation:
        """Return the resolved long relation of one attribute, folded over every layer.

        Joins the owning layers' rows to the owner map; a broadcast dim stored
        NULL takes each value it is owned for. Unmaterialised, in the long
        schema (`schema.long_columns_for`).

        Raises
        ------
        KeyError
            If no layer owns a row of `attribute`. Only a key set read before
            `rollback()` and used after it asks for such an attribute.
        duckdb.Error
            When fetched, where one layer holds two rows covering a key and
            neither is the more specific there (`_most_specific`), or a staged
            one holds two crossing rows without one naming both (`_crossings`).

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
        """
        con = self.con
        schema = self.schema
        fold = self._fold
        om = fold.owners(attribute)
        partial_dims = schema.partial_dims
        # This attribute's own columns, not every declared dim: one file per
        # attribute is one column set per attribute (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema).
        columns = schema.long_columns_for(attribute)
        # The join's arms are the map's key columns this attribute's file also
        # carries. A dim outside `partial` is in neither: it is not in the map
        # at all, so it constrains nothing and passes straight through.
        #
        # Of those that are, each matches NULL-aware
        # (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule).
        coordinates = schema.coordinates_of(attribute)
        broadcast_over = tuple(d for d in partial_dims if d in coordinates)
        owners, contested = self._owners(attribute, om)
        layers: list[DuckDBPyRelation] = []
        crossed: list[DuckDBPyRelation] = []
        for layer_uuid in owners:
            source = self._source_for(layer_uuid)
            rel = source.attribute(attribute)
            if rel is None:
                continue
            rel = with_columns(schema, rel, *columns)
            layers.append(rel.project(lit(layer_uuid).alias(LAYER_COLUMN), star()))
            if source.frozen:
                continue
            crossing = _crossings(rel, coordinates, attribute)
            if crossing is not None:
                crossed.append(crossing)
        if not layers:
            raise KeyError(attribute)
        unkeyed = [c for c in columns if c not in partial_dims]
        named = [_named_mask("l", broadcast_over).alias("_named")] if contested else []
        resolved = (
            union_all_by_name(layers, con)
            .set_alias("l")
            .join(
                _as_stored(om, broadcast_over).set_alias("o"),
                null_safe("l", "o", (LAYER_COLUMN, *broadcast_over)),
            )
            .project(
                *(
                    coalesce(col("l", dim), col("o", _owned(dim))).alias(dim)
                    if dim in broadcast_over
                    else col("l", dim)
                    for dim in columns
                ),
                *named,
            )
        )
        if contested:
            resolved = _most_specific(resolved, coordinates, attribute)
        resolved = _living(resolved, fold.dead, unkeyed)
        if not crossed:
            return resolved
        types = dict(zip(resolved.columns, resolved.types, strict=True))
        failing = [
            rel.project(*(lit(None).cast(t).alias(c) for c, t in types.items()))
            for rel in crossed
        ]
        return reduce(DuckDBPyRelation.union, failing, resolved)

    def _owners(self, attribute: str, om: DuckDBPyRelation) -> tuple[list[UUID], bool]:
        """Return the layers owning a key of `attribute`, and whether one holds keys both ways.

        A layer holds keys both ways where its rows name different sets of
        `partial` dims. Only then does `_relation` have to pick the more
        specific row.

        Where every source is frozen, one scan of the map answers every
        attribute and is kept; otherwise the map may change under the reader,
        so each call scans `om`, the map's rows of `attribute`.
        """
        if self.frozen:
            return self._cached_owners.get(attribute, ([], False))
        return _owners(om, self.schema).get(attribute, ([], False))

    @cached_property
    def _cached_owners(self) -> dict[str, tuple[list[UUID], bool]]:
        """Return `_owners` for every attribute, where every source is frozen.

        Plain data any thread computes the same, so it is kept on the record
        rather than as a shared table beside the owner map it is read from.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        """
        return _owners(self._fold.owner_map, self.schema)


@dataclass(frozen=True)
class PinnedRecord(Record):
    """A `Record` folding its unfrozen tail once rather than per access.

    For a reader that reads many keys while nothing stages, as `write_layer`
    does: the folded coords and owner map are built on first access and kept,
    so each key costs its own join rather than a re-fold. Held correct only
    while no source changes, and only on the cursor of the thread that built
    it, the relations kept being planned on that cursor. The owner map is a
    table in the database from the first access to `_fold` until `release`,
    which the reader must call.

    Notes
    -----
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """

    @cached_property
    def _coords(self) -> Coords:  # type: ignore[override]
        return super()._coords

    @cached_property
    def _fold(self) -> Fold:  # type: ignore[override]
        fold = super()._fold
        fold.owner_map.create(self._table)
        return dataclasses.replace(fold, owner_map=self.con.table(self._table))

    @cached_property
    def _table(self) -> str:
        return f"_pinned_{uuid4().hex}"

    def release(self) -> None:
        """Drop the table the owner map is held in; nothing reads the fold after."""
        if "_fold" in self.__dict__:
            self.con.execute(f"DROP TABLE IF EXISTS {self._table}")

    def _owners(self, attribute: str, om: DuckDBPyRelation) -> tuple[list[UUID], bool]:
        return self._cached_owners.get(attribute, ([], False))
