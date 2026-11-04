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

import threading
import weakref
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from functools import cached_property, partial, wraps
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from uuid import UUID, uuid4

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star
from duckdb._relation import _render

from datarecord.duck import (
    FOLD,
    empty_relation,
    fn,
    named_mask,
    nested,
    null_safe,
    struct_of,
    transaction,
    union_all_by_name,
)
from datarecord.protocols import Flags, LazyRelations
from datarecord.resolve.coords import Coords
from datarecord.resolve.fold import Fold
from datarecord.resolve.owner_keys import with_columns
from datarecord.resolve.owner_map import (
    _frozen_table,
    _table,
    owned_rows,
    source_for,
    split_frozen,
)
from datarecord.resolve.sources import LayerSource
from datarecord.schema.categories import with_properties
from datarecord.schema.schema import LAYER_COLUMN, Schema
from datarecord.select import Selection, select
from datarecord.settle import most_specific

if TYPE_CHECKING:
    from datarecord.store.store import Workspace, _Holds


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
    way.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    types = dict(zip(om.columns, om.types, strict=True))
    for d in dims:
        stored = fn.list_concat(
            duckdb.CaseExpression(
                fn.struct_extract(col("varies"), lit(d)),
                fn.list_value(col(d)),
            ),
            duckdb.CaseExpression(
                fn.struct_extract(col("broadcast"), lit(d)),
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
    different sets of `partial` dims, read off `varies`.
    """
    dims = schema.partial_dims
    contested = "false"
    if dims:
        named = struct_of({d: fn.struct_extract(col("varies"), lit(d)) for d in dims})
        ways = struct_of({"l": col(LAYER_COLUMN), "n": named})
        contested = f"count(DISTINCT {ways}) > count(DISTINCT {LAYER_COLUMN})"
    rows = om.aggregate(
        f"attribute, list(DISTINCT {LAYER_COLUMN}), {contested}", "attribute"
    ).fetchall()
    return {a: (layers, bool(c)) for a, layers, c in rows}


def _contested(stored: DuckDBPyRelation, dims: tuple[str, ...]) -> DuckDBPyRelation:
    """`stored` with `_contested` set on each row of a layer whose keys name different sets of `dims`.

    `stored` is one attribute's map rows as `_as_stored` gives them. Only a
    value row of such a layer can meet another at one coordinate, so only
    those rows need `_most_specific`: as data, so the plan stays fixed.
    """
    named = str(named_mask(dims))
    over = f"OVER (PARTITION BY {col(LAYER_COLUMN)})"
    contested = f"min({named}) {over} <> max({named}) {over}"
    return stored.project(star(), sql(contested).alias("_contested"))


def _empty_long(
    schema: Schema, attribute: str, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """A row-less relation shaped like `attribute`'s long file, typed as declared.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
    """
    types = {
        c: schema.value_type(attribute) if c == "value" else schema.column_type(c)
        for c in schema.long_columns_for(attribute)
    }
    return empty_relation(con, **{c: t or sqltypes.VARCHAR for c, t in types.items()})


def _empty_axis(schema: Schema, dim: str, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """A row-less relation shaped like `dims/<dim>.parquet`: its key and its properties.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    """
    names = (dim, *schema.properties.get(dim, {}))
    return empty_relation(
        con, **{n: schema.column_type(n, dim) or sqltypes.VARCHAR for n in names}
    )


def _most_specific(
    rel: DuckDBPyRelation, coordinates: Sequence[str], attribute: str
) -> DuckDBPyRelation:
    """`most_specific` over one layer's rows of `attribute`, refusing twins too.

    One layer may hold a row naming a label of a `partial` dim beside a row
    leaving that dim NULL, and both join the key that label names. Two rows
    crossing, or two at one breakpoint naming the same dims, are a layer
    `write_layer` refuses, so it was written another way, and the read fails
    at that coordinate.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    return most_specific(
        rel,
        coordinates,
        _refusal(attribute, coordinates, _INCOMPARABLE),
        _refusal(attribute, coordinates, "both name the same dims"),
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


class _PerCursor(threading.local):
    """What a `Record` keeps per thread, so per cursor: relations are planned on one."""

    def __init__(self) -> None:
        self.frozen_coords: Coords | None = None
        self.coords: Coords | None = None


@dataclass(frozen=True)
class Record:
    """A record's layers folded last-writer-wins, read as DuckDB relations.

    One class for every backing, because a fold over one source degenerates to
    a scan of it - so a plain parquet directory is read as the one layer it is
    (`Workspace.at`) rather than by a second code path. `RecordLike` is the
    protocol a caller annotates against; this is the class a caller holds.

    Nothing up to the last frozen source can go stale, so the fold over them is
    kept in shared tables. A source after it is folded onto those tables per
    read, so a staged edit reads back with nothing to invalidate. The record
    holds those tables from construction until `close()`; one collected
    unclosed leaves them to the workspace.

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

    Raises
    ------
    ValueError
        On a read once the record is `closed`.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
    - [the database's schemas](https://energy-models.github.io/datarecord/design/storage/store/#the-databases-schemas)
    """

    #: What a read of a closed record raises.
    _closed_message: ClassVar[str] = (
        "this `Record` is closed and its folds released; read the revision's "
        "`record` again "
        "(https://energy-models.github.io/datarecord/design/storage/store/#the-databases-schemas)"
    )

    revision_id: UUID
    sources: list[LayerSource]
    workspace: Workspace
    #: Runs once, at collection unless `close()` detaches it first.
    _release: weakref.finalize[..., None] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        holds = self.workspace._holds
        holds.hold(self._holder, self._kept_schemas)
        release = weakref.finalize(self, self._collected(holds), self._holder)
        object.__setattr__(self, "_release", release)

    def close(self) -> None:
        """Release the tables this record reads, each dropped once no other record or registration holds it.

        Closing again does nothing. A closed record refuses every read with a
        `ValueError`, and a relation taken from it before fails once its
        tables are dropped.

        Notes
        -----
        - [the database's schemas](https://energy-models.github.io/datarecord/design/storage/store/#the-databases-schemas)
        """
        if self._release.detach() is not None:
            self.workspace._holds.release(self._holder)

    @property
    def closed(self) -> bool:
        """Whether `close()` ran, after which the record refuses every read."""
        return not self._release.alive

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @cached_property
    def _holder(self) -> Hashable:
        """This record's key among the workspace's holders, one per instance."""
        return uuid4()

    def _collected(self, holds: _Holds) -> Callable[[Hashable], None]:
        """What collecting this record unclosed does with what it holds: abandon it, dropping nothing.

        A chained read, `store.revision(rid).record.attributes["p_nom"].df()`,
        collects its record before `.df()` runs, so a drop here would fail it.
        """
        return holds.abandon

    def _require_open(self) -> None:
        """Refuse a closed record before DuckDB finds its tables gone.

        Called in `_frozen_coords`, which every read takes: the coords and the
        owner map both. A subclass says what it lost in `_closed_message`.

        Raises
        ------
        ValueError
            Once `close()` ran.
        """
        if self.closed:
            raise ValueError(self._closed_message)

    @property
    def schema(self) -> Schema:
        """The schema the record is read under: its last source's."""
        return self.sources[-1].schema

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
        _, above = split_frozen(self.sources)
        return not above

    @property
    def _fixed(self) -> bool:
        """Whether this record's relations are planned without reading a row above its frozen layers.

        True where a source above them can still change, so a relation
        built before an edit reads it.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
        """
        return not self.frozen

    @_stable_cache
    def dims(self) -> LazyRelations:
        """Every dim the schema declares, an empty axis where no layer holds one."""
        return LazyRelations(tuple(self.schema.dimensions), self._axis_frame)

    @_stable_cache
    def properties(self) -> LazyRelations:
        """Every subject the schema declares properties of, an empty axis where no layer holds one."""
        return LazyRelations(tuple(self.schema.properties), self._property_frame)

    @_stable_cache
    def attributes(self) -> LazyRelations:
        """Every attribute the schema declares, an empty relation where no layer holds a row."""
        return LazyRelations(tuple(self.schema.attributes), self._relation)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Straight off the owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return self._fold.flags(**categories)

    def select(self, **labels: Any) -> Selection:
        """This record at the labels named, as `datarecord.select` takes them.

        Raises
        ------
        ValueError
            As `datarecord.select` does, naming the keyword.

        Notes
        -----
        - [selecting](https://energy-models.github.io/datarecord/design/use/selecting/)
        """
        return select(self, **labels)

    def register(self, name: str) -> None:
        """Create the DuckDB schema `name` with a view per dim, property subject and attribute.

        The views sit in `name.dims`, `name.properties` and `name.attributes`,
        each reading as its relation here; over a `WorkingRecord` they follow
        every edit. They hold the tables the record reads, its folds and a
        `WorkingRecord`'s staging tables, until `unregister(name)`, whether
        the record is closed or not. `name` is listed in the workspace's
        `registered()` table.

        Raises
        ------
        duckdb.CatalogException
            If the schema `name` exists, as `_fold`, `_staged` and `_scratch`
            always do.
        ValueError
            If `name` is a database attached to the workspace, which DuckDB
            would read as the catalog of the nested schemas.
        duckdb.TransactionException
            Where the cursor already holds an open transaction.

        Notes
        -----
        - [registered views](https://energy-models.github.io/datarecord/design/storage/views/)
        """
        con = self.con
        if (name,) in con.sql(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall():
            msg = f"`{name}` names a database attached to the workspace; register under another name"
            raise ValueError(msg)
        kinds = {
            "dims": self.dims,
            "properties": self.properties,
            "attributes": self.attributes,
        }
        rendered = {
            (name, kind, view): _render(rel, literals=True, wrap_text=True)[0]
            for kind, relations in kinds.items()
            for view, rel in relations.items()
        }
        holds = self.workspace._holds
        with transaction(
            con,
            "register",
            "https://energy-models.github.io/datarecord/design/storage/views/",
        ) as on_rollback:
            con.execute(f"CREATE SCHEMA {col(name)}")
            holds.hold(name, self._kept_schemas)
            on_rollback(partial(holds.release, name))
            for kind in kinds:
                con.execute(f"CREATE SCHEMA {col(name, kind)}")
            for path, query in rendered.items():
                con._create_view(path, query)
            con.execute(
                "INSERT INTO registered (name, revision, working, held)"
                " VALUES (?, ?, ?, ?)",
                [
                    name,
                    self.revision_id,
                    not self.frozen,
                    sorted(self._kept_schemas),
                ],
            )

    @property
    def _kept_schemas(self) -> frozenset[str]:
        """The schemas this record keeps, which a registration of it keeps too.

        The fold of each source: `Coords.shared` and `_frozen_table` key the
        fold by `revision_id`, the last frozen source's id, and `Coords.held`
        and `Fold.held` an ancestor's by that ancestor's.
        """
        return frozenset(nested(FOLD, s.layer_id) for s in self.sources)

    # -- relations, in member order (the resolved file's row order) (https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis) --

    def _axis(self, dim: str) -> DuckDBPyRelation:
        axis = self._coords.axes.get(dim)
        return _empty_axis(self.schema, dim, self.con) if axis is None else axis

    def _axis_frame(self, dim: str) -> DuckDBPyRelation:
        return self._axis(dim).project(col(dim))

    def _property_frame(self, subject: str) -> DuckDBPyRelation:
        rel = with_properties(self.schema, self._axis(subject), subject)
        names = (subject, *self.schema.properties[subject])
        return rel.project(*(col(n) for n in names))

    # -- the fold ------------------------------------------------------------

    @property
    def _coords(self) -> Coords:
        """Return every declared dim's folded relation, and the dead labels.

        The frozen layers' fold is a set of shared tables (`Coords.shared`),
        read on the calling thread's cursor. The sources above them are folded
        onto those tables once per thread too, so a read with staged edits
        opens no file: that fold's plan is fixed, reading the staging tables
        by name, so it reads every later edit.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/use/editing/#reading-with-pending-edits)
        """
        frozen = self._frozen_coords
        _, above = split_frozen(self.sources)
        if not above:
            return frozen
        cache = self._per_cursor
        if cache.coords is None:
            cache.coords = Coords.resolve(self.sources, (frozen, above))
        return cache.coords

    @property
    def _frozen_coords(self) -> Coords:
        """Return the frozen layers' coords, their shared tables read on the calling thread's cursor.

        Built once per thread: the tables are write-once, and a relation over
        a table reads it as it is when collected.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
        - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
        """
        self._require_open()
        cache = self._per_cursor
        if cache.frozen_coords is None:
            frozen, _ = split_frozen(self.sources)
            cache.frozen_coords = Coords.shared(self.revision_id, frozen)
        return cache.frozen_coords

    @cached_property
    def _per_cursor(self) -> _PerCursor:
        return _PerCursor()

    @property
    def _fold(self) -> Fold:
        """Return the folded coords and the owner map as one `Fold`.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        owner_map = _table(self.revision_id, self.sources, self._frozen_coords)
        return Fold(self._coords, owner_map)

    def _source_for(self, layer_uuid: UUID) -> LayerSource:
        """Return the source the fold stamped `layer_uuid` from.

        How a winning row is read back: the map names which layer owns a key,
        and the row itself comes from that layer's own member.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return source_for(self.sources, layer_uuid)

    def _relation(self, attribute: str) -> DuckDBPyRelation:
        """Return the resolved long relation of one attribute, folded over every layer.

        Joins the owning layers' rows to the owner map; a broadcast dim stored
        NULL takes each value it is owned for. Unmaterialised, in the long
        schema (`schema.long_columns_for`), and empty where no layer owns a
        row of `attribute`. Above the frozen layers the plan is fixed: the
        staged layers fold onto their map one attribute at a time, onto none
        where the frozen layers hold no row of it, and only the rows of a
        layer holding keys both ways are ranked (`_contested`).

        Raises
        ------
        ValueError
            If a frozen layer the owner map names holds no file of `attribute`.
        duckdb.Error
            When fetched, where one layer holds two rows covering a key and
            neither is the more specific there (`_most_specific`).

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
        """
        con = self.con
        schema = self.schema
        if self._fixed:
            in_frozen = attribute in self._frozen_owners
            om = _table(
                self.revision_id,
                self.sources,
                self._frozen_coords,
                attribute,
                in_frozen=in_frozen,
            )
            dead = self._coords.dead
        else:
            fold = self._fold
            om = fold.owners(attribute)
            dead = fold.coords.dead
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
        owners, contested = self._owners(attribute)
        layers: list[DuckDBPyRelation] = []
        for layer_uuid in owners:
            rel = owned_rows(self._source_for(layer_uuid), attribute)
            if rel is None:
                continue
            rel = with_columns(schema, rel, *columns)
            layers.append(rel.project(lit(layer_uuid).alias(LAYER_COLUMN), star()))
        if not layers:
            return _empty_long(schema, attribute, con)
        unkeyed = [c for c in columns if c not in partial_dims]
        values = union_all_by_name(layers).set_alias("l")
        stored = _as_stored(om, broadcast_over)

        def joined(keys: DuckDBPyRelation, *named: Expression) -> DuckDBPyRelation:
            return values.join(
                keys.set_alias("o"),
                null_safe("l", "o", (LAYER_COLUMN, *broadcast_over)),
            ).project(
                *(
                    coalesce(col("l", dim), col("o", _owned(dim))).alias(dim)
                    if dim in broadcast_over
                    else col("l", dim)
                    for dim in columns
                ),
                *named,
            )

        if not broadcast_over or not contested:
            return _living(joined(stored), dead, unkeyed)
        stored = _contested(stored, broadcast_over)
        alone = joined(stored.filter(~col("_contested")))
        ranked = _most_specific(
            joined(
                stored.filter(col("_contested")),
                named_mask(broadcast_over, "l").alias("_named"),
            ),
            coordinates,
            attribute,
        )
        return _living(alone.union(ranked), dead, unkeyed)

    def _owners(self, attribute: str) -> tuple[list[UUID], bool]:
        """Return the layers that may own a key of `attribute`, and whether `_relation` ranks rows.

        Where every source is frozen, one scan of the map answers every
        attribute and is kept, and rows are ranked only where a layer holds
        keys both ways, its rows naming different sets of `partial` dims.
        Otherwise the layers above the frozen ones may change under the
        reader, so each is a possible owner and every attribute is planned for
        ranking, which `_contested` restricts to such layers' rows as data.
        """
        if not self._fixed:
            return self._cached_owners.get(attribute, ([], False))
        layers, _ = self._frozen_owners.get(attribute, ([], False))
        _, above = split_frozen(self.sources)
        tail = [s.layer_id for s in above]
        return [*layers, *tail], True

    @cached_property
    def _frozen_owners(self) -> dict[str, tuple[list[UUID], bool]]:
        """Return `_owners` for every attribute of the frozen layers' map.

        What the layers above them fold onto. They may drop a frozen
        layer's every row of an attribute, which then joins nothing, but add
        no owner other than themselves.
        """
        frozen, _ = split_frozen(self.sources)
        rel = _frozen_table(self.revision_id, frozen)
        return {} if rel is None else _owners(rel, frozen[-1].schema)

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
