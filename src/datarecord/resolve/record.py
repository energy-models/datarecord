# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The `Record`: a record's layers folded, and the resolved reads gated by its owner map.

The map answers which layer owns each map row; `Record` exposes the reads over
it - one long relation per attribute. Tool-agnostic: turning
these into a framework's object is `datarecord.tools`.

Notes
-----
- [the DuckDB read path](https://energy-models.github.io/datarecord/reference/resolution/)
- [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
- [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
"""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cached_property, partial, reduce, wraps
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Self, cast
from uuid import UUID, uuid4

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import StarExpression as star
from duckdb._relation import _render

from datarecord.duck import (
    FOLD,
    anchored,
    empty_relation,
    ex_all,
    fn,
    nested,
    null_pattern,
    null_safe,
    transaction,
    union_all_by_name,
)
from datarecord.protocols import Flags, LazyRelations
from datarecord.resolve.axes import Axes
from datarecord.resolve.fold import Fold
from datarecord.resolve.owner_map import (
    Restated,
    _frozen_table,
    _table,
    owned_rows,
    restated_above,
    source_for,
    split_frozen,
)
from datarecord.resolve.owner_map_rows import shapes
from datarecord.resolve.sources import LayerSource
from datarecord.schema.categories import with_properties
from datarecord.schema.schema import LAYER_COLUMN, Schema, with_columns
from datarecord.select import Selection, select
from datarecord.settle import most_specific

if TYPE_CHECKING:
    from datarecord.store.store import Workspace


def _living(
    rel: DuckDBPyRelation,
    dead: dict[str, DuckDBPyRelation],
    outside_index: Sequence[str],
) -> DuckDBPyRelation:
    """`rel` without the rows naming, in one of `outside_index`, a dead label.

    The fold drops a removed label's rows by anti-joining the owner map on its
    index. A dimension outside `partial` is no index column, so the fold cannot
    reach the rows naming a label there: they are dropped here instead, against
    that dimension's `dead` labels (`Axes.dead`).

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    for c in outside_index:
        labels = dead.get(c)
        if labels is None or c not in rel.columns:
            continue
        rel = rel.set_alias("r").join(
            labels.set_alias("x"), null_safe("r", "x", (c,)), how="anti"
        )
    return rel


def _text(rel: DuckDBPyRelation) -> str:
    """Return the statement `rel` runs, its values written in, as `register` creates a view of."""
    return _render(rel, literals=True, wrap_text=True)[0]


def _owned(dim: str) -> str:
    return f"__owned_{dim}"


def _as_stored(om: DuckDBPyRelation, dims: tuple[str, ...]) -> DuckDBPyRelation:
    """`om` with each of `dims` as its owning layer stored it, the owned label in `__owned_<dim>`.

    A raw row then finds its owned map rows by equality, NULL-safe where it
    may leave a dimension NULL (`_joined`), which hashes where `raw IS NULL
    OR raw IS NOT DISTINCT FROM owned` does not. A map row NULL on a
    dimension stays NULL, the read returning it as written. The flags then
    say how the layer stored it, and a map row its layer wrote both ways
    appears once per way.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    types = dict(zip(om.columns, om.types, strict=True))
    for d in dims:
        stored = fn.list_concat(
            duckdb.CaseExpression(
                col("varies")[d],
                fn.list_value(col(d)),
            ),
            duckdb.CaseExpression(
                col("broadcast")[d],
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


def _label_of(schema: Schema, dim: str, alias: str) -> Expression:
    """What a row's NULL on `dim` under `alias` is tested on: its column, or a multi-dimension's first field.

    Parquet keeps no null count for a whole struct, so `IS NULL` on one prunes
    no row group, where it does on a field. The writer refuses a tuple with a
    NULL field, so the field is NULL exactly where the tuple is.

    Notes
    -----
    - [multi-dimensions](https://energy-models.github.io/datarecord/reference/data-model/#multi-dimensions)
    """
    over = schema.over(dim)
    return col(alias, dim)[next(iter(over))] if over else col(alias, dim)


def _joined(
    values: DuckDBPyRelation,
    stored: DuckDBPyRelation,
    *named: Expression,
    schema: Schema,
    columns: Sequence[str],
    over: tuple[str, ...],
    ways: Mapping[str, tuple[bool, ...]],
) -> DuckDBPyRelation:
    """The layers' rows `values` joined to their owned map rows `stored`, one branch per dimension of `over` a row leaves NULL first.

    `stored` is as `_as_stored` gives it, and `ways` maps each dimension to
    how map rows store it (`_Owners`). A dimension stored one way only is
    tested in every branch. Of the others, taken in the order of `ways`,
    branch `k` joins the rows that set the first `k` and leave the next one
    NULL, tested as `_label_of` gives, and one more branch joins the rows
    that set them all. A dimension a branch's rows set is projected from
    their own column, so DuckDB applies a filter on it in its scan of the
    layer files, which a `coalesce` with the owned label would prevent. The
    one they leave NULL takes the label of each map row they join, NULL
    included, and those after it match NULL-safe and take the `coalesce`.

    The order decides which row groups a branch reads, never the rows it
    returns: a branch reads only the row groups holding a NULL in the
    dimension its rows leave NULL, so `_owners` puts first the dimensions
    more map rows set.
    `n` free dimensions plan `n + 1` branches rather than one per subset, as
    DuckDB plans each and reads the statistics of every layer file per
    branch. The tests sit in the join condition, which DuckDB pushes to
    either side, rather than in filters: each relation operation binds its
    whole input again.

    Notes
    -----
    - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
    - [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
    """
    always = [d for d in over if ways[d] == (True,)]
    never = [d for d in over if ways[d] == (False,)]
    free = [d for d in ways if d in over and ways[d] == (True, False)]
    raw, owned = values.set_alias("l"), stored.set_alias("o")
    branches = []
    for k in range(len(free) + 1):
        setting = [*always, *free[:k]]
        leaving = [*never, *free[k : k + 1]]
        rest = free[k + 1 :]
        on = [col("l", c) == col("o", c) for c in (LAYER_COLUMN, *setting)]
        on += [_label_of(schema, d, "l").isnotnull() for d in setting]
        on += [_label_of(schema, d, a).isnull() for d in leaving for a in ("l", "o")]
        if rest:
            on.append(null_safe("l", "o", rest))
        taken = {d: col("o", _owned(d)).alias(d) for d in leaving}
        taken |= {d: coalesce(col("l", d), col("o", _owned(d))).alias(d) for d in rest}
        joined = raw.join(owned, ex_all(on))
        branches.append(
            joined.project(*(taken.get(c, col("l", c)) for c in columns), *named)
        )
    return reduce(DuckDBPyRelation.union, branches)


class _Owners(NamedTuple):
    """What `_relation` plans the read of one attribute by, read off the owner map.

    Attributes
    ----------
    layers
        The layers that may own a map row of the attribute.
    contested
        Whether one of them holds map rows both ways, so `_relation` ranks rows.
    ways
        Per dimension in `partial`, whether map rows set it (True), leave it
        NULL (False) or both: the branches `_joined` plans, which takes the
        dimensions in this order.
    """

    layers: list[UUID]
    contested: bool
    ways: Mapping[str, tuple[bool, ...]]


def _owners(om: DuckDBPyRelation, schema: Schema) -> dict[str, _Owners]:
    """Return the `_Owners` of each attribute in `om`.

    A layer holds map rows both ways where its rows of the attribute name
    different sets of `partial` dimensions, read off `varies`. `ways` puts
    first the dimension the most map rows set rather than leave NULL, a tie
    in declaration order.
    """
    dims = schema.partial_dimensions
    contested = "false"
    if dims:
        named = fn.struct_pack(**{d: col("varies")[d] for d in dims})
        ways = fn.struct_pack(**{"l": col(LAYER_COLUMN), "n": named})
        contested = f"count(DISTINCT {ways}) > count(DISTINCT {LAYER_COLUMN})"
    columns = [
        "attribute",
        f"list(DISTINCT {LAYER_COLUMN})",
        contested,
        *(str(fn.count_if(col(f)[d])) for d in dims for f in ("varies", "broadcast")),
    ]
    rows = om.aggregate(", ".join(columns), "attribute").fetchall()
    return {
        a: _Owners(layers, bool(c), _ways(dims, counts))
        for a, layers, c, *counts in rows
    }


def _ways(dims: Sequence[str], counts: Sequence[int]) -> dict[str, tuple[bool, ...]]:
    """`_Owners.ways` from the map rows that set and leave NULL each of `dims`, in pairs in `counts`."""
    pairs = dict(zip(dims, zip(counts[::2], counts[1::2], strict=True), strict=True))
    order = sorted(dims, key=lambda d: pairs[d][1] - pairs[d][0])
    return {d: tuple(w for w, n in zip((True, False), pairs[d]) if n) for d in order}


def _contested(stored: DuckDBPyRelation, dims: tuple[str, ...]) -> DuckDBPyRelation:
    """`stored` with `_contested` set on each row of a layer whose map rows name different sets of `dims`.

    `stored` is one attribute's map rows as `_as_stored` gives them. Only a
    value row of such a layer can meet another at one coordinate, so only
    those rows need `_most_specific`: as data, so the plan stays fixed.
    """
    named = null_pattern(dims)
    by = [col(LAYER_COLUMN)]
    contested = named.min().over(partition_by=by) != named.max().over(partition_by=by)
    return stored.project(star(), contested.alias("_contested"))


def _empty_long(
    schema: Schema, attribute: str, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """A row-less relation shaped like `attribute`'s long file, typed as declared.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    types = {
        c: schema.value_type(attribute) if c == "value" else schema.column_type(c)
        for c in schema.long_columns_for(attribute)
    }
    return empty_relation(con, **{c: t or sqltypes.VARCHAR for c, t in types.items()})


def _empty_axis(schema: Schema, dim: str, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """A row-less relation shaped like `dimensions/<dim>.parquet`: its column and its properties.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    """
    names = (dim, *schema.properties.get(dim, {}))
    return empty_relation(
        con, **{n: schema.column_type(n, dim) or sqltypes.VARCHAR for n in names}
    )


def _most_specific(
    rel: DuckDBPyRelation, dims: Sequence[str], attribute: str
) -> DuckDBPyRelation:
    """`most_specific` over one layer's rows of `attribute`, refusing twins too.

    One layer may hold a row naming a label of a `partial` dimension beside a row
    leaving that dimension NULL, and both join the map row that label names. Two rows
    crossing, or two at one breakpoint naming the same dimensions, are a layer
    `write_layer` refuses, so it was written another way, and the read fails
    at that coordinate.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    return most_specific(
        rel,
        dims,
        _refusal(attribute, dims, _INCOMPARABLE),
        _refusal(attribute, dims, "both name the same dimensions"),
    )


_INCOMPARABLE = "each names a dimension the other leaves NULL"


def _refusal(attribute: str, dims: Sequence[str], why: str) -> Expression:
    """An expression failing the query, naming `attribute`, the row's `dims` and `why`.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    coordinate = fn.concat_ws(
        lit(", "),
        *(
            fn.concat(
                lit(f"{c}="), coalesce(col(c).cast(sqltypes.VARCHAR), lit("NULL"))
            )
            for c in dims
        ),
    )
    return fn.error(
        fn.concat(
            lit(f"one layer holds two rows of {attribute!r} covering "),
            coordinate,
            lit(
                f", where {why}, so neither is the more specific there "
                "(https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)"
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
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
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
        self.frozen_axes: Axes | None = None
        self.frozen_map: DuckDBPyRelation | None = None
        self.axes: Axes | None = None
        self.restated: list[Restated] | None = None
        self.owners_of: dict[str, DuckDBPyRelation] = {}


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
    holds those tables from construction until `close()`, or until it and
    every relation taken from it are collected.

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
    - [the Record protocol](https://energy-models.github.io/datarecord/reference/record/)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
    - [the database's schemas](https://energy-models.github.io/datarecord/reference/store/#the-database)
    """

    #: What a read of a closed record raises.
    _closed_message: ClassVar[str] = (
        "this `Record` is closed and its folds released; read the revision's "
        "`record` again "
        "(https://energy-models.github.io/datarecord/reference/store/#the-database)"
    )

    revision_id: UUID
    sources: list[LayerSource]
    workspace: Workspace
    #: Runs once, at collection unless `close()` detaches it first.
    _release: weakref.finalize[..., None] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        holds = self.workspace._holds
        holds.hold(self._holder, self._kept_schemas)
        release = weakref.finalize(self, holds.release, self._holder)
        object.__setattr__(self, "_release", release)

    def close(self) -> None:
        """Release the tables this record reads, each dropped once no other record or registration holds it.

        Closing again does nothing. A closed record refuses every read with a
        `ValueError`, and a relation taken from it before fails once its
        tables are dropped.

        Notes
        -----
        - [the database's schemas](https://energy-models.github.io/datarecord/reference/store/#the-database)
        """
        if self._release.detach() is not None:
            self.workspace._holds.release(self._holder)

    @property
    def closed(self) -> bool:
        """Whether `close()` ran, after which the record refuses every read."""
        return not self._release.alive

    def prepare(self) -> None:
        """Build the shared tables this record's reads create on first use, committed.

        After it, a read of this record on any thread is safe inside a
        transaction that may roll back. Calling it again does nothing.

        Raises
        ------
        duckdb.TransactionException
            Where the tables are missing and the calling thread's cursor holds
            an open transaction.
        ValueError
            Once `close()` ran.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        self._frozen_axes  # noqa: B018 - built for the shared tables it creates
        self._frozen_map  # noqa: B018 - built for the shared table it creates

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @cached_property
    def _holder(self) -> Hashable:
        """This record's key among the workspace's holders, one per instance."""
        return uuid4()

    def _require_open(self) -> None:
        """Refuse a closed record before DuckDB finds its tables gone.

        Called where a read starts from a per-thread cache: `_frozen_axes`,
        `_frozen_map` and `_owners_of`. A subclass says what it lost in
        `_closed_message`.

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
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        return self.workspace.con

    @property
    def frozen(self) -> bool:
        """Whether every source is frozen, so a fold over them cannot go stale.

        False exactly when the last source is a staging area.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
        """
        _, above = split_frozen(self.sources)
        return not above

    @property
    def _fixed(self) -> bool:
        """Whether this record's relations are planned to hold for any rows above its frozen layers.

        True where a source above them can still change, so a relation
        built before an edit reads it.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
        """
        return not self.frozen

    @_stable_cache
    def dimensions(self) -> LazyRelations:
        """Every dimension the schema declares, an empty axis where no layer holds one."""
        return LazyRelations(
            tuple(self.schema.dimensions), partial(self._handed, self._axis_frame)
        )

    @_stable_cache
    def properties(self) -> LazyRelations:
        """Every subject the schema declares properties of, an empty axis where no layer holds one."""
        return LazyRelations(
            tuple(self.schema.properties), partial(self._handed, self._property_frame)
        )

    @_stable_cache
    def attributes(self) -> LazyRelations:
        """Every attribute the schema declares, an empty relation where no layer holds a row."""
        return LazyRelations(
            tuple(self.schema.attributes), partial(self._handed, self._relation)
        )

    def _handed(
        self, build: Callable[[str], DuckDBPyRelation], key: str
    ) -> DuckDBPyRelation:
        """`build(key)`, keeping this record, and so the tables it holds, alive while the relation lives.

        Only a relation handed to a caller is anchored: one cached on the
        record would keep it alive for good.

        Notes
        -----
        - [the database's schemas](https://energy-models.github.io/datarecord/reference/store/#the-database)
        """
        return anchored(build(key), self)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Straight off the owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
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
        - [selecting](https://energy-models.github.io/datarecord/reference/selecting/)
        """
        return select(self, **labels)

    def register(self, name: str) -> None:
        """Create the schema `name` with a view per dimension, property subject and attribute, and one of the owner map.

        The views sit in `name.dimensions`, `name.properties` and `name.attributes` of
        the workspace's database, as in `datarecord.name.dimensions.bus`, each reading as
        its relation here, and the owner map in `name.owner_map`; over a
        `WorkingRecord` they follow every edit. They hold
        the tables the record reads, its folds and a `WorkingRecord`'s staging
        tables, until `unregister(name)`, whether the record is closed or not.
        `name` is listed in `Workspace.registered()`.

        Raises
        ------
        duckdb.CatalogException
            If the schema `name` exists in the workspace's database, as
            `_fold` and `_staged` always do.
        duckdb.TransactionException
            Where the cursor already holds an open transaction, or another
            cursor registers `name` at the same time.

        Notes
        -----
        - [registered views](https://energy-models.github.io/datarecord/reference/format/#registered-views)
        - [the owner_map view](https://energy-models.github.io/datarecord/reference/format/#the-owner_map-view)
        """
        ws = self.workspace
        kinds = {
            "dimensions": self.dimensions,
            "properties": self.properties,
            "attributes": self.attributes,
        }
        views: dict[tuple[str, ...], DuckDBPyRelation] = {
            (ws.name, name, kind, view): rel
            for kind, relations in kinds.items()
            for view, rel in relations.items()
        }
        views[ws.name, name, "owner_map"] = self._owner_map
        rendered = {path: _text(rel) for path, rel in views.items()}
        con = self.con
        holds = ws._holds
        with transaction(
            con,
            "register",
            "https://energy-models.github.io/datarecord/reference/format/#registered-views",
        ) as on_rollback:
            con.execute(f"CREATE SCHEMA {col(ws.name, name)}")
            holds.hold(name, self._kept_schemas)
            on_rollback(partial(holds.release, name))
            for kind in kinds:
                con.execute(f"CREATE SCHEMA {col(ws.name, name, kind)}")
            for path, query in rendered.items():
                con._create_view(path, query)
            con.execute(
                f"INSERT INTO {ws.qualified('registered')} (name, revision, working, held)"
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

        The fold of each source: `Axes.shared` and `_frozen_table` key the
        fold by `revision_id`, the last frozen source's id, and `Axes.held`
        and `Fold.held` an ancestor's by that ancestor's.
        """
        return frozenset(nested(FOLD, s.layer_id) for s in self.sources)

    # -- relations, in axis order (the resolved file's row order) (https://energy-models.github.io/datarecord/reference/resolution/#axes) --

    def _axis(self, dim: str) -> DuckDBPyRelation:
        axis = self._axes.get(dim)
        return _empty_axis(self.schema, dim, self.con) if axis is None else axis

    def _axis_frame(self, dim: str) -> DuckDBPyRelation:
        return self._axis(dim).project(col(dim))

    def _property_frame(self, subject: str) -> DuckDBPyRelation:
        rel = with_properties(self.schema, self._axis(subject), subject)
        names = (subject, *self.schema.properties[subject])
        return rel.project(*(col(n) for n in names))

    # -- the fold ------------------------------------------------------------

    @property
    def _axes(self) -> Axes:
        """Return every declared dimension's folded relation, and the dead labels.

        The frozen layers' fold is a set of shared tables (`Axes.shared`),
        read on the calling thread's cursor. The sources above them are folded
        onto those tables once per thread too, so a read with staged edits
        opens no file: that fold's plan is fixed, reading the staging tables
        by name, so it reads every later edit.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
        """
        frozen = self._frozen_axes
        _, above = split_frozen(self.sources)
        if not above:
            return frozen
        cache = self._per_cursor
        if cache.axes is None:
            cache.axes = Axes.resolve(self.sources, (frozen, above))
        return cache.axes

    @property
    def _frozen_axes(self) -> Axes:
        """Return the frozen layers' axes, their shared tables read on the calling thread's cursor.

        Built once per thread: the tables are write-once, and a relation over
        a table reads it as it is when collected.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        - [a dimension with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dimension-with-one_of)
        """
        self._require_open()
        cache = self._per_cursor
        if cache.frozen_axes is None:
            frozen, _ = split_frozen(self.sources)
            cache.frozen_axes = Axes.shared(self.revision_id, frozen)
        return cache.frozen_axes

    @property
    def _frozen_map(self) -> DuckDBPyRelation | None:
        """Return the frozen layers' owner map, read on the calling thread's cursor, `None` over no frozen source.

        Built once per thread, as `_frozen_axes` is: finding the table asks
        the catalog or the store, which `register` would otherwise do once
        per attribute.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        self._require_open()
        frozen, _ = split_frozen(self.sources)
        if not frozen:
            return None
        cache = self._per_cursor
        if cache.frozen_map is None:
            cache.frozen_map = _frozen_table(self.revision_id, frozen)
        return cache.frozen_map

    @property
    def _restated(self) -> list[Restated]:
        """Return the sources above the frozen layers as the map folds them onto theirs (`restated_above`).

        Built once per thread, as `_frozen_axes` is: the labels a restatement
        removes are the same for every attribute `_owners_of` folds.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
        """
        cache = self._per_cursor
        if cache.restated is None:
            cache.restated = restated_above(self.sources, self._frozen_axes)
        return cache.restated

    @cached_property
    def _per_cursor(self) -> _PerCursor:
        return _PerCursor()

    @property
    def _fold(self) -> Fold:
        """Return the folded axes and the owner map as one `Fold`.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        owner_map = _table(self._frozen_map, self.sources, self._restated)
        return Fold(self._axes, owner_map)

    def _source_for(self, layer_uuid: UUID) -> LayerSource:
        """Return the source the fold stamped `layer_uuid` from.

        How a winning row is read back: the map names which layer owns a map row,
        and the row itself comes from that layer's own member.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        return source_for(self.sources, layer_uuid)

    def _relation(self, attribute: str) -> DuckDBPyRelation:
        """Return the resolved long relation of one attribute, folded over every layer.

        Joins the owning layers' rows to the owner map; a dimension stored
        NULL takes each value it is owned for. Unmaterialised, in the long
        schema (`schema.long_columns_for`), and empty where no layer owns a
        row of `attribute`. Above the frozen layers the plan is fixed: the
        staged layers fold onto their map one attribute at a time, onto none
        where the frozen layers hold no row of it, and only the rows of a
        layer holding map rows both ways are ranked (`_contested`).

        Raises
        ------
        ValueError
            If a frozen layer the owner map names holds no file of `attribute`.
        duckdb.Error
            When fetched, where one layer holds two rows covering a coordinate and
            neither is the more specific there (`_most_specific`).

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/reference/record/)
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        - [resolving a relation](https://energy-models.github.io/datarecord/reference/resolution/#reading-an-attribute)
        """
        con = self.con
        schema = self.schema
        if self._fixed:
            om = self._owners_of(attribute)
            dead = self._axes.dead
        else:
            fold = self._fold
            om = fold.owners(attribute)
            dead = fold.axes.dead
        partial_dimensions = schema.partial_dimensions
        # This attribute's own columns, not every declared dim: one file per
        # attribute is one column set per attribute (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet).
        columns = schema.long_columns_for(attribute)
        # The join's arms are the map's index columns this attribute's file also
        # carries. A dim outside `partial` is in neither: it is not in the map
        # at all, so it constrains nothing and passes straight through.
        #
        # Of those that are, a row matches on the ones it sets, and covers the
        # rest (https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule).
        dims = schema.dimensions_of(attribute)
        broadcast_over = tuple(d for d in partial_dimensions if d in dims)
        owners, contested, ways = self._owners(attribute)
        layers: list[DuckDBPyRelation] = []
        for layer_uuid in owners:
            rel = owned_rows(self._source_for(layer_uuid), attribute)
            if rel is None:
                continue
            rel = with_columns(schema, rel, *columns)
            layers.append(rel.project(lit(layer_uuid).alias(LAYER_COLUMN), star()))
        if not layers:
            return _empty_long(schema, attribute, con)
        outside_index = [c for c in columns if c not in partial_dimensions]
        values = union_all_by_name(layers)
        stored = _as_stored(om, broadcast_over)
        joined = partial(
            _joined,
            values,
            schema=schema,
            columns=columns,
            over=broadcast_over,
            ways=ways,
        )
        if not broadcast_over or not contested:
            return _living(joined(stored), dead, outside_index)
        both = joined(
            _contested(stored, broadcast_over),
            null_pattern(broadcast_over, "l").alias("_named"),
            col("o", "_contested").alias("_contested"),
        )
        kept = [col(c) for c in columns]
        alone = both.filter(~col("_contested")).project(*kept)
        ranked = _most_specific(
            both.filter(col("_contested")).project(*kept, col("_named")),
            dims,
            attribute,
        )
        return _living(alone.union(ranked), dead, outside_index)

    def _owners_of(self, attribute: str) -> DuckDBPyRelation:
        """Return the owner map's rows of `attribute`, folded above the frozen layers on their own.

        A plan that reads every later edit, which the whole map's does not:
        that folds only the attributes the staged layer holds when planned.
        So it is built once per thread.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/reference/editing/)
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        self._require_open()
        cache = self._per_cursor.owners_of
        if attribute not in cache:
            cache[attribute] = _table(
                self._frozen_map,
                self.sources,
                self._restated,
                {attribute: attribute in self._frozen_owners},
            )
        return cache[attribute]

    @property
    def _owner_map(self) -> DuckDBPyRelation:
        """Return the owner map `register` creates a view of, a table scan where every source is frozen.

        Above the frozen layers, a `UNION ALL BY NAME` of one fold per shape,
        each over every declared attribute of its shape, held by the staged
        layer yet or not, so its plan reads every later edit. Each branch is
        rendered as a statement of its own, its CTEs in a `WITH` list of their
        own: DuckDB's binder needs a depth of about the number of CTEs in one
        list.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        - [the owner_map view](https://energy-models.github.io/datarecord/reference/format/#the-owner_map-view)
        """
        if self.frozen or not self.schema.attributes:
            return self._fold.owner_map
        frozen_map, above = self._frozen_map, self._restated
        by_shape = shapes(self.schema, self.schema.attributes).values()
        held = [{a: a in self._frozen_owners for a in names} for names in by_shape]
        arms = (_text(_table(frozen_map, self.sources, above, h)) for h in held)
        return self.con.sql("\nUNION ALL BY NAME\n".join(f"({arm})" for arm in arms))

    def _owners(self, attribute: str) -> _Owners:
        """Return the layers that may own a map row of `attribute`, whether `_relation` ranks rows, and the ways it joins.

        Where every source is frozen, one scan of the map answers every
        attribute and is kept, and rows are ranked only where a layer holds
        map rows both ways, its rows naming different sets of `partial` dimensions.
        Otherwise the layers above the frozen ones may change under the
        reader, so each is a possible owner and every attribute is planned for
        ranking, which `_contested` restricts to such layers' rows as data,
        and for every way of storing a dimension. The dimensions are in the
        order of the frozen layers' map, else as declared: an order only
        speeds a read up.
        """
        none = _Owners([], False, {})
        if not self._fixed:
            return self._cached_owners.get(attribute, none)
        frozen = self._frozen_owners.get(attribute, none)
        _, above = split_frozen(self.sources)
        tail = [s.layer_id for s in above]
        order = [*frozen.ways, *self.schema.partial_dimensions]
        every = dict.fromkeys(order, (True, False))
        return _Owners([*frozen.layers, *tail], True, every)

    @cached_property
    def _frozen_owners(self) -> dict[str, _Owners]:
        """Return `_owners` for every attribute of the frozen layers' map.

        What the layers above them fold onto. They may drop a frozen
        layer's every row of an attribute, which then joins nothing, but add
        no owner other than themselves.
        """
        rel = self._frozen_map
        if rel is None:
            return {}
        frozen, _ = split_frozen(self.sources)
        return _owners(rel, frozen[-1].schema)

    @cached_property
    def _cached_owners(self) -> dict[str, _Owners]:
        """Return `_owners` for every attribute of the whole map.

        Plain data any thread computes the same, so it is kept on the record
        rather than as a shared table beside the owner map it is read from.
        Only a record whose every source is frozen reads it.

        Notes
        -----
        - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
        """
        return _owners(self._fold.owner_map, self.schema)
