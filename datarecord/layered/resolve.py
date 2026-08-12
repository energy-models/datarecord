"""The owner map, and the resolved reads gated by it (design doc §9).

The map answers which layer owns each key; `NodeCache` exposes the reads over
it - one long relation per attribute (§9.2), a type's member frame, this
layer's own outputs (§9.4). Tool-agnostic: turning these into a framework's
object is `datarecord.tools` (§12).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import UUID

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import (
    base_uri_of,
    dims_dirs,
    ex_all,
    fn,
    fold_axis,
    layer_dir,
    resolved_dir,
    schema_uri,
    try_read_parquet,
    union_all_by_name,
)
from datarecord.schema import Schema


def resolve_dims(schema: Schema, ancestry: list[UUID], con: DuckDBPyConnection) -> Dims:
    """Fold every dim `schema` declares to its axis relation.

    A dim's axis file is `{dim}s.parquet`.

    Parameters
    ----------
    schema
        The store's schema, which declares the dims and their keys (§5).
    ancestry : list of UUID
        Root first, ending in the record being resolved, truncated at the
        deepest materialised ancestor (`ancestry_to_read`).
    con : DuckDBPyConnection

    Returns
    -------
    Dims
    """
    dirs = dims_dirs(ancestry)
    axes = {}
    for dim in schema.dims:
        # Keyed by the axis key, not the dim alone: a nested dim's labels
        # identify a point only within its parents (§5.4), so `(period,
        # timestep)` is what last-writer-wins applies to.
        rel = fold_axis(dirs, f"{dim}s.parquet", schema.axis_key(dim), con)
        if rel is not None:
            axes[dim] = rel
    return Dims(schema=schema, axes=axes)


def broadcast_match(
    alias_a: str, alias_b: str, fixed: tuple[str, ...], dims: tuple[str, ...]
) -> Expression:
    """NULL-safe equality on `fixed`, broadcast-OR on `dims` (§5.5).

    A raw row's `dim = NULL` means "every value of `dim`" (§3.3), so it must
    match regardless of the resolved side's value there, unlike a plain
    `IS NOT DISTINCT FROM` which only matches NULL against NULL.
    """
    match = ex_all(
        sql(f"{col(alias_a, c)} IS NOT DISTINCT FROM {col(alias_b, c)}") for c in fixed
    )
    for dim in dims:
        match = match & (
            col(alias_a, dim).isnull()
            | sql(f"{col(alias_a, dim)} IS NOT DISTINCT FROM {col(alias_b, dim)}")
        )
    return match


@dataclass(frozen=True)
class Dims:
    """A store's schema, plus each declared dim's folded axis relation.

    They travel together because the fold needs both wherever it broadcasts a
    NULL against "all values of that dim" (§3.3).

    Parameters
    ----------
    schema : Schema
        The store's schema (§5).
    axes : dict of str to DuckDBPyRelation
        Each declared dim's folded axis relation, full row rather than the key
        column alone - `scenario`'s carries `weight` too. A dim with no rows
        anywhere is absent rather than present-and-empty.
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]

    def component_match(self, alias_a: str, alias_b: str, *fixed: str) -> Expression:
        """Match a raw `dims/components/` row against an already-resolved key."""
        return broadcast_match(alias_a, alias_b, fixed, self.schema.component_dims)

    def connection_match(self, alias_a: str, alias_b: str, *fixed: str) -> Expression:
        """Match a raw `dims/connections/` row against a resolved key (§6).

        Pass `bus` in `fixed`, never as a broadcast dim: it identifies the
        connection rather than broadcasting over an axis.
        """
        return broadcast_match(alias_a, alias_b, fixed, self.schema.connection_dims)

    def input_match(self, alias_a: str, alias_b: str, *fixed: str) -> Expression:
        """Match a raw `inputs/` row against an already-resolved key."""
        return broadcast_match(alias_a, alias_b, fixed, self.schema.input_dims)

    def expand_dims(
        self, rel: DuckDBPyRelation, layer_keys: tuple[str, ...]
    ) -> tuple[DuckDBPyRelation, dict[str, Expression]]:
        """Left-join `rel` against each of `layer_keys`' axis, broadcasting NULLs.

        Parameters
        ----------
        rel : DuckDBPyRelation
        layer_keys : tuple of str
            `schema.input_dims`, `schema.component_dims` or
            `schema.connection_dims`. Never `bus`, which is a required key
            column rather than a broadcast dim (§6).

        Returns
        -------
        DuckDBPyRelation
            `rel`, joined.
        dict of str to Expression
            Per dim, the expression to project for its (possibly broadcast)
            value - referencing `rel`'s original alias, since each join
            wraps the relation in a new auto-generated alias.

        Notes
        -----
        `rel` must carry a column for every dim in `layer_keys`, which
        `write_record` enforces (§5.5): a store whose frames do not is one the
        writer rejected, and binding here would resolve it as though the dim
        were broadcast everywhere.
        """
        alias = rel.alias
        exprs = {}
        for dim in layer_keys:
            axis = self.axes.get(dim)
            if axis is None:
                exprs[dim] = col(alias, dim)
                continue
            key = axis.project(dim)
            rel = rel.join(key, col(alias, dim).isnull(), how="left")
            exprs[dim] = coalesce(col(alias, dim), col(key.alias, dim))
        return rel, exprs


# -- paths and probes -------------------------------------------------------


def _map_uri(record_id: UUID, kind: str) -> str:
    """Where a record's `kind` owner map is materialised, under `resolved/` (§8.2)."""
    return f"{resolved_dir(record_id)}owner_map/{kind}.parquet"


_KIND_NAMES = ("inputs", "components", "connections")


def materialised(record_id: UUID, con: DuckDBPyConnection) -> bool:
    """Whether `record_id`'s node caches are materialised (§8.2).

    A node's caches are written together (`materialise`), so one map answers
    for all three, and for the resolved dims and schema beside them.

    This is a filesystem question rather than recorded state: layers are
    write-once (§8.1), so a materialised cache is valid forever and its
    presence is the whole answer.
    """
    return try_read_parquet(_map_uri(record_id, _KIND_NAMES[0]), con) is not None


def ancestry_to_read(ancestry: list[UUID], con: DuckDBPyConnection) -> list[UUID]:
    """`ancestry` truncated at the deepest materialised node, root first (§8.2).

    A materialised owner map is already folded over everything above it, so
    nothing further up need be read. Only proper ancestors count: the node
    being resolved is always read from its own layer, since stopping *at* it
    would return its cached answer instead of resolving it.

    Parameters
    ----------
    ancestry
        Root-first, ending in the node being resolved (`records.ancestry`).
    con : DuckDBPyConnection

    Returns
    -------
    list of UUID
        The suffix to fold over: from the deepest materialised proper ancestor
        (inclusive) to the node itself, or all of `ancestry` if none is.
    """
    for depth in range(len(ancestry) - 2, -1, -1):
        if materialised(ancestry[depth], con):
            return ancestry[depth:]
    return ancestry


def _table_name(record_id: UUID, kind: str) -> str:
    return f"owner_map_{kind}_{record_id.hex}"


# -- fold relations -----------------------------------------------------


def _deleted_relation(
    record_id: UUID,
    keys: Dims,
    con: DuckDBPyConnection,
    *,
    subdir: str = "components",
    fixed: tuple[str, ...] = ("name",),
    dims: tuple[str, ...] | None = None,
) -> DuckDBPyRelation:
    """This layer's tombstones of one kind (§8.3, §6).

    No `component_type` among the key columns: a tombstone is only ever
    anti-joined against a map's key, and `name` identifies the component (§3.5).

    Parameters
    ----------
    subdir
        `"components"` or `"connections"` - which `dims/` subdirectory the
        tombstones are read from.
    fixed
        Key columns compared NULL-safely and never axis-expanded. `bus` joins
        these for a connection tombstone, since it identifies the connection
        rather than broadcasting over an axis (§6).
    dims
        The dims the tombstone is scoped by, defaulting to
        `keys.schema.component_dims`. Never `keys.schema.input_dims`: deletion removes a
        component or connection whole, across every attribute.
    """
    if dims is None:
        dims = keys.schema.component_dims
    rel = try_read_parquet(
        layer_dir(record_id) + f"dims/{subdir}/*.parquet", con, union_by_name=True
    )
    if rel is None or "deleted" not in rel.columns:
        return _empty_relation(keys.schema, con, *fixed, *dims)
    rel = rel.filter(col("deleted")).set_alias("i")
    rel, expanded = keys.expand_dims(rel, dims)
    return rel.project(
        *(col("i", c) for c in fixed), *(expr.alias(d) for d, expr in expanded.items())
    ).distinct()


def _component_deleted(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """This layer's tombstoned components, scoped by `component_dims` (§8.3)."""
    return _deleted_relation(record_id, keys, con)


def _component_deleted_for_connections(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Component tombstones that remove a connection, keyed as the connections map is.

    A component tombstone kills every connection of that component (§8.3).
    Where the connections map is keyed by fewer dims, the excess are projected
    away and the tombstone applies across every value of them - the conservative
    reading of §14's open question, pinned by an `xfail` in
    `tests/test_connections.py`.
    """
    deleted = _component_deleted(record_id, keys, con)
    if not [
        d for d in keys.schema.component_dims if d not in keys.schema.connection_dims
    ]:
        return deleted
    shared = ("name", *keys.schema.connection_dims)
    return deleted.project(*(col(c) for c in shared)).distinct()


def _connection_deleted(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """This layer's tombstoned connections, scoped by `connection_dims` (§6)."""
    return _deleted_relation(
        record_id,
        keys,
        con,
        subdir="connections",
        fixed=("name", "bus"),
        dims=keys.schema.connection_dims,
    )


def _cast(schema: Schema, rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """`rel`, with its columns of a declared type cast, others as-is (§3.2)."""
    cols = ", ".join(
        f'"{c}"::{t} AS "{c}"' if (t := schema.column_type(c)) else f'"{c}"'
        for c in rel.columns
    )
    return rel.project(cols)


def _empty_relation(
    schema: Schema, con: DuckDBPyConnection, *columns: str
) -> DuckDBPyRelation:
    """A zero-row relation with `columns`, cast to their declared type, `VARCHAR` if none (§3.2)."""
    cols = ", ".join(
        f"NULL::{schema.column_type(c) or 'VARCHAR'} AS {c}" for c in columns
    )
    return con.sql(f"SELECT {cols} WHERE false")


def _struct_of(dims: tuple[str, ...], predicate: str) -> str:
    """`bool_or(predicate)` per dim, packed into one flag struct (§9.1).

    `predicate` is formatted with the dim name, so `'"_raw_{}" IS NULL'` gives
    the broadcast struct. A field aggregating to NULL - what a map written before
    the dim was declared yields - reads as "not set", the same as false.

    `dims` is never empty: `Schema` rejects attributes with no dims (§5.1),
    which is what keeps DuckDB's want of an empty struct off this path.
    """
    fields = ", ".join(f"'{d}': bool_or({predicate.format(d)})" for d in dims)
    return f"{{{fields}}}"


def _null_safe(alias_a: str, alias_b: str, columns: tuple[str, ...]) -> str:
    return " AND ".join(
        f"{alias_a}.{c} IS NOT DISTINCT FROM {alias_b}.{c}" for c in columns
    )


def _with_columns(
    schema: Schema, rel: DuckDBPyRelation, *columns: str
) -> DuckDBPyRelation:
    """`rel` with any of `columns` it lacks added as typed NULLs (§3).

    `bus` and `breakpoint` are part of the long schema but absent from files
    written before them, and a layer may legitimately carry neither: no
    connections, no curves. Reading them as NULL is that exact reading, and
    materialising the column here means every path downstream can project it
    unconditionally instead of branching on its presence.
    """
    missing = [c for c in columns if c not in rel.columns]
    if not missing:
        return rel
    added = ", ".join(
        f'NULL::{schema.column_type(c) or "VARCHAR"} AS "{c}"' for c in missing
    )
    return rel.project(f"*, {added}")


def fold_inputs(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This node's inputs map: `inputs/` keys, folded over `parent` (§9.1)."""
    rel = try_read_parquet(
        layer_dir(record_id) + "inputs/*.parquet", con, union_by_name=True
    )
    if rel is None:
        own = _empty_relation(keys.schema, con, *keys.schema.input_columns)
    else:
        # Every declared dim too, not just `bus`/`breakpoint`: a schema may
        # declare a dim no file carries a column for (§5), and the flags
        # project every dim's raw column below.
        rel = _cast(
            keys.schema,
            _with_columns(keys.schema, rel, "bus", "breakpoint", *keys.schema.dims),
        )
        rel, dims = keys.expand_dims(rel.set_alias("i"), keys.schema.input_dims)
        # Each dim is carried twice: the (possibly broadcast) key value, and
        # `_raw_<dim>` as the row stored it. The flags describe the stored form
        # - whether a row set the dim or left it NULL - so they cannot be read
        # off the expanded value, which is never NULL once broadcast (§4.3).
        tagged = rel.project(
            col("i", "name"),
            col("i", "bus"),
            *(expr.alias(d) for d, expr in dims.items()),
            *(col("i", d).alias(f"_raw_{d}") for d in keys.schema.dims),
            col("i", "attribute"),
            lit(str(record_id)).cast("UUID").alias("layer_uuid"),
            col("i", "breakpoint"),
        )
        own = tagged.aggregate(
            [
                *(col(c) for c in (*keys.schema.input_key, "layer_uuid")),
                sql(_struct_of(keys.schema.dims, '"_raw_{}" IS NOT NULL')).alias(
                    "varies"
                ),
                sql(_struct_of(keys.schema.dims, '"_raw_{}" IS NULL')).alias(
                    "broadcast"
                ),
                fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
            ]
        )

    # Deleting a component drops its attribute rows; deleting one connection
    # drops only that connection's, which the map can scope because `bus` is
    # in `input_key` (§6).
    kept = (
        parent.set_alias("p")
        .join(
            _component_deleted(record_id, keys, con).set_alias("x"),
            _null_safe("x", "p", keys.schema.component_key),
            how="anti",
        )
        .join(
            _connection_deleted(record_id, keys, con).set_alias("c"),
            _null_safe("c", "p", keys.schema.connection_key),
            how="anti",
        )
        .join(
            own.set_alias("o"), _null_safe("p", "o", keys.schema.input_key), how="anti"
        )
    )
    return union_all_by_name([kept, own], con)


def fold_components(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This node's components map: `dims/components/` keys, folded over `parent` (§9.1)."""
    return _fold_ordered(
        record_id,
        keys,
        con,
        parent,
        subdir="components",
        key=keys.schema.component_key,
        columns=keys.schema.component_columns,
        dims=keys.schema.component_dims,
    )


def fold_connections(
    record_id: UUID, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This node's connections map: `dims/connections/` keys, folded over `parent` (§6, §9.1).

    `fold_components` with one more key column (`bus`) and one more tombstone:
    a component tombstone removes every connection of it, so `parent` is
    anti-joined against that as well as against this layer's own connection
    tombstones.

    Where `component_dims` exceeds `connection_dims` (§6) the component
    tombstone is scoped more finely than a connection can be, so it only
    removes the connection when it covers the whole of the excess dim - a
    component deleted in one scenario, whose connections are not
    scenario-scoped, leaves them to the scenarios the component still has.
    """
    return _fold_ordered(
        record_id,
        keys,
        con,
        parent,
        subdir="connections",
        key=keys.schema.connection_key,
        columns=keys.schema.connection_columns,
        dims=keys.schema.connection_dims,
        fixed=("name", "bus"),
        # Keyed as the connections map is: `component_dims` may declare more,
        # and `_component_deleted_for_connections` resolves that excess.
        also_deleted=_component_deleted_for_connections,
        also_deleted_key=("name", *keys.schema.connection_dims),
    )


def _fold_ordered(
    record_id: UUID,
    keys: Dims,
    con: DuckDBPyConnection,
    parent: DuckDBPyRelation,
    *,
    subdir: str,
    key: tuple[str, ...],
    columns: tuple[str, ...],
    dims: tuple[str, ...],
    fixed: tuple[str, ...] = ("name",),
    also_deleted: Callable[[UUID, Dims, DuckDBPyConnection], DuckDBPyRelation]
    | None = None,
    also_deleted_key: tuple[str, ...] = (),
) -> DuckDBPyRelation:
    """The shared fold for the two maps that carry an `order_key` (§9.1, §9.1).

    `components` and `connections` differ only in which `dims/` subdirectory
    they read, which columns key them, and whether a second tombstone kind
    applies - so the `order_key` assignment, which is the subtle part, lives
    here once rather than in each.

    Parameters
    ----------
    fixed
        Key columns that are never axis-expanded (`bus` for connections).
        `component_type` is not among them: the map carries it as a column
        determined by `name` rather than as part of the key (§3.5), so it is
        projected and aggregated over below instead of grouped by.
    also_deleted, also_deleted_key
        A second tombstone relation to anti-join `parent` against, and the key
        to match it on. Connections use it for component tombstones (§6).
    """
    rel = try_read_parquet(
        layer_dir(record_id) + f"dims/{subdir}/*.parquet", con, union_by_name=True
    )
    if rel is None:
        # No rows, so `order_key` needs no values either - just the column.
        own = _empty_relation(keys.schema, con, *columns)
    else:
        not_deleted = ~col("deleted") if "deleted" in rel.columns else lit(True)
        rel = _cast(keys.schema, rel)
        rel, expanded = keys.expand_dims(rel.filter(not_deleted).set_alias("i"), dims)
        tagged = rel.project(
            col("i", "component_type"),
            *(col("i", c) for c in fixed),
            *(expr.alias(d) for d, expr in expanded.items()),
            lit(str(record_id)).cast("UUID").alias("layer_uuid"),
            sql("row_number() OVER ()").alias("_row"),
        )
        # `component_type` is aggregated rather than grouped by: it is a column
        # of the map determined by `name` (§3.5), and grouping on it would let
        # one name under two types survive as two rows - the collision the write
        # path rejects, silently resolved here instead.
        own = tagged.aggregate(
            [
                *(col(c) for c in (*key, "layer_uuid")),
                fn.any_value(col("component_type")).alias("component_type"),
                fn.min(col("_row")).alias("_row"),
            ]
        )
        own = con.sql(
            "SELECT * EXCLUDE (_row),"
            " COALESCE((SELECT max(order_key::BIGINT) FROM parent), -1)"
            " + row_number() OVER (ORDER BY _row) AS order_key"
            " FROM own"
        )
    kept = parent.set_alias("p")
    if also_deleted is not None:
        kept = kept.join(
            also_deleted(record_id, keys, con).set_alias("a"),
            _null_safe("p", "a", also_deleted_key),
            how="anti",
        )
    deleted = _deleted_relation(
        record_id, keys, con, subdir=subdir, fixed=fixed, dims=dims
    )
    kept = kept.join(
        deleted.set_alias("x"), _null_safe("p", "x", key), how="anti"
    ).join(own.set_alias("o"), _null_safe("p", "o", key), how="anti")
    return union_all_by_name([kept, own], con)


def _fold_map(
    ancestry: list[UUID],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    columns: Callable[[Dims], tuple[str, ...]],
    map_uri: Callable[[UUID], str],
    fold: Callable[
        [UUID, Dims, DuckDBPyConnection, DuckDBPyRelation], DuckDBPyRelation
    ],
) -> DuckDBPyRelation:
    """A record's owner map of one kind, folded down over `ancestry` (§9.1).

    `schema` is passed in rather than read here: one manifest serves the whole
    store (§5.6), so the three kinds fold under one read of it.
    """
    keys = resolve_dims(schema, ancestry, con)

    rel = _empty_relation(keys.schema, con, *columns(keys))
    root = try_read_parquet(map_uri(ancestry[0]), con) if len(ancestry) > 1 else None
    start = 0
    if root is not None:
        rel, start = _cast(keys.schema, root), 1

    for uid in ancestry[start:]:
        rel = fold(uid, keys, con, rel)
    return rel


# The three owner maps (§9.1), each a `kind` -> (column set, fold) pair.
_KINDS: dict[str, tuple[Callable[[Dims], tuple[str, ...]], Callable]] = {
    "inputs": (lambda keys: keys.schema.input_columns, fold_inputs),
    "components": (lambda keys: keys.schema.component_columns, fold_components),
    "connections": (lambda keys: keys.schema.connection_columns, fold_connections),
}


def _fold_kind(
    kind: str, ancestry: list[UUID], con: DuckDBPyConnection, schema: Schema
) -> DuckDBPyRelation:
    columns, fold = _KINDS[kind]
    return _fold_map(
        ancestry,
        con,
        schema,
        columns=columns,
        map_uri=lambda uid: _map_uri(uid, kind),
        fold=fold,
    )


def _table(
    record_id: UUID,
    ancestry: list[UUID],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    kind: str,
) -> DuckDBPyRelation:
    """One owner map for `record_id` (§9.1).

    Its own materialised map if it has one, else the fold over `ancestry`
    (already truncated at the deepest materialised ancestor, §8.2). The live fold
    is cached as a connection-scoped table, which never needs invalidating since
    layers are write-once (§8.1).
    """
    persisted = try_read_parquet(_map_uri(record_id, kind), con)
    if persisted is not None:
        return _cast(schema, persisted)

    name = _table_name(record_id, kind)
    try:
        return con.table(name)
    except duckdb.CatalogException:
        _fold_kind(kind, ancestry, con, schema).create(name)
        return con.table(name)


def materialise(record_id: UUID, ancestry: list[UUID], con: DuckDBPyConnection) -> None:
    """Write `record_id`'s node caches: owner maps and resolved dims (§8.2).

    Purely additive. It changes no answer a read would give, only how many
    layers a read touches to reach it: once these files exist, a descendant's
    fold stops here rather than walking further up (`ancestry_to_read`).

    Safe to call more than once, and safe to call on any node - layers are
    write-once (§8.1), so what is folded here cannot later become stale.
    """
    schema = read_schema(con)
    base = resolved_dir(record_id) + "owner_map/"
    if "://" not in base:
        # A record that wrote nothing to its layer has no node dir yet either.
        Path(base).mkdir(parents=True, exist_ok=True)
    for kind in _KINDS:
        _fold_kind(kind, ancestry, con, schema).to_parquet(_map_uri(record_id, kind))
    _materialise_dims(record_id, ancestry, con, schema)


def _materialise_dims(
    record_id: UUID, ancestry: list[UUID], con: DuckDBPyConnection, schema: Schema
) -> None:
    """Fold this node's resolved axes into its node cache, not its layer (§8.2)."""
    dims = resolve_dims(schema, ancestry, con)
    base = resolved_dir(record_id) + "dims/"
    if "://" not in base:
        Path(base).mkdir(parents=True, exist_ok=True)
    for dim, rel in dims.axes.items():
        rel.to_parquet(f"{base}{dim}s.parquet")


# -- the schema (design doc §5.6) ---------------------------------------------


def read_json(uri: str) -> dict[str, Any] | None:
    """Read one JSON file, or `None` if it doesn't exist (e.g. an undeclared schema).

    Only a genuine miss (local `FileNotFoundError`, remote 404/403) maps to
    `None` - any other failure raises rather than silently reading as absent.
    """
    try:
        if "://" in uri:
            with urlopen(uri) as fh:  # noqa: S310 - store URIs are derived, not user input
                return json.load(fh)
        with open(uri) as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except HTTPError as e:
        if e.code in (403, 404):  # 403: S3's "missing key" without ListBucket
            return None
        raise


def read_schema(con: DuckDBPyConnection | None = None) -> Schema:
    """The store's one schema, read from beside the layers (§5.6).

    No fold and no ancestry: a schema is not layered data. One file makes it a
    property of the store, knowable before any layer is read and stated once
    for a hundred-layer tree. A store that declares none reads as an empty
    `Schema`, which declares no dims and no attributes.

    Parameters
    ----------
    con
        Read the manifest beside *this* connection's layers. A connection is
        already scoped to one store root (`connect(base_uri=...)`), so the
        schema follows from it rather than from a separate parameter. `None`
        reads the process default, which is what a caller holding no
        connection gets.
    """
    base = None if con is None else base_uri_of(con)
    raw = read_json(schema_uri(base))
    return Schema() if raw is None else Schema.model_validate(raw)


def write_schema(schema: Schema, base_uri: str | None = None) -> None:
    """Write the store's one schema, beside the layers (§5.6).

    Amending it is a schema change rather than a patch, so this replaces the
    file: `Schema.compatible_with` is what says whether existing layers
    survive the amendment (§5.7).
    """
    uri = schema_uri(base_uri)
    if "://" not in uri:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    with open(uri, "w") as fh:
        fh.write(schema.model_dump_json())


# -- public API -------------------------------------------------------------


@dataclass(frozen=True)
class NodeCache:
    """A record's resolved view: owner map, dims, schema, and the relations over them.

    The cached artifacts (§9.1, §8.2, §8.2) and the reads gated by them
    (`relation`/`outputs`/`component_frame`/`connection_frame`/
    `attributes_of`, §9.2) live together because every one of the latter is a
    semi-join against the former. Tool-agnostic throughout: the long relations
    here are what a tool (`datarecord.tools`) builds its own object from.

    Notes
    -----
    `ancestry` is root first, ending in `record_id`, and already truncated at
    the deepest materialised ancestor (`ancestry_to_read`) - so a hundred-layer
    tree with a materialised parent resolves from two entries (§8.2).

    Everything here is a `cached_property` or reads a connection-scoped table:
    layers are write-once (§8.1), so nothing an instance caches can go stale.
    """

    record_id: UUID
    ancestry: list[UUID]
    con: DuckDBPyConnection

    def _map(self, kind: str) -> DuckDBPyRelation:
        return _table(self.record_id, self.ancestry, self.con, self.schema, kind=kind)

    @property
    def inputs(self) -> DuckDBPyRelation:
        return self._map("inputs")

    @property
    def components(self) -> DuckDBPyRelation:
        return self._map("components")

    @property
    def connections(self) -> DuckDBPyRelation:
        return self._map("connections")

    @cached_property
    def dims(self) -> Dims:
        return resolve_dims(self.schema, self.ancestry, self.con)

    @cached_property
    def schema(self) -> Schema:
        """This store's one manifest, read once per node (§5.6).

        From beside *this* connection's layers, so two records on different
        roots in one process each read their own.
        """
        return read_schema(self.con)

    def component_types(self) -> set[str]:
        """Types with any live component row, straight from the owner map (§8.2)."""
        rows = self.components.project("component_type").distinct().fetchall()
        return {r[0] for r in rows}

    def attribute_names(self) -> list[str]:
        """Every input attribute any layer owns a row for, from the owner map (§9.1).

        Across component types, matching the file layout: one
        `inputs/<attr>.parquet` holds every type's rows (§3). Ordered, so a
        `Record` over this has a stable key order (§9.3).
        """
        rows = self.inputs.project("attribute").distinct().order("attribute").fetchall()
        return [r[0] for r in rows]

    def output_names(self) -> list[str]:
        """Every result attribute this record's own layer holds (§9.4).

        Its own layer only: outputs do not overlay, so there is no map to
        consult and nothing inherited from an ancestor.
        """
        uri = f"{layer_dir(self.record_id)}outputs/*.parquet"
        rel = try_read_parquet(uri, self.con, union_by_name=True)
        if rel is None:
            return []
        rows = rel.project("attribute").distinct().order("attribute").fetchall()
        return [r[0] for r in rows]

    def attributes_of(
        self, ctype: str
    ) -> dict[str, tuple[frozenset[str], frozenset[str], bool]]:
        """Per attribute of `ctype`, which dims its rows use (§4.3).

        The map computes the flags per key, so per component; this unions them
        over the names of one type, which is the granularity a consumer assigns
        containers at. A type whose components disagree yields a dim in both
        sets - the instruction to use both containers, each taking the rows it
        matches (§4.3). The union stops at the type boundary: across types it
        would describe neither.

        Returns
        -------
        dict
            `attribute -> (varies, broadcast, breakpoints)`, the raw material
            `Record.flags` turns into `Flags` (§4.3).
        """
        dims = self.schema.dims
        # Scoping to a type is a semi-join to the components map on `name`
        # rather than a filter on the attribute rows, which no longer carry the
        # type (§3.5). The map is the entity table that says what a name is.
        of_type = self.components.filter(col("component_type") == lit(ctype)).project(
            "name"
        )
        rows = (
            self.inputs.set_alias("i")
            .join(of_type.distinct().set_alias("e"), "i.name = e.name", how="semi")
            .aggregate(
                [
                    col("attribute"),
                    sql(_struct_of(dims, '"varies"."{}"')).alias("varies"),
                    sql(_struct_of(dims, '"broadcast"."{}"')).alias("broadcast"),
                    fn.bool_or(col("breakpoints")).alias("breakpoints"),
                ]
            )
            .fetchall()
        )
        # The structs come back as dicts keyed by dim, so the two sets are a
        # filter rather than a positional slice. A field aggregating to NULL -
        # a dim declared after this map was written - is falsy, so absent.
        return {
            attribute: (
                frozenset(d for d in dims if varies.get(d)),
                frozenset(d for d in dims if broadcast.get(d)),
                bool(breakpoints),
            )
            for attribute, varies, broadcast, breakpoints in rows
        }

    def relation(self, attribute: str) -> DuckDBPyRelation:
        """The resolved long relation for one input attribute (§9.2).

        Semi-joins the owning layers' `inputs/<attribute>.parquet` to the
        `inputs` owner map, so only owned rows survive: the map already names
        the winning layer per key, so there is no per-read `MAX`/group-by and
        no tombstone filter (deletions are already absent from the map).

        A stored NULL for an `input_key` dim means "all values" and may be
        owned for only some of them (§5.5), so each key dim's join arm is
        NULL-aware and the row takes the value it is owned for.

        Returns
        -------
        DuckDBPyRelation
            Unmaterialised, in the long schema (`schema.long_columns`).
            Empty when no layer wrote the attribute - the consumer then
            applies the catalog `default` (§3).
        """
        con = self.con
        om = self.inputs.filter(col("attribute") == lit(attribute))
        keys = self.dims
        input_dims = keys.schema.input_dims
        layers = [
            _with_columns(
                keys.schema,
                con.read_parquet(f"{layer_dir(layer_uuid)}inputs/{attribute}.parquet"),
                "bus",
                "breakpoint",
            ).project(lit(layer_uuid).alias("layer_uuid"), col("*"))
            for (layer_uuid,) in om["layer_uuid"].distinct().fetchall()
        ]
        if not layers:
            return _empty_relation(keys.schema, con, *keys.schema.long_columns)

        return (
            union_all_by_name(layers, con)
            .set_alias("l")
            .join(
                om.set_alias("o"),
                # `bus` joins the fixed columns, not the broadcast dims: NULL
                # means "the component's own attribute", never "every bus",
                # so it is compared NULL-safely and never expanded (§6).
                keys.input_match("l", "o", "name", "bus", "layer_uuid"),
            )
            .project(
                *(
                    coalesce(col("l", dim), col("o", dim)).alias(dim)
                    if dim in input_dims
                    else col("l", dim)
                    for dim in keys.schema.long_columns
                )
            )
        )

    def outputs(self, attribute: str) -> DuckDBPyRelation:
        """A result attribute from this record's own layer; outputs do not overlay (§9.4).

        No fold and no owner map: if this layer has no `outputs/`, the record
        has no results - an ancestor's are not inherited.
        """
        uri = f"{layer_dir(self.record_id)}outputs/{attribute}.parquet"
        rel = try_read_parquet(uri, self.con)
        if rel is not None:
            return rel
        return _empty_relation(self.schema, self.con, *self.schema.long_columns)

    def component_frame(self, ctype: str) -> DuckDBPyRelation | None:
        """Wide static members of one type, resolved from the owner map (§8.2)."""
        return self._dim_frame(
            ctype,
            subdir="components",
            owner_map=self.components,
            match=("name",),
            dims=self.schema.component_dims,
        )

    def connection_frame(self, ctype: str) -> DuckDBPyRelation | None:
        """One type's connections, resolved from the `connections` owner map (§6).

        Carries `role` and any other non-key column of the connection row -
        those describe the connection rather than keying it, so the fold does
        not track them and they come straight from the owning layer's file.
        """
        return self._dim_frame(
            ctype,
            subdir="connections",
            owner_map=self.connections,
            match=("name", "bus"),
            dims=self.schema.connection_dims,
        )

    def _dim_frame(
        self,
        ctype: str,
        *,
        subdir: str,
        owner_map: DuckDBPyRelation,
        match: tuple[str, ...],
        dims: tuple[str, ...],
    ) -> DuckDBPyRelation | None:
        """One type's rows from a `dims/` subdirectory, gated by its owner map.

        Shared by `component_frame` and `connection_frame`: both semi-join the
        owning layers' files to a map keyed the same way, differing only in
        which columns match and which dims scope them.
        """
        con = self.con
        owned = owner_map.filter(col("component_type") == lit(ctype))
        owning_ids = [
            layer_uuid for (layer_uuid,) in owned["layer_uuid"].distinct().fetchall()
        ]
        if not owning_ids:
            return None

        layers = []
        for layer_uuid in owning_ids:
            uri = f"{layer_dir(layer_uuid)}dims/{subdir}/{ctype}.parquet"
            rel = try_read_parquet(uri, con)
            if rel is None:
                continue
            layers.append(rel.project(lit(layer_uuid).alias("layer_uuid"), star()))
        if not layers:
            return None

        # Semi-join to the owner map's winning rows; `order_key` already gives
        # the correct cross-layer, first-introduced order, so a consumer only
        # has to sort by it before `.df()` (a join doesn't preserve row order).
        union = union_all_by_name(layers, con)
        joined = union.set_alias("u").join(
            owned.set_alias("o"),
            # `match` columns are compared NULL-safely; `dims` broadcast.
            broadcast_match("u", "o", (*match, "layer_uuid"), dims),
        )
        # Project the file's attribute columns plus the map's key dims and
        # `order_key` explicitly: a bare star over the join would leak the
        # map's duplicate `name`/dim columns and the layer's `deleted` into
        # the frame. A NULL key dim in the file broadcasts (§5.5), so it takes
        # the value the row is owned for.
        skip = {"component_type", "layer_uuid", "deleted", *dims}
        return joined.project(
            *(col("u", c) for c in union.columns if c not in skip),
            *(
                (
                    coalesce(col("u", d), col("o", d))
                    if d in union.columns
                    else col("o", d)
                ).alias(d)
                for d in dims
            ),
            col("o", "order_key"),
        )
