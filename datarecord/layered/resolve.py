"""The owner map, and the resolved reads gated by it.

The map answers which layer owns each key; `NodeCache` exposes the reads over
it - one long relation per attribute, a type's member frame, this
layer's own outputs. Tool-agnostic: turning these into a framework's
object is `datarecord.tools`.

Notes
-----
- [the DuckDB read path](https://energy-models.github.io/datarecord/design/read-path/)
- [resolving a relation](https://energy-models.github.io/datarecord/design/read-path/#resolving-a-relation)
- [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property, partial
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import UUID

import duckdb
import narwhals as nw
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import (
    DuckTypes,
    base_uri_of,
    dims_dirs,
    ex_all,
    fn,
    fold_axis,
    layer_dir,
    resolved_dir,
    schema_uri,
    struct_of,
    try_read_parquet,
    union_all_by_name,
)
from datarecord.schema import Schema

# The owner map's `layer_uuid` column type - a layering mechanism, not
# something a schema declares.
LAYER_UUID_TYPE = "UUID"


def resolve_dims(schema: Schema, ancestry: list[UUID], con: DuckDBPyConnection) -> Dims:
    """Fold every dim `schema` declares to its axis relation.

    A dim's axis file is `{dim}.parquet`.

    Parameters
    ----------
    schema
        The record's schema, which declares the dims and their keys.
    ancestry : list of UUID
        Root first, ending in the record being resolved, truncated at the
        deepest materialised ancestor (`ancestry_to_read`).
    con : DuckDBPyConnection

    Returns
    -------
    Dims

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """
    # `ancestry` stopped either at a materialised ancestor or at the root, and
    # only the head can be the former - every entry below it is an
    # unmaterialised intermediate layer, or the record itself.
    from_cache = len(ancestry) > 1 and materialised(ancestry[0], con)
    dirs = dims_dirs(ancestry, from_cache=from_cache)
    axes = {}
    for dim in schema.dims:
        # Keyed by the axis key, not the dim alone: a nested dim's labels
        # identify a point only within its parents (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis), so `(period,
        # timestep)` is what last-writer-wins applies to.
        rel = fold_axis(dirs, f"{dim}.parquet", schema.axis_key(dim), con)
        if rel is not None:
            axes[dim] = rel
    return Dims(schema=schema, axes=axes)


def broadcast_match(
    alias_a: str, alias_b: str, fixed: tuple[str, ...], dims: tuple[str, ...]
) -> Expression:
    """NULL-safe equality on `fixed`, broadcast-OR on `dims`.

    A raw row's `dim = NULL` means "every value of `dim`", so it must
    match regardless of the resolved side's value there, unlike a plain
    `IS NOT DISTINCT FROM` which only matches NULL against NULL.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
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
    """A record's schema, plus each declared dim's folded axis relation.

    They travel together because the fold needs both wherever it broadcasts a
    NULL against "all values of that dim".

    Parameters
    ----------
    schema : Schema
        The record's schema.
    axes : dict of str to DuckDBPyRelation
        Each declared dim's folded axis relation, full row rather than the key
        column alone - `scenario`'s carries `weight` too. A dim with no rows
        anywhere is absent rather than present-and-empty.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]

    def entity_match(self, alias_a: str, alias_b: str, *fixed: str) -> Expression:
        """Match a raw entity or group row against an already-resolved key.

        No broadcast arm: an entity table's key columns address a row rather
        than expanding against an axis, so NULL-safe equality is the whole of
        it. `input_match` differs precisely there.
        """
        return broadcast_match(alias_a, alias_b, fixed, ())

    def input_match(
        self,
        alias_a: str,
        alias_b: str,
        *fixed: str,
        dims: tuple[str, ...] | None = None,
    ) -> Expression:
        """Match a raw `inputs/` row against an already-resolved key.

        `dims` narrows the broadcast arms to the coordinates the raw side
        actually carries, which one attribute's file is a subset of
        (`long_columns_for`). Defaults to every partial dim, for a caller
        matching against a relation carrying all of them.
        """
        return broadcast_match(
            alias_a,
            alias_b,
            fixed,
            self.schema.partial_dims if dims is None else dims,
        )

    def expand_dims(
        self, rel: DuckDBPyRelation, layer_keys: tuple[str, ...]
    ) -> tuple[DuckDBPyRelation, dict[str, Expression]]:
        """Left-join `rel` against each of `layer_keys`' axis, broadcasting NULLs.

        Parameters
        ----------
        rel : DuckDBPyRelation
        layer_keys : tuple of str
            `schema.partial_dims`. Never `entity` or a group's coordinate,
            which address a row rather than broadcasting over an axis.

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
        `write_record` enforces: a record whose frames do not is one the
        writer rejected, and binding here would resolve it as though the dim
        were broadcast everywhere.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
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


def _map_uri(revision_id: UUID, kind: str) -> str:
    """Where a record's `kind` owner map is materialised, under `resolved/`.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    return f"{resolved_dir(revision_id)}owner_map/{kind}.parquet"


def materialised(revision_id: UUID, con: DuckDBPyConnection) -> bool:
    """Whether `revision_id`'s node caches are materialised.

    A node's caches are written together (`materialise`), so one map answers
    for all three, and for the resolved dims and schema beside them.

    This is a filesystem question rather than recorded state: layers are
    write-once, so a materialised cache is valid forever and its
    presence is the whole answer.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    # `inputs` answers for the rest: the maps are written together, and it is
    # the one kind every schema has, whatever groups it declares.
    return try_read_parquet(_map_uri(revision_id, "inputs"), con) is not None


def ancestry_to_read(ancestry: list[UUID], con: DuckDBPyConnection) -> list[UUID]:
    """`ancestry` truncated at the deepest materialised node, root first.

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

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    for depth in range(len(ancestry) - 2, -1, -1):
        if materialised(ancestry[depth], con):
            return ancestry[depth:]
    return ancestry


def _table_name(revision_id: UUID, kind: str) -> str:
    return f"owner_map_{kind}_{revision_id.hex}"


# -- fold relations -----------------------------------------------------


def _deleted_relation(
    revision_id: UUID,
    keys: Dims,
    con: DuckDBPyConnection,
    *,
    uri: str,
    fixed: tuple[str, ...] = ("entity",),
) -> DuckDBPyRelation:
    """This layer's tombstones of one kind, keyed as the map they filter.

    A deletion removes the thing whole - across every attribute, and across
    every dim, since a row exists or it does not. So the tombstone is its key
    columns and nothing else: no axis to scope it along, and none to expand.

    Parameters
    ----------
    uri
        The layer file the tombstones are read from - the same one the map
        they filter is built from, since reading membership from one source
        and deletions from another would resolve a deletion the map never saw.
    fixed
        The key columns, compared NULL-safely: the entity for a component, a
        group's coordinates for one of its rows.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    rel = try_read_parquet(uri, con, union_by_name=True)
    if rel is None or "deleted" not in rel.columns:
        return _empty_relation(keys.schema, con, *fixed)
    return rel.filter(col("deleted")).project(*(col(c) for c in fixed)).distinct()


def _component_deleted(
    revision_id: UUID, keys: Dims, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """This layer's tombstoned components: one entity per row.

    Also what removes a group's rows, a component tombstone killing every row
    of a group `entity` keys - matched on the entity the two share.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    return _deleted_relation(
        revision_id, keys, con, uri=layer_dir(revision_id) + "dims/entity.parquet"
    )


def _group_deleted(
    group: str, revision_id: UUID, keys: Dims, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """This layer's tombstoned rows of one group, keyed by `group_key`.

    Not every coordinate: a tombstone names the tuple removed, not the `into`
    label it carried.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    return _deleted_relation(
        revision_id,
        keys,
        con,
        uri=f"{layer_dir(revision_id)}groups/{group}.parquet",
        fixed=keys.schema.group_key(group),
    )


def cast_declared(schema: Schema, rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """`rel`, with its columns of a declared type cast, others as-is.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    duck_types = DuckTypes(rel)
    cols = [
        col(c).cast(duck_types(t)).alias(c) if (t := schema.column_type(c)) else col(c)
        for c in rel.columns
    ]
    return rel.project(*cols)


def _empty_relation(
    schema: Schema, con: DuckDBPyConnection, *columns: str
) -> DuckDBPyRelation:
    """A zero-row relation with `columns`, cast to their declared type, `VARCHAR` if none.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    return DuckTypes(con).empty_relation(
        **{c: schema.column_type(c) or nw.String() for c in columns}
    )


def _null_safe(alias_a: str, alias_b: str, columns: tuple[str, ...]) -> str:
    return " AND ".join(
        f"{alias_a}.{c} IS NOT DISTINCT FROM {alias_b}.{c}" for c in columns
    )


def with_columns(
    schema: Schema, rel: DuckDBPyRelation, *columns: str
) -> DuckDBPyRelation:
    """`rel` with any of `columns` it lacks added as typed NULLs.

    One file carries one attribute's coordinates, so a column another attribute
    uses is simply absent - and a coordinate an attribute is not addressed by
    means the value applies across every value of it, which is what NULL means.
    Materialising it here lets every path downstream project the column
    unconditionally instead of branching on its presence.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    missing = [c for c in columns if c not in rel.columns]
    if not missing:
        return rel
    duck_types = DuckTypes(rel)
    added = [
        duck_types.null(schema.column_type(c) or nw.String()).alias(c) for c in missing
    ]
    return rel.project(star(), *added)


def fold_inputs(
    revision_id: UUID, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This node's inputs map: `inputs/` keys, folded over `parent`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    rel = try_read_parquet(
        layer_dir(revision_id) + "inputs/*.parquet", con, union_by_name=True
    )
    if rel is None:
        own = _empty_relation(keys.schema, con, *keys.schema.input_columns)
    else:
        # Every declared dim too, not just `breakpoint`: one file carries only
        # its own attribute's columns, so a coordinate another attribute uses
        # is absent here and must read as NULL rather than fail to bind.
        broadcast = keys.schema.broadcast_dims
        rel = cast_declared(
            keys.schema,
            with_columns(keys.schema, rel, "breakpoint", *keys.schema.dims),
        )
        rel, dims = keys.expand_dims(rel.set_alias("i"), keys.schema.partial_dims)
        # Each broadcast dim is carried twice: the (possibly broadcast) key
        # value, and `_raw_<dim>` as the row stored it. The flags describe the
        # stored form - whether a row set the dim or left it NULL - so they
        # cannot be read off the expanded value, which is never NULL once
        # broadcast (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        # `dims` covers the partial ones, expanded; the rest pass through as
        # stored. Together they are every declared dim, exactly once.
        expanded = set(dims)
        tagged = rel.project(
            *(col("i", d) for d in keys.schema.dims if d not in expanded),
            *(expr.alias(d) for d, expr in dims.items()),
            *(col("i", d).alias(f"_raw_{d}") for d in broadcast),
            col("i", "attribute"),
            lit(str(revision_id)).cast(LAYER_UUID_TYPE).alias("layer_uuid"),
            col("i", "breakpoint"),
        )
        own = tagged.aggregate(
            [
                *(col(c) for c in (*keys.schema.input_key, "layer_uuid")),
                # A field aggregating to NULL - what a map written before the
                # dim was declared yields - reads as "not set", the same as
                # false.
                struct_of(
                    {d: fn.bool_or(col(f"_raw_{d}").isnotnull()) for d in broadcast}
                ).alias("varies"),
                struct_of(
                    {d: fn.bool_or(col(f"_raw_{d}").isnull()) for d in broadcast}
                ).alias("broadcast"),
                fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
            ]
        )

    # Deleting a component drops its attribute rows; deleting one row of a
    # group drops only that row's, which the map can scope because a group's
    # coordinates are in `input_key` (https://energy-models.github.io/datarecord/design/record/#connections).
    #
    # Each anti-join is skipped where its key is not in the inputs key at all:
    # a schema declaring no dims is "no manifest yet", so there is no entity
    # column to match a tombstone against, and no rows either.
    tombstones: list[tuple[Callable[..., DuckDBPyRelation], tuple[str, ...]]] = [
        (_component_deleted, ("entity",))
    ]
    tombstones += [
        (partial(_group_deleted, group), keys.schema.group_key(group))
        for group in keys.schema.groups
    ]
    kept = parent.set_alias("p")
    keyed = set(keys.schema.input_key)
    for deleted, key in tombstones:
        if key and keyed.issuperset(key):
            kept = kept.join(
                deleted(revision_id, keys, con).set_alias("x"),
                _null_safe("x", "p", key),
                how="anti",
            ).set_alias("p")
    kept = kept.join(
        own.set_alias("o"), _null_safe("p", "o", keys.schema.input_key), how="anti"
    )
    return union_all_by_name([kept, own], con)


def fold_components(
    revision_id: UUID, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This node's components map: `dims/components/` keys, folded over `parent`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    return _fold_ordered(
        revision_id,
        keys,
        con,
        parent,
        uri=layer_dir(revision_id) + "dims/entity.parquet",
        key=("entity",),
        columns=keys.schema.component_columns,
    )


def fold_group(
    group: str,
    revision_id: UUID,
    keys: Dims,
    con: DuckDBPyConnection,
    parent: DuckDBPyRelation,
) -> DuckDBPyRelation:
    """One group's map: `groups/<group>.parquet` keys, folded over `parent`.

    `fold_components` keyed by the group's key coordinates rather than the
    entity alone, and with one more tombstone where the group is keyed by
    `entity`: a component tombstone removes every row of it, so `parent` is
    anti-joined against that as well as against this layer's own group
    tombstones.

    A group not keyed by `entity` has no such relation - `corridor` over
    `(from, to)` draws both from the entity axis but under names of its own,
    and which of them a tombstone should match is not this fold's to guess.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    key = keys.schema.group_key(group)
    over_entity = "entity" in key
    return _fold_ordered(
        revision_id,
        keys,
        con,
        parent,
        uri=f"{layer_dir(revision_id)}groups/{group}.parquet",
        key=key,
        columns=keys.schema.group_columns(group),
        also_deleted=_component_deleted if over_entity else None,
        also_deleted_key=("entity",) if over_entity else (),
    )


def _fold_ordered(
    revision_id: UUID,
    keys: Dims,
    con: DuckDBPyConnection,
    parent: DuckDBPyRelation,
    *,
    uri: str,
    key: tuple[str, ...],
    columns: tuple[str, ...],
    also_deleted: Callable[[UUID, Dims, DuckDBPyConnection], DuckDBPyRelation]
    | None = None,
    also_deleted_key: tuple[str, ...] = (),
) -> DuckDBPyRelation:
    """The shared fold for the maps that carry an `order_key`.

    `components` and a group's differ only in which file they read, which
    columns key them, whether they carry a type, and whether a second tombstone
    kind applies - so the `order_key` assignment, which is the subtle part,
    lives here once rather than in each.

    Parameters
    ----------
    key
        The columns one row is keyed by, compared NULL-safely and never
        expanded against an axis: the entity for a component, the group's
        coordinates for one of its rows.
    also_deleted, also_deleted_key
        A second tombstone relation to anti-join `parent` against, and the key
        to match it on. Connections use it for component tombstones.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    # Only the components map carries the type; a group's rows are keyed by its
    # coordinates and `entity_type` is not one of them (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).
    typed = "entity_type" in columns
    rel = try_read_parquet(uri, con, union_by_name=True)
    if rel is None:
        # No rows, so `order_key` needs no values either - just the column.
        own = _empty_relation(keys.schema, con, *columns)
    else:
        not_deleted = ~col("deleted") if "deleted" in rel.columns else lit(True)
        rel = cast_declared(keys.schema, rel)
        tagged = (
            rel.filter(not_deleted)
            .set_alias("i")
            .project(
                *([col("i", "entity_type")] if typed else []),
                *(col("i", c) for c in key),
                lit(str(revision_id)).cast(LAYER_UUID_TYPE).alias("layer_uuid"),
                sql("row_number() OVER ()").alias("_row"),
            )
        )
        # `entity_type` is aggregated, not grouped by: it is determined by
        # `name` (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types), and grouping on it would keep one name under two types
        # as two rows.
        own = tagged.aggregate(
            [
                *(col(c) for c in (*key, "layer_uuid")),
                *(
                    [fn.any_value(col("entity_type")).alias("entity_type")]
                    if typed
                    else []
                ),
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
            also_deleted(revision_id, keys, con).set_alias("a"),
            _null_safe("p", "a", also_deleted_key),
            how="anti",
        )
    deleted = _deleted_relation(revision_id, keys, con, uri=uri, fixed=key)
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
    """A record's owner map of one kind, folded down over `ancestry`.

    `schema` is passed in rather than read here: one manifest serves the whole
    record, so the three kinds fold under one read of it.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    keys = resolve_dims(schema, ancestry, con)

    rel = _empty_relation(keys.schema, con, *columns(keys))
    root = try_read_parquet(map_uri(ancestry[0]), con) if len(ancestry) > 1 else None
    start = 0
    if root is not None:
        rel, start = cast_declared(keys.schema, root), 1

    for uid in ancestry[start:]:
        rel = fold(uid, keys, con, rel)
    return rel


def map_kinds(
    schema: Schema,
) -> dict[str, tuple[Callable[[Dims], tuple[str, ...]], Callable]]:
    """This schema's owner maps, each a `kind` -> (column set, fold) pair.

    Two fixed - `inputs` for attribute values, `components` for the entity
    axis - and one per declared group. A group is a table of which tuples
    exist, so it is folded like any other: keys, tombstones and an
    `order_key`, differing only in which columns key it.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    kinds: dict[str, tuple[Callable[[Dims], tuple[str, ...]], Callable]] = {
        "inputs": (lambda keys: keys.schema.input_columns, fold_inputs),
        "components": (lambda keys: keys.schema.component_columns, fold_components),
    }
    for group in schema.groups:
        kinds[group] = (
            partial(_group_columns, group),
            partial(fold_group, group),
        )
    return kinds


def _group_columns(group: str, keys: Dims) -> tuple[str, ...]:
    return keys.schema.group_columns(group)


def _fold_kind(
    kind: str, ancestry: list[UUID], con: DuckDBPyConnection, schema: Schema
) -> DuckDBPyRelation:
    columns, fold = map_kinds(schema)[kind]
    return _fold_map(
        ancestry,
        con,
        schema,
        columns=columns,
        map_uri=lambda uid: _map_uri(uid, kind),
        fold=fold,
    )


def _table(
    revision_id: UUID,
    ancestry: list[UUID],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    kind: str,
) -> DuckDBPyRelation:
    """One owner map for `revision_id`.

    Its own materialised map if it has one, else the fold over `ancestry`
    (already truncated at the deepest materialised ancestor). The live fold
    is cached as a connection-scoped table, which never needs invalidating since
    layers are write-once.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    persisted = try_read_parquet(_map_uri(revision_id, kind), con)
    if persisted is not None:
        return cast_declared(schema, persisted)

    name = _table_name(revision_id, kind)
    try:
        return con.table(name)
    except duckdb.CatalogException:
        _fold_kind(kind, ancestry, con, schema).create(name)
        return con.table(name)


def materialise(
    revision_id: UUID, ancestry: list[UUID], con: DuckDBPyConnection
) -> None:
    """Write `revision_id`'s node caches: owner maps and resolved dims.

    Purely additive. It changes no answer a read would give, only how many
    layers a read touches to reach it: once these files exist, a descendant's
    fold stops here rather than walking further up (`ancestry_to_read`).

    Safe to call more than once, and safe to call on any node - layers are
    write-once, so what is folded here cannot later become stale.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    schema = read_schema(con)
    base = resolved_dir(revision_id) + "owner_map/"
    if "://" not in base:
        # A record that wrote nothing to its layer has no node dir yet either.
        Path(base).mkdir(parents=True, exist_ok=True)
    for kind in map_kinds(schema):
        _fold_kind(kind, ancestry, con, schema).to_parquet(_map_uri(revision_id, kind))
    _materialise_dims(revision_id, ancestry, con, schema)


def _materialise_dims(
    revision_id: UUID, ancestry: list[UUID], con: DuckDBPyConnection, schema: Schema
) -> None:
    """Fold this node's resolved axes into its node cache, not its layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    dims = resolve_dims(schema, ancestry, con)
    base = resolved_dir(revision_id) + "dims/"
    if "://" not in base:
        Path(base).mkdir(parents=True, exist_ok=True)
    for dim, rel in dims.axes.items():
        rel.to_parquet(f"{base}{dim}.parquet")


# -- the schema (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ---------------------------------------------


def read_json(uri: str) -> dict[str, Any] | None:
    """Read one JSON file, or `None` if it doesn't exist (e.g. an undeclared schema).

    Only a genuine miss (local `FileNotFoundError`, remote 404/403) maps to
    `None` - any other failure raises rather than silently reading as absent.
    """
    try:
        if "://" in uri:
            with urlopen(uri) as fh:  # noqa: S310 - record URIs are derived, not user input
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
    """The record's one schema, read from beside the layers.

    No fold and no ancestry: a schema is not layered data. One file makes it a
    property of the record, knowable before any layer is read and stated once
    for a hundred-layer tree. A record that declares none reads as an empty
    `Schema`, which declares no dims and no attributes.

    Parameters
    ----------
    con
        Read the manifest beside *this* connection's layers. A connection is
        already scoped to one record root (`connect(base_uri=...)`), so the
        schema follows from it rather than from a separate parameter. `None`
        reads the process default, which is what a caller holding no
        connection gets.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    base = None if con is None else base_uri_of(con)
    raw = read_json(schema_uri(base))
    return Schema() if raw is None else Schema.model_validate(raw)


def write_schema(schema: Schema, base_uri: str | None = None) -> None:
    """Write the record's one schema, beside the layers.

    Amending it is a schema change rather than a patch, so this replaces the
    file: `Schema.compatible_with` is what says whether existing layers
    survive the amendment.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
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

    The cached artifacts and the reads gated by them
    (`relation`/`outputs`/`component_frame`/`group_frame`/`attributes_of`) live
    together because every one of the latter is a semi-join against the former.
    Tool-agnostic throughout: the long relations here are what a tool
    (`datarecord.tools`) builds its own object from.

    Notes
    -----
    `ancestry` is root first, ending in `revision_id`, and already truncated at
    the deepest materialised ancestor (`ancestry_to_read`) - so a hundred-layer
    tree with a materialised parent resolves from two entries.

    Everything here is a `cached_property` or reads a connection-scoped table:
    layers are write-once, so nothing an instance caches can go stale.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/read-path/#resolving-a-relation)
    """

    revision_id: UUID
    ancestry: list[UUID]
    con: DuckDBPyConnection

    def _map(self, kind: str) -> DuckDBPyRelation:
        return _table(self.revision_id, self.ancestry, self.con, self.schema, kind=kind)

    @property
    def inputs(self) -> DuckDBPyRelation:
        return self._map("inputs")

    @property
    def components(self) -> DuckDBPyRelation:
        return self._map("components")

    def group(self, name: str) -> DuckDBPyRelation:
        """One declared group's owner map.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return self._map(name)

    @cached_property
    def dims(self) -> Dims:
        return resolve_dims(self.schema, self.ancestry, self.con)

    @cached_property
    def schema(self) -> Schema:
        """This record's one manifest, read once per node.

        From beside *this* connection's layers, so two records on different
        roots in one process each read their own.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        """
        return read_schema(self.con)

    def entity_types(self) -> set[str]:
        """Types with any live component row, straight from the owner map.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        rows = self.components.project("entity_type").distinct().fetchall()
        return {r[0] for r in rows}

    def attribute_names(self) -> list[str]:
        """Every input attribute any layer owns a row for, from the owner map.

        Across component types, matching the file layout: one
        `inputs/<attr>.parquet` holds every type's rows. Ordered, so a
        `Record` over this has a stable key order.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
        """
        rows = self.inputs.project("attribute").distinct().order("attribute").fetchall()
        return [r[0] for r in rows]

    def output_names(self) -> list[str]:
        """Every result attribute this record's own layer holds.

        Its own layer only: outputs do not overlay, so there is no map to
        consult and nothing inherited from an ancestor.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        uri = f"{layer_dir(self.revision_id)}outputs/*.parquet"
        rel = try_read_parquet(uri, self.con, union_by_name=True)
        if rel is None:
            return []
        rows = rel.project("attribute").distinct().order("attribute").fetchall()
        return [r[0] for r in rows]

    def attributes_of(
        self, ctype: str
    ) -> dict[str, tuple[frozenset[str], frozenset[str], bool]]:
        """Per attribute of `ctype`, which dims its rows use.

        The map computes the flags per key, so per component; this unions them
        over the names of one type, which is the granularity a consumer assigns
        containers at. A type whose components disagree yields a dim in both
        sets - the instruction to use both containers, each taking the rows it
        matches. The union stops at the type boundary: across types it
        would describe neither.

        Returns
        -------
        dict
            `attribute -> (varies, broadcast, breakpoints)`, the raw material
            `Record.flags` turns into `Flags`.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        """
        # The flags have a field per *broadcast* dim: an address coordinate
        # never broadcasts, so "did a row set it" is not a question about it.
        dims = self.schema.broadcast_dims
        # Scoped by a semi-join to the components map, the entity table saying
        # what type a name is (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
        of_type = self.components.filter(col("entity_type") == lit(ctype)).project(
            "entity"
        )
        rows = (
            self.inputs.set_alias("i")
            .join(of_type.distinct().set_alias("e"), "i.entity = e.entity", how="semi")
            .aggregate(
                [
                    col("attribute"),
                    struct_of({d: fn.bool_or(col("varies", d)) for d in dims}).alias(
                        "varies"
                    ),
                    struct_of({d: fn.bool_or(col("broadcast", d)) for d in dims}).alias(
                        "broadcast"
                    ),
                    fn.bool_or(col("breakpoints")).alias("breakpoints"),
                ]
            )
            .fetchall()
        )

        # The structs come back as dicts keyed by dim, so the two sets are a
        # filter rather than a positional slice. A field aggregating to NULL -
        # a dim declared after this map was written - is falsy, so absent.
        #
        # Both sets are scoped to the attribute's own coordinates. The map is
        # one relation over every attribute, so a dim one attribute is
        # addressed by reads NULL for the rows of one that is not - and an
        # unscoped aggregate would report that NULL as "every row broadcasts
        # over it" when the truth is "this attribute has no such axis".
        #
        # The distinction is what a consumer plans reads against: `varies |
        # broadcast` is the test for whether an attribute touches a dim at all
        # (https://energy-models.github.io/datarecord/design/record/#flags), and a dim in neither means there is nothing to
        # build a container along. Reporting an unaddressed dim as broadcast
        # would answer that question wrongly for every attribute in the record.
        def scope(attribute: str) -> tuple[str, ...]:
            own = set(self.schema.coordinates_of(attribute))
            return tuple(d for d in dims if d in own) if own else dims

        return {
            attribute: (
                frozenset(d for d in scope(attribute) if varies.get(d)),
                frozenset(d for d in scope(attribute) if broadcast.get(d)),
                bool(breakpoints),
            )
            for attribute, varies, broadcast, breakpoints in rows
        }

    def relation(self, attribute: str) -> DuckDBPyRelation:
        """The resolved long relation for one input attribute.

        Semi-joins the owning layers' `inputs/<attribute>.parquet` to the
        `inputs` owner map, so only owned rows survive: the map already names
        the winning layer per key, so there is no per-read `MAX`/group-by and
        no tombstone filter (deletions are already absent from the map).

        A stored NULL for an `input_key` dim means "all values" and may be
        owned for only some of them, so each key dim's join arm is
        NULL-aware and the row takes the value it is owned for.

        Returns
        -------
        DuckDBPyRelation
            Unmaterialised, in the long schema (`schema.long_columns`).
            Empty when no layer wrote the attribute - the consumer then
            applies the catalog `default`.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [resolving a relation](https://energy-models.github.io/datarecord/design/read-path/#resolving-a-relation)
        """
        con = self.con
        om = self.inputs.filter(col("attribute") == lit(attribute))
        keys = self.dims
        partial_dims = keys.schema.partial_dims
        # This attribute's own columns, not every declared dim: one file per
        # attribute is one column set per attribute (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
        columns = keys.schema.long_columns_for(attribute)
        # The join's arms are the map's key columns this attribute's file also
        # carries. A dim outside `partial` is in neither: it is not in the map
        # at all, so it constrains nothing and passes straight through.
        #
        # Of those that are, an address coordinate matches NULL-safely and a
        # broadcast dim NULL-aware - the split `broadcast_dims` draws
        # (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        coordinates = set(keys.schema.coordinates_of(attribute))
        broadcasts = set(keys.schema.broadcast_dims)
        address = tuple(
            d for d in partial_dims if d in coordinates and d not in broadcasts
        )
        broadcast_over = tuple(
            d for d in partial_dims if d in coordinates and d in broadcasts
        )
        layers = [
            with_columns(
                keys.schema,
                con.read_parquet(f"{layer_dir(layer_uuid)}inputs/{attribute}.parquet"),
                *columns,
            ).project(lit(layer_uuid).alias("layer_uuid"), col("*"))
            for (layer_uuid,) in om["layer_uuid"].distinct().fetchall()
        ]
        if not layers:
            return _empty_relation(keys.schema, con, *columns)

        return (
            union_all_by_name(layers, con)
            .set_alias("l")
            .join(
                om.set_alias("o"),
                keys.input_match("l", "o", *address, "layer_uuid", dims=broadcast_over),
            )
            .project(
                *(
                    coalesce(col("l", dim), col("o", dim)).alias(dim)
                    if dim in partial_dims
                    else col("l", dim)
                    for dim in columns
                )
            )
        )

    def outputs(self, attribute: str) -> DuckDBPyRelation:
        """A result attribute from this record's own layer; outputs do not overlay.

        No fold and no owner map: if this layer has no `outputs/`, the record
        has no results - an ancestor's are not inherited.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        uri = f"{layer_dir(self.revision_id)}outputs/{attribute}.parquet"
        rel = try_read_parquet(uri, self.con)
        if rel is not None:
            return rel
        return _empty_relation(self.schema, self.con, *self.schema.long_columns)

    def component_frame(self, ctype: str) -> DuckDBPyRelation | None:
        """Wide static members of one type, resolved from the owner map.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        return self._dim_frame(
            ctype,
            subdir="components",
            owner_map=self.components,
            match=("entity",),
        )

    def group_frame(self, group: str) -> DuckDBPyRelation | None:
        """One group's rows, resolved from that group's owner map.

        Not per type, which is no coordinate of a group.

        Carries every non-key column of the row - an attribute over the group,
        an `into` label. The fold does not track those, so they come straight
        from the owning layer's file.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return self._owned_frame(
            uri=lambda layer_uuid: f"{layer_dir(layer_uuid)}groups/{group}.parquet",
            owned=self.group(group),
            match=self.schema.group_key(group),
        )

    def _dim_frame(
        self,
        ctype: str,
        *,
        subdir: str,
        owner_map: DuckDBPyRelation,
        match: tuple[str, ...],
    ) -> DuckDBPyRelation | None:
        """One type's rows from a `dims/` subdirectory, gated by its owner map.

        The per-type read, which is `components` alone: a group is one file
        keyed by its coordinates and goes through `_owned_frame` directly.
        """
        return self._owned_frame(
            uri=lambda uid: f"{layer_dir(uid)}dims/{subdir}/{ctype}.parquet",
            owned=owner_map.filter(col("entity_type") == lit(ctype)),
            match=match,
        )

    def _owned_frame(
        self,
        *,
        uri: Callable[[UUID], str],
        owned: DuckDBPyRelation,
        match: tuple[str, ...],
    ) -> DuckDBPyRelation | None:
        """The owning layers' rows for one already-scoped slice of an owner map.

        Shared by `component_frame` and `group_frame`: both semi-join the owning
        layers' files to a map keyed the same way, differing only in which file
        each layer contributes and which columns match.
        """
        con = self.con
        owning_ids: list[UUID] = [
            layer_uuid for (layer_uuid,) in owned["layer_uuid"].distinct().fetchall()
        ]
        if not owning_ids:
            return None

        layers: list[DuckDBPyRelation] = []
        for layer_uuid in owning_ids:
            rel = try_read_parquet(uri(layer_uuid), con)
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
            # Key columns only, compared NULL-safely: a row exists or it does
            # not, so there is no axis to broadcast one over.
            broadcast_match("u", "o", (*match, "layer_uuid"), ()),
        )
        # Project the file's attribute columns and `order_key` explicitly: a
        # bare star over the join would leak the map's duplicate key columns
        # and the layer's `deleted` into the frame.
        skip = {"entity_type", "layer_uuid", "deleted"}
        return joined.project(
            *(col("u", c) for c in union.columns if c not in skip),
            col("o", "order_key"),
        )
