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

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any
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
    broadcast_match,
    distinct_values,
    ensure_local_dir,
    fn,
    fold_axis,
    null_safe,
    read_json,
    resolved_dir,
    schema_uri,
    struct_of,
    try_read_parquet,
    union_all_by_name,
)
from datarecord.layered.sources import LayerSource, ParquetLayer, ResolvedLayer
from datarecord.record import Flags
from datarecord.schema import Schema

# The owner map's `layer_uuid` column type - a layering mechanism, not
# something a schema declares.
LAYER_UUID_TYPE = "UUID"


def flags_from_rows(
    schema: Schema,
    dims: tuple[str, ...],
    rows: Iterable[tuple[str, Mapping[str, Any], Mapping[str, Any], Any]],
) -> dict[str, Flags]:
    """Fold `(attribute, varies, broadcast, breakpoints)` rows into `Flags`.

    Separate from the aggregate that produces the rows because the scoping below
    is about the *schema*, not about the map: it is the same cut whatever
    ownership decided, and keeping it out of the SQL is what makes it readable.

    Both sets are cut to the attribute's own coordinates. The relation aggregated
    over covers every attribute, so a dim one attribute is addressed by reads
    NULL for the rows of one that is not, and reporting that NULL as "every row
    broadcasts over it" would tell a consumer to build a container along an axis
    the attribute has no values on. An attribute the schema does not declare
    keeps every dim, its shape not being the schema's to say.

    Parameters
    ----------
    dims
        The broadcast dims the two mappings have an entry per; a dim missing
        from one - declared after a persisted map was written - reads as unset.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """

    def scope(attribute: str) -> tuple[str, ...]:
        own = set(schema.coordinates_of(attribute))
        return tuple(d for d in dims if d in own) if own else dims

    return {
        attribute: Flags(
            frozenset(d for d in scope(attribute) if varies.get(d)),
            frozenset(d for d in scope(attribute) if broadcast.get(d)),
            bool(breakpoints),
        )
        for attribute, varies, broadcast, breakpoints in rows
    }


def resolve_dims(
    schema: Schema, sources: Sequence[LayerSource], con: DuckDBPyConnection
) -> Dims:
    """Fold every dim `schema` declares to its axis relation.

    Parameters
    ----------
    schema
        The record's schema, which declares the dims and their keys.
    sources : sequence of LayerSource
        Root first, ending in the layer being resolved.
    con : DuckDBPyConnection

    Returns
    -------
    Dims

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """
    # One listing per source rather than one probe per declared dim per source
    # (D x A misses, most declared dims absent from most layers): `present` says
    # which dims that source actually has, so `fold_axis` below is only ever
    # asked for a dim at least one source holds, and only passed the sources
    # that hold it.
    present = [source.axes() for source in sources]
    axes = {}
    for dim in schema.dims:
        holding = [s for s, names in zip(sources, present) if dim in names]
        if not holding:
            continue
        # Keyed by the axis key, not the dim alone: a nested dim's labels
        # identify a point only within its parents (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis), so `(period,
        # timestep)` is what last-writer-wins applies to.
        rel = fold_axis([s.axis(dim) for s in holding], schema.axis_key(dim), con)
        if rel is not None:
            axes[dim] = rel
    return Dims(schema=schema, axes=axes, groups=resolve_groups(schema, sources, con))


def resolve_groups(
    schema: Schema, sources: Sequence[LayerSource], con: DuckDBPyConnection
) -> dict[str, DuckDBPyRelation]:
    """Fold every declared group to its resolved relation, keyed by `group_key`.

    A group folds through `fold_axis` like any axis with a composite key: last-
    writer-wins per `group_key`, static columns (`into`, group-attributes) on the
    winning row, its own tombstones honoured, member order the file's row order.
    Absent from the result where no layer wrote a row.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    groups = {}
    for group in schema.groups:
        key = schema.group_key(group)
        rows = [source.group(group) for source in sources]
        if not key or all(rel is None for rel in rows):
            continue
        rel = fold_axis(rows, key, con)
        if rel is not None:
            groups[group] = rel
    return groups


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
    groups : dict of str to DuckDBPyRelation
        Each declared group's folded relation, keyed by `group_key`, carrying its
        static columns, in member order - a group folds like an axis with a
        composite key, no owner map. Absent where no layer wrote a row.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]
    groups: dict[str, DuckDBPyRelation] = field(default_factory=dict)

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
    return ResolvedLayer(revision_id).map_uri(kind)


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


def sources_to_read(ancestry: list[UUID], con: DuckDBPyConnection) -> list[LayerSource]:
    """`ancestry` as the sources to fold, truncated at the deepest materialised node.

    A materialised owner map is already folded over everything above it, so
    nothing further up need be read: the truncation is *fewer sources* rather
    than a shorter list of UUIDs, and the node stopped at becomes a
    `ResolvedLayer` - standing for everything above it where every other entry
    stands for its own layer alone.

    Only proper ancestors count: the node being resolved is always read from its
    own layer, since stopping *at* it would return its cached answer instead of
    resolving it.

    Parameters
    ----------
    ancestry
        Root-first, ending in the node being resolved (`records.ancestry`).
    con : DuckDBPyConnection

    Returns
    -------
    list of LayerSource
        Root first, ending in the node's own layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    for depth in range(len(ancestry) - 2, -1, -1):
        if materialised(ancestry[depth], con):
            return [
                ResolvedLayer(ancestry[depth], con),
                *(ParquetLayer(uid, con) for uid in ancestry[depth + 1 :]),
            ]
    return [ParquetLayer(uid, con) for uid in ancestry]


def _table_name(revision_id: UUID, kind: str) -> str:
    return f"owner_map_{kind}_{revision_id.hex}"


# -- fold relations -----------------------------------------------------


def _deleted_relation(
    rel: DuckDBPyRelation | None,
    keys: Dims,
    con: DuckDBPyConnection,
    *,
    fixed: tuple[str, ...],
) -> DuckDBPyRelation:
    """One layer's tombstones of one membership, keyed as the map they filter.

    A deletion removes the thing whole - across every attribute and every dim,
    since a row exists or it does not. So the tombstone is its key columns and
    nothing else: no axis to scope it along, none to expand.

    Read from the same source relation the membership itself folds from
    (`source.axis(dim)`, `source.group(g)`), since reading membership from one
    file and deletions from another would resolve a deletion the map never saw.

    Parameters
    ----------
    rel
        The layer's rows of that membership, or `None` where it has none.
    fixed
        The key columns, compared NULL-safely: `entity` for a component, a
        group's coordinates for one of its tuples, a dim's `axis_key` for a
        coordinate.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    if rel is None or "deleted" not in rel.columns:
        return _empty_relation(keys.schema, con, *fixed)
    return rel.filter(col("deleted")).project(*(col(c) for c in fixed)).distinct()


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
    source: LayerSource, keys: Dims, con: DuckDBPyConnection, parent: DuckDBPyRelation
) -> DuckDBPyRelation:
    """This layer's inputs map: its `inputs/` keys, folded over `parent`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    rel = source.all_attributes("inputs")
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
            lit(str(source.layer_id)).cast(LAYER_UUID_TYPE).alias("layer_uuid"),
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

    # Each membership this layer tombstones anti-joins `parent`, keyed as it is
    # in `input_key` (https://energy-models.github.io/datarecord/design/read-path/#owner-map).
    schema = keys.schema
    keyed = set(schema.input_key)
    # `optional` where an absent key is legitimate: entity and groups drop out of
    # `input_key` when a schema declares no dims. A partial dim's `axis_key` is
    # always present, so a miss there is a nested dim whose parents the schema
    # failed to keep in the fold key, and the assert names it.
    memberships = [
        (source.axis("entity"), ("entity",), True),
        *((source.group(g), schema.group_key(g), True) for g in schema.groups),
        *((source.axis(d), schema.axis_key(d), False) for d in schema.partial or ()),
    ]
    kept = parent.set_alias("p")
    for rel_deleted, key, optional in memberships:
        if not key or (optional and not keyed.issuperset(key)):
            continue
        assert keyed.issuperset(key), (
            f"partial dim keyed by {key} outruns `input_key` {schema.input_key}; "
            "a nested dim needs its parents in the fold key"
        )
        kept = kept.join(
            _deleted_relation(rel_deleted, keys, con, fixed=key).set_alias("x"),
            null_safe("x", "p", key),
            how="anti",
        ).set_alias("p")
    # Parent minus what this layer restates, union this layer's own.
    kept = kept.join(
        own.set_alias("o"), null_safe("p", "o", keys.schema.input_key), how="anti"
    )
    return union_all_by_name([kept, own], con)


def _fold_map(
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    kind: str,
    columns: Callable[[Dims], tuple[str, ...]],
    fold: Callable[
        [LayerSource, Dims, DuckDBPyConnection, DuckDBPyRelation], DuckDBPyRelation
    ],
) -> DuckDBPyRelation:
    """A record's owner map of one kind, folded down over `sources`.

    Only `inputs` remains a kind; the axes fold to a resolved copy instead
    (`resolve_dims`). A leading `ResolvedLayer` is the fold's *seed* rather than a
    step of it: its materialised map is already folded over everything above it,
    so it is read as the starting relation and never folded again.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    keys = resolve_dims(schema, sources, con)

    rel = _empty_relation(keys.schema, con, *columns(keys))
    head, *rest = sources
    if isinstance(head, ResolvedLayer):
        seed = try_read_parquet(head.map_uri(kind), con)
        if seed is not None:
            rel = cast_declared(keys.schema, seed)
    else:
        rest = list(sources)

    for source in rest:
        rel = fold(source, keys, con, rel)
    return rel


def map_kinds(
    schema: Schema,
) -> dict[str, tuple[Callable[[Dims], tuple[str, ...]], Callable]]:
    """This schema's owner maps, each a `kind` -> (column set, fold) pair.

    One kind: `inputs`, the only relation with a genuine key/row split - an
    attribute's ownership spans every `inputs/<attr>.parquet`. Every axis is a
    single keyed file resolved inline (`resolve_dims`), so none needs a map.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    return {"inputs": (lambda keys: keys.schema.input_columns, fold_inputs)}


def _fold_kind(
    kind: str,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
) -> DuckDBPyRelation:
    columns, fold = map_kinds(schema)[kind]
    return _fold_map(sources, con, schema, kind=kind, columns=columns, fold=fold)


def frozen_prefix(sources: Sequence[LayerSource]) -> int:
    """How many leading sources cannot change under a reader.

    The one rule the source list has to carry: **the fold is materialised up to
    the last frozen source; everything after it stays a relation.** Everything a
    `NodeCache` caches rests on layers being write-once, which a staging area is
    not - so a staged source ends the prefix, and a frozen one under it stays
    outside without either needing to know about the other.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    """
    prefix = 0
    for source in sources:
        if not source.frozen:
            break
        prefix += 1
    return prefix


def _table(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    kind: str,
) -> DuckDBPyRelation:
    """One owner map for `revision_id`, materialised as far as `frozen` allows.

    The frozen prefix is folded once and kept as a connection-scoped table,
    which never needs invalidating since layers are write-once. Whatever follows
    it is folded on top per call and handed back as a relation, so a staging
    area's edits are picked up with no bookkeeping at all: a relation over a
    staging table reads whatever the table holds when it is collected.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    prefix = frozen_prefix(sources)
    rel = _frozen_table(revision_id, sources[:prefix], con, schema, kind=kind)
    if prefix == len(sources):
        return rel

    keys = resolve_dims(schema, sources, con)
    _, fold = map_kinds(schema)[kind]
    for source in sources[prefix:]:
        rel = fold(source, keys, con, rel)
    return rel


def _frozen_table(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    kind: str,
) -> DuckDBPyRelation:
    """The frozen prefix's owner map, its own materialised one or a cached fold.

    Keyed by `revision_id` rather than by the prefix: the prefix *is* what that
    node resolves from, a staged source only ever being appended after it.
    """
    persisted = try_read_parquet(_map_uri(revision_id, kind), con)
    if persisted is not None:
        return cast_declared(schema, persisted)

    if not sources:
        # Nothing frozen to fold, which is a `WorkingRecord` over a base that is
        # itself unfrozen; the tail folds onto an empty map.
        columns, _ = map_kinds(schema)[kind]
        return _empty_relation(schema, con, *columns(Dims(schema=schema, axes={})))

    name = _table_name(revision_id, kind)
    try:
        return con.table(name)
    except duckdb.CatalogException:
        _fold_kind(kind, sources, con, schema).create(name)
        return con.table(name)


def materialise(
    revision_id: UUID, sources: Sequence[LayerSource], con: DuckDBPyConnection
) -> None:
    """Write `revision_id`'s node caches: owner maps and resolved dims.

    Purely additive. It changes no answer a read would give, only how many
    layers a read touches to reach it: once these files exist, a descendant's
    fold stops here rather than walking further up (`sources_to_read`).

    Safe to call more than once, and safe to call on any node - layers are
    write-once, so what is folded here cannot later become stale.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    schema = read_schema(con)
    base = resolved_dir(revision_id) + "owner_map/"
    ensure_local_dir(base)
    for kind in map_kinds(schema):
        _fold_kind(kind, sources, con, schema).to_parquet(_map_uri(revision_id, kind))
    _materialise_dims(revision_id, sources, con, schema)


def _materialise_dims(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
) -> None:
    """Fold this node's resolved axes into its node cache, not its layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    dims = resolve_dims(schema, sources, con)
    base = resolved_dir(revision_id) + "dims/"
    ensure_local_dir(base)
    for dim, rel in dims.axes.items():
        rel.to_parquet(f"{base}{dim}.parquet")
    groups = resolved_dir(revision_id) + "groups/"
    ensure_local_dir(groups)
    for group, rel in dims.groups.items():
        rel.to_parquet(f"{groups}{group}.parquet")
    # The per-type wide static frames, folded across sources. A component's wide
    # columns live in a per-type file, not on the axis, so they are the one
    # membership value that needs materialising beside the axis for a descendant
    # to reach through a closed node (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
    types = resolved_dir(revision_id) + "dims/entity_type/"
    ensure_local_dir(types)
    axis = dims.axes.get("entity")
    live = (
        set()
        if axis is None
        else set(distinct_values(axis, "entity_type", order=False))
    )
    for ctype in live:
        wide = fold_axis(
            [source.entity_type(ctype) for source in sources], ("entity",), con
        )
        if wide is not None:
            wide.to_parquet(f"{types}{ctype}.parquet")


# -- the schema (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ---------------------------------------------


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
    ensure_local_dir(uri, parent=True)
    with open(uri, "w") as fh:
        fh.write(schema.model_dump_json())


# -- public API -------------------------------------------------------------


@dataclass(frozen=True)
class NodeCache:
    """A record's resolved view: owner map, dims, schema, and the relations over them.

    The cached artifacts and the reads gated by them
    (`relation`/`outputs`/`entity_type_frame`/`group_frame`/`attributes_of`) live
    together because every one of the latter is a semi-join against the former.
    Tool-agnostic throughout: the long relations here are what a tool
    (`datarecord.tools`) builds its own object from.

    Notes
    -----
    `sources` is root first, ending in the layer being resolved, and already
    truncated at the deepest materialised ancestor (`sources_to_read`) - so a
    hundred-layer tree with a materialised parent resolves from two entries. A
    `WorkingRecord`'s staged rows are one more entry on the end, which is the
    whole of what makes staging a layer.

    Nothing up to the last `frozen` source can go stale: layers are write-once,
    so the fold over them is materialised and cached. Past it there is nothing
    to invalidate, because nothing is materialised - `dims` and the maps re-fold
    the unfrozen tail per access, over a relation that reads whatever the
    staging tables hold when it is collected.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/read-path/#resolving-a-relation)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """

    revision_id: UUID
    sources: list[LayerSource]
    con: DuckDBPyConnection
    declared: Schema | None = None
    """This record's schema where it is not the connection root's.

    A standalone record carries its own `manifest.json` and may be read through
    a connection rooted anywhere, so `Record.at` reads it and passes it here.
    `None` for a node of a tree, whose schema sits once beside the tree and is
    the connection's to answer.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """

    def with_source(self, source: LayerSource) -> NodeCache:
        """This cache with one more layer folded on top.

        What a `WorkingRecord` builds to read its staged rows: the staged source
        is the last entry, resolved over whatever the record was reading before.
        The schema comes along unchanged - an edit is made *under* a
        declaration, never one that redeclares.

        Notes
        -----
        - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        """
        return NodeCache(
            self.revision_id, [*self.sources, source], self.con, self.declared
        )

    def _map(self, kind: str) -> DuckDBPyRelation:
        return _table(self.revision_id, self.sources, self.con, self.schema, kind=kind)

    def source_for(self, layer_uuid: UUID) -> LayerSource:
        """The source the fold stamped `layer_uuid` from.

        How a winning row is read back: the map names which layer owns a key,
        and the row itself comes from that layer's own member.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        for source in self.sources:
            if source.layer_id == layer_uuid:
                return source
        # A layer the map names but the source list does not hold: the map was
        # materialised over a longer ancestry than this node reads, so the
        # winning row is still in that layer's own directory.
        return ParquetLayer(layer_uuid, self.con)

    @property
    def inputs(self) -> DuckDBPyRelation:
        """The resolved inputs owner map: which layer owns each attribute key.

        Ownership only - membership is not gated here. A key whose coordinate is
        not live (a deleted component, a deleted group tuple) is dropped per
        attribute in `relation`, where the attribute's own dims say which
        memberships its rows carry.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return self._map("inputs")

    @property
    def entity_axis(self) -> DuckDBPyRelation | None:
        """The resolved entity axis: one row per live component, `entity_type` carried.

        Folded like any axis (`dims.axes`), not an owner map - the winning row is
        the whole row in one file. `None` where no layer wrote a component.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        return self.dims.axes.get("entity")

    def group(self, name: str) -> DuckDBPyRelation | None:
        """One declared group's resolved relation, folded like an axis.

        The winning row per `group_key`, static columns carried, in member order.
        `None` where no layer wrote a row.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return self.dims.groups.get(name)

    @property
    def stable(self) -> bool:
        """Whether every source is frozen, so a fold over them cannot go stale.

        What decides between caching an answer and re-folding per access - here
        and in the `Record` over this cache, where the same question governs its
        key sets. False exactly when the last source is a staging area.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        """
        return frozen_prefix(self.sources) == len(self.sources)

    @property
    def dims(self) -> Dims:
        """Every declared dim's folded axis relation.

        A plain property rather than a `cached_property`: the fold is only
        cacheable where every source is frozen, which is the same rule `_map`
        applies - one concept twice rather than a special case.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        """
        if self.stable:
            return self._cached_dims
        return resolve_dims(self.schema, self.sources, self.con)

    @cached_property
    def _cached_dims(self) -> Dims:
        """`dims` where every source is frozen, so the fold cannot go stale."""
        return resolve_dims(self.schema, self.sources, self.con)

    @cached_property
    def schema(self) -> Schema:
        """This record's one manifest, read once per node.

        From beside *this* connection's layers, so two records on different
        roots in one process each read their own - unless `declared` names one,
        which a standalone record read through a foreign connection needs.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        """
        return self.declared if self.declared is not None else read_schema(self.con)

    def entity_types(self) -> set[str]:
        """Types with any live component row, from the resolved entity axis.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        axis = self.entity_axis
        if axis is None:
            return set()
        return set(distinct_values(axis, "entity_type", order=False))

    def attribute_names(self) -> list[str]:
        """Every input attribute any layer owns a row for, from the owner map.

        Across component types, matching the file layout: one
        `inputs/<attr>.parquet` holds every type's rows. Ordered, so a
        `Record` over this has a stable key order.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
        """
        return list(distinct_values(self.inputs, "attribute"))

    def output_names(self) -> list[str]:
        """Every result attribute this record's own layer holds.

        Its own layer only: outputs do not overlay, so there is no map to
        consult and nothing inherited from an ancestor.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        rel = self.sources[-1].all_attributes("outputs")
        if rel is None:
            return []
        return list(distinct_values(rel, "attribute"))

    def attributes_of(self, ctype: str) -> dict[str, Flags]:
        """Per attribute of `ctype`, which dims its rows use.

        The map computes the flags per key, so per component; this unions them
        over the names of one type, which is the granularity a consumer assigns
        containers at. A type whose components disagree yields a dim in both
        sets - the instruction to use both containers, each taking the rows it
        matches. The union stops at the type boundary: across types it
        would describe neither.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        """
        # The flags have a field per *broadcast* dim: an address coordinate
        # never broadcasts, so "did a row set it" is not a question about it.
        dims = self.schema.broadcast_dims
        # Scoped by a semi-join to the resolved entity axis, the table saying
        # what type a name is (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
        axis = self.entity_axis
        if axis is None:
            return {}
        of_type = axis.filter(col("entity_type") == lit(ctype)).project("entity")
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

        # The structs come back as dicts keyed by dim, so each set is a filter by
        # name rather than a positional slice.
        return flags_from_rows(self.schema, dims, rows)

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
            with_columns(keys.schema, rel, *columns).project(
                lit(layer_uuid).alias("layer_uuid"), col("*")
            )
            for (layer_uuid,) in om["layer_uuid"].distinct().fetchall()
            if (rel := self.source_for(layer_uuid).attribute(attribute)) is not None
        ]
        if not layers:
            return _empty_relation(keys.schema, con, *columns)

        resolved = (
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
        return resolved

    def outputs(self, attribute: str) -> DuckDBPyRelation:
        """A result attribute from this record's own layer; outputs do not overlay.

        No fold and no owner map: if this layer has no `outputs/`, the record
        has no results - an ancestor's are not inherited.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        rel = self.sources[-1].attribute(attribute, "outputs")
        if rel is not None:
            return rel
        return _empty_relation(self.schema, self.con, *self.schema.long_columns)

    def entity_type_frame(self, ctype: str) -> DuckDBPyRelation | None:
        """Wide static members of one type, resolved inline, in member order.

        The one axis whose *values* live in another file: a component's wide
        static columns are per-type (`dims/entity_type/<ctype>.parquet`), not on
        the entity axis. So the per-type files fold on `entity` (last-writer-wins,
        each layer's own row winning), and the result is semi-joined to the live
        resolved entity axis - which decides membership, type and order - so a row
        whose component the axis does not carry drops out. `None` where the type
        has no live member.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        axis = self.entity_axis
        if axis is None:
            return None
        wide = fold_axis(
            [source.entity_type(ctype) for source in self.sources],
            ("entity",),
            self.con,
        )
        if wide is None:
            return None
        # `_pos` off the *unfiltered* axis, which is still in fold (member) order;
        # numbering after the type filter would rest on a filter preserving row
        # order, which it need not.
        live = axis.project(star(), sql("row_number() OVER ()").alias("_pos")).filter(
            col("entity_type") == lit(ctype)
        )
        return self._in_axis_order(wide, live, ("entity",))

    def group_frame(self, group: str) -> DuckDBPyRelation | None:
        """One group's resolved rows, folded like an axis, in member order.

        Not per type, which is no coordinate of a group. The folded relation is
        already the winning row per `group_key`, carrying every non-key column -
        an attribute over the group, an `into` label - in member order, read
        inline with no owner map.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        rel = self.group(group)
        if rel is None or rel.limit(1).fetchone() is None:
            return None
        return rel

    def _in_axis_order(
        self, wide: DuckDBPyRelation, axis: DuckDBPyRelation, match: tuple[str, ...]
    ) -> DuckDBPyRelation | None:
        """`wide`'s rows scoped to `axis`, in `axis`'s member order.

        `axis` carries a `_pos` in member order (`fold_axis`); the join does not
        preserve row order, so `_pos` is carried through it and sorted by, then
        dropped. The join also scopes `wide` to the live axis - a row whose
        component the axis does not carry drops out.
        """
        joined = wide.set_alias("u").join(
            axis.set_alias("o"), null_safe("u", "o", match)
        )
        cols = [c for c in wide.columns if c not in ("entity_type", "deleted")]
        result = joined.project(*(col("u", c) for c in cols), col("o", "_pos")).order(
            "_pos"
        )
        if result.limit(1).fetchone() is None:
            return None
        return result.project(star(exclude=["_pos"]))
