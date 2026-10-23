# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The owner map: which layer owns each key, folded over a record's layers.

The fold over a frozen prefix is kept in tables the workspace shares, the owner
map's and the coords' alike.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
- [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from hashlib import sha256
from uuid import UUID

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, sqltypes
from duckdb import StarExpression as star

from datarecord.duck import (
    empty_relation,
    fn,
    null_safe,
    struct_of,
    try_read_parquet,
    union_all_by_name,
)
from datarecord.resolve.coords import (
    Coords,
    _base_and_above,
    resolve_coords,
    restatement,
)
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import LAYER_COLUMN, Schema

# The owner map's `layer_uuid` column type - a layering mechanism, not
# something a schema declares.
LAYER_UUID_TYPE = "UUID"


def _table_name(revision_id: UUID) -> str:
    return f"owner_map_{revision_id.hex}"


def _coord_table(kind: str, name: str, revision_id: UUID) -> str:
    """The shared table one folded axis or group is kept in.

    The name is hashed rather than spelled, being a caller's string that becomes
    an identifier.
    """
    digest = sha256(name.encode()).hexdigest()[:16]
    return f"{kind}_{digest}_{revision_id.hex}"


def _shared(
    con: DuckDBPyConnection, name: str, build: Callable[[], DuckDBPyRelation]
) -> DuckDBPyRelation:
    """Table `name`, built by whichever cursor misses it first.

    Two cursors missing at once both build, and DuckDB fails the second
    `create` with a write-write conflict rather than a name clash; what it
    would have created is the winner's table, so that is read instead. The
    conflict can come before the winner commits, when its table is not yet
    visible, so the `create` is retried until it either lands or meets the
    committed table.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    """
    try:
        return con.table(name)
    except duckdb.CatalogException:
        pass
    rel = build()
    while True:
        try:
            rel.create(name)
        except duckdb.TransactionException as e:
            if "conflict" not in str(e).lower():
                raise
            time.sleep(0.001)
            continue
        except duckdb.CatalogException:
            pass
        return con.table(name)


def cast_declared(
    schema: Schema,
    rel: DuckDBPyRelation,
    subject: str | None = None,
    attribute: str | None = None,
) -> DuckDBPyRelation:
    """Return `rel` with each column the schema declares a type for cast to that type.

    Other columns are left as they are. `subject` and `attribute` are as in
    `Schema.declared`.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    cols = [
        col(c).cast(found[1]).alias(c)
        if (found := schema.declared(c, subject, attribute))
        else col(c)
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
    return empty_relation(
        con, **{c: schema.column_type(c) or sqltypes.VARCHAR for c in columns}
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
    added = [
        lit(None).cast(schema.column_type(c) or sqltypes.VARCHAR).alias(c)
        for c in missing
    ]
    return rel.project(star(), *added)


def fold_attributes(
    source: LayerSource,
    keys: Coords,
    con: DuckDBPyConnection,
    parent: DuckDBPyRelation,
    removed: Mapping[str, DuckDBPyRelation],
) -> DuckDBPyRelation:
    """This layer's owner map: its `attributes/` keys, folded over `parent`.

    The parent rows naming a label `removed` holds are dropped first, per
    `partial` dim or keyed group. A label inside a struct is reached through
    its tuple, which the layer removed with it.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    schema = keys.schema
    rel = source.all_attributes()
    if rel is None:
        own = _empty_relation(schema, con, *schema.owner_map_columns)
    else:
        # Every declared dim and group column too, not just `breakpoint`: one
        # file carries only its own attribute's columns, so a coordinate another
        # attribute uses is absent here and must read as NULL rather than fail
        # to bind.
        broadcast = schema.broadcast_dims
        rel = cast_declared(
            schema,
            with_columns(
                schema, rel, "breakpoint", *schema.group_columns, *schema.dims
            ),
        )
        rel, dims = keys.expand_dims(
            rel.set_alias("i"),
            tuple(d for d in schema.partial_dims if d in broadcast),
            con,
        )
        # Each broadcast dim is carried twice: the (possibly broadcast) key
        # value, and `_raw_<dim>` as the row stored it. The flags describe the
        # stored form - whether a row set the dim or left it NULL - so they
        # cannot be read off the expanded value, which is never NULL once
        # broadcast (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule).
        # `dims` covers the partial ones, expanded; the rest pass through as
        # stored. Together they are every declared dim, exactly once.
        expanded = set(dims)
        tagged = rel.project(
            *(
                col("i", d)
                for d in (*schema.group_columns, *schema.dims)
                if d not in expanded
            ),
            *(expr.alias(d) for d, expr in dims.items()),
            *(col("i", d).alias(f"_raw_{d}") for d in broadcast),
            col("i", "attribute"),
            lit(str(source.layer_id)).cast(LAYER_UUID_TYPE).alias(LAYER_COLUMN),
            col("i", "breakpoint"),
        )
        own = tagged.aggregate(
            [
                *(col(c) for c in (*schema.owner_map_key, LAYER_COLUMN)),
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

    kept = parent.set_alias("p")
    for name, gone in removed.items():
        on = null_safe("x", "p", (name,))
        kept = kept.join(gone.set_alias("x"), on, how="anti").set_alias("p")
    # Parent minus what this layer restates, union this layer's own.
    kept = kept.join(
        own.set_alias("o"), null_safe("p", "o", schema.owner_map_key), how="anti"
    )
    return union_all_by_name([kept, own], con)


def _fold_map(
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
) -> DuckDBPyRelation:
    """A record's owner map, folded down over `sources`.

    The axes fold to a resolved copy instead (`resolve_coords`). The deepest materialised source's map is the fold's
    *seed* rather than a step of it: it is already folded over everything at or
    below it, so it is read as the starting relation (`_frozen_table`) and only
    the sources above it are folded on top.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    base, above = split = _base_and_above(sources, con, schema)
    keys = resolve_coords(schema, sources, con, split)
    if base is None:
        rel = _empty_relation(keys.schema, con, *keys.schema.owner_map_columns)
    else:
        seed = sources[: len(sources) - len(above)]
        rel = _frozen_table(
            seed[-1].layer_id, seed, con, keys.schema, read=base.owner_map
        )

    held = {
        name: None if base is None else (base.groups | base.axes).get(name)
        for name in _pruned(schema)
    }
    return _fold_onto(rel, above, held, keys, con)


def _pruned(schema: Schema) -> tuple[str, ...]:
    """The memberships whose removed labels prune the owner map: the keyed ones.

    A `partial` dim, and a group in the owner-map key.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    keyed = set(schema.owner_map_key)
    return (
        *(g for g in schema.group_columns if g in keyed),
        *(d for d in schema.partial or () if d in schema.dimensions),
    )


def _fold_onto(
    rel: DuckDBPyRelation,
    sources: Sequence[LayerSource],
    held: dict[str, DuckDBPyRelation | None],
    keys: Coords,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Fold each of `sources` onto the owner map `rel`.

    A source restating a membership removes the labels the previous
    restatement held and its own omits. `held` is that previous restatement
    per `_pruned` membership as of the first source, `None` where there is
    none, so a first restatement removes nothing.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    schema = keys.schema
    held = dict(held)
    for source in sources:
        removed = {}
        for name, before in held.items():
            now = restatement(source, name, schema)
            if now is None:
                continue
            if before is not None:
                removed[name] = (
                    before.project(col(name))
                    .set_alias("b")
                    .join(
                        now.project(col(name)).set_alias("n"),
                        col("b", name) == col("n", name),
                        how="anti",
                    )
                )
            held[name] = now
        rel = fold_attributes(source, keys, con, rel, removed)
    return rel


def frozen_prefix(sources: Sequence[LayerSource]) -> int:
    """How many leading sources cannot change under a reader.

    The one rule the source list has to carry: **the fold is materialised up to
    the last frozen source; everything after it stays a relation.** Everything a
    `Record` caches rests on layers being write-once, which a staging area is
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
    keys: Coords,
    prefix: Coords,
) -> DuckDBPyRelation:
    """The owner map for `revision_id`, materialised as far as `frozen` allows.

    The frozen prefix is folded once and kept as a connection-scoped table,
    which never needs invalidating since layers are write-once. Whatever follows
    it is folded on top per call, against `keys`, the coords folded over every
    source, and handed back as a relation, so a staging area's edits are picked
    up with no bookkeeping at all: a relation over a staging table reads
    whatever the table holds when it is collected. `prefix` is the frozen
    prefix's coords, which a restatement after it removes labels from.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    schema = keys.schema
    frozen = frozen_prefix(sources)
    rel = _frozen_table(revision_id, sources[:frozen], con, schema)
    held = {name: (prefix.groups | prefix.axes).get(name) for name in _pruned(schema)}
    return _fold_onto(rel, sources[frozen:], held, keys, con)


def _frozen_table(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    read: DuckDBPyRelation | None = None,
) -> DuckDBPyRelation:
    """The frozen prefix's owner map, its own materialised one or a cached fold.

    Keyed by `revision_id` rather than by the prefix: the prefix *is* what that
    node resolves from, a staged source only ever being appended after it. The
    table is shared by every cursor on the workspace and carries no schema in
    its name, so a schema widened since the fold reads it as it is, a column it
    lacks padded as unset. A persisted map is cast, parquet not carrying the
    declared types.

    The one place a cached map enters a fold: a materialised seed is read here
    too (`_fold_map`).

    Parameters
    ----------
    read
        The prefix's materialised map where the caller already holds it.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    last = sources[-1] if sources else None
    rel = read
    if (
        rel is None
        and isinstance(last, ParquetLayer)
        and last.revision_id == revision_id
    ):
        rel = try_read_parquet(last.store.owner_map_uri(revision_id), con)
    if rel is not None:
        rel = cast_declared(schema, rel)
    elif not sources:
        # Nothing frozen to fold, which is a `WorkingRecord` over a base that is
        # itself unfrozen; the tail folds onto an empty map.
        return _empty_relation(schema, con, *schema.owner_map_columns)
    else:
        rel = _shared(
            con, _table_name(revision_id), lambda: _fold_map(sources, con, schema)
        )
    return with_columns(schema, rel, *schema.owner_map_key)


def _frozen_coords(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
) -> list[tuple[str, str]]:
    """The `(kind, name)` of every axis and group this frozen fold resolved.

    Each is a shared table (`_coord_table`); the list is one more, created after
    them, so a reader finding it finds every table it names.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    """

    def fold() -> DuckDBPyRelation:
        coords = resolve_coords(schema, sources, con)
        made = [
            *(("dims", dim, rel) for dim, rel in coords.axes.items()),
            *(("group", group, rel) for group, rel in coords.groups.items()),
            *(("dead", name, rel) for name, rel in coords.dead.items()),
        ]
        for kind, name, rel in made:
            table = _coord_table(kind, name, revision_id)
            _shared(con, table, lambda: rel)  # noqa: B023 - called before `rel` moves on
        return con.sql(
            "SELECT unnest($kinds::VARCHAR[]) AS kind, unnest($names::VARCHAR[]) AS name",
            params={"kinds": [k for k, _, _ in made], "names": [n for _, n, _ in made]},
        )

    rows = _shared(con, f"coords_{revision_id.hex}", fold).fetchall()
    return [(kind, name) for kind, name in rows]
