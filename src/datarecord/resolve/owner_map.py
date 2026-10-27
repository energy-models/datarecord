# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The owner map: which layer owns each key, folded over a record's layers.

The fold over a frozen prefix is kept in tables the workspace shares, the owner
map's and the coords' alike.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
- [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
"""

from __future__ import annotations

import operator
import time
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from contextlib import AbstractContextManager, contextmanager, nullcontext
from functools import reduce
from itertools import combinations
from uuid import UUID, uuid4

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import StarExpression as star

from datarecord.duck import (
    empty_relation,
    ex_all,
    fn,
    join_on,
    null_safe,
    struct_field,
    struct_of,
    union_all_by_name,
)
from datarecord.resolve.coords import (
    Coords,
    _base_and_above,
    resolve_coords,
)
from datarecord.resolve.fold import read_owner_map
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import LAYER_COLUMN, Schema

# The owner map's `layer_uuid` column type - a layering mechanism, not
# something a schema declares.
LAYER_UUID_TYPE = "UUID"


def _table_name(revision_id: UUID, schema: Schema) -> str:
    return _coord_table("owner_map", ",".join(schema.owner_map_key), revision_id)


def _coord_table(kind: str, name: str, revision_id: UUID) -> str:
    """The shared table one folded axis, dead label set or owner map is kept in.

    `name` is spelled as declared, which `Schema` holds to a plain identifier.
    """
    return f"{kind}_{name}_{revision_id.hex}"


def _shared(
    con: DuckDBPyConnection,
    name: str,
    build: Callable[[], AbstractContextManager[DuckDBPyRelation]],
) -> DuckDBPyRelation:
    """Table `name`, built by whichever cursor misses it first.

    `build` enters the relation the table is made of, and exits once the
    table holds it, releasing whatever the relation read. Two cursors missing
    at once both build, and DuckDB fails the second `create` with a
    write-write conflict rather than a name clash; what it would have created
    is the winner's table, so that is read instead. The conflict can come
    before the winner commits, when its table is not yet visible, so the
    `create` is retried until it either lands or meets the committed table.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """
    try:
        return con.table(name)
    except duckdb.CatalogException:
        pass
    with build() as rel:
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
            break
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
    - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
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
    - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
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
    - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
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
    schema: Schema,
    con: DuckDBPyConnection,
    parent: DuckDBPyRelation | None,
    removed: Mapping[str, DuckDBPyRelation],
) -> DuckDBPyRelation | None:
    """This layer's owner map: its `attributes/` keys as written, folded over `parent`.

    The parent rows naming a label `removed` holds are dropped first, per
    `partial` dim. A label inside a struct is reached through its tuple, which
    the layer removed with it. A parent row this layer's rows cover whole is
    dropped; one they cross stays as written, and the layer owns an overlap
    row for each crossing pair (`_overlaps`). `None` where neither side holds
    a row.

    Raises
    ------
    ValueError
        Where a row of this layer leaves a dim outside `partial` NULL over a
        parent row naming it, and the layer holds no row naming it there
        (`_refuse_unfilled`).

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    own = _own(source, schema)
    if parent is None:
        return own
    kept = parent
    for name, gone in removed.items():
        gone = gone.project(col(name).alias("_gone"))
        kept = kept.join(gone, col(name) == col("_gone"), how="anti")
    if own is None:
        return kept
    dims = schema.partial_dims
    masks = named_sets(own, dims)
    for mask in masks:
        drop = own.filter(names_exactly(dims, mask)).project(
            *(col(c).alias(f"_o_{c}") for c in ("attribute", *mask))
        )
        covered = ex_all(col(c) == col(f"_o_{c}") for c in ("attribute", *mask))
        kept = kept.join(drop, covered, how="anti")
    patterns = named_sets(kept, dims) if any(masks) else []
    pairs = {
        (p, o): _pair(kept, own, p, o, schema)
        for o in masks
        for p in patterns
        if not o <= p
    }
    _refuse_unfilled(pairs, own, schema, source)
    overlaps = [
        _overlaps(pair, o, schema) for (p, o), pair in pairs.items() if not p <= o
    ]
    if not overlaps:
        return union_all_by_name([kept, own], con)
    crossed = reduce(DuckDBPyRelation.union, overlaps).distinct()
    return union_all_by_name([kept, own, crossed], con)


def named_sets(rel: DuckDBPyRelation, columns: Sequence[str]) -> list[frozenset[str]]:
    """The distinct sets of `columns` the rows of `rel` hold a non-NULL value in."""
    if not columns:
        return [frozenset()]
    rows = rel.project(*(col(c).isnotnull() for c in columns)).distinct().fetchall()
    return [
        frozenset(c for c, n in zip(columns, row, strict=True) if n) for row in rows
    ]


def names_exactly(columns: Sequence[str], named: Set[str]) -> Expression:
    """Whether a row holds a non-NULL value in exactly the `named` of `columns`."""
    return reduce(
        operator.and_,
        (col(c).isnotnull() if c in named else col(c).isnull() for c in columns),
        lit(True),  # noqa: FBT003
    )


def unsettled(
    rel: DuckDBPyRelation, coordinates: Sequence[str]
) -> Iterator[tuple[frozenset[str], frozenset[str], DuckDBPyRelation]]:
    """Each pair of named sets of `rel` neither holds, with the coordinates its rows meet at that no row names both at.

    Two rows meet where they agree on every one of `coordinates` both name; a
    meeting coordinate takes the labels either names. There the broadcast rule
    finds no row more specific than both. The relation is lazy and yields one
    column per coordinate.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    sets = named_sets(rel, coordinates)

    def exactly(named: frozenset[str]) -> DuckDBPyRelation:
        return rel.filter(names_exactly(coordinates, named)).project(
            *map(col, coordinates)
        )

    for a, b in combinations(sets, 2):
        if a <= b or b <= a:
            continue
        on = [col("a", c) == col("b", c) for c in coordinates if c in a & b]
        meets = join_on(
            exactly(a).distinct().set_alias("a"),
            exactly(b).distinct().set_alias("b"),
            on,
        ).project(*(coalesce(col("a", c), col("b", c)).alias(c) for c in coordinates))
        if a | b in sets:
            settled = ex_all(col("m", c) == col("u", c) for c in a | b)
            meets = meets.set_alias("m").join(
                exactly(a | b).set_alias("u"), settled, how="anti"
            )
        yield a, b, meets


def _pair(
    kept: DuckDBPyRelation,
    own: DuckDBPyRelation,
    p: frozenset[str],
    o: frozenset[str],
    schema: Schema,
) -> DuckDBPyRelation:
    """Each parent row naming exactly `p` beside each row of `own` naming exactly `o` it covers a common key with.

    The own row's columns are prefixed `_o_`. Two rows cover a common key
    where they agree on every dim both name, an equi-join on those alone.
    """
    dims = schema.partial_dims
    mine = own.filter(names_exactly(dims, o))
    mine = mine.project(*(col(c).alias(f"_o_{c}") for c in mine.columns))
    on = [col(c) == col(f"_o_{c}") for c in ("attribute", *(o & p))]
    return join_on(kept.filter(names_exactly(dims, p)), mine, on)


def _overlaps(
    pairs: DuckDBPyRelation, o: frozenset[str], schema: Schema
) -> DuckDBPyRelation:
    """The overlap row of each crossing pair: every label either row names, owned as the own row is.

    The own row's layer and flags go with it, so a value row of that layer
    finds it by the same NULL-safe join as its own key and takes the parent
    row's labels. It names a strict superset of both rows, which is what lets
    the broadcast rule pick it where they cross.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    return pairs.project(
        *_met(o, schema),
        *(
            col(f"_o_{c}").alias(c)
            for c in ("attribute", LAYER_COLUMN, "varies", "broadcast", "breakpoints")
        ),
    )


def _met(o: frozenset[str], schema: Schema) -> list[Expression]:
    """A crossing pair's overlap key: the own row's label on each dim of `o`, the parent row's elsewhere."""
    return [col(f"_o_{d}" if d in o else d).alias(d) for d in schema.partial_dims]


def _refuse_unfilled(
    pairs: Mapping[tuple[frozenset[str], frozenset[str]], DuckDBPyRelation],
    own: DuckDBPyRelation,
    schema: Schema,
    source: LayerSource,
) -> None:
    """Refuse a layer leaving a dim outside `partial` NULL over a parent row naming it, unfilled.

    The layer must hold a row naming the dim at the key the two share.
    `pairs` is `_pair` per `(p, o)`.

    Raises
    ------
    ValueError
        Naming the attribute, the dim and the key.

    Notes
    -----
    - [a dim outside `partial`](https://energy-models.github.io/datarecord/design/layers/resolution/#a-dim-outside-partial)
    """
    dims = schema.partial_dims
    others = [d for d in schema.broadcast_dims if d not in dims]
    if not others or not pairs:
        return

    unset = lit(False)  # noqa: FBT003

    def keyed(o: frozenset[str], pair: DuckDBPyRelation) -> DuckDBPyRelation:
        need = {
            d: struct_field(pair, "varies", d, unset)
            & ~struct_field(pair, "_o_varies", d, unset)
            for d in others
        }
        return pair.filter(reduce(operator.or_, need.values())).project(
            *_met(o, schema),
            col("attribute"),
            *(need[d].alias(f"_need_{d}") for d in others),
        )

    open_ = reduce(DuckDBPyRelation.union, (keyed(o, r) for (_, o), r in pairs.items()))
    open_ = open_.set_alias("k")
    filled = own.project(
        *(col(c) for c in ("attribute", *dims)),
        *(struct_field(own, "varies", d, unset).alias(f"_has_{d}") for d in others),
    ).set_alias("f")
    unfilled = open_.join(
        filled, null_safe("k", "f", ("attribute", *dims)), how="left"
    ).filter(
        reduce(
            operator.or_,
            (col(f"_need_{d}") & ~coalesce(col(f"_has_{d}"), unset) for d in others),
        )
    )
    row = (
        unfilled.project(
            col("k", "attribute"),
            *(col("k", d).cast(sqltypes.VARCHAR) for d in dims),
            *(col(f"_need_{d}") for d in others),
        )
        .limit(1)
        .fetchall()
    )
    if not row:
        return
    attribute, *key = row[0][: 1 + len(dims)]
    dim = next(d for d, n in zip(others, row[1 + len(dims) :], strict=True) if n)
    at = {d: k for d, k in zip(dims, key, strict=True) if k is not None}
    msg = (
        f"layer {source.layer_id} leaves {dim!r} NULL for {attribute!r} at {at} "
        f"where a parent row names it; write it along the parent's {dim!r} "
        f"labels there, as `WorkingRecord.set` does"
    )
    raise ValueError(msg)


def _own(source: LayerSource, schema: Schema) -> DuckDBPyRelation | None:
    """`source`'s owner-map rows: one per key as written, `None` without attributes.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    rel = source.all_attributes()
    if rel is None:
        return None
    broadcast = schema.broadcast_dims
    rel = cast_declared(
        schema,
        with_columns(schema, rel, "breakpoint", *schema.broadcast_dims),
    )
    tagged = rel.project(
        *(col(d) for d in broadcast),
        col("attribute"),
        lit(str(source.layer_id)).cast(LAYER_UUID_TYPE).alias(LAYER_COLUMN),
        col("breakpoint"),
    )
    over = {d: _over(schema, d) for d in broadcast}
    return tagged.aggregate(
        [
            *(col(c) for c in (*schema.owner_map_key, LAYER_COLUMN)),
            struct_of(
                {d: fn.bool_or(col(d).isnotnull() & over[d]) for d in broadcast}
            ).alias("varies"),
            struct_of(
                {d: fn.bool_or(col(d).isnull() & over[d]) for d in broadcast}
            ).alias("broadcast"),
            fn.bool_or(col("breakpoint").isnotnull()).alias("breakpoints"),
        ]
    )


def _over(schema: Schema, dim: str) -> Expression:
    """Whether a row's attribute is over `dim`.

    An attribute not over `dim` holds NULL there for want of the column, which
    is neither a label nor a broadcast.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
    """
    lacking = [a for a in schema.attributes if dim not in schema.coordinates_of(a)]
    if not lacking:
        return lit(True)  # noqa: FBT003
    return ~col("attribute").isin(*(lit(a) for a in lacking))


def _fold_map(
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    made: list[str] | None = None,
) -> DuckDBPyRelation:
    """A record's owner map, folded down over `sources`, checkpointed into `made` as `_fold_onto` does.

    The axes fold to a resolved copy instead (`resolve_coords`). The deepest
    materialised source's map is the fold's *seed* rather than a step of it: it
    is already folded over everything at or below it, so it is read as the
    starting relation (`_frozen_table`) and only the sources above it are
    folded on top.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    base, above = _base_and_above(sources, con, schema)
    if base is None:
        rel = None
    else:
        seed = sources[: len(sources) - len(above)]
        rel = _frozen_table(seed[-1].layer_id, seed, con, schema, read=base.owner_map)

    held = {
        name: None if base is None else base.axes.get(name) for name in _pruned(schema)
    }
    return _fold_onto(rel, above, held, schema, con, made=made)


@contextmanager
def _checkpointed(
    sources: Sequence[LayerSource], con: DuckDBPyConnection, schema: Schema
) -> Iterator[DuckDBPyRelation]:
    """`_fold_map` with each source's fold kept in a table, every one dropped on exit.

    The relation entered reads the last table, so it is read before exit.
    """
    made: list[str] = []
    try:
        yield _fold_map(sources, con, schema, made=made)
    finally:
        for name in made:
            con.execute(f"DROP TABLE IF EXISTS {name}")


def _checkpoint(
    rel: DuckDBPyRelation, made: list[str], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """`rel` as a table, dropping the tables in `made` and leaving its name there."""
    name = f"_fold_{uuid4().hex}"
    rel.create(name)
    for previous in made:
        con.execute(f"DROP TABLE {previous}")
    made[:] = [name]
    return con.table(name)


def _pruned(schema: Schema) -> tuple[str, ...]:
    """The memberships whose removed labels prune the owner map: the `partial` dims.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    return schema.partial_dims


def _fold_onto(
    rel: DuckDBPyRelation | None,
    sources: Sequence[LayerSource],
    held: dict[str, DuckDBPyRelation | None],
    schema: Schema,
    con: DuckDBPyConnection,
    *,
    made: list[str] | None = None,
) -> DuckDBPyRelation:
    """Fold each of `sources` onto the owner map `rel`, `None` being no map yet.

    A source restating a membership removes the labels the previous
    restatement held and its own omits. `held` is that previous restatement
    per `_pruned` membership as of the first source, `None` where there is
    none, so a first restatement removes nothing.

    Where `made` is a list, each source's fold is kept in a table of its own
    before the next folds onto it, since DuckDB binds a relation's whole
    tree at every step, which makes a long chain quadratic to build. The
    table the map is read from is left named in `made`, for the caller to
    drop once it has read the map (`_checkpointed`).

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    held = dict(held)
    for source in sources:
        removed = {}
        for name, before in held.items():
            now = source.axis(name)
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
        rel = fold_attributes(source, schema, con, rel, removed)
        if made is not None and rel is not None:
            rel = _checkpoint(rel, made, con)
    if rel is None:
        return _empty_relation(schema, con, *schema.owner_map_columns)
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
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
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
    prefix: Coords,
) -> DuckDBPyRelation:
    """The owner map for `revision_id`, materialised as far as `frozen` allows.

    The frozen prefix is folded once and kept as a connection-scoped table,
    which never needs invalidating since layers are write-once. Whatever follows
    it is folded on top per call and handed back as a relation, so a staging
    area's edits are picked up with no bookkeeping at all: a relation over a
    staging table reads whatever the table holds when it is collected.
    `prefix` is the frozen prefix's coords, which a restatement after it
    removes labels from.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    frozen = frozen_prefix(sources)
    rel = _frozen_table(revision_id, sources[:frozen], con, schema)
    held = {name: prefix.axes.get(name) for name in _pruned(schema)}
    return _fold_onto(rel, sources[frozen:], held, schema, con)


def _frozen_table(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
    *,
    read: DuckDBPyRelation | None = None,
) -> DuckDBPyRelation | None:
    """The frozen prefix's owner map, its own materialised one or a cached fold, `None` over no source.

    Keyed by `revision_id` rather than by the prefix: the prefix *is* what that
    node resolves from, a staged source only ever being appended after it. The
    table is shared by every cursor on the workspace and carries the owner-map
    key in its name, so a schema whose `partial` changed folds afresh, while
    one only declaring more reads it as it is, a flag field it lacks read as
    unset. A persisted map is read only where keyed as the schema keys one
    (`read_owner_map`), and cast, parquet not carrying the declared types.

    The one place a cached map enters a fold: a materialised seed is read here
    too (`_fold_map`).

    Parameters
    ----------
    read
        The prefix's materialised map where the caller already holds it.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """
    last = sources[-1] if sources else None
    rel = read
    if (
        rel is None
        and isinstance(last, ParquetLayer)
        and last.revision_id == revision_id
    ):
        rel = read_owner_map(revision_id, con, schema, last.store)
    if rel is not None:
        return cast_declared(schema, rel)
    if not sources:
        # Nothing frozen to fold, which is a `WorkingRecord` over a base that is
        # itself unfrozen; the tail folds onto no map.
        return None
    return _shared(
        con,
        _table_name(revision_id, schema),
        lambda: _checkpointed(sources, con, schema),
    )


def _frozen_coords(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    schema: Schema,
) -> list[tuple[str, str, UUID | None]]:
    """The `(kind, name, owner)` of every axis and dead label set this frozen fold resolved.

    A `one_of` dim's axis is left out, being the schema's rather than the fold's.
    `owner` is an axis's `Coords.dim_owners` entry, `None` for dead labels.

    Each is a shared table (`_coord_table`); the list is one more, created after
    them, so a reader finding it finds every table it names.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """

    def fold() -> DuckDBPyRelation:
        coords = resolve_coords(schema, sources, con)
        made = [
            *(
                ("dims", dim, rel, str(coords.dim_owners[dim]))
                for dim, rel in coords.axes.items()
                if dim not in schema.one_of_dims
            ),
            *(("dead", name, rel, None) for name, rel in coords.dead.items()),
        ]
        for kind, name, rel, _ in made:
            table = _coord_table(kind, name, revision_id)
            _shared(con, table, lambda: nullcontext(rel))  # noqa: B023 - called before `rel` moves on
        return con.sql(
            "SELECT unnest($kinds::VARCHAR[]) AS kind, unnest($names::VARCHAR[]) AS name, "
            f"unnest($owners::VARCHAR[])::{LAYER_UUID_TYPE} AS owner",
            params={
                "kinds": [m[0] for m in made],
                "names": [m[1] for m in made],
                "owners": [m[3] for m in made],
            },
        )

    rows = _shared(
        con, f"coords_{revision_id.hex}", lambda: nullcontext(fold())
    ).fetchall()
    return [(kind, name, owner) for kind, name, owner in rows]
