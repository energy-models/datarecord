# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The owner map: which layer owns each key, folded over a record's layers.

The fold over a record's frozen layers is kept in tables the workspace shares, the owner
map's and the coords' alike.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
- [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
"""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from contextlib import contextmanager
from functools import reduce
from itertools import combinations
from uuid import UUID

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import (
    FOLD,
    ex_all,
    fn,
    join_on,
    named_mask,
    null_safe,
    shared_table,
    temp_table,
    union_all_by_name,
)
from datarecord.resolve.coords import (
    Coords,
    _base_and_above,
    _dead_keys,
    removed_labels,
)
from datarecord.resolve.fold import Fold, read_owner_map
from datarecord.resolve.owner_keys import (
    _empty_relation,
    _flags,
    _per_schema,
    _tagged,
    cast_declared,
    owner_keys,
    with_columns,
)
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import (
    FLAG_COLUMNS,
    LAYER_COLUMN,
    Schema,
)


def fold_layer(
    parent: DuckDBPyRelation | None,
    own: DuckDBPyRelation | None,
    removed: Mapping[str, DuckDBPyRelation],
    schema: Schema,
    attribute: str | None = None,
    *,
    refuse: UUID | None = None,
    keep: Callable[[DuckDBPyRelation], DuckDBPyRelation] = lambda rel: rel,
) -> DuckDBPyRelation | None:
    """One layer's owner-map rows `own` folded onto `parent`, the map below it.

    `schema` is the layer's own, which `parent` must already be extended to
    (`_widened`). The parent rows naming a label `removed` holds are dropped
    first, per `partial` dim; a label inside a struct is reached through its
    tuple, which the layer removed with it. A parent row a row of `own`
    covers, naming no `partial` dim the parent row leaves NULL, is dropped;
    one an own row crosses stays as written, and the layer owns an overlap
    row naming every label either names. `None` where neither side holds a
    row.

    Parameters
    ----------
    attribute
        The one attribute `parent` and `own` hold rows of, where they hold one.
    refuse
        The layer `own` is, where it is checked for leaving a dim outside
        `partial` unfilled: a frozen layer, as a workspace first folds it.
    keep
        Applied to `own`, the parent rows it leaves and the pairs the check
        reads, each read more than once: `_checkpoint` keeps them in tables.

    Raises
    ------
    ValueError
        Where `refuse` is given and a row of `own` leaves a dim outside
        `partial` NULL over a parent row naming it, and the layer holds no row
        naming it there.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [crossing rows](https://energy-models.github.io/datarecord/reference/resolution/#crossing-rows)
    - [one plan for every layer](https://energy-models.github.io/datarecord/reference/resolution/#folding-a-layer-onto-its-parent)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    if own is not None:
        own = keep(own)
    if parent is None:
        return own
    kept = _prune(parent, removed)
    if own is None:
        return kept
    kept = keep(_uncovered_by(kept, own, schema))
    if refuse is not None:
        within = keep(_meeting(kept, own, schema, attribute, _not_within))
        _refuse_unfilled(within, own, schema, refuse)
    if attribute is not None and _domain(schema, attribute).bit_count() < 2:
        return union_all_by_name([kept, own])
    if refuse is None:
        pairs = _meeting(kept, own, schema, attribute, _incomparable)
    else:
        pairs = within.filter(sql(_not_within("_o__m", "_m")))
    return union_all_by_name([kept, own, _overlaps(pairs, schema).distinct()])


def _prune(
    parent: DuckDBPyRelation, removed: Mapping[str, DuckDBPyRelation]
) -> DuckDBPyRelation:
    """`parent` without the rows naming a label `removed` holds, per `partial` dim."""
    for name, gone in removed.items():
        gone = gone.project(col(name).alias("_gone"))
        parent = parent.join(gone, col(name) == col("_gone"), how="anti")
    return parent


def _domain(schema: Schema, attribute: str) -> int:
    """The `partial` dims `attribute` is over, as a `named_mask` bitmask; every one for an undeclared attribute."""
    if attribute not in schema.attributes:
        return (1 << len(schema.partial_dims)) - 1
    over = schema.coordinates_of(attribute)
    return sum(1 << i for i, d in enumerate(schema.partial_dims) if d in over)


@_per_schema
def _domain_of(schema: Schema) -> Expression:
    """Each row's `_domain`, by a CASE over its attribute."""
    full = (1 << len(schema.partial_dims)) - 1
    by: dict[int, list[str]] = {}
    for name in schema.attributes:
        by.setdefault(_domain(schema, name), []).append(name)
    cases = [
        (col("attribute").isin(*map(lit, names)), lit(dom))
        for dom, names in by.items()
        if dom != full
    ]
    if not cases:
        return lit(full)
    (first, value), *rest = cases
    case = duckdb.CaseExpression(first, value)
    for when, then in rest:
        case = case.when(when, then)
    return case.otherwise(lit(full))


def _incomparable(p: str, o: str) -> str:
    """Whether a parent row naming `p` and an own row naming `o` can cross."""
    return f"({p} & {o}) <> {p} AND ({p} & {o}) <> {o}"


def _not_within(p: str, o: str) -> str:
    """Whether an own row naming `o` names a dim a parent row naming `p` leaves NULL."""
    return f"({o} & {p}) <> {o}"


def _choices(
    schema: Schema, attribute: str | None, allowed: Callable[[str, str], str]
) -> Expression:
    """Per row, the masks `x` within its attribute's domain that `allowed(_m, x)` admits.

    Computed per row rather than bound as a constant list, whose size grows
    as 4 to the number of `partial` dims.
    """
    dom = _domain_of(schema) if attribute is None else lit(_domain(schema, attribute))
    n = 1 << len(schema.partial_dims)
    return fn.list_filter(
        fn.range(lit(n)), sql(f"lambda x: (x & {dom}) = x AND {allowed('_m', 'x')}")
    )


def _expanded(
    rel: DuckDBPyRelation, schema: Schema, choices: Expression, prefix: str = ""
) -> DuckDBPyRelation:
    """`rel` with `_m` its row's named set, and one copy per mask in `choices`.

    Each copy holds its mask in `_x` and, in `_k_<dim>`, the row's label on
    each `partial` dim the mask names, NULL elsewhere: the key the row
    agrees with any row naming exactly that mask on. Every column is
    prefixed `prefix`.
    """
    dims = schema.partial_dims
    rel = rel.project(star(), named_mask(dims).alias("_m"))
    rel = rel.project(star(), fn.unnest(choices).alias("_x"))
    keys = [
        duckdb.CaseExpression(sql(f"(_x & {1 << i}) <> 0"), col(d)).alias(f"_k_{d}")
        for i, d in enumerate(dims)
    ]
    rel = rel.project(star(), *keys)
    return rel.project(*(col(c).alias(f"{prefix}{c}") for c in rel.columns))


def _uncovered_by(
    kept: DuckDBPyRelation, own: DuckDBPyRelation, schema: Schema
) -> DuckDBPyRelation:
    """`kept` without each row a row of `own` covers naming no `partial` dim it leaves NULL.

    Each row of `kept` is expanded into one copy per subset of its named
    set, keyed by its labels there, which an own row naming exactly that
    subset equals: one semi-join finds the covered rows, one anti-join drops
    them.
    """
    dims = schema.partial_dims
    probe = _expanded(
        kept.project(col("attribute"), *map(col, dims)),
        schema,
        sql("list_filter(range(_m + 1), lambda x: (x & _m) = x)"),
    )
    keys = own.project(
        col("attribute").alias("_o_attribute"),
        named_mask(dims).alias("_o__m"),
        *(col(d).alias(f"_o__k_{d}") for d in dims),
    )
    on = ex_all(
        [
            col("attribute") == col("_o_attribute"),
            col("_x") == col("_o__m"),
            *(_same(f"_k_{d}", f"_o__k_{d}") for d in dims),
        ]
    )
    covered = probe.join(keys, on, how="semi").project(
        *(col(c).alias(f"_c_{c}") for c in ("attribute", *dims))
    )
    gone = ex_all(_same(c, f"_c_{c}") for c in ("attribute", *dims))
    return kept.join(covered, gone, how="anti")


def _same(a: str, b: str) -> Expression:
    """NULL-safe equality of two columns, which DuckDB hashes."""
    return sql(f"{col(a)} IS NOT DISTINCT FROM {col(b)}")


def _meeting(
    kept: DuckDBPyRelation,
    own: DuckDBPyRelation,
    schema: Schema,
    attribute: str | None,
    allowed: Callable[[str, str], str],
) -> DuckDBPyRelation:
    """Each row of `kept` beside each row of `own` it shares a key with, where `allowed(named(parent), named(own))`.

    Two rows share a key where they agree on the dims both name. Each side
    is expanded into a copy per named set the other side's row may hold
    under `allowed`, keyed by its labels on the dims the two sets share; one
    equi-join on (attribute, the two named sets, those labels) then matches
    each pair exactly once. The own row's columns are prefixed `_o_`.
    """
    dims = schema.partial_dims
    left = _expanded(kept, schema, _choices(schema, attribute, allowed))
    right = _expanded(
        own, schema, _choices(schema, attribute, lambda o, p: allowed(p, o)), "_o_"
    )
    on = ex_all(
        [
            col("attribute") == col("_o_attribute"),
            col("_x") == col("_o__m"),
            col("_m") == col("_o__x"),
            *(_same(f"_k_{d}", f"_o__k_{d}") for d in dims),
        ]
    )
    return left.join(right, on)


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


def uncovered(
    rel: DuckDBPyRelation, staged: DuckDBPyRelation, columns: Sequence[str]
) -> DuckDBPyRelation:
    """`rel` without the rows a row of `staged` reaches, equal or NULL on each of `columns`.

    Split by which `columns` `staged` leaves NULL, each part an equi-join over
    the columns it names: a match on "NULL or equal" cannot hash.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    for mask in named_sets(staged, columns):
        part = staged.filter(names_exactly(columns, mask))
        rel = rel.set_alias("r").join(
            part.set_alias("s"),
            null_safe("r", "s", [c for c in columns if c in mask]),
            how="anti",
        )
    return rel


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
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
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


def _overlaps(pairs: DuckDBPyRelation, schema: Schema) -> DuckDBPyRelation:
    """The overlap row of each crossing pair: every label either row names, owned as the own row is.

    The own row's layer and flags go with it, so a value row of that layer
    finds it by the same NULL-safe join as its own key and takes the parent
    row's labels. It names a strict superset of both rows, which is what lets
    the broadcast rule pick it where they cross.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    return pairs.project(
        *_met(schema),
        *(
            col(f"_o_{c}").alias(c)
            for c in ("attribute", LAYER_COLUMN, "varies", "broadcast", "breakpoints")
        ),
    )


def _met(schema: Schema) -> list[Expression]:
    """A pair's common key: the own row's label on each dim it names, the parent row's elsewhere."""
    return [coalesce(col(f"_o_{d}"), col(d)).alias(d) for d in schema.partial_dims]


def _refuse_unfilled(
    pairs: DuckDBPyRelation, own: DuckDBPyRelation, schema: Schema, layer_id: UUID
) -> None:
    """Refuse a layer leaving a dim outside its `partial` NULL over a parent row naming it, unfilled.

    The layer must hold a row naming the dim at the key the two share.
    `pairs` is `_meeting` of the parent rows and `own` where the own row
    names a dim the parent row leaves NULL.

    Raises
    ------
    ValueError
        Naming the layer, the attribute, the dim and the key.

    Notes
    -----
    - [a dim outside `partial`](https://energy-models.github.io/datarecord/reference/resolution/#a-dim-outside-partial)
    """
    dims = schema.partial_dims
    others = [d for d in schema.broadcast_dims if d not in dims]
    if not others:
        return
    unset = lit(False)  # noqa: FBT003
    need = {d: _field("varies", d) & ~_field("_o_varies", d) for d in others}
    open_ = (
        pairs.filter(reduce(operator.or_, need.values()))
        .project(
            *_met(schema),
            col("attribute"),
            *(need[d].alias(f"_need_{d}") for d in others),
        )
        .set_alias("k")
    )
    filled = own.project(
        *(col(c) for c in ("attribute", *dims)),
        *(_field("varies", d).alias(f"_has_{d}") for d in others),
    ).set_alias("f")
    unfilled = open_.join(
        filled, null_safe("k", "f", ("attribute", *dims)), how="left"
    ).filter(
        reduce(
            operator.or_,
            (col(f"_need_{d}") & ~coalesce(col(f"_has_{d}"), unset) for d in others),
        )
    )
    rows = (
        unfilled.project(
            col("k", "attribute"),
            *(col("k", d).cast(sqltypes.VARCHAR) for d in dims),
            *(col(f"_need_{d}") for d in others),
        )
        .limit(1)
        .fetchall()
    )
    if not rows:
        return
    row = rows[0]
    attribute, *key = row[: 1 + len(dims)]
    dim = next(d for d, n in zip(others, row[1 + len(dims) :], strict=True) if n)
    at = {d: k for d, k in zip(dims, key, strict=True) if k is not None}
    msg = (
        f"layer {layer_id} leaves {dim!r} NULL for {attribute!r} at {at} "
        f"where a parent row names it; write it along the parent's {dim!r} "
        f"labels there, as `WorkingRecord.set` does"
    )
    raise ValueError(msg)


def _gained(was: Schema, now: Schema, dim: str) -> list[str]:
    """The attributes of `was` over `dim` under `now` but not under `was`."""
    return [
        a
        for a in was.attributes
        if dim in now.coordinates_of(a) and dim not in was.coordinates_of(a)
    ]


def owned_rows(source: LayerSource, attribute: str) -> DuckDBPyRelation | None:
    """`source`'s rows of `attribute`, which the owner map names it as owning.

    `None` only from a staging area, which may have dropped them since.

    Raises
    ------
    ValueError
        Where a frozen source holds no rows of `attribute`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    rel = source.attribute(attribute)
    if rel is None and source.frozen:
        msg = (
            f"the owner map names layer {source.layer_id} as owning rows of "
            f"{attribute!r}, which it holds no file of"
        )
        raise ValueError(msg)
    return rel


def _field(flag: str, dim: str) -> Expression:
    return col(flag)[dim]


def _widened(
    rel: DuckDBPyRelation,
    was: Schema,
    now: Schema,
    dead: Callable[[str], DuckDBPyRelation | None],
    owner: Callable[[UUID], LayerSource],
) -> DuckDBPyRelation:
    """The map `rel`, keyed and flagged under `was`, extended to `now`, which is `compatible_with` it.

    A dim new to the schema gains its field in both flag structs, false. A
    dim new to an attribute's `dims` sets `broadcast` on that attribute's
    rows, which hold NULL along it. A dim joining `partial` gains a key
    column, NULL on each row not varying along it, and each row varying along
    it is split (`_split`). `rel` itself where `now` changes none of these.

    Parameters
    ----------
    dead
        A dim joining `partial` to the labels removed along it as of `rel`'s
        node and not held again, `None` where there are none.
    owner
        The source a map row's `layer_uuid` names.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    if was == now:
        return rel
    joining = [d for d in now.partial_dims if d not in was.partial_dims]
    gained = {d: _gained(was, now, d) for d in now.broadcast_dims}
    unchanged = now.broadcast_dims == was.broadcast_dims
    if not joining and unchanged and not any(gained.values()):
        return rel
    varying = [d for d in joining if d in was.broadcast_dims]
    splits = reduce(
        operator.or_,
        (_field("varies", d) for d in varying),
        lit(False),  # noqa: FBT003
    )
    parts = []
    if varying and rel.aggregate([fn.bool_or(splits)]).fetchall()[0][0]:
        parts.append(_split(rel.filter(splits), was, now, joining, dead, owner))
        rel = rel.filter(~splits)

    def old(flag: str, d: str) -> Expression:
        return _field(flag, d) if d in was.broadcast_dims else lit(False)  # noqa: FBT003

    def broadcast(d: str) -> Expression:
        held = old("broadcast", d)
        if not gained[d]:
            return held
        return held | col("attribute").isin(*(lit(a) for a in gained[d]))

    extended = with_columns(now, rel, *joining).project(
        *(col(c) for c in (*now.owner_map_key, LAYER_COLUMN)),
        fn.struct_pack(**{d: old("varies", d) for d in now.broadcast_dims}).alias(
            "varies"
        ),
        fn.struct_pack(**{d: broadcast(d) for d in now.broadcast_dims}).alias(
            "broadcast"
        ),
        col("breakpoints"),
    )
    return union_all_by_name([extended, *parts])


def _split(
    rows: DuckDBPyRelation,
    was: Schema,
    now: Schema,
    joining: Sequence[str],
    dead: Callable[[str], DuckDBPyRelation | None],
    owner: Callable[[UUID], LayerSource],
) -> DuckDBPyRelation:
    """`rows`, each varying along a dim of `joining`, one per label its owning layer wrote there at its key.

    One NULL-safe join of each row's key, as its layer stored it, to that
    layer's file rows of its attribute. Each new key's flags are aggregated
    afresh from the file rows it names, as `owner_keys` does: inherited, a row
    holding a dim both ways would also join the layer's row NULL there and
    answer its label twice. A new key naming a label `dead` holds is dropped,
    since the read hid that row while the dim was outside `partial` and none
    will once it is a key column.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    keys = was.partial_dims
    key_cols = [f"_key_{k}" for k in keys]
    carried = ["_was_varies", "_was_broadcast"]
    filed = [*now.broadcast_dims, "attribute", LAYER_COLUMN, "breakpoint"]
    grouped = [*key_cols, *joining, "attribute", LAYER_COLUMN, *carried]
    pairs = rows.project(col(LAYER_COLUMN), col("attribute")).distinct().fetchall()
    attributes = tuple(sorted({a for _, a in pairs}))
    files = [
        _tagged(now, rel, layer)
        for layer, attribute in pairs
        if (rel := owned_rows(owner(layer), attribute)) is not None
    ]
    stored = rows.project(
        *(duckdb.CaseExpression(_field("varies", k), col(k)).alias(k) for k in keys),
        *(col(k).alias(c) for k, c in zip(keys, key_cols, strict=True)),
        col("attribute"),
        col(LAYER_COLUMN),
        col("varies").alias("_was_varies"),
        col("broadcast").alias("_was_broadcast"),
    ).set_alias("m")
    joined = stored.join(
        union_all_by_name(files).set_alias("f"),
        null_safe("m", "f", (LAYER_COLUMN, "attribute", *keys)),
    ).project(
        *(col("m", c).alias(c) for c in (*key_cols, *carried)),
        *(col("f", c).alias(c) for c in filed),
    )
    split = joined.aggregate([*map(col, grouped), *_flags(now, attributes)]).project(
        *(col(f"_key_{k}").alias(k) if k in keys else col(k) for k in now.partial_dims),
        *(col(c) for c in ("attribute", LAYER_COLUMN, *FLAG_COLUMNS)),
    )
    gone = {d: g for d in joining if (g := dead(d)) is not None}
    return _prune(split, gone)


def _fold_map(
    sources: Sequence[LayerSource],
    *,
    made: list[str] | None = None,
) -> DuckDBPyRelation:
    """A record's owner map, folded down over `sources`, checkpointed into `made` as `_fold_onto` does.

    Keyed by the last source's schema. The axes fold to a resolved copy
    instead (`Coords.resolve`). The deepest materialised source's map is the
    fold's *seed* rather than a step of it: it is already folded over
    everything at or below it, so it is read as the starting relation, cast
    and keyed by that node's schema, and only the sources above it are folded
    on top.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    schema, con = sources[-1].schema, sources[-1].workspace.con
    base, above = _base_and_above(sources, _fold_at_hand)
    rel: DuckDBPyRelation | None = None
    keyed, held, dead = schema, dict.fromkeys(_pruned(schema)), {}
    if base is not None:
        rel = cast_declared(base.coords.schema, base.owner_map)
        keyed, dead = base.coords.schema, base.coords.dead
        held = {name: base.coords.axes.get(name) for name in _pruned(schema)}
    return _fold_onto(rel, above, held, con, keyed=keyed, dead=dead, made=made)


def _fold_at_hand(source: LayerSource) -> Fold | None:
    """`source`'s node's `Fold`, materialised or held, `None` where neither."""
    return source.materialised() or Fold.held(source)


@contextmanager
def _checkpointed(sources: Sequence[LayerSource]) -> Iterator[DuckDBPyRelation]:
    """`_fold_map` with each source's fold kept in a temporary table, every one dropped on exit.

    The relation entered reads the last table, so it is read before exit.
    """
    made: list[str] = []
    try:
        yield _fold_map(sources, made=made)
    finally:
        for name in made:
            sources[-1].workspace.con.execute(f"DROP TABLE IF EXISTS {name}")


def _checkpoint(
    rel: DuckDBPyRelation, made: list[str], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """`rel` as a temporary table of `con`, its name appended to `made`.

    `con` alone sees it, so the fold that makes it reads and drops it there.
    """
    name = temp_table(con, rel, "fold")
    made.append(name)
    return con.table(name)


def _drop_but_last(made: list[str], con: DuckDBPyConnection) -> None:
    """Drop every table `made` names but the last, which stays named there."""
    for name in made[:-1]:
        con.execute(f"DROP TABLE {name}")
    del made[:-1]


def _pruned(schema: Schema) -> tuple[str, ...]:
    """The memberships whose removed labels prune the owner map: the `partial` dims.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    return schema.partial_dims


def _fold_onto(
    rel: DuckDBPyRelation | None,
    sources: Sequence[LayerSource],
    held: dict[str, DuckDBPyRelation | None],
    con: DuckDBPyConnection,
    *,
    keyed: Schema,
    dead: Mapping[str, DuckDBPyRelation],
    made: list[str] | None = None,
    attribute: str | None = None,
) -> DuckDBPyRelation:
    """Fold each of `sources` onto the owner map `rel`, keyed by `keyed`, `None` being no map yet.

    Each source folds under its own schema, the map extended to it first
    (`_widened`), so the map is keyed by the last source's schema, `keyed`
    where there is none. A source restating a membership removes the labels
    the previous restatement held and its own omits. `held` is that previous
    restatement per `_pruned` membership of the node's schema as of the first
    source, `None` where there is none, so a first restatement removes
    nothing. A membership outside a source's `partial` is no key column there,
    so its restatements are kept instead, beside `dead`, its labels removed as
    of the first source, until it joins `partial`.

    Where `made` is a list, each source's fold is kept in a table of its own
    before the next folds onto it, and so are the pieces of its fold read more
    than once (`fold_layer`). The table the map is read from is left named in
    `made`, for the caller to drop once it has read the map (`_checkpointed`).

    Where `attribute` is given, `rel` holds its rows alone and each source
    folds its rows of it alone, a plan that reads no staged row
    (`fold_layer`).

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    - [a fold starts from the nearest fold at hand](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    held = dict(held)
    past = {
        name: [r for r in (dead.get(name), axis) if r is not None]
        for name, axis in held.items()
    }

    def dead_at(name: str) -> DuckDBPyRelation | None:
        axis = held[name]
        return None if axis is None else _dead_keys(past[name], axis, name, fixed=False)

    def owner(layer: UUID) -> LayerSource:
        return source_for(sources, layer)

    def keep(rel: DuckDBPyRelation) -> DuckDBPyRelation:
        return rel if made is None else _checkpoint(rel, made, con)

    for source in sources:
        if rel is not None:
            rel = _widened(rel, keyed, source.schema, dead_at, owner)
        keyed = source.schema
        removed = {}
        for name, before in held.items():
            now = source.axis(name)
            if now is None:
                continue
            if name not in keyed.partial_dims:
                past[name].append(now)
            elif before is not None:
                removed[name] = removed_labels(before, now, name)
            held[name] = now
        own = _layer_owners(source, attribute)
        refuse = source.layer_id if source.frozen else None
        rel = fold_layer(rel, own, removed, keyed, attribute, refuse=refuse, keep=keep)
        if made is not None and rel is not None:
            rel = keep(rel)
            _drop_but_last(made, con)
    if rel is None:
        return _empty_relation(keyed, con, *keyed.owner_map_columns)
    return rel


def _layer_owners(
    source: LayerSource, attribute: str | None
) -> DuckDBPyRelation | None:
    """`source`'s owner-map rows, of `attribute` alone where given; `None` where it holds none.

    The rows a layer keeps (`LayerSource.owners`) where it keeps them, so the
    fold does not aggregate a staging area's rows once per use; a file
    layer's are aggregated from its rows.
    """
    if attribute is not None:
        kept = source.owners(attribute)
        if kept is not None:
            return kept
        rows = source.attribute(attribute)
        if rows is None:
            return None
        return owner_keys(rows, source.schema, source.layer_id, (attribute,))
    owned = [
        k for a in sorted(source.attributes()) if (k := source.owners(a)) is not None
    ]
    if owned:
        return union_all_by_name(owned)
    rows = source.all_attributes()
    if rows is None:
        return None
    held = tuple(sorted(source.attributes()))
    return owner_keys(rows, source.schema, source.layer_id, held)


def source_for(sources: Sequence[LayerSource], layer_uuid: UUID) -> LayerSource:
    """The source of `sources` stamped `layer_uuid`, else that layer of their store.

    A map materialised over a longer ancestry than `sources` names layers
    beyond them, whose files are still in their own directories, in the
    store of the first `ParquetLayer`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    for source in sources:
        if source.layer_id == layer_uuid:
            return source
    layer = next(s for s in sources if isinstance(s, ParquetLayer))
    return layer.sibling(layer_uuid)


def split_frozen(
    sources: Sequence[LayerSource],
) -> tuple[list[LayerSource], list[LayerSource]]:
    """Split `sources` into the leading ones that cannot change under a reader, then the rest.

    The one rule the source list has to carry: **the fold is materialised up to
    the last frozen source; everything after it stays a relation.** Everything a
    `Record` caches rests on layers being write-once, which a staging area is
    not - so a staged source ends the frozen ones, and a frozen one under it
    stays above without either needing to know about the other.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    """
    count = next((i for i, s in enumerate(sources) if not s.frozen), len(sources))
    return list(sources[:count]), list(sources[count:])


def _table(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    frozen_coords: Coords,
    attribute: str | None = None,
    *,
    in_frozen: bool = True,
) -> DuckDBPyRelation:
    """The owner map for `revision_id`, materialised as far as `frozen` allows.

    The frozen layers are folded once and kept as a connection-scoped table,
    which never needs invalidating since layers are write-once. Whatever follows
    it is folded on top per call and handed back as a relation, so a staging
    area's edits are picked up with no bookkeeping at all: a relation over a
    staging table reads whatever the table holds when it is collected.
    `frozen_coords` are the frozen layers' coords, which a restatement above
    them removes labels from. Where `attribute` is given, only its rows,
    folded one attribute at a time above the frozen layers (`_fold_onto`),
    and onto no map unless `in_frozen`, which says whether the frozen layers'
    map holds rows of it: a write-once table answers that for good.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    schema = sources[-1].schema
    frozen, above = split_frozen(sources)
    rel = _frozen_table(revision_id, frozen)
    keyed = frozen[-1].schema if frozen else schema
    if attribute is not None and rel is not None:
        rel = rel.filter(col("attribute") == lit(attribute)) if in_frozen else None
    held = {name: frozen_coords.axes.get(name) for name in _pruned(schema)}
    return _fold_onto(
        rel,
        above,
        held,
        sources[-1].workspace.con,
        keyed=keyed,
        dead=frozen_coords.dead,
        attribute=attribute,
    )


def _frozen_table(
    revision_id: UUID, sources: Sequence[LayerSource]
) -> DuckDBPyRelation | None:
    """The frozen layers' owner map, its own materialised one or a cached fold.

    `None` over no source, so that the tail folds onto no map.

    Keyed by `revision_id` rather than by the layers: they *are* what that
    node resolves from, a staged source only ever being appended after it.
    The map is keyed by the last source's schema, which `revision_id` fixes,
    so the table, `_fold."<id>".owner_map` of its database, is shared by every cursor on the
    workspace and valid for every reader of the node. A persisted map is
    cast, parquet not carrying the declared types.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    if not sources:
        return None
    last = sources[-1]
    con = last.workspace.con
    if isinstance(last, ParquetLayer) and last.layer_id == revision_id:
        rel = read_owner_map(revision_id, con, last.store)
        if rel is not None:
            return cast_declared(last.schema, rel)
    return shared_table(
        con,
        last.workspace.name,
        FOLD,
        revision_id,
        "owner_map",
        lambda: _checkpointed(sources),
    )
