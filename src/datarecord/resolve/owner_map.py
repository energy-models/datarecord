# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The owner map: which layer owns each map row, folded over a record's layers.

The fold over a record's frozen layers is kept in tables the workspace shares, the owner
map's and the axes' alike.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
- [a cursor per thread](https://energy-models.github.io/datarecord/reference/store/#threads)
"""

from __future__ import annotations

import operator
from collections.abc import (
    Callable,
    Collection,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import contextmanager
from functools import reduce
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
    null_pattern,
    null_safe,
    pattern,
    shared_table,
    temp_table,
    union_all_by_name,
)
from datarecord.resolve.axes import (
    Axes,
    _base_and_above,
    _dead_labels,
    removed_labels,
)
from datarecord.resolve.fold import Fold, read_owner_map
from datarecord.resolve.owner_map_rows import (
    SHAPE_COLUMN,
    Shape,
    _flags,
    _isin,
    _mask,
    _tagged,
    owner_map_rows,
    shape_of,
    shapes,
)
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import (
    FLAG_COLUMNS,
    LAYER_COLUMN,
    Schema,
    cast_declared,
    empty_declared,
    with_columns,
)


def fold_layer(
    parent: DuckDBPyRelation | None,
    own: DuckDBPyRelation | None,
    removed: Mapping[str, DuckDBPyRelation],
    schema: Schema,
    shape: Shape,
    *,
    refuse: UUID | None = None,
    keep: Callable[[DuckDBPyRelation], DuckDBPyRelation] = lambda rel: rel,
) -> DuckDBPyRelation | None:
    """One layer's owner-map rows `own` folded onto `parent` for attributes of a single `shape`.

    `schema` is the layer's own, which `parent` must already be extended to
    (`_widened`), and `shape` is under it. The parent rows naming a label
    `removed` holds are dropped first, per `partial` dimension; a label inside a struct is reached through its
    tuple, which the layer removed with it. A parent row a row of `own`
    covers, naming no `partial` dimension the parent row leaves NULL, is dropped;
    one an own row crosses stays as written, and the layer owns an overlap
    row naming every label either names. `None` where neither side holds a
    row.

    Parameters
    ----------
    refuse
        The layer `own` is, where it is checked for leaving a dimension outside
        `partial` unfilled: a frozen layer, as a workspace first folds it.
    keep
        Applied to `own`, the parent rows it leaves and the pairs the check
        reads, where each is read more than once: `_checkpoint` keeps them in
        tables.

    Raises
    ------
    ValueError
        Where `refuse` is given and a row of `own` leaves a dimension outside
        `partial` NULL over a parent row naming it, and the layer holds no row
        naming it there.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    - [crossing rows](https://energy-models.github.io/datarecord/reference/resolution/#crossing-rows)
    - [one plan for every layer](https://energy-models.github.io/datarecord/reference/resolution/#folding-a-layer-onto-its-parent)
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    if parent is None:
        return own
    kept = _prune(parent, removed)
    if own is None:
        return kept
    own = keep(own)
    kept = keep(_uncovered_by(kept, own, schema))
    crossing = pattern(schema.partial_dimensions, shape).bit_count() >= 2
    if refuse is not None:
        within = _meeting(kept, own, schema, shape, _not_within)
        if crossing and _outside(schema, shape):
            within = keep(within)
        _refuse_unfilled(within, own, schema, shape, refuse)
    if not crossing:
        return union_all_by_name([kept, own])
    if refuse is None:
        pairs = _meeting(kept, own, schema, shape, _incomparable)
    else:
        pairs = within.filter(sql(_not_within("_o__p", "_p")))
    return union_all_by_name([kept, own, _overlaps(pairs, schema).distinct()])


def _prune(
    parent: DuckDBPyRelation, removed: Mapping[str, DuckDBPyRelation]
) -> DuckDBPyRelation:
    """`parent` without the rows naming a label `removed` holds, per `partial` dimension."""
    for name, gone in removed.items():
        gone = gone.project(col(name).alias("_gone"))
        parent = parent.join(gone, col(name) == col("_gone"), how="anti")
    return parent


def _incomparable(p: str, o: str) -> str:
    """Whether a parent row naming `p` and an own row naming `o` can cross."""
    return f"({p} & {o}) <> {p} AND ({p} & {o}) <> {o}"


def _not_within(p: str, o: str) -> str:
    """Whether an own row naming `o` names a dimension a parent row naming `p` leaves NULL."""
    return f"({o} & {p}) <> {o}"


def _choices(
    schema: Schema, shape: Shape, allowed: Callable[[str, str], str]
) -> Expression:
    """Per row, the patterns `x` within `shape`'s domain that `allowed(_p, x)` admits.

    Computed per row rather than bound as a constant list, whose size grows
    as 4 to the number of `partial` dimensions.
    """
    dom = pattern(schema.partial_dimensions, shape)
    n = 1 << len(schema.partial_dimensions)
    return fn.list_filter(
        fn.range(lit(n)), sql(f"lambda x: (x & {dom}) = x AND {allowed('_p', 'x')}")
    )


def _expanded(
    rel: DuckDBPyRelation, schema: Schema, choices: Expression, prefix: str = ""
) -> DuckDBPyRelation:
    """`rel` with `_p` its row's pattern, and one copy per pattern in `choices`.

    Each copy holds its pattern in `_x` and, in `_k_<dim>`, the row's label on
    each `partial` dimension the pattern names, NULL elsewhere: the index values the
    row agrees with any row with exactly that pattern on. Every column is
    prefixed `prefix`.
    """
    dims = schema.partial_dimensions
    rel = rel.project(star(), null_pattern(dims).alias("_p"))
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
    """`kept` without each row a row of `own` covers naming no `partial` dimension it leaves NULL.

    Each row of `kept` is expanded into one copy per subset of the set of
    dimensions it sets, keyed by its labels there, which an own row naming exactly that
    subset equals: one semi-join finds the covered rows, one anti-join drops
    them.
    """
    dims = schema.partial_dimensions
    probe = _expanded(
        kept.project(col("attribute"), *map(col, dims)),
        schema,
        sql("list_filter(range(_p + 1), lambda x: (x & _p) = x)"),
    )
    keys = own.project(
        col("attribute").alias("_o_attribute"),
        null_pattern(dims).alias("_o__p"),
        *(col(d).alias(f"_o__k_{d}") for d in dims),
    )
    on = ex_all(
        [
            col("attribute") == col("_o_attribute"),
            col("_x") == col("_o__p"),
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
    shape: Shape,
    allowed: Callable[[str, str], str],
) -> DuckDBPyRelation:
    """Each row of `kept` beside each row of `own` it shares index values with, where `allowed(named(parent), named(own))`.

    Two rows share index values where they agree on the dimensions both name. Each side
    is expanded into a copy per pattern the other side's row may hold
    under `allowed`, keyed by its labels on the dimensions the two patterns share; one
    equi-join on (attribute, the two patterns, those labels) then matches
    each pair exactly once. The own row's columns are prefixed `_o_`.
    """
    dims = schema.partial_dimensions
    left = _expanded(kept, schema, _choices(schema, shape, allowed))
    right = _expanded(
        own, schema, _choices(schema, shape, lambda o, p: allowed(p, o)), "_o_"
    )
    on = ex_all(
        [
            col("attribute") == col("_o_attribute"),
            col("_x") == col("_o__p"),
            col("_p") == col("_o__x"),
            *(_same(f"_k_{d}", f"_o__k_{d}") for d in dims),
        ]
    )
    return left.join(right, on)


def _overlaps(pairs: DuckDBPyRelation, schema: Schema) -> DuckDBPyRelation:
    """The overlap row of each crossing pair: every label either row names, owned as the own row is.

    The own row's layer and flags go with it, so a value row of that layer
    finds it by the same NULL-safe join as its own map row and takes the parent
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
    """A pair's common index values: the own row's label on each dimension it names, the parent row's elsewhere."""
    return [
        coalesce(col(f"_o_{d}"), col(d)).alias(d) for d in schema.partial_dimensions
    ]


def _refuse_unfilled(
    pairs: DuckDBPyRelation,
    own: DuckDBPyRelation,
    schema: Schema,
    shape: Shape,
    layer_id: UUID,
) -> None:
    """Refuse a layer leaving a dimension outside its `partial` NULL over a parent row naming it, unfilled.

    The layer must hold a row naming the dimension at the index values the two share.
    `pairs` is `_meeting` of the parent rows and `own` where the own row
    names a dimension the parent row leaves NULL.

    Raises
    ------
    ValueError
        Naming the layer, the attribute, the dimension and the labels the two share.

    Notes
    -----
    - [a dimension outside `partial`](https://energy-models.github.io/datarecord/reference/resolution/#a-dimension-outside-partial)
    """
    dims = schema.partial_dimensions
    others = _outside(schema, shape)
    if not others:
        return
    unset = lit(False)  # noqa: FBT003
    need = {d: _flag("varies", d) & ~_flag("_o_varies", d) for d in others}
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
        *(_flag("varies", d).alias(f"_has_{d}") for d in others),
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
    attribute, *labels = row[: 1 + len(dims)]
    dim = next(d for d, n in zip(others, row[1 + len(dims) :], strict=True) if n)
    at = {d: k for d, k in zip(dims, labels, strict=True) if k is not None}
    msg = (
        f"layer {layer_id} leaves {dim!r} NULL for {attribute!r} at {at} "
        f"where a parent row names it; write it along the parent's {dim!r} "
        f"labels there, as `WorkingRecord.set` does"
    )
    raise ValueError(msg)


def _outside(schema: Schema, shape: Shape) -> list[str]:
    """The dimensions of `shape` outside `partial`."""
    return [
        d
        for d in schema.dimensions
        if d in shape and d not in schema.partial_dimensions
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


def _flag(flag: str, dim: str) -> Expression:
    return col(flag)[dim]


def _widened(
    maps: dict[Shape, DuckDBPyRelation],
    was: Schema,
    now: Schema,
    dead: Callable[[str], DuckDBPyRelation | None],
    owner: Callable[[UUID], LayerSource],
) -> dict[Shape, DuckDBPyRelation]:
    """The map `maps`, one relation per shape under `was`, extended to `now`, which is `compatible_with` it, one per shape under `now`.

    An attribute whose shape `now` changes moves to its new shape's relation
    (`_moves`), and each part moved is extended (`_extended`). `maps` itself
    where `now` is `was`.

    Parameters
    ----------
    dead
        A dimension joining `partial` to the labels removed along it as of the
        map's node and not held again, `None` where there are none.
    owner
        The source a map row's `layer_uuid` names.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    if was == now:
        return maps
    joining = [d for d in now.partial_dimensions if d not in was.partial_dimensions]
    moved: dict[Shape, list[DuckDBPyRelation]] = {}
    for shape, rel in maps.items():
        for to, part in _moves(rel, shape, was, now).items():
            extended = _extended(part, shape, to, was, now, joining, dead, owner)
            moved.setdefault(to, []).append(extended)
    return {to: union_all_by_name(parts) for to, parts in moved.items()}


def _moves(
    rel: DuckDBPyRelation, shape: Shape, was: Schema, now: Schema
) -> dict[Shape, DuckDBPyRelation]:
    """`rel`, the map's rows of `shape` under `was`, split by each attribute's shape under `now`.

    `rel` itself where they share one. Otherwise each part names its
    attributes in an IN-list: what is split is a map a schema reshapes, never
    a fold's step.
    """
    targets = shapes(now, (a for a in was.attributes if shape_of(was, a) == shape))
    if len(targets) == 1:
        return dict.fromkeys(targets, rel)
    return {to: rel.filter(_isin(names)) for to, names in targets.items()}


def _by_shape(
    rel: DuckDBPyRelation, schema: Schema, attributes: Iterable[str] | None = None
) -> dict[Shape, DuckDBPyRelation]:
    """The map `rel`, one relation `_stamped` under `schema`, one relation per shape: of `attributes` alone, or of every attribute where `None`."""
    if attributes is None:
        every = shapes(schema, schema.attributes)
        return {shape: _of_shape(rel, schema, shape) for shape in every}
    grouped = shapes(schema, attributes)
    return {s: _of_shape(rel, schema, s).filter(_isin(n)) for s, n in grouped.items()}


def _of_shape(rel: DuckDBPyRelation, schema: Schema, shape: Shape) -> DuckDBPyRelation:
    """The rows of `shape` of the map `rel`, `_stamped` under `schema`, unstamped."""
    return _unstamped(rel.filter(col(SHAPE_COLUMN) == _mask(schema, shape)))


def _as_one(
    maps: Mapping[Shape, DuckDBPyRelation], schema: Schema, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """The map `maps`, one relation per shape under `schema`, as one."""
    if not maps:
        return empty_declared(schema, con, *schema.owner_map_columns)
    return union_all_by_name(list(maps.values()))


def _stamped(
    maps: Mapping[Shape, DuckDBPyRelation], keyed: Schema, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """`_as_one`, each row's shape in `SHAPE_COLUMN`, where the map is kept as one table or file.

    One arm per shape, in ascending `SHAPE_COLUMN`, which DuckDB writes to
    row groups of their own, so a fold reading one shape skips the others
    (`_by_shape`).
    """
    if not maps:
        maps = {frozenset(): empty_declared(keyed, con, *keyed.owner_map_columns)}
    arms = [
        maps[shape].project(star(), _mask(keyed, shape).alias(SHAPE_COLUMN))
        for shape in sorted(maps, key=lambda shape: pattern(keyed.dimensions, shape))
    ]
    return union_all_by_name(arms)


def _unstamped(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """The map `rel` as a caller reads it, without `SHAPE_COLUMN`."""
    return rel.project(star(exclude=[SHAPE_COLUMN]))


def _extended(
    rel: DuckDBPyRelation,
    shape: Shape,
    to: Shape,
    was: Schema,
    now: Schema,
    joining: Sequence[str],
    dead: Callable[[str], DuckDBPyRelation | None],
    owner: Callable[[UUID], LayerSource],
) -> DuckDBPyRelation:
    """`rel`, map rows of attributes of `shape` under `was` and of `to` under `now`, extended to `now`.

    A dimension new to the schema gains its entry in both flag structs, false.
    A dimension of `to` outside `shape`, new to the attributes' `dims`, sets
    `broadcast`: the rows hold NULL along it. A dimension of `joining`, joining
    `partial`, gains an index column, NULL on each row not varying along it,
    and each row varying along it is split (`_split`). `rel` itself where
    `now` changes none of these.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    gained = to - shape
    if not joining and not gained and list(now.dimensions) == list(was.dimensions):
        return rel
    varying = [d for d in joining if d in shape]
    splits = reduce(
        operator.or_,
        (_flag("varies", d) for d in varying),
        lit(False),  # noqa: FBT003
    )
    parts = []
    if varying and rel.aggregate([fn.bool_or(splits)]).fetchall()[0][0]:
        parts.append(_split(rel.filter(splits), was, now, joining, to, dead, owner))
        rel = rel.filter(~splits)

    def old(flag: str, d: str) -> Expression:
        return _flag(flag, d) if d in was.dimensions else lit(False)  # noqa: FBT003

    def broadcast(d: str) -> Expression:
        return lit(True) if d in gained else old("broadcast", d)  # noqa: FBT003

    extended = with_columns(now, rel, *joining).project(
        *(col(c) for c in (*now.owner_map_index, LAYER_COLUMN)),
        fn.struct_pack(**{d: old("varies", d) for d in now.dimensions}).alias("varies"),
        fn.struct_pack(**{d: broadcast(d) for d in now.dimensions}).alias("broadcast"),
        col("breakpoints"),
    )
    return union_all_by_name([extended, *parts])


def _split(
    rows: DuckDBPyRelation,
    was: Schema,
    now: Schema,
    joining: Sequence[str],
    shape: Shape,
    dead: Callable[[str], DuckDBPyRelation | None],
    owner: Callable[[UUID], LayerSource],
) -> DuckDBPyRelation:
    """`rows`, each varying along a dimension of `joining`, one per label its owning layer wrote there at its index values.

    The rows are of attributes of `shape` under `now`.

    One NULL-safe join of each row's index values, as its layer stored them, to that
    layer's file rows of its attribute. Each new map row's flags are aggregated
    afresh from the file rows it names, as `owner_map_rows` does: inherited, a row
    holding a dimension both ways would also join the layer's row NULL there and
    answer its label twice. A new map row naming a label `dead` holds is dropped,
    since the read hid that row while the dimension was outside `partial` and none
    will once it is an index column.

    Notes
    -----
    - [extending the map to a wider schema](https://energy-models.github.io/datarecord/reference/resolution/#a-wider-schema)
    """
    was_dims = was.partial_dimensions
    was_cols = [f"_was_{d}" for d in was_dims]
    carried = ["_was_varies", "_was_broadcast"]
    filed = [*now.dimensions, "attribute", LAYER_COLUMN, "breakpoint"]
    grouped = [*was_cols, *joining, "attribute", LAYER_COLUMN, *carried]
    pairs = rows.project(col(LAYER_COLUMN), col("attribute")).distinct().fetchall()
    files = [
        _tagged(now, rel, layer)
        for layer, attribute in pairs
        if (rel := owned_rows(owner(layer), attribute)) is not None
    ]
    stored = rows.project(
        *(duckdb.CaseExpression(_flag("varies", d), col(d)).alias(d) for d in was_dims),
        *(col(d).alias(c) for d, c in zip(was_dims, was_cols, strict=True)),
        col("attribute"),
        col(LAYER_COLUMN),
        col("varies").alias("_was_varies"),
        col("broadcast").alias("_was_broadcast"),
    ).set_alias("m")
    joined = stored.join(
        union_all_by_name(files).set_alias("f"),
        null_safe("m", "f", (LAYER_COLUMN, "attribute", *was_dims)),
    ).project(
        *(col("m", c).alias(c) for c in (*was_cols, *carried)),
        *(col("f", c).alias(c) for c in filed),
    )
    split = joined.aggregate([*map(col, grouped), *_flags(now, shape)]).project(
        *(
            col(f"_was_{d}").alias(d) if d in was_dims else col(d)
            for d in now.partial_dimensions
        ),
        *(col(c) for c in ("attribute", LAYER_COLUMN, *FLAG_COLUMNS)),
    )
    gone = {d: g for d in joining if (g := dead(d)) is not None}
    return _prune(split, gone)


def _fold_map(
    sources: Sequence[LayerSource],
    *,
    made: dict[str, DuckDBPyRelation] | None = None,
) -> dict[Shape, DuckDBPyRelation]:
    """A record's owner map, folded down over `sources`, checkpointed into `made` as `_fold_onto` does.

    One relation per shape under the last source's schema. The axes fold to a
    resolved copy instead (`Axes.resolve`). The deepest materialised source's
    map is the fold's *seed* rather than a step of it: it is already folded
    over everything at or below it, so it is read as the starting relation,
    cast and keyed by that node's schema, and only the sources above it are
    folded on top.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    schema, con = sources[-1].schema, sources[-1].workspace.con
    base, above = _base_and_above(sources, _fold_at_hand)
    maps: dict[Shape, DuckDBPyRelation] = {}
    keyed, held, dead = schema, dict.fromkeys(_pruned(schema)), {}
    if base is not None:
        keyed, dead = base.axes.schema, base.axes.dead
        maps = _by_shape(cast_declared(keyed, base.owner_map), keyed)
        held = {name: base.axes.get(name) for name in _pruned(schema)}
    return _fold_onto(maps, above, held, con, keyed=keyed, dead=dead, made=made)


def _fold_at_hand(source: LayerSource) -> Fold | None:
    """`source`'s node's `Fold`, materialised or held, `None` where neither."""
    return source.materialised() or Fold.held(source)


@contextmanager
def _checkpointed(sources: Sequence[LayerSource]) -> Iterator[DuckDBPyRelation]:
    """`_fold_map` with each source's fold but the last kept in a temporary table per shape, every one dropped on exit, `_stamped`.

    The relation entered reads those tables, so it is read once, before exit.
    """
    made: dict[str, DuckDBPyRelation] = {}
    schema, con = sources[-1].schema, sources[-1].workspace.con
    try:
        maps = _fold_map(sources, made=made)
        yield _stamped(maps, schema, con)
    finally:
        for name in made:
            con.execute(f"DROP TABLE IF EXISTS {name}")


def _checkpoint(
    rel: DuckDBPyRelation, made: dict[str, DuckDBPyRelation], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """`rel` as a temporary table of `con`, `made` mapping its name to the relation returned.

    `con` alone sees it, so the fold that makes it reads and drops it there.
    """
    name = temp_table(con, rel, "fold")
    made[name] = con.table(name)
    return made[name]


def _drop_unread(
    made: dict[str, DuckDBPyRelation],
    read: Iterable[DuckDBPyRelation],
    con: DuckDBPyConnection,
) -> None:
    """Drop each table `made` records whose relation is none of `read`, the rest staying recorded there.

    Matched by identity rather than alias, which a projection inherits from
    the table it reads; `made` holding each relation keeps its identity its own.
    """
    read_ids = {id(rel) for rel in read}
    for name in [n for n, rel in made.items() if id(rel) not in read_ids]:
        con.execute(f"DROP TABLE {name}")
        del made[name]


def _pruned(schema: Schema) -> tuple[str, ...]:
    """The memberships whose removed labels prune the owner map: the `partial` dimensions.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    return schema.partial_dimensions


def _fold_onto(
    maps: dict[Shape, DuckDBPyRelation],
    sources: Sequence[LayerSource],
    held: dict[str, DuckDBPyRelation | None],
    con: DuckDBPyConnection,
    *,
    keyed: Schema,
    dead: Mapping[str, DuckDBPyRelation],
    made: dict[str, DuckDBPyRelation] | None = None,
    attributes: Collection[str] | None = None,
) -> dict[Shape, DuckDBPyRelation]:
    """Fold each of `sources` onto the owner map `maps`, one relation per shape under `keyed`, into one per shape under the last source's schema.

    Each source folds under its own schema, the map extended to it first
    (`_widened`), so the map is keyed by the last source's schema, `keyed`
    where there is none. The map stays one relation per shape under the
    source's schema, and each shape folds on its own (`fold_layer`). A source
    restating a membership removes the labels the previous restatement held
    and its own omits. `held` is that previous restatement per `_pruned` membership of
    the node's schema as of the first source, `None` where there is none, so
    a first restatement removes nothing. A membership outside a source's
    `partial` is no index column there, so its restatements are kept
    instead, beside `dead`, its labels removed as of the first source, until
    it joins `partial`.

    Where `made` is given, each source's fold but the last is kept in a table
    per shape before the next folds onto it, and so are the labels it removes
    and the pieces of its fold read more than once (`fold_layer`); a relation
    already a table is not copied. The tables the map is read from are left
    recorded in `made`, for the caller to drop once it has read the map
    (`_checkpointed`).

    Where `attributes` is given, `maps` holds their rows alone and each source
    folds its rows of them alone, a plan that reads no staged row
    (`_layer_owner_map_rows`).

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
        return (
            None if axis is None else _dead_labels(past[name], axis, name, fixed=False)
        )

    def owner(layer: UUID) -> LayerSource:
        return source_for(sources, layer)

    def keep(rel: DuckDBPyRelation) -> DuckDBPyRelation:
        if made is None or rel.type == "TABLE_RELATION":
            return rel
        return _checkpoint(rel, made, con)

    for source in sources:
        maps = _widened(maps, keyed, source.schema, dead_at, owner)
        keyed = source.schema
        removed = {}
        for name, before in held.items():
            now = source.axis(name)
            if now is None:
                continue
            if name not in keyed.partial_dimensions:
                past[name].append(now)
            elif before is not None:
                removed[name] = keep(removed_labels(before, now, name))
            held[name] = now
        owned = _layer_owner_map_rows(
            source, source.attributes() if attributes is None else attributes
        )
        refuse = source.layer_id if source.frozen else None
        folded = {}
        for shape in dict.fromkeys([*maps, *owned]):
            rel = fold_layer(
                maps.get(shape),
                owned.get(shape),
                removed,
                keyed,
                shape,
                refuse=refuse,
                keep=keep,
            )
            if rel is not None:
                folded[shape] = rel
        if made is not None and source is not sources[-1]:
            folded = {shape: keep(rel) for shape, rel in folded.items()}
            _drop_unread(made, folded.values(), con)
        maps = folded
    return maps


def _layer_owner_map_rows(
    source: LayerSource, attributes: Iterable[str]
) -> dict[Shape, DuckDBPyRelation]:
    """`source`'s owner-map rows of `attributes`, one relation per shape under its schema, none for a shape it holds no row of.

    The rows a layer keeps (`LayerSource.owner_map_rows`) where it keeps them, else
    aggregated from its rows, read at once per shape.
    """
    schema, owned = source.schema, {}
    for shape, names in shapes(schema, attributes).items():
        rel = source.owner_map_rows(shape, names)
        if rel is None and (rows := source.rows(names)) is not None:
            rel = owner_map_rows(rows, schema, source.layer_id, shape)
        if rel is not None:
            owned[shape] = rel
    return owned


def stored_owner_map_rows(
    written: LayerSource, owner_rows_from: LayerSource | None
) -> tuple[DuckDBPyRelation, tuple[int, ...]] | None:
    """The rows the `owner_map_rows.parquet` of the layer `written` holds and their `SHAPE_COLUMN` values, ascending; `None` where it holds none.

    Every owner-map row of each shape in which a row of `written` names a
    dimension outside `partial`, `_stamped` under `written`'s schema and
    without `layer_uuid`, which a read stamps as the layer it reads. A shape
    whose rows name `partial` dimensions alone has an owner row per row, which
    a fold aggregates as fast as it reads them.

    Parameters
    ----------
    written
        The layer's files as written, read as one layer.
    owner_rows_from
        What `written` was written from, whose owner rows are taken where it
        keeps them, as a staging area does; `None` aggregates them from
        `written`'s files.

    Notes
    -----
    - [`owner_map_rows.parquet`](https://energy-models.github.io/datarecord/reference/format/#owner_map_rowsparquet)
    """
    schema = written.schema
    kept = written if owner_rows_from is None else owner_rows_from
    grouped = shapes(schema, written.attributes())
    stored = {s: n for s, n in grouped.items() if _named_outside(written, s, n)}
    if not stored:
        return None
    names = [a for n in stored.values() for a in n]
    owned = _stamped(_layer_owner_map_rows(kept, names), schema, written.workspace.con)
    masks = tuple(sorted(pattern(schema.dimensions, s) for s in stored))
    return owned.project(star(exclude=[LAYER_COLUMN])), masks


def _named_outside(source: LayerSource, shape: Shape, names: Sequence[str]) -> bool:
    """Whether a row of `source`'s attributes `names`, of one `shape`, names a dimension of it outside `partial`."""
    outside = _outside(source.schema, shape)
    if not outside:
        return False
    rows = source.rows(names)
    if rows is None:
        return False
    named = reduce(operator.or_, (col(d).isnotnull() for d in outside))
    return bool(rows.filter(named).limit(1).fetchall())


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
    frozen_map: DuckDBPyRelation | None,
    sources: Sequence[LayerSource],
    frozen_axes: Axes,
    attributes: Mapping[str, bool] | None = None,
) -> DuckDBPyRelation:
    """The owner map over `sources`, folded onto the frozen layers' `frozen_map` (`_frozen_table`), as a caller reads it (`_unstamped`).

    Whatever follows the frozen layers is folded on top per call and handed
    back as a relation, so a staging area's edits are picked up with no
    bookkeeping at all: a relation over a staging table reads whatever the
    table holds when it is collected. `frozen_map` is `None` where no source
    is frozen. `frozen_axes` are the frozen layers' axes, which a
    restatement above them removes labels from.

    Parameters
    ----------
    attributes
        Where given, the map of these attributes alone, folded above the
        frozen layers (`_fold_onto`) onto the frozen layers' rows of those
        mapped to `True`, the attributes their map holds rows of: a write-once
        table answers that for good, and a shape none of whose attributes it
        holds folds onto no map.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    frozen, above = split_frozen(sources)
    rel = frozen_map
    if rel is not None and attributes is None and not above:
        return _unstamped(rel)
    schema, con = sources[-1].schema, sources[-1].workspace.con
    keyed = frozen[-1].schema if frozen else schema
    maps: dict[Shape, DuckDBPyRelation] = {}
    if rel is not None:
        held_by = (
            None if attributes is None else [a for a, h in attributes.items() if h]
        )
        maps = _by_shape(rel, keyed, held_by)
    held = {name: frozen_axes.get(name) for name in _pruned(schema)}
    maps = _fold_onto(
        maps,
        above,
        held,
        con,
        keyed=keyed,
        dead=frozen_axes.dead,
        attributes=attributes,
    )
    return _as_one(maps, schema, con)


def _frozen_table(
    revision_id: UUID, sources: Sequence[LayerSource]
) -> DuckDBPyRelation:
    """The frozen layers' owner map, its own materialised one or a cached fold, `_stamped`.

    The frozen layers are folded once and kept as a connection-scoped table,
    which never needs invalidating since layers are write-once. Each call
    asks the catalog or the store for it, so a caller keeps the relation
    rather than calling again (`Record._frozen_map`).

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
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
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
