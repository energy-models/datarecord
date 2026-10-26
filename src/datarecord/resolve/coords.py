# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The folded axes and dead labels of a record's layers.

Notes
-----
- [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
- [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import reduce

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb.sqltypes import VARCHAR

from datarecord.duck import fn
from datarecord.resolve.fold import Fold
from datarecord.resolve.sources import LayerSource
from datarecord.schema.schema import Schema


def _fold_axis(
    seed: DuckDBPyRelation | None,
    files: Sequence[tuple[LayerSource, DuckDBPyRelation]],
    key: str,
    file: str,
) -> DuckDBPyRelation | None:
    """Return the deepest of `files`, else `seed`: one node's resolved axis.

    The one fold for every axis, a multi-dim's included: a layer that changes
    one restates it whole, so the deepest restatement is the answer, in its
    own row order. `files` is root first. A frozen file holding `key` twice is
    refused; a staging area's table is keyed, so it holds no key twice.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    """
    if not files:
        return seed
    source, rel = files[-1]
    if source.frozen:
        refuse_twice(rel, [key], file)
    return rel


def refuse_twice(
    rel: DuckDBPyRelation,
    keys: Sequence[str],
    file: str,
    why: str = "an axis file holds each of its labels once "
    "(https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)",
) -> None:
    """Refuse `file`, read as `rel`, holding two rows that agree on every one of `keys`.

    NULL agrees with NULL.

    Raises
    ------
    ValueError
        Naming the values both rows hold, how many rows hold them, and `why`.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    """
    key = ", ".join(str(col(k)) for k in keys)
    twin = (
        rel.aggregate(f"{key}, count(*) AS _rows", key)
        .filter(col("_rows") > lit(1))
        .limit(1)
        .fetchone()
    )
    if twin is None:
        return
    *values, rows = twin
    at = ", ".join(
        f"{k}={v}" for k, v in zip(keys, values, strict=True) if v is not None
    )
    msg = f"{file} holds {rows} rows at {at or 'every coordinate'}; {why}"
    raise ValueError(msg)


def refuse_unheld(
    rows: DuckDBPyRelation,
    schema: Schema,
    columns: Iterable[str],
    held: Callable[[str], DuckDBPyRelation | None],
    where: str,
    *,
    staged: bool,
    subject: str | None = None,
) -> None:
    """Refuse a label or tuple in `rows` that the axis holding it lacks.

    A dim's column of `columns` names labels of that dim, a multi-dim's column
    tuples of that multi-dim and, through each coordinate, labels of the dim it
    draws on, and the column of a property of `subject` that refers to a dim
    names labels of that dim. Any other column is not checked. `subject`'s own
    column holds the labels or tuples being added, so of it only a multi-dim's
    coordinates are checked.
    Labels are checked before tuples, so a tuple naming an unheld label is
    refused for that label. NULL is no label and passes.

    Parameters
    ----------
    held
        Returns the axis of a name, `None` where there is none:
        as staged where `staged`, else the layer's final one.
    where
        What names the labels, to start the message with.

    Raises
    ------
    KeyError
        Where `staged`, naming the dim and up to three such labels
        or tuples.
    ValueError
        Where not `staged`, naming the same.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/use/editing/#validation)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
    """
    taken = {n: d for (s, n), d in schema.one_of_properties().items() if s == subject}
    labels: dict[str, tuple[Expression, str]] = {}
    fields: dict[str, tuple[Expression, str]] = {}
    tuples: dict[str, tuple[Expression, str]] = {}
    for c in columns:
        if c in taken:
            labels[f"{subject}.{c}"] = (col(c), taken[c])
        elif c in schema.multi_dims:
            fields |= {
                f"{c}.{f}": (fn.struct_extract(col(c), lit(f)), d)
                for f, d in schema.over(c).items()
            }
            if c != subject:
                tuples[c] = (col(c), c)
        elif c in schema.dimensions and c != subject:
            labels[c] = (col(c), c)
    named = labels | fields | tuples
    unheld = _first_unheld(rows, {k: (e, held(n), n) for k, (e, n) in named.items()})
    if unheld is None:
        return
    key, found = unheld
    name = named[key][1]
    multi = name in schema.multi_dims
    shown = (
        f"{name!r} tuples [{', '.join(found)}]" if multi else f"{name!r} labels {found}"
    )
    via = "" if key == name else f" in {key!r}"
    if staged:
        holder = "the staged axis"
        if schema.dimensions[name].one_of is not None:
            remedy = "that dim has `one_of`, so use one of its labels"
        else:
            remedy = "`add` them first"
        link = "use/editing/#validation"
    else:
        holder = f"the layer's dims/{name}.parquet"
        remedy = "add them there or drop those rows"
        link = "use/writing/"
    msg = (
        f"{where} names {shown}{via}, which {holder} does not hold; {remedy} "
        f"(https://energy-models.github.io/datarecord/design/{link})"
    )
    raise (KeyError if staged else ValueError)(msg)


def _first_unheld(
    rows: DuckDBPyRelation,
    named: Mapping[str, tuple[Expression, DuckDBPyRelation | None, str]],
) -> tuple[str, list[str]] | None:
    """Return the first name in `named` whose labels in `rows` its relation lacks, with up to three of them.

    `named` maps a name to the expression over `rows` giving a label or a
    tuple, the axis relation that holds them, and that relation's
    key column. A `None` relation holds nothing. NULL is no label and
    passes. The labels come back as text, in sorted order. `rows` is
    anti-joined against every relation in one query.
    """
    parts = []
    for i, (label, held, key) in enumerate(named.values()):
        part = rows.project(label.alias("_label")).filter(col("_label").isnotnull())
        if held is not None:
            part = part.set_alias("r").join(
                held.project(col(key).alias("_held")).set_alias("h"),
                col("r", "_label") == col("h", "_held"),
                how="anti",
            )
        parts.append(
            part.project(
                lit(i).alias("_i"), col("_label").cast(VARCHAR).alias("_label")
            )
        )
    if not parts:
        return None
    first = (
        reduce(DuckDBPyRelation.union, parts)
        .aggregate("_i, list(DISTINCT _label ORDER BY _label)[1:3] AS _labels", "_i")
        .order("_i")
        .limit(1)
        .fetchone()
    )
    if first is None:
        return None
    return list(named)[first[0]], first[1]


def _dead_keys(
    held: Sequence[DuckDBPyRelation],
    final: DuckDBPyRelation,
    key: str,
) -> DuckDBPyRelation | None:
    """Return the labels of `held` that `final` lacks, `None` where there is none.

    `held` is every set of labels the coordinate had: the base's dead labels
    and resolved axis, and each restatement above the base. A label one of
    them holds and the final axis lacks is one some layer removed. `None`
    rather than an empty relation, so `_living` adds no join for it.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    labels = [rel.project(col(key)) for rel in held]
    union = reduce(DuckDBPyRelation.union, labels).distinct().set_alias("h")
    dead = union.join(
        final.project(col(key)).set_alias("f"),
        col("h", key) == col("f", key),
        how="anti",
    )
    # fetchone on a hit keeps the parquet open, and Windows then refuses the staging rename
    if not dead.limit(1).fetchall():
        return None
    return dead


def _base_and_above(
    sources: Sequence[LayerSource], con: DuckDBPyConnection, schema: Schema
) -> tuple[Fold | None, list[LayerSource]]:
    """Split `sources` at the deepest materialised one: its `Fold`, then the rest.

    The deepest materialised source's `Fold` is the fold's base - already folded
    over everything at or below it - and only the sources above it are folded on
    top. No materialised source means no base and the whole list folds from the
    root.

    The last source is never the base: a node resolves from its own layer, never
    from its own cache. Reading a node through its own materialised `Fold` would
    return the cached answer instead of resolving it, and folding *nothing* on
    top - the same reason `sources_to_read` truncates only at proper ancestors.
    """
    for i in range(len(sources) - 2, -1, -1):
        base = sources[i].materialised(con, schema)
        if base is not None:
            return base, list(sources[i + 1 :])
    return None, list(sources)


def resolve_coords(
    schema: Schema,
    sources: Sequence[LayerSource],
    con: DuckDBPyConnection,
    split: tuple[Fold | Coords | None, list[LayerSource]] | None = None,
) -> Coords:
    """Resolve every dim `schema` declares, multi-dims among them, and the dead labels.

    Parameters
    ----------
    schema
        The record's schema, which declares the dims and their keys.
    sources : sequence of LayerSource
        Root first, ending in the layer being resolved.
    con : DuckDBPyConnection
    split
        A folded seed and the sources to fold onto it, where the caller
        already holds them: `_base_and_above(sources, ...)`, or a frozen
        prefix's coords and the sources after it.

    Returns
    -------
    Coords

    Raises
    ------
    ValueError
        If the deepest file of an axis holds one label twice.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    base, above = split or _base_and_above(sources, con, schema)
    listed = [source.axes() for source in above]
    keep = mortal(schema)
    axes: dict[str, DuckDBPyRelation] = {}
    dead: dict[str, DuckDBPyRelation] = {}
    for name in schema.dimensions:
        files = [
            (s, rel)
            for s, held in zip(above, listed, strict=True)
            if name in held and (rel := s.axis(name)) is not None
        ]
        seed = None if base is None else base.axes.get(name)
        rel = _fold_axis(seed, files, name, f"dims/{name}.parquet")
        if rel is None:
            continue
        axes[name] = rel
        if name not in keep:
            continue
        before = None if base is None else base.dead.get(name)
        if files:
            held = [r for r in (before, seed, *(r for _, r in files)) if r is not None]
            before = _dead_keys(held, rel, name)
        if before is not None:
            dead[name] = before
    return Coords(schema=schema, axes=axes, dead=dead)


def resolve_one(
    schema: Schema, sources: Sequence[LayerSource], con: DuckDBPyConnection, name: str
) -> DuckDBPyRelation | None:
    """Return the resolved axis `name` over `sources`, `None` where none has it.

    The deepest file for it, or the resolved one of the deepest materialised
    source above that file.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    for source in reversed(sources):
        rel = source.axis(name)
        if rel is not None:
            return rel
        fold = source.materialised(con, schema)
        if fold is not None:
            return fold.axes.get(name)
    return None


def mortal(schema: Schema) -> tuple[str, ...]:
    """The coordinates a fold keeps the dead labels of.

    One the owner map does not key, whose values naming a dead label only the
    read can drop (`_living`), and a dim a multi-dim's tuples range over or a
    property refers to, which the writer checks them against
    (`write._require_living_references`). A keyed coordinate's values the fold
    drops itself (`fold_attributes`).

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    drawn = {
        *(d for m in schema.multi_dims for d in schema.over(m).values()),
        *schema.one_of_properties().values(),
    }
    unkeyed = set(schema.broadcast_dims) - set(schema.partial_dims)
    return tuple(c for c in schema.broadcast_dims if c in drawn | unkeyed)


@dataclass(frozen=True)
class Coords:
    """A record's schema, plus each declared dim's folded axis relation.

    They travel together because the fold needs both wherever it broadcasts a
    NULL against "all values of that dim".

    Parameters
    ----------
    schema : Schema
        The record's schema.
    axes : dict of str to DuckDBPyRelation
        Each declared dim's folded axis relation, full row rather than the key
        column alone - its properties ride along; a multi-dim's is keyed by
        its struct column. A dim with no rows anywhere is absent rather than
        present-and-empty.
    dead : dict of str to DuckDBPyRelation
        Each `mortal` coordinate's dead labels, keyed as its axis.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]
    dead: dict[str, DuckDBPyRelation] = field(default_factory=dict)
