# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The folded axes, groups and dead labels of a record's layers.

Notes
-----
- [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
- [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import reduce

from duckdb import CoalesceOperator as coalesce
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
    """Return the deepest of `files`, else `seed`: one node's resolved axis or group.

    The one fold for every axis, a dim's or a group's: a layer that changes
    one restates it whole, so the deepest restatement is the answer, in its
    own row order. `files` is root first. A frozen file holding `key` twice is
    refused; a staging area's table is keyed, so it holds no key twice.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
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
    why: str = "an axis or group file holds each of its labels once "
    "(https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)",
) -> None:
    """Refuse `file`, read as `rel`, holding two rows that agree on every one of `keys`.

    NULL agrees with NULL.

    Raises
    ------
    ValueError
        Naming the values both rows hold, how many rows hold them, and `why`.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
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
    """Refuse a label or tuple in `rows` that the axis or group holding it lacks.

    A dim's column of `columns` names labels of that dim, a group's column
    tuples of that group and, through each field, labels of the field's dim,
    and a property of `subject` that takes its labels from a dim labels of
    that dim. Any other column is not checked. `subject`'s own column holds
    the labels or tuples being added, so of it only a group's fields are
    checked.
    Labels are checked before tuples, so a tuple naming an unheld label is
    refused for that label. NULL is no label and passes.

    Parameters
    ----------
    held
        Returns the axis or group of a name, `None` where there is none:
        as staged where `staged`, else the layer's final one.
    where
        What names the labels, to start the message with.

    Raises
    ------
    KeyError
        Where `staged`, naming the dim or group and up to three such labels
        or tuples.
    ValueError
        Where not `staged`, naming the same.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/design/working-record/#validation)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    taken = {n: d for (s, n), d in schema.one_of_properties().items() if s == subject}
    labels: dict[str, tuple[Expression, str]] = {}
    fields: dict[str, tuple[Expression, str]] = {}
    tuples: dict[str, tuple[Expression, str]] = {}
    for c in columns:
        if c in taken:
            labels[f"{subject}.{c}"] = (col(c), taken[c])
        elif c in schema.groups:
            fields |= {
                f"{c}.{f}": (fn.struct_extract(col(c), lit(f)), d)
                for f, d in schema.groups[c].over.items()
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
    group = name in schema.groups
    shown = (
        f"{name!r} tuples [{', '.join(found)}]" if group else f"{name!r} labels {found}"
    )
    via = "" if key == name else f" in {key!r}"
    if staged:
        holder = f"the staged {'group' if group else 'axis'}"
        if group:
            remedy = "`add_group` them first"
        elif schema.dimensions[name].one_of is not None:
            remedy = "that dim lists its labels, so use one of them"
        else:
            remedy = "`add` them first"
        link = "working-record/#validation"
    else:
        holder = f"the layer's {'groups' if group else 'dims'}/{name}.parquet"
        remedy = "add them there or drop those rows"
        link = "writing/"
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
    tuple, the axis or group relation that holds them, and that relation's
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
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    labels = [rel.project(col(key)) for rel in held]
    union = reduce(DuckDBPyRelation.union, labels).distinct().set_alias("h")
    dead = union.join(
        final.project(col(key)).set_alias("f"),
        col("h", key) == col("f", key),
        how="anti",
    )
    if dead.limit(1).fetchone() is None:
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
    """Resolve every dim and group `schema` declares, and the dead labels.

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
        If the deepest file of an axis or group holds one label twice.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    base, above = split or _base_and_above(sources, con, schema)
    listed = [source.axes() | source.groups() for source in above]
    keep = mortal(schema)
    axes: dict[str, DuckDBPyRelation] = {}
    groups: dict[str, DuckDBPyRelation] = {}
    dead: dict[str, DuckDBPyRelation] = {}
    for name in (*schema.dims, *schema.groups):
        kind, resolved = ("groups", groups) if name in schema.groups else ("dims", axes)
        files = [
            (s, rel)
            for s, held in zip(above, listed, strict=True)
            if name in held and (rel := restatement(s, name, schema)) is not None
        ]
        seed = None if base is None else (base.groups | base.axes).get(name)
        rel = _fold_axis(seed, files, name, f"{kind}/{name}.parquet")
        if rel is None:
            continue
        resolved[name] = rel
        if name not in keep:
            continue
        before = None if base is None else base.dead.get(name)
        if files:
            held = [r for r in (before, seed, *(r for _, r in files)) if r is not None]
            before = _dead_keys(held, rel, name)
        if before is not None:
            dead[name] = before
    return Coords(schema=schema, axes=axes, groups=groups, dead=dead)


def restatement(
    source: LayerSource, name: str, schema: Schema
) -> DuckDBPyRelation | None:
    """Return `source`'s file of the axis or group `name`, `None` where it has none."""
    return source.group(name) if name in schema.groups else source.axis(name)


def resolve_one(
    schema: Schema, sources: Sequence[LayerSource], con: DuckDBPyConnection, name: str
) -> DuckDBPyRelation | None:
    """Return the resolved axis or group `name` over `sources`, `None` where none has it.

    The deepest file for it, or the resolved one of the deepest materialised
    source above that file.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    for source in reversed(sources):
        rel = restatement(source, name, schema)
        if rel is not None:
            return rel
        fold = source.materialised(con, schema)
        if fold is not None:
            return (fold.groups | fold.axes).get(name)
    return None


def mortal(schema: Schema) -> tuple[str, ...]:
    """The coordinates a fold keeps the dead labels of.

    One the owner map does not key, whose values naming a dead label only the
    read can drop (`_living`), and a dim a group's tuples range over or a
    property takes its labels from, which the writer checks them against
    (`write._require_living_references`). A keyed coordinate's values the fold
    drops itself (`fold_attributes`).

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    drawn = {
        *(d for g in schema.group_columns for d in schema.groups[g].over.values()),
        *schema.one_of_properties().values(),
    }
    unkeyed = set(schema.broadcast_dims) - set(schema.partial_dims)
    return tuple(
        c for c in (*schema.dims, *schema.group_columns) if c in drawn | unkeyed
    )


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
        column alone - its properties ride along. A dim with no rows
        anywhere is absent rather than present-and-empty.
    groups : dict of str to DuckDBPyRelation
        Each declared group's folded relation, keyed by its struct column,
        carrying its static columns, in member order - a group folds like an
        axis, no owner map. Absent where no layer wrote a row.
    dead : dict of str to DuckDBPyRelation
        Each `mortal` coordinate's dead labels, keyed as its axis or group.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]
    groups: dict[str, DuckDBPyRelation] = field(default_factory=dict)
    dead: dict[str, DuckDBPyRelation] = field(default_factory=dict)

    def expand_dims(
        self,
        rel: DuckDBPyRelation,
        layer_keys: tuple[str, ...],
        con: DuckDBPyConnection,
    ) -> tuple[DuckDBPyRelation, dict[str, Expression]]:
        """Join `rel` against each of `layer_keys`' axis, broadcasting NULLs.

        Only a row of an attribute whose `dims` name the dim broadcasts: any
        other is NULL there in every layer, so its key needs no
        expansion, and expanding it would multiply its owner-map rows by the
        axis length for no change in ownership. A NULL reaches every label of
        the dim, categorized or not.

        Parameters
        ----------
        rel : DuckDBPyRelation
            Carries an `attribute` column.
        layer_keys : tuple of str
            The broadcast coordinates of `schema.partial_dims`; a group's
            axis is its resolved relation.

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
        `write_layer` enforces: a record whose frames do not is one the
        writer rejected, and binding here would resolve it as though the dim
        were broadcast everywhere.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        alias = rel.alias
        exprs = {}
        for dim in layer_keys:
            axis = self.axes.get(dim, self.groups.get(dim))
            over = [
                a
                for a in self.schema.attributes
                if dim in self.schema.coordinates_of(a)
            ]
            if axis is None or not over:
                exprs[dim] = col(alias, dim)
                continue
            # `... = true` rather than the bare condition: an equality hashes,
            # a one-sided condition is a nested loop over every row and label.
            # An attribute not over `dim` holds NULL there for want of the
            # column, which is no broadcast.
            broadcasts = col(alias, dim).isnull() & col(alias, "attribute").isin(
                *(lit(a) for a in over)
            )
            key = axis.project(col(dim), lit(True).alias("_broadcast"))  # noqa: FBT003
            rel = rel.join(key, broadcasts == col(key.alias, "_broadcast"), how="left")
            exprs[dim] = coalesce(col(alias, dim), col(key.alias, dim))
        return rel, exprs
