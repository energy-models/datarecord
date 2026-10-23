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

from collections.abc import Sequence
from dataclasses import dataclass, field

from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression

from datarecord.duck import dead_keys, fold_axis
from datarecord.resolve.fold import Fold
from datarecord.resolve.sources import LayerSource
from datarecord.schema.schema import Schema


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
    """Fold every dim `schema` declares to its axis relation.

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

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    """
    # One listing per source rather than one probe per declared dim per source
    # (D x A misses, most declared dims absent from most layers): `present` says
    # which dims that source actually has, so `fold_axis` below is only ever
    # asked for a dim at least one source holds, and only passed the sources
    # that hold it.
    base, above = split or _base_and_above(sources, con, schema)
    present = [source.axes() for source in above]
    axes = {}
    for dim in schema.dims:
        # The base's resolved axis is depth 0 - already folded and tombstone-
        # free - so a layer above it still wins per key and its tombstones still
        # remove a base key, exactly as folding from the root would.
        seed = None if base is None else base.axes.get(dim)
        holding = [s for s, names in zip(above, present) if dim in names]
        if seed is None and not holding:
            continue
        rel = fold_axis([seed, *(s.axis(dim) for s in holding)], dim, con)
        if rel is not None:
            axes[dim] = rel
    return Coords(
        schema=schema,
        axes=axes,
        groups=resolve_groups(schema, base, above, con),
        dead=_resolve_dead(schema, base, above, present, con),
    )


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


def _resolve_dead(
    schema: Schema,
    base: Fold | Coords | None,
    above: Sequence[LayerSource],
    present: Sequence[set[str]],
    con: DuckDBPyConnection,
) -> dict[str, DuckDBPyRelation]:
    """Each `mortal` coordinate's dead labels: those whose winning row is a tombstone.

    The base's resolved axes and groups are tombstone-free, so its dead labels
    seed depth 0 from its own cache, a label a layer above restates reviving.
    A coordinate is absent where no layer carries a `deleted` column for it,
    and an empty relation where one does but nothing is dead.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    dead = {}
    for name in mortal(schema):
        if name in schema.groups:
            rows = [source.group(name) for source in above]
        else:
            rows = [s.axis(name) for s, names in zip(above, present) if name in names]
        seed = None if base is None else base.dead.get(name)
        if seed is not None:
            seed = seed.project(col(name), lit(True).alias("deleted"))  # noqa: FBT003
        rel = dead_keys([seed, *rows], name, con)
        if rel is not None:
            dead[name] = rel
    return dead


def resolve_groups(
    schema: Schema,
    base: Fold | Coords | None,
    above: Sequence[LayerSource],
    con: DuckDBPyConnection,
) -> dict[str, DuckDBPyRelation]:
    """Fold every declared group to its resolved relation, keyed by its struct column.

    A group folds through `fold_axis` like any axis, keyed by the one column
    named for it: last-writer-wins per tuple, its properties on the winning
    row, its own tombstones honoured, member
    order the file's row order.
    The base `Fold`'s resolved group seeds depth 0. Absent from the result where
    no layer wrote a row.

    Notes
    -----
    - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    groups = {}
    for group in schema.groups:
        seed = None if base is None else base.groups.get(group)
        rows = [seed, *(source.group(group) for source in above)]
        if all(rel is None for rel in rows):
            continue
        rel = fold_axis(rows, group, con)
        if rel is not None:
            groups[group] = rel
    return groups


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
