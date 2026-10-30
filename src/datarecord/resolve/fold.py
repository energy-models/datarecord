# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A node's resolved view: the folded axes and owner map, and the reads.

A `Fold` is what `materialise` wrote and what a live resolution computes: the
folded axes and the owner map, each
folded over the node's whole ancestry. `Record._fold` takes the deepest
materialised source's `Fold` as its base and folds the layers below it on top,
so a `Fold` read from disk is the prior incarnation of one computed live - the
same shape either tense.

A `Fold` is held as a base, never one of the `sources` folded on top of it.
The folding that builds
a live `Fold` stays in `coords.py` and `owner_map.py`, where the DuckDB
expression machinery lives; `Fold.read` reads a materialised one back, and the
map reads gated by the fold (`owners`, `attributes`, `flags`) are here, on the
object that holds the map.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
- [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Any
from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression

from datarecord.duck import (
    distinct_values,
    fn,
    struct_field,
    struct_of,
    try_read_parquet,
)
from datarecord.protocols import Flags
from datarecord.schema.categories import (
    categorized_dim,
    matching,
    selected,
)
from datarecord.schema.schema import LAYER_COLUMN

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from duckdb import DuckDBPyConnection

    from datarecord.schema.schema import Schema
    from datarecord.store.store import Store


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
    - [Flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
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


@dataclass(frozen=True)
class Fold:
    """A node's resolved view, and the map reads over it.

    Each member is folded over the node's ancestry: `read` builds one from a
    `resolved/` cache, and `Record._fold` builds one live over sources. Either way
    it is the same shape, which is what lets a live fold start from a read one as
    its base.

    Attributes
    ----------
    schema
        The record's one schema, which says which broadcast dims the flags carry.
    axes
        Each resolved dim's axis relation, keyed by dim.
    dim_owners
        Each axis's owner: the layer whose `dims/<dim>.parquet` it resolved
        to, a dim with `one_of` having none.
    owner_map
        The resolved owner map: `(owner_map_key, layer_uuid, varies,
        broadcast, breakpoints)`, one row per key as its layer wrote it,
        expanded along a dim only where a deeper layer crossed it there.
    dead
        Each `coords.mortal` coordinate's dead labels, where it has any.
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]
    dim_owners: dict[str, UUID]
    owner_map: DuckDBPyRelation
    dead: dict[str, DuckDBPyRelation]

    def attributes(self) -> list[str]:
        """Return every attribute the owner map holds a row for.

        Ordered, so a `Record` over this has a stable key order.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return list(distinct_values(self.owner_map, "attribute"))

    def owners(self, attribute: str) -> DuckDBPyRelation:
        """The owner-map rows for one attribute - which layer owns each of its keys.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return self.owner_map.filter(col("attribute") == lit(attribute))

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Per attribute, which dims its rows use, across the record or one category.

        A dim in both sets is the instruction to use both containers, each
        taking the rows it matches.

        Parameters
        ----------
        categories
            Categorizing properties of one dim, each one category or several:
            only rows reaching a matching label count. None given, every row
            counts.

        Raises
        ------
        ValueError
            If the keywords categorize no dim or several.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
        - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
        """
        dims = self.schema.broadcast_dims
        if not categories:
            # The structs come back as dicts keyed by dim, so each set is a filter
            # by name rather than a positional slice.
            return flags_from_rows(
                self.schema, dims, self._flag_rows(self.owner_map, dims)
            )
        dim = categorized_dim(self.schema, categories)
        chosen = selected(self.schema, dim, categories)
        axis = self.axes.get(dim)
        if axis is None:
            return {}
        rows = self._owned_reaching(dim, matching(axis, chosen).project(col(dim)))
        return flags_from_rows(self.schema, dims, self._flag_rows(rows, dims))

    def _owned_reaching(self, dim: str, labels: DuckDBPyRelation) -> DuckDBPyRelation:
        """The owner-map rows reaching one of `labels`, on `dim` or a multi-dim's coordinate over it.

        A row names a label, or leaves the coordinate NULL for an attribute
        over it, which reaches every label the coordinate holds: such a row
        counts where `labels` holds a label of `dim`, or the multi-dim a tuple
        whose coordinate names one. One semi-join per path, unioned rather than
        OR-ed into one condition, which would be a nested loop; a row matched
        twice changes no flag.
        """
        om = self.owner_map.set_alias("o")
        named = labels.set_alias("l")
        paths: list[tuple[str, str | None]] = [(dim, None)]
        paths += [
            (m, f)
            for m in self.schema.multi_dims
            if (f := self.schema.coordinate_over(m, dim))
        ]
        reached = []
        for column, field in paths:
            if column not in om.columns:
                continue
            on = _path("o", column, field) == col("l", dim)
            reached.append(om.join(named, on, how="semi"))
            if field is None:
                hit = labels
            elif (held := self.axes.get(column)) is not None:
                on = _path("g", column, field) == col("l", dim)
                hit = held.set_alias("g").join(named, on, how="semi")
            else:
                continue
            over = [
                a
                for a in self.schema.attributes
                if column in self.schema.coordinates_of(a)
            ]
            if over and hit.limit(1).fetchall():
                reached.append(
                    om.filter(
                        col("o", column).isnull()
                        & col("o", "attribute").isin(*(lit(a) for a in over))
                    )
                )
        if not reached:
            return om.filter(lit(False))  # noqa: FBT003
        return reduce(DuckDBPyRelation.union, reached)

    def _flag_rows(
        self, rel: DuckDBPyRelation, dims: tuple[str, ...]
    ) -> list[tuple[Any, ...]]:
        """`rel`'s flag columns, aggregated per attribute."""
        flag = {
            c: struct_of(
                {d: fn.bool_or(struct_field(rel, c, d, lit(False))) for d in dims}
            ).alias(c)
            for c in ("varies", "broadcast")
        }
        return rel.aggregate(
            [
                col("attribute"),
                flag["varies"],
                flag["broadcast"],
                fn.bool_or(col("breakpoints")).alias("breakpoints"),
            ]
        ).fetchall()

    @classmethod
    def read(
        cls,
        revision_id: UUID,
        con: DuckDBPyConnection,
        schema: Schema,
        store: Store,
    ) -> Fold | None:
        """This node's `Fold`, or `None` if it is not materialised under `schema`.

        The owner map is the presence marker: the map is written last
        (`cache.materialise`), so if it is absent or stale (`read_owner_map`)
        the node reads as having no cache at all. Each axis is read from the
        layer `dim_owners.parquet` names for it, and a dim's dead labels only
        where it says the dim has any.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
        """
        owner_map = read_owner_map(revision_id, con, schema, store)
        if owner_map is None:
            return None
        rows = read_dim_owners(revision_id, con, store)
        dead = f"{store.resolved_dir(revision_id)}dead/"
        return cls(
            schema=schema,
            axes={
                dim: con.read_parquet(store.axis_uri(layer, dim))
                for dim, layer, _ in rows
            },
            dim_owners={dim: layer for dim, layer, _ in rows},
            owner_map=owner_map,
            dead={
                dim: con.read_parquet(f"{dead}{dim}.parquet")
                for dim, _, has_dead in rows
                if has_dead
            },
        )


def _path(alias: str, column: str, field: str | None) -> Expression:
    """`column` under `alias`, or its struct's `field` where one is given."""
    c = col(alias, column)
    return c if field is None else fn.struct_extract(c, lit(field))


def read_owner_map(
    revision_id: UUID, con: DuckDBPyConnection, schema: Schema, store: Store
) -> DuckDBPyRelation | None:
    """`revision_id`'s persisted owner map, `None` where it is absent or keyed otherwise than `schema` keys one.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    rel = try_read_parquet(store.owner_map_uri(revision_id), con)
    if rel is None or set(rel.columns) != set(schema.owner_map_columns):
        return None
    return rel


def read_dim_owners(
    revision_id: UUID, con: DuckDBPyConnection, store: Store
) -> list[tuple[str, UUID, bool]]:
    """`revision_id`'s materialised `(dim, layer_uuid, has_dead)` rows (`Coords.owners`).

    A dim absent has no axis.

    Raises
    ------
    duckdb.IOException
        If the node holds no `dim_owners.parquet`.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    rel = con.read_parquet(store.dim_owners_uri(revision_id))
    return rel.project(col("dim"), col(LAYER_COLUMN), col("has_dead")).fetchall()
