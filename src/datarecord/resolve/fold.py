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
a live `Fold` stays in `axes.py` and `owner_map.py`, where the DuckDB
expression machinery lives; `Fold.read` reads a materialised one back, and the
map reads gated by the fold (`owners`, `flags`) are here, on the
object that holds the map.

Notes
-----
- [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, Expression

from datarecord.duck import FOLD, fn, tables_in, union_all_by_name
from datarecord.protocols import Flags
from datarecord.resolve.axes import Axes, _fold_name
from datarecord.resolve.owner_map_rows import owner_map_rows
from datarecord.schema.categories import (
    categorized_dim,
    matching,
    selected,
)
from datarecord.schema.schema import LAYER_COLUMN, empty_declared

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from duckdb import DuckDBPyConnection

    from datarecord.protocols import RecordLike
    from datarecord.resolve.sources import LayerSource, ParquetLayer
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

    Both sets are cut to the attribute's own dims. The relation aggregated
    over covers every attribute, so a dim one attribute is addressed by reads
    NULL for the rows of one that is not, and reporting that NULL as "every row
    broadcasts over it" would tell a consumer to build a container along an axis
    the attribute has no values on.

    Parameters
    ----------
    dims
        The dims the two mappings have an entry per.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
    """

    def scope(attribute: str) -> tuple[str, ...]:
        own = schema.dims_of(attribute)
        return tuple(d for d in dims if d in own)

    return {
        attribute: Flags(
            frozenset(d for d in scope(attribute) if varies[d]),
            frozenset(d for d in scope(attribute) if broadcast[d]),
            bool(breakpoints),
        )
        for attribute, varies, broadcast, breakpoints in rows
    }


@dataclass(frozen=True)
class Fold:
    """A node's resolved view, and the map reads over it.

    Each member is folded over the node's ancestry: `read` builds one from a
    node's `resolved/` files, and `Record._fold` builds one live over sources. Either way
    it is the same shape, which is what lets a live fold start from a read one as
    its base.

    Attributes
    ----------
    axes
        The folded axes, their owners and dead labels, under the schema the
        record is read under, which says which dims the flags carry.
    owner_map
        The resolved owner map: `(owner_map_index, layer_uuid, varies,
        broadcast, breakpoints)`, one map row as its layer wrote it,
        expanded along a dim only where a deeper layer crossed it there.
    """

    axes: Axes
    owner_map: DuckDBPyRelation

    def owners(self, attribute: str) -> DuckDBPyRelation:
        """The owner-map rows for one attribute - which layer owns each of its map rows.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
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
        - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
        - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
        """
        schema = self.axes.schema
        dims = tuple(schema.dimensions)
        if not categories:
            # The structs come back as dicts keyed by dim, so each set is a filter
            # by name rather than a positional slice.
            return flags_from_rows(schema, dims, self._flag_rows(self.owner_map, dims))
        dim = categorized_dim(schema, categories)
        chosen = selected(schema, dim, categories)
        axis = self.axes.get(dim)
        if axis is None:
            return {}
        rows = self._owned_reaching(dim, matching(axis, chosen).project(col(dim)))
        return flags_from_rows(schema, dims, self._flag_rows(rows, dims))

    def _owned_reaching(self, dim: str, labels: DuckDBPyRelation) -> DuckDBPyRelation:
        """The owner-map rows reaching one of `labels`, on `dim` or a multi-dim's field over it.

        A row names a label, or leaves the dim NULL for an attribute
        over it, which covers every label the dim holds: such a row
        counts where `labels` holds a label of `dim`, or the multi-dim a tuple
        whose field names one. One semi-join per path, unioned rather than
        OR-ed into one condition, which would be a nested loop; a row matched
        twice changes no flag.
        """
        schema = self.axes.schema
        om = self.owner_map.set_alias("o")
        named = labels.set_alias("l")
        paths: list[tuple[str, str | None]] = [(dim, None)]
        paths += [(m, f) for m in schema.multi_dims if (f := schema.field_over(m, dim))]
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
            over = [a for a in schema.attributes if column in schema.dims_of(a)]
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
            c: fn.struct_pack(**{d: fn.bool_or(col(c)[d]) for d in dims}).alias(c)
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
    def as_one_layer(cls, record: RecordLike) -> Fold:
        """A record that no layer holds, folded as one layer.

        Its `flags` answer as a record written from it would, so a `RecordLike`
        that is no `Record`, a tool presenting a framework object among them,
        can answer them. The owner map is collected once, so each `flags` call
        aggregates it alone. A layered `Record` passed here is flattened: the
        map rows each of its layers wrote are lost.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        schema, attributes, con = record.schema, record.attributes, record.con
        layer = uuid4()
        owned = [owner_map_rows(attributes[a], schema, layer, (a,)) for a in attributes]
        owner_map = (
            union_all_by_name(owned)
            if owned
            else empty_declared(schema, con, *schema.owner_map_columns)
        )
        axes = Axes(schema, dict(record.properties), {})
        return cls(axes, owner_map.materialize())

    @classmethod
    def read(cls, layer: ParquetLayer) -> Fold | None:
        """`layer`'s node's `Fold` under its own schema, `None` if it is not materialised.

        The owner map is the presence marker: the map is written last
        (`materialise.materialise`), so if it is absent the node reads as not
        materialised at all. Each axis is read from the layer `dim_owners.parquet`
        names for it, and a dim's dead labels only where it says the dim has
        any.

        Notes
        -----
        - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
        """
        revision_id, store, con = layer.layer_id, layer.store, layer.workspace.con
        owner_map = read_owner_map(revision_id, con, store)
        if owner_map is None:
            return None
        dead = f"{store.resolved_dir(revision_id)}dead/"
        axes = Axes.from_rows(
            layer.schema,
            read_dim_owners(revision_id, con, store),
            lambda dim, owner: con.read_parquet(store.axis_uri(owner, dim)),
            lambda dim: con.read_parquet(f"{dead}{dim}.parquet"),
        )
        return cls(axes, owner_map)

    @classmethod
    def held(cls, source: LayerSource) -> Fold | None:
        """`source`'s node's `Fold` from the workspace's shared tables, `None` where they lack it.

        The axes and the owner map are tabled independently, so each is
        looked up (`Axes.held`).
        """
        rid, ws = source.layer_id, source.workspace
        if "owner_map" not in tables_in(ws.con, ws.name, FOLD, rid):
            return None
        owner_map = ws.con.table(_fold_name(ws, rid, "owner_map"))
        axes = Axes.held(source)
        return None if axes is None else cls(axes, owner_map)


def _path(alias: str, column: str, field: str | None) -> Expression:
    """`column` under `alias`, or its struct's `field` where one is given."""
    c = col(alias, column)
    return c if field is None else c[field]


def read_owner_map(
    revision_id: UUID, con: DuckDBPyConnection, store: Store
) -> DuckDBPyRelation | None:
    """`revision_id`'s persisted owner map, keyed by its own schema, `None` where it is absent.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    """
    uri = store.owner_map_uri(revision_id)
    return con.read_parquet(uri) if store.files.exists(uri) else None


def read_dim_owners(
    revision_id: UUID, con: DuckDBPyConnection, store: Store
) -> list[tuple[str, UUID, bool]]:
    """`revision_id`'s materialised `(dim, layer_uuid, has_dead)` rows (`Axes.owners`).

    A dim absent has no axis.

    Raises
    ------
    duckdb.IOException
        If the node holds no `dim_owners.parquet`.

    Notes
    -----
    - [materialised revisions](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    rel = con.read_parquet(store.dim_owners_uri(revision_id))
    return rel.project(col("dim"), col(LAYER_COLUMN), col("has_dead")).fetchall()
