"""A node's resolved view: the folded axes, groups and owner map, and the reads.

A `Fold` is what `materialise` wrote and what a live resolution computes: the
folded axes, groups, per-type wide frames, and the `inputs` owner map, each
folded over the node's whole ancestry. `Resolver.fold` takes the deepest
materialised source's `Fold` as its base and folds the layers below it on top,
so a `Fold` read from disk is the prior incarnation of one computed live - the
same shape either tense.

This is the object `ResolvedLayer` used to masquerade as: a fold-result, held as
a base, never an entry in the fold-input `sources` list. The folding that builds
a live `Fold` stays in `resolve.py`, where the DuckDB expression machinery lives;
`Fold.read` reads a materialised one back, and the map reads gated by the fold
(`owners`, `attributes`, `flags`) are here, on the object that holds the map.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit

from datarecord.duck import (
    distinct_values,
    fn,
    parquet_names,
    resolved_dir,
    struct_of,
    try_read_parquet,
)
from datarecord.record import Flags

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from datarecord.schema import Schema


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


@dataclass(frozen=True)
class Fold:
    """A node's resolved view, and the map reads over it.

    Each member is folded over the node's ancestry: `read` builds one from a
    `resolved/` cache, and `resolve.py` builds one live over sources. Either way
    it is the same shape, which is what lets a live fold start from a read one as
    its base.

    Attributes
    ----------
    schema
        The record's one schema, which says which broadcast dims the flags carry.
    axes
        Each resolved dim's axis relation, keyed by dim; `entity` among them.
    groups
        Each resolved group's relation, keyed by group.
    entity_types
        Each type's resolved wide static frame, keyed by type.
    owner_map
        The resolved `inputs` owner map: `(input_key, layer_uuid, varies,
        broadcast, breakpoints)`, one row per owned key.
    """

    schema: Schema
    axes: dict[str, DuckDBPyRelation]
    groups: dict[str, DuckDBPyRelation]
    entity_types: dict[str, DuckDBPyRelation]
    owner_map: DuckDBPyRelation

    @property
    def entity_axis(self) -> DuckDBPyRelation | None:
        """The resolved entity axis, or `None` where no layer wrote a component."""
        return self.axes.get("entity")

    def attributes(self) -> list[str]:
        """Every input attribute any layer owns a row for, from the owner map.

        Across component types, matching the file layout: one
        `inputs/<attr>.parquet` holds every type's rows. Ordered, so a `Record`
        over this has a stable key order.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return list(distinct_values(self.owner_map, "attribute"))

    def owners(self, attribute: str) -> DuckDBPyRelation:
        """The owner-map rows for one attribute - which layer owns each of its keys.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return self.owner_map.filter(col("attribute") == lit(attribute))

    def flags(self, entity_type: str | None = None) -> dict[str, Flags]:
        """Per attribute, which dims its rows use - whole-record, or one type.

        Whole-record when `entity_type` is `None`; scoped to a type's components
        when named, by a semi-join to the resolved entity axis. A type whose
        components disagree yields a dim in both sets - the instruction to use
        both containers, each taking the rows it matches. The union stops at the
        type boundary: across types it would describe neither.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        # The flags have a field per *broadcast* dim: an address coordinate never
        # broadcasts, so "did a row set it" is not a question about it.
        dims = self.schema.broadcast_dims
        rows = self._flag_rows(entity_type, dims)
        if rows is None:
            return {}
        # The structs come back as dicts keyed by dim, so each set is a filter by
        # name rather than a positional slice.
        return flags_from_rows(self.schema, dims, rows)

    def _flag_rows(
        self, entity_type: str | None, dims: tuple[str, ...]
    ) -> list[tuple[Any, ...]] | None:
        """The aggregated flag rows, scoped to `entity_type` if named.

        `None` when a type is named but no entity axis exists, so `flags` returns
        empty rather than aggregating an unscoped map.
        """
        rel = self.owner_map.set_alias("i")
        if entity_type is not None:
            axis = self.entity_axis
            if axis is None:
                return None
            of_type = axis.filter(col("entity_type") == lit(entity_type)).project(
                "entity"
            )
            rel = rel.join(
                of_type.distinct().set_alias("e"), "i.entity = e.entity", how="semi"
            )
        return rel.aggregate(
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
        ).fetchall()

    @classmethod
    def read(
        cls,
        revision_id: UUID,
        con: DuckDBPyConnection,
        schema: Schema,
        base_uri: str | None = None,
    ) -> Fold | None:
        """This node's `Fold`, or `None` if it is not materialised.

        The `inputs` owner map is the presence marker: the maps, dims, groups and
        per-type frames are written together (`resolve.materialise`), so if the
        map is absent the node has no `resolved/` cache at all.
        """
        base = resolved_dir(revision_id, base_uri)
        owner_map = try_read_parquet(f"{base}owner_map/inputs.parquet", con)
        if owner_map is None:
            return None
        return cls(
            schema=schema,
            axes=_read_dir(f"{base}dims/", con),
            groups=_read_dir(f"{base}groups/", con),
            entity_types=_read_dir(f"{base}dims/entity_type/", con),
            owner_map=owner_map,
        )


def _read_dir(dir_uri: str, con: DuckDBPyConnection) -> dict[str, DuckDBPyRelation]:
    """Every `<name>.parquet` in `dir_uri`, keyed by name, tolerant of an absent dir."""
    rels = {}
    for name in parquet_names(dir_uri, con):
        stem = name.removesuffix(".parquet")
        rel = try_read_parquet(f"{dir_uri}{name}", con, union_by_name=True)
        if rel is not None:
            rels[stem] = rel
    return rels
