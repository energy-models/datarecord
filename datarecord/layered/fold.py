"""The resolved fold of a node, read back from its `resolved/` cache.

A `Fold` is what `materialise` wrote: the folded axes, groups, per-type wide
frames, and the `inputs` owner map, each already folded over everything above
the node. `Resolver.fold` takes the deepest materialised source's `Fold` as its
base and folds the layers below it on top - so a `Fold` read from disk is the
prior incarnation of a `Fold` computed live, the same shape either tense.

This is the object `ResolvedLayer` used to masquerade as: a fold-result, held as
a base, never an entry in the fold-input `sources` list.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from datarecord.duck import parquet_names, resolved_dir, try_read_parquet

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


@dataclass(frozen=True)
class Fold:
    """A node's resolved view, read from `resolved/`.

    Each member is already folded over the node's whole ancestry, so a live fold
    starting from this base folds only the layers below the materialised node.

    Attributes
    ----------
    axes
        Each resolved dim's axis relation, keyed by dim (`resolved/dims/`).
    groups
        Each resolved group's relation, keyed by group (`resolved/groups/`).
    entity_types
        Each type's resolved wide static frame, keyed by type
        (`resolved/dims/entity_type/`).
    owner_map
        The resolved `inputs` owner map (`resolved/owner_map/inputs.parquet`).
    """

    axes: dict[str, DuckDBPyRelation]
    groups: dict[str, DuckDBPyRelation]
    entity_types: dict[str, DuckDBPyRelation]
    owner_map: DuckDBPyRelation

    @classmethod
    def read(
        cls,
        revision_id: UUID,
        con: DuckDBPyConnection,
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
