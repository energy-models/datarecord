"""Where a layer's rows come from, behind a protocol the fold reads them through.

The fold names files, not locations: `inputs/p_nom.parquet` is what it wants
and a source hands over its rows. Three answer - a parquet directory under
`layers/<uuid>/`, a plain directory read as one layer, and a staging area whose
rows are tables rather than files (`mutable.StagedSource`) - so the fold is
written once and knows about none of them.

Notes
-----
- [the record format](https://energy-models.github.io/datarecord/design/format/)
- [the DuckDB read path](https://energy-models.github.io/datarecord/design/read-path/)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable
from uuid import UUID, uuid5

from datarecord.duck import layer_dir, parquet_names, resolved_dir, try_read_parquet

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation

Kind = Literal["inputs", "outputs"]
"""Which long directory an attribute lives in - the alias `set` takes."""


@runtime_checkable
class LayerSource(Protocol):
    """One layer's own rows, however they are stored.

    "The layer as it would be written", not "the rows as stored": a source
    hands over what `write_record` would persist, so a staging area's `_seq`
    collapsing happens behind it and the fold never learns about it. `None`
    means this layer wrote nothing of that kind.

    Rows only. Everything the fold does to them - padding to the long schema,
    expanding broadcasts against an axis, the ownership aggregate, `order_key` -
    stays in the fold, which is the point: an implementation computing any of it
    would be a second copy of the fold.

    Structural, so an implementation needs no import from here - which is what
    lets `mutable.py` satisfy it without importing `layered` at module level.

    Attributes
    ----------
    layer_id : UUID
        What the fold stamps as `layer_uuid`, and what a read dispatches a
        winning row back through.
    frozen : bool
        Whether these rows can change under a reader. `False` for a staging
        area, which is what stops the fold materialising past it.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    """

    # Properties rather than variables, so a frozen dataclass satisfies this:
    # mypy reads a plain annotation here as a *settable* attribute, which a
    # frozen field is not.
    @property
    def layer_id(self) -> UUID: ...

    @property
    def frozen(self) -> bool: ...

    def axes(self) -> set[str]:
        """Which dims this layer has an axis file for.

        One listing rather than a probe per declared dim: `resolve_dims` asks
        each source once and folds only the dims some source holds.
        """
        ...

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """`dims/<dim>.parquet` - one axis's full row, not the key alone.

        `entity` is a dim like any other, so the entity axis is `axis("entity")`;
        what differs is only that the fold takes it as the components map,
        with `order_key` and tombstones, where `resolve_dims` folds the rest.
        """
        ...

    def entity_type(self, name: str) -> DuckDBPyRelation | None:
        """`dims/entity_type/<name>.parquet` - one type's wide member rows.

        A different thing from `axis("entity")`: read after the map named a
        winner, rather than folded to find one.
        """
        ...

    def group(self, name: str) -> DuckDBPyRelation | None:
        """`groups/<name>.parquet` - one group's rows, tombstones included."""
        ...

    def attribute(self, name: str, kind: Kind = "inputs") -> DuckDBPyRelation | None:
        """`<kind>/<name>.parquet` - one attribute's own columns, unpadded.

        Not the singular of `all_attributes`: this is the *owned* read, and it
        must keep exactly the columns the file has, a padded one being ambiguous
        against the broadcast rule.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        ...

    def all_attributes(self, kind: Kind = "inputs") -> DuckDBPyRelation | None:
        """Every `<kind>/*.parquet` unioned by name, unprojected.

        Only the long kinds have one: they share `input_key`, so a single scan
        answers the ownership `GROUP BY` for every attribute at once. An axis or
        a group folds on a key of its own, so a union across them would have
        nothing to fold on.
        """
        ...


class _FileLayer:
    """The layer layout over a URI, shared by every file-backed source.

    Each member is one file, addressed by what keys it. A subclass supplies
    where the layer root is (`uri`) and the connection to read it with, and
    inherits every accessor unchanged - which is what keeps "a directory read
    as a layer" from being a second reading of the format.
    """

    con: DuckDBPyConnection | None
    frozen: bool = True

    def uri(self, path: str = "") -> str:
        """Where `path` is, relative to the layer root.

        Empty is the root itself, with its trailing slash.
        """
        raise NotImplementedError

    @property
    def _con(self) -> DuckDBPyConnection:
        """The connection to read through.

        Optional on the field so `uri` alone is usable without one - which the
        write path does, having nothing to read.
        """
        if self.con is None:
            msg = f"{type(self).__name__} was built without a connection to read with"
            raise ValueError(msg)
        return self.con

    def _read(self, path: str, **kwargs: object) -> DuckDBPyRelation | None:
        return try_read_parquet(self.uri(path), self._con, **kwargs)

    def axes(self) -> set[str]:
        return {
            name.removesuffix(".parquet")
            for name in parquet_names(self.uri("dims/"), self._con)
        }

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        return self._read(f"dims/{dim}.parquet", union_by_name=True)

    def entity_type(self, name: str) -> DuckDBPyRelation | None:
        return self._read(f"dims/entity_type/{name}.parquet")

    def group(self, name: str) -> DuckDBPyRelation | None:
        return self._read(f"groups/{name}.parquet", union_by_name=True)

    def attribute(self, name: str, kind: Kind = "inputs") -> DuckDBPyRelation | None:
        return self._read(f"{kind}/{name}.parquet")

    def all_attributes(self, kind: Kind = "inputs") -> DuckDBPyRelation | None:
        return self._read(f"{kind}/*.parquet", union_by_name=True)


@dataclass(frozen=True)
class ParquetLayer(_FileLayer):
    """A layer as a parquet directory, located from its revision UUID.

    The location is derived, never stored, so changing the layout is a change
    to `layer_dir` and nothing else.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """

    revision_id: UUID
    con: DuckDBPyConnection | None = None
    base_uri: str | None = None
    frozen: bool = True

    @property
    def layer_id(self) -> UUID:
        return self.revision_id

    def uri(self, path: str = "") -> str:
        return layer_dir(self.revision_id, self.base_uri) + path


@dataclass(frozen=True)
class ResolvedLayer(ParquetLayer):
    """A materialised ancestor: the fold stops here, so it stands for everything above.

    `sources_to_read` stops *at* such a node, and what it stops at is the fold
    over everything above it. So the two folded members come from its node cache
    rather than from its layer - `axis` from `resolved/dims/`, and the owner map
    from `resolved/owner_map/`, which `_fold_map` reads as the seed instead of
    folding this source at all.

    Everything else stays the layer's own. A materialised map still names *this*
    node as a key's owner where it wrote one, and that row is in
    `layers/<id>/`, not in the cache beside it - the cache holds folded keys,
    never the rows they resolve to.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """

    def resolved_uri(self, path: str = "") -> str:
        """`path` under this node's cache, beside the layer rather than in it."""
        return resolved_dir(self.revision_id, self.base_uri) + path

    def map_uri(self, kind: str) -> str:
        """Where this node's `kind` owner map was materialised."""
        return self.resolved_uri(f"owner_map/{kind}.parquet")

    def axes(self) -> set[str]:
        return {
            name.removesuffix(".parquet")
            for name in parquet_names(self.resolved_uri("dims/"), self._con)
        }

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        return try_read_parquet(
            self.resolved_uri(f"dims/{dim}.parquet"), self._con, union_by_name=True
        )


@dataclass(frozen=True)
class DirectorySource(_FileLayer):
    """A plain parquet directory read as one layer.

    What a `WorkingRecord` over a `DirectoryRecord` folds its staged rows onto:
    the layout is the same, so every member is one of the directory's files and
    the fold needs no conditional path.

    `layer_id` is derived rather than stored, as `ParquetLayer`'s location is
    and for the mirrored reason: a directory has no revision to be stamped
    with, but it has a location, and that is what makes it one layer rather
    than another. So two readings of one directory agree on which layer they
    read without being told, and a caller has nothing to allocate or keep.

    It is no node in a layer tree either way - there is nothing for
    `NewChild()` to branch from, so this makes such a record *readable* through
    the fold, not committable to a tree.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """

    base: str
    con: DuckDBPyConnection | None = None
    frozen: bool = True

    @property
    def layer_id(self) -> UUID:
        return uuid5(_DIRECTORY_NAMESPACE, self.base)

    def uri(self, path: str = "") -> str:
        return self.base + path


# A fixed namespace, so one directory's `layer_id` is the same in every process
# - a random one would make it per-reader, which is what deriving it avoids.
_DIRECTORY_NAMESPACE = UUID("6f3d9f4e-1c2b-4a5d-8e7f-0a1b2c3d4e5f")
