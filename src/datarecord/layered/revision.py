# SPDX-FileCopyrightText: datarecord Contributors
#
# SPDX-License-Identifier: MIT

"""The `Revision` node and its ancestry query.

A thin façade over `resolve`: it holds the node's identity and reads the
`revisions` table, knowing nothing about owner maps or parquet layers. A node
has no state beyond its place in the tree - whether its caches are materialised
is a filesystem question, answered by `resolve.materialised`.

Notes
-----
- [layered resolution](https://energy-models.github.io/datarecord/design/layers/)
- [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
"""

from dataclasses import dataclass
from functools import cached_property
from typing import Self
from uuid import UUID

import narwhals as nw
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from pydantic import BaseModel, PrivateAttr

from datarecord.duck import default_connection
from datarecord.layered import resolve
from datarecord.layered.resolve import NodeCache
from datarecord.record import Flags, LazyFrames
from datarecord.schema import Schema

_ANCESTRY = """
WITH RECURSIVE ancestors(id, parent, depth) AS (
  SELECT id, parent, 0 FROM revisions WHERE id = ?
  UNION ALL
  SELECT d.id, d.parent, ancestors.depth + 1
  FROM revisions d
  JOIN ancestors ON d.id = ancestors.parent
)
SELECT id FROM ancestors ORDER BY depth DESC
"""


def _insert(con: DuckDBPyConnection, parent: UUID | None) -> tuple[UUID, UUID | None]:
    """Insert a new revision, letting the DB assign the UUID."""
    row = con.execute(
        "INSERT INTO revisions (parent) VALUES (?) RETURNING id, parent", [parent]
    ).fetchone()
    assert row is not None
    return row


def _fetch(con: DuckDBPyConnection, revision_id: UUID) -> tuple[UUID, UUID | None]:
    """Read one revision's row, or raise `KeyError`."""
    row = con.execute("SELECT id, parent FROM revisions WHERE id = ?", [revision_id]).fetchone()
    if row is None:
        msg = f"No revision {revision_id}"
        raise KeyError(msg)
    return row


def ancestry(con: DuckDBPyConnection, revision_id: UUID) -> list[UUID]:
    """Revision ids along the root->node path, root first - resolution order.

    The whole path. Truncating it at the nearest materialised node is the
    reader's business (`resolve.ancestry_to_read`), since whether a node's
    caches exist is a fact about the filesystem rather than about the tree.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    return [r[0] for r in con.execute(_ANCESTRY, [revision_id]).fetchall()]


class Revision(BaseModel):
    """A node in the tree of layers.

    Each revision adds one parquet layer on top of its parent's; the record it
    resolves to is that layer over its ancestors'. The layer location
    derives from `id` via `layer_dir`, it is not stored.

    Holds the connection it was created/loaded with (`con`), so every method
    below reuses it without needing it passed at every call. It is a private
    attribute, not a field: it never round-trips through (de)serialization
    (e.g. across a Prefect process boundary) - a revision that comes back
    without one lazily reattaches the process-level default on next access.

    Notes
    -----
    - [layered resolution](https://energy-models.github.io/datarecord/design/layers/)
    """

    id: UUID
    parent: UUID | None = None

    _con: DuckDBPyConnection | None = PrivateAttr(default=None)
    _node_cache: NodeCache | None = PrivateAttr(default=None)

    @property
    def con(self) -> DuckDBPyConnection:
        if self._con is None:
            self._con = default_connection()
        return self._con

    @property
    def node_cache(self) -> NodeCache:
        """This record's `NodeCache`, built once from its ancestry.

        The ancestry is truncated at the deepest materialised ancestor, so a
        deep tree resolves from few entries. Cached rather than rebuilt per
        call: layers are write-once, so the only thing that can change
        the truncation point is `materialise`, which clears this itself.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        if self._node_cache is None:
            full = ancestry(self.con, self.id)
            to_read = resolve.ancestry_to_read(full, self.con)
            self._node_cache = NodeCache(self.id, to_read, self.con)
        return self._node_cache

    @property
    def record(self) -> "LayeredRecord":
        """This revision's resolved overlay as a `Record`.

        The framework-agnostic view of a record: the same interface a plain
        parquet directory satisfies, so a consumer need not know the record is
        an overlay at all. `node_cache` remains the DuckDB-shaped view, which
        `datarecord.tools` still builds from.

        Notes
        -----
        - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        return LayeredRecord(self.node_cache)

    # -- tree ---------------------------------------------------------------

    @classmethod
    def create(cls, con: DuckDBPyConnection | None = None, parent: UUID | None = None) -> Self:
        """Insert a new revision, letting the DB assign the UUID."""
        con = con or default_connection()
        id_, parent_ = _insert(con, parent)
        record = cls(id=id_, parent=parent_)
        record._con = con
        return record

    @classmethod
    def get(cls, revision_id: UUID, con: DuckDBPyConnection | None = None) -> Self:
        """Load a revision by id."""
        con = con or default_connection()
        id_, parent = _fetch(con, revision_id)
        record = cls(id=id_, parent=parent)
        record._con = con
        return record

    def child(self) -> Self:
        """Branch a new revision off this one.

        Any node may be a parent: a layer is write-once, so a base cannot
        shift under its descendants.

        Notes
        -----
        - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
        """
        return type(self).create(self.con, parent=self.id)

    def materialise(self) -> None:
        """Write this node's caches - owner maps and resolved dims.

        A policy rather than a lifecycle step, and purely additive: it changes
        no answer, only how many layers a descendant's read touches. Once these
        exist, a read stops here instead of walking further up.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        resolve.materialise(self.id, self.node_cache.ancestry, self.con)
        self._node_cache = None

    # -- read ---------------------------------------------------------------

    def ancestry(self) -> list[UUID]:
        """Revision ids along the root->self path, root first.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        return ancestry(self.con, self.id)


@dataclass(frozen=True)
class LayeredRecord:
    """A record's resolved overlay, as a `Record`.

    The protocol's shape over `NodeCache`, not a second implementation of it, so
    a member costs what the equivalent `NodeCache` call costs - `flags` in
    particular is free, the owner map having folded it in.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """

    node_cache: NodeCache

    @property
    def schema(self) -> Schema:
        return self.node_cache.schema

    @property
    def con(self) -> DuckDBPyConnection:
        return self.node_cache.con

    @cached_property
    def dims(self) -> LazyFrames:
        axes = self.node_cache.dims.axes
        return LazyFrames(tuple(axes), lambda dim: nw.from_native(axes[dim]))

    @cached_property
    def components(self) -> LazyFrames:
        types = tuple(sorted(self.node_cache.component_types()))
        return LazyFrames(types, self._component_frame)

    @cached_property
    def connections(self) -> LazyFrames:
        rows = self.node_cache.connections.project("component_type").distinct()
        types = tuple(sorted(r[0] for r in rows.fetchall()))
        return LazyFrames(types, self._connection_frame)

    @cached_property
    def attributes(self) -> LazyFrames:
        names = tuple(self.node_cache.attribute_names())
        return LazyFrames(names, lambda attr: nw.from_native(self.node_cache.relation(attr)))

    @cached_property
    def outputs(self) -> LazyFrames:
        names = tuple(self.node_cache.output_names())
        return LazyFrames(names, lambda attr: nw.from_native(self.node_cache.outputs(attr)))

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Straight off the `inputs` owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return {
            attribute: Flags(varies, broadcast, breakpoints)
            for attribute, (varies, broadcast, breakpoints) in (
                self.node_cache.attributes_of(ctype).items()
            )
        }

    # -- frames, ordered by the map's `order_key` (https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations) ---------------------

    def _component_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.component_frame(ctype), ctype)

    def _connection_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.connection_frame(ctype), ctype)

    def _ordered(self, rel: DuckDBPyRelation | None, ctype: str) -> nw.LazyFrame:
        """`rel` in member order, which for an overlay means sorted by `order_key`.

        The fold's own output has no order (its union puts a layer's own
        contribution first), so the order a `Record` promises is imposed here.
        `order_key` stays in the frame rather than being projected away.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        if rel is None:
            raise KeyError(ctype)
        return nw.from_native(rel.order("order_key"))
