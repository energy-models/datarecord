"""The `Revision` node and its ancestry query (design doc §8).

A thin façade over `resolve`: it holds the node's identity and reads the
`data_records` table, knowing nothing about owner maps or parquet layers. A node
has no state beyond its place in the tree - whether its caches are materialised
is a filesystem question, answered by `resolve.materialised` (§8.1, §8.2).
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
from datarecord.schema import Schema
from datarecord.record import Flags, LazyFrames

_ANCESTRY = """
WITH RECURSIVE ancestors(id, parent, depth) AS (
  SELECT id, parent, 0 FROM data_records WHERE id = ?
  UNION ALL
  SELECT d.id, d.parent, ancestors.depth + 1
  FROM data_records d
  JOIN ancestors ON d.id = ancestors.parent
)
SELECT id FROM ancestors ORDER BY depth DESC
"""


def _insert(con: DuckDBPyConnection, parent: UUID | None) -> tuple[UUID, UUID | None]:
    """Insert a new record, letting the DB assign the UUID."""
    row = con.execute(
        "INSERT INTO data_records (parent) VALUES (?) RETURNING id, parent", [parent]
    ).fetchone()
    assert row is not None
    return row


def _fetch(con: DuckDBPyConnection, record_id: UUID) -> tuple[UUID, UUID | None]:
    """Read one record's row, or raise `KeyError`."""
    row = con.execute(
        "SELECT id, parent FROM data_records WHERE id = ?", [record_id]
    ).fetchone()
    if row is None:
        msg = f"No data record {record_id}"
        raise KeyError(msg)
    return row


def ancestry(con: DuckDBPyConnection, record_id: UUID) -> list[UUID]:
    """Record ids along the root->node path, root first - resolution order (§8.2).

    The whole path. Truncating it at the nearest materialised node is the
    reader's business (`resolve.ancestry_to_read`), since whether a node's
    caches exist is a fact about the filesystem rather than about the tree.
    """
    return [r[0] for r in con.execute(_ANCESTRY, [record_id]).fetchall()]


class Revision(BaseModel):
    """A node in the tree of layers.

    Each record adds one parquet store layer on top of its parent's; the
    record's data is the resolution of its layer over its ancestors' (§8).
    The layer location derives from `id` via `layer_dir`, it is not stored.

    Holds the connection it was created/loaded with (`con`), so every method
    below reuses it without needing it passed at every call. It is a private
    attribute, not a field: it never round-trips through (de)serialization
    (e.g. across a Prefect process boundary) - a record that comes back
    without one lazily reattaches the process-level default on next access.
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
        """This record's `NodeCache`, built once from its ancestry (§8.2).

        The ancestry is truncated at the deepest materialised ancestor, so a
        deep tree resolves from few entries. Cached rather than rebuilt per
        call: layers are write-once (§8.1), so the only thing that can change
        the truncation point is `materialise`, which clears this itself.
        """
        if self._node_cache is None:
            full = ancestry(self.con, self.id)
            to_read = resolve.ancestry_to_read(full, self.con)
            self._node_cache = NodeCache(self.id, to_read, self.con)
        return self._node_cache

    @property
    def store(self) -> "LayeredRecord":
        """This record's resolved overlay as a `Record` (§9.3).

        The framework-agnostic view of a record: the same interface a plain
        parquet directory satisfies, so a consumer need not know the record is
        an overlay at all. `node_cache` remains the DuckDB-shaped view, which
        `datarecord.tools` still builds from (§12).
        """
        return LayeredRecord(self.node_cache)

    # -- tree ---------------------------------------------------------------

    @classmethod
    def create(
        cls, con: DuckDBPyConnection | None = None, parent: UUID | None = None
    ) -> Self:
        """Insert a new record, letting the DB assign the UUID."""
        con = con or default_connection()
        id_, parent_ = _insert(con, parent)
        record = cls(id=id_, parent=parent_)
        record._con = con
        return record

    @classmethod
    def get(cls, record_id: UUID, con: DuckDBPyConnection | None = None) -> Self:
        """Load a record by id."""
        con = con or default_connection()
        id_, parent = _fetch(con, record_id)
        record = cls(id=id_, parent=parent)
        record._con = con
        return record

    def child(self) -> Self:
        """Branch a new record off this one (§8.2).

        Any node may be a parent: a layer is write-once, so a base cannot
        shift under its descendants.
        """
        return type(self).create(self.con, parent=self.id)

    def materialise(self) -> None:
        """Write this node's caches - owner maps and resolved dims (§8.2).

        A policy rather than a lifecycle step, and purely additive: it changes
        no answer, only how many layers a descendant's read touches. Once these
        exist, a read stops here instead of walking further up.
        """
        resolve.materialise(self.id, self.node_cache.ancestry, self.con)
        self._node_cache = None

    # -- read ---------------------------------------------------------------

    def ancestry(self) -> list[UUID]:
        """Record ids along the root->self path, root first (§8.2)."""
        return ancestry(self.con, self.id)

    def relation(self, attribute: str) -> DuckDBPyRelation:
        """The resolved long relation for one input attribute (§9.2)."""
        return self.node_cache.relation(attribute)

    def outputs(self, attribute: str) -> DuckDBPyRelation:
        """This layer's own result attribute; outputs do not overlay (§9.4)."""
        return self.node_cache.outputs(attribute)


@dataclass(frozen=True)
class LayeredRecord:
    """A record's resolved overlay, as a `Record` (§9.3).

    The protocol's shape over `NodeCache`, not a second implementation of it, so
    a member costs what the equivalent `NodeCache` call costs - `flags` in
    particular is free, the owner map having folded it in (§9.1).
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
        return LazyFrames(
            names, lambda attr: nw.from_native(self.node_cache.relation(attr))
        )

    @cached_property
    def outputs(self) -> LazyFrames:
        names = tuple(self.node_cache.output_names())
        return LazyFrames(
            names, lambda attr: nw.from_native(self.node_cache.outputs(attr))
        )

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Straight off the `inputs` owner map, which folded these in for free (§9.1)."""
        return {
            attribute: Flags(varies, broadcast, breakpoints)
            for attribute, (varies, broadcast, breakpoints) in (
                self.node_cache.attributes_of(ctype).items()
            )
        }

    # -- frames, ordered by the map's `order_key` (§9.3) ---------------------

    def _component_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.component_frame(ctype), ctype)

    def _connection_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.connection_frame(ctype), ctype)

    def _ordered(self, rel: DuckDBPyRelation | None, ctype: str) -> nw.LazyFrame:
        """`rel` in member order, which for an overlay means sorted by `order_key`.

        The fold's own output has no order (its union puts a layer's own
        contribution first), so the order a `Record` promises is imposed here.
        `order_key` stays in the frame rather than being projected away (§9.1).
        """
        if rel is None:
            raise KeyError(ctype)
        return nw.from_native(rel.order("order_key"))
