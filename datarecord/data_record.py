"""The `DataRecord` node (design doc §8, §13).

A thin façade: it holds the node's identity and delegates to the modules that
do the work (`records`, `node_cache`, `write`). Those modules take ids rather
than a record, so nothing imports back into this one - which is also why the
resolved reads live on `NodeCache` rather than here: a tool needs them too.
"""

from typing import TYPE_CHECKING, Self
from uuid import UUID

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from pydantic import BaseModel, PrivateAttr

from datarecord import node_cache, records, write
from datarecord.duck import default_connection
from datarecord.node_cache import NodeCache
from datarecord.store import LayeredStore

if TYPE_CHECKING:
    import pypsa


class DataRecord(BaseModel):
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
            full = records.ancestry(self.con, self.id)
            ancestry = node_cache.ancestry_to_read(full, self.con)
            self._node_cache = NodeCache(self.id, ancestry, self.con)
        return self._node_cache

    @property
    def store(self) -> LayeredStore:
        """This record's resolved overlay as a `Store` (§9.3).

        The framework-agnostic view of a record: the same interface a plain
        parquet directory satisfies, so a consumer need not know the record is
        an overlay at all. `node_cache` remains the DuckDB-shaped view, which
        `datarecord.tools` still builds from (§12).
        """
        return LayeredStore(self.node_cache)

    # -- tree ---------------------------------------------------------------

    @classmethod
    def create(
        cls, con: DuckDBPyConnection | None = None, parent: UUID | None = None
    ) -> Self:
        """Insert a new record, letting the DB assign the UUID."""
        con = con or default_connection()
        id_, parent_ = records.insert(con, parent)
        record = cls(id=id_, parent=parent_)
        record._con = con
        return record

    @classmethod
    def get(cls, record_id: UUID, con: DuckDBPyConnection | None = None) -> Self:
        """Load a record by id."""
        con = con or default_connection()
        id_, parent = records.fetch(con, record_id)
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
        node_cache.materialise(self.id, self.node_cache.ancestry, self.con)
        self._node_cache = None

    # -- read ---------------------------------------------------------------

    def ancestry(self) -> list[UUID]:
        """Record ids along the root->self path, root first (§8.2)."""
        return records.ancestry(self.con, self.id)

    def relation(self, attribute: str) -> DuckDBPyRelation:
        """The resolved long relation for one input attribute (§9.2)."""
        return self.node_cache.relation(attribute)

    def outputs(self, attribute: str) -> DuckDBPyRelation:
        """This layer's own result attribute; outputs do not overlay (§9.4)."""
        return self.node_cache.outputs(attribute)

    # -- write ---------------------------------------------------------------

    def add_patch(self, n: "pypsa.Network", n_old: "pypsa.Network") -> Self:
        """Write `n.diff(n_old)` into a new child layer (§12) - not implemented."""
        return write.add_patch(self, n, n_old, self.con)
