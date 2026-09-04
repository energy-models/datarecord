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

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import Self, cast
from uuid import UUID

import narwhals as nw
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from pydantic import BaseModel, PrivateAttr

from datarecord.duck import default_connection, read_json
from datarecord.layered import resolve
from datarecord.layered.resolve import NodeCache
from datarecord.layered.sources import DirectorySource, LayerSource
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
    row = con.execute(
        "SELECT id, parent FROM revisions WHERE id = ?", [revision_id]
    ).fetchone()
    if row is None:
        msg = f"No revision {revision_id}"
        raise KeyError(msg)
    return row


def ancestry(con: DuckDBPyConnection, revision_id: UUID) -> list[UUID]:
    """Revision ids along the root->node path, root first - resolution order.

    The whole path. Truncating it at the nearest materialised node is the
    reader's business (`resolve.sources_to_read`), since whether a node's
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
            sources = resolve.sources_to_read(full, self.con)
            self._node_cache = NodeCache(self.id, sources, self.con)
        return self._node_cache

    @property
    def record(self) -> Record:
        """This revision's resolved view, as a `Record`.

        The framework-agnostic view: narwhals frames, with no sign of how many
        layers were folded to produce them. `node_cache` remains the
        DuckDB-shaped view, which `datarecord.tools` still builds from.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        return Record(self.node_cache)

    # -- tree ---------------------------------------------------------------

    @classmethod
    def create(
        cls, con: DuckDBPyConnection | None = None, parent: UUID | None = None
    ) -> Self:
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
        resolve.materialise(self.id, self.node_cache.sources, self.con)
        self._node_cache = None

    # -- read ---------------------------------------------------------------

    def ancestry(self) -> list[UUID]:
        """Revision ids along the root->self path, root first.

        Notes
        -----
        - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
        """
        return ancestry(self.con, self.id)


def _stable_cache(method: Callable[[Record], LazyFrames]) -> property:
    """A `Record` member cached only while the fold behind it cannot go stale.

    Every one of these answers a *key set*, which is a question about which
    layers exist rather than about their rows - so it is cacheable exactly when
    `NodeCache.stable` holds. Past a staging area it is not: an `add` or a `set`
    naming a new attribute changes the answer, and a cached tuple of names would
    hide the edit that was just made.

    The same rule `NodeCache.dims` applies to the fold itself, one level up, so
    a `WorkingRecord` inherits these unchanged rather than overriding them.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    """
    attr = f"_cached_{method.__name__}"

    @wraps(method)
    def get(self: Record) -> LazyFrames:
        if not self.node_cache.stable:
            return method(self)
        try:
            return cast("LazyFrames", object.__getattribute__(self, attr))
        except AttributeError:
            value = method(self)
            object.__setattr__(self, attr, value)
            return value

    return property(get)


@dataclass(frozen=True)
class Record:
    """A record's resolved view, as narwhals frames.

    The narwhals interface over one `NodeCache`, not a second implementation of
    it: a member costs what the equivalent `NodeCache` call costs, and `flags`
    in particular is free, the owner map having folded it in.

    One class for every backing, because a fold over one source degenerates to
    a scan of it - so a plain parquet directory is read as the one layer it is
    (`at`) rather than by a second code path. `RecordLike` is the protocol a
    caller annotates against; this is the class a caller constructs.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """

    node_cache: NodeCache

    @classmethod
    def over(
        cls,
        *sources: LayerSource,
        con: DuckDBPyConnection,
        declared: Schema | None = None,
    ) -> Record:
        """A record folded over `sources`, root first.

        The last source's `layer_id` names the record, which for a layer tree is
        the revision being resolved. `con` is explicit because a source is only
        obliged to hand over rows - where it reads them is its own business, and
        the protocol carries no connection. `declared` likewise: a source hands
        over rows, not a schema.
        """
        if not sources:
            msg = "a `Record` needs at least one source to fold"
            raise ValueError(msg)
        return cls(NodeCache(sources[-1].layer_id, list(sources), con, declared))

    @classmethod
    def at(cls, uri: str, con: DuckDBPyConnection | None = None) -> Record:
        """A plain parquet directory, read as the one layer it is.

        Any directory in the layer layout - a single layer of a tree, or a
        standalone record `write_record` produced. It needs no `revisions` row
        and no tree: being a layer layout is what the fold requires, and that is
        what a record directory is. Over one source the fold degenerates to a
        scan, so this costs a scan and not a resolution.

        A **standalone** record carries its own `manifest.json`, being one whole
        record rather than a layer of one, and that is what it is read under -
        so such a directory reads the same through any connection. A single
        layer directory has none, and is read under the connection's root.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/format/)
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        """
        con = con or default_connection()
        source = DirectorySource(uri, con)
        raw = read_json(source.uri("manifest.json"))
        declared = None if raw is None else Schema.model_validate(raw)
        return cls.over(source, con=con, declared=declared)

    @property
    def schema(self) -> Schema:
        return self.node_cache.schema

    @property
    def con(self) -> DuckDBPyConnection:
        return self.node_cache.con

    @_stable_cache
    def dims(self) -> LazyFrames:
        axes = self.node_cache.dims.axes
        return LazyFrames(tuple(axes), lambda dim: nw.from_native(axes[dim]))

    @_stable_cache
    def entity_types(self) -> LazyFrames:
        types = tuple(sorted(self.node_cache.entity_types()))
        return LazyFrames(types, self._entity_type_frame)

    @_stable_cache
    def groups(self) -> LazyFrames:
        """Each declared group's rows, keyed by group - one frame each.

        Only groups some layer wrote a row of; a declared group nothing
        populates is absent rather than present-and-empty.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        groups = tuple(
            g
            for g in self.node_cache.schema.groups
            if self.node_cache.group(g) is not None
        )
        return LazyFrames(groups, self._group_frame)

    @_stable_cache
    def attributes(self) -> LazyFrames:
        names = tuple(self.node_cache.attribute_names())
        return LazyFrames(
            names, lambda attr: nw.from_native(self.node_cache.relation(attr))
        )

    @_stable_cache
    def outputs(self) -> LazyFrames:
        names = tuple(self.node_cache.output_names())
        return LazyFrames(
            names, lambda attr: nw.from_native(self.node_cache.outputs(attr))
        )

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Straight off the `inputs` owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return self.node_cache.attributes_of(ctype)

    # -- frames, in member order (the resolved file's row order) (https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) --

    def _entity_type_frame(self, ctype: str) -> nw.LazyFrame:
        return self._frame(self.node_cache.entity_type_frame(ctype), ctype)

    def _group_frame(self, group: str) -> nw.LazyFrame:
        return self._frame(self.node_cache.group_frame(group), group)

    def _frame(self, rel: DuckDBPyRelation | None, key: str) -> nw.LazyFrame:
        """`rel` as a frame; it already carries member order as its row order.

        The resolved relation is folded in first-introduced member order and read
        back order-preserving (`fold_axis`), so a `Record` promises that order
        without a re-sort and without a persisted `order_key`.

        Parameters
        ----------
        key
            What the frame was looked up by - a component type or a group name -
            so a miss raises the `KeyError` the caller asked with.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        if rel is None:
            raise KeyError(key)
        return nw.from_native(rel)
