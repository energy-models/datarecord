# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Materialised node caches: a node's dim owners, dead labels and owner map, on disk.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
- [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from datarecord.duck import ensure_local_dir
from datarecord.resolve.coords import resolve_coords
from datarecord.resolve.fold import read_owner_map
from datarecord.resolve.owner_map import LAYER_UUID_TYPE, _checkpointed
from datarecord.resolve.sources import LayerSource, ParquetLayer
from datarecord.schema.schema import LAYER_COLUMN, Schema
from datarecord.store.manifest import read_schema

if TYPE_CHECKING:
    from datarecord.store.store import Store


def materialised(revision_id: UUID, store: Store, schema: Schema) -> bool:
    """Whether `revision_id`'s node caches are materialised under `schema`.

    A node's caches are written together (`materialise`), the map last, so
    one map answers for the dim owners and dead labels beside it. A map
    keyed otherwise than `schema` keys one is no cache (`read_owner_map`).

    This is a filesystem question rather than recorded state: layers are
    write-once, so a materialised cache is valid forever and its
    presence is the whole answer.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    return read_owner_map(revision_id, store.workspace.con, schema, store) is not None


def sources_to_read(
    ancestry: list[UUID], store: Store, schema: Schema
) -> list[LayerSource]:
    """`ancestry` as the sources to fold, truncated at the deepest materialised node.

    A materialised owner map is already folded over everything above it, so
    nothing further up need be read: the truncation is *fewer sources* rather
    than a shorter list of UUIDs. The node stopped at stays a plain
    `ParquetLayer`; its `materialised(con)` yields the base `Fold` the live fold
    starts from, so the fold folds only the layers below it.

    Only proper ancestors count: the node being resolved is always read from its
    own layer, since stopping *at* it would return its cached answer instead of
    resolving it.

    Parameters
    ----------
    ancestry
        Root-first, ending in the node being resolved (`store.ancestry`).
    store
        Where the layers are.
    schema
        The record's one manifest, carried by every source so `write_layer`
        can read it off the layer it is handed.

    Returns
    -------
    list of LayerSource
        Root first, ending in the node's own layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    for depth in range(len(ancestry) - 2, -1, -1):
        if materialised(ancestry[depth], store, schema):
            return [ParquetLayer(uid, schema, store) for uid in ancestry[depth:]]
    return [ParquetLayer(uid, schema, store) for uid in ancestry]


def materialise(
    revision_id: UUID, sources: Sequence[LayerSource], store: Store
) -> None:
    """Write `revision_id`'s node caches: owner map, dim owners and dead labels.

    Purely additive. It changes no answer a read would give, only how many
    layers a read touches to reach it: once these files exist, a descendant's
    fold stops here rather than walking further up (`sources_to_read`).

    Safe to call more than once, and safe to call on any node - layers are
    write-once, so what is folded here cannot later become stale. Two writers
    write the same content, so racing is harmless.

    `owner_map.parquet` is what `materialised` reads as the whole
    cache's presence, so it is written last: a reader that sees it finds the
    rest, and a write that fails part-way leaves the node unmaterialised.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/tree/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    schema = read_schema(store)
    con = store.workspace.con
    _materialise_dims(revision_id, sources, store, schema)
    with _checkpointed(sources, con, schema) as owner_map:
        _to_parquet(owner_map, store.owner_map_uri(revision_id))


def _to_parquet(rel: DuckDBPyRelation, uri: str) -> None:
    """`rel.to_parquet(uri)`, never visible half-written where `uri` is local.

    `to_parquet` creates the file at its final path before it is complete, and
    a reader meeting it mid-write fails rather than misses, so a local file is
    written under a temporary name and renamed into place.
    """
    if "://" in uri:
        rel.to_parquet(uri)
        return
    tmp = f"{uri}.{uuid4().hex}.tmp"
    rel.to_parquet(tmp)
    os.replace(tmp, uri)


def _to_parquet_unless_empty(
    rel: DuckDBPyRelation, uri: str, con: DuckDBPyConnection
) -> None:
    """`_to_parquet`, writing nothing where `rel` has no row.

    `rel` is evaluated once, into a temporary table, rather than once to
    probe and again to write.
    """
    table = f"_unless_empty_{uuid4().hex}"
    rel.create(table)
    try:
        rows = con.table(table)
        if rows.limit(1).fetchall():
            _to_parquet(rows, uri)
    finally:
        con.execute(f"DROP TABLE {table}")


def _materialise_dims(
    revision_id: UUID,
    sources: Sequence[LayerSource],
    store: Store,
    schema: Schema,
) -> None:
    """Write this node's dim owners and dead labels into its node cache, not its layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    """
    con = store.workspace.con
    coords = resolve_coords(schema, sources, con)
    dead = f"{store.resolved_dir(revision_id)}dead/"
    ensure_local_dir(dead)
    for name, rel in coords.dead.items():
        _to_parquet_unless_empty(rel, f"{dead}{name}.parquet", con)
    owners = coords.dim_owners
    rows = con.sql(
        f"SELECT unnest($dims::VARCHAR[]) AS dim, "
        f"unnest($layers::VARCHAR[])::{LAYER_UUID_TYPE} AS {LAYER_COLUMN}",
        params={"dims": list(owners), "layers": [str(u) for u in owners.values()]},
    )
    _to_parquet(rows, store.dim_owners_uri(revision_id))
