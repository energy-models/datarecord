# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Materialised node caches: a node's dim owners, dead labels and owner map, on disk.

Notes
-----
- [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
- [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from contextlib import nullcontext
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from datarecord.duck import ensure_local_dir, write_parquet
from datarecord.resolve.axes import Axes
from datarecord.resolve.fold import Fold
from datarecord.resolve.owner_map import _checkpointed
from datarecord.resolve.sources import LayerSource, ParquetLayer

if TYPE_CHECKING:
    from datarecord.store.manifest import Manifest
    from datarecord.store.store import Store


def materialised(revision_id: UUID, store: Store) -> bool:
    """Whether `revision_id`'s node caches are materialised.

    A node's caches are written together (`materialise`), the map last, so
    one map answers for the dim owners and dead labels beside it.

    This is a filesystem question rather than recorded state: layers are
    write-once, so a materialised cache is valid forever and its
    presence is the whole answer.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    return store.files.exists(store.owner_map_uri(revision_id))


def sources_to_read(lineage: list[Manifest], store: Store) -> list[LayerSource]:
    """`lineage` as the sources to fold, truncated at the deepest materialised node.

    A materialised owner map is already folded over everything above it, so
    nothing further up need be read. The node stopped at stays a plain
    `ParquetLayer`; its `materialised()` yields the base `Fold` the live fold
    starts from, so the fold folds only the layers below it.

    Only proper ancestors count: the node being resolved is always read from its
    own layer, since stopping *at* it would return its cached answer instead of
    resolving it.

    Parameters
    ----------
    lineage
        The manifests root first, ending in the node being resolved.

    Returns
    -------
    list of LayerSource
        Root first, ending in the node's own layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    start = next(
        (
            depth
            for depth in range(len(lineage) - 2, -1, -1)
            if materialised(lineage[depth].id, store)
        ),
        0,
    )
    return [ParquetLayer(manifest, store) for manifest in lineage[start:]]


def materialise(
    revision_id: UUID, sources: Sequence[LayerSource], store: Store
) -> None:
    """Write `revision_id`'s node caches: owner map, dim owners and dead labels.

    Purely additive. It changes no answer a read would give, only how many
    layers a read touches to reach it: once these files exist, a descendant's
    fold stops here rather than walking further up (`sources_to_read`).

    Safe to call more than once, and safe to call on any node - layers are
    write-once, so what is folded here cannot later become stale. Two writers
    write the same content, so racing is harmless. Where the workspace holds
    the node's fold as tables (`Axes.held`), they are written rather than folded
    again.

    `owner_map.parquet` is what `materialised` reads as the whole
    cache's presence, so it is written last: a reader that sees it finds the
    rest, and a write that fails part-way leaves the node unmaterialised.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/reference/layers/#write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    if materialised(revision_id, store):
        return
    con = store.workspace.con
    _materialise_dims(revision_id, sources, store)
    held = Fold.held(sources[-1])
    fold = _checkpointed(sources) if held is None else nullcontext(held.owner_map)
    with fold as owner_map:
        _to_parquet(owner_map, store.owner_map_uri(revision_id), con)


def _to_parquet(rel: DuckDBPyRelation, uri: str, con: DuckDBPyConnection) -> None:
    """`write_parquet` in order, never visible half-written where `uri` is local.

    A `COPY` creates the file at its final path before it is complete, and
    a reader meeting it mid-write fails rather than misses, so a local file is
    written under a temporary name and renamed into place.
    """
    if "://" in uri:
        write_parquet(rel, uri, con, ordered=True)
        return
    tmp = f"{uri}.{uuid4().hex}.tmp"
    write_parquet(rel, tmp, con, ordered=True)
    os.replace(tmp, uri)


def _materialise_dims(
    revision_id: UUID, sources: Sequence[LayerSource], store: Store
) -> None:
    """Write this node's dim owners and dead labels into its node cache, not its layer.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/reference/resolution/#materialised-revisions)
    """
    con = store.workspace.con
    axes = Axes.held(sources[-1]) or Axes.resolve(sources)
    dead = f"{store.resolved_dir(revision_id)}dead/"
    ensure_local_dir(dead if axes.dead else store.resolved_dir(revision_id))
    for name, rel in axes.dead.items():
        _to_parquet(rel, f"{dead}{name}.parquet", con)
    _to_parquet(axes.owners(con), store.dim_owners_uri(revision_id), con)
