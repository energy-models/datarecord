"""The `data_records` metadata table (design doc §8.2).

This module owns the node tree as *data*: ids, parent links and ancestry. It
knows nothing about owner maps, parquet layers or any modelling framework, so
every other module in the package can depend on it freely.

A node has no state beyond its place in the tree (§8.2). Whether its caches are
materialised is a question about the filesystem, answered where that is visible
(`node_cache.materialised`), not recorded here.
"""

from uuid import UUID

from duckdb import DuckDBPyConnection

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS data_records (
  id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parent   UUID
)
"""

_ANCESTRY = """
WITH RECURSIVE anc(id, parent, depth) AS (
  SELECT id, parent, 0 FROM data_records WHERE id = ?
  UNION ALL
  SELECT d.id, d.parent, anc.depth + 1
  FROM data_records d
  JOIN anc ON d.id = anc.parent
)
SELECT id FROM anc ORDER BY depth DESC
"""


def insert(con: DuckDBPyConnection, parent: UUID | None) -> tuple[UUID, UUID | None]:
    """Insert a new record, letting the DB assign the UUID."""
    row = con.execute(
        "INSERT INTO data_records (parent) VALUES (?) RETURNING id, parent", [parent]
    ).fetchone()
    assert row is not None
    return row


def fetch(con: DuckDBPyConnection, record_id: UUID) -> tuple[UUID, UUID | None]:
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
    reader's business (`node_cache.ancestry_to_read`), since whether a node's
    caches exist is a fact about the filesystem rather than about the tree.
    """
    return [r[0] for r in con.execute(_ANCESTRY, [record_id]).fetchall()]
