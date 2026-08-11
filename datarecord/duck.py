"""DuckDB connection setup for the record layer (design doc §13).

Owns the `data_records` metadata table and the `layer_dir` path convention
that maps a record UUID to its store location. The connection is passed as a
parameter throughout, never a module global, so each test can open its own
`:memory:` connection.

Nothing here knows about a modelling framework: `component_type` and
`attribute` are plain `VARCHAR`, so a record whose types no tool recognises
still reads, and it is a tool's `verify` that reports it (§5, §12).
"""

import os
from collections.abc import Iterable, MutableMapping, Sequence
from functools import reduce
from uuid import UUID
from weakref import WeakKeyDictionary

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS data_records (
  id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parent   UUID
)
"""

# Where layers live; `layer_dir(id)` derives every store path from it (§13).
DEFAULT_BASE_URI = os.environ.get("BLOCKS_RECORD_BASE_URI", "")

# The store root each connection was opened against (§5.6). A connection is
# already scoped to one store - `connect` registers its `layer_dir`/`node_dir`
# macros from this root - so the schema beside those layers is a property of
# the connection too, and reading it needs no separate parameter threaded
# through the fold. Weak, so a closed connection does not pin its entry.
_BASE_URIS: MutableMapping[DuckDBPyConnection, str] = WeakKeyDictionary()


def base_uri_of(con: DuckDBPyConnection) -> str:
    """The store root `con` was opened against, or the process default (§5.6).

    A connection not opened through `connect` - one a caller made itself -
    falls back to `DEFAULT_BASE_URI`, which is what every path helper here
    already does when passed no explicit root.
    """
    return _BASE_URIS.get(con, DEFAULT_BASE_URI)


def schema_uri(base_uri: str | None = None) -> str:
    """Where a layered store's one schema lives: beside `layers/`, not in it (§5.6).

    One schema for the whole tree. A layer directory holds only data, which is
    what keeps it a plain parquet store a tool knowing nothing about layering
    can read.
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return f"{base.rstrip('/')}/manifest.json" if base else "manifest.json"


def layer_dir(record_id: UUID | str, base_uri: str | None = None) -> str:
    """Return the store directory for `record_id`, with a trailing slash.

    The layer location is derived from the UUID, never stored (§13), so
    changing the layout is a change in this one function and its SQL macro.
    This is a plain PyPSA parquet store (§3): only the layer's own
    contribution lives here, so a non-blocks reader sees a normal store
    (§13). Node-derived caches (the owner map, resolved dims) live under
    `node_dir` instead, never inside the layer.
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return f"{base.rstrip('/')}/layers/{record_id}/" if base else f"layers/{record_id}/"


def node_dir(record_id: UUID | str, base_uri: str | None = None) -> str:
    """Return the node-scoped cache directory for `record_id`, with a trailing slash.

    Sibling to `layer_dir`, not nested inside it: caches derived from the
    fold (the owner map, resolved dims) are node state, not layer data, so
    they must never be mistaken for something the layer itself wrote.
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return f"{base.rstrip('/')}/nodes/{record_id}/" if base else f"nodes/{record_id}/"


def _register_macros(con: DuckDBPyConnection, base_uri: str) -> None:
    """Register `layer_dir`/`node_dir` so SQL composes store paths inline (§13)."""
    base = base_uri.rstrip("/")
    prefix = f"{base}/" if base else ""
    con.execute("DROP MACRO IF EXISTS layer_dir")
    con.execute(f"CREATE MACRO layer_dir(id) AS '{prefix}layers/' || id || '/'")
    con.execute("DROP MACRO IF EXISTS node_dir")
    con.execute(f"CREATE MACRO node_dir(id) AS '{prefix}nodes/' || id || '/'")


def connect(
    database: str = ":memory:", base_uri: str | None = None
) -> DuckDBPyConnection:
    """Open a connection with the record layer's table and macros.

    Parameters
    ----------
    database
        DuckDB database, `:memory:` by default.
    base_uri
        Store root; `layer_dir`/`node_dir` derive every path from it (§13).
    """
    con = duckdb.connect(database)
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    # `httpfs` and the S3 secret are only meaningful for remote layers, and
    # `PROVIDER credential_chain` probes instance-metadata endpoints that hang
    # for ~2 minutes where none exist. Never pay that for a local store.
    if "://" in base:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            # S3 credentials come from the environment, never hard-coded (§13).
            con.execute(
                "CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain)"
            )
        except duckdb.Error:
            pass
    con.execute(CREATE_TABLE)
    _register_macros(con, base)
    # Remembered so `read_schema` finds the manifest beside *this* store's
    # layers rather than the process default's (§5.6).
    _BASE_URIS[con] = base
    return con


_default_con: DuckDBPyConnection | None = None


def default_connection() -> DuckDBPyConnection:
    """The process-level connection, created on first use.

    `DataRecord` attaches this lazily to any record created/loaded without an
    explicit `con` (including one deserialized in a new process, e.g. across
    a Prefect task boundary); tests always pass their own connection instead.
    """
    global _default_con
    if _default_con is None:
        _default_con = connect()
    return _default_con


def try_read_parquet(
    uri: str, con: DuckDBPyConnection, **kwargs: object
) -> DuckDBPyRelation | None:
    """`con.read_parquet(uri)`, or `None` if `uri` (possibly a glob) matches nothing.

    Parameters
    ----------
    uri
        Path or glob to read, local or remote.
    con
        Connection to read with.
    **kwargs
        Forwarded to `con.read_parquet`.

    Returns
    -------
    DuckDBPyRelation | None
        The relation, or `None` if `uri` matches no files.

    Raises
    ------
    duckdb.Error
        If the read fails for any reason other than a local or remote miss
        (e.g. DNS/TLS/timeout) - a connection failure should not be mistaken
        for a missing layer.
    """
    try:
        return con.read_parquet(uri, **kwargs)
    except duckdb.HTTPException as e:
        # This duckdb build exposes no status code, only the message ("HTTP
        # Error: ... (HTTP 404 Not Found)"). 403 also counts as a miss: S3
        # signals a missing key that way when the caller lacks ListBucket.
        if "HTTP 404" in str(e) or "HTTP 403" in str(e):
            return None
        raise
    except duckdb.IOException as e:
        if "No files found" in str(e):
            return None
        raise


def union_all_by_name(
    rels: Sequence[DuckDBPyRelation], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Fold relations pairwise through `UNION ALL BY NAME` (§9.2)."""
    u = rels[0]
    for rel in rels[1:]:  # pyright: ignore[reportUnusedVariable]
        u = con.sql("FROM u UNION ALL BY NAME FROM rel")
    return u


def ex_all(exprs: Iterable[Expression]) -> Expression:
    """AND every expression in `exprs` together."""
    return reduce(lambda x, y: x & y, exprs)


def dims_dirs(ancestry: list[UUID]) -> list[str]:
    """`dims/`-containing directories for resolving a record's axes (§8.2).

    `ancestry` is root first, ending in the record being resolved and already
    truncated at the deepest materialised ancestor (`ancestry_to_read`). Every
    entry but the last therefore has resolved dims in its node cache, while the
    last is the record itself and contributes its layer's raw `dims/`.
    """
    last = len(ancestry) - 1
    return [
        (layer_dir(uid) if depth == last else node_dir(uid)) + "dims/"
        for depth, uid in enumerate(ancestry)
    ]


def fold_axis(
    dims_dirs: list[str], filename: str, key: tuple[str, ...], con: DuckDBPyConnection
) -> DuckDBPyRelation | None:
    """Fold a `<dir>/<filename>` axis table over `dims_dirs`, keyed by `key`.

    `dims_dirs` is a list of `dims/`-containing directories, root first - one
    per ancestor, already resolved by the caller to either that ancestor's
    layer (`layer_dir`) or its node cache (`node_dir`), whichever holds the
    relevant axis file (§8.2). Same last-writer-wins rule as `dims/components`
    (design doc §8): a descendant layer may add a new row (e.g. a new
    scenario or period) or replace an existing row's static data, keyed by
    `key` rather than the full row. Row order follows the directory that
    first introduced the key, same as `NodeCache.static`.

    `key` is the axis key `Schema.axis_key` derives, so a nested dim is keyed
    by `(*parents, dim)` (§5.4): `t1` alone names nothing when `timestep` is
    `within` `period`, and keying by the label alone would fold two periods'
    identically-labelled timesteps into one row.

    `_row` is tagged per directory, before any union: a bare
    `row_number() OVER ()` on the relation `union_all_by_name` returns would
    have no defined order (`UNION ALL` gives none), so it would silently
    scramble which row counts as "first introduced" - the same pitfall
    `fold_components` avoids by tagging `_row` pre-union too.
    """
    layers = []
    for depth, dims_dir in enumerate(dims_dirs):
        rel = try_read_parquet(f"{dims_dir}{filename}", con)
        if rel is None:
            continue
        layers.append(
            rel.project(
                lit(depth).alias("_depth"),
                sql("row_number() OVER ()").alias("_row"),
                col("*"),
            )
        )
    if not layers:
        return None

    union = union_all_by_name(layers, con)
    partition = ", ".join(f'"{c}"' for c in key)
    ranked = union.project(
        star(),
        sql(f"row_number() OVER (PARTITION BY {partition} ORDER BY _depth DESC)").alias(
            "_rank"
        ),
        # The first-introducing (depth, row) pair, as one orderable struct -
        # `min()` over two separate window aggregates would answer "smallest
        # _depth" and "smallest _row" independently, not the pair belonging
        # to the earliest actual row.
        sql(f"min({{'d': _depth, 'r': _row}}) OVER (PARTITION BY {partition})").alias(
            "_first"
        ),
    )
    return (
        ranked.filter(col("_rank") == lit(1))
        .order("_first")
        .project(star(exclude=["_depth", "_rank", "_row", "_first"]))
    )
