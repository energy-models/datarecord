"""DuckDB connection setup for the record layer (design doc §13).

Owns the `revisions` metadata table and the `layer_dir` path convention
that maps a record UUID to its record location. The connection is passed as a
parameter throughout, never a module global, so each test can open its own
`:memory:` connection.

Nothing here knows about a modelling framework: `component_type` and
`attribute` are plain `VARCHAR`, so a record whose types no tool recognises
still reads, and it is a tool's `verify` that reports it (§5, §12).
"""

import os
from collections.abc import Callable, Iterable, MutableMapping, Sequence
from functools import partial, reduce
from uuid import UUID
from weakref import WeakKeyDictionary

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, FunctionExpression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star


class _Functions:
    """`fn.bool_or(x)` for `FunctionExpression("bool_or", x)`.

    Attribute access reads closer to the SQL it builds than a name-as-string
    call does, and keeps aggregates out of `SQLExpression` strings, where a
    column name would have to be quoted by hand.
    """

    def __getattr__(self, name: str) -> Callable[..., Expression]:
        return partial(FunctionExpression, name)


fn = _Functions()

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS revisions (
  id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parent   UUID
)
"""

# Where layers live; `layer_dir(id)` derives every record path from it (§13).
DEFAULT_BASE_URI = os.environ.get("BLOCKS_RECORD_BASE_URI", "")

# The record root each connection was opened against (§5.6). A connection is
# already scoped to one record - `connect` registers its `layer_dir` macro from
# this root - so the schema beside those layers is a property of the connection
# too, and reading it needs no separate parameter threaded through the fold.
# Weak, so a closed connection does not pin its entry.
_BASE_URIS: MutableMapping[DuckDBPyConnection, str] = WeakKeyDictionary()


def base_uri_of(con: DuckDBPyConnection) -> str:
    """The record root `con` was opened against, or the process default (§5.6).

    A connection not opened through `connect` - one a caller made itself -
    falls back to `DEFAULT_BASE_URI`, which is what every path helper here
    already does when passed no explicit root.
    """
    return _BASE_URIS.get(con, DEFAULT_BASE_URI)


def schema_uri(base_uri: str | None = None) -> str:
    """Where a layered record's one schema lives: beside `layers/`, not in it (§5.6).

    One schema for the whole tree. A layer directory holds only data, which is
    what keeps it a plain parquet directory a tool knowing nothing about layering
    can read.
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return f"{base.rstrip('/')}/manifest.json" if base else "manifest.json"


def layer_dir(revision_id: UUID | str, base_uri: str | None = None) -> str:
    """Return the record directory for `revision_id`, with a trailing slash.

    The layer location is derived from the UUID, never stored (§13), so
    changing the layout is a change in this one function and its SQL macro.
    This is a plain PyPSA parquet directory (§3): only the layer's own
    contribution lives at the top level, so a non-blocks reader sees a normal
    record (§13). Derived caches (the owner map, resolved dims) go in the
    `resolved/` subdirectory, which no single-level glob reaches.
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return (
        f"{base.rstrip('/')}/layers/{revision_id}/"
        if base
        else f"layers/{revision_id}/"
    )


def resolved_dir(revision_id: UUID | str, base_uri: str | None = None) -> str:
    """Return the resolved-cache directory for `revision_id`, with a trailing slash.

    A subdirectory of `layer_dir`, not a sibling tree: one directory per record
    holds both what the layer wrote and what the fold derived from it. The
    nesting is safe because every glob into a layer is single-level
    (`inputs/*.parquet`, `dims/*.parquet`), so `resolved/` is invisible to a
    reader that knows nothing about it - which is what keeps a layer directory a
    plain parquet directory a foreign reader can consume (§8.3).

    Only the layer's *inputs* are write-once, then: `materialise` writes here
    after the fact, which invalidates nothing because results and caches are
    derived rather than depended on (§8.1, §8.2).
    """
    return f"{layer_dir(revision_id, base_uri)}resolved/"


def _register_macros(con: DuckDBPyConnection, base_uri: str) -> None:
    """Register `layer_dir` so SQL composes record paths inline (§13)."""
    base = base_uri.rstrip("/")
    prefix = f"{base}/" if base else ""
    con.execute("DROP MACRO IF EXISTS layer_dir")
    con.execute(f"CREATE MACRO layer_dir(id) AS '{prefix}layers/' || id || '/'")


def connect(
    database: str = ":memory:", base_uri: str | None = None
) -> DuckDBPyConnection:
    """Open a connection with the record layer's table and macros.

    Parameters
    ----------
    database
        DuckDB database, `:memory:` by default.
    base_uri
        Root of the record tree; `layer_dir` derives every path from it (§13).
    """
    con = duckdb.connect(database)
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    # `httpfs` and the S3 secret are only meaningful for remote layers, and
    # `PROVIDER credential_chain` probes instance-metadata endpoints that hang
    # for ~2 minutes where none exist. Never pay that for a local record.
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
    # Remembered so `read_schema` finds the manifest beside *this* record's
    # layers rather than the process default's (§5.6).
    _BASE_URIS[con] = base
    return con


_default_con: DuckDBPyConnection | None = None


def default_connection() -> DuckDBPyConnection:
    """The process-level connection, created on first use.

    `Revision` attaches this lazily to any record created/loaded without an
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
        return con.read_parquet(uri, **kwargs)  # type: ignore[arg-type]
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
    """Fold relations pairwise through `UNION ALL BY NAME` (§9.2).

    The local names `u` and `rel` are load-bearing: DuckDB's replacement scan
    binds them by *name* out of this frame's locals, so the SQL below reads
    them despite them looking unused. Renaming either silently returns a short
    union; `test_union_all_by_name_folds_every_relation` pins it.

    Not `con.register`: measured at 2.2-2.6x slower here, widening with layer
    count (the deep-overlay case §8.2 exists to make cheap), and it leaves a
    named view per call on a connection that outlives the fold.
    """
    u = rels[0]
    for rel in rels[1:]:  # noqa: F841 - bound by name in the SQL below
        u = con.sql("FROM u UNION ALL BY NAME FROM rel")
    return u


def ex_all(exprs: Iterable[Expression]) -> Expression:
    """AND every expression in `exprs` together."""
    return reduce(lambda x, y: x & y, exprs)


def dims_dirs(ancestry: list[UUID]) -> list[str]:
    """`dims/`-containing directories for resolving a record's axes (§8.2).

    `ancestry` is root first, ending in the record being resolved and already
    truncated at the deepest materialised ancestor (`ancestry_to_read`). Every
    entry but the last therefore has resolved dims under `resolved/`, while the
    last is the record itself and contributes its layer's raw `dims/`.

    The two live in the same record directory but stay distinct paths -
    `layers/<id>/dims/` against `layers/<id>/resolved/dims/` - so a record read
    as an ancestor and the same record read as itself never alias (§8.2).
    """
    last = len(ancestry) - 1
    return [
        (layer_dir(uid) if depth == last else resolved_dir(uid)) + "dims/"
        for depth, uid in enumerate(ancestry)
    ]


def fold_axis(
    dims_dirs: list[str], filename: str, key: tuple[str, ...], con: DuckDBPyConnection
) -> DuckDBPyRelation | None:
    """Fold a `<dir>/<filename>` axis table over `dims_dirs`, keyed by `key`.

    `dims_dirs` is root first, each entry already resolved by the caller to that
    ancestor's layer or its `resolved/` cache (§8.2). Last-writer-wins per `key`, which
    is `Schema.axis_key` - so a nested dim is keyed by `(*parents, dim)` and two
    periods' identically-labelled timesteps stay distinct (§5.4). Row order
    follows the directory that first introduced the key (§3.4).

    `_row` is tagged **per directory, before any union**: `UNION ALL` defines no
    order, so a bare `row_number() OVER ()` over the unioned relation would
    silently scramble which row counts as first-introduced. `_fold_ordered`
    avoids the same pitfall the same way.
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
    partition = ", ".join(str(col(c)) for c in key)
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
