"""DuckDB connection setup for the record layer.

Owns the `revisions` metadata table and the `layer_dir` path convention
that maps a record UUID to its record location. The connection is passed as a
parameter throughout, never a module global, so each test can open its own
`:memory:` connection.

Nothing here knows about a modelling framework: the entity-type axis is typed
as the *schema* declares it, whatever a tool's registry holds, so a record whose
types no tool recognises still reads and it is a tool's `verify` that reports
them.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

import json
import os
from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from functools import partial, reduce
from glob import glob
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import UUID
from weakref import WeakKeyDictionary

import duckdb
import narwhals as nw
import narwhals._duckdb.utils as _nw_duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, FunctionExpression
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star
from narwhals._utils import Version


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

# Where layers live; `layer_dir(id)` derives every record path from it (https://energy-models.github.io/datarecord/design/module-layout/).
DEFAULT_BASE_URI = os.environ.get("DATARECORD_BASE_URI", "")

# The record root each connection was opened against (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record). A connection is
# already scoped to one record - `connect` registers its `layer_dir` macro from
# this root - so the schema beside those layers is a property of the connection
# too, and reading it needs no separate parameter threaded through the fold.
# Weak, so a closed connection does not pin its entry.
_BASE_URIS: MutableMapping[DuckDBPyConnection, str] = WeakKeyDictionary()


def base_uri_of(con: DuckDBPyConnection) -> str:
    """The record root `con` was opened against, or the process default.

    A connection not opened through `connect` - one a caller made itself -
    falls back to `DEFAULT_BASE_URI`, which is what every path helper here
    already does when passed no explicit root.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    return _BASE_URIS.get(con, DEFAULT_BASE_URI)


def schema_uri(base_uri: str | None = None) -> str:
    """Where a layered record's one schema lives: beside `layers/`, not in it.

    One schema for the whole tree. A layer directory holds only data, which is
    what keeps it a plain parquet directory a tool knowing nothing about layering
    can read.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    return f"{base.rstrip('/')}/manifest.json" if base else "manifest.json"


def layer_dir(revision_id: UUID | str, base_uri: str | None = None) -> str:
    """Return the record directory for `revision_id`, with a trailing slash.

    The layer location is derived from the UUID, never stored, so
    changing the layout is a change in this one function and its SQL macro.
    This is a plain PyPSA parquet directory: only the layer's own
    contribution lives at the top level, so a non-blocks reader sees a normal
    record. Derived caches (the owner map, resolved dims) go in the
    `resolved/` subdirectory, which no single-level glob reaches.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    - [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
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
    plain parquet directory a foreign reader can consume.

    Only the layer's *inputs* are write-once, then: `materialise` writes here
    after the fact, which invalidates nothing because results and caches are
    derived rather than depended on.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    return f"{layer_dir(revision_id, base_uri)}resolved/"


def _register_macros(con: DuckDBPyConnection, base_uri: str) -> None:
    """Register `layer_dir` so SQL composes record paths inline.

    Notes
    -----
    - [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
    """
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
        Root of the record tree; `layer_dir` derives every path from it.

    Notes
    -----
    - [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
    """
    con = duckdb.connect(database)
    base = DEFAULT_BASE_URI if base_uri is None else base_uri
    # `httpfs` and the S3 secret are only meaningful for remote layers, and
    # `PROVIDER credential_chain` probes instance-metadata endpoints that hang
    # for ~2 minutes where none exist. Never pay that for a local record.
    if "://" in base:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            # S3 credentials come from the environment, never hard-coded (https://energy-models.github.io/datarecord/design/module-layout/).
            con.execute(
                "CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain)"
            )
        except duckdb.Error:
            pass
    con.execute(CREATE_TABLE)
    _register_macros(con, base)
    # Remembered so `read_schema` finds the manifest beside *this* record's
    # layers rather than the process default's (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record).
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

    Notes
    -----
    A local miss is answered without asking DuckDB at all. Inside a transaction
    a failed read *aborts* it, and catching the exception here does not undo
    that - every later statement on the connection would fail until a rollback,
    which would discard whatever the transaction had staged. A missing kind is
    ordinary (an attribute a layer never wrote), so it must not depend on being
    outside one.
    """
    if "://" not in uri and not (
        glob(uri) if any(c in uri for c in "*?[") else Path(uri).exists()
    ):
        return None
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


def parquet_names(dir_uri: str, con: DuckDBPyConnection) -> set[str]:
    """Basenames of the `*.parquet` files directly under `dir_uri`.

    One listing regardless of how many files exist there, local or remote -
    what a caller otherwise probing one filename at a time (`try_read_parquet`
    per candidate) should glob instead.
    """
    rows = con.sql(
        "SELECT file FROM glob(?)", params=[f"{dir_uri}*.parquet"]
    ).fetchall()
    return {row[0].rsplit("/", 1)[-1] for row in rows}


def union_all_by_name(
    rels: Sequence[DuckDBPyRelation], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Fold relations pairwise through `UNION ALL BY NAME`.

    The local names `u` and `rel` are load-bearing: DuckDB's replacement scan
    binds them by *name* out of this frame's locals, so the SQL below reads
    them despite them looking unused. Renaming either silently returns a short
    union; `test_union_all_by_name_folds_every_relation` pins it.

    Not `con.register`: measured at 2.2-2.6x slower here, widening with layer
    count (the deep-overlay case materialised caches exist to make cheap), and it leaves a
    named view per call on a connection that outlives the fold.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/read-path/#resolving-a-relation)
    """
    u = rels[0]
    for rel in rels[1:]:  # noqa: F841 - bound by name in the SQL below
        u = con.sql("FROM u UNION ALL BY NAME FROM rel")
    return u


def ex_all(exprs: Iterable[Expression]) -> Expression:
    """AND every expression in `exprs` together."""
    return reduce(lambda x, y: x & y, exprs)


def null_safe(alias_a: str, alias_b: str, columns: Iterable[str]) -> Expression:
    """NULL-safe equality on `columns`, between two aliased relations.

    What an address coordinate is matched on everywhere: a row exists or it does
    not, so NULL means "this key has no value there" and must match the same
    NULL on the other side, which a plain `=` never does.
    """
    return ex_all(
        sql(f"{col(alias_a, c)} IS NOT DISTINCT FROM {col(alias_b, c)}")
        for c in columns
    )


def broadcast_match(
    alias_a: str, alias_b: str, fixed: Iterable[str], dims: Iterable[str]
) -> Expression:
    """NULL-safe equality on `fixed`, broadcast-OR on `dims`.

    A raw row's `dim = NULL` means "every value of `dim`", so it must match
    regardless of the resolved side's value there, unlike the `IS NOT DISTINCT
    FROM` of `null_safe` which only matches NULL against NULL. `alias_a` is the
    broadcasting side.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    match = null_safe(alias_a, alias_b, fixed)
    for dim in dims:
        match = match & (
            col(alias_a, dim).isnull()
            | sql(f"{col(alias_a, dim)} IS NOT DISTINCT FROM {col(alias_b, dim)}")
        )
    return match


def distinct_values(
    rel: DuckDBPyRelation, column: str, *, order: bool = True
) -> tuple[Any, ...]:
    """`rel`'s distinct values of `column`, as a tuple.

    What keys a `LazyFrames` built over a relation: the values are read from a
    column rather than from a directory listing, so one code path serves a local
    directory and a remote prefix alike.
    """
    projected = rel.project(column).distinct()
    if order:
        projected = projected.order(column)
    return tuple(r[0] for r in projected.fetchall())


def as_relation(frame: nw.LazyFrame, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """One narwhals frame as a DuckDB relation, without collecting where possible.

    A DuckDB-backed frame is already a plan, so it passes straight through; any
    other backend is collected to arrow and re-registered. The one boundary where
    a native representation is reached, shared by the write and edit paths.
    """
    native = frame.to_native()
    if isinstance(native, DuckDBPyRelation):
        return native
    arrow = frame.collect(backend="pyarrow").to_native()  # noqa: F841 - bound by name
    return con.sql("FROM arrow")


def ensure_local_dir(uri: str, *, parent: bool = False) -> None:
    """Create `uri`'s directory where it is a local path, a no-op for a remote one.

    A remote store needs no directory created; a local write does, and a record
    that wrote nothing to its layer has no directory yet either.

    Parameters
    ----------
    parent
        `uri` names a file whose directory is created, rather than the directory
        itself.
    """
    if "://" in uri:
        return
    path = Path(uri)
    (path.parent if parent else path).mkdir(parents=True, exist_ok=True)


def struct_of(fields: Mapping[str, Expression]) -> Expression:
    """A struct expression: field name to the expression for its value.

    A struct rather than a column per field, because it comes back as a dict
    keyed by name: the caller filters it by name instead of counting columns
    into a positional slice.

    `struct_pack` is what says this in DuckDB, and the field names are its
    *keyword* arguments - which `FunctionExpression` cannot pass, being
    positional-only. So the call is assembled as text here and the values are
    interpolated as the expressions they already are, keeping SQL text out of
    every caller.

    Never empty, DuckDB having no empty struct.
    """
    packed = ", ".join(f'"{name}" := {value}' for name, value in fields.items())
    return sql(f"struct_pack({packed})")


class DuckTypes:
    """Builds DuckDB types and typed shapes from narwhals dtypes, just in time.

    Built once per connection and called per dtype - the constructor's
    relation is where a timezone-aware `Datetime` resolves its zone (DuckDB
    keeps timezone on the connection, not the dtype), and is fetched at most
    once no matter how many dtypes this instance translates.

    Reaches into narwhals' private DuckDB backend
    (`narwhals._duckdb.utils.narwhals_to_native_dtype`) rather than a local
    translation table - unversioned within the `narwhals>=2,<3` pin, so
    `pixi run test` is what catches a break, not a type error here.

    Parameters
    ----------
    rel_or_con
        A connection is queried for the throwaway relation
        `DeferredTimeZone` needs; a relation already in hand is used as-is,
        skipping that query.
    """

    deferred_tz: _nw_duckdb.DeferredTimeZone
    con: DuckDBPyConnection | None

    def __init__(self, rel_or_con: DuckDBPyRelation | DuckDBPyConnection):
        if isinstance(rel_or_con, DuckDBPyConnection):
            self.con = rel_or_con
            rel = rel_or_con.sql("SELECT 1")
        else:
            self.con = None
            rel = rel_or_con
        self.deferred_tz = _nw_duckdb.DeferredTimeZone(rel)

    def __call__(self, dtype: nw.dtypes.DType) -> duckdb.sqltypes.DuckDBPyType:
        """`dtype`'s DuckDB type."""
        return _nw_duckdb.narwhals_to_native_dtype(
            dtype, Version.MAIN, self.deferred_tz
        )

    def lit(self, value: Any, dtype: nw.dtypes.DType) -> Expression:
        """`value`, cast to `dtype`'s DuckDB type - a typed literal."""
        return lit(value).cast(self(dtype))

    def null(self, dtype: nw.dtypes.DType) -> Expression:
        """A typed NULL, which is one column of a shape-only relation."""
        return self.lit(None, dtype)

    def empty_relation(self, **columns: nw.dtypes.DType) -> DuckDBPyRelation:
        """A row-less relation with `columns`' names and types.

        What a staging table is created from: `create` takes the table's
        shape from the relation, so a shape is built as expressions rather
        than assembled as DDL text. One row of typed NULLs, kept for its
        types and dropped for its rows.

        A tuple rather than a list is what routes `values` to its expression
        overload; the stub types only the scalar one.
        """
        assert self.con is not None
        return self.con.values(
            tuple(self.null(t).alias(n) for n, t in columns.items())
        ).limit(0)  # type: ignore[arg-type]


def fold_axis(
    axes: Sequence[DuckDBPyRelation | None],
    key: tuple[str, ...],
    con: DuckDBPyConnection,
) -> DuckDBPyRelation | None:
    """Fold one dim's axis relation over each layer's, keyed by `key`.

    `axes` is root first, one entry per layer - `None` where that layer has no
    rows for the dim. Last-writer-wins per `key`, which is `Schema.axis_key`, so
    a nested dim is keyed by `(*parents, dim)` and two periods' identically
    labelled timesteps stay distinct. Row order follows the layer that first
    introduced the key.

    `_row` is tagged **per layer, before any union**: `UNION ALL` defines no
    order, so a bare `row_number() OVER ()` over the unioned relation would
    silently scramble which row counts as first-introduced. `_fold_ordered`
    avoids the same pitfall the same way.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/#materialised-node-caches)
    """
    layers = []
    for depth, rel in enumerate(axes):
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
    # `OVER (PARTITION BY ...)` stays text below: DuckDB's expression API has no
    # window construct, so only what the window wraps is built as an expression.
    ranked = union.project(
        star(),
        sql(f"row_number() OVER (PARTITION BY {partition} ORDER BY _depth DESC)").alias(
            "_rank"
        ),
        # The first-introducing (depth, row) pair, as one orderable struct -
        # `min()` over two separate window aggregates would answer "smallest
        # _depth" and "smallest _row" independently, not the pair belonging
        # to the earliest actual row.
        sql(
            f"{fn.min(struct_of({'d': col('_depth'), 'r': col('_row')}))} "
            f"OVER (PARTITION BY {partition})"
        ).alias("_first"),
    )
    return (
        ranked.filter(col("_rank") == lit(1))
        .order("_first")
        .project(star(exclude=["_depth", "_rank", "_row", "_first"]))
    )


def read_json(uri: str) -> dict[str, Any] | None:
    """Read one JSON file, or `None` if it doesn't exist (e.g. an undeclared schema).

    Only a genuine miss (local `FileNotFoundError`, remote 404/403) maps to
    `None` - any other failure raises rather than silently reading as absent.
    """
    try:
        if "://" in uri:
            with urlopen(uri) as fh:  # noqa: S310 - record URIs are derived, not user input
                return json.load(fh)
        with open(uri) as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except HTTPError as e:
        if e.code in (403, 404):  # 403: S3's "missing key" without ListBucket
            return None
        raise
