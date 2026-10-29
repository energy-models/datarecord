# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Relation helpers for the record layer.

Each takes the connection it composes on as a parameter; where a connection
comes from is the `Workspace`'s business, and paths are a `Store`'s.

Nothing here knows about a modelling framework: every column is typed as the
*schema* declares it, whatever a tool's registry holds, so a record whose labels
no tool recognises still reads and it is a tool's `verify` that reports them.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [module layout](https://energy-models.github.io/datarecord/design/package/)
"""

import os
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from functools import partial, reduce
from glob import glob
from pathlib import Path
from typing import Any

import duckdb
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import (
    DuckDBPyConnection,
    DuckDBPyRelation,
    Expression,
    FunctionExpression,
)
from duckdb import SQLExpression as sql
from duckdb.sqltypes import DuckDBPyType


class _Functions:
    """`fn.bool_or(x)` for `FunctionExpression("bool_or", x)`.

    Attribute access reads closer to the SQL it builds than a name-as-string
    call does, and keeps aggregates out of `SQLExpression` strings, where a
    column name would have to be quoted by hand.
    """

    def __getattr__(self, name: str) -> Callable[..., Expression]:
        return partial(FunctionExpression, name)


fn = _Functions()

#: The options of every parquet file datarecord writes. Format v2 delta-encodes a
#: series' snapshots and byte-stream-splits its values; the dictionary limit keeps
#: a row group's few entity names in a dictionary but not 8760 timestamps.
_PARQUET_OPTIONS = (
    "FORMAT parquet, COMPRESSION zstd, PARQUET_VERSION v2, DICTIONARY_SIZE_LIMIT 4096"
)


def write_parquet(
    rel: DuckDBPyRelation, uri: str, con: DuckDBPyConnection, *, ordered: bool
) -> None:
    """Write `rel` to the parquet file `uri`, keeping its row order where `ordered`.

    Leave `ordered` off where the row order carries no meaning: an
    order-preserving write buffers every batch a parallel scan finishes early,
    which spills tens of GB for a series melted into hundreds of millions of rows.
    """
    order = "" if ordered else ", PRESERVE_ORDER false"
    con.execute(f"COPY (FROM rel) TO {lit(uri)} ({_PARQUET_OPTIONS}{order})")


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
    return {os.path.basename(row[0]) for row in rows}


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
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    """
    u = rels[0]
    for rel in rels[1:]:  # noqa: F841 - bound by name in the SQL below
        u = con.sql("FROM u UNION ALL BY NAME FROM rel")
    return u


def ex_all(exprs: Iterable[Expression]) -> Expression:
    """AND every expression in `exprs` together."""
    return reduce(lambda x, y: x & y, exprs)


def join_on(
    left: DuckDBPyRelation, right: DuckDBPyRelation, on: Sequence[Expression]
) -> DuckDBPyRelation:
    """Inner join `left` and `right` on every equality in `on`, a cross join where it is empty."""
    return left.join(right, ex_all(on)) if on else left.cross(right)


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


def distinct_values(
    rel: DuckDBPyRelation, column: str, *, order: bool = True
) -> tuple[Any, ...]:
    """`rel`'s distinct values of `column`, as a tuple.

    What keys a `LazyRelations` built over a relation: the values are read from a
    column rather than from a directory listing, so one code path serves a local
    directory and a remote prefix alike.
    """
    projected = rel.project(column).distinct()
    if order:
        projected = projected.order(column)
    return tuple(r[0] for r in projected.fetchall())


def is_frame(value: Any) -> bool:
    """Return whether `value` is a relation, or a pandas, polars or arrow frame DuckDB scans.

    Only modules already imported are checked, so none of them becomes a
    dependency: a value from a module never imported cannot be its frame.
    """
    scanned: list[type] = [DuckDBPyRelation]
    if (pandas := sys.modules.get("pandas")) is not None:
        scanned.append(pandas.DataFrame)
    if (polars := sys.modules.get("polars")) is not None:
        scanned += [polars.DataFrame, polars.LazyFrame]
    if (pyarrow := sys.modules.get("pyarrow")) is not None:
        scanned += [pyarrow.Table, pyarrow.RecordBatch]
    return isinstance(value, tuple(scanned))


def as_relation(frame: Any, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """Return a relation, or a pandas, polars or arrow frame, as a relation on `con`.

    A relation passes straight through; a frame is read by DuckDB's own
    scanner, without collecting it first. The one boundary where a caller's
    frame becomes a relation, shared by the write and edit paths.

    Raises
    ------
    ValueError
        For a relation on another connection, or another thread's cursor:
        copying it over would collect it on a connection the caller never
        asked to read. On a column of timestamps with a time zone: a record's
        are naive, and converting them would pick a zone for the caller.
    TypeError
        For anything that is no relation or frame DuckDB scans.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    - [the write API](https://energy-models.github.io/datarecord/design/use/editing/#set)
    """
    # `FROM frame` is a replacement scan, binding the local `frame` by name.
    if isinstance(frame, DuckDBPyRelation):
        try:
            con.sql("FROM frame")
        except duckdb.InvalidInputException as e:
            msg = (
                "this relation was built on another connection or thread; build "
                "it on `record.con` in this thread, or hand over a pandas, "
                "polars or arrow frame to land"
            )
            raise ValueError(msg) from e
        rel = frame
    elif is_frame(frame):
        rel = con.sql("FROM frame")
    else:
        msg = (
            f"a {type(frame).__name__} is no frame; hand over a DuckDB relation, "
            f"or a pandas, polars or arrow frame"
        )
        raise TypeError(msg)
    zoned = [
        c
        for c, t in zip(rel.columns, rel.types, strict=True)
        if t.id == "timestamp with time zone"
    ]
    if zoned:
        msg = (
            f"{zoned} carry timestamps with a time zone, which a record does not "
            f"store; convert them to naive time first (e.g. `tz_convert(None)`)"
        )
        raise ValueError(msg)
    return rel


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
    packed = ", ".join(f"{col(name)} := {value}" for name, value in fields.items())
    return sql(f"struct_pack({packed})")


def struct_field(
    rel: DuckDBPyRelation, column: str, name: str, default: Expression
) -> Expression:
    """Struct `column`'s field `name` in `rel`, `default` where NULL or absent.

    Whether the field exists is read off `rel`'s type as the plan is built, so
    a struct lacking it costs nothing per row - where `struct_extract` would
    fail to bind.
    """
    fields = dict(rel.types[rel.columns.index(column)].children)
    if name not in fields:
        return default
    return coalesce(fn.struct_extract(col(column), lit(name)), default)


def values_relation(
    con: DuckDBPyConnection,
    names: Sequence[str],
    rows: Sequence[Sequence[Expression]],
) -> DuckDBPyRelation:
    """Return the literal table `rows`, its columns named `names`.

    A SQL `VALUES` list under a table alias naming its columns; `con.values`
    has no such alias, so its columns come out `col0`, `col1`, ...

    Raises
    ------
    duckdb.InvalidInputException
        Where `rows` is empty, which a `VALUES` list cannot be.
    """
    listed = ", ".join(f"({', '.join(map(str, row))})" for row in rows)
    named = ", ".join(str(col(n)) for n in names)
    return con.sql(f"FROM (VALUES {listed}) t({named})")


def empty_relation(
    con: DuckDBPyConnection, **columns: DuckDBPyType
) -> DuckDBPyRelation:
    """Return a relation with no rows and the names and types given in `columns`.

    What a staging table is created from: `create` takes the table's shape
    from the relation, so a shape is built as expressions rather than
    assembled as DDL text. One row of typed NULLs, kept for its types and
    dropped for its rows.
    """
    nulls = [lit(None).cast(t) for t in columns.values()]
    return values_relation(con, list(columns), [nulls]).limit(0)
