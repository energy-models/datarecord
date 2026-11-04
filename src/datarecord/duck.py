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

import operator
import sys
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from functools import partial, reduce
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import (
    DuckDBPyConnection,
    DuckDBPyRelation,
    Expression,
    TransactionException,
    sqltypes,
)
from duckdb import SQLExpression as sql
from duckdb.sqltypes import DuckDBPyType


class _Functions:
    """`fn.round(x, precision=1)` for `round(x, precision := 1)`.

    Positional arguments pass in order and keyword arguments as DuckDB named
    arguments. Attribute access reads closer to the SQL it builds than a
    name-as-string call does, and keeps calls out of `SQLExpression` strings,
    where a column name would have to be quoted by hand.
    """

    def __getattr__(self, name: str) -> Callable[..., Expression]:
        return partial(duckdb.fn, name)


fn = _Functions()

#: The schema each revision's fold is kept in, as a schema of its own inside.
FOLD = "_fold"
#: The schema each `WorkingRecord`'s staging tables are kept in, as a schema of its own inside.
STAGED = "_staged"
#: The schema a call keeps its intermediate tables in, each dropped by that call.
SCRATCH = "_scratch"


def nested(parent: str, id: UUID) -> str:
    """The schema `id` names inside schema `parent`, quoted for SQL."""
    return str(col(parent, id.hex))


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


@contextmanager
def transaction(
    con: DuckDBPyConnection, operation: str, url: str
) -> Iterator[Callable[[Callable[[], object]], None]]:
    """Run the block in one transaction on `con`, committed where it returns, rolled back where it raises.

    The block may register undo steps for what it changed outside the
    database, which the rollback cannot reach: each runs, last first, where
    the block or the commit fails.

    Parameters
    ----------
    operation
        What runs, named in the refusal.
    url
        The design page the refusal links to, as a URL.

    Yields
    ------
    Callable
        Registers one undo step.

    Raises
    ------
    TransactionException
        Where `con` already holds an open transaction, which DuckDB cannot nest
        and the block could not roll back alone.
    """
    try:
        con.begin()
    except TransactionException as e:
        msg = (
            f"`{operation}` runs in its own transaction, and this cursor already "
            f"holds one open; commit or roll it back before calling it ({url})"
        )
        raise TransactionException(msg) from e
    undo: list[Callable[[], object]] = []
    try:
        try:
            yield undo.append
        except BaseException:
            con.rollback()
            raise
        con.commit()
    except BaseException:
        for step in reversed(undo):
            step()
        raise


def refuse_open_transaction(con: DuckDBPyConnection, operation: str, url: str) -> None:
    """Refuse `operation` where `con` holds an open transaction, as `transaction` does.

    Raises
    ------
    TransactionException
        Where `con` already holds an open transaction.
    """
    with transaction(con, operation, url):
        pass


def union_all_by_name(rels: Sequence[DuckDBPyRelation]) -> DuckDBPyRelation:
    """Fold relations pairwise through `UNION ALL BY NAME`, an arm lacking a column reading NULL there.

    Notes
    -----
    - [materialised node caches](https://energy-models.github.io/datarecord/design/layers/resolution/#materialised-node-caches)
    - [resolving a relation](https://energy-models.github.io/datarecord/design/layers/resolution/#resolving-a-relation)
    """
    return reduce(DuckDBPyRelation.union_by_name, rels)


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


def named_mask(dims: Sequence[str], alias: str | None = None) -> Expression:
    """A row's named set over `dims` as a bitmask, bit `i` for `dims[i]`, its columns under `alias` where given."""
    return reduce(
        operator.add,
        (
            (col(d) if alias is None else col(alias, d))
            .isnotnull()
            .cast(sqltypes.INTEGER)
            * lit(1 << i)
            for i, d in enumerate(dims)
        ),
        lit(0),
    )


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

    Never empty, DuckDB having no empty struct.
    """
    return fn.struct_pack(**fields)


def values_relation(
    con: DuckDBPyConnection,
    names: Sequence[str],
    rows: Sequence[Sequence[Expression]],
) -> DuckDBPyRelation:
    """Return the literal table `rows`, its columns named `names`.

    A SQL `VALUES` list under a table alias naming its columns, so the rows
    live in the query text and a registered view can read them. `con.values`
    holds its rows in the process, which `create_view` refuses, and names its
    columns `col0`, `col1`, ...

    Raises
    ------
    duckdb.InvalidInputException
        Where `rows` is empty, which a `VALUES` list cannot be.
    """
    listed = ", ".join(f"({', '.join(map(str, row))})" for row in rows)
    named = ", ".join(str(col(n)) for n in names)
    return con.sql(f"FROM (VALUES {listed}) t({named})")


def numbered_relation(
    con: DuckDBPyConnection,
    name: str,
    values: Sequence[Any],
    dtype: DuckDBPyType,
    number: str,
) -> DuckDBPyRelation:
    """Return `values` as the column `name` of `dtype`, numbered from 0 in their order in `number`.

    Read once, at the call: the values are bound as one parameter rather than
    rendered as literals, which a relation re-renders and DuckDB re-parses
    wherever it is composed, at a cost growing with every value.
    """
    return con.sql(
        f"SELECT unnest($1::{dtype}[]) AS {col(name)}, unnest(range($2)) AS {col(number)}",
        params=[list(values), len(values)],
    )


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


def tables_in(con: DuckDBPyConnection, parent: str, id: UUID) -> set[str]:
    """The tables of schema `id` inside schema `parent`, none where it does not exist yet.

    Asked of the catalog rather than found by `con.table` failing: on a
    missing table DuckDB scans the calling frame's Python variable of the
    table's name instead.
    """
    rows = con.execute(
        "SELECT t.table_name FROM duckdb_tables() t"
        " JOIN duckdb_schemas() s ON t.schema_oid = s.oid"
        " WHERE s.parent_schema = ? AND s.schema_name = ?",
        [parent, id.hex],
    ).fetchall()
    return {table for (table,) in rows}


def shared_table(
    con: DuckDBPyConnection,
    parent: str,
    id: UUID,
    table: str,
    build: Callable[[], AbstractContextManager[DuckDBPyRelation]],
) -> DuckDBPyRelation:
    """Table `table` of schema `id` inside `parent`, it and its schema built by whichever cursor misses them first.

    `build` enters the relation the table is made of, and exits once the
    table holds it, releasing whatever the relation read. Two cursors missing
    at once both create, and DuckDB fails the second with a write-write
    conflict rather than a name clash; what it would have created is the
    winner's, so that is read instead. The conflict can come before the
    winner commits, when its schema or table is not yet visible, so each
    `create` is retried until it either lands or meets the committed one.

    Notes
    -----
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/storage/concurrency/#a-cursor-per-thread)
    """
    schema = nested(parent, id)
    name = f"{schema}.{table}"
    if table in tables_in(con, parent, id):
        return con.table(name)
    _past_conflicts(lambda: con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    with build() as rel, suppress(duckdb.CatalogException):
        _past_conflicts(lambda: rel.create(name))
    return con.table(name)


def _past_conflicts(create: Callable[[], object]) -> None:
    """Run `create` again for as long as DuckDB fails it with a write-write conflict."""
    while True:
        try:
            create()
        except duckdb.TransactionException as e:
            if "conflict" not in str(e).lower():
                raise
            time.sleep(0.001)
            continue
        return


@contextmanager
def scratch_table(
    con: DuckDBPyConnection, rel: DuckDBPyRelation
) -> Iterator[DuckDBPyRelation]:
    """`rel` collected into a table of its own, read as one, and dropped on exit.

    A table both pins what `rel` reads, where a later write would change it,
    and stops each step over it from binding `rel`'s whole tree again.
    """
    name = f"{SCRATCH}.t_{uuid4().hex}"
    rel.create(name)
    try:
        yield con.table(name)
    finally:
        con.execute(f"DROP TABLE {name}")
