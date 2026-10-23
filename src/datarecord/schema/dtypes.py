# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The DuckDB type of each mathspec dtype, and whether a given type fits a declared one.

Notes
-----
- [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
"""

from collections.abc import Iterable, Mapping
from typing import cast

from duckdb import sqltypes
from duckdb.sqltypes import DuckDBPyType

DUCK_TYPES: Mapping[str, DuckDBPyType] = {
    "float": sqltypes.DOUBLE,
    "int": sqltypes.BIGINT,
    "bool": sqltypes.BOOLEAN,
    "str": sqltypes.VARCHAR,
    "datetime": sqltypes.TIMESTAMP,
}
"""The DuckDB type of each mathspec dtype; every declared column is typed through this map.

Notes
-----
- [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
"""


def enum_type(labels: Iterable[str]) -> DuckDBPyType:
    """Return the `ENUM` type of `labels`, in their order: the type of a dim that lists its labels."""
    quoted = ", ".join("'" + label.replace("'", "''") + "'" for label in labels)
    return DuckDBPyType(f"ENUM({quoted})")


def enum_labels(dtype: DuckDBPyType) -> tuple[str, ...] | None:
    """Return the labels of `dtype` if it is an `ENUM`, else None."""
    if dtype.id != "enum":
        return None
    return tuple(cast("list[str]", dict(dtype.children)["values"]))


_INTEGERS = {"tinyint", "smallint", "integer", "utinyint", "usmallint", "uinteger"}

_WIDENS = {
    "bigint": _INTEGERS,
    "double": {*_INTEGERS, "bigint", "float", "decimal"},
    "timestamp": {"timestamp_s", "timestamp_ms", "timestamp_ns"},
    "varchar": {"enum"},
    "enum": {"varchar"},
}
"""For each declared type id, the type ids of a caller's column that are cast to it."""


def takes(declared: DuckDBPyType, given: DuckDBPyType) -> bool:
    """Return whether type `given` fits type `declared`, that is, a column of it is cast to it.

    It is where the types are equal, `given` is NULL, `_WIDENS` lists the
    widening, or both are structs and each field of `declared` fits the
    field of `given` of the same name. An `ENUM` and text are cast both ways,
    and the cast checks the labels. A widening keeps the kind of value, not
    every digit: an integer past 2^53 rounds as `DOUBLE`, and a nanosecond
    timestamp truncates to microseconds.
    """
    if given.id == "null" or given == declared:
        return True
    if declared.id == "struct":
        fields = dict(given.children) if given.id == "struct" else {}
        return all(
            name in fields
            and takes(cast("DuckDBPyType", t), cast("DuckDBPyType", fields[name]))
            for name, t in declared.children
        )
    return given.id == declared.id or given.id in _WIDENS.get(declared.id, ())
