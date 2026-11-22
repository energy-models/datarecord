# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""`meta`, the JSON a revision and a schema carry and never interpret.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/reference/store/#store)
- [the schema](https://energy-models.github.io/datarecord/reference/schema/#meta)
"""

from __future__ import annotations

import reprlib
import sys
from typing import Any

type MetaValue = (
    str | int | float | bool | None | list[MetaValue] | dict[str, MetaValue]
)
"""A JSON value of `meta`: a string, number, boolean, null, array or object.

A float may be `inf`, `-inf` or `nan`, written as the bare `Infinity`,
`-Infinity` and `NaN` that Python's `json` writes and reads.
"""

Meta = dict[str, MetaValue]
"""A revision's or a schema's `meta`: a JSON object with string keys."""


def validate_meta(meta: Meta | None) -> Meta:
    """`meta` checked as a JSON object with string keys, copied; `None` is `{}`.

    Raises
    ------
    TypeError
        Naming the path of the first value that is no JSON value, or of a
        key that is no string.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
    - [the schema](https://energy-models.github.io/datarecord/reference/schema/#meta)
    """
    if meta is None:
        return {}
    if not isinstance(meta, dict):
        msg = f"meta is of type {type(meta).__name__}; meta is a dict"
        raise TypeError(msg)
    return _meta_object(meta, "meta")


def _meta_object(value: dict[Any, Any], path: str) -> dict[str, MetaValue]:
    """`value` with every key a string and every value checked by `_meta_value`."""
    for key in value:
        if not isinstance(key, str):
            msg = (
                f"{path} has the key {key!r} of type {type(key).__name__}; a meta "
                f"key is a str"
            )
            raise TypeError(msg)
    return {k: _meta_value(v, f"{path}[{k!r}]") for k, v in value.items()}


def _numpy_scalars() -> tuple[Any, ...]:
    """NumPy's integer, floating and bool scalar types, none while NumPy is not imported.

    Looked up rather than imported, since core does not depend on NumPy, and
    no NumPy value exists before NumPy is imported.
    """
    np = sys.modules.get("numpy")
    return () if np is None else (np.integer, np.floating, np.bool_)


def _meta_value(value: Any, path: str) -> MetaValue:
    """`value` copied where it is a JSON value, a NumPy scalar as its Python value, raising at `path` on the first that is not."""
    if isinstance(value, _numpy_scalars()):
        return value.item()
    match value:
        case None | str() | int() | float():
            return value
        case list():
            return [_meta_value(v, f"{path}[{i}]") for i, v in enumerate(value)]
        case dict():
            return _meta_object(value, path)
    msg = (
        f"{path} is {reprlib.repr(value)} of type {type(value).__name__}; a meta value "
        f"is a str, int, float, bool (NumPy's too), None, list or dict"
    )
    raise TypeError(msg)
