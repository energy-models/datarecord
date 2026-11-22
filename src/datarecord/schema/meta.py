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

import json
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


def validate_meta(meta: Meta | None, *, json_keys: bool = False) -> Meta:
    """`meta` checked as a JSON object with string keys, copied; `None` is `{}`.

    Parameters
    ----------
    json_keys
        Take an int, float, bool or None key as the string `json.dumps`
        writes for it, such as `"2030"` for `2030`, where it would raise.

    Raises
    ------
    TypeError
        Naming the path of the first value that is no JSON value, of a key
        that is no string, or of two keys that become the same string.

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
    return _meta_object(meta, "meta", json_keys)


def _meta_object(
    value: dict[Any, Any], path: str, json_keys: bool
) -> dict[str, MetaValue]:
    """`value` with every key a string by `_meta_key` and every value checked by `_meta_value`."""
    out: dict[str, MetaValue] = {}
    for key, item in value.items():
        name = _meta_key(key, path, json_keys)
        if name in out:
            msg = f"{path} has the key {key!r} and another that is also {name!r}"
            raise TypeError(msg)
        out[name] = _meta_value(item, f"{path}[{key!r}]", json_keys)
    return out


def _meta_key(key: Any, path: str, json_keys: bool) -> str:
    """`key` where it is a string, else with `json_keys` the string `json.dumps` writes for a scalar."""
    if isinstance(key, str):
        return key
    if isinstance(key, _numpy_scalars()):
        key = key.item()
    if json_keys and isinstance(key, int | float | None):
        return json.dumps(key)
    kinds = "str, int, float, bool or None" if json_keys else "str"
    msg = f"{path} has the key {key!r} of type {type(key).__name__}; a meta key is a {kinds}"
    raise TypeError(msg)


def _numpy_scalars() -> tuple[Any, ...]:
    """NumPy's integer, floating and bool scalar types, none while NumPy is not imported.

    Looked up rather than imported, since core does not depend on NumPy, and
    no NumPy value exists before NumPy is imported.
    """
    np = sys.modules.get("numpy")
    return () if np is None else (np.integer, np.floating, np.bool_)


def _meta_value(value: Any, path: str, json_keys: bool) -> MetaValue:
    """`value` copied where it is a JSON value, a NumPy scalar as its Python value, raising at `path` on the first that is not."""
    if isinstance(value, _numpy_scalars()):
        return value.item()
    match value:
        case None | str() | int() | float():
            return value
        case list():
            return [
                _meta_value(v, f"{path}[{i}]", json_keys) for i, v in enumerate(value)
            ]
        case dict():
            return _meta_object(value, path, json_keys)
    msg = (
        f"{path} is {reprlib.repr(value)} of type {type(value).__name__}; a meta value "
        f"is a str, int, float, bool (NumPy's too), None, list or dict"
    )
    raise TypeError(msg)
