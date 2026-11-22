# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A layer's `manifest.json`, which makes it a revision, and its `claim.json`.

A layer carries its own manifest: its id, its parent, the full schema it was
written under, the files it wrote and its `meta`. A claim reserves the id
before any data is written, and is kept.

Notes
-----
- [workspaces and stores](https://energy-models.github.io/datarecord/reference/store/)
- [the record format](https://energy-models.github.io/datarecord/reference/format/)
"""

from __future__ import annotations

import json
import reprlib
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self
from uuid import UUID

from pydantic import ConfigDict, Field, InstanceOf, TypeAdapter, model_validator
from pydantic.dataclasses import dataclass

from datarecord.duck import pattern
from datarecord.schema.schema import Schema, schema_digest

if TYPE_CHECKING:
    from datarecord.store.files import Files

_CONFIG = ConfigDict(extra="forbid", ser_json_inf_nan="constants")

type MetaValue = (
    str | int | float | bool | None | list[MetaValue] | dict[str, MetaValue]
)
"""A JSON value of `meta`: a string, number, boolean, null, array or object.

A float may be `inf`, `-inf` or `nan`, written as the bare `Infinity`,
`-Infinity` and `NaN` that Python's `json` writes and reads.
"""

Meta = dict[str, MetaValue]
"""A revision's own `meta`, set at its write and not inherited."""


@dataclass(frozen=True, config=_CONFIG)
class LayerFiles:
    """What a layer wrote, by declared name: `dimensions/<dim>.parquet`, `attributes/<attr>.parquet`.

    A read opens exactly these and ignores any other file in the layer.

    Attributes
    ----------
    owner_map_rows
        The `_shape` of each shape whose owner-map rows the layer wrote to
        `owner_map_rows.parquet`, ascending: bit `i` for the `i`-th dimension of
        its schema. Empty where it wrote no such file.

    Notes
    -----
    - [`owner_map_rows.parquet`](https://energy-models.github.io/datarecord/reference/format/#owner_map_rowsparquet)
    """

    dimensions: tuple[str, ...] = ()
    attributes: tuple[str, ...] = ()
    owner_map_rows: tuple[int, ...] = ()


@dataclass(frozen=True, config=_CONFIG)
class Manifest:
    """A layer's `manifest.json`: written last and create-only, it makes the revision exist.

    Attributes
    ----------
    parent
        The revision this layer folds over, `None` for a root or a standalone
        directory.
    schema
        The full schema the layer was written under, never a delta. Taken as
        built where it is handed in, and parsed by `read` once per distinct
        schema.
    meta
        The revision's own: set at the write, and not inherited by a child,
        unlike `schema.meta`.

    Notes
    -----
    - [workspaces and stores](https://energy-models.github.io/datarecord/reference/store/)
    """

    id: UUID
    parent: UUID | None
    schema: InstanceOf[Schema]
    files: LayerFiles
    meta: Meta = Field(default_factory=dict)

    @model_validator(mode="after")
    def _declared_files(self) -> Self:
        """Refuse an attribute file whose name `schema` does not declare, and an `owner_map_rows` entry out of order or no listed attribute's shape."""
        undeclared = sorted(set(self.files.attributes) - set(self.schema.attributes))
        if undeclared:
            msg = (
                f"layer {self.id} lists attribute files {undeclared} its schema "
                f"does not declare"
            )
            raise ValueError(msg)
        masks = self.files.owner_map_rows
        if list(masks) != sorted(set(masks)):
            msg = f"layer {self.id} lists owner_map_rows {list(masks)} not ascending"
            raise ValueError(msg)
        dims = self.schema.dimensions
        held = {
            pattern(dims, self.schema.dimensions_of(a)) for a in self.files.attributes
        }
        if strays := sorted(set(masks) - held):
            msg = (
                f"layer {self.id} lists owner_map_rows {strays}, the shape of no "
                f"attribute file it lists"
            )
            raise ValueError(msg)
        return self

    def write(self, files: Files, uri: str) -> None:
        """Write this manifest at `uri`, create-only.

        Raises
        ------
        PreconditionFailedError
            If a manifest is already there.
        """
        _create(files, uri, _MANIFEST, self)

    @classmethod
    def read(
        cls, files: Files, uri: str, schemas: dict[str, Schema]
    ) -> Manifest | None:
        """The manifest at `uri`, or `None` where there is none.

        Its schema is taken from `schemas` by `schema_digest`, and parsed and
        added there only where it is missing.

        Raises
        ------
        pydantic.ValidationError
            If the manifest, or a schema not in `schemas`, is malformed.
        """
        found = files.read_text(uri)
        if found is None:
            return None
        raw = json.loads(found)
        text, digest = schema_digest(raw["schema"])
        if digest not in schemas:
            schemas[digest] = Schema.model_validate_json(text)
        return _MANIFEST.validate_python({**raw, "schema": schemas[digest]})


@dataclass(frozen=True, config=_CONFIG)
class ClaimFile:
    """A layer's `claim.json`: written create-only when its id is handed out, and kept.

    Attributes
    ----------
    meta
        The revision's `meta` as claimed, which its commit may extend and
        never change.
    created
        When the id was handed out, which an age-limited cleanup compares.
    """

    id: UUID
    parent: UUID | None
    meta: Meta
    created: datetime

    def write(self, files: Files, uri: str) -> None:
        """Write this claim at `uri`, create-only, so an id is handed out once.

        Raises
        ------
        PreconditionFailedError
            If the id was claimed before.
        """
        _create(files, uri, _CLAIM, self)

    @classmethod
    def read(cls, files: Files, uri: str) -> ClaimFile | None:
        """The claim at `uri`, or `None` where there is none."""
        return _read(files, uri, _CLAIM)


_MANIFEST = TypeAdapter(Manifest)
_CLAIM = TypeAdapter(ClaimFile)


def _create[T](files: Files, uri: str, adapter: TypeAdapter[T], value: T) -> None:
    files.create(uri, adapter.dump_json(value).decode())


def _read[T](files: Files, uri: str, adapter: TypeAdapter[T]) -> T | None:
    found = files.read_text(uri)
    return None if found is None else adapter.validate_json(found)


def validate_meta(meta: Meta | None) -> Meta:
    """`meta` checked as a JSON object with string keys, copied.

    Raises
    ------
    TypeError
        Naming the path of the first value that is no JSON value, or of a
        key that is no string.

    Notes
    -----
    - [the store](https://energy-models.github.io/datarecord/reference/store/#store)
    """
    meta = meta or {}
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


def _meta_value(value: Any, path: str) -> MetaValue:
    """`value` copied where it is a JSON value, raising at `path` on the first that is not."""
    match value:
        case None | str() | int() | float():
            return value
        case list():
            return [_meta_value(v, f"{path}[{i}]") for i, v in enumerate(value)]
        case dict():
            return _meta_object(value, path)
    msg = (
        f"{path} is {reprlib.repr(value)} of type {type(value).__name__}; a meta value "
        f"is a str, int, float, bool, None, list or dict"
    )
    raise TypeError(msg)


def merge_meta(claimed: Meta, given: Meta) -> Meta:
    """`given` added to `claimed`, which a commit may extend and never change.

    A claimed key may be restated with an equal value, compared as JSON, so
    `1`, `1.0` and `True` differ, inside an array or object too.

    Raises
    ------
    ValueError
        Naming each claimed key `given` sets otherwise.

    Notes
    -----
    - [the steps of a commit](https://energy-models.github.io/datarecord/reference/store/#a-revision-exists-only-by-its-manifest)
    """
    changed = sorted(
        k for k, v in given.items() if k in claimed and _json(claimed[k]) != _json(v)
    )
    if changed:
        msg = (
            f"the commit changes the claimed meta {changed}; a commit keeps every "
            f"key of its claim's meta unchanged and may only add keys"
        )
        raise ValueError(msg)
    return {**claimed, **given}


def _json(value: MetaValue) -> str:
    """`value` as canonical JSON, which tells `1`, `1.0` and `True` apart at any depth."""
    return json.dumps(value, sort_keys=True)
