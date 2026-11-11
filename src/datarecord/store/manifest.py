# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A layer's `manifest.json`, which makes it a revision, and its `claim.json`.

A layer carries its own manifest: its id, its parent, the full schema it was
written under, the files it wrote and a flat `meta`. A claim reserves the id
before any data is written, and is kept.

Notes
-----
- [workspaces and stores](https://energy-models.github.io/datarecord/reference/store/)
- [the record format](https://energy-models.github.io/datarecord/reference/format/)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from pydantic import ConfigDict, Field, InstanceOf, TypeAdapter
from pydantic.dataclasses import dataclass

from datarecord.schema.schema import Schema, schema_digest

if TYPE_CHECKING:
    from datarecord.store.files import Files

Meta = dict[str, str | int | Annotated[float, Field(allow_inf_nan=False)] | bool]
"""A revision's own `meta`: flat, set at its write, and not inherited.

A float is finite, since JSON has no NaN or infinity to store it as.
"""


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
class LayerFiles:
    """What a layer wrote, by declared name: `dimensions/<dim>.parquet`, `attributes/<attr>.parquet`.

    A read opens exactly these and ignores any other file in the layer.
    """

    dimensions: tuple[str, ...] = ()
    attributes: tuple[str, ...] = ()


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
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


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
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
_META = TypeAdapter(Meta)


def _create[T](files: Files, uri: str, adapter: TypeAdapter[T], value: T) -> None:
    files.create(uri, adapter.dump_json(value).decode())


def _read[T](files: Files, uri: str, adapter: TypeAdapter[T]) -> T | None:
    found = files.read_text(uri)
    return None if found is None else adapter.validate_json(found)


def validate_meta(meta: Meta | None) -> Meta:
    """`meta` checked flat, each value a string, finite number or boolean.

    Raises
    ------
    pydantic.ValidationError
        For any other value.
    """
    return _META.validate_python(meta or {}, strict=True)


def merge_meta(claimed: Meta, given: Meta) -> Meta:
    """`given` added to `claimed`, which a commit may extend and never change.

    Raises
    ------
    ValueError
        Naming each claimed key `given` sets otherwise.
    """
    changed = sorted(k for k, v in given.items() if k in claimed and claimed[k] != v)
    if changed:
        msg = (
            f"the commit changes the claimed meta {changed}; a commit keeps every "
            f"key of its claim's meta unchanged and may only add keys"
        )
        raise ValueError(msg)
    return {**claimed, **given}
