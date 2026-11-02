# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A layer's `manifest.json`, which makes it a revision, and its `claim.json`.

A layer carries its own manifest: its id, its parent, the full schema it was
written under, the files it wrote and a flat `meta`. A claim reserves the id
before any data is written, and is kept.

Notes
-----
- [workspaces and stores](https://energy-models.github.io/datarecord/design/storage/store/)
- [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass

from datarecord.schema.schema import Schema
from datarecord.store.files import Absent

if TYPE_CHECKING:
    from datarecord.store.files import Files

Meta = dict[str, str | int | float | bool]
"""A revision's own `meta`: flat, set at its write, and not inherited."""


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
class LayerFiles:
    """What a layer wrote, by declared name: `dims/<dim>.parquet`, `attributes/<attr>.parquet`.

    A read opens exactly these and ignores any other file in the layer.
    """

    dims: tuple[str, ...] = ()
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
        The full schema the layer was written under, never a delta.
    meta
        The revision's own: set at the write, and not inherited by a child,
        unlike `schema.meta`.

    Notes
    -----
    - [workspaces and stores](https://energy-models.github.io/datarecord/design/storage/store/)
    """

    id: UUID
    parent: UUID | None
    schema: Schema
    files: LayerFiles
    meta: Meta = Field(default_factory=dict)

    def json(self) -> str:
        return _MANIFEST.dump_json(self).decode()

    def write(self, files: Files, uri: str) -> None:
        """Write this manifest at `uri`, create-only.

        Raises
        ------
        PreconditionFailedError
            If a manifest is already there.
        """
        files.write_text(uri, self.json(), expect=Absent())

    @classmethod
    def read(cls, files: Files, uri: str) -> Manifest | None:
        """The manifest at `uri`, or `None` where there is none."""
        found = files.read_text(uri)
        return None if found is None else _MANIFEST.validate_json(found[0])


_MANIFEST = TypeAdapter(Manifest)
_META = TypeAdapter(Meta)


def schema_hash(text: str) -> str:
    """What keys a schema's JSON in the workspace's `schemas` table."""
    return hashlib.sha256(text.encode()).hexdigest()


def validate_meta(meta: Meta | None) -> Meta:
    """`meta` checked flat, each value a string, number or boolean.

    Raises
    ------
    pydantic.ValidationError
        For any other value.
    """
    return _META.validate_python(meta or {}, strict=True)


def write_claim(
    files: Files, uri: str, id: UUID, parent: UUID | None, meta: Meta
) -> None:
    """Write a layer's `claim.json`, create-only, so an id is handed out once.

    Raises
    ------
    PreconditionFailedError
        If `id` was claimed before.
    """
    body = {
        "id": str(id),
        "parent": None if parent is None else str(parent),
        "meta": meta,
        "created": datetime.now(UTC).isoformat(),
    }
    files.write_text(uri, json.dumps(body), expect=Absent())


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
