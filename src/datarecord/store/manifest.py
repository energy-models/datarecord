# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The record's one schema, read from and written to `manifest.json` beside its layers.

Notes
-----
- [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
- [the manifest's compare-and-swap](https://energy-models.github.io/datarecord/design/storage/concurrency/#the-manifests-compare-and-swap)
"""

from __future__ import annotations

import random
from time import sleep
from typing import TYPE_CHECKING

from datarecord.schema.schema import Schema
from datarecord.store.files import Absent, PreconditionFailedError, Version

if TYPE_CHECKING:
    from datarecord.store.store import Store


def read_schema(store: Store) -> Schema:
    """The record's one schema, read from beside `store`'s layers.

    No fold and no ancestry: a schema is not layered data. One file makes it a
    property of the record, knowable before any layer is read and stated once
    for a hundred-layer tree. A record that declares none reads as an empty
    `Schema`, which declares no dims and no attributes.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
    """
    found = store.files.read_text(store.manifest_uri)
    return Schema() if found is None else Schema.model_validate_json(found[0])


def reconcile_schema(schema: Schema, store: Store) -> None:
    """Declare the record's schema, or check this layer agrees with it.

    A schema is not layered data, so there is nothing to fold: the first writer
    states it and the rest must be `compatible_with` it. Read and written beside
    `store`'s own layers, so one record never consults another's manifest.

    A compare-and-swap: the manifest is written only if unchanged since it was
    read, and a writer that lost re-reads and checks again, so two writers
    widening at once end with both widenings or an incompatibility reported,
    never one silently lost. A lost write backs off with jitter and is retried
    up to `_ATTEMPTS` times.

    Raises
    ------
    ValueError
        If this layer's schema would make the record's existing layers
        unreadable.
    RuntimeError
        If every attempt lost: the manifest kept changing, or it cannot be read
        and a 403 was taken for a missing key.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    - [the manifest's compare-and-swap](https://energy-models.github.io/datarecord/design/storage/concurrency/#the-manifests-compare-and-swap)
    """
    schema = schema.model_copy(update={"id": None})
    files = store.files
    for attempt in range(_ATTEMPTS):
        found = files.read_text(store.manifest_uri)
        expect: Version | Absent = Absent() if found is None else found[1]
        existing = Schema() if found is None else Schema.model_validate_json(found[0])
        if existing.dimensions or existing.attributes:
            if schema == existing:
                return
            problems = schema.compatible_with(existing)
            if problems:
                msg = (
                    f"this layer's schema is incompatible with the record's: "
                    f"{'; '.join(problems)} (https://energy-models.github.io/datarecord/design/model/schema/#versioning)"
                )
                raise ValueError(msg)
        try:
            files.write_text(
                store.manifest_uri, schema.model_dump_json(), expect=expect
            )
        except PreconditionFailedError:
            sleep(random.uniform(0, 0.05 * 2**attempt))
            continue
        return
    msg = (
        f"{store.manifest_uri} changed under each of {_ATTEMPTS} writes, or cannot "
        f"be read: a 403 on read counts as missing, so check s3:GetObject "
        f"(https://energy-models.github.io/datarecord/design/storage/concurrency/#the-manifests-compare-and-swap)"
    )
    raise RuntimeError(msg)


_ATTEMPTS = 10
"""How many times `reconcile_schema` writes the manifest before giving up."""
