# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

import importlib.metadata
import warnings

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:  # pragma: no cover
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"

from datarecord.mutable import WorkingRecord
from datarecord.protocols import (
    Flags,
    LazyRelations,
    RecordLike,
    Relations,
)
from datarecord.resolve import Record
from datarecord.schema import (
    AttributeSpec,
    Dimension,
    Group,
    PropertySpec,
    Schema,
)
from datarecord.store import Revision, Store, Workspace

__all__ = [
    "AttributeSpec",
    "Dimension",
    "Flags",
    "Group",
    "LazyRelations",
    "PropertySpec",
    "Record",
    "RecordLike",
    "Relations",
    "Revision",
    "Schema",
    "Store",
    "WorkingRecord",
    "Workspace",
]
