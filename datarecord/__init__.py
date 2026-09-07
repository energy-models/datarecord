import importlib.metadata
import warnings

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:  # pragma: no cover
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"

from datarecord.directory import DirectoryRecord
from datarecord.duck import connect, layer_dir
from datarecord.layered.revision import LayeredRecord, Revision
from datarecord.layered.write import write_record
from datarecord.mutable import Directory, NewChild, Pending, WorkingRecord
from datarecord.record import (
    Flags,
    Frames,
    LazyFrames,
    Record,
)
from datarecord.schema import AttributeSpec, ComponentType, Dimension, Group, Schema

__all__ = [
    "AttributeSpec",
    "ComponentType",
    "Dimension",
    "Directory",
    "DirectoryRecord",
    "Flags",
    "Frames",
    "Group",
    "LayeredRecord",
    "LazyFrames",
    "NewChild",
    "Pending",
    "Record",
    "Revision",
    "Schema",
    "WorkingRecord",
    "connect",
    "layer_dir",
    "write_record",
]
