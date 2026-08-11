import importlib.metadata
import warnings

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:  # pragma: no cover
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"

from datarecord.directory import DirectoryStore
from datarecord.duck import connect, layer_dir
from datarecord.layered.record import DataRecord, LayeredStore
from datarecord.layered.write import write_layer
from datarecord.mutable import Directory, MutableStore, NewChild, Pending
from datarecord.store import (
    Flags,
    Frames,
    LazyFrames,
    Solved,
    Store,
)

__all__ = [
    "DataRecord",
    "Directory",
    "DirectoryStore",
    "Flags",
    "Frames",
    "LayeredStore",
    "LazyFrames",
    "MutableStore",
    "NewChild",
    "Pending",
    "Solved",
    "Store",
    "connect",
    "layer_dir",
    "write_layer",
]
