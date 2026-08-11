import importlib.metadata
import warnings

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:  # pragma: no cover
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"

from datarecord.data_record import DataRecord
from datarecord.duck import connect, layer_dir
from datarecord.mutable import Directory, MutableStore, NewChild, Pending
from datarecord.store import (
    DirectoryStore,
    Flags,
    Frames,
    LayeredStore,
    LazyFrames,
    Solved,
    Store,
)
from datarecord.write import write_layer

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
