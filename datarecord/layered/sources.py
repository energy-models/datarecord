"""Where a layer's data is, behind a protocol the fold reads it through.

The fold names files, not locations: `inputs/p_nom.parquet` is what it wants
and a source says where that is. Today one source answers - a parquet directory
under `layers/<uuid>/` - so this is `layer_dir` with a seam in front of it.

Notes
-----
- [the record format](https://energy-models.github.io/datarecord/design/format/)
- [the DuckDB read path](https://energy-models.github.io/datarecord/design/read-path/)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from datarecord.duck import layer_dir

if TYPE_CHECKING:
    from uuid import UUID


@runtime_checkable
class LayerSource(Protocol):
    """One layer's data, addressed by the path a reader asks for.

    Structural, so an implementation needs no import from here: what makes
    something a source is answering `uri`, not inheriting.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """

    def uri(self, path: str = "") -> str:
        """Where `path` is, `path` being relative to the layer root.

        `"inputs/p_nom.parquet"`, `"dims/entity.parquet"`, or a glob the caller
        builds - the source neither parses nor validates it, the layout being
        the reader's knowledge rather than the source's. Empty is the layer
        root itself, with its trailing slash.
        """
        ...


@dataclass(frozen=True)
class ParquetLayer:
    """A layer as a parquet directory, located from its revision UUID.

    The location is derived, never stored, so changing the layout is a change
    to `layer_dir` and nothing else.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """

    revision_id: UUID
    base_uri: str | None = None

    def uri(self, path: str = "") -> str:
        """`path` under this layer's directory."""
        return layer_dir(self.revision_id, self.base_uri) + path
