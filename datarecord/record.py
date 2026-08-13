"""The `Record` protocol: what a record answers, however it is backed.

Backings: `layered.revision.LayeredRecord` (a resolved overlay) and
`directory.DirectoryRecord` (a plain directory). See design doc §4 for the
protocol itself, §4.4 for why it names no engine, and §9.3 for what differs
between the two backings.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import narwhals as nw

from datarecord.schema import Schema

Frames = Mapping[str, "nw.LazyFrame"]
"""What a `Record` hands over: named frames, each an unmaterialised plan (§4.2).

The `Mapping` ABC, so a plain `dict` satisfies it as fully as `LazyFrames`
does. A DuckDB-backed record reaches it with `nw.from_native(rel)`, which stays
an unexecuted plan.
"""


class LazyFrames(Mapping[str, "nw.LazyFrame"]):
    """A `Frames` whose values are built on `__getitem__`, not up front (§4.2).

    For a backing where *building* a frame is itself I/O: `read_parquet` reads
    the footer to bind the schema, a round trip per file against a remote record.

    Parameters
    ----------
    keys
        Every key this mapping holds, in iteration order. Listing them must be
        cheap: nothing here builds a frame.
    build
        Called with one key to produce its frame, once per `__getitem__`;
        memoise inside `build` if repeated lookups matter.
    """

    def __init__(self, keys: tuple[str, ...], build: Callable[[str], nw.LazyFrame]):
        self._keys = keys
        self._build = build

    def __getitem__(self, key: str) -> nw.LazyFrame:
        if key not in self._keys:
            raise KeyError(key)
        return self._build(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __contains__(self, key: object) -> bool:
        # `Mapping`'s default answers this by calling `__getitem__` and
        # catching `KeyError`, which would build a frame just to test for one.
        return key in self._keys

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self._keys)!r})"


def _unreachable(key: str) -> nw.LazyFrame:
    raise KeyError(key)  # pragma: no cover - `EMPTY` has no keys to look up


EMPTY = LazyFrames((), _unreachable)
"""A `LazyFrames` with no keys, for a record that has none of some kind."""


@dataclass(frozen=True)
class Flags:
    """Which axes an attribute's rows actually use, for one component type (§4.3).

    The two sets are **not** complements: a dim in both means this type's
    components disagree, which is the instruction to use both containers rather
    than an ambiguity to resolve (§4.3). `varies | broadcast` is the test for
    whether an attribute touches a dim at all.

    Parameters
    ----------
    varies
        Dims some row of this attribute sets.
    broadcast
        Dims some row leaves NULL, i.e. "all values of that dim" (§3.3).
    breakpoints
        Whether any row carries a breakpoint. Not a dim (§7).
    """

    varies: frozenset[str]
    broadcast: frozenset[str]
    breakpoints: bool = False


@runtime_checkable
class Record(Protocol):
    """What a record answers, however it is backed (§4).

    Read-only: writing is `write_record(revision_id, source, con)` (§10).
    """

    @property
    def schema(self) -> Schema:
        """The record's schema: its dims, its attributes, its patch granularity (§5)."""
        ...

    @property
    def dims(self) -> Frames:
        """Axis frames, keyed by dim (`"scenario"` -> `dims/scenarios.parquet`)."""
        ...

    @property
    def components(self) -> Frames:
        """Wide member frames, keyed by component type, in member order (§3)."""
        ...

    @property
    def connections(self) -> Frames:
        """Connection rows, keyed by component type, in member order (§6)."""
        ...

    @property
    def attributes(self) -> Frames:
        """Long input frames, keyed by attribute name - one per file (§3.2, §4).

        Not by component type: one `inputs/p_max_pu.parquet` holds every type's
        rows, keyed by `name` alone. A row carries no `component_type` - names
        are unique across every type - so a reader wanting one type joins `components`
        on `name` (§3.5).
        """
        ...

    @property
    def outputs(self) -> Frames:
        """Long result frames, keyed by attribute name (§9.4).

        Empty for a record carrying no results.

        Unlike its neighbours, this does **not** overlay on a layered record: a
        record's results are its own layer's, never a resolution over its
        ancestors' (§9.4).
        """
        ...

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Every attribute of `ctype`, mapped to the shape its rows take (§4.3).

        The key set is the existence answer: an attribute with no rows for this
        type is absent rather than mapped to empty sets.
        """
        ...
