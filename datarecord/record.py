"""The `Record` protocol: what a record answers, however it is backed.

Backings: `layered.revision.LayeredRecord` (a resolved overlay) and
`directory.DirectoryRecord` (a plain directory).

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
- [the protocol names no engine](https://energy-models.github.io/datarecord/design/record/#the-protocol-names-no-engine)
- [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import narwhals as nw

from datarecord.schema import Schema

Frames = Mapping[str, "nw.LazyFrame"]
"""What a `Record` hands over: named frames, each an unmaterialised plan.

The `Mapping` ABC, so a plain `dict` satisfies it as fully as `LazyFrames`
does. A DuckDB-backed record reaches it with `nw.from_native(rel)`, which stays
an unexecuted plan.

Notes
-----
- [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
"""


class LazyFrames(Mapping[str, "nw.LazyFrame"]):
    """A `Frames` whose values are built on `__getitem__`, not up front.

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

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
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
    """Which axes an attribute's rows actually use, for one component type.

    The two sets are **not** complements: a dim in both means this type's
    components disagree, which is the instruction to use both containers rather
    than an ambiguity to resolve. `varies | broadcast` is the test for
    whether an attribute touches a dim at all.

    Parameters
    ----------
    varies
        Dims some row of this attribute sets.
    broadcast
        Dims some row leaves NULL, i.e. "all values of that dim".
    breakpoints
        Whether any row carries a breakpoint. Not a dim.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """

    varies: frozenset[str]
    broadcast: frozenset[str]
    breakpoints: bool = False


@runtime_checkable
class Record(Protocol):
    """What a record answers, however it is backed.

    Read-only: writing is `write_record(revision_id, source, con)`.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """

    @property
    def schema(self) -> Schema:
        """The record's schema: its dims, its attributes, its patch granularity.

        Notes
        -----
        - [the schema](https://energy-models.github.io/datarecord/design/schema/)
        """
        ...

    @property
    def dims(self) -> Frames:
        """Axis frames, keyed by dim (`"scenario"` -> `dims/scenarios.parquet`)."""
        ...

    @property
    def components(self) -> Frames:
        """Wide member frames, keyed by component type, in member order.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
        - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
        """
        ...

    @property
    def groups(self) -> Mapping[str, Frames]:
        """Each declared group's rows, keyed by group then by component type.

        A group declares which tuples over several dims exist - `connection`
        over `(entity, bus)` is the one every record with connections has, and
        it is one instance rather than a member of its own.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        ...

    @property
    def attributes(self) -> Frames:
        """Long input frames, keyed by attribute name - one per file.

        Not by component type: one `inputs/p_max_pu.parquet` holds every type's
        rows, keyed by `entity` alone. A row carries no `component_type` - entities
        are unique across every type - so a reader wanting one type joins `components`
        on `name`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        ...

    @property
    def outputs(self) -> Frames:
        """Long result frames, keyed by attribute name.

        Empty for a record carrying no results.

        Unlike its neighbours, this does **not** overlay on a layered record: a
        record's results are its own layer's, never a resolution over its
        ancestors'.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        ...

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Every attribute of `ctype`, mapped to the shape its rows take.

        Only attributes with rows are present, so the key set also answers
        which attributes this type has at all.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        """
        ...
