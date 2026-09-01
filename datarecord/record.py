"""The `RecordLike` protocol: what a record answers, however it is backed.

`layered.revision.Record` is the class this package provides; a framework
object presenting itself as a record satisfies the protocol structurally,
which is what `tools/` is built on.

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
- [the protocol names no engine](https://energy-models.github.io/datarecord/design/record/#the-protocol-names-no-engine)
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

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


def flags_from_rows(
    schema: Schema,
    dims: tuple[str, ...],
    rows: Iterable[tuple[str, Mapping[str, Any], Mapping[str, Any], Any]],
) -> dict[str, Flags]:
    """Fold `(attribute, varies, broadcast, breakpoints)` rows into `Flags`.

    Every backing aggregates the flags differently - the fold reads them off the
    owner map, a directory scans `inputs/`, a `WorkingRecord` unions its staging
    tables - but all three answer in this shape and must scope it identically,
    so the scoping lives here once.

    Both sets are cut to the attribute's own coordinates. The relation aggregated
    over covers every attribute, so a dim one attribute is addressed by reads
    NULL for the rows of one that is not, and reporting that NULL as "every row
    broadcasts over it" would tell a consumer to build a container along an axis
    the attribute has no values on. An attribute the schema does not declare
    keeps every dim, its shape not being the schema's to say.

    Parameters
    ----------
    dims
        The broadcast dims the two mappings have an entry per; a dim missing
        from one - declared after a persisted map was written - reads as unset.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """

    def scope(attribute: str) -> tuple[str, ...]:
        own = set(schema.coordinates_of(attribute))
        return tuple(d for d in dims if d in own) if own else dims

    return {
        attribute: Flags(
            frozenset(d for d in scope(attribute) if varies.get(d)),
            frozenset(d for d in scope(attribute) if broadcast.get(d)),
            bool(breakpoints),
        )
        for attribute, varies, broadcast, breakpoints in rows
    }


def collision_detail(rows: Iterable[tuple[Any, Any]]) -> str:
    """`(entity, entity_type)` pairs as the detail of a name-collision message.

    Sorted here rather than in the query the pairs came from: the message must
    be deterministic, and a collision is a handful of rows.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    by_name: dict[str, list[str]] = {}
    for name, ctype in rows:
        by_name.setdefault(str(name), []).append(str(ctype))
    return "; ".join(
        f"{name!r} is a {' and a '.join(sorted(types))}"
        for name, types in sorted(by_name.items())
    )


@runtime_checkable
class RecordLike(Protocol):
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
        """Axis frames, keyed by dim (`"scenario"` -> `dims/scenarios.parquet`).

        An axis frame is its key column and the attributes addressed by it alone
        (`Schema.attributes_on`) - so a per-country CO2 budget or a per-type icon
        is read from here rather than from `attributes`, which holds long frames
        only. A column absent from the frame is one no layer wrote, whose value
        is that attribute's `default`.

        No classification column: which buses a country holds is the group
        `into` it, read from `groups`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
        """
        ...

    @property
    def entity_types(self) -> Frames:
        """Wide member frames, keyed by component type, in member order.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
        - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
        """
        ...

    @property
    def groups(self) -> Frames:
        """Each declared group's rows, keyed by group - one frame each.

        A group declares which tuples over several dims exist - `connection`
        over `(entity, bus)` is the one every record with connections has, and
        it is one instance rather than a member of its own.

        Not split by component type, which is no coordinate of a group.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [where the rows live](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        ...

    @property
    def attributes(self) -> Frames:
        """Long input frames, keyed by attribute name - one per file.

        Not by component type: one `inputs/p_max_pu.parquet` holds every type's
        rows, keyed by `entity` alone. A row carries no `entity_type` - entities
        are unique across every type - so a reader wanting one type joins `entity_types`
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
