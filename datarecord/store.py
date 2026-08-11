"""The `Store` protocol (design doc §4, §9.3).

One interface over everything a parquet store holds, with backings a consumer
cannot tell apart: `datarecord.layered.record.LayeredStore` over a record's
resolved overlay, and `datarecord.directory.DirectoryStore` over a plain
parquet directory. That is what lets a modelling framework read a
hundred-layer overlay without knowing layering exists.

The protocol lives on its own rather than with either backing: `write_layer`
consumes a `Store`, `datarecord.tools.pypsa` both implements and consumes one,
and `MutableStore` extends it. None of those is where it belongs.

Framework-independent, like the rest of `datarecord`: these hand over
narwhals frames and name no modelling framework.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import narwhals as nw

from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyRelation


Frames = Mapping[str, "nw.LazyFrame"]
"""What a `Store` hands over: named frames, each an unmaterialised plan (§4).

Deliberately the `Mapping` ABC rather than `LazyFrames` below. A consumer needs
only `Mapping` - list the keys, test one, look one up - and every property the
interface promises comes from the *values* being lazy, not from the mapping
being. So a `dict[str, nw.LazyFrame]` satisfies this as fully as `LazyFrames`
does, and an implementation is free to build its frames up front or on lookup
without the protocol changing.

`LazyFrames` is then one implementation of it, for a backing where *building* a
frame is itself I/O - `read_parquet` reads the footer to bind the schema, which
is a round trip per file against a remote store.
"""


class LazyFrames(Mapping[str, "nw.LazyFrame"]):
    """A `Mapping` whose values are built on `__getitem__`, not up front.

    Laziness holds in two senses: a frame is built only when its key is looked
    up, and what is built is itself an unmaterialised plan. So a store is
    explorable without being consumed - `list(frames)` names every key,
    `"x" in frames` answers, `frames["x"]` builds exactly one - which an
    iterator of pairs is not (§4).

    Parameters
    ----------
    keys
        Every key this mapping holds, in the order they should be iterated.
        Listing them must be cheap: nothing here builds a frame.
    build
        Called with one key to produce its frame. Invoked once per
        `__getitem__`; memoise inside `build` if repeated lookups matter
        (§4 leaves caching to the implementation).
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
"""A `LazyFrames` with no keys, for a store that has none of some kind."""


@dataclass(frozen=True)
class Flags:
    """Which axes an attribute's rows actually use, for one component type (§4.3).

    Named for the dims themselves rather than for what a framework calls their
    absence: whether a value is "static" is a statement about one axis being
    NULL, and which axis that is depends on the schema. So `"timestep" in
    varies` says what a `has_series` flag could only imply, and stays
    meaningful for a `region`- or `vintage`-varying attribute that no time axis
    describes.

    The two sets are not complements. An attribute may hold per-timestep rows
    for one component and a single NULL-timestep row for another, so both sets
    contain `timestep` - which is not an ambiguity but the instruction to use
    both containers, each taking the rows it matches. A consumer splitting
    constant from varying data reads the sets in two passes: `timestep in
    broadcast` selects the NULL-timestep rows, `timestep in varies` the rest.

    `varies | broadcast` is then the test for whether an attribute touches a dim
    at all; both empty for a dim means no rows mention it, so neither container
    applies.

    Per component type, because one `inputs/<attr>.parquet` holds every type's
    rows: unioning across them would report a Generator's per-timestep rows and
    a Link's single row as one shape, which describes neither.

    Parameters
    ----------
    varies
        Dims some row of this attribute sets.
    broadcast
        Dims some row leaves NULL, i.e. "all values of that dim" (§3.3).
    breakpoints
        Whether any row carries a breakpoint. Not a dim (§2): a breakpoint is
        an abscissa within one row's value, not an axis indexing it.
    """

    varies: frozenset[str]
    broadcast: frozenset[str]
    breakpoints: bool = False


@runtime_checkable
class Store(Protocol):
    """One parquet store's contents, however it is backed (§9.3).

    The same things `NodeCache` resolves and `write_layer` persists, so a
    resolved overlay (`LayeredStore`) and a plain parquet directory
    (`DirectoryStore`) both satisfy it and a consumer cannot tell which it
    holds. Read-only: writing is `write_layer(record_id, store, con)`, a
    function over a store rather than a method on one.

    Called `Store` rather than `Layer` because a layer is one node's own
    contribution (§8) and a resolved overlay is not one - but both are stores
    in the sense §3 uses.
    """

    @property
    def schema(self) -> Schema:
        """The store's schema: its dims, its attributes, its patch granularity (§5)."""
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
        """Long input frames, keyed by attribute name (§3).

        Keyed by name alone because that is the file: one
        `inputs/p_max_pu.parquet` holds every component type's rows. A reader
        wanting one type's filters on `component_type`.
        """
        ...

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Every attribute of `ctype`, mapped to the shape its rows take (§4.3).

        Unioned over the type's components, so a dim may land in both sets
        where they disagree - which is the instruction to use both containers,
        not an ambiguity (§4.3).

        The key set is the existence answer: an attribute with no rows for this
        type is absent rather than mapped to empty sets.
        """
        ...


@runtime_checkable
class Solved(Store, Protocol):
    """A store that also carries results (§9.4).

    Separate from `Store` because `outputs` does not behave like its
    neighbours. Results do not overlay, so a layered store's `outputs` reads
    one layer where every other member reads the resolution - semantics that
    would differ silently from the rest of the interface. And most stores have
    none: an unsolved record's results are absent rather than empty, so this is
    the thing to test for rather than a member to find empty.

    A store may satisfy both, and `write_layer` writes `outputs/` for one that
    does (§13).
    """

    @property
    def outputs(self) -> Frames:
        """Long result frames, keyed by attribute name (§9.4)."""
        ...


def _lazy(rel: DuckDBPyRelation) -> nw.LazyFrame:
    """A DuckDB relation as a narwhals frame, still unmaterialised.

    `nw.from_native` over a `DuckDBPyRelation` gives a `LazyFrame` whose
    `implementation` is `duckdb`, and narwhals operations on it push into the
    DuckDB plan rather than executing - so a `Store` over a resolved overlay
    stays as lazy as the overlay is (§9.2, §9.3).
    """
    return nw.from_native(rel)
