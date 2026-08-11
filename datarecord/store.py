"""The `Store` interface and its two backings (design doc §4, §9.3).

One interface over everything a parquet store holds, with two backings a
consumer cannot tell apart: `LayeredStore` over a record's resolved overlay, and
`DirectoryStore` over a plain parquet directory. That is what lets a modelling
framework read a hundred-layer overlay without knowing layering exists.

The interface lives here with the backings rather than with any one consumer of
it: `write_layer` consumes a `Store`, `datarecord.tools.pypsa` both implements and
consumes one, and a future `MutableStore` extends it. None of those is where it
belongs.

Framework-independent, like the rest of `datarecord`: these hand over
narwhals frames and name no modelling framework.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import narwhals as nw

from datarecord.duck import try_read_parquet
from datarecord.node_cache import NodeCache, read_json, read_schema
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


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
class Solved(Protocol):
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


@dataclass(frozen=True)
class LayeredStore:
    """A record's resolved overlay, as a `Store` (§9.3).

    Every member delegates to `NodeCache`: the resolution is already there, and
    this is the protocol's shape over it rather than a second implementation of
    it. So an overlay-backed store costs exactly what the equivalent
    `NodeCache` call costs, and `flags` in particular is free - the owner map
    computed it at fold time (§9.1).
    """

    node_cache: NodeCache

    @property
    def schema(self) -> Schema:
        return self.node_cache.schema

    @property
    def con(self) -> DuckDBPyConnection:
        return self.node_cache.con

    @cached_property
    def dims(self) -> LazyFrames:
        axes = self.node_cache.dims.axes
        return LazyFrames(tuple(axes), lambda dim: _lazy(axes[dim]))

    @cached_property
    def components(self) -> LazyFrames:
        types = tuple(sorted(self.node_cache.component_types()))
        return LazyFrames(types, self._component_frame)

    @cached_property
    def connections(self) -> LazyFrames:
        rows = self.node_cache.connections.project("component_type").distinct()
        types = tuple(sorted(r[0] for r in rows.fetchall()))
        return LazyFrames(types, self._connection_frame)

    @cached_property
    def attributes(self) -> LazyFrames:
        names = tuple(self.node_cache.attribute_names())
        return LazyFrames(names, lambda attr: _lazy(self.node_cache.relation(attr)))

    @cached_property
    def outputs(self) -> LazyFrames:
        names = tuple(self.node_cache.output_names())
        return LazyFrames(names, lambda attr: _lazy(self.node_cache.outputs(attr)))

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Straight off the `inputs` owner map, which folded these in for free (§9.1)."""
        return {
            attribute: Flags(varies, broadcast, breakpoints)
            for attribute, (varies, broadcast, breakpoints) in (
                self.node_cache.attributes_of(ctype).items()
            )
        }

    # -- frames, ordered by the map's `order_key` (§9.3) ---------------------

    def _component_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.component_frame(ctype), ctype)

    def _connection_frame(self, ctype: str) -> nw.LazyFrame:
        return self._ordered(self.node_cache.connection_frame(ctype), ctype)

    def _ordered(self, rel: DuckDBPyRelation | None, ctype: str) -> nw.LazyFrame:
        """`rel` in member order, which for an overlay means sorted by `order_key`.

        A `Store` promises member order (§9.3); the fold's own output has none
        of its own (its union puts a layer's own contribution first), so the
        order is imposed here. `order_key` stays in the frame rather than being
        projected away - see §14 on whether it should.
        """
        if rel is None:
            raise KeyError(ctype)
        return _lazy(rel.order("order_key"))


@dataclass(frozen=True)
class DirectoryStore:
    """A plain parquet directory, as a `Store` (§9.3).

    One store, no overlay: what the files hold is what it presents. Reading a
    single layer directly, or any standard parquet store blocks did not write.

    Unlike `LayeredStore` there is no owner map, so `flags` is aggregated from the
    files - a narrow scan, but a scan (§9.3). Cached per component type, since
    a build asks once per type and the answer cannot change under a read-only
    store.
    """

    uri: str
    con: DuckDBPyConnection

    def __post_init__(self) -> None:
        object.__setattr__(self, "_flags_cache", {})

    @property
    def base(self) -> str:
        return self.uri if self.uri.endswith("/") else self.uri + "/"

    @cached_property
    def schema(self) -> Schema:
        """This store's own `manifest.json`, else the one beside `con`'s layers (§5.6).

        No fold: a directory is one store, so there is nothing to merge across
        (§8.2). A standalone store carries its own manifest and that is the
        answer. A single *layer* of a layered store does not - its schema lives
        beside `layers/`, one for the whole tree - and a connection is already
        scoped to one such root, so reading one layer directly needs nothing
        supplied. Neither present reads as an empty `Schema`: it describes no
        dims, so it resolves no dimensioned data, which is the honest answer
        rather than a guessed default.
        """
        raw = read_json(self.base + "manifest.json")
        if raw is not None:
            return Schema.model_validate(raw)
        return read_schema(self.con)

    @cached_property
    def dims(self) -> LazyFrames:
        # A dim's axis file is `{dim}s.parquet`, so the declared dims name the
        # files to look for; only those that exist become keys.
        declared = self.schema.dims
        present = tuple(d for d in declared if self._read(f"dims/{d}s.parquet"))
        return LazyFrames(
            present, lambda dim: _lazy(self._require(f"dims/{dim}s.parquet"))
        )

    @cached_property
    def components(self) -> LazyFrames:
        return self._by_type("dims/components")

    @cached_property
    def connections(self) -> LazyFrames:
        return self._by_type("dims/connections")

    @cached_property
    def attributes(self) -> LazyFrames:
        return self._by_attribute("inputs")

    @cached_property
    def outputs(self) -> LazyFrames:
        return self._by_attribute("outputs")

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Aggregated from `inputs/*.parquet`, grouped by component type (§4.3).

        Parquet's footer statistics cannot answer this: `stats_null_count` is
        per row group, not per component type, so a file mixing one type's
        per-timestep rows with another's single row says nothing about either.
        Hence a real aggregate - the dim columns projected, no value pages read.

        Which dims to report on comes from the schema (§5), intersected
        with what the files actually carry: a store may declare a dim no file
        has a column for, and `varies`/`broadcast` describe rows.
        """
        cache: dict[str, dict[str, Flags]] = self._flags_cache  # type: ignore[attr-defined]
        if ctype in cache:
            return cache[ctype]

        rel = self._read("inputs/*.parquet", union_by_name=True)
        result: dict[str, Flags] = {}
        if rel is not None:
            declared = self.schema.dims
            dims = tuple(d for d in declared if d in rel.columns)
            pwl = (
                "bool_or(breakpoint IS NOT NULL)"
                if "breakpoint" in rel.columns
                else "false"
            )
            projections = ", ".join(
                [
                    *(f'bool_or("{d}" IS NOT NULL) AS "v_{d}"' for d in dims),
                    *(f'bool_or("{d}" IS NULL) AS "b_{d}"' for d in dims),
                    f"{pwl} AS breakpoints",
                ]
            )
            rows = self.con.sql(
                f"SELECT attribute, {projections}"
                " FROM rel WHERE component_type = $ctype"
                " GROUP BY attribute",
                params={"ctype": ctype},
            ).fetchall()
            n = len(dims)
            result = {
                r[0]: Flags(
                    frozenset(
                        d for d, on in zip(dims, r[1 : 1 + n], strict=True) if on
                    ),
                    frozenset(
                        d
                        for d, on in zip(dims, r[1 + n : 1 + 2 * n], strict=True)
                        if on
                    ),
                    bool(r[1 + 2 * n]),
                )
                for r in rows
            }
        cache[ctype] = result
        return result

    # -- reads --------------------------------------------------------------

    def _read(self, path: str, **kwargs: object) -> DuckDBPyRelation | None:
        return try_read_parquet(self.base + path, self.con, **kwargs)

    def _require(self, path: str) -> DuckDBPyRelation:
        rel = self._read(path)
        if rel is None:  # pragma: no cover - keys come from what exists
            raise KeyError(path)
        return rel

    def _by_type(self, subdir: str) -> LazyFrames:
        """Keys from the `<Type>.parquet` files present in `subdir`."""
        rel = self._read(f"{subdir}/*.parquet", union_by_name=True)
        if rel is None:
            return EMPTY
        rows = rel.project("component_type").distinct().order("component_type")
        types = tuple(r[0] for r in rows.fetchall())
        return LazyFrames(
            types, lambda ctype: _lazy(self._require(f"{subdir}/{ctype}.parquet"))
        )

    def _by_attribute(self, subdir: str) -> LazyFrames:
        """Keys from the `<attr>.parquet` files present in `subdir`.

        Read from the `attribute` column rather than by listing filenames, so
        one code path serves a local directory and a remote prefix alike.
        """
        rel = self._read(f"{subdir}/*.parquet", union_by_name=True)
        if rel is None:
            return EMPTY
        rows = rel.project("attribute").distinct().order("attribute")
        names = tuple(r[0] for r in rows.fetchall())
        return LazyFrames(
            names, lambda attr: _lazy(self._require(f"{subdir}/{attr}.parquet"))
        )
