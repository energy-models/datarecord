# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The `RecordLike` protocol and `Record`, the class this package provides.

`Record` is the one resolver-backed record; a framework object presenting
itself as a record satisfies the protocol structurally, which is what `tools/`
is built on.

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
- [relations](https://energy-models.github.io/datarecord/design/record/#relations)
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from duckdb import ColumnExpression as col
from duckdb import DuckDBPyRelation

from datarecord.layered.categories import with_properties
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from datarecord.layered.resolve import Resolver

Relations = Mapping[str, DuckDBPyRelation]
"""What a `Record` hands over: named relations, each an unmaterialised plan.

The `Mapping` ABC, so a plain `dict` satisfies it as fully as `LazyRelations`
does.

Notes
-----
- [relations](https://energy-models.github.io/datarecord/design/record/#relations)
"""


class LazyRelations(Mapping[str, DuckDBPyRelation]):
    """A `Relations` whose values are built on `__getitem__`, not up front.

    For a backing where *building* a relation is itself I/O: `read_parquet`
    reads the footer to bind the schema, a round trip per file against a remote
    record.

    Parameters
    ----------
    keys
        Every key this mapping holds, in iteration order. Listing them must be
        cheap: nothing here builds a relation.
    build
        Called with one key to produce its relation, once per `__getitem__`;
        memoise inside `build` if repeated lookups matter.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/record/#relations)
    """

    def __init__(self, keys: tuple[str, ...], build: Callable[[str], DuckDBPyRelation]):
        self._keys = keys
        self._build = build

    def __getitem__(self, key: str) -> DuckDBPyRelation:
        if key not in self._keys:
            raise KeyError(key)
        return self._build(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __contains__(self, key: object) -> bool:
        # `Mapping`'s default answers this by calling `__getitem__` and
        # catching `KeyError`, which would build a relation just to test for one.
        return key in self._keys

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self._keys)!r})"


def _unreachable(key: str) -> DuckDBPyRelation:
    raise KeyError(key)  # pragma: no cover - `EMPTY` has no keys to look up


EMPTY = LazyRelations((), _unreachable)
"""A `LazyRelations` with no keys, for a record that has none of some kind."""


@dataclass(frozen=True)
class Flags:
    """Which axes an attribute's rows actually use.

    The two sets are **not** complements: a dim in both means the rows
    disagree, which is the instruction to use both containers rather
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
class RecordLike(Protocol):
    """What a record answers, however it is backed, as DuckDB relations.

    Read-only: writing is `store.root(source)`, `rev.child(source)` or
    `ws.write(source, uri)`, whose writer reads a `LayerData` rather than this -
    a framework's own `RecordLike` reaches it through an adapter.

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
    def dims(self) -> Relations:
        """Axis relations, keyed by dim: each one's label column, nothing else.

        Properties are in `properties`, attributes in `attributes`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
        """
        ...

    @property
    def properties(self) -> Relations:
        """Per dim or group declaring properties, its key and their stored values, no default applied.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
        """
        ...

    @property
    def groups(self) -> Relations:
        """Each declared group's rows, keyed by group - one relation each.

        A group declares which tuples over several dims exist - `connection`
        over `(entity, bus)` is the one every record with connections has, and
        it is one instance rather than a member of its own.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [where the rows live](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        ...

    @property
    def attributes(self) -> Relations:
        """Long attribute relations, keyed by attribute name - one per file.

        A row carries its coordinates alone; select by category through
        `properties`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        ...

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Every attribute with rows, mapped to the shape its rows take.

        Given `categories` of one dim, only rows reaching a label among them
        count, and only attributes those labels carry.

        Raises
        ------
        ValueError
            If the keywords categorize no dim or several.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
        """
        ...


@runtime_checkable
class LayerData(Protocol):
    """The rows of one thing - a layer, or a fold - enumerated and read.

    `write_layer`'s input: it needs `schema` and, per kind, which keys this
    thing holds and each one's rows - the enumerate-and-read pairs below, no
    more. `layered.sources.LayerSource` and `layered.resolve.Resolver` both
    satisfy this structurally, one answering for its own layer and the other
    for everything folded into it; the object decides which, never a mode flag
    or a name prefix. A framework's own producer speaks `RecordLike` instead,
    which `write_layer` adapts.

    `schema` is not a peer of the other members but what decides which of them
    exist: `groups`/`group` answer only for the groups it declares - an
    enumerator answers the empty set rather than a phantom key where the schema
    declares nothing.

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/record/#layerdata)
    """

    @property
    def schema(self) -> Schema: ...

    @property
    def frozen(self) -> bool: ...

    def axes(self) -> Iterable[str]: ...
    def axis(self, dim: str) -> DuckDBPyRelation | None: ...

    def groups(self) -> Iterable[str]: ...
    def group(self, name: str) -> DuckDBPyRelation | None: ...

    def attributes(self) -> Iterable[str]: ...
    def attribute(self, name: str) -> DuckDBPyRelation | None: ...


def _stable_cache(method: Callable[[Record], LazyRelations]) -> property:
    """A `Record` member cached only while the fold behind it cannot go stale.

    Every one of these answers a *key set*, which is a question about which
    layers exist rather than about their rows - so it is cacheable exactly when
    `Resolver.frozen` holds. Past a staging area it is not: an `add` or a `set`
    naming a new attribute changes the answer, and a cached tuple of names would
    hide the edit that was just made.

    What is cached is the names and a lookup building each relation per access,
    so none is held and the record reads from any thread.

    Notes
    -----
    - [a layer's data is write-once](https://energy-models.github.io/datarecord/design/layers/#a-layers-data-is-write-once)
    - [reading with pending edits](https://energy-models.github.io/datarecord/design/working-record/#reading-with-pending-edits)
    - [a cursor per thread](https://energy-models.github.io/datarecord/design/read-path/#a-cursor-per-thread)
    """
    attr = f"_cached_{method.__name__}"

    @wraps(method)
    def get(self: Record) -> LazyRelations:
        if not self.resolver.frozen:
            return method(self)
        try:
            return cast("LazyRelations", object.__getattribute__(self, attr))
        except AttributeError:
            value = method(self)
            object.__setattr__(self, attr, value)
            return value

    return property(get)


@dataclass(frozen=True)
class Record:
    """A record's resolved view, as DuckDB relations.

    The interface over one `Resolver`, not a second implementation of it: a member costs what the equivalent `Resolver` call costs, and `flags`
    in particular is free, the owner map having folded it in.

    One class for every backing, because a fold over one source degenerates to
    a scan of it - so a plain parquet directory is read as the one layer it is
    (`Workspace.at`) rather than by a second code path. `RecordLike` is the
    protocol a caller annotates against; this is the class a caller holds.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """

    resolver: Resolver

    @property
    def schema(self) -> Schema:
        return self.resolver.schema

    @property
    def con(self) -> DuckDBPyConnection:
        """The calling thread's cursor, which a relation from this record is planned on."""
        return self.resolver.con

    @_stable_cache
    def dims(self) -> LazyRelations:
        names = tuple(self.resolver.dims.axes)
        return LazyRelations(names, self._axis_frame)

    @_stable_cache
    def properties(self) -> LazyRelations:
        coords = self.resolver.dims
        subjects = tuple(
            s for s in self.schema.properties if s in coords.axes or s in coords.groups
        )
        return LazyRelations(subjects, self._property_frame)

    @_stable_cache
    def groups(self) -> LazyRelations:
        """Each declared group's rows, keyed by group - one relation each.

        Only groups some layer wrote a row of; a declared group nothing
        populates is absent rather than present-and-empty.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        groups = tuple(
            g for g in self.resolver.schema.groups if self.resolver.group(g) is not None
        )
        return LazyRelations(groups, self._group_frame)

    @_stable_cache
    def attributes(self) -> LazyRelations:
        names = tuple(self.resolver.attributes())
        return LazyRelations(names, self._attribute_relation)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Straight off the owner map, which folded these in for free.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return self.resolver.fold.flags(**categories)

    # -- relations, in member order (the resolved file's row order) (https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold) --

    def _axis_frame(self, dim: str) -> DuckDBPyRelation:
        return self._rel(self.resolver.axis(dim), dim).project(col(dim))

    def _attribute_relation(self, attribute: str) -> DuckDBPyRelation:
        return self._rel(self.resolver.attribute(attribute), attribute)

    def _property_frame(self, subject: str) -> DuckDBPyRelation:
        is_group = subject in self.schema.groups
        found = (
            self.resolver.group(subject) if is_group else self.resolver.axis(subject)
        )
        rel = with_properties(self.schema, self._rel(found, subject), subject)
        names = (subject, *self.schema.properties[subject])
        return rel.project(*(col(n) for n in names))

    def _group_frame(self, group: str) -> DuckDBPyRelation:
        return self._rel(self.resolver.group_frame(group), group)

    @staticmethod
    def _rel(rel: DuckDBPyRelation | None, key: str) -> DuckDBPyRelation:
        """`rel`, which carries member order as its row order; a miss raises `KeyError(key)`.

        The resolved relation is folded in first-introduced member order and read
        back order-preserving (`fold_axis`), so a `Record` promises that order
        without a re-sort and without a persisted `order_key`.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        if rel is None:
            raise KeyError(key)
        return rel
