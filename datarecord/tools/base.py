"""The tool interface: verify a store, build a model, read results back.

The seam between a tool-agnostic record and one modelling framework (design doc
§12). The call runs from the tool inward (`PyPSA.build(record.store)`), so the
record layer imports nothing from here, and a tool reads through `Record` (§4).
Tools are module-level objects imported by name - no registry (§12).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from duckdb import DuckDBPyRelation

if TYPE_CHECKING:
    from collections.abc import Callable

    import narwhals as nw

    from datarecord.record import Frames, Record


@dataclass(frozen=True)
class Requirements:
    """What a tool needs from a record, and what a given record fails to supply.

    Parameters
    ----------
    dims : frozenset of str
        Dims the tool cannot build a model without.
    component_types : frozenset of str
        Component types the tool requires the record to define members for.
    attributes : frozenset of tuple of (str, str)
        `(component_type, attribute)` pairs the tool requires a value for.
        Named in the *record's* vocabulary, not the tool's, so a caller can
        act on them: an attribute the tool renames or computes (`Schema`) is
        reported by the source attribute it is missing.
    unsupported_keys : frozenset of tuple of (str, str)
        `(key, dim)` pairs the schema declares that this tool cannot honour;
        `key` is `"input_key"`, `"component_key"` or `"connection_key"`. The
        record layer trusts every declared key, so this is a tool's verdict on
        the store it was handed, not a schema rejection (§12).
    unsupported_values : frozenset of tuple of (str, str)
        `(component_type, attribute)` pairs whose stored *shape* this tool
        cannot represent, as opposed to a value it is missing - a
        piecewise-linear attribute where the tool takes a scalar (§7).
    names : frozenset of str
        Names two of the framework's components claim, which a record scopes
        store-wide (§3.5).
    """

    dims: frozenset[str] = frozenset()
    component_types: frozenset[str] = frozenset()
    attributes: frozenset[tuple[str, str]] = frozenset()
    unsupported_keys: frozenset[tuple[str, str]] = frozenset()
    unsupported_values: frozenset[tuple[str, str]] = frozenset()
    names: frozenset[str] = frozenset()

    def __bool__(self) -> bool:
        """Whether anything is required (or, for a `verify` result, missing)."""
        return bool(
            self.dims
            or self.component_types
            or self.attributes
            or self.unsupported_keys
            or self.unsupported_values
            or self.names
        )

    def describe(self) -> str:
        """A one-line summary, for an error message or a log line."""
        parts = []
        if self.dims:
            parts.append(f"dims {sorted(self.dims)}")
        if self.component_types:
            parts.append(f"component types {sorted(self.component_types)}")
        if self.attributes:
            parts.append(f"attributes {sorted(self.attributes)}")
        if self.unsupported_keys:
            unsupported = ", ".join(
                f"{dim} as {key}" for key, dim in sorted(self.unsupported_keys)
            )
            parts.append(f"unsupported keys ({unsupported})")
        if self.unsupported_values:
            parts.append(
                f"piecewise-linear attributes {sorted(self.unsupported_values)}"
            )
        if self.names:
            parts.append(
                f"names claimed by more than one component type "
                f"{sorted(self.names)} (§3.5)"
            )
        return ", ".join(parts) if parts else "nothing"


def to_relation(frame: nw.LazyFrame) -> DuckDBPyRelation:
    """A `Record` frame as the DuckDB relation this tool builds against.

    Unwrapping costs nothing and the plan stays lazy (§4.4). For a tool needing
    DuckDB's own SQL - `PIVOT`, which narwhals has no expression for - rather
    than reaching past the store to a `NodeCache`.

    Raises
    ------
    TypeError
        If `frame` is not DuckDB-backed.
    """
    native = frame.to_native()
    if not isinstance(native, DuckDBPyRelation):
        msg = (
            f"expected a DuckDB-backed store frame, got {type(native).__name__}; "
            "the PyPSA tool builds with DuckDB SQL"
        )
        raise TypeError(msg)
    return native


# -- record -> tool attribute mapping ---------------------------------------


@dataclass(frozen=True)
class Attr:
    """One tool attribute and the record attribute(s) it is built from.

    The vocabulary seam (§12): a rename, or a value computed from several,
    declared rather than open-coded in a `build`.

    Parameters
    ----------
    name : str
        The tool's name for the attribute.
    source : tuple of str
        Record attribute name(s) it is read from. A rename has one.
    compute : callable, optional
        Maps one resolved long relation per `source`, in order, to this
        attribute's long relation; `None` for a plain rename, which requires
        exactly one `source`. A relation in, a relation out, so the plan stays
        unmaterialised and a computed attribute needs no special case downstream.
    """

    name: str
    source: tuple[str, ...]
    compute: Callable[..., DuckDBPyRelation] | None = None

    def __post_init__(self) -> None:
        if self.compute is None and len(self.source) != 1:
            msg = (
                f"attribute {self.name!r} maps {len(self.source)} sources with no "
                "compute; a plain rename needs exactly one"
            )
            raise ValueError(msg)

    def resolve(self, store: Record) -> DuckDBPyRelation:
        """This attribute's long relation, read through the `Record` interface.

        The store rather than the record, so a tool builds from any backing (§4).
        """
        rels = [to_relation(store.attributes[s]) for s in self.source]
        if self.compute is None:
            return rels[0]
        return self.compute(*rels)


@dataclass(frozen=True)
class Schema:
    """A tool's attribute mapping against the record's vocabulary (§12).

    Only differing attributes need an entry; anything unlisted is read under the
    same name, so an empty `Schema` is the identity. `Attr.source` names
    attributes in the *record's* vocabulary (§5.2).

    Parameters
    ----------
    attrs : dict of str to tuple of Attr
        Per component type, the attributes that are renamed or computed.
    """

    attrs: dict[str, tuple[Attr, ...]] = field(default_factory=dict)

    def attr(self, ctype: str, name: str) -> Attr:
        """`ctype`'s mapping for `name`, or the identity when it has none."""
        for a in self.attrs.get(ctype, ()):
            if a.name == name:
                return a
        return Attr(name=name, source=(name,))

    def sources(self, ctype: str, name: str) -> tuple[str, ...]:
        """Which record attribute(s) `ctype`'s `name` needs; itself, if unmapped.

        What `verify` checks resolvability against: a renamed or computed
        attribute is satisfied by its sources, not by its own name.
        """
        return self.attr(ctype, name).source

    def resolve(self, store: Record, ctype: str, name: str) -> DuckDBPyRelation:
        """`ctype`'s `name` as a long relation over `store`, mapping applied."""
        return self.attr(ctype, name).resolve(store)


class UnsupportedRecordError(ValueError):
    """A record does not define everything the tool needs (`Tool.verify`)."""

    def __init__(self, tool: str, missing: Requirements) -> None:
        super().__init__(
            f"record cannot build a {tool} model; missing: {missing.describe()}"
        )
        self.missing = missing


@runtime_checkable
class Tool(Protocol):
    """One modelling framework's view of a record.

    Implementations are stateless (a module-level singleton is fine): every
    method takes the store or model it operates on. A structural type, for
    annotating code that takes any tool - not a dispatch table; tools are
    reached by importing them.

    A `Record` rather than a record throughout, so a tool builds from a
    directory as readily as from an overlay and has no reason to know layering
    exists (§12).
    """

    name: str
    schema: Schema

    def requires(self, store: Record) -> Requirements:
        """What this tool needs from `store` to build a model.

        Record-dependent, not a constant: which attributes are required
        follows the store's own component types and its declared schema.
        """
        ...

    def verify(self, store: Record) -> Requirements:
        """What `store` fails to supply; falsy when the store is usable."""
        ...

    def build(self, store: Record) -> Any:
        """The tool's model object, built from the resolved store."""
        ...

    def to_datarecord(self, model: Any) -> Record:
        """`model` presented as a layer `write_record` can persist (§4).

        The inverse of `build`. Framework-specific: undoing a framework's own
        shape is exactly what a tool knows and the record layer does not.
        """
        ...

    def results(self, model: Any) -> Frames:
        """This model's result attributes in the record's long form (§3).

        Keyed by attribute, each frame in the long schema - `name`, the dim
        columns, `value` - the same shape `Record.outputs` presents, so results
        go straight to `write_record` or to `set(..., kind="outputs")`. One frame
        spans every component type, needing no type column (§3.5).

        Narwhals frames, so the seam names no one dataframe library, and lazy so
        an implementation may fetch a result attribute on demand rather than
        materialising every one.
        """
        ...
