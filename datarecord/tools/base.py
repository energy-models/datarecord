"""The tool interface: verify a record, build a model, read results back.

A `Tool` is the seam between the tool-agnostic record (dims, components,
attributes - design doc §8/§9) and one concrete modelling framework. The
call runs from the tool inward: `PyPSA.build(record)`. So the record layer imports
nothing from here, and a tool reads a record through its `Store` (§4) - how the
store resolves what it hands over is not a parameter of the interface.

Tools are plain module-level objects, imported by name (`from datarecord.tools.pypsa import
PyPSA`). There is no registry: a typo is then an `ImportError` at the call site rather
than a `KeyError` several frames in, and the return type of `build` is the framework's
own.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from duckdb import DuckDBPyRelation

if TYPE_CHECKING:
    from collections.abc import Callable

    import narwhals as nw

    from datarecord.data_record import DataRecord
    from datarecord.store import Store


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
        `(key, dim)` pairs the schema declares that this tool cannot honour
        - either because the record's files carry no column for the dim, or
        because the tool's own representation cannot express an overlay keyed
        that way. `key` is `"input_key"`, `"component_key"` or
        `"connection_key"`.

        The record layer trusts every key the schema declares (§3 fixes no
        column set for `dims/components/`, and what a key means downstream is
        framework-specific), so this is reported by a tool against the store
        it is actually handed rather than rejected when the schema is
        parsed.
    unsupported_values : frozenset of tuple of (str, str)
        `(component_type, attribute)` pairs whose stored *shape* this tool
        cannot represent, as opposed to a value it is missing. A
        piecewise-linear attribute (§7) where the tool takes only a scalar
        is the case that exists: the record stores it correctly and it is the
        translation that cannot express it.
    """

    dims: frozenset[str] = frozenset()
    component_types: frozenset[str] = frozenset()
    attributes: frozenset[tuple[str, str]] = frozenset()
    unsupported_keys: frozenset[tuple[str, str]] = frozenset()
    unsupported_values: frozenset[tuple[str, str]] = frozenset()

    def __bool__(self) -> bool:
        """Whether anything is required (or, for a `verify` result, missing)."""
        return bool(
            self.dims
            or self.component_types
            or self.attributes
            or self.unsupported_keys
            or self.unsupported_values
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
        return ", ".join(parts) if parts else "nothing"


def to_relation(frame: nw.LazyFrame) -> DuckDBPyRelation:
    """A `Store` frame as the DuckDB relation this tool builds against.

    `Store` hands over narwhals frames so the interface names no backend (§4),
    but a DuckDB-backed one wraps a relation and narwhals keeps it
    unmaterialised, so unwrapping costs nothing and the plan stays lazy. A tool
    that needs DuckDB's own SQL - `PIVOT`, which narwhals has no expression for
    - goes through here rather than reaching past the store to a `NodeCache`.

    Raises
    ------
    TypeError
        If `frame` is not DuckDB-backed. A tool written against DuckDB cannot
        build from another backing, and saying so here beats an attribute error
        several frames in.
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

    The seam for the mismatches between a record's vocabulary and a tool's.
    A record's attribute set is a superset across every tool, and tools
    disagree: one names an attribute differently, another wants a value
    derived from several. Both are declared here rather than open-coded in a
    `build`.

    Parameters
    ----------
    name : str
        The tool's name for the attribute.
    source : tuple of str
        Record attribute name(s) it is read from. A rename has one.
    compute : callable, optional
        Maps one resolved long relation per `source`, in order, to this
        attribute's long relation. `None` for a plain rename, which requires
        exactly one `source`.

        A relation in, a relation out: the transformation stays an
        unmaterialised DuckDB plan, so a computed attribute flows into the
        same pivot as an identity one with no special case downstream.
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

    def resolve(self, record: DataRecord) -> DuckDBPyRelation:
        """This attribute's long relation, resolved from `record`."""
        return self.resolve_store(record.store)

    def resolve_store(self, store: Store) -> DuckDBPyRelation:
        """This attribute's long relation, read through the `Store` interface.

        Takes the store rather than the record so a tool can build from any
        backing (§4), and unwraps each source's narwhals frame to the DuckDB
        relation `compute` is written against.
        """
        rels = [to_relation(store.attributes[s]) for s in self.source]
        if self.compute is None:
            return rels[0]
        return self.compute(*rels)


@dataclass(frozen=True)
class Schema:
    """A tool's attribute mapping against the record's vocabulary.

    Only the attributes that differ need an entry; anything unlisted is read
    from the record under the same name. An empty `Schema` is therefore the
    identity, which is what a tool whose vocabulary *is* the record's uses.

    Parameters
    ----------
    attrs : dict of str to tuple of Attr
        Per component type, the attributes that are renamed or computed.

    Notes
    -----
    `source` names attributes in the record's vocabulary - the names the
    store's schema declares (§5.2). This `Schema` is what reconciles that
    vocabulary with the tool's own, so a renamed or computed attribute is
    resolved from its sources rather than looked for under the tool's name.
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

    def resolve(self, record: DataRecord, ctype: str, name: str) -> DuckDBPyRelation:
        """`ctype`'s `name` as a long relation over `record`, mapping applied."""
        return self.attr(ctype, name).resolve(record)

    def resolve_store(self, store: Store, ctype: str, name: str) -> DuckDBPyRelation:
        """`ctype`'s `name` as a long relation over `store`, mapping applied."""
        return self.attr(ctype, name).resolve_store(store)


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
    method takes the record or model it operates on. A structural type, for
    annotating code that takes any tool - not a dispatch table; tools are
    reached by importing them.
    """

    name: str
    schema: Schema

    def requires(self, record: DataRecord) -> Requirements:
        """What this tool needs from `record` to build a model.

        Record-dependent, not a constant: which attributes are required
        follows the record's own component types and its declared schema.
        """
        ...

    def verify(self, record: DataRecord) -> Requirements:
        """What `record` fails to supply; falsy when the record is usable."""
        ...

    def build(self, record: DataRecord) -> Any:
        """The tool's model object, built from the resolved record."""
        ...

    def to_datarecord(self, model: Any) -> Store:
        """`model` presented as a layer `write_layer` can persist (§4).

        The inverse of `build`. Framework-specific: undoing a framework's own
        shape is exactly what a tool knows and the record layer does not.
        """
        ...

    def results(self, model: Any) -> dict[tuple[str, str], nw.DataFrame]:
        """This model's result attributes in the record's long form (§3).

        Keyed by `(component_type, attribute)`, each frame carrying the long
        schema's dim columns plus `value`. Narwhals frames, so the seam names
        no one dataframe library: a tool backed by polars returns the same
        type as one backed by pandas, and the write path (§12, v2) hits a
        native representation only at the DuckDB boundary.
        """
        ...


@dataclass
class _Missing:
    """Accumulator for a `verify` pass; internal to tool implementations."""

    dims: set[str] = field(default_factory=set)
    component_types: set[str] = field(default_factory=set)
    attributes: set[tuple[str, str]] = field(default_factory=set)
    unsupported_keys: set[tuple[str, str]] = field(default_factory=set)
    unsupported_values: set[tuple[str, str]] = field(default_factory=set)

    def freeze(self) -> Requirements:
        return Requirements(
            dims=frozenset(self.dims),
            component_types=frozenset(self.component_types),
            attributes=frozenset(self.attributes),
            unsupported_keys=frozenset(self.unsupported_keys),
            unsupported_values=frozenset(self.unsupported_values),
        )
