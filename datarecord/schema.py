"""The schema: what a store's data is, and how a patch to it behaves (§5).

One schema per store, and `manifest.json` is how it is written down - the two
words name the same thing, the file and the object (§5.6).

Framework-independent. `component_type`, `name` and `attribute` are strings
because those vocabularies belong to a modelling framework and this package
knows none: a type no tool recognises reads back fine and is reported by the
tool that cannot build it, not rejected here.
"""

from __future__ import annotations

import math
from graphlib import CycleError, TopologicalSorter
from typing import Any, Literal

from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

KeyKind = Literal["component", "connection"]
"""Which entity table a dim keys (§5.3)."""

BusRelation = Literal["component", "connection"]
"""Whether an attribute belongs to a component or to one of its connections (§6)."""

# Columns the format fixes, whatever the schema declares (§3.2). Neither
# declared nor optional: `bus`/`breakpoint` are NULL for the ordinary
# component-level scalar, so one column set serves every kind of row.
STRUCTURAL_TYPES = {
    "component_type": "VARCHAR",
    "name": "VARCHAR",
    "bus": "VARCHAR",
    "attribute": "VARCHAR",
    "breakpoint": "DOUBLE",
    "layer_uuid": "UUID",
    "order_key": "BIGINT",
    "deleted": "BOOLEAN",
    "breakpoints": "BOOLEAN",
}


# The owner map's flag columns: two structs with a field per declared dim, so
# the map's column set does not depend on the schema and adding a dim stays the
# compatible change §5.7 says it is. `breakpoints` is outside both, being no dim
# (§9.1, §7).
FLAG_COLUMNS = ("varies", "broadcast", "breakpoints")


def flag_type(dims: tuple[str, ...]) -> str:
    """The DuckDB type of one flag struct: a BOOLEAN per declared dim (§9.1).

    `dims` is never empty: a schema declaring no dims describes no dimensioned
    data, which `Schema` rejects (§5.1) - so the struct always has a field and
    needs no placeholder for DuckDB's want of an empty one.
    """
    fields = ", ".join(f'"{d}" BOOLEAN' for d in dims)
    return f"STRUCT({fields})"


class Dimension(BaseModel):
    """One axis attribute data may vary over: its shape, not its data (§5.1).

    Not which dims an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared (§3.4).

    Parameters
    ----------
    dtype
        The axis labels' type, as a DuckDB type name.
    within
        Dims this one's labels identify a point only *within*; transitive (§5.4).
    keys
        Which entity tables this dim keys, so an entity exists per value of it
        (§5.3).
    unit
        What this axis's *labels* measure, if anything - `None` is undeclared,
        `""` genuinely dimensionless (§5.8).
    description
        What the axis is, in prose. Never interpreted (§5.8).
    """

    dtype: str
    within: frozenset[str] = frozenset()
    keys: frozenset[KeyKind] = frozenset()
    unit: str | None = None
    description: str | None = None


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take (§5.2).

    Parameters
    ----------
    dtype
        The value column's type, as a DuckDB type name.
    dims
        Dims this attribute may vary over; a subset of those declared. Varying
        over nothing is what puts it in `dims/components/<Type>.parquet` rather
        than `inputs/`, so the schema decides the file split (§3.1).
    default
        The value a coordinate no row covers takes (§3.3).
    breakpoints
        Whether it may carry a piecewise-linear curve (§7).
    bus
        Whether it belongs to a component or to one of its connections (§6).
    unit
        What the values measure - `"MW"`, `"EUR/MWh"`. Stored and never
        interpreted; `None` is undeclared, `""` genuinely dimensionless (§5.8).
    description
        What the attribute is, in prose. Never interpreted (§5.8).
    """

    dtype: str
    default: Any | None = None
    dims: frozenset[str] = frozenset()
    breakpoints: bool = False
    bus: BusRelation = "component"
    unit: str | None = None
    description: str | None = None

    @field_serializer("default")
    def _serialise_default(self, value: Any) -> Any:
        """Encode a non-finite default as a string; JSON has no literal for one.

        `inf` is an ordinary default for an unbounded capacity, and JSON's
        `Infinity` is not valid JSON - a plain dump reads back as `None`,
        turning "unbounded" into "no default". `_parse_default` reverses this.
        """
        if isinstance(value, float) and not math.isfinite(value):
            return f"__{value}__"
        return value

    @field_validator("default", mode="before")
    @classmethod
    def _parse_default(cls, value: Any) -> Any:
        """Decode what `_serialise_default` encoded."""
        if isinstance(value, str) and value.startswith("__") and value.endswith("__"):
            try:
                return float(value[2:-2])
            except ValueError:
                return value
        return value

    @property
    def varying(self) -> bool:
        """Whether this attribute lives in `inputs/` rather than `dims/components/` (§3.1)."""
        return bool(self.dims)


class Schema(BaseModel):
    """One store's schema (§5).

    Parameters
    ----------
    version
        Bumped by any change to the declarations (§5.7). A reader meeting a
        version it was not written for should refuse rather than guess.
    dimensions
        Every declared axis, keyed by dim name.
    attributes
        Component type -> attribute -> spec.
    partial
        Which dims a layer may patch value by value (§5.5). `None` for a store
        with no layers, since nothing overrides anything. A dim outside it is
        one a layer owns entirely once it touches it.
    meta
        A framework's own top-level data - network attributes, CRS, free-form
        metadata. Stored and never interpreted, since none of it describes the
        dimensioned data.
    """

    version: int = 1
    dimensions: dict[str, Dimension] = Field(default_factory=dict)
    attributes: dict[str, dict[str, AttributeSpec]] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes (§5.1, §5.3, §5.4)."""
        declared = set(self.dimensions)

        # Attributes but no axes is a table, not a record (§5.1). Rejected here
        # so the owner map never needs a struct with no fields, which DuckDB has
        # no type for. A wholly empty `Schema()` stays legal: "no manifest yet".
        if self.attributes and not declared:
            msg = (
                "a schema declaring attributes must declare at least one dim; "
                "attribute data varying over no axis is not a record (§5.1)"
            )
            raise ValueError(msg)

        for dim, spec in self.dimensions.items():
            unknown = sorted(spec.within - declared)
            if unknown:
                msg = f"dim {dim!r} is `within` undeclared dims {unknown}"
                raise ValueError(msg)
            if dim in spec.within:
                msg = f"dim {dim!r} is `within` itself"
                raise ValueError(msg)

        # `TopologicalSorter` only needs preparing to reject a cycle, and
        # `CycleError.args[1]` is the offending path - so the acyclicity §5.4
        # requires is stdlib rather than a graph walk kept here.
        try:
            TopologicalSorter(
                {d: s.within for d, s in self.dimensions.items()}
            ).prepare()
        except CycleError as e:
            msg = f"`within` is cyclic: {' -> '.join(e.args[1])}"
            raise ValueError(msg) from e

        for ctype, attrs in self.attributes.items():
            for attr, attr_spec in attrs.items():
                unknown = sorted(attr_spec.dims - declared)
                if unknown:
                    msg = f"{ctype}.{attr} varies over undeclared dims {unknown}"
                    raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared)
            if unknown:
                msg = f"`partial` names undeclared dims {unknown}"
                raise ValueError(msg)
            # Nothing for the tombstone to select (§5.3).
            for dim, spec in self.dimensions.items():
                if spec.keys and dim not in self.partial:
                    msg = (
                        f"dim {dim!r} keys {sorted(spec.keys)} but is not `partial`; "
                        f"membership cannot be scoped per value of an axis owned whole"
                    )
                    raise ValueError(msg)
        return self

    # -- derived key sets (§5.3, §5.5) --------------------------------------

    @property
    def dims(self) -> tuple[str, ...]:
        """Every declared dim, in declaration order - the long schema's dim columns (§3.2)."""
        return tuple(self.dimensions)

    def owned_per(self, ctype: str, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per (§5.5).

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `Schema.partial` which axes a layer may patch
        value by value, and ownership is their intersection. A dim in `dims`
        but not `partial` is owned whole, so a patch to one of its values
        restates the attribute's entire extent along it.
        """
        spec = self.attributes.get(ctype, {}).get(attribute)
        if spec is None:
            return frozenset()
        return spec.dims & (self.partial or frozenset())

    @property
    def input_dims(self) -> tuple[str, ...]:
        """Dims that key `inputs/` rows, in declaration order (§9.1).

        `partial` itself: the fold's key is one fixed tuple over all
        attributes, so it must carry every axis *any* layer may patch by
        value, not only those some currently declared attribute varies over.
        An attribute not owned per one of them writes NULL there, which is the
        existing "NULL means all values" rule (§3.3) - and that is also what
        lets a schema declare an axis before any attribute uses it.
        """
        partial = self.partial or frozenset()
        return tuple(d for d in self.dims if d in partial)

    def _keyed(self, kind: KeyKind) -> tuple[str, ...]:
        return tuple(d for d, s in self.dimensions.items() if kind in s.keys)

    @property
    def component_dims(self) -> tuple[str, ...]:
        """Dims keying `dims/components/<Type>.parquet` and its tombstones (§5.3)."""
        return self._keyed("component")

    @property
    def connection_dims(self) -> tuple[str, ...]:
        """Dims keying `dims/connections/<Type>.parquet` and its tombstones (§5.3)."""
        return self._keyed("connection")

    def axis_key(self, dim: str) -> tuple[str, ...]:
        """A dim's axis-table key: `(*parents, dim)`, parents first (§5.4).

        Parents in declaration order, and transitively - a dim `within` another
        that is itself `within` a third is keyed by all three.
        """
        seen = _ancestors(dim, {d: s.within for d, s in self.dimensions.items()})
        return (*(d for d in self.dims if d in seen), dim)

    # -- key and column sets (§3.2, §9.1) -----------------------------------

    @property
    def long_columns(self) -> tuple[str, ...]:
        """The long schema's full column set (§3.2).

        `bus` and `breakpoint` are part of the schema, not optional extensions
        to it: both are NULL for the ordinary component-level scalar, so one
        column set serves every kind of attribute row (§6, §7).

        No `component_type` (§3.5).
        """
        return (
            "name",
            "bus",
            *self.dims,
            "attribute",
            "breakpoint",
            "value",
        )

    @property
    def input_key(self) -> tuple[str, ...]:
        """Inputs-map key columns, compared NULL-safely when folding (§9.1).

        `bus` is in the key so a per-connection attribute is owned per
        connection rather than per component (§6); it is NULL for a
        component-level attribute, which then keys against the map's NULL.
        """
        return ("name", "bus", *self.input_dims, "attribute")

    @property
    def component_key(self) -> tuple[str, ...]:
        """Components-map key columns, compared NULL-safely when folding (§9.1).

        The type is carried as a column, not keyed (§3.5).
        """
        return ("name", *self.component_dims)

    @property
    def connection_key(self) -> tuple[str, ...]:
        """Connections-map key columns (§6).

        `bus` identifies the connection, so it is a required key column rather
        than a broadcast dim: never expanded against an axis, only compared
        NULL-safely. `role` and `component_type` describe the connection and key
        nothing (§3.5).
        """
        return ("name", "bus", *self.connection_dims)

    @property
    def input_columns(self) -> tuple[str, ...]:
        """The inputs map's full column set (§9.1)."""
        return (*self.input_key, "layer_uuid", *FLAG_COLUMNS)

    @property
    def component_columns(self) -> tuple[str, ...]:
        """The components map's full column set; the type is carried, not keyed (§3.5, §9.1)."""
        return ("component_type", *self.component_key, "layer_uuid", "order_key")

    @property
    def connection_columns(self) -> tuple[str, ...]:
        """The connections map's full column set (§9.1)."""
        return ("component_type", *self.connection_key, "layer_uuid", "order_key")

    # -- typing (§3.2, §10) -------------------------------------------------

    def column_type(self, column: str) -> str | None:
        """The declared type for one column, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims, and
        the owner map's two flag structs, whose fields follow the schema's dims
        (§9.1).

        A schema declaring no dims at all is "no manifest yet" (§5.6) rather
        than a store to fold, and DuckDB has no empty struct - so the flag
        columns are undeclared there, and a caller building an empty relation
        falls back to `VARCHAR` for a map that will never hold a row.
        """
        if column in STRUCTURAL_TYPES:
            return STRUCTURAL_TYPES[column]
        if column in self.dimensions:
            return self.dimensions[column].dtype
        if column in ("varies", "broadcast"):
            return flag_type(self.dims) if self.dims else None
        return None

    def value_type(self, ctype: str, attribute: str) -> str | None:
        """The `value` column's type for one attribute (§3.2)."""
        spec = self.attributes.get(ctype, {}).get(attribute)
        return None if spec is None else spec.dtype

    def types_declaring(self, attribute: str) -> frozenset[str]:
        """Which component types declare `attribute` - what `names=None` targets (§11.2)."""
        return frozenset(
            c for c, attrs in self.attributes.items() if attribute in attrs
        )

    # -- versioning (§5.7) --------------------------------------------------

    def compatible_with(self, other: Schema) -> list[str]:
        """Why layers written under `other` would not read under `self` (§5.7).

        Empty when the change is compatible: old layers stay readable and only
        `version` moves. The compatible changes are those where NULL already
        means what the new schema needs it to mean, so the decode rule absorbs
        them without touching a row (§3.3).

        Returns
        -------
        list of str
            One reason per incompatibility, empty if there are none.
        """
        problems = []

        for dim, was in other.dimensions.items():
            now = self.dimensions.get(dim)
            if now is None:
                problems.append(f"dim {dim!r} removed")
                continue
            if now.dtype != was.dtype:
                problems.append(f"dim {dim!r} dtype {was.dtype} -> {now.dtype}")
            if now.within != was.within:
                problems.append(
                    f"dim {dim!r} nesting changed; the axis key changes shape"
                )
            gained = now.keys - was.keys
            if gained:
                problems.append(
                    f"dim {dim!r} gained keys {sorted(gained)}; existing entity rows "
                    f"carry no such column"
                )

        for ctype, attrs in other.attributes.items():
            for attr, was_spec in attrs.items():
                now_spec = self.attributes.get(ctype, {}).get(attr)
                if now_spec is None:
                    problems.append(f"{ctype}.{attr} removed")
                    continue
                if now_spec.dtype != was_spec.dtype:
                    problems.append(
                        f"{ctype}.{attr} dtype {was_spec.dtype} -> {now_spec.dtype}"
                    )
                narrowed = was_spec.dims - now_spec.dims
                if narrowed:
                    problems.append(
                        f"{ctype}.{attr} no longer varies over {sorted(narrowed)}; "
                        f"rows setting those dims have no valid reading"
                    )

        if other.partial is not None and self.partial is not None:
            lost = other.partial - self.partial
            if lost:
                problems.append(
                    f"{sorted(lost)} no longer `partial`; a layer that patched one "
                    f"value along such an axis is now a partial override of an axis "
                    f"owned whole"
                )
        return problems


def _ancestors(dim: str, within: dict[str, frozenset[str]]) -> set[str]:
    """Every dim `dim` is transitively `within`."""
    seen: set[str] = set()
    stack = list(within.get(dim, ()))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(within.get(node, ()))
    return seen
