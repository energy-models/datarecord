# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The schema: what a record's data is, and how a patch to it behaves.

One schema per record, and `manifest.json` is how it is written down - the two
words name the same thing, the file and the object.

Framework-independent. `entity_type`, `name` and `attribute` are strings
because those vocabularies belong to a modelling framework and this package
knows none: a type no tool recognises reads back fine and is reported by the
tool that cannot build it, not rejected here.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
"""

from __future__ import annotations

import math
from graphlib import CycleError, TopologicalSorter
from typing import Any

import narwhals as nw
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

# Narwhals dtypes `manifest.json` encodes by bare class name, covering what a
# dim or attribute actually declares. `Enum` and `Datetime` carry parameters
# and are handled separately in `_dump_dtype`/`_parse_dtype` - extend here as
# a new bare dtype shows up rather than widening the parametrized branches.
_BARE_DTYPES: dict[str, type[nw.dtypes.DType]] = {
    "String": nw.String,
    "Float64": nw.Float64,
    "Int64": nw.Int64,
    "Boolean": nw.Boolean,
    "Date": nw.Date,
}


def _dump_dtype(dtype: nw.dtypes.DType) -> Any:
    """Encode a narwhals dtype for `manifest.json`.

    A bare class name for what `_BARE_DTYPES` covers; a one-key dict of class
    name to constructor args for `Enum`/`Datetime`, the parametrized dtypes a
    schema actually declares.
    """
    if isinstance(dtype, nw.Enum):
        return {"Enum": list(dtype.categories)}
    if isinstance(dtype, nw.Datetime):
        return {"Datetime": dtype.time_unit}
    base = dtype.base_type()
    if base.__name__ in _BARE_DTYPES:
        return base.__name__
    msg = f"no manifest.json encoding for narwhals dtype {base.__name__}"
    raise ValueError(msg)


def _parse_dtype(value: Any) -> nw.dtypes.DType:
    """The `dtype=` field_validator shared by `Dimension` and `AttributeSpec`.

    An instance (`nw.String()`) passes through; anything else is what
    `_dump_dtype` wrote to `manifest.json`, decoded back to an instance -
    `dtype=` takes an instance only, not a bare class.
    """
    if isinstance(value, nw.dtypes.DType):
        return value
    if isinstance(value, str):
        if value not in _BARE_DTYPES:
            msg = f"unknown narwhals dtype {value!r} in manifest.json"
            raise ValueError(msg)
        return _BARE_DTYPES[value]()
    if isinstance(value, dict) and len(value) == 1:
        ((name, arg),) = value.items()
        if name == "Enum":
            return nw.Enum(arg)
        if name == "Datetime":
            return nw.Datetime(arg)
    msg = f"unrecognised dtype encoding in manifest.json: {value!r}"
    raise ValueError(msg)


# Columns the format fixes, whatever the schema declares (https://energy-models.github.io/datarecord/design/format/#the-long-schema). Not the
# dims: `entity`, a relation's `bus` and `entity_type` are declared like any
# other axis and typed from that declaration, which is what lets an `Enum` there
# pin its vocabulary. These are the ones no schema names - `breakpoint` is NULL
# for the ordinary component-level scalar, so one column set serves every row.
STRUCTURAL_TYPES = {
    "attribute": nw.String(),
    "breakpoint": nw.Float64(),
    "deleted": nw.Boolean(),
    "breakpoints": nw.Boolean(),
}


# Every long row's trailing columns, whatever coordinates precede them: the
# attribute named, the abscissa of a piecewise-linear value, and the value
# (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
LONG_TAIL = ("attribute", "breakpoint", "value")


# The owner map's flag columns: two structs with a field per declared dim, so
# the map's column set does not depend on the schema and adding a dim stays the
# compatible change versioning calls it. `breakpoints` is outside both, being no dim
# (https://energy-models.github.io/datarecord/design/read-path/#owner-map, https://energy-models.github.io/datarecord/design/record/#flags).
FLAG_COLUMNS = ("varies", "broadcast", "breakpoints")


def flag_type(dims: tuple[str, ...]) -> nw.dtypes.DType:
    """One flag struct's type: a BOOLEAN field per declared dim.

    `dims` is never empty: a schema declaring no dims describes no dimensioned
    data, which `Schema` rejects - so the struct always has a field and
    needs no placeholder for DuckDB's want of an empty one.

    Notes
    -----
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    return nw.Struct({d: nw.Boolean() for d in dims})


class Dimension(BaseModel):
    """One axis attribute data may vary over: its shape, not its data.

    Not which dims an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared.

    Attributes
    ----------
    dtype
        The axis labels' type, as a narwhals dtype instance (`nw.String()`,
        `nw.Datetime()`, ...) - translated to its DuckDB name only where a
        column of it is built.
    within
        Dims this one's labels identify a point only *within*; transitive.
    unit
        What this axis's *labels* measure, if anything - `None` is undeclared,
        `""` genuinely dimensionless.
    description
        What the axis is, in prose. Never interpreted.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dtype: nw.dtypes.DType
    within: frozenset[str] = frozenset()
    unit: str | None = None
    description: str | None = None

    @field_validator("dtype", mode="before")
    @classmethod
    def _parse_dtype(cls, value: Any) -> Any:
        return _parse_dtype(value)

    @field_serializer("dtype")
    def _dump_dtype(self, value: nw.dtypes.DType) -> Any:
        return _dump_dtype(value)


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take.

    Attributes
    ----------
    dtype
        The value column's type, as a narwhals dtype instance (`nw.String()`,
        `nw.Datetime()`, ...) - translated to its DuckDB name only where a
        column of it is built.
    dims
        Dims this attribute may vary over; a subset of those declared. One dim
        alone puts it on that dim's axis file rather than in `attributes/`, so the
        schema decides the file split.
    default
        The value a coordinate no row covers takes.
    breakpoints
        Whether it may carry a piecewise-linear curve.
    unit
        What the values measure - `"MW"`, `"EUR/MWh"`. Stored and never
        interpreted; `None` is undeclared, `""` genuinely dimensionless.
    description
        What the attribute is, in prose. Never interpreted.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dtype: nw.dtypes.DType
    default: Any | None = None
    dims: frozenset[str] = frozenset()
    breakpoints: bool = False
    unit: str | None = None
    description: str | None = None

    @field_validator("dtype", mode="before")
    @classmethod
    def _parse_dtype(cls, value: Any) -> Any:
        return _parse_dtype(value)

    @field_serializer("dtype")
    def _dump_dtype(self, value: nw.dtypes.DType) -> Any:
        return _dump_dtype(value)

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
        """Whether this attribute's values are long rows rather than a column.

        "Varies beyond its address", not "has dims": naming exactly one
        addressing coordinate is a column on that thing's own table, so
        `dims={"entity"}` is a component column and `dims={"connection"}` a
        column of the relation's table. Anything more is `attributes/<attr>.parquet`.

        A bare `bool(dims)` was the test before `entity` was a declared dim,
        when a component attribute declared none - it would now call every
        attribute varying and route every constant to `attributes/`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return len(self.dims) > 1


class Relation(BaseModel):
    """Which tuples over several dims exist: rows unique per key, a sparse subset of a dim product.

    Not a dim. A dim declares an axis of labels and NULL in its column means
    "every value of it"; a relation declares *which combinations are there*,
    which no axis can say because the product is sparse - a component attaches
    to two buses out of a thousand.

    An attribute names the relation in its `dims` and its rows carry the
    relation's key column names, never the relation's own name. Columns rather
    than dims because two of them may draw on the same axis: a corridor between
    two entities is `(from, to)`, which a set of dims could not spell.

    Attributes
    ----------
    key
        Key column name -> the dim it draws its labels from. A list is sugar for
        the dict with identical keys and values; the dict form is what lets two
        columns draw on one dim, as `corridor`'s `{from: bus, to: bus}` does.
    values
        The dim each row carries exactly one label of, a column named after it,
        or `None` for a bare tuple set. Must name a declared dim.
    description
        What the relation is, in prose. Never interpreted.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
    """

    key: dict[str, str]
    values: str | None = None
    description: str | None = None

    @field_validator("key", mode="before")
    @classmethod
    def _parse_key(cls, value: Any) -> Any:
        """Expand the list form to the dict it is sugar for."""
        if isinstance(value, (list, tuple)):
            return {c: c for c in value}
        return value

    @property
    def columns(self) -> tuple[str, ...]:
        """This relation's columns: the key in declaration order, then `values` where declared.

        Notes
        -----
        - [values](https://energy-models.github.io/datarecord/design/schema/#values-a-relation-that-classifies)
        """
        if self.values is None:
            return tuple(self.key)
        return (*self.key, self.values)


class Schema(BaseModel):
    """One record's schema.

    Attributes
    ----------
    version
        Bumped by any change to the declarations. A reader meeting a
        version it was not written for should refuse rather than guess.
    dimensions
        Every declared axis, keyed by dim name.
    attributes
        Attribute -> spec, flat and record-wide. One attribute is one spec and
        one `attributes/<attr>.parquet`, so a dtype cannot differ per type.
    relations
        Relation name -> which tuples over several dims exist. `connection` is
        the one every record with connections declares, and the entity-type
        axis is the `values` of the relation keyed by `[entity]`.
    partial
        Which dims a layer may patch value by value. `None` for a record
        with no layers, since nothing overrides anything. A dim outside it is
        one a layer owns entirely once it touches it.
    meta
        A framework's own top-level data - network attributes, CRS, free-form
        metadata. Stored and never interpreted, since none of it describes the
        dimensioned data.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """

    version: int = 1
    dimensions: dict[str, Dimension] = Field(default_factory=dict)
    attributes: dict[str, AttributeSpec] = Field(default_factory=dict)
    relations: dict[str, Relation] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
        """
        declared = set(self.dimensions)

        # Attributes but no axes is a table, not a record (https://energy-models.github.io/datarecord/design/schema/#dimensions). Rejected here
        # so the owner map never needs a struct with no fields, which DuckDB has
        # no type for. A wholly empty `Schema()` stays legal: "no manifest yet".
        if self.attributes and not declared:
            msg = (
                "a schema declaring attributes must declare at least one dim; "
                "attribute data varying over no axis is not a record (https://energy-models.github.io/datarecord/design/schema/#dimensions)"
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
        # `CycleError.args[1]` is the offending path - so the acyclicity `within`
        # requires is stdlib rather than a graph walk kept here.
        try:
            TopologicalSorter(
                {d: s.within for d, s in self.dimensions.items()}
            ).prepare()
        except CycleError as e:
            msg = f"`within` is cyclic: {' -> '.join(e.args[1])}"
            raise ValueError(msg) from e

        # A relation's key columns draw their labels from declared dims, and so
        # does `values`. No check that the name is free of the dims: a collision
        # is shadowing rather than an ambiguity (https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x).
        for relation, relation_spec in self.relations.items():
            unknown = sorted(set(relation_spec.key.values()) - declared)
            if unknown:
                msg = f"relation {relation!r} is keyed by undeclared dims {unknown}"
                raise ValueError(msg)
            if (
                relation_spec.values is not None
                and relation_spec.values not in declared
            ):
                msg = (
                    f"relation {relation!r} has `values` in undeclared dim "
                    f"{relation_spec.values!r}; `values` names the axis whose labels "
                    f"the relation's rows carry (https://energy-models.github.io/datarecord/design/schema/#values-a-relation-that-classifies)"
                )
                raise ValueError(msg)
            if (
                relation_spec.values is not None
                and relation_spec.values in relation_spec.key
            ):
                msg = (
                    f"relation {relation!r} has `values` {relation_spec.values!r}, which "
                    f"is also one of its `key` columns; a relation cannot map a "
                    f"column to itself"
                )
                raise ValueError(msg)

        addressable = declared | set(self.relations)
        for attr, attr_spec in self.attributes.items():
            unknown = sorted(attr_spec.dims - addressable)
            if unknown:
                msg = (
                    f"attribute {attr!r} is addressed by undeclared "
                    f"dims or relations {unknown}"
                )
                raise ValueError(msg)
            for relation, relation_spec in self.relations.items():
                if (
                    relation_spec.values is None
                    or relation_spec.values not in attr_spec.dims
                ):
                    continue
                both = sorted(set(relation_spec.key) & attr_spec.dims)
                if both:
                    msg = (
                        f"attribute {attr!r} is addressed by "
                        f"{relation_spec.values!r} and {both}, which the relation "
                        f"{relation!r} maps it from; `values` says the first follows "
                        f"from the second, so naming both keys a row twice over"
                    )
                    raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared)
            if unknown:
                msg = f"`partial` names undeclared dims {unknown}"
                raise ValueError(msg)
        keys = {dim for r in self.relations.values() for dim in r.key.values()}
        missing = sorted(keys - (self.partial or frozenset()))
        if missing:
            msg = (
                f"`partial` must name {missing}: a relation is keyed by them, and "
                f"a layer adds or removes one of its rows at a time"
            )
            raise ValueError(msg)

        return self

    # -- declarations in mathspec's vocabulary --------------------------------

    @classmethod
    def from_mathspec(
        cls, spec: Any, *, storage: dict[str, Any] | None = None
    ) -> Schema:
        """A schema from a mathspec spec's data declarations, plus what only storage needs.

        Needs the `mathspec` extra: `pip install 'datarecord[mathspec]'`.

        Parameters
        ----------
        spec
            What `mathspec.to_spec` takes - a `Spec`, a path, YAML text or a
            dict. Its `dimensions`, `relations` and `parameters` become dims,
            relations and attributes; its math, if it has any, is not read.
        storage
            `partial` and `meta` as `Schema` takes them, and under
            `dimensions` and `parameters` the fields mathspec has no place for:
            `unit` and `within` on a dim, `default`, `unit` and `breakpoints` on a
            parameter.

        Raises
        ------
        ImportError
            If mathspec is not installed.
        ValueError
            If a relation or dtype has no datarecord form: a relation
            determining more than one column, or under a role named other than
            its dim.
        """
        try:
            import mathspec
        except ImportError as e:
            msg = "Schema.from_mathspec needs mathspec: pip install 'datarecord[mathspec]'"
            raise ImportError(msg) from e

        spec = mathspec.to_spec(spec)
        storage = storage or {}
        dim_extra = storage.get("dimensions", {})
        param_extra = storage.get("parameters", {})
        dimensions = {
            d: Dimension(
                dtype=_FROM_MATHSPEC[b.dtype](),
                description=b.description,
                **dim_extra.get(d, {}),
            )
            for d, b in spec.dimensions.items()
        }
        relations = {
            r: _relation_from_mathspec(r, b) for r, b in spec.relations.items()
        }
        attributes = {
            a: AttributeSpec(
                dtype=_FROM_MATHSPEC[b.dtype](),
                dims=frozenset(b.dims),
                description=b.description,
                **param_extra.get(a, {}),
            )
            for a, b in spec.parameters.items()
        }
        rest = {
            k: v for k, v in storage.items() if k not in ("dimensions", "parameters")
        }
        return cls(
            dimensions=dimensions, relations=relations, attributes=attributes, **rest
        )

    def to_mathspec(self) -> dict[str, Any]:
        """This schema's dims, relations and attributes as a mathspec declarations file.

        What `from_mathspec` reads back, less the storage block. A model spec
        merges with it (`mathspec.merge`) and reads its parameters as `given:`.

        Raises
        ------
        ValueError
            If a dtype has no mathspec form, or an attribute is addressed by a
            relation, which a mathspec parameter cannot be.
        """
        relations = {}
        for r, relation in self.relations.items():
            key: Any = (
                dict(relation.key)
                if any(k != v for k, v in relation.key.items())
                else list(relation.key)
            )
            if isinstance(key, list) and len(key) == 1:
                key = key[0]
            relations[r] = {"key": key} | (
                {"values": relation.values} if relation.values else {}
            )
        parameters = {}
        for a, spec in self.attributes.items():
            if spec.dims & set(self.relations):
                msg = f"attribute {a!r} is addressed by a relation; a mathspec parameter is over dims only"
                raise ValueError(msg)
            parameters[a] = {
                "dims": [d for d in self.dimensions if d in spec.dims],
                "dtype": _to_mathspec(spec.dtype, f"attribute {a!r}"),
            } | ({"description": spec.description} if spec.description else {})
        return {
            "dimensions": {
                d: {"dtype": _to_mathspec(s.dtype, f"dim {d!r}")}
                | ({"description": s.description} if s.description else {})
                for d, s in self.dimensions.items()
            },
            "relations": relations,
            "parameters": parameters,
        }

    # -- derived key sets (https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) --------------------------------------

    @property
    def dims(self) -> tuple[str, ...]:
        """Every declared dim, in declaration order - the long schema's dim columns.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return tuple(self.dimensions)

    @property
    def broadcast_dims(self) -> tuple[str, ...]:
        """The dims a NULL may broadcast over: every declared dim.

        A NULL here means "every value of this dim", which the fold expands
        against the axis - but only for an attribute that names the dim in its
        own `dims` (`broadcasts_over`). A coordinate an attribute reaches
        through a relation never broadcasts, because "every bus of this component"
        is the relation's rows, not the bus axis.

        What the `varies`/`broadcast` structs have a field per.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        """
        return self.dims

    def broadcasts_over(self, attribute: str) -> tuple[str, ...]:
        """The dims a NULL in `attribute`'s rows means "every value" of.

        The dims its spec names directly, in declaration order. A coordinate it
        reaches through a relation is not among them: the domain there is the
        relation's rows, which a NULL cannot name. An undeclared attribute
        broadcasts over nothing.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(d for d in self.dims if d in spec.dims)

    def coordinates_of(self, attribute: str) -> tuple[str, ...]:
        """The dim columns one attribute's rows carry, relations expanded.

        One rule resolves a name in `dims`: **it is the dim of that name if one
        is declared, and otherwise the relation of that name expanded to its
        coordinates**. So `dims={"connection", "snapshot"}` gives `("entity",
        "bus", "snapshot")` where no dim `connection` exists, and
        `dims={"country"}` gives `("country",)` - the dim, where a relation of that
        name is shadowed.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL `entity` on a record-level
        weighting would be a column claiming a component the value has none of.

        Notes
        -----
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        named: set[str] = set()
        for d in spec.dims:
            relation = None if d in self.dimensions else self.relations.get(d)
            named.update(relation.columns if relation is not None else (d,))
        # Declaration order, so every consumer sees one column order.
        return tuple(d for d in self.dims if d in named)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """One attribute's full long column set, in order.

        An attribute carries the coordinates its `dims` name and no others, so a
        record-level weighting has no `entity` column and a component attribute
        has no `bus`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return (*self.coordinates_of(attribute), *LONG_TAIL)

    def owned_per(self, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per.

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `partial_dims` the fold key, and ownership is
        their intersection. A
        dim in `dims` but not the fold key - a non-`partial` value axis like
        `timestep` - is owned whole, so a patch to one of its values restates the
        attribute's entire extent along it (`_owned_whole`).

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return frozenset()
        return spec.dims & set(self.partial_dims)

    @property
    def partial_dims(self) -> tuple[str, ...]:
        """The fold key's dims, in declaration order.

        The dims a layer patches one value or one row at a time (`partial`),
        which include every relation key. The fold's key is one fixed tuple over all
        attributes, so it carries every axis *any* layer may patch by value or
        by row, not only those some currently declared attribute varies over. An
        attribute not owned per one of them writes NULL there, the "NULL means
        all values" rule - which also lets a schema declare an axis before any
        attribute uses it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        partial = self.partial or frozenset()
        return tuple(d for d in self.dims if d in partial)

    def axis_key(self, dim: str) -> tuple[str, ...]:
        """A dim's axis-table key: `(*parents, dim)`, parents first.

        Parents in declaration order, and transitively - a dim `within` another
        that is itself `within` a third is keyed by all three.

        Notes
        -----
        - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
        """
        seen = _ancestors(dim, {d: s.within for d, s in self.dimensions.items()})
        return (*(d for d in self.dims if d in seen), dim)

    def attributes_on(self, dim: str) -> tuple[str, ...]:
        """Attributes stored as columns of `dims/{dim}.parquet`.

        An attribute addressed by `dim` alone: a per-country CO2 budget, a
        snapshot weighting, a per-type icon. `AttributeSpec.varying` is False
        for exactly these, and this is the axis-side counterpart of
        `addresses_entity`: a component's constant columns live on
        `dims/entity.parquet` like any other axis's.

        Keyed off `dims` rather than `coordinates_of`, because a relation with one
        coordinate is indistinguishable there: `dims={"connection"}` over a
        single `bus` coordinate also yields `("bus",)`, and it belongs in the
        relation's file rather than on the bus axis. A relation over `entity` alone is
        keyed by the relation name, not `entity`, so its `values` label and any
        attribute it bundles never match here.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        if dim not in self.dimensions:
            return ()
        return tuple(
            a for a, spec in self.attributes.items() if spec.dims == frozenset({dim})
        )

    # -- key and column sets (https://energy-models.github.io/datarecord/design/format/#the-long-schema, https://energy-models.github.io/datarecord/design/read-path/#owner-map) -----------------------------------

    @property
    def long_columns(self) -> tuple[str, ...]:
        """The long schema's full column set.

        The *map's* column set, which is uniform across attributes because the
        map is one relation over all of them. An individual file carries only
        its own attribute's columns (`long_columns_for`), and `union_by_name`
        supplies NULL for the rest when the fold unions them here.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return (*self.dims, *LONG_TAIL)

    @property
    def input_key(self) -> tuple[str, ...]:
        """Inputs-map key columns, compared NULL-safely when folding.

        `partial_dims`, plus `attribute`. `entity` and a relation's coordinates are
        in it as membership keys - a layer may patch one component's value, or
        one connection's, without restating every other's - and the broadcast
        `partial` value dims beside them.

        A coordinate an attribute's own file does not carry reads as NULL,
        which is what makes the key one fixed tuple over attributes whose
        columns differ.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return (*self.partial_dims, "attribute")

    def relations_of(self, attribute: str) -> tuple[str, ...]:
        """Which declared relations address `attribute`, in declaration order.

        An attribute is a connection attribute because its `dims` name the
        `connection` relation - not because a separate field says so. That is
        what lets a second relation exist without a second field.

        A relation a dim shadows is not one of them, `dims: [country]` naming the
        axis.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(
            r for r in self.relations if r in spec.dims and r not in self.dimensions
        )

    def relation_columns(self, relation: str) -> tuple[str, ...]:
        """One relation's columns, or `()` if it is not declared.

        Every column of the relation's file, `values` included. Column names
        rather than dim names, so two drawing on one axis stay two columns.

        Notes
        -----
        - [relations](https://energy-models.github.io/datarecord/design/schema/#relations)
        """
        spec = self.relations.get(relation)
        return () if spec is None else spec.columns

    def relation_key(self, relation: str) -> tuple[str, ...]:
        """One relation's key columns, or `()` if it is not declared.

        `relation_columns` minus `values` - what the fold keys ownership by and
        what a tombstone names.

        Notes
        -----
        - [values](https://energy-models.github.io/datarecord/design/schema/#values-a-relation-that-classifies)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        spec = self.relations.get(relation)
        return () if spec is None else tuple(spec.key)

    @property
    def input_columns(self) -> tuple[str, ...]:
        """The inputs map's full column set.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return (*self.input_key, "layer_uuid", *FLAG_COLUMNS)

    # -- typing (https://energy-models.github.io/datarecord/design/format/#the-long-schema, https://energy-models.github.io/datarecord/design/writing/) -------------------------------------------------

    def column_type(self, column: str) -> nw.dtypes.DType | None:
        """The declared type for one column, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims, the
        attributes an axis file carries as columns (`attributes_on`), and the
        owner map's two flag structs, whose fields follow the schema's dims. A
        narwhals dtype, translated to DuckDB (`duck.DuckTypes`) only where a
        caller builds a column of it.

        No dim is structural - `entity` and a relation's `bus` included: each is
        declared, and typed from that declaration. So an `Enum` on the entity-type
        axis pins its vocabulary everywhere the column is built, and an axis a
        schema happens to call `kind` is typed no differently.

        An attribute addressed by one axis alone is a *column* rather than a
        `value` cell, so this is where its type is read from - `cast_declared`
        would otherwise leave an axis file's attribute column as whatever the
        incoming frame happened to carry. An attribute with any other `dims` is
        `value_type`'s, not this: it is a long row's value.

        A schema declaring no dims at all is "no manifest yet" rather
        than a record to fold, and DuckDB has no empty struct - so the flag
        columns are undeclared there, and a caller building an empty relation
        falls back to `VARCHAR` for a map that will never hold a row.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        if column in STRUCTURAL_TYPES:
            return STRUCTURAL_TYPES[column]
        if column in self.dimensions:
            return self.dimensions[column].dtype
        if column in ("varies", "broadcast"):
            return flag_type(self.broadcast_dims) if self.broadcast_dims else None
        spec = self.attributes.get(column)
        if spec is not None and not spec.varying:
            (dim,) = spec.dims
            if column in self.attributes_on(dim):
                return spec.dtype
        return None

    def value_type(self, attribute: str) -> nw.dtypes.DType | None:
        """The `value` column's type for one attribute.

        No `ctype`: one attribute is one `attributes/<attr>.parquet` with one
        `value` column, so the dtype is the attribute's alone. A narwhals
        dtype, translated to DuckDB (`duck.DuckTypes`) only where a caller builds
        a column of it.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        return None if spec is None else spec.dtype

    # -- versioning (https://energy-models.github.io/datarecord/design/schema/#versioning) --------------------------------------------------

    def compatible_with(self, other: Schema) -> list[str]:
        """Why layers written under `other` would not read under `self`.

        Empty when the change is compatible: old layers stay readable and only
        `version` moves. The compatible changes are those where NULL already
        means what the new schema needs it to mean, so the broadcast rule absorbs
        them without touching a row.

        Returns
        -------
        list of str
            One reason per incompatibility, empty if there are none.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
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

        for attr, was_spec in other.attributes.items():
            now_spec = self.attributes.get(attr)
            if now_spec is None:
                problems.append(f"attribute {attr!r} removed")
                continue
            if now_spec.dtype != was_spec.dtype:
                problems.append(
                    f"attribute {attr!r} dtype {was_spec.dtype} -> {now_spec.dtype}"
                )
            narrowed = was_spec.dims - now_spec.dims
            if narrowed:
                problems.append(
                    f"attribute {attr!r} no longer varies over {sorted(narrowed)}; "
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


_FROM_MATHSPEC: dict[str, type[nw.dtypes.DType]] = {
    "str": nw.String,
    "int": nw.Int64,
    "float": nw.Float64,
    "bool": nw.Boolean,
    "datetime": nw.Datetime,
}


def _to_mathspec(dtype: nw.dtypes.DType, where: str) -> str:
    """`dtype` as the mathspec dtype name that reads back to it, or a refusal naming `where`."""
    for name, kind in _FROM_MATHSPEC.items():
        if type(dtype) is kind:
            return name
    msg = f"{where}: dtype {dtype} has no mathspec form"
    raise ValueError(msg)


def _relation_from_mathspec(name: str, block: Any) -> Relation:
    """A mathspec relation as a `Relation`, which determines at most one column.

    A `Relation`'s `values` column is named after its dim; a mathspec relation
    may determine several, under any role, and those have no `Relation` form.
    """
    key = dict(block.pairs[: len(block.key_roles)])
    values = block.pairs[len(block.key_roles) :]
    if len(values) > 1 or any(role != dim for role, dim in values):
        msg = f"relation {name!r} determines {values}; a datarecord relation has one `values` dim, named after it"
        raise ValueError(msg)
    return Relation(key=key, values=values[0][1] if values else None)
