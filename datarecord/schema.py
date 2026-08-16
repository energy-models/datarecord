"""The schema: what a record's data is, and how a patch to it behaves.

One schema per record, and `manifest.json` is how it is written down - the two
words name the same thing, the file and the object.

Framework-independent. `component_type`, `name` and `attribute` are strings
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

from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

# Columns the format fixes, whatever the schema declares (https://energy-models.github.io/datarecord/design/format/#the-long-schema). Neither
# declared nor optional: `bus`/`breakpoint` are NULL for the ordinary
# component-level scalar, so one column set serves every kind of row.
STRUCTURAL_TYPES = {
    "component_type": "VARCHAR",
    "entity": "VARCHAR",
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
# compatible change versioning calls it. `breakpoints` is outside both, being no dim
# (https://energy-models.github.io/datarecord/design/read-path/#owner-map, https://energy-models.github.io/datarecord/design/record/#flags).
FLAG_COLUMNS = ("varies", "broadcast", "breakpoints")


def flag_type(dims: tuple[str, ...]) -> str:
    """The DuckDB type of one flag struct: a BOOLEAN per declared dim.

    `dims` is never empty: a schema declaring no dims describes no dimensioned
    data, which `Schema` rejects - so the struct always has a field and
    needs no placeholder for DuckDB's want of an empty one.

    Notes
    -----
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    fields = ", ".join(f'"{d}" BOOLEAN' for d in dims)
    return f"STRUCT({fields})"


class Dimension(BaseModel):
    """One axis attribute data may vary over: its shape, not its data.

    Not which dims an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared.

    Attributes
    ----------
    dtype
        The axis labels' type, as a DuckDB type name.
    within
        Dims this one's labels identify a point only *within*; transitive.
    on
        Dims this one *classifies*: each of their labels carries exactly one of
        mine, as a column on their axis file named after me. A dim declaring
        `on` is a mapping - `country` on `bus` means every bus is in one
        country, and `dims/bus.parquet` gains a `country` column.

        The opposite direction from `within`, which names my *parents*: a
        mapping is one flat label set partitioning another axis, where `within`
        scopes my labels per parent so `t1` in two periods is two points.
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

    dtype: str
    within: frozenset[str] = frozenset()
    on: frozenset[str] = frozenset()
    unit: str | None = None
    description: str | None = None

    @property
    def mapping(self) -> bool:
        """Whether this dim classifies another rather than standing alone."""
        return bool(self.on)


class AttributeSpec(BaseModel):
    """What shape one attribute's data may take.

    Attributes
    ----------
    dtype
        The value column's type, as a DuckDB type name.
    dims
        Dims this attribute may vary over; a subset of those declared. Varying
        over nothing is what puts it in `dims/components/<Type>.parquet` rather
        than `inputs/`, so the schema decides the file split.
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

    dtype: str
    default: Any | None = None
    dims: frozenset[str] = frozenset()
    breakpoints: bool = False
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
        """Whether this attribute lives in `inputs/` rather than `dims/components/`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return bool(self.dims)


class Group(BaseModel):
    """Which tuples over several dims exist: a sparse subset of a dim product.

    Not a dim. A dim declares an axis of labels and NULL in its column means
    "every value of it"; a group declares *which combinations are there*, which
    no axis can say because the product is sparse - a component attaches to two
    buses out of a thousand.

    An attribute names the group in its `dims` and its rows carry the group's
    *coordinate* names as columns, never the group's own name. Coordinates
    rather than dims because two of them may draw on the same axis: a corridor
    between two entities is `(from, to)`, which a set of dims could not spell.

    Attributes
    ----------
    over
        Coordinate name -> the dim it draws its labels from.
    description
        What the group is, in prose. Never interpreted.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """

    over: dict[str, str]
    description: str | None = None

    @property
    def coordinates(self) -> tuple[str, ...]:
        """The columns a row of this group is keyed by, in declaration order."""
        return tuple(self.over)


class ComponentType(BaseModel):
    """What one component type carries: the traits it subscribes to, plus its own.

    A type does not *own* an attribute - `Schema.attributes` declares each once,
    record-wide, and a type subscribes. This is the subscription.

    Attributes
    ----------
    traits
        Traits this type subscribes to; every attribute they bundle applies.
    attributes
        Attributes this type carries that no trait bundles.
    description
        What the type is, in prose. Never interpreted.

    Notes
    -----
    - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
    """

    traits: frozenset[str] = frozenset()
    attributes: frozenset[str] = frozenset()
    description: str | None = None


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
        one `inputs/<attr>.parquet`, so a dtype cannot differ per type.
    groups
        Group name -> which tuples over several dims exist. `connection` is
        the one every record with connections declares.
    traits
        Trait -> the attributes it bundles. A vocabulary a consumer dispatches
        on, declared rather than derived.
    component_types
        Component type -> what it carries. `attributes_for` resolves the two.
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
    groups: dict[str, Group] = Field(default_factory=dict)
    traits: dict[str, frozenset[str]] = Field(default_factory=dict)
    component_types: dict[str, ComponentType] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
        - [keys](https://energy-models.github.io/datarecord/design/schema/#keys-which-entity-tables-a-dim-keys)
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
            unknown = sorted(spec.on - declared)
            if unknown:
                msg = f"dim {dim!r} is `on` undeclared dims {unknown}"
                raise ValueError(msg)
            if dim in spec.on:
                msg = f"dim {dim!r} is `on` itself"
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

        # A separate graph from `within`, and pointing the other way: `on`
        # names the dims I classify, so a cycle here is `country on state on
        # country` rather than a label scoped by its own descendant.
        try:
            TopologicalSorter({d: s.on for d, s in self.dimensions.items()}).prepare()
        except CycleError as e:
            msg = f"`on` is cyclic: {' -> '.join(e.args[1])}"
            raise ValueError(msg) from e

        # A group's coordinates draw their labels from declared dims, and its
        # name may not collide with one: an attribute's `dims` names either,
        # so one namespace has to answer.
        for group, group_spec in self.groups.items():
            if group in declared:
                msg = f"group {group!r} collides with a declared dim"
                raise ValueError(msg)
            unknown = sorted(set(group_spec.over.values()) - declared)
            if unknown:
                msg = f"group {group!r} is over undeclared dims {unknown}"
                raise ValueError(msg)

        addressable = declared | set(self.groups)
        for attr, attr_spec in self.attributes.items():
            unknown = sorted(attr_spec.dims - addressable)
            if unknown:
                msg = (
                    f"attribute {attr!r} is addressed by undeclared "
                    f"dims or groups {unknown}"
                )
                raise ValueError(msg)

        # A trait or a type may only name an attribute that is declared: the
        # subscription says which attributes apply, never what they are, so a
        # name with no spec is a typo rather than a shorthand declaration.
        for trait, bundled in self.traits.items():
            unknown = sorted(bundled - set(self.attributes))
            if unknown:
                msg = f"trait {trait!r} bundles undeclared attributes {unknown}"
                raise ValueError(msg)

        for ctype, subscription in self.component_types.items():
            unknown = sorted(subscription.traits - set(self.traits))
            if unknown:
                msg = f"component type {ctype!r} subscribes to undeclared traits {unknown}"
                raise ValueError(msg)
            unknown = sorted(subscription.attributes - set(self.attributes))
            if unknown:
                msg = (
                    f"component type {ctype!r} carries undeclared attributes {unknown}"
                )
                raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared)
            if unknown:
                msg = f"`partial` names undeclared dims {unknown}"
                raise ValueError(msg)
            # A dim that does not broadcast is one whose values are addressed
            # individually, so a layer necessarily patches it value by value -
            # non-broadcast and `partial` are the same fact from two sides.
            # Declared rather than assumed: a schema omitting one would key
            # ownership without it, and resolve one layer's edit as owning
            # every value of it at once.
            missing = sorted(set(self.dims) - set(self.broadcast_dims) - self.partial)
            if missing:
                msg = (
                    f"{missing} do not broadcast, so a layer patches them value "
                    f"by value; declare them `partial` or the fold owns every "
                    f"value of them at once"
                )
                raise ValueError(msg)
        return self

    # -- derived key sets (https://energy-models.github.io/datarecord/design/schema/#keys-which-entity-tables-a-dim-keys, https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override) --------------------------------------

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
        """The dims a NULL broadcasts over: every dim but `entity` and a group's.

        A NULL here means "every value of this dim", which the fold expands
        against the axis. The two exclusions cannot mean that:

        - `entity`, because a NULL there is a value belonging to no component
          rather than to all of them. The one dim named literally, being the
          one the component types partition: `dims/entity.parquet` carries
          `component_type`, which decides an attribute vocabulary, and no
          other dim does anything of the sort.
        - A group's coordinate, because there is no axis to expand against.
          "Every bus of this component" is the group's rows, not the bus axis -
          a sparse subset only the group's table knows.

        The complement of this is what `Schema` requires to be `partial`: a dim
        whose values are addressed individually is one a layer patches value by
        value.

        What the `varies`/`broadcast` structs have a field per, and what
        `expand_dims` joins.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        sparse = {c for g in self.groups.values() for c in g.coordinates}
        return tuple(d for d in self.dims if d != "entity" and d not in sparse)

    def coordinates_of(self, attribute: str) -> tuple[str, ...]:
        """The dim columns one attribute's rows carry, groups expanded.

        An attribute is addressed by what its `dims` declare, and a group there
        expands to its coordinates - so `dims={"connection", "snapshot"}` gives
        `("entity", "bus", "snapshot")` and `dims={"snapshot"}` gives
        `("snapshot",)` with no entity column at all.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL `entity` on a record-level
        weighting would be a column claiming a component the value has none of.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        named: set[str] = set()
        for d in spec.dims:
            group = self.groups.get(d)
            named.update(group.coordinates if group is not None else (d,))
        # Declaration order, so every consumer sees one column order.
        return tuple(d for d in self.dims if d in named)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """One attribute's full `inputs/` column set, in order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        return (
            *self.coordinates_of(attribute),
            "attribute",
            "breakpoint",
            "value",
        )

    def attributes_for(self, ctype: str) -> dict[str, AttributeSpec]:
        """Which attributes `ctype` carries: its traits' bundles, plus its own.

        The resolved view of the two declarations, and what every per-type
        question reads. Empty for a type the schema does not declare, which is
        why callers wanting to reject an unknown type test `component_types`
        rather than this.

        Notes
        -----
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        """
        spec = self.component_types.get(ctype)
        if spec is None:
            return {}
        names = set(spec.attributes)
        for trait in spec.traits:
            names |= self.traits.get(trait, frozenset())
        return {a: self.attributes[a] for a in sorted(names) if a in self.attributes}

    def owned_per(self, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per.

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `Schema.partial` which axes a layer may patch
        value by value, and ownership is their intersection. A dim in `dims`
        but not `partial` is owned whole, so a patch to one of its values
        restates the attribute's entire extent along it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return frozenset()
        return spec.dims & (self.partial or frozenset())

    @property
    def partial_dims(self) -> tuple[str, ...]:
        """The dims a layer may patch value by value, in declaration order.

        `partial` itself, ordered - not the dims of an `inputs/` row, which are
        every declared one. The fold's key is one fixed tuple over all
        attributes, so it must carry every axis *any* layer may patch by
        value, not only those some currently declared attribute varies over.
        An attribute not owned per one of them writes NULL there, which is the
        existing "NULL means all values" rule - and that is also what
        lets a schema declare an axis before any attribute uses it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
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

    def mappings_on(self, dim: str) -> tuple[str, ...]:
        """Mappings classifying `dim` - the extra columns its axis file carries.

        One column per mapping, named for the mapping rather than for `dim`:
        `country` declared `on={"bus"}` puts a `country` column on
        `dims/bus.parquet`. Immediate only, never transitive - a chain is not
        denormalised, so a bus carries `state` and the country is reached
        through `dims/state.parquet`.

        Notes
        -----
        - [mappings](https://energy-models.github.io/datarecord/design/schema/#on-a-mapping-over-another-axis)
        """
        return tuple(d for d, s in self.dimensions.items() if dim in s.on)

    # -- key and column sets (https://energy-models.github.io/datarecord/design/format/#the-long-schema, https://energy-models.github.io/datarecord/design/read-path/#owner-map) -----------------------------------

    @property
    def long_columns(self) -> tuple[str, ...]:
        """The long schema's full column set.

        The *map's* column set, which is uniform across attributes because the
        map is one relation over all of them. An individual file carries only
        its own attribute's columns (`long_columns_for`), and `union_by_name`
        supplies NULL for the rest when the fold unions them here.

        No `component_type`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        return (
            *self.dims,
            "attribute",
            "breakpoint",
            "value",
        )

    @property
    def input_key(self) -> tuple[str, ...]:
        """Inputs-map key columns, compared NULL-safely when folding.

        `partial` itself, plus `attribute`. `entity` and a group's coordinates
        are in it because a layer may patch one component's value, or one
        connection's, without restating every other's - which is what `partial`
        says, and why they are declared `partial` rather than added here.

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

    def groups_of(self, attribute: str) -> tuple[str, ...]:
        """Which declared groups address `attribute`, in declaration order.

        An attribute is a connection attribute because its `dims` name the
        `connection` group - not because a separate field says so. That is
        what lets a second group exist without a second field.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(g for g in self.groups if g in spec.dims)

    def group_coordinates(self, group: str) -> tuple[str, ...]:
        """One group's coordinate columns, or `()` if it is not declared.

        What a row of the group is keyed by, before the dims its membership is
        scoped along. Coordinate names rather than dim names, so two drawing on
        one axis stay two columns.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        spec = self.groups.get(group)
        return () if spec is None else spec.coordinates

    @property
    def input_columns(self) -> tuple[str, ...]:
        """The inputs map's full column set.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return (*self.input_key, "layer_uuid", *FLAG_COLUMNS)

    @property
    def component_columns(self) -> tuple[str, ...]:
        """The components map's full column set; the type is carried, not keyed.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return ("component_type", "entity", "layer_uuid", "order_key")

    def group_columns(self, group: str) -> tuple[str, ...]:
        """One group's owner-map column set: its coordinates, plus what it carries.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        return (
            "component_type",
            *self.group_coordinates(group),
            "layer_uuid",
            "order_key",
        )

    # -- typing (https://energy-models.github.io/datarecord/design/format/#the-long-schema, https://energy-models.github.io/datarecord/design/writing/) -------------------------------------------------

    def column_type(self, column: str) -> str | None:
        """The declared type for one column, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims, and
        the owner map's two flag structs, whose fields follow the schema's dims.

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
        return None

    def value_type(self, attribute: str) -> str | None:
        """The `value` column's type for one attribute.

        No `ctype`: one attribute is one `inputs/<attr>.parquet` with one
        `value` column, so the dtype is the attribute's alone.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        return None if spec is None else spec.dtype

    def types_declaring(self, attribute: str) -> frozenset[str]:
        """Which component types carry `attribute` - what `names=None` targets.

        Empty for a record-level attribute, which no type carries and which
        therefore targets no names at all.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        return frozenset(
            c for c in self.component_types if attribute in self.attributes_for(c)
        )

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

        # A type losing an attribute is incompatible for the same reason a
        # narrowed `dims` is: its rows are still in the file, now unreadable
        # for that type. Losing a whole type says the same of all of them.
        for ctype in other.component_types:
            was_attrs = set(other.attributes_for(ctype))
            dropped = sorted(was_attrs - set(self.attributes_for(ctype)))
            if dropped:
                problems.append(
                    f"component type {ctype!r} no longer carries {dropped}; "
                    f"rows written for it have no valid reading"
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
