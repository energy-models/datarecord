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
# dims: `entity`, a group's `bus` and the entity-type axis are declared like any
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
        Dims this attribute may vary over; a subset of those declared. Varying
        over nothing is what puts it in `dims/entity_type/<Type>.parquet` rather
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
        column of the group's table. Anything more is `inputs/<attr>.parquet`.

        A bare `bool(dims)` was the test before `entity` was a declared dim,
        when a component attribute declared none - it would now call every
        attribute varying and route every constant to `inputs/`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return len(self.dims) > 1


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
        Coordinate name -> the dim it draws its labels from. A list is sugar for
        the dict with identical keys and values; the dict form is what lets two
        coordinates draw on one dim, as `corridor`'s `{from: bus, to: bus}` does.
    into
        The dim each tuple of `over` carries exactly one label of, or `None` for
        a bare tuple set. Must name a declared dim.
    description
        What the group is, in prose. Never interpreted.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """

    over: dict[str, str]
    into: str | None = None
    description: str | None = None

    @field_validator("over", mode="before")
    @classmethod
    def _parse_over(cls, value: Any) -> Any:
        """Expand the list form to the dict it is sugar for."""
        if isinstance(value, (list, tuple)):
            return {c: c for c in value}
        return value

    @property
    def coordinates(self) -> tuple[str, ...]:
        """This group's columns in declaration order, `into` last where declared.

        Notes
        -----
        - [into](https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)
        """
        if self.into is None:
            return tuple(self.over)
        return (*self.over, self.into)

    @property
    def key(self) -> tuple[str, ...]:
        """The coordinates a row is unique over: `coordinates` minus `into`.

        Every coordinate for a group without one.

        Notes
        -----
        - [into](https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)
        """
        return tuple(self.over)


class Trait(BaseModel):
    """A named bundle of attributes, and which entity types and components carry it.

    The narrowing direction: an attribute the schema declares is carried by
    every entity type it can address, and a trait is how that is cut down to
    some of them, and further to some of those. A trait bundling `p_max_pu`
    `on={"entity_type": {"Generator"}}` says those attributes reach generators
    and nothing else.

    Attributes
    ----------
    attributes
        The attributes this trait bundles. Each must be declared. Includes
        `switch` once parsed, whatever the author wrote.
    on
        Mapping dim -> the labels of it this trait applies to. Only the
        entity-type axis may key this, since that is the axis an attribute
        vocabulary partitions - `Schema` rejects any other. Empty means this
        narrowing does not apply, reaching every type.
    switch
        The attribute deciding, per component, whether this trait applies -
        `dims={"entity"}` exactly. `None` means this narrowing does not
        apply, reaching every component.
    description
        What the trait is, in prose. Never interpreted.

    Notes
    -----
    - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
    - [switch](https://energy-models.github.io/datarecord/design/schema/#switch-a-trait-a-component-opts-into)
    """

    attributes: frozenset[str] = frozenset()
    on: dict[str, frozenset[str]] = Field(default_factory=dict)
    switch: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _fold_switch_into_attributes(self) -> Trait:
        """Add `switch` to `attributes`, so a caller need not name it twice."""
        if self.switch is not None and self.switch not in self.attributes:
            self.attributes = self.attributes | {self.switch}
        return self


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
        the one every record with connections declares, and the entity-type
        axis is the group `into` that axis over `[entity]`.
    traits
        Trait -> the attributes it bundles and the entity types carrying them.
        A vocabulary a consumer dispatches on, declared rather than derived,
        and the only thing that narrows an attribute to some entity types.
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
    results: dict[str, AttributeSpec] = Field(default_factory=dict)
    """What a solve computes, keyed like `attributes` and shaped the same.

    Separate because the two are governed differently, not because they are
    stored differently: a result is written to `outputs/<attr>.parquet` rather
    than `inputs/`, never overlays a parent's, and may name a component the
    record does not declare. Keeping it out of `attributes` is what stops it
    reaching `attributes_for`, and so `add`'s wide-frame split and the input
    validation, neither of which a result should meet.
    """

    groups: dict[str, Group] = Field(default_factory=dict)
    traits: dict[str, Trait] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
        """
        declared = set(self.dimensions)

        # Attributes but no axes is a table, not a record (https://energy-models.github.io/datarecord/design/schema/#dimensions). Rejected here
        # so the owner map never needs a struct with no fields, which DuckDB has
        # no type for. A wholly empty `Schema()` stays legal: "no manifest yet".
        if (self.attributes or self.results) and not declared:
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

        # A group's coordinates draw their labels from declared dims, and so
        # does `into`. No check that the name is free of the dims: a collision
        # is shadowing rather than an ambiguity (https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x).
        for group, group_spec in self.groups.items():
            unknown = sorted(set(group_spec.over.values()) - declared)
            if unknown:
                msg = f"group {group!r} is over undeclared dims {unknown}"
                raise ValueError(msg)
            if group_spec.into is not None and group_spec.into not in declared:
                msg = (
                    f"group {group!r} is `into` undeclared dim "
                    f"{group_spec.into!r}; `into` names the axis whose labels "
                    f"the group's tuples carry (https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)"
                )
                raise ValueError(msg)
            if group_spec.into is not None and group_spec.into in group_spec.over:
                msg = (
                    f"group {group!r} is `into` {group_spec.into!r}, which is "
                    f"also one of its `over` coordinates; a group cannot map a "
                    f"coordinate to itself"
                )
                raise ValueError(msg)

        # One group may classify `entity`, or none: every caller of
        # `attributes_for` asks for exactly one vocabulary.
        classifying = sorted(
            g.into
            for g in self.groups.values()
            if g.into is not None and tuple(g.over.values()) == ("entity",)
        )
        if len(classifying) > 1:
            msg = (
                f"{classifying} all classify `entity`; a component has one type, "
                f"so at most one group may be `into` a dim over `entity` alone"
            )
            raise ValueError(msg)
        entity_type = classifying[0] if classifying else None

        # One name means one file with one `value` column, so a name declared as
        # both would have to be an input and a result at once - two files, two
        # governing rules, one key.
        clashing = sorted(set(self.attributes) & set(self.results))
        if clashing:
            msg = (
                f"{clashing} are declared as both an attribute and a result; "
                f"one name is one file, so it is one or the other"
            )
            raise ValueError(msg)

        addressable = declared | set(self.groups)
        for attr, attr_spec in (*self.attributes.items(), *self.results.items()):
            unknown = sorted(attr_spec.dims - addressable)
            if unknown:
                msg = (
                    f"attribute {attr!r} is addressed by undeclared "
                    f"dims or groups {unknown}"
                )
                raise ValueError(msg)
            for group, group_spec in self.groups.items():
                if group_spec.into is None or group_spec.into not in attr_spec.dims:
                    continue
                both = sorted(set(group_spec.over) & attr_spec.dims)
                if both:
                    msg = (
                        f"attribute {attr!r} is addressed by "
                        f"{group_spec.into!r} and {both}, which the group "
                        f"{group!r} maps it from; `into` says the first follows "
                        f"from the second, so naming both keys a row twice over"
                    )
                    raise ValueError(msg)

        # A trait may only name an attribute that is declared: it says which
        # attributes apply, never what they are, so a name with no spec is a
        # typo rather than a shorthand declaration.
        for trait, trait_spec in self.traits.items():
            unknown = sorted(trait_spec.attributes - set(self.attributes))
            if unknown:
                msg = f"trait {trait!r} bundles undeclared attributes {unknown}"
                raise ValueError(msg)
            # Only the entity-type axis may scope a trait. Any other
            # classification would make the vocabulary depend on data rather
            # than on the schema - which attributes a component carries would
            # follow from what its bus maps to, a per-entity lookup every caller
            # of `attributes_for` treats as answerable from the schema alone.
            for dim in trait_spec.on:
                if dim != entity_type:
                    msg = (
                        f"trait {trait!r} is `on` {dim!r}, which does not classify "
                        f"`entity`; only the entity-type axis partitions an "
                        f"attribute vocabulary"
                    )
                    raise ValueError(msg)
            if trait_spec.switch is not None:
                switch_spec = self.attributes.get(trait_spec.switch)
                if switch_spec is not None and switch_spec.dims != {"entity"}:
                    msg = (
                        f"trait {trait!r} is switched on {trait_spec.switch!r}, "
                        f"which is addressed by {sorted(switch_spec.dims)}; a "
                        f"switch decides a trait per component, so it is "
                        f"addressed by `entity` alone"
                    )
                    raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared)
            if unknown:
                msg = f"`partial` names undeclared dims {unknown}"
                raise ValueError(msg)
            # `partial` names *value* dims a layer patches per value (a
            # per-scenario weight). A membership key - `entity`, a group's
            # coordinate - is in the fold key by being membership, not by being
            # `partial`, so naming it here is a category error: it does not
            # broadcast and has no "value" to patch, only rows that exist or do
            # not (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
            keys = sorted(set(self.partial) - set(self.broadcast_dims) - {entity_type})
            if keys:
                msg = (
                    f"`partial` names membership keys {keys}, which are patched "
                    f"per row by every layer already; `partial` is for value "
                    f"dims a layer patches per value"
                )
                raise ValueError(msg)

        # The fold key rests on every dim being exactly one of: a broadcast dim
        # (NULL means all-values), a membership key (a row exists or not), or the
        # entity-type axis (a column of `dims/entity.parquet`, not addressable).
        # All three derive from `broadcast_dims`, so this cannot fail unless that
        # derivation drifts - which would silently drop a coordinate from the fold
        # key or double-count it, the one broadcasting bug worth an assertion
        # (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
        broadcast = set(self.broadcast_dims)
        membership = set(self.membership_keys)
        type_axis = {entity_type} & set(self.dims) if entity_type else set()
        covered = broadcast | membership | type_axis
        disjoint = len(broadcast) + len(membership) + len(type_axis) == len(covered)
        if covered != set(self.dims) or not disjoint:
            msg = (
                f"every dim must be exactly one of broadcast, membership key or "
                f"the entity-type axis; got broadcast {sorted(broadcast)}, "
                f"membership {sorted(membership)}, type axis {sorted(type_axis)} "
                f"over dims {sorted(self.dims)}"
            )
            raise ValueError(msg)
        return self

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
        """The dims a NULL broadcasts over: every dim but `entity`, the type axis and a group's key.

        A NULL here means "every value of this dim", which the fold expands
        against the axis. The three exclusions cannot mean that:

        - `entity`, because a NULL there is a value belonging to no component
          rather than to all of them. The one dim named literally, being the
          one every entity-type axis classifies.
        - The entity-type axis, because it inherits `entity`'s exclusion: its
          labels are a column of `dims/entity.parquet`, so a NULL there is a
          component whose type is unknown rather than one of every type. An
          attribute addressed by the type *alone* never reaches here: it is a
          column of the type axis file rather than a long row, so it has no
          NULL to expand.
        - A coordinate a group is keyed by, because there is no axis to expand
          against. "Every bus of this component" is the group's rows, not the
          bus axis - a sparse subset only the group's table knows.

        A functional group's `into` dim is *not* excluded, though it is one of
        the group's `coordinates`: only `key` addresses a row, so `country`
        broadcasts like any other axis.

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
        sparse = {c for g in self.groups.values() for c in g.key}
        entity_type = self.entity_type_dim
        return tuple(
            d for d in self.dims if d not in ("entity", entity_type) and d not in sparse
        )

    def coordinates_of(self, attribute: str) -> tuple[str, ...]:
        """The dim columns one attribute's rows carry, groups expanded.

        One rule resolves a name in `dims`: **it is the dim of that name if one
        is declared, and otherwise the group of that name expanded to its
        coordinates**. So `dims={"connection", "snapshot"}` gives `("entity",
        "bus", "snapshot")` where no dim `connection` exists, and
        `dims={"country"}` gives `("country",)` - the dim, where a group of that
        name is shadowed.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL `entity` on a record-level
        weighting would be a column claiming a component the value has none of.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.spec_for(attribute)
        if spec is None:
            return ()
        named: set[str] = set()
        for d in spec.dims:
            group = None if d in self.dimensions else self.groups.get(d)
            named.update(group.coordinates if group is not None else (d,))
        # Declaration order, so every consumer sees one column order.
        return tuple(d for d in self.dims if d in named)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """One attribute's full long column set, in order - input or result.

        An attribute carries the coordinates its `dims` name and no others, so a
        record-level weighting has no `entity` column and a component attribute
        has no `bus`.

        An attribute neither vocabulary declares is `long_columns` - every
        declared dim, the widest shape. That is a schema with no manifest yet,
        every declared attribute having its own coordinates.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [results](https://energy-models.github.io/datarecord/design/working-record/#results-through-kindoutputs)
        """
        if self.spec_for(attribute) is None:
            return self.long_columns
        return (*self.coordinates_of(attribute), *LONG_TAIL)

    @property
    def entity_type_dim(self) -> str | None:
        """The dim classifying `entity`, or `None` where none does.

        The `into` of the group over `entity` alone, of which the schema admits
        at most one.

        Notes
        -----
        - [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
        """
        return next(
            (
                g.into
                for g in self.groups.values()
                if g.into is not None and tuple(g.over.values()) == ("entity",)
            ),
            None,
        )

    @property
    def entity_types(self) -> frozenset[str]:
        """Every declared entity-type label - the types a component may be.

        The entity-type axis's enum categories, so a schema declaring it as a
        plain `String` has none: the labels are then data rather than
        declarations, and `attributes_for` accepts any of them.

        Notes
        -----
        - [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
        """
        dim = self.entity_type_dim
        if dim is None:
            return frozenset()
        dtype = self.dimensions[dim].dtype
        return (
            frozenset(dtype.categories) if isinstance(dtype, nw.Enum) else frozenset()
        )

    def addresses_entity(self, attribute: str) -> bool:
        """Whether `attribute` reaches a component at all.

        True where its `dims` name `entity`, or a group one of whose
        coordinates draws on `entity`. False for an attribute over an axis
        alone - a snapshot weighting belongs to the record, so no entity type
        carries it however few traits mention it.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return "entity" in self.coordinates_of(attribute)

    def attributes_for(self, ctype: str) -> dict[str, AttributeSpec]:
        """Which attributes entity type `ctype` carries.

        Every attribute addressed by `entity` that no trait narrows, plus those
        the traits naming `ctype` bundle. Untraited is carried by all: writing
        `entity` in an attribute's `dims` is what says it is per component, and
        declining to bundle it says it is so for every type - the same thing
        `dims={"scenario"}` already means along the scenario axis.

        Empty for a label no declared entity-type axis lists, which is why
        callers rejecting an unknown type test `entity_types` rather than this.
        A schema declaring no entity type at all carries everything addressed
        by `entity`, whatever `ctype` is asked for.

        Notes
        -----
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        """
        known = self.entity_types
        if known and ctype not in known:
            return {}
        narrowed: set[str] = set()
        names: set[str] = set()
        for trait in self.traits.values():
            # A trait with no `on` narrows nothing: it is a bundle to dispatch
            # on, so its attributes stay carried by every type.
            scoped = {ctype for labels in trait.on.values() for ctype in labels}
            if not scoped:
                continue
            narrowed |= trait.attributes
            if any(ctype in labels for labels in trait.on.values()):
                names |= trait.attributes
        names |= {
            a for a in self.attributes if a not in narrowed and self.addresses_entity(a)
        }
        return {a: self.attributes[a] for a in sorted(names)}

    def owned_per(self, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per.

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `partial_dims` the fold key (membership keys
        plus the `partial` value dims), and ownership is their intersection. A
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
    def membership_keys(self) -> tuple[str, ...]:
        """The dims addressed per row rather than broadcast: `entity` and group coords.

        A membership key is a coordinate a layer patches one row of at a time -
        one component, one connection - never "every value" of an axis. It is
        the non-broadcast, addressable dims: every dim but the broadcast ones
        and the entity-type axis, which is a column of `dims/entity.parquet`
        rather than an addressable coordinate.

        These land in the fold key by being membership, not by being `partial`.

        Notes
        -----
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        broadcast = set(self.broadcast_dims)
        entity_type = self.entity_type_dim
        return tuple(d for d in self.dims if d not in broadcast and d != entity_type)

    @property
    def partial_dims(self) -> tuple[str, ...]:
        """The fold key's dims, in declaration order.

        The membership keys plus the broadcast value dims a layer may patch per
        value (`partial`). The fold's key is one fixed tuple over all
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
        keys = set(self.membership_keys)
        return tuple(d for d in self.dims if d in keys or d in partial)

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
        `addresses_entity` - what `dims/entity_type/<Type>.parquet` is to a
        component's constant columns, the axis file is to these.

        `entity` is one of these axes only where no group declares the type
        axis: with no type to classify a component into there is no member file
        for its constant columns, so they live on `dims/entity.parquet` like any
        other axis's (`entity_type_dim`). Where a group *does* declare the axis
        this returns `()` for `entity` - the columns are the *component* frame's,
        `dims/entity_type/<Type>.parquet`, a different destination with a
        different key.

        Keyed off `dims` rather than `coordinates_of`, because a group with one
        coordinate is indistinguishable there: `dims={"connection"}` over a
        single `bus` coordinate also yields `("bus",)`, and it belongs in the
        group's file rather than on the bus axis. A group over `entity` alone is
        keyed by the group name, not `entity`, so its `into` label and any
        attribute it bundles never match here.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [entity types](https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds)
        """
        if dim not in self.dimensions:
            return ()
        if dim == "entity" and self.entity_type_dim is not None:
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

        No `entity_type`: a row here is keyed by entity, and an entity is unique
        record-wide, so a type column would restate what the entity already says
        and let the two disagree. Every *declared* attribute is shaped by
        `long_columns_for` rather than by this, where naming both is rejected
        outright and one addressed by the type alone is a column of the type
        axis file (`attributes_on`) rather than a long row at all.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        entity_type = self.entity_type_dim
        return (*(d for d in self.dims if d != entity_type), *LONG_TAIL)

    @property
    def input_key(self) -> tuple[str, ...]:
        """Inputs-map key columns, compared NULL-safely when folding.

        `partial_dims`, plus `attribute`. `entity` and a group's coordinates are
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

    def groups_of(self, attribute: str) -> tuple[str, ...]:
        """Which declared groups address `attribute`, in declaration order.

        An attribute is a connection attribute because its `dims` name the
        `connection` group - not because a separate field says so. That is
        what lets a second group exist without a second field.

        A group a dim shadows is not one of them, `dims: [country]` naming the
        axis.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(
            g for g in self.groups if g in spec.dims and g not in self.dimensions
        )

    def group_coordinates(self, group: str) -> tuple[str, ...]:
        """One group's columns, or `()` if it is not declared.

        Every column of the group's file, `into` included. Coordinate names
        rather than dim names, so two drawing on one axis stay two columns.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        spec = self.groups.get(group)
        return () if spec is None else spec.coordinates

    def group_key(self, group: str) -> tuple[str, ...]:
        """One group's key columns, or `()` if it is not declared.

        `group_coordinates` minus `into` - what the fold keys ownership by and
        what a tombstone names.

        Notes
        -----
        - [into](https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        spec = self.groups.get(group)
        return () if spec is None else spec.key

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

        No dim is structural - `entity` and a group's `bus` included: each is
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

    def spec_for(self, attribute: str) -> AttributeSpec | None:
        """`attribute`'s spec, whether it is an input or a result.

        The one lookup that spans both vocabularies, for the questions the long
        schema asks of a stored attribute regardless of which file holds it -
        its dtype and its coordinates. Anything governing how an attribute may
        be *written* asks `attributes` or `results` directly, the two differing
        exactly there.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        return self.attributes.get(attribute) or self.results.get(attribute)

    def value_type(self, attribute: str) -> nw.dtypes.DType | None:
        """The `value` column's type for one attribute, input or result.

        No `ctype`: one attribute is one `<kind>/<attr>.parquet` with one
        `value` column, so the dtype is the attribute's alone. A narwhals
        dtype, translated to DuckDB (`duck.DuckTypes`) only where a caller builds
        a column of it.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.spec_for(attribute)
        return None if spec is None else spec.dtype

    def types_declaring(self, attribute: str) -> frozenset[str]:
        """Which entity types carry `attribute` - what `names=None` targets.

        Empty for a record-level attribute, which no type carries and which
        therefore targets no names at all, and empty too for a schema declaring
        no entity-type labels, where the caller has no type vocabulary to
        enumerate and works from the resolved components instead.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        return frozenset(
            c for c in self.entity_types if attribute in self.attributes_for(c)
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

        # Results version like inputs: a layer's `outputs/<attr>.parquet` is
        # unreadable for the same reasons its `inputs/` counterpart would be.
        for kind, mine, theirs in (
            ("attribute", self.attributes, other.attributes),
            ("result", self.results, other.results),
        ):
            for attr, was_spec in theirs.items():
                now_spec = mine.get(attr)
                if now_spec is None:
                    problems.append(f"{kind} {attr!r} removed")
                    continue
                if now_spec.dtype != was_spec.dtype:
                    problems.append(
                        f"{kind} {attr!r} dtype {was_spec.dtype} -> {now_spec.dtype}"
                    )
                narrowed = was_spec.dims - now_spec.dims
                if narrowed:
                    problems.append(
                        f"{kind} {attr!r} no longer varies over {sorted(narrowed)}; "
                        f"rows setting those dims have no valid reading"
                    )

        # A type losing an attribute is incompatible for the same reason a
        # narrowed `dims` is: its rows are still in the file, now unreadable
        # for that type. Losing a whole type says the same of all of them.
        for ctype in other.entity_types:
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
