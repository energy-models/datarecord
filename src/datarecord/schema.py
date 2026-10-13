# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The schema: what a record's data is, and how a patch to it behaves.

One schema per record, and `manifest.json` is how it is written down - the two
words name the same thing, the file and the object.

Framework-independent. Dims, properties and attributes are names a schema
declares, because those vocabularies belong to a modelling framework and this
package knows none: a label no tool recognises reads back fine and is reported
by the tool that cannot build it, not rejected here.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
"""

from __future__ import annotations

import math
from collections.abc import Iterable
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
# dims or properties: each is declared and typed from that declaration, which is
# what lets an `Enum` there pin its vocabulary. These are the ones no schema
# names - `breakpoint` is NULL for an ordinary scalar, so one column set serves
# every row.
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


LAYER_COLUMN = "layer_uuid"
"""The owner map's column naming the layer each row's value is read from.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
"""


DERIVED = ("order_key",)
"""Columns a resolved frame carries that no layer file may.

The fold's answer *about* a frame rather than data in it, so writing one would
both put a column in a file the format does not define and read as stored order
where the fold always re-derives it from file order.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
"""


RESERVED = frozenset(
    {*STRUCTURAL_TYPES, *LONG_TAIL, *FLAG_COLUMNS, LAYER_COLUMN, *DERIVED}
)
"""Columns datarecord writes beside a schema's own, which no declaration may take.

Compared ignoring case, as DuckDB resolves a column name. The fold's scratch
columns are not listed: every one starts with `_`, which `Schema` refuses as a
declared name's first character instead.

Notes
-----
- [names](https://energy-models.github.io/datarecord/design/schema/#names)
"""


def _misnamed(name: str) -> str | None:
    """Why `name` cannot be a declared dim, group, coordinate or attribute, if so.

    `duckdb.ColumnExpression` is how every declared name becomes a column
    reference, and it reads `a.b` as column `b` of table `a` and cannot spell a
    `"` at all; those two are what make a name unquotable, rather than a space
    or a keyword, which it quotes.
    """
    if '"' in name:
        return 'contains `"`, which no DuckDB column reference can spell'
    if "." in name:
        return "contains `.`, which DuckDB reads as a table qualifier"
    if name.startswith("_"):
        return "starts with `_`, kept for the columns a fold adds and drops"
    if name.lower() in RESERVED:
        return f"is `{name.lower()}`, a column datarecord writes into every record"
    return None


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


class _ValueSpec(BaseModel):
    """What `AttributeSpec` and `PropertySpec` share: a typed value and its defaults.

    The `default`/`default_by` encoding lives here once, so a property and an
    attribute round-trip `manifest.json` identically.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dtype: nw.dtypes.DType
    default: Any | None = None
    default_by: dict[str, dict[str, Any]] = Field(default_factory=dict)
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
        return _encode_default(value)

    @field_validator("default", mode="before")
    @classmethod
    def _parse_default(cls, value: Any) -> Any:
        return _decode_default(value)

    @field_serializer("default_by")
    def _serialise_default_by(
        self, value: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        return {
            key: {label: _encode_default(v) for label, v in by.items()}
            for key, by in value.items()
        }

    @field_validator("default_by", mode="before")
    @classmethod
    def _parse_default_by(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        return {
            key: {label: _decode_default(v) for label, v in by.items()}
            if isinstance(by, dict)
            else by
            for key, by in value.items()
        }


def _encode_default(value: Any) -> Any:
    """Encode a non-finite default as a string; JSON has no literal for one.

    `inf` is an ordinary default for an unbounded capacity, and JSON's
    `Infinity` is not valid JSON - a plain dump reads back as `None`,
    turning "unbounded" into "no default". `_decode_default` reverses this.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return f"__{value}__"
    return value


def _decode_default(value: Any) -> Any:
    """Decode what `_encode_default` encoded."""
    if isinstance(value, str) and value.startswith("__") and value.endswith("__"):
        try:
            return float(value[2:-2])
        except ValueError:
            return value
    return value


class AttributeSpec(_ValueSpec):
    """What shape one attribute's data may take.

    Attributes
    ----------
    dtype
        The value column's type, as a narwhals dtype instance (`nw.String()`,
        `nw.Datetime()`, ...) - translated to its DuckDB name only where a
        column of it is built.
    dims
        Dims and groups this attribute may vary over; a subset of those
        declared. Exactly one dim makes it a column of that dim's axis file
        rather than `inputs/`, so the schema decides the file split.
    default
        The value a coordinate no row covers takes.
    default_by
        The discriminator's name -> label -> the default for members of that
        label, overriding `default`. Read through `Schema.default_for`; never
        applied by the record layer.
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

    dims: frozenset[str] = frozenset()
    breakpoints: bool = False

    @property
    def varying(self) -> bool:
        """Whether this attribute's values are long rows rather than a column.

        "Varies beyond its address", not "has dims": naming exactly one
        addressing coordinate is a column on that thing's own table, so
        `dims={"bus"}` is a column of the bus axis and `dims={"connection"}`
        a column of the group's table. Anything more is `inputs/<attr>.parquet`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        return len(self.dims) > 1


class PropertySpec(_ValueSpec):
    """One column of a dim's axis file that describes its labels.

    A property is keyed by its subject dim (`Schema.properties`) and stored as a
    column of `dims/{dim}.parquet`, beside the attributes addressed by that dim
    alone.

    Attributes
    ----------
    dtype
        The column's type, as a narwhals dtype instance.
    default
        The value a label no row sets takes.
    default_by
        The discriminator's name -> label -> the default for members of that
        label, overriding `default`. Read through `Schema.default_for`.
    discriminator
        Whether this property partitions its dim's labels into the kinds traits
        are `on`. At most one per schema, and its `dtype` must be an `Enum`,
        whose categories are the labels.
    unit
        What the values measure; `None` is undeclared.
    description
        What the property is, in prose. Never interpreted.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """

    discriminator: bool = False


class Group(BaseModel):
    """Which tuples over several dims exist: a sparse subset of a dim product.

    Not a dim. A dim declares an axis of labels and NULL in its column means
    "every value of it"; a group declares *which combinations are there*, which
    no axis can say because the product is sparse - an entity attaches to two
    buses out of a thousand.

    An attribute names the group in its `dims` and its rows carry the group as
    one struct column of the group's name, a field per key coordinate
    (`Schema.group_type`). Coordinates rather than dims because two of them may
    draw on the same axis: a corridor between two entities is `(from, to)`,
    which a set of dims could not spell.

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
    def key(self) -> tuple[str, ...]:
        """The coordinates a row is unique over: `over`'s, in declaration order.

        `into` is none of them: it follows from the key.

        Notes
        -----
        - [into](https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)
        """
        return tuple(self.over)


class Trait(BaseModel):
    """A named bundle of attributes, and which discriminator labels and members carry it.

    The narrowing direction: an attribute the schema declares is carried by
    every label of the discriminator, and a trait is how that is cut down to
    some of them, and further to some of their members. A trait bundling
    `p_max_pu` `on={"entity_type": {"Generator"}}` says those attributes reach
    generators and nothing else.

    Attributes
    ----------
    attributes
        The attributes this trait bundles. Each must be declared. Includes
        `switch` once parsed, whatever the author wrote.
    on
        Mapping discriminator name -> the labels of it this trait applies to.
        Only the discriminator may key this - `Schema` rejects any other. Empty
        means this narrowing does not apply, reaching every label.
    switch
        The attribute deciding, per member of the discriminator's dim, whether
        this trait applies - addressed by that dim exactly. `None` means this
        narrowing does not apply, reaching every member.
    description
        What the trait is, in prose. Never interpreted.

    Notes
    -----
    - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
    - [switch](https://energy-models.github.io/datarecord/design/schema/#switch-a-trait-an-entity-opts-into)
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
    properties
        Subject dim -> property name -> spec: the columns of `dims/{dim}.parquet`
        that describe its labels. One of them may be the `discriminator`.
    groups
        Group name -> which tuples over several dims exist.
    traits
        Trait -> the attributes it bundles and the discriminator labels
        carrying them. A vocabulary a consumer dispatches on, declared rather
        than derived, and the only thing that narrows an attribute to some
        labels.
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
    than `inputs/`, never overlays a parent's, and may name an entity the
    record does not declare. Keeping it out of `attributes` is what stops it
    reaching `attributes_for`, and so `add`'s wide-frame split and the input
    validation, neither of which a result should meet.
    """

    properties: dict[str, dict[str, PropertySpec]] = Field(default_factory=dict)
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
        - [names](https://energy-models.github.io/datarecord/design/schema/#names)
        """
        self._check_names()
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

        for group, group_spec in self.groups.items():
            unknown = sorted(set(group_spec.over.values()) - declared)
            if unknown:
                msg = f"group {group!r} is over undeclared dims {unknown}"
                raise ValueError(msg)
            if group in declared:
                renamed = "_".join((*group_spec.over, group))
                msg = (
                    f"group {group!r} shares its name with the dim {group!r}; a "
                    f"group is carried as a column named for it, so the two would "
                    f"be one column. Rename the group, e.g. {renamed!r} into "
                    f"{group!r} (https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)"
                )
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

        self._check_properties()

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
                if group in self.groups_of(attr):
                    twice = sorted(set(group_spec.key) & attr_spec.dims)
                    if twice:
                        msg = (
                            f"attribute {attr!r} is addressed by the group "
                            f"{group!r} and by {twice}, which the group's "
                            f"column already carries; naming both keys a row "
                            f"twice over (https://energy-models.github.io/datarecord/design/schema/#groups)"
                        )
                        raise ValueError(msg)
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
            # Only the discriminator may scope a trait. Any other property would
            # make the vocabulary depend on something no schema pins - which
            # attributes a member carries is then answerable from the schema
            # alone, as every caller of `attributes_for` assumes.
            discriminator = self.discriminator
            for name in trait_spec.on:
                if discriminator is None or name != discriminator[1]:
                    msg = (
                        f"trait {trait!r} is `on` {name!r}, which is not the "
                        f"schema's discriminator {discriminator}; only the "
                        f"discriminator partitions an attribute vocabulary"
                    )
                    raise ValueError(msg)
            if trait_spec.switch is not None:
                switch_spec = self.attributes.get(trait_spec.switch)
                subject = (
                    {discriminator[0]}
                    if discriminator is not None
                    else switch_spec.dims
                    if switch_spec is not None and len(switch_spec.dims) == 1
                    else None
                )
                if switch_spec is not None and switch_spec.dims != subject:
                    msg = (
                        f"trait {trait!r} is switched on {trait_spec.switch!r}, "
                        f"which is addressed by {sorted(switch_spec.dims)}; a "
                        f"switch decides a trait per member of one dim - the "
                        f"discriminator's, where one is declared - so it is "
                        f"addressed by that dim alone"
                    )
                    raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared - set(self.groups))
            if unknown:
                msg = f"`partial` names undeclared dims or groups {unknown}"
                raise ValueError(msg)
        return self

    def _check_properties(self) -> None:
        """Refuse a property on an undeclared dim, and a malformed discriminator or `default_by`.

        Run after `_check_names`, so a property name is already known not to
        clash with an attribute's.
        """
        unknown = sorted(set(self.properties) - set(self.dimensions))
        if unknown:
            msg = (
                f"properties are declared on undeclared dims {unknown}; a "
                f"property is a column of its subject dim's axis file"
            )
            raise ValueError(msg)
        flagged = [
            (dim, name)
            for dim, specs in self.properties.items()
            for name, spec in specs.items()
            if spec.discriminator
        ]
        if len(flagged) > 1:
            msg = (
                f"{flagged} are all flagged `discriminator`; a schema has at "
                f"most one, every caller of `attributes_for` asking for one "
                f"vocabulary"
            )
            raise ValueError(msg)
        for dim, name in flagged:
            dtype = self.properties[dim][name].dtype
            if not isinstance(dtype, nw.Enum):
                msg = (
                    f"discriminator {name!r} on {dim!r} is {dtype}; a "
                    f"discriminator is an `Enum`, its categories being the "
                    f"labels traits are `on`"
                )
                raise ValueError(msg)
        subject, discriminator = flagged[0] if flagged else (None, None)
        specs: list[tuple[str, _ValueSpec, bool]] = [
            *(
                (f"property {n!r}", s, d == subject)
                for d, by_name in self.properties.items()
                for n, s in by_name.items()
            ),
            *(
                (
                    f"attribute {a!r}",
                    s,
                    subject is not None and self.addresses(a, subject),
                )
                for a, s in (*self.attributes.items(), *self.results.items())
            ),
        ]
        labels = self.discriminator_labels
        for what, spec, reaches in specs:
            if not spec.default_by:
                continue
            keys = sorted(spec.default_by)
            if keys != [discriminator]:
                msg = (
                    f"{what} has `default_by` keyed by {keys}; its one key is "
                    f"the discriminator, {discriminator!r}"
                )
                raise ValueError(msg)
            if not reaches:
                msg = (
                    f"{what} has `default_by` but does not reach the "
                    f"discriminator's dim {subject!r}, so no member of it has a "
                    f"label to default by"
                )
                raise ValueError(msg)
            unknown = sorted(set(spec.default_by[keys[0]]) - labels)
            if unknown:
                msg = (
                    f"{what} has `default_by` for {unknown}, which are not "
                    f"categories of the discriminator {keys[0]!r}"
                )
                raise ValueError(msg)

    def _check_names(self) -> None:
        """Refuse every declared name that cannot be a column of its own.

        A group's name is a column too, its struct column, and so is a
        property's. Two spellings of one name differing only in case are
        refused, DuckDB resolving both to one column; one spelling declared
        twice, as a group's coordinate naming a dim, is left to the rules that
        govern it - except across attributes, results and properties, where one
        name is one spec.
        """
        valued = [
            *(("attribute", a) for a in self.attributes),
            *(("result", r) for r in self.results),
            *(
                (f"property of {d!r}", p)
                for d, specs in self.properties.items()
                for p in specs
            ),
        ]
        declared = [
            *(("dim", d) for d in self.dimensions),
            *(("group", g) for g in self.groups),
            *(
                (f"coordinate of group {g!r}", c)
                for g, spec in self.groups.items()
                for c in spec.over
            ),
            *valued,
        ]
        problems = []
        for kind, name in declared:
            why = _misnamed(name)
            if why is not None:
                problems.append(f"{kind} {name!r} {why}")
        kinds: dict[str, list[str]] = {}
        for kind, name in valued:
            kinds.setdefault(name, []).append(kind)
        clashing = {n: k for n, k in kinds.items() if len(k) > 1}
        if clashing:
            detail = "; ".join(
                f"{n!r} as {' and '.join(k)}" for n, k in clashing.items()
            )
            msg = (
                f"one name is one spec, but these are declared more than once: {detail}"
            )
            raise ValueError(msg)
        spellings: dict[str, dict[str, str]] = {}
        for kind, name in declared:
            spellings.setdefault(name.lower(), {}).setdefault(name, kind)
        for named in spellings.values():
            if len(named) > 1:
                clash = ", ".join(f"{kind} {name!r}" for name, kind in named.items())
                problems.append(f"{clash} differ only in case, one column to DuckDB")
        if problems:
            msg = (
                f"a schema may not declare these names: {'; '.join(problems)}. "
                f"Rename each: a declared name becomes a column beside those "
                f"datarecord writes itself, {sorted(RESERVED)} and any starting "
                f"with `_` (https://energy-models.github.io/datarecord/design/schema/#names)"
            )
            raise ValueError(msg)

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
        """The coordinates a NULL broadcasts over: every declared dim and every group column.

        A NULL here means "every value of this dim", which the fold expands
        against the axis; a NULL struct means every tuple of the resolved
        group.

        What the `varies`/`broadcast` structs have a field per, and what
        `expand_dims` joins.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return (*self.dims, *self.group_columns)

    def coordinates_of(self, attribute: str) -> tuple[str, ...]:
        """The coordinate columns one attribute's rows carry, groups first.

        A group in `dims` is carried as one struct column named for it
        (`group_type`), so `dims={"connection", "snapshot"}` gives
        `("connection", "snapshot")`.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL column of a dim the attribute
        is not addressed by would claim a coordinate the value has none of.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.spec_for(attribute)
        if spec is None:
            return ()
        return (*self.groups_of(attribute), *(d for d in self.dims if d in spec.dims))

    @property
    def group_columns(self) -> tuple[str, ...]:
        """Every group, each carried in a row as one struct column named for it.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return tuple(self.groups)

    def group_type(self, group: str) -> nw.dtypes.DType:
        """The struct a row carries `group` as: a field per key coordinate.

        Each field typed as the dim the coordinate draws on. `into` is no field:
        it follows from the key, so carrying it would key a row twice over.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        spec = self.groups[group]
        return nw.Struct({c: self.dimensions[spec.over[c]].dtype for c in spec.key})

    def check_group_columns(
        self,
        groups: Iterable[str],
        frame: nw.LazyFrame[Any],
        *,
        carrier: str,
    ) -> None:
        """Reject each of `groups` carried other than as one struct column of exactly its key, whole or NULL.

        A carrier without a group's column passes.

        Parameters
        ----------
        carrier
            What carries the columns, named in the message.

        Raises
        ------
        ValueError
            On a key coordinate as a column of its own, a group column not a
            struct of exactly the key, naming the struct to pass instead, or a
            struct with some fields NULL and not all.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        """
        columns = frame.collect_schema()
        for group in groups:
            key = self.group_key(group)
            flat = [c for c in key if c in columns]
            dtype = columns.get(group)
            fields = (
                key
                if dtype is None
                else [f.name for f in dtype.fields]
                if isinstance(dtype, nw.Struct)
                else None
            )
            if flat or fields is None or set(fields) != set(key):
                carried = f"{flat} flat" if flat else f"{group!r} as {dtype}"
                msg = (
                    f"{carrier} carries {carried}; pass {group!r} as one struct "
                    f"column with a field per key coordinate, {list(key)} (https://energy-models.github.io/datarecord/design/schema/#groups)"
                )
                raise ValueError(msg)
            if dtype is None:
                continue
            struct = nw.col(group)
            partly = ~struct.is_null() & nw.any_horizontal(
                *(struct.struct.field(c).is_null() for c in key), ignore_nulls=True
            )
            if not frame.filter(partly).head(1).collect().is_empty():
                msg = (
                    f"{carrier} carries a {group!r} with some fields NULL; a "
                    f"group value is a whole tuple, or NULL for every tuple (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)"
                )
                raise ValueError(msg)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """One attribute's full long column set, in order - input or result.

        An attribute carries the coordinates its `dims` name and no others, so a
        per-snapshot weighting has no column for any other dim.

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
    def discriminator(self) -> tuple[str, str] | None:
        """`(dim, property)` of the one property flagged `discriminator`, or `None`.

        Notes
        -----
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        """
        return next(
            (
                (dim, name)
                for dim, specs in self.properties.items()
                for name, spec in specs.items()
                if spec.discriminator
            ),
            None,
        )

    @property
    def discriminator_labels(self) -> frozenset[str]:
        """The discriminator's `Enum` categories; empty where none is declared.

        Notes
        -----
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        """
        found = self.discriminator
        if found is None:
            return frozenset()
        dim, name = found
        dtype = self.properties[dim][name].dtype
        return (
            frozenset(dtype.categories) if isinstance(dtype, nw.Enum) else frozenset()
        )

    def default_for(self, name: str, label: str | None = None) -> Any:
        """The default of attribute, result or property `name`, for discriminator `label`.

        `label`'s entry in `default_by` where it has one, else `default`.

        Raises
        ------
        KeyError
            If no attribute, result or property is called `name`.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
        """
        spec: _ValueSpec | None = self.spec_for(name)
        if spec is None:
            spec = next((s[name] for s in self.properties.values() if name in s), None)
        if spec is None:
            raise KeyError(name)
        found = self.discriminator
        if label is None or found is None:
            return spec.default
        return spec.default_by.get(found[1], {}).get(label, spec.default)

    def addresses(self, attribute: str, dim: str) -> bool:
        """Whether `attribute` reaches a label of `dim` at all.

        True where its `dims` name `dim`, or a group one of whose coordinates
        draws on `dim`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        spec = self.spec_for(attribute)
        return (spec is not None and dim in spec.dims) or any(
            dim in self.groups[g].over.values() for g in self.groups_of(attribute)
        )

    def attributes_for(self, label: str) -> dict[str, AttributeSpec]:
        """Which attributes the discriminator's `label` carries.

        Every attribute addressing the discriminator's dim that no trait
        narrows, plus those the traits naming `label` bundle. Untraited is
        carried by all: declining to bundle an attribute says it is carried by
        every label.

        Empty for a label the discriminator's categories do not list. A schema
        declaring no discriminator narrows nothing, so every attribute is
        carried whatever `label` is asked for.

        Notes
        -----
        - [traits](https://energy-models.github.io/datarecord/design/schema/#traits)
        """
        found = self.discriminator
        if found is None:
            return dict(sorted(self.attributes.items()))
        if label not in self.discriminator_labels:
            return {}
        narrowed: set[str] = set()
        names: set[str] = set()
        for trait in self.traits.values():
            # A trait with no `on` narrows nothing: it is a bundle to dispatch
            # on, so its attributes stay carried by every label.
            if not any(trait.on.values()):
                continue
            narrowed |= trait.attributes
            if any(label in labels for labels in trait.on.values()):
                names |= trait.attributes
        names |= {
            a
            for a in self.attributes
            if a not in narrowed and self.addresses(a, found[0])
        }
        return {a: self.attributes[a] for a in sorted(names)}

    def owned_per(self, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per.

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `partial_dims` the fold key (group columns
        plus the `partial` dims), and ownership is their intersection. A
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
        """The fold key's coordinates, in declaration order.

        The dims and groups a layer may patch per value (`partial`). The fold's
        key is one fixed tuple over all attributes, so it carries every axis *any* layer may patch by value or
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
        return tuple(d for d in self.broadcast_dims if d in partial)

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
        """Attributes and properties stored as columns of `dims/{dim}.parquet`.

        An attribute addressed by `dim` alone - a per-country CO2 budget, a
        snapshot weighting - for which `AttributeSpec.varying` is False, then
        the properties declared on `dim`.

        Keyed off `dims` rather than `coordinates_of`, because a group with one
        coordinate is indistinguishable there: `dims={"connection"}` over a
        single `bus` coordinate also yields `("bus",)`, and it belongs in the
        group's file rather than on the bus axis.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        if dim not in self.dimensions:
            return ()
        return (
            *(
                a
                for a, spec in self.attributes.items()
                if spec.dims == frozenset({dim})
            ),
            *self.properties.get(dim, {}),
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
        return (*self.group_columns, *self.dims, *LONG_TAIL)

    @property
    def input_key(self) -> tuple[str, ...]:
        """Inputs-map key columns, compared NULL-safely when folding.

        `partial_dims`, plus `attribute`: a layer may patch one value along a
        `partial` dim or group without restating every other's.

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
        """Which declared groups address `attribute`, input or result, in declaration order.

        An attribute is a connection attribute because its `dims` name the
        `connection` group - not because a separate field says so. That is
        what lets a second group exist without a second field.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        """
        spec = self.spec_for(attribute)
        if spec is None:
            return ()
        return tuple(g for g in self.groups if g in spec.dims)

    def group_key(self, group: str) -> tuple[str, ...]:
        """One group's key coordinates - its struct column's fields - or `()` if undeclared.

        `into` is none of them: the tuple is the key, whatever label it carries.

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
        return (*self.input_key, LAYER_COLUMN, *FLAG_COLUMNS)

    # -- typing (https://energy-models.github.io/datarecord/design/format/#the-long-schema, https://energy-models.github.io/datarecord/design/writing/) -------------------------------------------------

    def column_type(self, column: str) -> nw.dtypes.DType | None:
        """The declared type for one column, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims, the
        attributes and properties an axis file carries as columns
        (`attributes_on`), and the owner map's two flag structs, whose fields
        follow the schema's dims. A narwhals dtype, translated to DuckDB
        (`duck.DuckTypes`) only where a caller builds a column of it.

        No dim is structural: each is declared, and typed from that
        declaration. So an `Enum` property pins its vocabulary everywhere the
        column is built.

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
        if column in self.groups:
            return self.group_type(column)
        if column in ("varies", "broadcast"):
            return flag_type(self.broadcast_dims) if self.broadcast_dims else None
        spec = self.attributes.get(column)
        if spec is not None and not spec.varying:
            (dim,) = spec.dims
            if column in self.attributes_on(dim):
                return spec.dtype
        for specs in self.properties.values():
            if column in specs:
                return specs[column].dtype
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

    def labels_declaring(self, attribute: str) -> frozenset[str]:
        """Which discriminator labels carry `attribute`.

        Empty for an attribute not addressing the discriminator's dim, which no
        label carries, and empty too for a schema declaring no discriminator.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        return frozenset(
            c for c in self.discriminator_labels if attribute in self.attributes_for(c)
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

        for dim, specs in other.properties.items():
            for name, was_prop in specs.items():
                now_prop = self.properties.get(dim, {}).get(name)
                if now_prop is None:
                    problems.append(f"property {name!r} of {dim!r} removed")
                elif now_prop.dtype != was_prop.dtype:
                    problems.append(
                        f"property {name!r} of {dim!r} dtype {was_prop.dtype} "
                        f"-> {now_prop.dtype}"
                    )

        # A label losing an attribute is incompatible for the same reason a
        # narrowed `dims` is: its rows are still in the file, now unreadable
        # for that label. Losing a whole label says the same of all of them.
        for label in other.discriminator_labels:
            was_attrs = set(other.attributes_for(label))
            dropped = sorted(was_attrs - set(self.attributes_for(label)))
            if dropped:
                problems.append(
                    f"label {label!r} no longer carries {dropped}; "
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
