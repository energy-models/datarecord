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

import json
import math
from collections.abc import Iterable, Mapping
from typing import Any, get_args
from uuid import UUID

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, sqltypes
from duckdb import SQLExpression as sql
from duckdb.sqltypes import DuckDBPyType
from mathspec.program import DimensionDtype, ParameterDtype
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from datarecord.duck import DUCK_TYPES, enum_type, fn, takes


def _parse_dtype(value: Any, plain: tuple[str, ...], one_of: str | None) -> Any:
    """`value` as a dtype allows it, refusing a malformed `one_of`.

    Parameters
    ----------
    plain
        The mathspec dtypes a declaration of this kind takes.
    one_of
        How its `one_of` is written, `[...]` for a dim and `"<dim>"` for a
        property, or None where it takes none.

    Notes
    -----
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    """
    forms = ", ".join(plain) + (f', or {{"one_of": {one_of}}}' if one_of else "")
    if isinstance(value, str) and value not in plain:
        msg = f"dtype {value!r} is none of {forms}"
        raise ValueError(msg)
    if isinstance(value, Mapping) and "one_of" in value:
        labels = value["one_of"]
        if one_of is None:
            msg = (
                f"dtype {json.dumps(value)} takes no `one_of`, as a mathspec "
                f"parameter has none; write one of {forms}"
            )
            raise ValueError(msg)
        if isinstance(labels, str) == (one_of == "[...]"):
            msg = f'dtype {json.dumps(value)} writes `one_of` as {{"one_of": {one_of}}}'
            raise ValueError(msg)
    return value


class _Declared(BaseModel):
    """A model `manifest.json` is parsed into, refusing a key it does not declare."""

    model_config = ConfigDict(extra="forbid")


class OneOf(_Declared):
    """A dim's dtype listing its labels, `{"one_of": [...]}`: a closed dim.

    Its column is an `ENUM` of the labels, in their order.

    Notes
    -----
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    """

    one_of: tuple[str, ...] = Field(min_length=1)

    @field_validator("one_of")
    @classmethod
    def _distinct(cls, labels: tuple[str, ...]) -> tuple[str, ...]:
        repeated = sorted({label for label in labels if labels.count(label) > 1})
        if repeated:
            msg = f"`one_of` lists {repeated} more than once; list each label once"
            raise ValueError(msg)
        return labels


class OneOfDim(_Declared):
    """A property's dtype naming the dim its values are labels of, `{"one_of": "<dim>"}`.

    Its column has that dim's type.

    Notes
    -----
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
    """

    one_of: str


# Columns the format fixes, whatever the schema declares (https://energy-models.github.io/datarecord/design/format/#the-long-schema). Not the
# dims or properties: each is declared and typed from that declaration, which is
# what lets a closed dim's `ENUM` pin its vocabulary. These are the ones no schema
# names - `breakpoint` is NULL for an ordinary scalar, so one column set serves
# every row.
STRUCTURAL_TYPES = {
    "attribute": sqltypes.VARCHAR,
    "breakpoint": sqltypes.DOUBLE,
    "deleted": sqltypes.BOOLEAN,
    "breakpoints": sqltypes.BOOLEAN,
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


def flag_type(dims: tuple[str, ...]) -> DuckDBPyType:
    """One flag struct's type: a BOOLEAN field per declared dim.

    `dims` is never empty: a schema declaring no dims describes no dimensioned
    data, which `Schema` rejects - so the struct always has a field and
    needs no placeholder for DuckDB's want of an empty one.

    Notes
    -----
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    return duckdb.struct_type(dict.fromkeys(dims, sqltypes.BOOLEAN))


class Dimension(_Declared):
    """One axis attribute data may vary over: its shape, not its data.

    Not which dims an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared.

    Attributes
    ----------
    dtype
        The axis labels' type: a mathspec `DimensionDtype`, or
        `{"one_of": [...]}` listing them, which closes the dim.
    unit
        What this axis's *labels* measure, if anything - `None` is undeclared,
        `""` genuinely dimensionless.
    description
        What the axis is, in prose. Never interpreted.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/record/#axis-order)
    - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    """

    dtype: DimensionDtype | OneOf
    unit: str | None = None
    description: str | None = None

    @field_validator("dtype", mode="before")
    @classmethod
    def _parse_dtype(cls, value: Any) -> Any:
        return _parse_dtype(value, get_args(DimensionDtype), "[...]")


class _ValueSpec(_Declared):
    """What `AttributeSpec` and `PropertySpec` share: a typed value and its defaults.

    The `default`/`default_by` encoding lives here once, so a property and an
    attribute round-trip `manifest.json` identically.
    """

    default: Any | None = None
    default_by: dict[str, dict[str, Any]] = Field(default_factory=dict)
    unit: str | None = None
    description: str | None = None

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
        The value column's type, a mathspec `ParameterDtype`.
    dims
        Dims and groups this attribute may vary over; a subset of those
        declared. Whatever they are, its values are long rows of
        `attributes/<attr>.parquet`.
    default
        The value a coordinate no row covers takes.
    default_by
        A categorizing property, as `subject.name`, -> category -> the default
        for labels of that category, overriding `default`; the first key, in
        declaration order, listing a label's category wins. Each key must
        categorize a dim the attribute reaches. Read through `Schema.default_for`; never applied by
        the record layer.
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
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    """

    dtype: ParameterDtype
    dims: frozenset[str] = frozenset()
    breakpoints: bool = False

    @field_validator("dtype", mode="before")
    @classmethod
    def _parse_dtype(cls, value: Any) -> Any:
        return _parse_dtype(value, get_args(ParameterDtype), None)


class PropertySpec(_ValueSpec):
    """One value per label of a dim, or per tuple of a group, describing it.

    A property is keyed by its subject (`Schema.properties`) and stored as a
    column of `dims/{dim}.parquet`, or of `groups/{group}.parquet` for a group.
    Its name is its subject's alone, so it is addressed as `subject.name`
    (`Schema.resolve`).
    A property of a dim in `partial` naming a closed dim categorizes it
    (`Schema.categorizing`); a group's never does.

    Attributes
    ----------
    dtype
        The column's type: a mathspec `ParameterDtype`, or `{"one_of": "<dim>"}`
        naming the dim whose labels its values are, typed as that dim. A
        stored non-NULL value of the latter names a live label of it.
    default
        The value a label no row sets takes.
    default_by
        A categorizing property of a dim the subject reaches, as
        `subject.name`, -> category -> the default for labels of that category,
        overriding `default`. Refused on a categorizing property itself. Read
        through `Schema.default_for`.
    unit
        What the values measure; `None` is undeclared.
    description
        What the property is, in prose. Never interpreted.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
    """

    dtype: ParameterDtype | OneOfDim

    @field_validator("dtype", mode="before")
    @classmethod
    def _parse_dtype(cls, value: Any) -> Any:
        return _parse_dtype(value, get_args(ParameterDtype), '"<dim>"')


class Group(_Declared):
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
    description
        What the group is, in prose. Never interpreted.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """

    over: dict[str, str]
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

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return tuple(self.over)


class Schema(_Declared):
    """One record's schema.

    Attributes
    ----------
    id
        A standalone record's id, which its `manifest.json` carries beside the
        declarations and `Workspace.write` mints; `None` on a tree's schema,
        and then left out of a dump.
    version
        Bumped by any change to the declarations. A reader meeting a
        version it was not written for should refuse rather than guess.
    dimensions
        Every declared axis, keyed by dim name.
    attributes
        Attribute -> spec, flat and record-wide. One attribute is one spec and
        one `attributes/<attr>.parquet`, so a dtype cannot differ per type.
    properties
        Subject dim or group -> property name -> spec: the columns of
        `dims/{dim}.parquet` or `groups/{group}.parquet` that describe its
        labels. A name is unique within its subject only, addressed as
        `subject.name`. A dim's properties naming a closed dim categorize it
        where it is in `partial`.
    groups
        Group name -> which tuples over several dims exist.
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

    id: UUID | None = Field(default=None, exclude_if=lambda id: id is None)
    version: int = 1
    dimensions: dict[str, Dimension] = Field(default_factory=dict)
    attributes: dict[str, AttributeSpec] = Field(default_factory=dict)
    properties: dict[str, dict[str, PropertySpec]] = Field(default_factory=dict)
    groups: dict[str, Group] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/design/schema/#dimensions)
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        - [names](https://energy-models.github.io/datarecord/design/schema/#names)
        """
        self._check_names()
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
                    f"be one column. Rename the group, e.g. {renamed!r} "
                    f"(https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)"
                )
                raise ValueError(msg)

        self._check_properties()

        addressable = declared | set(self.groups)
        for attr, attr_spec in self.attributes.items():
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

        if self.partial is not None:
            unknown = sorted(self.partial - declared - set(self.groups))
            if unknown:
                msg = f"`partial` names undeclared dims or groups {unknown}"
                raise ValueError(msg)
        return self

    def _categorized_by(self, name: str, what: str) -> tuple[str, str]:
        """The dim and property dotted `name` categorizes, for a `default_by` key `what` names."""
        subject, _, prop = name.partition(".")
        if prop not in self.properties.get(subject, {}):
            reason = "is no property, named `subject.name`"
        elif subject in self.groups:
            reason = (
                f"is a property of the group {subject!r}; a group is never categorized"
            )
        elif not isinstance(dtype := self.properties[subject][prop].dtype, OneOfDim):
            reason = 'is no `{"one_of": <dim>}` property'
        elif not isinstance(self.dimensions[dtype.one_of].dtype, OneOf):
            reason = (
                f"names {dtype.one_of!r}, which lists no labels, so they are data, "
                f"not categories"
            )
        elif subject not in (self.partial or set()):
            reason = f"is a property of {subject!r}, which `partial` does not list"
        else:
            return subject, prop
        msg = f"{what} is keyed by {name!r}, which {reason}"
        raise ValueError(msg)

    def _check_properties(self) -> None:
        """Refuse a property on an undeclared subject or naming an undeclared dim, or a malformed `default_by`."""
        unknown = sorted(set(self.properties) - set(self.dimensions) - set(self.groups))
        if unknown:
            msg = (
                f"properties are declared on undeclared dims or groups {unknown}; "
                f"a property is a column of its subject's axis or group file"
            )
            raise ValueError(msg)
        for (subject, name), dim in self.one_of_properties().items():
            if dim not in self.dimensions:
                msg = (
                    f'property {subject}.{name} is `{{"one_of": {dim!r}}}`, an '
                    f"undeclared dim; `one_of` names the dim whose labels its "
                    f"values are (https://energy-models.github.io/datarecord/design/schema/#properties)"
                )
                raise ValueError(msg)
        for subject, by_name in self.properties.items():
            for name in self.categorizing(subject):
                if by_name[name].default_by:
                    msg = (
                        f"property {subject}.{name} categorizes {subject!r} and has a "
                        f"`default_by`; a category defaulted by a category is "
                        f"circular - give it a plain `default`"
                    )
                    raise ValueError(msg)
        valued: list[tuple[str, _ValueSpec, frozenset[str]]] = [
            *(
                (f"property {subject}.{n}", s, self._reached_by(subject))
                for subject, by_name in self.properties.items()
                for n, s in by_name.items()
            ),
            *(
                (
                    f"attribute {a!r}",
                    s,
                    frozenset(d for d in self.dimensions if self.addresses(a, d)),
                )
                for a, s in self.attributes.items()
            ),
        ]
        for what, spec, reached in valued:
            for key, by in spec.default_by.items():
                subject, prop = self._categorized_by(key, f"{what}'s `default_by`")
                if subject not in reached:
                    msg = (
                        f"{what} has `default_by` {key!r} but does not reach "
                        f"its dim {subject!r}, so no label of it has a category "
                        f"to default by"
                    )
                    raise ValueError(msg)
                unknown = sorted(set(by) - set(self._categories(subject, prop)))
                if unknown:
                    msg = (
                        f"{what} has `default_by` for {unknown}, which are not "
                        f"categories of {key!r}"
                    )
                    raise ValueError(msg)

    def one_of_properties(self) -> dict[tuple[str, str], str]:
        """Each property `{"one_of": <dim>}`, as `(subject, name)` -> that dim.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
        """
        return {
            (subject, name): spec.dtype.one_of
            for subject, by_name in self.properties.items()
            for name, spec in by_name.items()
            if isinstance(spec.dtype, OneOfDim)
        }

    def _reached_by(self, subject: str) -> frozenset[str]:
        """The dims a property of `subject` reaches: the dim, or those its group draws on."""
        if subject in self.groups:
            return frozenset(self.groups[subject].over.values())
        return frozenset({subject})

    def _check_names(self) -> None:
        """Refuse every declared name that cannot be a column of its own.

        A group's name is a column too, its struct column, and so is a
        property's, in its subject's file only. Two spellings of one name
        differing only in case are refused record-wide, DuckDB resolving both
        to one column; one spelling declared twice is left to the rules that
        govern it - except a property named like a column of its subject's file
        or a coordinate of its group.

        Notes
        -----
        - [names](https://energy-models.github.io/datarecord/design/schema/#names)
        - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
        """
        declared = [
            *(("dim", d) for d in self.dimensions),
            *(("group", g) for g in self.groups),
            *(
                (f"coordinate of group {g!r}", c)
                for g, spec in self.groups.items()
                for c in spec.over
            ),
            *(("attribute", a) for a in self.attributes),
            *(
                (f"property of {s!r}", p)
                for s, specs in self.properties.items()
                for p in specs
            ),
        ]
        problems = []
        for kind, name in declared:
            why = _misnamed(name)
            if why is not None:
                problems.append(f"{kind} {name!r} {why}")
        for subject, specs in self.properties.items():
            keyed = sorted(set(specs) & self._key_of(subject))
            if keyed:
                msg = (
                    f"properties {keyed} of {subject!r} are named like a column "
                    f"of its key, which they would collide with - rename the "
                    f"properties"
                )
                raise ValueError(msg)
            group = self.groups.get(subject)
            shared = sorted(set(specs) & set(group.over if group else ()))
            if shared:
                msg = (
                    f"properties {shared} of group {subject!r} share their names "
                    f"with its coordinates, which name a tuple's fields - "
                    f"rename the properties"
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

    def _key_of(self, subject: str) -> set[str]:
        """The columns of `subject`'s file a property cannot be named: the dim, or the group's struct column."""
        return {subject}

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
        spec = self.attributes.get(attribute)
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

    def group_type(self, group: str) -> DuckDBPyType:
        """The struct a row carries `group` as: a field per key coordinate.

        Each field typed as the dim the coordinate draws on.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        spec = self.groups[group]
        return duckdb.struct_type(
            {c: self.duck_type(self.dimensions[spec.over[c]].dtype) for c in spec.key}
        )

    def duck_type(
        self, dtype: DimensionDtype | ParameterDtype | OneOf | OneOfDim
    ) -> DuckDBPyType:
        """The DuckDB type a declared `dtype` maps to.

        A closed dim's is the `ENUM` of its labels, and a property naming a dim
        takes that dim's.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
        """
        if isinstance(dtype, OneOf):
            return enum_type(dtype.one_of)
        if isinstance(dtype, OneOfDim):
            return self.duck_type(self.dimensions[dtype.one_of].dtype)
        return DUCK_TYPES[dtype]

    def check_group_columns(
        self,
        groups: Iterable[str],
        rel: DuckDBPyRelation,
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
        types = dict(zip(rel.columns, rel.types, strict=True))
        for group in groups:
            key = self.group_key(group)
            flat = [c for c in key if c in types]
            dtype = types.get(group)
            fields = (
                key
                if dtype is None
                else [name for name, _ in dtype.children]
                if dtype.id == "struct"
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
            some = " OR ".join(
                f"{fn.struct_extract(col(group), lit(c))} IS NULL" for c in key
            )
            partly = sql(f"{col(group)} IS NOT NULL AND ({some})")
            if rel.filter(partly).limit(1).fetchone() is not None:
                msg = (
                    f"{carrier} carries a {group!r} with some fields NULL; a "
                    f"group value is a whole tuple, or NULL for every tuple (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)"
                )
                raise ValueError(msg)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """One attribute's full long column set, in order.

        An attribute carries the coordinates its `dims` name and no others, so a
        per-snapshot weighting has no column for any other dim.

        An undeclared attribute is `long_columns` - every
        declared dim, the widest shape. That is a schema with no manifest yet,
        every declared attribute having its own coordinates.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        if self.attributes.get(attribute) is None:
            return self.long_columns
        return (*self.coordinates_of(attribute), *LONG_TAIL)

    def categorizing(self, dim: str) -> tuple[str, ...]:
        """The properties of `dim` naming a closed dim, if `partial` lists it: what `default_by` keys on.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/design/schema/#categories-labels-of-a-closed-dim)
        """
        if dim not in self.dimensions or dim not in (self.partial or set()):
            return ()
        return tuple(
            name
            for name, spec in self.properties.get(dim, {}).items()
            if isinstance(spec.dtype, OneOfDim)
            and isinstance(self.dimensions[spec.dtype.one_of].dtype, OneOf)
        )

    def _categories(self, dim: str, name: str) -> tuple[str, ...]:
        """The labels the closed dim property `name` of `dim` names lists; empty for any other property."""
        return self._listed(self.properties[dim][name].dtype) or ()

    def _listed(self, dtype: Any) -> tuple[str, ...] | None:
        """The labels a closed dim lists, for its own dtype or a property's naming it; else None."""
        if isinstance(dtype, OneOfDim):
            dtype = self.dimensions[dtype.one_of].dtype
        return dtype.one_of if isinstance(dtype, OneOf) else None

    def require_categorizing(self, subject: str, keys: Iterable[str]) -> None:
        """Refuse a key that is not a categorizing property of `subject`.

        Raises
        ------
        ValueError
            Naming the keys and `subject`'s categorizing properties.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/design/schema/#categories-labels-of-a-closed-dim)
        """
        allowed = self.categorizing(subject)
        unknown = sorted(set(keys) - set(allowed))
        if unknown:
            msg = (
                f"{unknown} do not categorize {subject!r}; its categorizing "
                f"properties are {list(allowed)}"
            )
            raise ValueError(msg)

    def require_listed(
        self, subject: str, data: Mapping[str, Iterable[Any]], what: str
    ) -> None:
        """Refuse a label of a closed dim it does not list.

        `data` maps names to values: `subject` itself where it is a closed dim,
        or a property of `subject` naming one. Any other name is skipped, as
        is a NULL.

        Raises
        ------
        ValueError
            Naming `what`, the column, the unlisted values and the labels listed.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
        """
        specs = self.properties.get(subject, {})
        for name, values in data.items():
            if name in specs:
                listed = self._listed(specs[name].dtype)
            elif name == subject and subject in self.dimensions:
                listed = self._listed(self.dimensions[subject].dtype)
            else:
                continue
            if listed is None:
                continue
            unknown = sorted({str(v) for v in values if v is not None} - {*listed})
            if unknown:
                msg = (
                    f"{what} gives {name!r} labels {unknown}, which its closed dim "
                    f"does not list; it lists {list(listed)}"
                )
                raise ValueError(msg)

    def default_for(self, name: str, **categories: str) -> Any:
        """The default of attribute or `subject.name` property `name` for a label of `categories`.

        `categories` are keyed by each categorizing property's own name. The
        first `default_by` key listing the label's category decides, else
        `default`; a category left out matches none.

        Raises
        ------
        KeyError
            As `resolve` does.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
        - [default_by](https://energy-models.github.io/datarecord/design/schema/#default_by-a-default-per-category)
        """
        resolved = self.resolve(name)
        spec = resolved[1] if isinstance(resolved, tuple) else resolved
        for key, by in spec.default_by.items():
            category = categories.get(key.partition(".")[2])
            if category in by:
                return by[category]
        return spec.default

    def addresses(self, attribute: str, dim: str) -> bool:
        """Whether `attribute` reaches a label of `dim` at all.

        True where its `dims` name `dim`, or a group one of whose coordinates
        draws on `dim`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        spec = self.attributes.get(attribute)
        return (spec is not None and dim in spec.dims) or any(
            dim in self.groups[g].over.values() for g in self.groups_of(attribute)
        )

    def resolve(self, name: str) -> AttributeSpec | tuple[str, PropertySpec]:
        """What `name` addresses: an attribute, or a property as `subject.name`.

        Read off the name alone: a plain name is never a property.

        Returns
        -------
        AttributeSpec or tuple of str and PropertySpec
            The attribute's spec, or a property's subject and spec.

        Raises
        ------
        KeyError
            If `name` addresses nothing; for a plain name some subject's
            property is called, naming each dotted form.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/schema/#properties)
        """
        subject, dot, prop = name.partition(".")
        if dot:
            spec = self.properties.get(subject, {}).get(prop)
            if spec is None:
                msg = f"the schema declares no property {name!r}"
                raise KeyError(msg)
            return subject, spec
        attribute = self.attributes.get(name)
        if attribute is not None:
            return attribute
        dotted = sorted(
            f"`{s}.{name}`" for s, specs in self.properties.items() if name in specs
        )
        hint = f"; did you mean {' or '.join(dotted)}" if dotted else ""
        msg = f"the schema declares no attribute {name!r}{hint}"
        raise KeyError(msg)

    def field_over(self, group: str, dim: str) -> str | None:
        """The one field of `group` over `dim`, if any.

        Raises
        ------
        ValueError
            If several fields of `group` are over `dim`.
        """
        fields = [f for f, d in self.groups[group].over.items() if d == dim]
        if len(fields) > 1:
            msg = (
                f"group {group!r} is over {dim!r} through {fields}, so no one "
                f"label of it decides what a tuple carries"
            )
            raise ValueError(msg)
        return fields[0] if fields else None

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
        """Which declared groups address `attribute`, in declaration order.

        An attribute is a connection attribute because its `dims` name the
        `connection` group - not because a separate field says so. That is
        what lets a second group exist without a second field.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/design/schema/#addressing-dims-x)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(g for g in self.groups if g in spec.dims)

    def group_key(self, group: str) -> tuple[str, ...]:
        """One group's key coordinates - its struct column's fields - or `()` if undeclared.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
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

    def column_type(
        self, column: str, subject: str | None = None
    ) -> DuckDBPyType | None:
        """The declared DuckDB type for one column, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims and
        groups, the properties of `subject` - columns of its axis or group
        file, read first - and the owner map's two flag structs, whose fields
        follow the schema's dims.

        No dim is structural: each is declared, and typed from that
        declaration. So a closed dim's `ENUM` pins its vocabulary everywhere
        the column is built. An attribute is no column but a long row's value,
        typed by `value_type`.

        A schema declaring no dims at all is "no manifest yet" rather
        than a record to fold, and DuckDB has no empty struct - so the flag
        columns are undeclared there, and a caller building an empty relation
        falls back to `VARCHAR` for a map that will never hold a row.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
        - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
        """
        found = self.declared(column, subject)
        return None if found is None else found[1]

    def _structural_type(self, column: str) -> DuckDBPyType | None:
        """Return the type the format fixes for `column`: a group struct or a flag struct."""
        if column in STRUCTURAL_TYPES:
            return STRUCTURAL_TYPES[column]
        if column in self.groups:
            return self.group_type(column)
        if column in ("varies", "broadcast"):
            return flag_type(self.broadcast_dims) if self.broadcast_dims else None
        return None

    def declared(
        self, column: str, subject: str | None = None, attribute: str | None = None
    ) -> tuple[str, DuckDBPyType, str] | None:
        """Return what declares `column`, its DuckDB type, and its dtype as an error shows it.

        None where the schema declares no type for `column`.

        Parameters
        ----------
        subject
            The dim or group whose file the column is in, typing its
            properties, named `subject.name`.
        attribute
            The attribute whose long rows the column is in, typing `value`,
            named for the attribute.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
        """
        own = self.properties.get(subject or "", {})
        dtype: DimensionDtype | ParameterDtype | OneOf | OneOfDim
        if attribute is not None and column == "value":
            name, dtype = attribute, self.attributes[attribute].dtype
        elif column in own:
            name, dtype = f"{subject}.{column}", own[column].dtype
        elif column in self.dimensions and column not in STRUCTURAL_TYPES:
            name, dtype = column, self.dimensions[column].dtype
        else:
            duck = self._structural_type(column)
            return None if duck is None else (column, duck, str(duck))
        duck = self.duck_type(dtype)
        shown = (
            dtype.model_dump_json()
            if isinstance(dtype, _Declared)
            else f"{dtype} ({duck})"
        )
        return name, duck, shown

    def require_takes(
        self,
        rel: DuckDBPyRelation,
        what: str,
        subject: str | None = None,
        attribute: str | None = None,
    ) -> DuckDBPyRelation:
        """Return `rel`, refusing a column of a type its declared type does not `take`.

        Reads `rel`'s types, and the rows only of a column so refused: one
        holding no value but NULL passes, cast to its declared type, since a
        dataframe library types an empty or all-None column by guesswork.
        `subject` and `attribute` are as `declared` takes them.

        Raises
        ------
        ValueError
            Naming `what`, each such column, its type, what declares it and
            its dtype.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/schema/#dtypes)
        - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
        """
        wrong = []
        nulls: dict[str, DuckDBPyType] = {}
        for c, given in zip(rel.columns, rel.types, strict=True):
            found = self.declared(c, subject, attribute)
            if found is None or takes(found[1], given):
                continue
            if rel.filter(col(c).isnotnull()).limit(1).fetchone() is None:
                nulls[c] = found[1]
                continue
            name, _, shown = found
            wrong.append(
                f"column {c!r} is {given}, which {name!r} does not take, its "
                f"dtype being {shown}"
            )
        if wrong:
            msg = f"{what}: {'; '.join(wrong)}"
            raise ValueError(msg)
        if not nulls:
            return rel
        return rel.project(
            *(
                col(c).cast(nulls[c]).alias(c) if c in nulls else col(c)
                for c in rel.columns
            )
        )

    def value_type(self, attribute: str) -> DuckDBPyType | None:
        """The `value` column's DuckDB type for one attribute.

        No `ctype`: one attribute is one `attributes/<attr>.parquet` with one
        `value` column, so the dtype is the attribute's alone.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        return None if spec is None else DUCK_TYPES[spec.dtype]

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

        if other.partial is not None and self.partial is not None:
            lost = other.partial - self.partial
            if lost:
                problems.append(
                    f"{sorted(lost)} no longer `partial`; a layer that patched one "
                    f"value along such an axis is now a partial override of an axis "
                    f"owned whole"
                )
        return problems
