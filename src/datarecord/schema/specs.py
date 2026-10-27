# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The declarations a schema is made of: dims, groups, properties and attributes.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
"""

from __future__ import annotations

import math
from typing import Any

from mathspec.program import DimensionDtype, ParameterDtype
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)


class _Declared(BaseModel):
    """Base of the models `manifest.json` is parsed into, which refuse undeclared keys."""

    model_config = ConfigDict(extra="forbid")


class Dimension(_Declared):
    """One axis attribute data may vary over: its shape, not its data.

    Not which dims an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared.

    Attributes
    ----------
    dtype
        The axis labels' type, a mathspec `DimensionDtype`.
    one_of
        The labels the dim allows, in order, or None for any. A dim with
        `one_of` has an `ENUM` of them as its column, and `add` refuses a
        label outside the list. Only a `str` dim takes `one_of`.
    unit
        What this axis's *labels* measure, if anything - `None` is undeclared,
        `""` genuinely dimensionless.
    description
        What the axis is, in prose. Never interpreted.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    - [dimensions](https://energy-models.github.io/datarecord/design/model/dims/)
    - [unit and description](https://energy-models.github.io/datarecord/design/model/schema/#unit-and-description)
    - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
    """

    dtype: DimensionDtype = "str"
    one_of: tuple[str, ...] | None = Field(
        default=None, min_length=1, exclude_if=lambda one_of: one_of is None
    )
    unit: str | None = None
    description: str | None = None

    @field_validator("one_of", mode="before")
    @classmethod
    def _strict_labels(cls, value: Any, info: ValidationInfo) -> Any:
        dtype = info.data.get("dtype", "str")
        if value is not None and dtype != "str":
            msg = (
                f"a dim of dtype {dtype!r} takes no `one_of`; "
                'only a "str" dim takes `one_of`'
            )
            raise ValueError(msg)
        if isinstance(value, (list, tuple)):
            repeated = sorted({label for label in value if value.count(label) > 1})
            if repeated:
                msg = f"`one_of` lists {repeated} more than once; list each label once"
                raise ValueError(msg)
        return value


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
    - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
    - [attributes over a group](https://energy-models.github.io/datarecord/design/model/groups/#attributes-over-a-group)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
    - [unit and description](https://energy-models.github.io/datarecord/design/model/schema/#unit-and-description)
    - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
    """

    dtype: ParameterDtype
    dims: frozenset[str] = frozenset()
    breakpoints: bool = False


class PropertySpec(_ValueSpec):
    """One value per label of a dim, or per tuple of a group, describing it.

    A property is keyed by its subject (`Schema.properties`) and stored as a
    column of `dims/{dim}.parquet`, or of `groups/{group}.parquet` for a group.
    Its name is its subject's alone, so it is addressed as `subject.name`
    (`Schema.resolve`).
    A property declares exactly one of `dtype` and `one_of`. A property of a
    dim in `partial` that refers to a dim with `one_of`
    categorizes its subject (`Schema.categorizing`). A property of a group
    never does.

    Attributes
    ----------
    dtype
        The column's type, a mathspec `ParameterDtype`.
    one_of
        The dim the property refers to. Its values have that
        dim's type, and a stored value that is not NULL must be a live label
        of the dim.
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
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
    """

    dtype: ParameterDtype | None = Field(
        default=None, exclude_if=lambda dtype: dtype is None
    )
    one_of: str | None = Field(default=None, exclude_if=lambda dim: dim is None)

    @model_validator(mode="after")
    def _dtype_or_one_of(self) -> PropertySpec:
        if (self.dtype is None) == (self.one_of is None):
            msg = (
                "a property declares exactly one of `dtype` and `one_of`, "
                f"not {'both' if self.one_of else 'neither'}"
            )
            raise ValueError(msg)
        return self


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
    - [groups](https://energy-models.github.io/datarecord/design/model/groups/)
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
        - [groups](https://energy-models.github.io/datarecord/design/model/groups/)
        """
        return tuple(self.over)
