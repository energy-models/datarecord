# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The declarations a schema is made of: dimensions, properties and attributes.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/reference/schema/)
"""

from __future__ import annotations

import math
from typing import Annotated, Any

from mathspec.program import DimensionDtype, ParameterDtype
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

Names = Annotated[
    frozenset[str], PlainSerializer(sorted, return_type=list[str], when_used="json")
]
"""A set of names, written to JSON sorted, so a schema dumps alike in every process."""


class _Declared(BaseModel):
    """Base of the models `manifest.json` is parsed into: frozen, refusing undeclared keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Dimension(_Declared):
    """One axis attribute data may vary over: its shape, not its data.

    Not which dimensions an *attribute* varies over (`AttributeSpec.dims`), nor the
    patch granularity (`Schema.partial`), nor order - an axis is ordered by its
    file's row order, undeclared.

    A dimension declaring `over` is a multi-dimension: its labels are tuples of other dimensions'
    labels, carried in a row as one struct column named for it, of one field
    per entry of `over` (`Schema.duck_type`). Fields rather than dimensions
    because two of them may draw on one dimension: a corridor between two buses is
    `(from, to)`.

    Attributes
    ----------
    over
        Field name -> the dimension it draws its labels from, or None for a dimension
        that is no multi-dimension. A list is sugar for the dict with identical keys
        and values. Declared without `dtype` and `one_of`.
    dtype
        The axis labels' type, a mathspec `DimensionDtype`; `"str"` where
        undeclared, None on a multi-dimension.
    one_of
        The dimension's labels, in order, or None for labels a layer writes. A
        dimension with `one_of` has its axis derived from the list, no axis file
        and no properties, and an `ENUM` of the labels as its column; `add`
        and `remove` refuse it. Only a `str` dimension takes `one_of`.
    unit
        What this axis's *labels* measure, if anything - `None` is undeclared,
        `""` genuinely dimensionless.
    description
        What the axis is, in prose. Never interpreted.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/reference/data-model/#dimensions-and-labels)
    - [dimensions](https://energy-models.github.io/datarecord/reference/data-model/)
    - [multi-dimensions](https://energy-models.github.io/datarecord/reference/data-model/#multi-dimensions)
    - [unit and description](https://energy-models.github.io/datarecord/reference/schema/#unit-and-description)
    - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
    """

    over: dict[str, str] | None = Field(
        default=None, exclude_if=lambda over: over is None
    )
    dtype: DimensionDtype | None = Field(
        default=None, exclude_if=lambda dtype: dtype is None
    )
    one_of: tuple[str, ...] | None = Field(
        default=None, min_length=1, exclude_if=lambda one_of: one_of is None
    )
    unit: str | None = None
    description: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _over_alone(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("over") is not None:
            typed = sorted({"dtype", "one_of"} & data.keys())
            if typed:
                msg = (
                    f"a dimension declaring `over` is a multi-dimension, whose labels are "
                    f"tuples of its fields' labels, and takes no {typed}"
                )
                raise ValueError(msg)
        return data

    @field_validator("over", mode="before")
    @classmethod
    def _parse_over(cls, value: Any) -> Any:
        """Expand the list form to the dict it is sugar for."""
        if isinstance(value, (list, tuple)):
            return {c: c for c in value}
        return value

    @field_validator("one_of", mode="before")
    @classmethod
    def _strict_labels(cls, value: Any, info: ValidationInfo) -> Any:
        dtype = info.data.get("dtype") or "str"
        if value is not None and dtype != "str":
            msg = (
                f"a dimension of dtype {dtype!r} takes no `one_of`; "
                'only a "str" dimension takes `one_of`'
            )
            raise ValueError(msg)
        if isinstance(value, (list, tuple)):
            repeated = sorted({label for label in value if value.count(label) > 1})
            if repeated:
                msg = f"`one_of` lists {repeated} more than once; list each label once"
                raise ValueError(msg)
        return value

    @model_validator(mode="before")
    @classmethod
    def _default_dtype(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and data.get("over") is None
            and data.get("dtype") is None
        ):
            return {**data, "dtype": "str"}
        return data


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
    dimensions
        Dimensions this attribute may vary over; a subset
        of those declared. Whatever they are, its values are long rows of
        `attributes/<attr>.parquet`.
    default
        The value at a coordinate no row covers.
    default_by
        A categorizing property, as `subject.name`, -> category -> the default
        for labels of that category, overriding `default`; the first key, in
        declaration order, listing a label's category wins. Each key must
        categorize a dimension the attribute reaches. Read through `Schema.default_for`; never applied by
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
    - [wide and long rows](https://energy-models.github.io/datarecord/reference/data-model/#wide-and-long-rows)
    - [attributes over a multi-dimension](https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dimensions-column)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    - [AttributeSpec](https://energy-models.github.io/datarecord/reference/schema/#attributes)
    - [unit and description](https://energy-models.github.io/datarecord/reference/schema/#unit-and-description)
    - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
    """

    dtype: ParameterDtype
    dims: Names = frozenset()
    breakpoints: bool = False


class PropertySpec(_ValueSpec):
    """One value per label of a dimension, describing it.

    A property is keyed by its subject (`Schema.properties`) and stored as a
    column of `dimensions/{dim}.parquet`.
    Its name is its subject's alone, so it is addressed as `subject.name`
    (`Schema.resolve`).
    A property declares exactly one of `dtype` and `refers`. A property of a
    dimension in `partial` that refers to a dimension with
    `one_of` categorizes its subject (`Schema.categorizing`).

    Attributes
    ----------
    dtype
        The column's type, a mathspec `ParameterDtype`.
    refers
        The dimension the property refers to. Its values have that
        dimension's type, and a stored value that is not NULL must be a live label
        of the dimension.
    default
        The value a label no row sets takes.
    default_by
        A categorizing property of a dimension the subject reaches, as
        `subject.name`, -> category -> the default for labels of that category,
        overriding `default`. Refused on a categorizing property itself. Read
        through `Schema.default_for`.
    unit
        What the values measure; `None` is undeclared.
    description
        What the property is, in prose. Never interpreted.

    Notes
    -----
    - [the schema](https://energy-models.github.io/datarecord/reference/schema/)
    - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
    """

    dtype: ParameterDtype | None = Field(
        default=None, exclude_if=lambda dtype: dtype is None
    )
    refers: str | None = Field(default=None, exclude_if=lambda dim: dim is None)

    @model_validator(mode="after")
    def _dtype_or_refers(self) -> PropertySpec:
        if (self.dtype is None) == (self.refers is None):
            msg = (
                "a property declares exactly one of `dtype` and `refers`, "
                f"not {'both' if self.refers else 'neither'}"
            )
            raise ValueError(msg)
        return self
