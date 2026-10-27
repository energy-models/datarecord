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
- [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
- [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, cast
from uuid import UUID

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyRelation, sqltypes
from duckdb import SQLExpression as sql
from duckdb.sqltypes import DuckDBPyType
from pydantic import Field, model_validator

from datarecord.duck import fn
from datarecord.schema.dtypes import DUCK_TYPES, enum_type, takes
from datarecord.schema.specs import (
    AttributeSpec,
    Dimension,
    PropertySpec,
    _Declared,
    _ValueSpec,
)

# Columns the format fixes, whatever the schema declares (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema). Not the
# dims or properties: each is declared and typed from that declaration, which is
# what lets the `ENUM` of a dim with `one_of` pin them. These are the ones no schema
# names - `breakpoint` is NULL for an ordinary scalar, so one column set serves
# every row.
STRUCTURAL_TYPES = {
    "attribute": sqltypes.VARCHAR,
    "breakpoint": sqltypes.DOUBLE,
    "breakpoints": sqltypes.BOOLEAN,
}


# Every long row's trailing columns, whatever coordinates precede them: the
# attribute named, the abscissa of a piecewise-linear value, and the value
# (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema).
LONG_TAIL = ("attribute", "breakpoint", "value")


# The owner map's flag columns: two structs with a field per declared dim, so
# the map's column set does not depend on the schema and adding a dim stays the
# compatible change versioning calls it. `breakpoints` is outside both, being no dim
# (https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map, https://energy-models.github.io/datarecord/design/use/reading/#flags).
FLAG_COLUMNS = ("varies", "broadcast", "breakpoints")


LAYER_COLUMN = "layer_uuid"
"""The owner map's column naming the layer each row's value is read from.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
"""


DERIVED = ("order_key",)
"""Columns a resolved frame carries that no layer file may.

The fold's answer *about* a frame rather than data in it, so writing one would
both put a column in a file the format does not define and read as stored order
where the fold always re-derives it from file order.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
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
- [names](https://energy-models.github.io/datarecord/design/model/schema/#names)
"""


def _misnamed(name: str) -> str | None:
    """Why `name` cannot be a declared dim, coordinate or attribute, if so.

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
    - [dimensions](https://energy-models.github.io/datarecord/design/model/dims/)
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    """
    return duckdb.struct_type(dict.fromkeys(dims, sqltypes.BOOLEAN))


def _typed_as(spec: Dimension | AttributeSpec | PropertySpec) -> str:
    """Show how `spec` declares its type: its dtype, or the fields its manifest writes."""
    declared = spec.model_dump(include={"dtype", "one_of", "over"})
    return (
        str(declared["dtype"]) if declared.keys() == {"dtype"} else json.dumps(declared)
    )


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
    properties
        Subject dim -> property name -> spec: the columns of
        `dims/{dim}.parquet` that describe its labels. A name is unique
        within its subject only, addressed as `subject.name`. A property of a
        dim in `partial` that refers to a dim with
        `one_of` categorizes its subject. A dim with `one_of` has none.
    attributes
        Attribute -> spec, record-wide. One attribute is one spec and
        one `attributes/<attr>.parquet` with one dtype.
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
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
    """

    id: UUID | None = Field(default=None, exclude_if=lambda id: id is None)
    version: int = 1
    dimensions: dict[str, Dimension] = Field(default_factory=dict)
    attributes: dict[str, AttributeSpec] = Field(default_factory=dict)
    properties: dict[str, dict[str, PropertySpec]] = Field(default_factory=dict)
    partial: frozenset[str] | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/design/model/dims/)
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        - [names](https://energy-models.github.io/datarecord/design/model/schema/#names)
        """
        self._check_names()
        declared = set(self.dimensions)

        # Attributes but no axes is a table, not a record (https://energy-models.github.io/datarecord/design/model/dims/). Rejected here
        # so the owner map never needs a struct with no fields, which DuckDB has
        # no type for. A wholly empty `Schema()` stays legal: "no manifest yet".
        if self.attributes and not declared:
            msg = (
                "a schema declaring attributes must declare at least one dim; "
                "attribute data varying over no axis is not a record (https://energy-models.github.io/datarecord/design/model/dims/)"
            )
            raise ValueError(msg)

        for multi in self.multi_dims:
            drawn = set(self.over(multi).values())
            unknown = sorted(drawn - declared)
            if unknown:
                msg = f"multi-dim {multi!r} is over undeclared dims {unknown}"
                raise ValueError(msg)
            nested = sorted(drawn & set(self.multi_dims))
            if nested:
                msg = (
                    f"multi-dim {multi!r} is over the multi-dims {nested}; each "
                    f"coordinate draws on a dim that is no multi-dim "
                    f"(https://energy-models.github.io/datarecord/design/model/multi-dims/)"
                )
                raise ValueError(msg)

        self._check_properties()

        for attr, attr_spec in self.attributes.items():
            unknown = sorted(attr_spec.dims - declared)
            if unknown:
                msg = f"attribute {attr!r} is addressed by undeclared dims {unknown}"
                raise ValueError(msg)
            for multi in self.multi_dims_of(attr):
                twice = sorted(set(self.over(multi)) & attr_spec.dims)
                if twice:
                    msg = (
                        f"attribute {attr!r} is addressed by the multi-dim "
                        f"{multi!r} and by {twice}, which share a name with "
                        f"the multi-dim's coordinates, so a keyword such as "
                        f"{twice[0]}= would address both. Name the multi-dim's "
                        f"coordinate differently, e.g. over={{..., 'port': "
                        f"{twice[0]!r}}} (https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)"
                    )
                    raise ValueError(msg)

        if self.partial is not None:
            unknown = sorted(self.partial - declared)
            if unknown:
                msg = f"`partial` names undeclared dims {unknown}"
                raise ValueError(msg)
        return self

    def _categorized_by(self, name: str, what: str) -> tuple[str, str]:
        """Split the `default_by` key `name` into a dim and one of its categorizing properties.

        `what` names the declaration holding the key, for the error.

        Raises
        ------
        ValueError
            If `name` is not `subject.name` of a categorizing property.
        """
        subject, _, prop = name.partition(".")
        coordinate = prop in self.over(subject) if subject in self.dimensions else False
        if coordinate:
            reason = f"is a coordinate of the multi-dim {subject!r}, part of its key"
        elif prop not in self.properties.get(subject, {}):
            reason = "is no property, named `subject.name`"
        elif (one_of := self.properties[subject][prop].one_of) is None:
            reason = "refers to no dim"
        elif self.dimensions[one_of].one_of is None:
            reason = f"refers to {one_of!r}, but {one_of!r} is not a one_of dim"
        elif subject not in (self.partial or set()):
            reason = f"is a property of {subject!r}, which `partial` does not list"
        else:
            return subject, prop
        msg = f"{what} is keyed by {name!r}, which {reason}"
        raise ValueError(msg)

    def _check_properties(self) -> None:
        """Refuse a property of an undeclared subject, one that refers to an undeclared dim, and a malformed `default_by`."""
        unknown = sorted(set(self.properties) - set(self.dimensions))
        if unknown:
            msg = (
                f"properties are declared on undeclared dims {unknown}; "
                f"a property is a column of its subject's axis file"
            )
            raise ValueError(msg)
        on_one_of = sorted(set(self.properties) & set(self.one_of_dims))
        if on_one_of:
            msg = (
                f"properties are declared on {on_one_of}, which list their labels "
                f"with `one_of`; such a dim's axis is the schema's list, with "
                f"no axis file to hold a property "
                f"(https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)"
            )
            raise ValueError(msg)
        for (subject, name), dim in self.one_of_properties().items():
            if dim not in self.dimensions:
                msg = (
                    f"property {subject}.{name} refers to {dim!r}, "
                    f"an undeclared dim "
                    f"(https://energy-models.github.io/datarecord/design/model/properties/#properties)"
                )
                raise ValueError(msg)
            if dim in self.multi_dims:
                msg = (
                    f"property {subject}.{name} refers to the multi-dim {dim!r}; "
                    f"a property refers to a dim that is no multi-dim "
                    f"(https://energy-models.github.io/datarecord/design/model/properties/#referring-to-a-dim)"
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
        """Map each property that refers to a dim, as `(subject, name)`, to that dim.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
        """
        return {
            (subject, name): spec.one_of
            for subject, by_name in self.properties.items()
            for name, spec in by_name.items()
            if spec.one_of is not None
        }

    def _reached_by(self, subject: str) -> frozenset[str]:
        """The dims a property of `subject` reaches: the dim, and those its coordinates draw on."""
        return frozenset({subject, *self.over(subject).values()})

    def _check_names(self) -> None:
        """Refuse every declared name that cannot be a column of its own.

        A multi-dim's coordinate is a column too, a field of its struct column,
        and so is a property's, in its subject's file only. Two spellings of one
        name differing only in case are refused record-wide, DuckDB resolving
        both to one column; one spelling declared twice is left to the rules
        that govern it - except a property named like a column of its subject's
        file or a coordinate of its multi-dim.

        Notes
        -----
        - [names](https://energy-models.github.io/datarecord/design/model/schema/#names)
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
        """
        declared = [
            *(("dim", d) for d in self.dimensions),
            *(
                (f"coordinate of multi-dim {m!r}", c)
                for m in self.multi_dims
                for c in self.over(m)
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
            coordinates = self.over(subject) if subject in self.dimensions else {}
            shared = sorted(set(specs) & set(coordinates))
            if shared:
                msg = (
                    f"properties {shared} of multi-dim {subject!r} share their "
                    f"names with its coordinates, which name a tuple's fields - "
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
                f"with `_` (https://energy-models.github.io/datarecord/design/model/schema/#names)"
            )
            raise ValueError(msg)

    def _key_of(self, subject: str) -> set[str]:
        """Return the key column of the file of `subject`, a name no property of it may take.

        That is the dim's own column, a multi-dim's struct column.
        """
        return {subject}

    # -- derived key sets (https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override) --------------------------------------

    @property
    def dims(self) -> tuple[str, ...]:
        """Every declared dim but the multi-dims, in declaration order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.over is None)

    @property
    def multi_dims(self) -> tuple[str, ...]:
        """Every declared multi-dim, in declaration order, each carried in a row as one struct column named for it.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.over is not None)

    @property
    def one_of_dims(self) -> tuple[str, ...]:
        """Every dim with `one_of`, in declaration order: those whose axis is the schema's list.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.one_of)

    def over(self, dim: str) -> dict[str, str]:
        """The coordinates of `dim`, each to the dim it draws on, in field order; empty for a dim that is no multi-dim.

        Raises
        ------
        KeyError
            If the schema declares no `dim`.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        """
        return self.dimensions[dim].over or {}

    @property
    def broadcast_dims(self) -> tuple[str, ...]:
        """The coordinates a NULL broadcasts over: every declared dim, the multi-dims last.

        A NULL here means "every value of this dim", which the fold expands
        against the axis; a NULL struct means every tuple of the resolved
        multi-dim.

        What the `varies`/`broadcast` structs have a field per, and what
        `expand_dims` joins.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        """
        return (*self.dims, *self.multi_dims)

    def coordinates_of(self, attribute: str) -> tuple[str, ...]:
        """The coordinate columns one attribute's rows carry, multi-dims first.

        A multi-dim in `dims` is carried as one struct column named for it, so
        `dims={"connection", "snapshot"}` gives `("connection", "snapshot")`.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL column of a dim the attribute
        is not addressed by would claim a coordinate the value has none of.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        - [addressing](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
        - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return (
            *self.multi_dims_of(attribute),
            *(d for d in self.dims if d in spec.dims),
        )

    def duck_type(self, spec: Dimension | AttributeSpec | PropertySpec) -> DuckDBPyType:
        """Return the DuckDB type of a declared dim, attribute or property.

        A dim with `one_of` has the `ENUM` of them, a multi-dim the struct of
        a field per coordinate, typed as the dim it draws on. A property that
        refers to a dim has the type of that dim.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        """
        if isinstance(spec, PropertySpec) and spec.one_of is not None:
            return self.duck_type(self.dimensions[spec.one_of])
        if isinstance(spec, Dimension) and spec.over is not None:
            return duckdb.struct_type(
                {c: self.duck_type(self.dimensions[d]) for c, d in spec.over.items()}
            )
        if isinstance(spec, Dimension) and spec.one_of is not None:
            return enum_type(spec.one_of)
        return DUCK_TYPES[cast("str", spec.dtype)]

    def check_multi_dim_columns(
        self,
        multi_dims: Iterable[str],
        rel: DuckDBPyRelation,
        *,
        carrier: str,
    ) -> None:
        """Reject each of `multi_dims` carried other than as one struct column of exactly its coordinates, whole or NULL.

        A carrier without a multi-dim's column passes.

        Parameters
        ----------
        carrier
            What carries the columns, named in the message.

        Raises
        ------
        ValueError
            On a coordinate as a column of its own, a multi-dim column not a
            struct of exactly the coordinates, naming the struct to pass
            instead, or a struct with some fields NULL and not all.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
        """
        types = dict(zip(rel.columns, rel.types, strict=True))
        for multi in multi_dims:
            key = list(self.over(multi))
            flat = [c for c in key if c in types]
            dtype = types.get(multi)
            fields = (
                key
                if dtype is None
                else [name for name, _ in dtype.children]
                if dtype.id == "struct"
                else None
            )
            if flat or fields is None or set(fields) != set(key):
                carried = f"{flat} flat" if flat else f"{multi!r} as {dtype}"
                msg = (
                    f"{carrier} carries {carried}; pass {multi!r} as one struct "
                    f"column with a field per coordinate, {key} (https://energy-models.github.io/datarecord/design/model/multi-dims/)"
                )
                raise ValueError(msg)
            if dtype is None:
                continue
            some = " OR ".join(
                f"{fn.struct_extract(col(multi), lit(c))} IS NULL" for c in key
            )
            partly = sql(f"{col(multi)} IS NOT NULL AND ({some})")
            if rel.filter(partly).limit(1).fetchone() is not None:
                msg = (
                    f"{carrier} carries a {multi!r} with some fields NULL; a "
                    f"multi-dim value is a whole tuple, or NULL for every tuple (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)"
                )
                raise ValueError(msg)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """Return one attribute's full long column set, in order.

        An attribute carries the coordinates its `dims` name and no others, so a
        per-snapshot weighting has no column for any other dim.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
        """
        if attribute not in self.attributes:
            msg = f"the schema declares no attribute {attribute!r}"
            raise KeyError(msg)
        return (*self.coordinates_of(attribute), *LONG_TAIL)

    def categorizing(self, dim: str) -> tuple[str, ...]:
        """Return the categorizing properties of `dim`, the ones `default_by` keys on.

        A categorizing property is a property of `dim` that refers to a dim with
        `one_of`. Only a dim in `partial` has any.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
        """
        if dim not in (self.partial or set()):
            return ()
        return tuple(
            name
            for name, spec in self.properties.get(dim, {}).items()
            if spec.one_of is not None
            and self.dimensions[spec.one_of].one_of is not None
        )

    def _categories(self, dim: str, name: str) -> tuple[str, ...]:
        """Return the categories property `name` of `dim` can hold.

        These are the `one_of` labels of the dim the property refers to; there are none if that dim has no `one_of`.
        """
        return self._one_of_labels(self.properties[dim][name]) or ()

    def _one_of_labels(self, spec: Dimension | PropertySpec) -> tuple[str, ...] | None:
        """Return the `one_of` labels of a dim, or None if it has none.

        `spec` is the dim, or a property that refers to the dim.
        """
        if isinstance(spec, PropertySpec):
            if spec.one_of is None:
                return None
            spec = self.dimensions[spec.one_of]
        return spec.one_of

    def require_categorizing(self, subject: str, keys: Iterable[str]) -> None:
        """Refuse a key that is not a categorizing property of `subject`.

        Raises
        ------
        ValueError
            Naming the keys and `subject`'s categorizing properties.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/design/model/properties/#categories)
        """
        allowed = self.categorizing(subject)
        unknown = sorted(set(keys) - set(allowed))
        if unknown:
            msg = (
                f"{unknown} do not categorize {subject!r}; its categorizing "
                f"properties are {list(allowed)}"
            )
            raise ValueError(msg)

    def require_one_of(
        self, subject: str, data: Mapping[str, Iterable[Any]], what: str
    ) -> None:
        """Refuse a label its dim does not list.

        `data` maps column names to values. A name is checked where it is
        `subject` and `subject` has `one_of`, or where it is a property of
        `subject` that refers to a dim with `one_of`. Other names
        and NULL values are skipped.

        Raises
        ------
        ValueError
            Naming `what`, the column, the unknown labels and the dim's
            `one_of` labels.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
        """
        specs = self.properties.get(subject, {})
        for name, values in data.items():
            if name in specs:
                labels = self._one_of_labels(specs[name])
            elif name == subject and subject in self.dimensions:
                labels = self._one_of_labels(self.dimensions[subject])
            else:
                continue
            if labels is None:
                continue
            unknown = sorted({str(v) for v in values if v is not None} - {*labels})
            if unknown:
                msg = (
                    f"{what} gives {name!r} labels {unknown}, which its dim's "
                    f"`one_of` does not hold; `one_of` is {list(labels)}"
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
            As `resolve` does, and on a coordinate, which has no default.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
        - [default_by](https://energy-models.github.io/datarecord/design/model/properties/#default_by-a-default-per-category)
        """
        resolved = self.resolve(name)
        spec = resolved[1] if isinstance(resolved, tuple) else resolved
        if isinstance(spec, Dimension):
            msg = f"{name!r} is a coordinate, part of its multi-dim's key, and has no default"
            raise KeyError(msg)
        for key, by in spec.default_by.items():
            category = categories.get(key.partition(".")[2])
            if category in by:
                return by[category]
        return spec.default

    def addresses(self, attribute: str, dim: str) -> bool:
        """Whether `attribute` reaches a label of `dim` at all.

        True where its `dims` name `dim`, or a multi-dim one of whose
        coordinates draws on `dim`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
        """
        spec = self.attributes.get(attribute)
        return (spec is not None and dim in spec.dims) or any(
            dim in self.over(m).values() for m in self.multi_dims_of(attribute)
        )

    def resolve(
        self, name: str
    ) -> AttributeSpec | tuple[str, PropertySpec] | tuple[str, Dimension]:
        """What `name` addresses: an attribute, or a property or coordinate as `subject.name`.

        Read off the name alone: a plain name is never a property.

        Returns
        -------
        AttributeSpec or tuple of str and PropertySpec or Dimension
            The attribute's spec, a property's subject and spec, or a
            coordinate's multi-dim and the dim the coordinate draws on.

        Raises
        ------
        KeyError
            If `name` addresses nothing; for a plain name some subject's
            property is called, naming each dotted form.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/design/model/properties/#properties)
        - [coordinates](https://energy-models.github.io/datarecord/design/model/multi-dims/#coordinates)
        """
        subject, dot, prop = name.partition(".")
        if dot:
            spec = self.properties.get(subject, {}).get(prop)
            if spec is not None:
                return subject, spec
            drawn = self.over(subject).get(prop) if subject in self.dimensions else None
            if drawn is None:
                msg = f"the schema declares no property {name!r}"
                raise KeyError(msg)
            return subject, self.dimensions[drawn]
        attribute = self.attributes.get(name)
        if attribute is not None:
            return attribute
        dotted = sorted(
            f"`{s}.{name}`" for s, specs in self.properties.items() if name in specs
        )
        hint = f"; did you mean {' or '.join(dotted)}" if dotted else ""
        msg = f"the schema declares no attribute {name!r}{hint}"
        raise KeyError(msg)

    def coordinate_over(self, multi: str, dim: str) -> str | None:
        """The one coordinate of the multi-dim `multi` drawing on `dim`, if any.

        Raises
        ------
        ValueError
            If several coordinates of `multi` draw on `dim`.
        """
        fields = [f for f, d in self.over(multi).items() if d == dim]
        if len(fields) > 1:
            msg = (
                f"multi-dim {multi!r} is over {dim!r} through {fields}, so no one "
                f"label of it decides what a tuple carries"
            )
            raise ValueError(msg)
        return fields[0] if fields else None

    def owned_per(self, attribute: str) -> frozenset[str]:
        """Which dims a layer owns `attribute` per.

        Derived rather than declared: `AttributeSpec.dims` says which axes the
        attribute may vary over, `partial_dims` the fold key (the `partial`
        dims), and ownership is their intersection. A
        dim in `dims` but not the fold key - a non-`partial` value axis like
        `timestep` - is owned whole, so a patch to one of its values restates the
        attribute's entire extent along it (`_owned_whole`).

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return frozenset()
        return spec.dims & set(self.partial_dims)

    @property
    def partial_dims(self) -> tuple[str, ...]:
        """The fold key's coordinates, in declaration order.

        The dims a layer may patch per value (`partial`). The fold's
        key is one fixed tuple over all attributes, so it carries every axis *any* layer may patch by value or
        by row, not only those some currently declared attribute varies over. An
        attribute not owned per one of them writes NULL there, the "NULL means
        all values" rule - which also lets a schema declare an axis before any
        attribute uses it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        - [one fold for every axis](https://energy-models.github.io/datarecord/design/layers/resolution/#one-fold-for-every-axis)
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        partial = self.partial or frozenset()
        return tuple(d for d in self.broadcast_dims if d in partial)

    # -- key and column sets (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema, https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map) -----------------------------------

    @property
    def long_columns(self) -> tuple[str, ...]:
        """The long schema's full column set.

        The *map's* column set, which is uniform across attributes because the
        map is one relation over all of them. An individual file carries only
        its own attribute's columns (`long_columns_for`), and `union_by_name`
        supplies NULL for the rest when the fold unions them here.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
        """
        return (*self.multi_dims, *self.dims, *LONG_TAIL)

    @property
    def owner_map_key(self) -> tuple[str, ...]:
        """Owner-map key columns, compared NULL-safely when folding.

        `partial_dims`, plus `attribute`: a layer may patch one value along a
        `partial` dim without restating every other's.

        A coordinate an attribute's own file does not carry reads as NULL,
        which is what makes the key one fixed tuple over attributes whose
        columns differ.

        Notes
        -----
        - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return (*self.partial_dims, "attribute")

    def multi_dims_of(self, attribute: str) -> tuple[str, ...]:
        """Which declared multi-dims address `attribute`, in declaration order.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return ()
        return tuple(m for m in self.multi_dims if m in spec.dims)

    @property
    def owner_map_columns(self) -> tuple[str, ...]:
        """The owner map's full column set.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        return (*self.owner_map_key, LAYER_COLUMN, *FLAG_COLUMNS)

    # -- typing (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema, https://energy-models.github.io/datarecord/design/use/writing/) -------------------------------------------------

    def column_type(
        self, column: str, subject: str | None = None
    ) -> DuckDBPyType | None:
        """Return the declared DuckDB type of `column`, or None if the schema declares none.

        Covers the structural columns the format fixes, the declared dims,
        the properties of `subject` - columns of its
        axis file, read first - and the owner map's two flag structs, whose fields
        follow the schema's dims.

        No dim is structural: each is declared, and typed from that
        declaration. So the `ENUM` of a dim with `one_of` pins them
        everywhere the column is built. An attribute is no column but a long row's value,
        typed by `value_type`.

        A schema declaring no dims at all is "no manifest yet" rather
        than a record to fold, and DuckDB has no empty struct - so the flag
        columns are undeclared there, and a caller building an empty relation
        falls back to `VARCHAR` for a map that will never hold a row.

        Notes
        -----
        - [one schema per record](https://energy-models.github.io/datarecord/design/model/schema/#one-schema-per-record)
        - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
        """
        found = self.declared(column, subject)
        return None if found is None else found[1]

    def _structural_type(self, column: str) -> DuckDBPyType | None:
        """Return the type the format fixes for `column`: a structural column or a flag struct."""
        if column in STRUCTURAL_TYPES:
            return STRUCTURAL_TYPES[column]
        if column in ("varies", "broadcast"):
            return flag_type(self.broadcast_dims) if self.broadcast_dims else None
        return None

    def declared(
        self, column: str, subject: str | None = None, attribute: str | None = None
    ) -> tuple[str, DuckDBPyType, str] | None:
        """Look up the declaration of `column`.

        Parameters
        ----------
        subject
            The dim whose file holds the column. A column named like
            one of its properties is that property, named `subject.name`.
        attribute
            The attribute whose long rows hold the column. Its `value` column
            is the attribute, named as it is.

        Returns
        -------
        tuple of str, DuckDBPyType and str, or None
            The declared name, the DuckDB type, and the dtype as an error
            message shows it; None where the schema declares no type for
            `column`.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
        """
        own = self.properties.get(subject or "", {})
        spec: Dimension | AttributeSpec | PropertySpec
        if attribute is not None and column == "value":
            name, spec = attribute, self.attributes[attribute]
        elif column in own:
            name, spec = f"{subject}.{column}", own[column]
        elif column in self.dimensions and column not in STRUCTURAL_TYPES:
            name, spec = column, self.dimensions[column]
        else:
            duck = self._structural_type(column)
            return None if duck is None else (column, duck, str(duck))
        duck = self.duck_type(spec)
        return name, duck, f"{_typed_as(spec)} ({duck})"

    def require_takes(
        self,
        rel: DuckDBPyRelation,
        what: str,
        subject: str | None = None,
        attribute: str | None = None,
    ) -> DuckDBPyRelation:
        """Check that each column of `rel` has a type that fits its declaration, and return `rel`, with its all-NULL columns cast to their declared types.

        A column type fits where `takes` says so. A column
        that fails this but holds only NULLs passes and is cast to its declared
        type, because dataframe libraries guess the type of an empty or
        all-None column. Only such columns have their rows read. `subject` and
        `attribute` are as in `declared`.

        Raises
        ------
        ValueError
            Naming `what`, and for each refused column its type, its
            declared name and its dtype.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/design/model/schema/#dtypes)
        - [set](https://energy-models.github.io/datarecord/design/use/editing/#set)
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
                f"column {c!r} has type {given}, which does not fit {name!r} "
                f"of dtype {shown}"
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
        """Return the DuckDB type of the `value` column of `attribute`.

        One attribute is one `attributes/<attr>.parquet` with one `value`
        column, so the dtype is the attribute's alone.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
        """
        spec = self.attributes.get(attribute)
        return None if spec is None else DUCK_TYPES[spec.dtype]

    # -- versioning (https://energy-models.github.io/datarecord/design/model/schema/#versioning) --------------------------------------------------

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
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
        - [versioning](https://energy-models.github.io/datarecord/design/model/schema/#versioning)
        """
        problems = []

        for dim, was in other.dimensions.items():
            now = self.dimensions.get(dim)
            if now is None:
                problems.append(f"dim {dim!r} removed")
                continue
            if _typed_as(now) != _typed_as(was):
                problems.append(f"dim {dim!r} {_typed_as(was)} -> {_typed_as(now)}")

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
                elif _typed_as(now_prop) != _typed_as(was_prop):
                    problems.append(
                        f"property {name!r} of {dim!r} {_typed_as(was_prop)} "
                        f"-> {_typed_as(now_prop)}"
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
