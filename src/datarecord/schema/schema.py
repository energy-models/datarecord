# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The schema: what a record's data is, and how a patch to it behaves.

Each layer's `manifest.json` carries the full schema it was written under.

Framework-independent. Dims, properties and attributes are names a schema
declares, because those vocabularies belong to a modelling framework and this
package knows none: a label no tool recognises reads back fine and is reported
by the tool that cannot build it, not rejected here.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/reference/schema/)
- [one schema per record](https://energy-models.github.io/datarecord/reference/schema/#one-schema-per-record)
"""

from __future__ import annotations

import hashlib
import json
import operator
import re
import threading
from collections.abc import Callable, Container, Iterable, Mapping
from functools import cached_property, reduce, wraps
from typing import Any, Concatenate, Self, cast

import duckdb
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation, Expression, sqltypes
from duckdb import StarExpression as star
from duckdb.sqltypes import DuckDBPyType
from mathspec._expression_parser import NAME
from pydantic import Field, model_validator

from datarecord.duck import empty_relation, fn
from datarecord.schema.dtypes import DUCK_TYPES, enum_type, takes
from datarecord.schema.specs import (
    AttributeSpec,
    Dimension,
    Names,
    PropertySpec,
    _Declared,
    _ValueSpec,
)

# Columns the format fixes, whatever the schema declares (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet). Not the
# dims or properties: each is declared and typed from that declaration, which is
# what lets the `ENUM` of a dim with `one_of` pin them. These are the ones no schema
# names - `breakpoint` is NULL for an ordinary scalar, so one column set serves
# every row.
STRUCTURAL_TYPES = {
    "attribute": sqltypes.VARCHAR,
    "breakpoint": sqltypes.DOUBLE,
    "breakpoints": sqltypes.BOOLEAN,
}


# Every long row's trailing columns, after one column per dim: the
# attribute named, the abscissa of a piecewise-linear value, and the value
# (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet).
LONG_TAIL = ("attribute", "breakpoint", "value")


# The owner map's flag columns: two structs with a field per declared dim, so
# the map's column set does not depend on the schema and adding a dim stays the
# compatible change versioning calls it. `breakpoints` is outside both, being no dim
# (https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map, https://energy-models.github.io/datarecord/reference/record/#flags).
FLAG_COLUMNS = ("varies", "broadcast", "breakpoints")


# The owner map's `layer_uuid` column type - a layering mechanism, not
# something a schema declares.
LAYER_UUID_TYPE = "UUID"

LAYER_COLUMN = "layer_uuid"
"""The owner map's column naming the layer each row's value is read from.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
"""


DERIVED = ("order_key",)
"""Columns a resolved frame carries that no layer file may.

The fold's answer *about* a frame rather than data in it, so writing one would
both put a column in a file the format does not define and read as stored order
where the fold always re-derives it from file order.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
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
- [names](https://energy-models.github.io/datarecord/reference/schema/#names)
"""


def _misnamed(name: str) -> str | None:
    """Why `name` cannot be a declared dim, field or attribute, if so.

    mathspec's `NAME` is the rule, since an expression must be able to write
    the name; it also keeps a dim's name usable unquoted in the fold's shared
    table names (`_fold_name`).
    """
    if name.startswith("_"):
        return "starts with `_`, kept for the columns a fold adds and drops"
    if re.fullmatch(NAME, name) is None:
        return "is not a name: use only ASCII letters, digits and `_`, starting with a letter"
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
    - [dimensions](https://energy-models.github.io/datarecord/reference/data-model/)
    - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
    """
    return duckdb.struct_type(dict.fromkeys(dims, sqltypes.BOOLEAN))


def _typed_as(spec: Dimension | AttributeSpec | PropertySpec) -> str:
    """Show how `spec` declares its type: its dtype, or the fields its manifest writes."""
    declared = spec.model_dump(include={"dtype", "one_of", "over"})
    return (
        str(declared["dtype"]) if declared.keys() == {"dtype"} else json.dumps(declared)
    )


def schema_digest(raw: Any) -> tuple[str, str]:
    """`raw`, a schema's JSON form, as compact JSON text and that text's sha256.

    Keys keep their order, which a schema means: a multi-dim's members are
    ordered.
    """
    text = json.dumps(raw, separators=(",", ":"))
    return text, hashlib.sha256(text.encode()).hexdigest()


class Schema(_Declared):
    """One record's schema.

    Attributes
    ----------
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
    - [the schema](https://energy-models.github.io/datarecord/reference/schema/)
    - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
    """

    version: int = 1
    dimensions: dict[str, Dimension] = Field(default_factory=dict)
    attributes: dict[str, AttributeSpec] = Field(default_factory=dict)
    properties: dict[str, dict[str, PropertySpec]] = Field(default_factory=dict)
    partial: Names | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @cached_property
    def digest(self) -> str:
        """The sha256 hex of the schema's compact JSON, as its manifest writes it.

        Equal for equal schemas and stable across processes, so it keys a
        schema wherever the schema itself cannot. Computed once: the nested
        mappings must not be mutated; derive a changed schema with
        `model_copy(update=...)`.
        """
        return schema_digest(json.loads(self.model_dump_json()))[1]

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        """A copy as pydantic makes it, without the `digest` of the original."""
        copied = super().model_copy(update=update, deep=deep)
        copied.__dict__.pop("digest", None)
        return copied

    @model_validator(mode="after")
    def _validate(self) -> Schema:
        """Check the rules the format itself fixes.

        Notes
        -----
        - [dimensions](https://energy-models.github.io/datarecord/reference/data-model/)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        - [names](https://energy-models.github.io/datarecord/reference/schema/#names)
        """
        self._check_names()
        declared = set(self.dimensions)

        # Attributes but no axes is a table, not a record (https://energy-models.github.io/datarecord/reference/data-model/). Rejected here
        # so the owner map never needs a struct with no fields, which DuckDB has
        # no type for. A wholly empty `Schema()` stays legal: "no manifest yet".
        if self.attributes and not declared:
            msg = (
                "a schema declaring attributes must declare at least one dim; "
                "attribute data varying over no axis is not a record (https://energy-models.github.io/datarecord/reference/data-model/)"
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
                    f"field draws on a dim that is no multi-dim "
                    f"(https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)"
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
                        f"the multi-dim's fields, so a keyword such as "
                        f"{twice[0]}= would address both. Name the multi-dim's "
                        f"field differently, e.g. over={{..., 'port': "
                        f"{twice[0]!r}}} (https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column)"
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
        field = prop in self.over(subject) if subject in self.dimensions else False
        if field:
            reason = f"is a field of the multi-dim {subject!r}, not a property"
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
                f"(https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)"
            )
            raise ValueError(msg)
        for (subject, name), dim in self.one_of_properties().items():
            if dim not in self.dimensions:
                msg = (
                    f"property {subject}.{name} refers to {dim!r}, "
                    f"an undeclared dim "
                    f"(https://energy-models.github.io/datarecord/reference/data-model/#properties)"
                )
                raise ValueError(msg)
            if dim in self.multi_dims:
                msg = (
                    f"property {subject}.{name} refers to the multi-dim {dim!r}; "
                    f"a property refers to a dim that is no multi-dim "
                    f"(https://energy-models.github.io/datarecord/reference/data-model/#properties)"
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
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        """
        return {
            (subject, name): spec.one_of
            for subject, by_name in self.properties.items()
            for name, spec in by_name.items()
            if spec.one_of is not None
        }

    def _reached_by(self, subject: str) -> frozenset[str]:
        """The dims a property of `subject` reaches: the dim, and those its fields draw on."""
        return frozenset({subject, *self.over(subject).values()})

    def _check_names(self) -> None:
        """Refuse every declared name that cannot be a column of its own.

        A multi-dim's field is a column too, inside its struct column,
        and so is a property's, in its subject's file only. Two spellings of one
        name differing only in case are refused record-wide, DuckDB resolving
        both to one column; one spelling declared twice is left to the rules
        that govern it - except a property named like a column of its subject's
        file or a field of its multi-dim.

        Notes
        -----
        - [names](https://energy-models.github.io/datarecord/reference/schema/#names)
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        """
        declared = [
            *(("dim", d) for d in self.dimensions),
            *(
                (f"field of multi-dim {m!r}", c)
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
            clashing = sorted(set(specs) & self._column_of(subject))
            if clashing:
                msg = (
                    f"properties {clashing} of {subject!r} are named like its column "
                    f"{subject!r}, which they would collide with - rename the "
                    f"properties"
                )
                raise ValueError(msg)
            fields = self.over(subject) if subject in self.dimensions else {}
            shared = sorted(set(specs) & set(fields))
            if shared:
                msg = (
                    f"properties {shared} of multi-dim {subject!r} share their "
                    f"names with its fields, so `{subject}.<name>` would name both - "
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
                f"with `_` (https://energy-models.github.io/datarecord/reference/schema/#names)"
            )
            raise ValueError(msg)

    def _column_of(self, subject: str) -> set[str]:
        """Return the column of the file of `subject`, a name no property of it may take.

        That is the dim's own column, a multi-dim's struct column.
        """
        return {subject}

    # -- derived dim sets (https://energy-models.github.io/datarecord/reference/layers/#partial) --------------------------------------

    @property
    def flat_dims(self) -> tuple[str, ...]:
        """Every dim that is not a multi-dim (a flat dim), in declaration order.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.over is None)

    @property
    def multi_dims(self) -> tuple[str, ...]:
        """Every declared multi-dim, in declaration order, each carried in a row as one struct column named for it.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.over is not None)

    @property
    def one_of_dims(self) -> tuple[str, ...]:
        """Every dim with `one_of`, in declaration order: those whose axis is the schema's list.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
        """
        return tuple(d for d, spec in self.dimensions.items() if spec.one_of)

    def over(self, dim: str) -> dict[str, str]:
        """The fields of `dim`, each to the dim it draws on, in `over` order; empty for a dim that is no multi-dim.

        Raises
        ------
        KeyError
            If the schema declares no `dim`.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        return self.dimensions[dim].over or {}

    def fields(self, dim: str) -> dict[str, str]:
        """The fields of `dim`, each to the dim it draws on: a multi-dim's `over`, a flat dim's one field named for itself.

        Raises
        ------
        KeyError
            If the schema declares no `dim`.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        return self.over(dim) or {dim: dim}

    def fields_of(self, attribute: str) -> dict[str, str]:
        """The fields of every dim `attribute` varies over, each to the dim it draws on.

        These are the names `set` and `select` take labels by, such as `bus=`
        for an attribute over `connection`.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        return {
            f: d for dim in self.dims_of(attribute) for f, d in self.fields(dim).items()
        }

    @property
    def dims(self) -> tuple[str, ...]:
        """The dims a NULL broadcasts over: every declared dim, the flat dims first.

        A NULL means "every label of this dim", which the fold expands against
        the axis; a NULL struct means every tuple of the resolved multi-dim.
        The `varies` and `broadcast` structs have one field per dim, in this
        order.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        return (*self.flat_dims, *self.multi_dims)

    def dims_of(self, attribute: str) -> tuple[str, ...]:
        """The dims `attribute` varies over, in its rows' column order: multi-dims first.

        A multi-dim is one struct column named for it, so
        `dims={"connection", "snapshot"}` gives `("connection", "snapshot")`.

        Per attribute rather than schema-wide: one file per attribute means one
        column set per attribute, and an all-NULL column of a dim the attribute
        is not addressed by would claim a dim the value has no label on.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        - [addressing](https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column)
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        multi = self.multi_dims_of(attribute)
        spec = self.attributes[attribute]
        return (*multi, *(d for d in self.flat_dims if d in spec.dims))

    def duck_type(self, spec: Dimension | AttributeSpec | PropertySpec) -> DuckDBPyType:
        """Return the DuckDB type of a declared dim, attribute or property.

        A dim with `one_of` has the `ENUM` of them, a multi-dim the struct of
        its fields, each typed as the dim it draws on. A property that refers
        to a dim has the type of that dim.

        Notes
        -----
        - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
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
        """Reject each of `multi_dims` carried other than as one struct column of exactly its fields.

        Judged by the relation's columns and types, so no row is read. A
        carrier without a multi-dim's column passes.

        Parameters
        ----------
        carrier
            What carries the columns, named in the message.

        Raises
        ------
        ValueError
            On a field as a column of its own, or a multi-dim's column
            not a struct of exactly its fields, naming the struct to pass
            instead.

        Notes
        -----
        - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
        """
        types = dict(zip(rel.columns, rel.types, strict=True))
        for multi in multi_dims:
            over = list(self.over(multi))
            flat = [c for c in over if c in types]
            dtype = types.get(multi)
            fields = (
                over
                if dtype is None
                else [name for name, _ in dtype.children]
                if dtype.id == "struct"
                else None
            )
            if flat or fields is None or set(fields) != set(over):
                carried = f"{flat} flat" if flat else f"{multi!r} as {dtype}"
                msg = (
                    f"{carrier} carries {carried}; pass {multi!r} as one struct "
                    f"column of its fields, {over} (https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)"
                )
                raise ValueError(msg)

    def refuse_partial_tuples(
        self,
        multi_dims: Iterable[str],
        rel: DuckDBPyRelation,
        *,
        carrier: str,
    ) -> None:
        """Reject a struct of any of `multi_dims` in `rel` with some fields NULL and not all.

        Reads the rows, unlike `check_multi_dim_columns`. A carrier without a
        multi-dim's column passes.

        Parameters
        ----------
        carrier
            What carries the columns, named in the message.

        Raises
        ------
        ValueError
            Naming `carrier` and the multi-dim.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        """
        for multi in multi_dims:
            if multi not in rel.columns:
                continue
            some = reduce(
                operator.or_, (col(multi)[c].isnull() for c in self.over(multi))
            )
            partly = col(multi).isnotnull() & some
            if rel.filter(partly).limit(1).fetchall():
                msg = (
                    f"{carrier} carries a {multi!r} with some fields NULL; a "
                    f"multi-dim value is a whole tuple, or NULL for every tuple (https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)"
                )
                raise ValueError(msg)

    def long_columns_for(self, attribute: str) -> tuple[str, ...]:
        """Return one attribute's full long column set, in order.

        An attribute carries a column per dim its `dims` name, so a per-snapshot
        weighting carries `snapshot` alone.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        return (*self.dims_of(attribute), *LONG_TAIL)

    def categorizing(self, dim: str) -> tuple[str, ...]:
        """Return the categorizing properties of `dim`, the ones `default_by` keys on.

        A categorizing property is a property of `dim` that refers to a dim with
        `one_of`. Only a dim in `partial` has any.

        Notes
        -----
        - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
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
        - [categories](https://energy-models.github.io/datarecord/reference/data-model/#categories)
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
        - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
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
            As `resolve` does, and on a field, which has no default.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/reference/schema/#attributes)
        - [default_by](https://energy-models.github.io/datarecord/reference/schema/#default_by)
        """
        resolved = self.resolve(name)
        spec = resolved[1] if isinstance(resolved, tuple) else resolved
        if isinstance(spec, Dimension):
            msg = f"{name!r} is a field of a multi-dim, not a property, and has no default"
            raise KeyError(msg)
        for key, by in spec.default_by.items():
            category = categories.get(key.partition(".")[2])
            if category in by:
                return by[category]
        return spec.default

    def addresses(self, attribute: str, dim: str) -> bool:
        """Whether `attribute` reaches a label of `dim` at all.

        True where its `dims` name `dim`, or a multi-dim one of whose
        fields draws on `dim`.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
        """
        return (
            any(dim in self.over(m).values() for m in self.multi_dims_of(attribute))
            or dim in self.attributes[attribute].dims
        )

    def resolve(
        self, name: str
    ) -> AttributeSpec | tuple[str, PropertySpec] | tuple[str, Dimension]:
        """What `name` addresses: an attribute, or a property or field as `subject.name`.

        Read off the name alone: a plain name is never a property.

        Returns
        -------
        AttributeSpec or tuple of str and PropertySpec or Dimension
            The attribute's spec, a property's subject and spec, or a
            field's multi-dim and the dim the field draws on.

        Raises
        ------
        KeyError
            If `name` addresses nothing; for a plain name some subject's
            property is called, naming each dotted form.

        Notes
        -----
        - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
        - [fields](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
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

    def field_over(self, multi: str, dim: str) -> str | None:
        """The one field of the multi-dim `multi` drawing on `dim`, if any.

        Raises
        ------
        ValueError
            If several fields of `multi` draw on `dim`.
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
        attribute may vary over, `partial_dims` the owner map's index (the
        `partial` dims), and ownership is their intersection. A
        dim in `dims` but not the owner map's index - a non-`partial` value axis like
        `timestep` - is owned whole, so a patch to one of its values restates the
        attribute's entire extent along it (`_owned_whole`).

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
        """
        spec = self.attributes.get(attribute)
        if spec is None:
            return frozenset()
        return spec.dims & set(self.partial_dims)

    @property
    def partial_dims(self) -> tuple[str, ...]:
        """The dims of the owner map's index, in declaration order.

        The dims a layer may patch per value (`partial`). The owner map's
        index is one fixed tuple over all attributes, so it carries every axis *any* layer may patch by value or
        by row, not only those some currently declared attribute varies over. An
        attribute not owned per one of them writes NULL there, the "NULL means
        all values" rule - which also lets a schema declare an axis before any
        attribute uses it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        - [one fold for every axis](https://energy-models.github.io/datarecord/reference/resolution/#axes)
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        partial = self.partial or frozenset()
        return tuple(d for d in self.dims if d in partial)

    # -- index and column sets (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet, https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map) -----------------------------------

    @property
    def long_columns(self) -> tuple[str, ...]:
        """The long schema's full column set.

        The *map's* column set, which is uniform across attributes because the
        map is one relation over all of them. An individual file carries only
        its own attribute's columns (`long_columns_for`), and `union_by_name`
        supplies NULL for the rest when the fold unions them here.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        return (*self.multi_dims, *self.flat_dims, *LONG_TAIL)

    @property
    def owner_map_index(self) -> tuple[str, ...]:
        """The owner map's index columns, compared NULL-safely when folding.

        `partial_dims`, plus `attribute`: a layer may patch one value along a
        `partial` dim without restating every other's.

        A column an attribute's own file does not carry reads as NULL,
        which is what makes the index one fixed tuple over attributes whose
        columns differ.

        Notes
        -----
        - [attributes over a multi-dim](https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column)
        - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        return (*self.partial_dims, "attribute")

    def multi_dims_of(self, attribute: str) -> tuple[str, ...]:
        """Which declared multi-dims address `attribute`, in declaration order.

        Raises
        ------
        KeyError
            If the schema does not declare `attribute`.

        Notes
        -----
        - [addressing](https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column)
        """
        if attribute not in self.attributes:
            msg = f"the schema declares no attribute {attribute!r}"
            raise KeyError(msg)
        return tuple(m for m in self.multi_dims if m in self.attributes[attribute].dims)

    @property
    def owner_map_columns(self) -> tuple[str, ...]:
        """The owner map's full column set.

        Notes
        -----
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        return (*self.owner_map_index, LAYER_COLUMN, *FLAG_COLUMNS)

    # -- typing (https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet, https://energy-models.github.io/datarecord/reference/layers/) -------------------------------------------------

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
        - [one schema per record](https://energy-models.github.io/datarecord/reference/schema/#one-schema-per-record)
        - [the owner map](https://energy-models.github.io/datarecord/reference/resolution/#the-owner-map)
        """
        found = self.declared(column, subject)
        return None if found is None else found[1]

    def _structural_type(self, column: str) -> DuckDBPyType | None:
        """Return the type the format fixes for `column`: a structural column or a flag struct."""
        if column in STRUCTURAL_TYPES:
            return STRUCTURAL_TYPES[column]
        if column in ("varies", "broadcast"):
            return flag_type(self.dims) if self.dims else None
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
        - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
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
        - [dtypes](https://energy-models.github.io/datarecord/reference/schema/#dtypes)
        - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
        """
        wrong = []
        nulls: dict[str, DuckDBPyType] = {}
        for c, given in zip(rel.columns, rel.types, strict=True):
            found = self.declared(c, subject, attribute)
            if found is None or takes(found[1], given):
                continue
            if not rel.filter(col(c).isnotnull()).limit(1).fetchall():
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
        - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
        """
        spec = self.attributes.get(attribute)
        return None if spec is None else DUCK_TYPES[spec.dtype]

    # -- versioning (https://energy-models.github.io/datarecord/reference/schema/#compatibility) --------------------------------------------------

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
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
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


def packed(
    schema: Schema, multi: str, present: Container[str], dims: Mapping[str, Any]
) -> Expression:
    """The struct column of the multi-dim `multi`, packed from flat field columns.

    A field is the column of that name where the relation carries one, else
    the caller's keyword. No field given at all is a NULL struct, every tuple.

    Raises
    ------
    ValueError
        Where some fields are given and others not: a struct NULL in some
        fields and not all matches no tuple.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    over = schema.over(multi)
    struct = schema.duck_type(schema.dimensions[multi])
    missing = [c for c in over if c not in present and c not in dims]
    if len(missing) == len(over):
        return lit(None).cast(struct)
    if missing:
        msg = (
            f"{multi!r} has the fields {list(over)}, and {missing} was not given; "
            f"name it as a keyword, e.g. `{missing[0]}=...` (https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)"
        )
        raise ValueError(msg)
    fields = {
        c: col(c)
        if c in present
        else lit(dims[c]).cast(schema.duck_type(schema.dimensions[d]))
        for c, d in over.items()
    }
    return fn.struct_pack(**fields).cast(struct)


def unpacked(schema: Schema, multi: str, rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """`rel` with the struct column of the multi-dim `multi` spelled as a column per field, first; as it is where it carries no such column.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
    """
    if multi not in rel.columns or multi not in schema.multi_dims:
        return rel
    return rel.project(
        *(col(multi)[c].alias(c) for c in schema.over(multi)),
        *(col(c) for c in rel.columns if c != multi),
    )


def cast_declared(
    schema: Schema,
    rel: DuckDBPyRelation,
    subject: str | None = None,
    attribute: str | None = None,
) -> DuckDBPyRelation:
    """Return `rel` with each column the schema declares a type for cast to that type.

    Other columns are left as they are. `subject` and `attribute` are as in
    `Schema.declared`.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    cols = [
        col(c).cast(found[1]).alias(c)
        if (found := schema.declared(c, subject, attribute))
        else col(c)
        for c in rel.columns
    ]
    return rel.project(*cols)


def empty_declared(
    schema: Schema, con: DuckDBPyConnection, *columns: str
) -> DuckDBPyRelation:
    """A zero-row relation with `columns`, cast to their declared type, `VARCHAR` if none.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    return empty_relation(
        con, **{c: schema.column_type(c) or sqltypes.VARCHAR for c in columns}
    )


def with_columns(
    schema: Schema, rel: DuckDBPyRelation, *columns: str
) -> DuckDBPyRelation:
    """`rel` with any of `columns` it lacks added as typed NULLs.

    One file carries a column per dim of one attribute, so a column another
    attribute uses is simply absent - and a dim an attribute is not addressed by
    means the value applies across every value of it, which is what NULL means.
    Materialising it here lets every path downstream project the column
    unconditionally instead of branching on its presence.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    missing = [c for c in columns if c not in rel.columns]
    if not missing:
        return rel
    added = [
        lit(None).cast(schema.column_type(c) or sqltypes.VARCHAR).alias(c)
        for c in missing
    ]
    return rel.project(star(), *added)


def per_schema[**P, T](
    build: Callable[Concatenate[Schema, P], T],
) -> Callable[Concatenate[Schema, P], T]:
    """`build` memoised on a `Schema` and further hashable arguments, for the last 64 keys.

    A schema is unhashable, and every layer holds its own equal copy, so it is
    keyed by its `digest`.
    """
    held: dict[tuple[object, ...], T] = {}
    lock = threading.Lock()

    @wraps(build)
    def memo(schema: Schema, *args: P.args, **kwargs: P.kwargs) -> T:
        key = (schema.digest, *args, *sorted(kwargs.items()))
        with lock:
            found = held.get(key)
        if found is not None:
            return found
        value = build(schema, *args, **kwargs)
        with lock:
            held[key] = value
            if len(held) > 64:
                del held[next(iter(held))]
        return value

    return memo
