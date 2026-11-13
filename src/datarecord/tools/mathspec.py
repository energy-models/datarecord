# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""A mathspec spec's view of a record: the schema its data needs, and the tables it reads.

Not a `Tool`: a `Tool` is one framework's fixed vocabulary, and here the
vocabulary is the spec's, so both functions take the spec.

Notes
-----
- [the mathspec adapter](https://energy-models.github.io/datarecord/reference/tools/#the-mathspec-adapter)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from mathspec import to_spec

from datarecord.duck import ex_all, join_on, null_safe, values_relation
from datarecord.protocols import Flags
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from datarecord.settle import settle

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence
    from pathlib import Path

    from duckdb import DuckDBPyRelation, Expression
    from mathspec.program import Program
    from mathspec.spec import Spec

    from datarecord.protocols import RecordLike

    Source = str | Path | Mapping[str, object] | Spec

_DATA_MODEL = "https://energy-models.github.io/datarecord/reference/data-model"
_ADAPTER = (
    "https://energy-models.github.io/datarecord/reference/tools/#the-mathspec-adapter"
)


def schema_of(
    spec: Source,
    *,
    partial: Collection[str] = (),
    default_by: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
) -> Schema:
    """The schema of a record holding the data `spec` reads.

    A relation is a multi-dim, a map is a property that refers to its dim, and
    a parameter is an attribute. An attribute is over each relation its
    parameter lists under `in:`, and over each dim of the parameter that no
    such relation is over. A parameter's `missing:` value is its attribute's
    `default`. A variable and a named expression have no attribute.

    Parameters
    ----------
    spec
        A YAML path, the YAML text, a mapping or a loaded `Spec`, as
        mathspec's `to_spec` takes it.
    partial
        The dims a layer may patch value by value, as `Schema.partial` takes
        them. The spec does not state them, because they describe storage.
    default_by
        Per parameter, its `default_by` as `AttributeSpec` takes it. The spec
        does not state it either, because its `missing:` is one value.
        `tables` returns a row for each label whose default differs from that
        value.

    Raises
    ------
    mathspec.errors.LanguageError
        If `spec` does not load.
    FileNotFoundError
        If `spec` is a path to no file.
    ValueError
        If a key of `default_by` is no parameter of `spec`, or is one under
        `missing: absent`. Also if a parameter lists two relations over one
        dim under `in:`, or `Schema` refuses a declaration.

    Notes
    -----
    - [the mathspec adapter](https://energy-models.github.io/datarecord/reference/tools/#the-mathspec-adapter)
    - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
    - [properties](https://energy-models.github.io/datarecord/reference/data-model/#properties)
    - [default_by](https://energy-models.github.io/datarecord/reference/schema/#default_by)
    - [partial](https://energy-models.github.io/datarecord/reference/layers/#partial)
    """
    program = to_spec(spec).program
    default_by = default_by or {}
    _refuse_defaults(program, default_by)
    dimensions = {
        name: Dimension(
            dtype=d.dtype,
            one_of=list(d.one_of) if d.one_of else None,
            description=d.description,
        )
        for name, d in program.dimensions.items()
    }
    properties: dict[str, dict[str, PropertySpec]] = {}
    for name, table in program.relations.items():
        if name not in program.dimensions:
            dimensions[name] = Dimension(
                over={role: table.dim(role) for role in table.key},
                description=table.description,
            )
        if table.values:
            properties[name] = {
                role: PropertySpec(one_of=table.dim(role)) for role in table.values
            }
    attributes = {
        name: AttributeSpec(
            dtype=p.dtype,
            dims=_attribute_dims(program, name, p.dims, p.in_),
            default=None if isinstance(p.missing, str) else p.missing,
            default_by={k: dict(v) for k, v in default_by.get(name, {}).items()},
            description=p.description,
        )
        for name, p in program.parameters.items()
    }
    return Schema(
        dimensions=dimensions,
        properties=properties,
        attributes=attributes,
        partial=frozenset(partial),
    )


def tables(record: RecordLike, spec: Source) -> dict[str, DuckDBPyRelation]:
    """The tables `spec` reads its data from, by the name it reads each under.

    The table of a dim has its labels and a column per map, and the table of
    a relation has a column per column of the relation. The table of a
    parameter has a column per dim and `value`. Its rows are the stored rows,
    settled by the broadcast rule, and a row with the schema's default at each
    point they leave out. No row is added where the default is the spec's
    `missing:` value, or under `missing: absent`. A dim with `one_of` has no
    table, because the spec lists its labels.

    Parameters
    ----------
    record
        A record whose schema is `schema_of(spec, ...)`, or declares at
        least what that does.
    spec
        As `schema_of` takes it.

    Raises
    ------
    mathspec.errors.LanguageError
        If `spec` does not load.
    FileNotFoundError
        If `spec` is a path to no file.
    KeyError
        If the record's schema declares no dim or attribute of a name
        `spec` reads.
    ValueError
        If a parameter has defaults by the categories of two dims.

    Notes
    -----
    - [the mathspec adapter](https://energy-models.github.io/datarecord/reference/tools/#the-mathspec-adapter)
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [default_by](https://energy-models.github.io/datarecord/reference/schema/#default_by)
    """
    program = to_spec(spec).program
    supplied = [d for d, dim in program.dimensions.items() if not dim.one_of]
    relations = [r for r in program.relations if r not in program.dimensions]
    out = {
        name: _subject_table(record, program, name) for name in (*supplied, *relations)
    }
    for name in program.parameters:
        stored = _stored(record, program, name)
        fill = _fill(record, program, name, stored)
        out[name] = stored if fill is None else stored.union(fill)
    return out


def _refuse_defaults(
    program: Program, default_by: Mapping[str, Mapping[str, Mapping[str, Any]]]
) -> None:
    """Refuse a `default_by` the spec gives no place: on no parameter, or on one whose missing row is no value.

    Under `missing: absent` the model reads a missing row as no row, so a
    default there would add rows the spec says do not exist.
    """
    for name in default_by:
        if name not in program.parameters:
            msg = (
                f"default_by names {name!r}, and the spec declares no parameter "
                f"{name!r}; its parameters are {sorted(program.parameters)}"
            )
            raise ValueError(msg)
        if program.parameters[name].missing == "absent":
            msg = (
                f"default_by gives {name!r} a default, and the spec declares it "
                "`missing: absent`, where a missing row is no value; declare a "
                f"`missing:` value in the spec, or give no default ({_ADAPTER})"
            )
            raise ValueError(msg)


def _attribute_dims(
    program: Program, name: str, dims: Sequence[str], in_: Sequence[str]
) -> frozenset[str]:
    """The dims of the attribute for parameter `name`: each relation of its `in:`, and each of its dims they leave out.

    Two multi-dims of one attribute are independent coordinates, so their
    tuples pair freely. The spec means the combinations both relations list,
    which agree on a shared dim, and a record cannot say that.

    Raises
    ------
    ValueError
        If two relations of `in_` are over one dim.
    """
    covered: dict[str, str] = {}
    for relation in in_:
        for dim in program.relations[relation].dims:
            if dim in covered:
                msg = (
                    f"{name!r} is `in:` {covered[dim]!r} and {relation!r}, both over "
                    f"{dim!r}; an attribute over two multi-dims pairs every tuple of "
                    f"one with every tuple of the other, and the spec means the "
                    f"combinations both list; list those combinations in one "
                    f"relation ({_DATA_MODEL}/#multi-dims)"
                )
                raise ValueError(msg)
            covered[dim] = relation
    return frozenset({*in_, *(d for d in dims if d not in covered)})


def _subject_table(record: RecordLike, program: Program, name: str) -> DuckDBPyRelation:
    """The table of dim or relation `name`: its key, flat, and the maps the spec declares on it."""
    held = name in record.properties
    rows = record.properties[name] if held else record.dims[name]
    table = program.relations.get(name)
    maps = table.values if table is not None else ()
    over = record.schema.over(name)
    key = [col(name)[c].alias(c) for c in over] or [col(name)]
    return rows.project(*key, *(col(m) for m in maps))


def _by_dim(schema: Schema, coordinate: str) -> dict[str, Expression]:
    """The dims `coordinate` gives a label of, each to the expression reading it off a row.

    A relation's column is named for its role, and a parameter's for its dim,
    so a field of a multi-dim is read under the dim it draws on. `in:` takes
    a relation over each dim once, so the dim names one field.
    """
    over = schema.over(coordinate)
    if not over:
        return {coordinate: col(coordinate)}
    return {d: col(coordinate)[c].alias(d) for c, d in over.items()}


def _columns(schema: Schema, program: Program, name: str) -> dict[str, Expression]:
    """Parameter `name`'s dims, in the program's order, each to the expression reading it off a row of its attribute."""
    held: dict[str, Expression] = {}
    for coordinate in schema.coordinates_of(name):
        held |= _by_dim(schema, coordinate)
    return {d: held[d] for d in program.parameters[name].dims}


def _stored(record: RecordLike, program: Program, name: str) -> DuckDBPyRelation:
    """Parameter `name`'s stored rows, one per coordinate a row reaches, as its dims and `value`."""
    schema = record.schema
    labels = {d: record.dims[d] for d in schema.coordinates_of(name)}
    flags = record.flags().get(name, Flags(frozenset(), frozenset()))
    settled = settle(record.attributes[name], labels, flags)
    return settled.project(*_columns(schema, program, name).values(), col("value"))


def _fill(
    record: RecordLike, program: Program, name: str, stored: DuckDBPyRelation
) -> DuckDBPyRelation | None:
    """The rows parameter `name` needs and `stored` lacks, or None where the spec's `missing:` gives them all.

    A coordinate with no row takes its default in the record's schema. Where
    that is the spec's own `missing:` value, or there is none, the model reads
    the missing row as the spec says, so only a category whose default differs
    has rows here. That keeps the table at the stored rows plus the
    categories the spec cannot fill.
    """
    schema = record.schema
    spec = schema.attributes[name]
    given = program.parameters[name].missing
    subjects = {key.partition(".")[0] for key in spec.default_by}
    if not subjects or given == "absent":
        return None
    if len(subjects) > 1:
        msg = (
            f"{name!r} has defaults by the categories of {sorted(subjects)}, and "
            f"`tables` fills a parameter by the categories of one dim; key its "
            f"`default_by` by one dim's properties ({_ADAPTER})"
        )
        raise ValueError(msg)
    subject = subjects.pop()
    keys = schema.categorizing(subject)
    held = record.properties[subject]
    combos = held.project(*(col(k) for k in keys)).distinct().fetchall()
    differing = [
        (combo, default)
        for combo in combos
        if (default := _default(schema, name, keys, combo)) is not None
        and default != given
    ]
    if not differing:
        return None
    value = schema.duck_type(spec)
    types = dict(zip(held.columns, held.types, strict=True))
    defaults = values_relation(
        record.con,
        [*keys, "value"],
        [
            [*(lit(c).cast(types[k]) for k, c in zip(keys, combo, strict=True)), lit(d)]
            for combo, d in differing
        ],
    ).set_alias("t")
    key = _by_dim(schema, subject)
    labelled = join_on(
        held.set_alias("s"), defaults, [null_safe("s", "t", keys)]
    ).project(*key.values(), col("t", "value").cast(value).alias("value"))
    columns = _columns(schema, program, name)
    every = _coordinates(record, name).project(*columns.values()).set_alias("c")
    filled = join_on(
        every, labelled.set_alias("d"), [col("c", k) == col("d", k) for k in key]
    ).project(*(col("c", d) for d in columns), col("d", "value"))
    return filled.set_alias("f").join(
        stored.set_alias("h"),
        ex_all(col("f", d) == col("h", d) for d in columns),
        how="anti",
    )


def _default(
    schema: Schema, name: str, keys: Sequence[str], combo: Sequence[Any]
) -> Any:
    """Attribute `name`'s default for a label of the categories `combo`, a NULL category left out."""
    categories = {k: c for k, c in zip(keys, combo, strict=True) if c is not None}
    return schema.default_for(name, **categories)


def _coordinates(record: RecordLike, name: str) -> DuckDBPyRelation:
    """Every coordinate of attribute `name`: the labels of each of its dims, crossed."""
    axes = [record.dims[d] for d in record.schema.coordinates_of(name)]
    every = axes[0]
    for axis in axes[1:]:
        every = every.cross(axis)
    return every
