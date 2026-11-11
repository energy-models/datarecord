# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Select a view of a record at some of its labels.

Notes
-----
- [selecting](https://energy-models.github.io/datarecord/reference/selecting/)
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from duckdb import (
    CaseExpression,
    DuckDBPyConnection,
    DuckDBPyRelation,
    Expression,
)
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import StarExpression as star
from duckdb.sqltypes import DuckDBPyType

from datarecord.duck import fn, numbered_relation
from datarecord.protocols import Flags, LazyRelations, RecordLike
from datarecord.schema.categories import categorized_dim, with_defaults
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, PropertySpec
from datarecord.settle import settle

_AT = "_at"
"""The column a relation of kept labels numbers them in, in axis order."""

_ORDERED = frozenset({"bigint", "double", "timestamp"})
"""The type ids of an axis a slice takes: numbers and datetimes."""


@dataclass(frozen=True)
class Selection:
    """A record at the labels `select` names, itself a `RecordLike`.

    A dim given one label is squeezed out of `schema`; every other dim keeps
    the labels that refer to no label the selection drops. `dimensions` and
    `properties` keep the base's order, and every relation is a plan on the
    base's connection.

    Attributes
    ----------
    base
        The record selected from, never a `Selection`.
    labels
        The keywords asked for, normalized: one label for a squeezed dim, else
        the list of labels matched.
    schema
        The base's schema without the squeezed dims, their axes, their
        properties and every property referring to one.

    Notes
    -----
    - [selecting](https://energy-models.github.io/datarecord/reference/selecting/)
    """

    base: RecordLike
    labels: Mapping[str, Any]
    schema: Schema
    _squeezed: Mapping[str, Any] = field(repr=False)
    _kept: Mapping[str, DuckDBPyRelation] = field(repr=False, compare=False)

    @property
    def con(self) -> DuckDBPyConnection:
        """The base's connection, which every relation here is planned on."""
        return self.base.con

    @property
    def dimensions(self) -> LazyRelations:
        """The axes of the dims left, each at its kept labels in axis order."""
        names = _declared(self.base.dimensions, self.schema.dimensions)
        return LazyRelations(names, self._axis)

    @property
    def properties(self) -> LazyRelations:
        """The stored properties left, at the kept labels of their subject."""
        names = _declared(self.base.properties, self.schema.properties)
        return LazyRelations(names, self._subject)

    @property
    def attributes(self) -> LazyRelations:
        """Long rows at the kept labels or NULL there, each squeezed dim settled and dropped.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
        """
        names = _declared(self.base.attributes, self.schema.attributes)
        return LazyRelations(names, self._attribute)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """The base's flags without the squeezed dims, overstating the view's rows.

        Raises
        ------
        ValueError
            If the keywords categorize no dim of the view, or several.

        Notes
        -----
        - [Flags](https://energy-models.github.io/datarecord/reference/record/#flags)
        """
        if categories:
            categorized_dim(self.schema, categories)
        gone = frozenset(self._squeezed)
        return {
            name: Flags(f.varies - gone, f.broadcast - gone, f.breakpoints)
            for name, f in self.base.flags(**categories).items()
        }

    def select(self, **labels: Any) -> Selection:
        """Select `base` once more, `labels` replacing the keywords of the same name.

        Raises
        ------
        ValueError
            As `select` does.
        """
        return select(self.base, **{**self.labels, **labels})

    def lift(self, attribute: str, rel: DuckDBPyRelation) -> DuckDBPyRelation:
        """`rel`, rows of `attribute` over this view, with the squeezed labels put back.

        Each squeezed dim the base declares `attribute` over is added as a
        column at its label, and each squeezed field back into its
        multi-dim's struct; a NULL struct stays NULL. An attribute the base
        declares over no squeezed dim comes back as it came.

        Raises
        ------
        KeyError
            If the base declares no `attribute`.

        Notes
        -----
        - [lifting rows back](https://energy-models.github.io/datarecord/reference/selecting/#lift)
        """
        base = self.base.schema
        dims = base.dimensions_of(attribute)
        changed = [
            c
            for c in dims
            if c in self._squeezed or (c in rel.columns and self._shrunk(c))
        ]
        if not changed:
            return rel
        lifted: list[Expression] = []
        for c in dims:
            if c in self._squeezed:
                lifted.append(_literal(self._squeezed[c], self._type(c)).alias(c))
            elif c in changed:
                fields = {
                    f: col(c)[f]
                    if f in self.schema.over(c)
                    else _literal(self._squeezed[d], self._type(d))
                    for f, d in base.over(c).items()
                }
                lifted.append(_tuple(c, fields, self._type(c)))
            elif c in rel.columns:
                lifted.append(col(c))
        rest = [col(c) for c in rel.columns if c not in dims]
        return rel.project(*lifted, *rest)

    def _type(self, dim: str) -> DuckDBPyType:
        """The DuckDB type of `dim` in the base."""
        schema = self.base.schema
        return schema.duck_type(schema.dimensions[dim])

    def _shrunk(self, dim: str) -> bool:
        """Whether `dim` is a multi-dim the view keeps with some fields squeezed."""
        if dim not in self.schema.multi_dimensions:
            return False
        return self.schema.over(dim) != self.base.schema.over(dim)

    def _keyed(self, dim: str, rel: DuckDBPyRelation) -> DuckDBPyRelation:
        """`rel`, keyed by `dim`, at the kept labels of `dim` in axis order, its tuples reduced to the view's."""
        if dim in self._kept:
            kept = self._kept[dim].set_alias("_k")
            rel = (
                rel.set_alias("_r")
                .join(kept, col("_r", dim) == col("_k", dim))
                .project(*(col("_r", c) for c in rel.columns), col("_k", _AT))
                .order(_AT)
                .project(star(exclude=[_AT]))
            )
        return self._reduced(rel)

    def _reduced(self, rel: DuckDBPyRelation) -> DuckDBPyRelation:
        """`rel` with each struct of a multi-dim the view keeps reduced to the view's fields."""
        if not any(self._shrunk(c) for c in rel.columns):
            return rel
        return rel.project(*(self._column(c) for c in rel.columns))

    def _column(self, c: str) -> Expression:
        """Column `c`, a multi-dim's struct reduced to the view's fields."""
        if not self._shrunk(c):
            return col(c)
        fields = {f: col(c)[f] for f in self.schema.over(c)}
        return _tuple(c, fields, self.schema.duck_type(self.schema.dimensions[c]))

    def _axis(self, dim: str) -> DuckDBPyRelation:
        return self._keyed(dim, self.base.dimensions[dim])

    def _subject(self, subject: str) -> DuckDBPyRelation:
        rel = self._keyed(subject, self.base.properties[subject])
        names = (subject, *self.schema.properties[subject])
        return rel.project(*(col(n) for n in names))

    def _attribute(self, name: str) -> DuckDBPyRelation:
        rel = self.base.attributes[name]
        squeezed = {
            d: self._kept[d].project(col(d)) for d in self._squeezed if d in rel.columns
        }
        if squeezed:
            over = frozenset(squeezed)
            rel = settle(rel, squeezed, Flags(over, over)).project(
                star(exclude=list(squeezed))
            )
        for c in rel.columns:
            if c in self._kept:
                rel = _within(rel, c, self._kept[c])
        return self._reduced(rel)


def select(record: RecordLike, /, **labels: Any) -> Selection:
    """`record` at the labels named, as xarray's `sel` takes them.

    A keyword names a dim; one naming a multi-dim's field that is no dim
    narrows its tuples, and any other names a property by its bare name and
    narrows its subject to the labels whose value, its default applied, is
    selected. One label squeezes its dim. A list, a slice (both ends
    inclusive, `None` open) or a partial date string on a datetime axis keeps
    it. Every label referring to a dropped one goes with it, through
    properties and multi-dim tuples, until nothing more goes. Over a
    `Selection`, its keywords are merged and its base selected.

    Returns
    -------
    Selection
        The view, which reads the axes it matched and propagated on as they
        were at the call, and plans every attribute on the base's connection.

    Raises
    ------
    ValueError
        Naming the keyword: one naming no dim, field or property, a
        multi-dim, a field of several multi-dims or a property several
        subjects declare; a label the axis lacks; an empty list; a slice with
        a step, of an axis neither numeric nor datetime, with an end the axis
        cannot compare or covering no label; a squeezed label another
        keyword's selection drops; a squeeze folding a category into the
        default a kept label NULL on it does not take; squeezing every dim of
        a record with attributes.

    Notes
    -----
    - [selecting](https://energy-models.github.io/datarecord/reference/selecting/)
    - [what goes with a dropped label](https://energy-models.github.io/datarecord/reference/selecting/#what-goes-with-a-dropped-label)
    """
    if isinstance(record, Selection):
        return record.select(**labels)
    schema = record.schema
    read = _Read(record)
    asked: dict[str, Any] = {}
    squeezed: dict[str, Any] = {}
    kept: dict[str, set[Any]] = {}
    for keyword, value in labels.items():
        if keyword in schema.multi_dimensions:
            raise _refusal(
                keyword,
                value,
                "names a multi-dim, whose tuples are selected through its fields",
            )
        if keyword in schema.dimensions:
            matched = _match(
                keyword, value, read.axis(keyword), _dtype(schema, keyword)
            )
            asked[keyword] = matched
            if not isinstance(matched, list):
                squeezed[keyword] = matched
            _narrow(kept, keyword, {_key(m) for m in _listed(matched)})
            continue
        subject, allowed, asked[keyword] = _narrowing(schema, read, keyword, value)
        _narrow(kept, subject, allowed)
    for m in schema.multi_dimensions:
        over = schema.over(m)
        if all(d in squeezed for d in over.values()):
            squeezed[m] = {c: squeezed[d] for c, d in over.items()}
            _narrow(kept, m, {_key(t) for t in read.axis(m)})
    if schema.attributes and schema.dimensions.keys() <= squeezed.keys():
        raise _refused(
            {k: v for k, v in asked.items() if k in squeezed},
            "squeezes every dim, and attributes over no axis are no record; "
            "keep one dim as a list of one",
        )
    _propagate(schema, read, kept)
    for keyword, label in asked.items():
        if keyword in squeezed and _key(label) not in kept[keyword]:
            raise _refusal(
                keyword,
                label,
                "is squeezed at a label the selection drops through another keyword",
            )
    _refuse_uncategorized(schema, read, squeezed, kept)
    narrowed = {
        d: numbered_relation(
            record.con,
            d,
            [label for label in read.axis(d) if _key(label) in held],
            _dtype(schema, d),
            _AT,
        )
        for d, held in kept.items()
        if d in squeezed or len(held) < len(read.axis(d))
    }
    view = _schema(schema, squeezed, read.category(squeezed))
    return Selection(record, asked, view, squeezed, narrowed)


class _Read:
    """The base's axes and property values `select` matches and propagates on, each fetched once."""

    def __init__(self, record: RecordLike) -> None:
        self.record = record
        self._axes: dict[str, list[Any]] = {}
        self._values: dict[tuple[str, str], dict[Any, Any]] = {}

    def axis(self, dim: str) -> list[Any]:
        """The labels of `dim` in axis order, a tuple as a dict."""
        if dim not in self._axes:
            self._axes[dim] = [
                label for (label,) in self.record.dimensions[dim].fetchall()
            ]
        return self._axes[dim]

    def values(self, subject: str, name: str) -> dict[Any, Any]:
        """Each label of `subject`, by `_key`, to its property `name`, its default applied."""
        if (subject, name) not in self._values:
            record = self.record
            rel = with_defaults(
                record.schema, subject, record.properties[subject], [name], record.con
            )
            self._values[subject, name] = {_key(k): v for k, v in rel.fetchall()}
        return self._values[subject, name]

    def category(self, squeezed: Mapping[str, Any]) -> Callable[[str, str], Any]:
        """What property `name` of `subject` holds where one of the two is squeezed.

        The label of the dim the property refers to, where that is squeezed;
        else the value of the squeezed subject's label, None where it has none.
        """

        def category(subject: str, name: str) -> Any:
            spec = self.record.schema.properties[subject][name]
            if spec.refers is not None and spec.refers in squeezed:
                return squeezed[spec.refers]
            return self.values(subject, name).get(_key(squeezed[subject]))

        return category


def _narrowing(
    schema: Schema, read: _Read, keyword: str, value: Any
) -> tuple[str, set[Any], list[Any]]:
    """The subject a keyword naming no dim narrows, the keys of its labels kept, and the values matched.

    A field of a multi-dim keeps the tuples holding a matched label
    there; a property keeps the labels whose value, its default applied, is
    matched. The values a property is matched against are the axis of the dim
    it refers to, else the values its labels hold, sorted.

    Raises
    ------
    ValueError
        If `keyword` is a field of several multi-dims, a property of
        several subjects or neither, or as `_match` does.
    """
    multis = [m for m in schema.multi_dimensions if keyword in schema.over(m)]
    if len(multis) > 1:
        raise _refusal(
            keyword,
            value,
            f"names a field of each of {multis}; select through a dim instead",
        )
    if multis:
        (multi,) = multis
        drawn = schema.over(multi)[keyword]
        matched = _listed(
            _match(keyword, value, read.axis(drawn), _dtype(schema, drawn))
        )
        wanted = {_key(m) for m in matched}
        tuples = read.axis(multi)
        return multi, {_key(t) for t in tuples if _key(t[keyword]) in wanted}, matched
    owners = [s for s, specs in schema.properties.items() if keyword in specs]
    if not owners:
        raise _refusal(keyword, value, "names no dim, field or property")
    if len(owners) > 1:
        raise _refusal(
            keyword,
            value,
            f"names a property of each of {owners}; select through a dim instead",
        )
    (subject,) = owners
    spec = schema.properties[subject][keyword]
    values = read.values(subject, keyword)
    held = (
        read.axis(spec.refers)
        if spec.refers is not None
        else sorted({v for v in values.values() if v is not None})
    )
    matched = _listed(_match(keyword, value, held, schema.duck_type(spec)))
    wanted = {_key(m) for m in matched}
    allowed = {k for k, v in values.items() if v is not None and _key(v) in wanted}
    return subject, allowed, matched


def _propagate(schema: Schema, read: _Read, kept: dict[str, set[Any]]) -> None:
    """Narrow `kept` until no kept label refers to one dropped.

    A label goes where a property, its default applied, names a dropped label,
    and a tuple where one of its fields does; a NULL names none.

    Notes
    -----
    - [what goes with a dropped label](https://energy-models.github.io/datarecord/reference/selecting/#what-goes-with-a-dropped-label)
    """
    referring = schema.referring_properties()
    while True:
        before = {d: len(held) for d, held in kept.items()}
        dropped = {
            d: {_key(label) for label in read.axis(d)} - held
            for d, held in kept.items()
        }
        for (subject, name), dim in referring.items():
            if dropped.get(dim):
                values = read.values(subject, name)
                gone = {
                    k
                    for k, v in values.items()
                    if v is not None and _key(v) in dropped[dim]
                }
                _narrow(kept, subject, set(values) - gone)
        for multi in schema.multi_dimensions:
            for f, dim in schema.over(multi).items():
                if dropped.get(dim):
                    _narrow(
                        kept,
                        multi,
                        {
                            _key(t)
                            for t in read.axis(multi)
                            if _key(t[f]) not in dropped[dim]
                        },
                    )
        if {d: len(held) for d, held in kept.items()} == before:
            return


def _refuse_uncategorized(
    schema: Schema,
    read: _Read,
    squeezed: Mapping[str, Any],
    kept: Mapping[str, set[Any]],
) -> None:
    """Refuse a squeeze whose category default a kept label NULL on the category would take.

    `_schema` folds a `default_by` keyed on a property referring to a squeezed
    dim into the default for the squeezed label, which a label NULL there,
    matching no key in the base, never took. Only a key listing that label
    misstates a default.

    Raises
    ------
    ValueError
        Naming the squeezed dim's keyword.
    """
    specs = [
        *schema.attributes.values(),
        *(spec for specs in schema.properties.values() for spec in specs.values()),
    ]
    for (subject, name), dim in schema.referring_properties().items():
        if dim not in squeezed:
            continue
        label, key = squeezed[dim], f"{subject}.{name}"
        if not any(label in spec.default_by.get(key, {}) for spec in specs):
            continue
        values = read.values(subject, name)
        if any(values[k] is None for k in kept.get(subject, values.keys())):
            raise _refusal(
                dim,
                label,
                f"would give the labels of `{subject}` NULL on `{key}` the default "
                f"`default_by` sets for {label!r}; select [{label!r}] to keep `{dim}`",
            )


def _narrow(kept: dict[str, set[Any]], dim: str, allowed: set[Any]) -> None:
    """Keep of `dim` only the labels in `allowed` too."""
    kept[dim] = kept[dim] & allowed if dim in kept else set(allowed)


def _schema(
    schema: Schema, squeezed: Mapping[str, Any], category: Callable[[str, str], Any]
) -> Schema:
    """`schema` without the `squeezed` dims, their properties and the properties referring to one.

    A multi-dim loses the fields drawing on a squeezed dim, and an
    attribute its squeezed dims. A `default_by` keyed on a property that goes
    applies its entry for the squeezed label's category as the default, which
    the keys before it still override, and the keys after it never reach.
    """
    gone = {
        (subject, name)
        for subject, specs in schema.properties.items()
        for name, spec in specs.items()
        if subject in squeezed or spec.refers in squeezed
    }

    def defaulted[S: (PropertySpec, AttributeSpec)](spec: S) -> S:
        default, by = spec.default, {}
        for key, entries in spec.default_by.items():
            subject, _, name = key.partition(".")
            if (subject, name) not in gone:
                by[key] = entries
                continue
            c = category(subject, name)
            if c in entries:
                default = entries[c]
                break
        return spec.model_copy(update={"default": default, "default_by": by})

    return Schema(
        version=schema.version,
        dimensions={
            d: spec
            if spec.over is None
            else spec.model_copy(
                update={
                    "over": {c: x for c, x in spec.over.items() if x not in squeezed}
                }
            )
            for d, spec in schema.dimensions.items()
            if d not in squeezed
        },
        properties={
            subject: {
                n: defaulted(s) for n, s in specs.items() if (subject, n) not in gone
            }
            for subject, specs in schema.properties.items()
            if any((subject, n) not in gone for n in specs)
        },
        attributes={
            a: defaulted(spec).model_copy(
                update={"dims": spec.dims.difference(squeezed)}
            )
            for a, spec in schema.attributes.items()
        },
        partial=None if schema.partial is None else schema.partial.difference(squeezed),
        meta=schema.meta,
    )


def _match(keyword: str, value: Any, labels: Sequence[Any], dtype: DuckDBPyType) -> Any:
    """The labels `value` names among `labels`: one label, or the list of them in order.

    One label is matched as pandas' `.loc` matches it, so on a datetime axis a
    date string coarser than the axis names every label in its span. A list
    names each label exactly, a date string in it read as one instant. A slice
    is pandas' label slice on the labels sorted, both ends inclusive.

    Raises
    ------
    ValueError
        Naming `keyword`, as `select` does.
    """
    if isinstance(value, slice):
        return _sliced(keyword, value, labels, dtype)
    timed = dtype.id == "timestamp"
    held = {_key(label) for label in labels}
    if not _is_scalar(value):
        listed = [_instant(keyword, v) if timed else v for v in value]
        if not listed:
            raise _refusal(keyword, value, "is an empty list, naming no label")
        unknown = [v for v in listed if _key(v) not in held]
        if unknown:
            raise _refusal(keyword, value, f"names {unknown}, which the axis lacks")
        wanted = {_key(v) for v in listed}
        return [label for label in labels if _key(label) in wanted]
    if timed and isinstance(value, str):
        start, stop, resolution = _span(keyword, value)
        if resolution < _resolution(labels):
            within = [label for label in labels if start <= label < stop]
            if not within:
                raise _refusal(keyword, value, "names no label of the axis")
            return within
        value = start
    if _key(value) not in held:
        raise _refusal(keyword, value, "names a label the axis lacks")
    return next(label for label in labels if _key(label) == _key(value))


def _sliced(
    keyword: str, value: slice, labels: Sequence[Any], dtype: DuckDBPyType
) -> list[Any]:
    """The labels between the ends of `value`, both inclusive, in order.

    A date string at an end of a datetime axis covers its span.

    Raises
    ------
    ValueError
        Naming `keyword`: a step, an axis neither numeric nor datetime, an end
        the axis cannot compare, or no label covered.
    """
    if value.step is not None:
        raise _refusal(keyword, value, "is a slice with a step")
    if dtype.id not in _ORDERED:
        raise _refusal(
            keyword, value, f"slices an axis of {dtype}, neither numbers nor datetimes"
        )
    timed = dtype.id == "timestamp"
    start, stop, before = value.start, value.stop, None
    if timed and isinstance(start, str):
        start = _span(keyword, start)[0]
    if timed and isinstance(stop, str):
        before = _span(keyword, stop)[1]
        stop = None
    try:
        within = [
            label
            for label in labels
            if (start is None or label >= start)
            and (stop is None or label <= stop)
            and (before is None or label < before)
        ]
    except TypeError as e:
        raise _refusal(keyword, value, "has an end the axis cannot compare") from e
    if not within:
        raise _refusal(keyword, value, "covers no label")
    return within


_DATE = re.compile(
    r"(\d{4})(?:-(\d{1,2})(?:-(\d{1,2})(?:[ T](\d{1,2})(?::(\d{1,2})(?::(\d{1,2}))?)?)?)?)?"
)
"""A date string down to the second, each part after the year optional."""

_STEPS = {
    3: timedelta(days=1),
    4: timedelta(hours=1),
    5: timedelta(minutes=1),
    6: timedelta(seconds=1),
}
"""The length of a span by its resolution, from a day's on."""


def _span(keyword: str, text: str) -> tuple[datetime, datetime, int]:
    """The span `text` names: its first instant, the instant after it, and its resolution.

    The resolution counts the parts given, a year being 1 and a second 6, and
    is 7 for a string `datetime.fromisoformat` reads with a fraction of a
    second.

    Raises
    ------
    ValueError
        Naming `keyword`, where `text` is no date.
    """
    found = _DATE.fullmatch(text.strip())
    try:
        if found is None:
            instant = datetime.fromisoformat(text)
            return instant, instant + timedelta(microseconds=1), 7
        parts = [int(p) for p in found.groups() if p is not None]
        resolution = len(parts)
        year, month, day, hour, minute, second = [
            *parts,
            *(1, 1, 0, 0, 0)[resolution - 1 :],
        ]
        start = datetime(year, month, day, hour, minute, second)
    except ValueError as e:
        raise _refusal(keyword, text, "is no date the axis can compare") from e
    if resolution == 1:
        return start, start.replace(year=start.year + 1), resolution
    if resolution == 2:
        later = start.month == 12
        stop = start.replace(year=start.year + later, month=start.month % 12 + 1)
        return start, stop, resolution
    return start, start + _STEPS[resolution], resolution


def _resolution(labels: Iterable[datetime]) -> int:
    """The finest resolution of `labels` as `_span` counts it, at least a day's, as pandas reads it."""

    def finest(t: datetime) -> int:
        parts = (t.hour, t.minute, t.second, t.microsecond)
        return 3 + max((i + 1 for i, part in enumerate(parts) if part), default=0)

    return max(map(finest, labels), default=3)


def _instant(keyword: str, value: Any) -> Any:
    """`value` as an instant: a date string read as one, anything else as it is.

    Raises
    ------
    ValueError
        Naming `keyword`, where a string is no date.
    """
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError as e:
        raise _refusal(keyword, value, "is no date the axis can compare") from e


def _is_scalar(value: Any) -> bool:
    """Whether `value` is one label rather than an iterable of them, a string being one."""
    return isinstance(value, str | bytes) or not isinstance(value, Iterable)


def _listed(matched: Any) -> list[Any]:
    """`matched` as a list, one label being a list of one."""
    return matched if isinstance(matched, list) else [matched]


def _key(label: Any) -> Any:
    """`label` as a set holds it: a tuple's fields, in order, as a tuple."""
    return tuple(label.values()) if isinstance(label, dict) else label


def _declared(
    relations: Mapping[str, DuckDBPyRelation], declared: Mapping[str, Any]
) -> tuple[str, ...]:
    """The keys of `relations` that `declared` holds, in their order."""
    return tuple(k for k in relations if k in declared)


def _dtype(schema: Schema, dim: str) -> DuckDBPyType:
    return schema.duck_type(schema.dimensions[dim])


def _literal(label: Any, dtype: DuckDBPyType) -> Expression:
    """`label` as a constant of `dtype`, a tuple as its struct."""
    if isinstance(label, dict):
        return fn.struct_pack(**{f: lit(v) for f, v in label.items()}).cast(dtype)
    return lit(label).cast(dtype)


def _tuple(
    column: str, fields: Mapping[str, Expression], dtype: DuckDBPyType
) -> Expression:
    """The struct of `fields` as `column` of `dtype`, NULL where `column` is NULL."""
    whole = CaseExpression(col(column).isnull(), lit(None)).otherwise(
        fn.struct_pack(**fields)
    )
    return whole.cast(dtype).alias(column)


def _within(
    rel: DuckDBPyRelation, column: str, kept: DuckDBPyRelation
) -> DuckDBPyRelation:
    """The rows of `rel` NULL on `column` or holding one of `kept` there.

    One left join against `kept` marked, rather than a NULL test beside a
    semi join, which DuckDB plans as a nested loop.
    """
    marked = kept.project(col(column), lit(True).alias("_in")).set_alias("_k")
    return (
        rel.set_alias("_r")
        .join(marked, col("_r", column) == col("_k", column), how="left")
        .filter(col("_r", column).isnull() | col("_k", "_in").isnotnull())
        .project(*(col("_r", c) for c in rel.columns))
    )


def _refusal(keyword: str, value: Any, why: str) -> ValueError:
    """The error refusing `keyword=value`, saying `why`."""
    return _refused({keyword: value}, why)


def _refused(labels: Mapping[str, Any], why: str) -> ValueError:
    """The error refusing `select(**labels)`, saying `why`."""
    asked = ", ".join(f"{k}={v!r}" for k, v in labels.items())
    return ValueError(
        f"`select({asked})` {why} "
        f"(https://energy-models.github.io/datarecord/reference/selecting/)"
    )
