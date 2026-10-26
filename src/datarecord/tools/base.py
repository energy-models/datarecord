# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The tool interface: verify a record, build a model, read it back.

The seam between a tool-agnostic record and one modelling framework. The call runs from the tool inward (`PyPSA.build(revision.record)`), so the
record layer imports nothing from here, and a tool reads through `Record`.
Tools are module-level objects imported by name - no registry.

Notes
-----
- [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from __future__ import annotations

import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from duckdb import CaseExpression, DuckDBPyConnection, DuckDBPyRelation, sqltypes
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import fn, null_safe

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from datarecord.protocols import Flags, RecordLike


def settle(
    rel: DuckDBPyRelation,
    labels: Mapping[str, DuckDBPyRelation],
    flags: Flags,
    *,
    filter: bool = True,
) -> DuckDBPyRelation:
    """Settle the long rows of `rel` by the broadcast rule, one row per coordinate.

    A row NULL on a coordinate in `labels` takes each label there, and a row
    naming a label outside `labels`, or reaching none, is dropped. At each
    coordinate the row naming every coordinate in `labels` that the other rows
    there name is kept. A coordinate outside `labels` is kept as written, a
    NULL there being one more label, and `breakpoint` is no coordinate, so a
    curve is kept or dropped whole.

    Parameters
    ----------
    labels
        Per coordinate, a relation holding its labels, or its tuples for a
        group, in a column of the coordinate's name.
    flags
        The attribute's `Flags`, or flags overstating them; they only spare
        work, so overstating changes no value.
    filter
        False keeps a row naming a label outside `labels` as written, for a
        caller whose own joins on the labels drop it anyway.

    Raises
    ------
    duckdb.Error
        On fetch, where no row names every coordinate the rows reaching it name.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    - [settling rows](https://energy-models.github.io/datarecord/design/tools/#settling-rows)
    """
    over = [d for d in labels if d in rel.columns and (filter or d in flags.broadcast)]
    mixed = [d for d in over if d in flags.broadcast and d in flags.varies]
    if mixed:
        named = reduce(
            operator.add,
            (
                col(d).isnotnull().cast(sqltypes.INTEGER) * lit(1 << i)
                for i, d in enumerate(mixed)
            ),
        )
        rel = rel.project(star(), named.alias("_named"))
    for d in over:
        rows = rel.set_alias("_r")
        if d not in flags.broadcast:
            axis = labels[d].project(col(d)).set_alias("_ax")
            rel = rows.join(axis, col("_r", d) == col("_ax", d), how="semi")
        elif filter:
            rel = _broadcast_onto(rows, labels[d], d)
        else:
            rel = _broadcast_beside(rows, labels[d], d)
    if not mixed:
        return rel
    coordinates = [c for c in rel.columns if c not in ("_named", "breakpoint", "value")]
    over_sql = f"OVER (PARTITION BY {', '.join(str(col(c)) for c in coordinates)})"
    # A window stays SQL text: DuckDB's expression API has no window construct.
    ranked = rel.project(
        star(),
        sql(f"bit_or(_named) {over_sql}").alias("_all"),
        sql(f"max(bit_count(_named)) {over_sql}").alias("_most"),
    )
    coordinate = fn.concat_ws(
        lit(", "),
        *(
            fn.concat(
                lit(f"{c}="), coalesce(col(c).cast(sqltypes.VARCHAR), lit("NULL"))
            )
            for c in coordinates
        ),
    )
    crossed = fn.error(
        fn.concat(
            lit("two rows cross at "),
            coordinate,
            lit(
                " and no row reaching it names both, so neither is the more specific "
                "there (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)"
            ),
        )
    )
    keep = CaseExpression(fn.bit_count(col("_all")) != col("_most"), crossed).otherwise(
        col("_named") == col("_all")
    )
    return ranked.filter(keep).project(star(exclude=("_named", "_all", "_most")))


def _broadcast_onto(
    rows: DuckDBPyRelation, labels: DuckDBPyRelation, d: str
) -> DuckDBPyRelation:
    """`rows` with a NULL `d` taking each of `labels`, and a label not in them dropped.

    One hash join against `labels` keyed by itself and by NULL, so expanding
    and filtering cost a single pass.
    """
    named_to = labels.project(col(d), col(d).alias("_label"))
    null_to = labels.project(lit(None).alias(d), col(d).alias("_label"))
    return rows.join(
        named_to.union(null_to).set_alias("_ax"), null_safe("_r", "_ax", [d])
    ).project(
        *(
            col("_ax", "_label").alias(c) if c == d else col("_r", c)
            for c in rows.columns
        )
    )


def _broadcast_beside(
    rows: DuckDBPyRelation, labels: DuckDBPyRelation, d: str
) -> DuckDBPyRelation:
    """`rows` with a NULL `d` taking each of `labels`, a named row kept as written."""
    # `IS NULL = true` hashes; the one-sided `IS NULL` is a nested loop.
    axis = labels.project(col(d), lit(True).alias("_broadcast"))  # noqa: FBT003
    return (
        rows.join(
            axis.set_alias("_ax"),
            col("_r", d).isnull() == col("_ax", "_broadcast"),
            how="left",
        )
        .project(
            *(
                coalesce(col("_r", d), col("_ax", d)).alias(c)
                if c == d
                else col("_r", c)
                for c in rows.columns
            )
        )
        .filter(col(d).isnotnull())
    )


@dataclass(frozen=True)
class Requirements:
    """What a tool needs from a record, and what a given record fails to supply.

    Parameters
    ----------
    dims : frozenset of str
        Dims the tool cannot build a model without.
    categories : frozenset of str
        Categories the tool requires the record to define labels of, or, in a
        `verify` result, ones it has no counterpart for.
    attributes : frozenset of tuple of (str, str)
        `(category, attribute)` pairs: an attribute the tool requires a value
        for on the labels of that category. Named in the *record's*
        vocabulary, not the tool's, so a caller can act on them: an attribute
        the tool renames or computes (`Schema`) is reported by the source
        attribute it is missing.
    unsupported_keys : frozenset of tuple of (str, str)
        `(key, dim)` pairs the schema declares that this tool cannot honour;
        `key` is `"owner_map_key"`, the fold's unit of ownership. The record layer
        trusts every declared key, so this is a tool's verdict on the record it
        was handed, not a schema rejection.
    unsupported_values : frozenset of tuple of (str, str)
        `(category, attribute)` pairs whose stored *shape* this tool
        cannot represent, as opposed to a value it is missing - a
        piecewise-linear attribute where the tool takes a scalar.
    names : frozenset of str
        Labels the framework scopes per category and two of its categories
        claim, where a record has one axis row per label.
    columns : frozenset of tuple of (str, str)
        `(category, column)` pairs of a framework's own column whose name the
        record declares with another shape, so it has nowhere to be stored:
        one name is one spec record-wide.
    ignored : frozenset of tuple of (str, str)
        `(category, attribute)` pairs the record stores a value for that the
        tool's category does not take, so the build leaves it out. A report,
        not a failure: it leaves the result falsy and `describe` silent.

    Notes
    -----
    - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
    - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    dims: frozenset[str] = frozenset()
    categories: frozenset[str] = frozenset()
    attributes: frozenset[tuple[str, str]] = frozenset()
    unsupported_keys: frozenset[tuple[str, str]] = frozenset()
    unsupported_values: frozenset[tuple[str, str]] = frozenset()
    names: frozenset[str] = frozenset()
    columns: frozenset[tuple[str, str]] = frozenset()
    ignored: frozenset[tuple[str, str]] = frozenset()

    def __bool__(self) -> bool:
        """Whether anything is required (or, for a `verify` result, missing); `ignored` aside."""
        return bool(
            self.dims
            or self.categories
            or self.attributes
            or self.unsupported_keys
            or self.unsupported_values
            or self.names
            or self.columns
        )

    def describe(self) -> str:
        """A one-line summary, for an error message or a log line."""
        parts = []
        if self.dims:
            parts.append(f"dims {sorted(self.dims)}")
        if self.categories:
            parts.append(f"categories {sorted(self.categories)}")
        if self.attributes:
            parts.append(f"attributes {sorted(self.attributes)}")
        if self.unsupported_keys:
            unsupported = ", ".join(
                f"{dim} as {key}" for key, dim in sorted(self.unsupported_keys)
            )
            parts.append(f"unsupported keys ({unsupported})")
        if self.unsupported_values:
            parts.append(
                f"piecewise-linear attributes {sorted(self.unsupported_values)}"
            )
        if self.names:
            parts.append(
                f"labels claimed by more than one category "
                f"{sorted(self.names)} (https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)"
            )
        if self.columns:
            parts.append(
                f"columns whose name is declared with another shape "
                f"{sorted(self.columns)}"
            )
        return ", ".join(parts) if parts else "nothing"


# -- record -> tool attribute mapping ---------------------------------------


@dataclass(frozen=True)
class Attr:
    """One tool attribute and the record attribute(s) it is built from.

    The vocabulary seam: a rename, or a value computed from several,
    declared rather than open-coded in a `build`.

    Parameters
    ----------
    name : str
        The tool's name for the attribute.
    source : tuple of str
        Record attribute name(s) it is read from. A rename has one.
    compute : callable, optional
        Maps one settled long relation per `source` (`settle`), in order, to
        this attribute's long relation; `None` for a plain rename, which requires
        exactly one `source`. A relation in, a relation out, so the plan stays
        unmaterialised and a computed attribute needs no special case downstream.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    name: str
    source: tuple[str, ...]
    compute: Callable[..., DuckDBPyRelation] | None = None

    def __post_init__(self) -> None:
        if self.compute is None and len(self.source) != 1:
            msg = (
                f"attribute {self.name!r} maps {len(self.source)} sources with no "
                "compute; a plain rename needs exactly one"
            )
            raise ValueError(msg)

    def resolve(
        self,
        record: RecordLike,
        labels: Mapping[str, DuckDBPyRelation],
        flags: Mapping[str, Flags],
        *,
        filter: bool = True,
    ) -> DuckDBPyRelation:
        """This attribute's long relation, each source settled (`settle`) before `compute`.

        Parameters
        ----------
        flags
            Per source, its `Flags` as `settle` takes them.
        filter
            As `settle` takes it.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
        - [settling rows](https://energy-models.github.io/datarecord/design/tools/#settling-rows)
        """
        rels = [
            settle(record.attributes[s], labels, flags[s], filter=filter)
            for s in self.source
        ]
        if self.compute is None:
            return rels[0]
        return self.compute(*rels)


@dataclass(frozen=True)
class Schema:
    """A tool's attribute mapping against the record's vocabulary.

    Only differing attributes need an entry; anything unlisted is read under the
    same name, so an empty `Schema` is the identity. `Attr.source` names
    attributes in the *record's* vocabulary.

    Parameters
    ----------
    attrs : dict of str to tuple of Attr
        Per entity type, the attributes that are renamed or computed.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    attrs: dict[str, tuple[Attr, ...]] = field(default_factory=dict)

    def attr(self, ctype: str, name: str) -> Attr:
        """`ctype`'s mapping for `name`, or the identity when it has none."""
        for a in self.attrs.get(ctype, ()):
            if a.name == name:
                return a
        return Attr(name=name, source=(name,))

    def sources(self, ctype: str, name: str) -> tuple[str, ...]:
        """Which record attribute(s) `ctype`'s `name` needs; itself, if unmapped.

        What `verify` checks resolvability against: a renamed or computed
        attribute is satisfied by its sources, not by its own name.
        """
        return self.attr(ctype, name).source

    def resolve(
        self,
        record: RecordLike,
        ctype: str,
        name: str,
        labels: Mapping[str, DuckDBPyRelation],
        flags: Mapping[str, Flags],
        *,
        filter: bool = True,
    ) -> DuckDBPyRelation:
        """`ctype`'s `name` as a long relation over `record`, mapping applied and settled (`Attr.resolve`)."""
        return self.attr(ctype, name).resolve(record, labels, flags, filter=filter)


class UnsupportedRecordError(ValueError):
    """A record does not define everything the tool needs (`Tool.verify`)."""

    def __init__(self, tool: str, missing: Requirements) -> None:
        super().__init__(
            f"record cannot build a {tool} model; missing: {missing.describe()}"
        )
        self.missing = missing


@runtime_checkable
class Tool(Protocol):
    """One modelling framework's view of a record.

    Implementations are stateless (a module-level singleton is fine): every
    method takes the record or model it operates on. A structural type, for
    annotating code that takes any tool - not a dispatch table; tools are
    reached by importing them.

    A `Record` rather than a record throughout, so a tool builds from a
    directory as readily as from an overlay and has no reason to know layering
    exists.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    name: str
    schema: Schema

    def requires(self, record: RecordLike) -> Requirements:
        """What this tool needs from `record` to build a model.

        Record-dependent, not a constant: which attributes are required
        follows the categories the record's labels have and its declared
        schema.
        """
        ...

    def verify(self, record: RecordLike) -> Requirements:
        """What `record` fails to supply; falsy when the record is usable."""
        ...

    def build(self, record: RecordLike) -> Any:
        """The tool's model object, built from the resolved record."""
        ...

    def to_datarecord(
        self,
        model: Any,
        con: DuckDBPyConnection,
        *,
        results_only: bool = False,
        base: RecordLike | None = None,
    ) -> RecordLike:
        """`model` presented as a layer `write_layer` can persist.

        The inverse of `build`. Framework-specific: undoing a framework's own
        shape is exactly what a tool knows and the record layer does not.

        Parameters
        ----------
        con
            The connection the relations are planned on - in practice `ws.con` of
            the workspace that will write them.
        results_only
            Present only what a solve produced - its result attributes and the
            intermediates they need - as a thin layer to write as a child of
            `base`.
        base
            The record `model` was built from: required with `results_only`,
            refused without it.

        Raises
        ------
        ValueError
            If `base` is given without `results_only`, or missing with it.

        Notes
        -----
        - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        ...
