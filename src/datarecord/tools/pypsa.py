# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The PyPSA tool: record -> `pypsa.Network` -> record.

The only module that knows PyPSA's network shape - that its axes are
`snapshot`/`period`/`scenario`, that a stochastic network is indexed by
`(scenario, name)`, that its buses and carriers are the `bus` and `carrier`
axes, and how its static/series split maps onto the record's `dims/` +
`attributes/` split.

Notes
-----
- [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from __future__ import annotations

import math
import re
import warnings
from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cache, cached_property, reduce
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple
from uuid import uuid4

import numpy as np
import pandas as pd
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import (
    DuckDBPyConnection,
    DuckDBPyRelation,
    Expression,
    sqltypes,
)
from duckdb import StarExpression as star
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from datarecord.duck import empty_relation, ex_all, fn, null_safe, struct_of
from datarecord.protocols import Flags, LazyRelations, RecordLike, Relations
from datarecord.resolve.owner_map import cast_declared
from datarecord.resolve.record import Record
from datarecord.schema.categories import matching, with_defaults
from datarecord.schema.schema import Schema as RecordSchema
from datarecord.schema.specs import (
    AttributeSpec,
    Dimension,
    PropertySpec,
)
from datarecord.settle import settle
from datarecord.tools.base import (
    Requirements,
    Schema,
    Tool,
    UnsupportedRecordError,
)

if TYPE_CHECKING:
    import pypsa

SNAPSHOT, PERIOD, SCENARIO = "snapshot", "period", "scenario"
#: PyPSA's name for a multi-period snapshot's inner level: the dim of those
#: timesteps, and the property of `snapshot` that refers to it where the
#: labels are positions (`_snapshot_frame`).
TIMESTEP = "timestep"
# Not PyPSA's `weight`: that name is a Generator attribute, and a name holds one spec record-wide.
SCENARIO_WEIGHT = "scenario_weight"
ENTITY, BUS, CARRIER, CONNECTION = "entity", "bus", "carrier", "connection"
SUB_NETWORK = "sub_network"
CTYPE = "ctype"
#: `ctype` as a `default_by` names it, by its subject.
BY_CTYPE = f"{ENTITY}.{CTYPE}"
#: The PyPSA types whose members are labels of a dim of their own, not entities.
_AXIS_TYPES = {"Bus": BUS, "Carrier": CARRIER, "SubNetwork": SUB_NETWORK}
#: The registry inputs declared as properties, by subject: what describes a
#: label rather than data a model optimises against. `entity` also has
#: `ctype`, and a `carrier` property is one of the `carrier` dim.
_PROPERTIES = {
    ENTITY: ("carrier", "sign", "type"),
    BUS: ("carrier", "type", "x", "y", "unit", "location"),
    CARRIER: ("color", "nice_name"),
    SUB_NETWORK: (CARRIER,),
}
#: How a SubNetwork's `carrier` is described, PyPSA registering it as an output.
_SUB_NETWORK_CARRIER = "The carrier of a sub-network's buses."
#: A property of the `connection` multi-dim, and the dim with `one_of`
#: that holds its labels. The port vocabulary is this tool's.
ROLE = "role"
REQUIRED_DIMS = frozenset({SNAPSHOT, PERIOD, SCENARIO})
#: The `meta` key naming, per entity type, the custom static columns it was
#: exported with: the part of `_taken`'s vocabulary the registry lacks.
_CUSTOM = "custom_columns"
#: How close two floats held by members of a collection agree, as `numpy.isclose`'s defaults.
_RTOL, _ATOL = 1e-5, 1e-8


@dataclass(frozen=True)
class _Weighting:
    """One column of the weighting frame PyPSA keeps beside an axis."""

    dim: str
    column: str
    description: str


#: Every axis weighting, by the attribute over its axis it is stored as.
_WEIGHTINGS = {
    SCENARIO_WEIGHT: _Weighting(
        SCENARIO, "weight", "How much this scenario counts in the expectation."
    ),
    "snapshot_weighting_objective": _Weighting(
        SNAPSHOT, "objective", "How much this snapshot counts in the objective."
    ),
    "snapshot_weighting_stores": _Weighting(
        SNAPSHOT, "stores", "How long this snapshot lasts for a store's energy."
    ),
    "snapshot_weighting_generators": _Weighting(
        SNAPSHOT,
        "generators",
        "How much this snapshot counts towards a generator's energy limits.",
    ),
    "period_weighting_objective": _Weighting(
        PERIOD, "objective", "How much this period counts in the objective."
    ),
    "period_weighting_years": _Weighting(
        PERIOD, "years", "How many years this period lasts."
    ),
}

#: PyPSA's `defaults["typ"]` -> the mathspec dtype stored, `str` for anything unlisted.
_DTYPES = {bool: "bool", int: "int", float: "float", str: "str"}
#: PyPSA's `typ` of a geometry column, which a record stores as WKT text.
GEOMETRY = "geometry"


def _default(value: Any) -> Any:
    """One `defaults["default"]` cell as JSON-storable, NaN as absent."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _same(a: Any, b: Any) -> bool:
    """Whether two `_default` values are one default, `True` and `1` being two."""
    return type(a) is type(b) and a == b


def _text(value: Any) -> str | None:
    """One `unit`/`description` cell as text, None (undeclared, not blank) where PyPSA has none.

    Notes
    -----
    - [unit and description](https://energy-models.github.io/datarecord/design/model/schema/#unit-and-description)
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class NetworkShape:
    """PyPSA's reading of a record's axis frames: `snapshot` is time, a non-empty `scenario` stochastic.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    dims: Relations
    properties: Relations

    def _axis(self, dim: str) -> pd.DataFrame:
        """`dim`'s axis as a DataFrame, empty where the record holds no rows of it."""
        rel = self.dims.get(dim)
        return rel.df() if rel is not None else pd.DataFrame(columns=[dim])

    @cached_property
    def snapshots(self) -> pd.DataFrame:
        """The `snapshot` axis in axis order, with its properties where it has any."""
        axis = self._axis(SNAPSHOT)
        frame = self.properties.get(SNAPSHOT)
        if frame is None:
            return axis
        return axis.merge(frame.df(), on=SNAPSHOT, how="left")

    @property
    def snapshot_levels(self) -> pd.DataFrame:
        """`snapshots` as PyPSA's index levels: `(period, timestep)` where a snapshot has a period, else the timestep.

        The timestep is `snapshot.timestep` where declared, else the label
        (`_snapshot_frame`), so one period selected out of several is indexed by its timesteps.
        """
        timestep = self.snapshots.get(TIMESTEP, self.snapshots[SNAPSHOT])
        if PERIOD not in self.snapshots:
            return pd.DataFrame({SNAPSHOT: timestep})
        return pd.DataFrame({PERIOD: self.snapshots[PERIOD], TIMESTEP: timestep})

    @cached_property
    def periods(self) -> pd.DataFrame:
        return self._axis(PERIOD)

    @cached_property
    def scenarios(self) -> pd.DataFrame:
        return self._axis(SCENARIO)

    @property
    def stochastic(self) -> bool:
        return not self.scenarios.empty

    @property
    def multiperiod(self) -> bool:
        return not self.periods.empty

    @property
    def index_names(self) -> list[str]:
        """The static frame's index: PyPSA carries `scenario` as a level."""
        return [SCENARIO, "entity"] if self.stochastic else ["entity"]


def _connection(record: RecordLike) -> DuckDBPyConnection:
    """The DuckDB connection `record`'s relations belong to, which `PIVOT` needs.

    Raises
    ------
    TypeError
        If `record` is not a `Record`.
    """
    while isinstance(record, _Selection):
        record = record.record
    if not isinstance(record, Record):
        msg = (
            f"the PyPSA tool builds from a Record, not a {type(record).__name__}: "
            "it runs DuckDB SQL on the record's connection"
        )
        raise TypeError(msg)
    return record.con


def _own_dims(schema: RecordSchema) -> list[str]:
    """The dims `schema` declares that PyPSA's does not: a collection's, in declared order.

    Notes
    -----
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    """
    return [d for d in schema.dimensions if d not in _registry().schema.dimensions]


def _rebuilt(schema: RecordSchema, **fields: Any) -> RecordSchema:
    """`schema` with `fields` replaced, validated afresh."""
    return RecordSchema(**{**dict(schema), **fields})


#: The dims `build` selects labels of without squeezing them, a scalar being a one-label list.
_FILTERED = (SNAPSHOT, BUS, ENTITY, CARRIER)
#: The dims `build` slices, with a record's own, where their labels are numbers or datetimes.
_ORDERED = (SNAPSHOT, PERIOD)
#: PyPSA's `build_year` and `lifetime` where a record declares no default.
_ACTIVE_FALLBACK = {"build_year": 0, "lifetime": math.inf}
#: The period a `GlobalConstraint` applies in, NULL applying in every period.
_INVESTMENT_PERIOD = "investment_period"


@dataclass(frozen=True)
class _Selection:
    """`record` at one label of each dim in `squeeze`, and at the labels in `keep` of the others.

    A squeezed dim's attribute rows are those at its label and those NULL on
    it, settled, the dim dropped, with every attribute over squeezed dims
    alone and every property referring to one; a squeezed dim of the record's
    own (`_own_dims`) leaves the schema. A kept dim's axis, properties and
    attribute rows are those at a kept label, a row NULL on it kept, and
    `entity` and `bus` keep the `connection` tuples by their fields. A
    squeezed `period` drops `investment_period`, a single-period network
    holding no period to name.
    `scenario_weight` over kept scenarios is renormalised to sum to 1, as
    PyPSA requires.

    Notes
    -----
    - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """

    record: RecordLike
    squeeze: Mapping[str, Any]
    keep: Mapping[str, list[Any]] = field(default_factory=dict)

    @cached_property
    def schema(self) -> RecordSchema:
        schema = self.record.schema
        own = self.squeeze.keys() & set(_own_dims(schema))
        return _rebuilt(
            schema,
            dimensions={d: s for d, s in schema.dimensions.items() if d not in own},
            properties={
                subject: {
                    n: s for n, s in specs.items() if s.one_of not in self.squeeze
                }
                for subject, specs in schema.properties.items()
            },
            attributes={
                name: spec.model_copy(update={"dims": spec.dims - own})
                for name, spec in schema.attributes.items()
                if not (spec.dims and spec.dims <= self.squeeze.keys())
                and not (name == _INVESTMENT_PERIOD and PERIOD in self.squeeze)
            },
            partial=(schema.partial or frozenset()) - own,
        )

    @property
    def dims(self) -> LazyRelations:
        dims = self.record.dims
        kept = tuple(d for d in dims if d not in self.squeeze)
        return LazyRelations(kept, lambda dim: self._kept(dims[dim]))

    @property
    def properties(self) -> LazyRelations:
        return LazyRelations(tuple(self.record.properties), self._properties_of)

    @property
    def attributes(self) -> LazyRelations:
        names = tuple(a for a in self.record.attributes if a in self.schema.attributes)
        return LazyRelations(names, self._attribute)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """`record`'s flags, overstating the selection's."""
        flags = self.record.flags(**categories)
        return {a: f for a, f in flags.items() if a in self.schema.attributes}

    def _properties_of(self, subject: str) -> DuckDBPyRelation:
        rel = self._kept(self.record.properties[subject])
        specs = self.record.schema.properties.get(subject, {})
        gone = [n for n, s in specs.items() if s.one_of in self.squeeze]
        return rel.project(star(exclude=gone)) if gone else rel

    def _attribute(self, name: str) -> DuckDBPyRelation:
        if name == SCENARIO_WEIGHT and SCENARIO in self.keep:
            return self._scenario_weights()
        rel = self.record.attributes[name]
        labels = {
            d: self.record.dims[d].filter(col(d) == lit(label))
            for d, label in self.squeeze.items()
            if d in rel.columns
        }
        if labels:
            over = frozenset(labels)
            settled = settle(rel, labels, Flags(over, over))
            rel = settled.project(star(exclude=list(labels)))
        return self._kept(rel)

    def _kept(self, rel: DuckDBPyRelation) -> DuckDBPyRelation:
        """`rel`'s rows at a kept label, or NULL, of each dim in `keep` it carries, `connection`'s fields among them, in `rel`'s order."""
        for dim in self.keep:
            if dim in rel.columns:
                on = col(dim)
            elif dim in (ENTITY, BUS) and CONNECTION in rel.columns:
                on = fn.struct_extract(col(CONNECTION), lit(dim))
            else:
                continue
            labels = self._labels[dim]  # noqa: F841 - bound by name for the query below
            kept = f"(SELECT list({col(dim)}) FROM labels)"
            rel = rel.filter(f"{on} IS NULL OR list_contains({kept}, {on})")
        return rel

    @cached_property
    def _labels(self) -> dict[str, DuckDBPyRelation]:
        """The labels of each dim in `keep`, for `_kept` to filter against.

        `_kept` reads them as one list rather than joining them: DuckDB may
        flip a join, or one downstream of it, losing `rel`'s row order.
        """
        con = _connection(self.record)
        return {
            dim: con.from_df(pd.DataFrame({dim: labels}))
            for dim, labels in self.keep.items()
        }

    def _scenario_weights(self) -> DuckDBPyRelation:
        """The kept scenarios' weights, each as `_weightings` reads it over the whole axis, renormalised."""
        con = _connection(self.record)
        whole = replace(
            self, keep={d: v for d, v in self.keep.items() if d != SCENARIO}
        )
        index = pd.Index(_axis_labels(whole, SCENARIO), name=SCENARIO)
        equal = pd.DataFrame({"weight": 1 / len(index)}, index=index)
        kept = _weightings(whole, SCENARIO, equal, con)["weight"].loc[
            self.keep[SCENARIO]
        ]
        if not kept.sum() > 0:
            msg = f"`select` keeps scenarios {list(kept.index)} weighing nothing in all"
            raise ValueError(msg)
        return con.from_df((kept / kept.sum()).rename("value").reset_index())


def _axis_labels(record: RecordLike, dim: str) -> list[Any]:
    """`dim`'s labels in axis order, none where the record holds no axis of it."""
    axis = record.dims.get(dim)
    return [] if axis is None else [label for (label,) in axis.fetchall()]


def _is_scalar(value: Any) -> bool:
    """Whether `value` is one label rather than an iterable or a slice of them, a string being one."""
    return isinstance(value, str) or not isinstance(value, Iterable | slice)


def _keyed_axis(record: RecordLike, dim: str) -> pd.Series:
    """`dim`'s labels in axis order, indexed by what `select` matches: a snapshot's `timestep` where declared, else the label.

    A multi-period record whose timesteps repeat labels its snapshots by
    position (`_snapshot_frame`), so a timestamp names one in each period.
    """
    labels = _axis_labels(record, dim)
    keys: Any = labels
    if dim == SNAPSHOT and TIMESTEP in record.schema.properties.get(SNAPSHOT, {}):
        frame = record.properties[SNAPSHOT].project(col(SNAPSHOT), col(TIMESTEP)).df()
        keys = frame.set_index(SNAPSHOT)[TIMESTEP].reindex(labels)
    return pd.Series(labels, index=pd.Index(keys), dtype=object)


def _labels(record: RecordLike, dim: str, value: Any) -> tuple[list[Any], bool]:
    """The labels of `dim` that `value`, one key, a list or a slice of them, names, in axis order, and whether it names one label.

    A key is matched as pandas' `.loc` matches it on the sorted keys
    (`_keyed_axis`), so a partial date string names every label in its span
    and so keeps the dim. A key in a list is matched exactly, read by
    `pd.Timestamp` on a datetime axis. A slice is pandas' label slice: both
    ends inclusive, a partial date string covering its span.

    Raises
    ------
    ValueError
        If `value` names no label or one the axis lacks, or is a slice with a
        step, of a dim `_ORDERED` leaves out or of an axis neither numeric nor
        datetime, with an end the axis cannot compare, or matching nothing.
    """
    keyed = _keyed_axis(record, dim)
    ordered = keyed.sort_index(kind="stable")
    if isinstance(value, slice):
        sortable = is_numeric_dtype(keyed.index) or is_datetime64_any_dtype(keyed.index)
        orderable = dim in _ORDERED or dim in _own_dims(record.schema)
        if value.step is not None or not (orderable and sortable):
            msg = (
                f"`select` slices `{dim}` by a step or along an unordered axis: {value}"
            )
            raise ValueError(msg)
        try:
            within = ordered.loc[value]
        except (KeyError, TypeError) as err:
            msg = f"`select` slices `{dim}` by an end its axis cannot compare: {value}"
            raise ValueError(msg) from err
        if within.empty:
            msg = f"`select` slices no label of `{dim}`: {value}"
            raise ValueError(msg)
        return keyed[keyed.isin(within)].tolist(), False
    if _is_scalar(value):
        try:
            found = ordered.loc[value]
        except (KeyError, TypeError) as err:
            msg = f"`select` names {value!r}, a label `{dim}`'s axis lacks"
            raise ValueError(msg) from err
        if isinstance(found, pd.Series):
            return keyed[keyed.isin(found)].tolist(), False
        return [found], True
    requested = list(value)
    if isinstance(keyed.index, pd.DatetimeIndex):
        requested = [pd.Timestamp(key) for key in requested]
    unknown = [key for key in requested if key not in keyed.index]
    if not requested or unknown:
        held = f"labels {unknown} its axis lacks" if unknown else "no label"
        msg = f"`select` names {held} of `{dim}`"
        raise ValueError(msg)
    return keyed[keyed.index.isin(requested)].tolist(), False


def _resolved(record: RecordLike, attr: str, fallback: float) -> pd.Series:
    """`attr` of each entity, indexed by entity: its row, else the declared default, else `fallback`.

    Raises
    ------
    ValueError
        If an entity holds several values: the selection keeps a dim it varies over.
    """
    schema = record.schema
    frame = record.properties[ENTITY].project(col(ENTITY), col(CTYPE)).df()
    entities = {ENTITY: record.dims[ENTITY]}
    over = Flags(frozenset(entities), frozenset(entities))
    declared = pd.Series(None, index=frame.index, dtype=object)
    if attr in schema.attributes:
        declared = frame[CTYPE].map(lambda c: schema.default_for(attr, **{CTYPE: c}))
    default = declared.where(declared.notna(), fallback).astype(float)
    rows = record.attributes.get(attr)
    held = pd.DataFrame(columns=[ENTITY, "value"])
    if rows is not None:
        held = settle(rows, entities, over).df()
    kept = [c for c in held.columns if c in schema.dimensions and c != ENTITY]
    if held[kept].notna().any(axis=None):
        msg = f"`{attr}` varies over a dim the selection keeps, of {kept}: select one label of it"
        raise ValueError(msg)
    by_entity = held.set_index(ENTITY)["value"]
    values = frame[ENTITY].map(by_entity).astype(float).fillna(default)
    return values.set_axis(frame[ENTITY])


def _active(record: RecordLike, period: int) -> set[Any]:
    """The entities PyPSA holds active in `period`: `build_year <= period < build_year + lifetime`.

    An entity with no row takes the declared default, else PyPSA's.

    Raises
    ------
    ValueError
        If an entity holds several values of either: the selection keeps a dim it varies over.
    """
    start, lifetime = (_resolved(record, a, f) for a, f in _ACTIVE_FALLBACK.items())
    active = (start <= period) & (period < start + lifetime)
    return set(active.index[active])


def _applying(record: RecordLike, periods: list[Any]) -> set[Any]:
    """The entities whose `investment_period`, its declared default applied, is NULL or one of `periods`.

    Raises
    ------
    ValueError
        If an entity holds several values: the selection keeps a dim it varies over.
    """
    invested = _resolved(record, _INVESTMENT_PERIOD, math.nan)
    return set(invested.index[invested.isna() | invested.isin(periods)])


def _select(record: RecordLike, select: Mapping[str, Any]) -> _Selection:
    """`record` at the labels `select` names, and at those referring to none it drops.

    A key naming one label of `period`, `scenario` or a dim of the record's
    own squeezes the dim; anything else keeps it at the labels named.

    Raises
    ------
    ValueError
        If `select` names a dim it cannot select, no label, an unknown one or
        a slice `_labels` refuses, or leaves no snapshot.

    Notes
    -----
    - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
    """
    schema = record.schema
    squeezable = {PERIOD, SCENARIO, *_own_dims(schema)}
    selectable = [d for d in schema.dimensions if d in squeezable or d in _FILTERED]
    squeeze: dict[str, Any] = {}
    keep: dict[str, list[Any]] = {}
    for dim, value in select.items():
        if dim not in selectable:
            msg = f"`select` takes labels of {selectable}, not of `{dim}`"
            raise ValueError(msg)
        labels, one = _labels(record, dim, value)
        if dim in squeezable and one:
            squeeze[dim] = labels[0]
        else:
            keep[dim] = labels
    keep = _propagated(record, squeeze, keep)
    if keep.get(SNAPSHOT) == []:
        msg = f"`select={dict(select)!r}` leaves no snapshot"
        raise ValueError(msg)
    return _Selection(record, squeeze, keep)


def _propagated(
    record: RecordLike, squeeze: Mapping[str, Any], keep: Mapping[str, list[Any]]
) -> dict[str, list[Any]]:
    """`keep`, narrowed to the labels referring to no label the selection drops.

    A label whose property refers to a selected dim stays where the value is
    selected or NULL, naming no label. A period holding no kept snapshot goes,
    since PyPSA takes no period without one. An entity attached to a dropped
    bus goes, and over a squeezed `period` one PyPSA holds inactive in it
    (`_active`). Over selected periods an entity whose `investment_period`
    names another goes (`_applying`).
    """
    chosen = {**{d: [label] for d, label in squeeze.items()}, **keep}
    schema = record.schema
    con = _connection(record)
    kept = dict(keep)

    def narrow(dim: str, allowed: Container[Any]) -> None:
        labels = kept[dim] if dim in kept else _axis_labels(record, dim)
        kept[dim] = [label for label in labels if label in allowed]

    for subject, specs in schema.properties.items():
        refs = [n for n, s in specs.items() if s.one_of in chosen]
        if not refs or subject not in record.properties:
            continue
        frame = with_defaults(
            schema, subject, record.properties[subject], refs, con
        ).df()
        selected = [
            frame[n].isna() | frame[n].isin(chosen[str(specs[n].one_of)]) for n in refs
        ]
        narrow(subject, set(frame.loc[np.logical_and.reduce(selected), subject]))
    timed = PERIOD in schema.properties.get(SNAPSHOT, {})
    if SNAPSHOT in kept and PERIOD not in squeeze and timed:
        periods = record.properties[SNAPSHOT].project(col(SNAPSHOT), col(PERIOD)).df()
        held = periods.loc[periods[SNAPSHOT].isin(kept[SNAPSHOT]), PERIOD]
        narrow(PERIOD, set(held))
    connections = _connection_frame(record) if BUS in kept else None
    if connections is not None:
        detached = connections.loc[~connections[BUS].isin(kept[BUS]), ENTITY]
        narrow(ENTITY, set(_axis_labels(record, ENTITY)) - set(detached))
    if PERIOD in squeeze:
        view = _Selection(record, squeeze, dict(kept))
        narrow(ENTITY, _active(view, squeeze[PERIOD]))
    periods = [squeeze[PERIOD]] if PERIOD in squeeze else kept.get(PERIOD)
    if periods is not None:
        others = {d: label for d, label in squeeze.items() if d != PERIOD}
        narrow(ENTITY, _applying(_Selection(record, others, dict(kept)), periods))
    return kept


def _snapshot_frame(snapshots: pd.Index) -> pd.DataFrame:
    """`n.snapshots` as the `snapshot` axis with its properties, in PyPSA's order.

    A multi-period snapshot's label is its timestep where none repeats across
    periods, else its position, the timestep then its `timestep` property.
    """
    if not isinstance(snapshots, pd.MultiIndex):
        return snapshots.to_frame(index=False)
    period = snapshots.get_level_values(PERIOD)
    timestep = snapshots.get_level_values(TIMESTEP)
    if timestep.is_unique:
        return pd.DataFrame({SNAPSHOT: timestep, PERIOD: period})
    return pd.DataFrame(
        {SNAPSHOT: np.arange(len(snapshots)), PERIOD: period, TIMESTEP: timestep}
    )


def _snapshot_properties(snapshots: pd.DataFrame) -> dict[str, PropertySpec]:
    """The properties of a multi-period network's `snapshot`, by `_snapshot_frame`'s columns."""
    properties = {
        PERIOD: PropertySpec(
            one_of=PERIOD,
            description="The investment period a snapshot falls in.",
        )
    }
    if TIMESTEP in snapshots:
        properties[TIMESTEP] = PropertySpec(
            one_of=TIMESTEP,
            description="PyPSA's timestep of a snapshot labelled by its position.",
        )
    return properties


# -- long -> wide -----------------------------------------------------------


@dataclass(frozen=True)
class Index:
    """A pandas Index paired with a DuckDB relation carrying `_pos`, its row position, to restore its order."""

    index: pd.Index
    rel: DuckDBPyRelation

    @classmethod
    def of(
        cls,
        index: pd.Index,
        con: DuckDBPyConnection,
        keys: pd.DataFrame | None = None,
    ) -> Index:
        """`index` keyed by its own levels, or by `keys`, one row per entry in its order."""
        df = index.to_frame(index=False) if keys is None else keys  # noqa: F841 - queried by name below
        return cls(index, con.sql("SELECT *, row_number() OVER () AS _pos FROM df"))

    def expand(self, rel: DuckDBPyRelation, column: str) -> DuckDBPyRelation:
        """Broadcast rows with a null `column` across this index, others as-is; `rel` must carry its alias."""
        if len(self.index) == 0:
            return rel
        # `IS NULL = true` hashes; the one-sided `IS NULL` is a nested loop.
        axis = self.rel.project(col(column), lit(True).alias("_broadcast"))  # noqa: FBT003
        return rel.join(
            axis.set_alias("_ax"),
            col(rel.alias, column).isnull() == col("_ax", "_broadcast"),
            how="left",
        ).project(
            *(col(rel.alias, c) for c in rel.columns if c != column),
            coalesce(col(rel.alias, column), col("_ax", column)).alias(column),
        )

    def left_join(self, other: DuckDBPyRelation, on: Sequence[str]) -> DuckDBPyRelation:
        """LEFT JOIN `other` onto this index, restored to `index`'s row order."""
        return self.rel.join(
            other,
            ex_all(col(self.rel.alias, f) == col(other.alias, f) for f in on),
            how="left",
        ).order(f"{self.rel.alias}._pos")


def _assign_static(
    static: DuckDBPyRelation,
    attributes: dict[str, tuple[DuckDBPyRelation, Flags]],
    shape: NetworkShape,
    scenarios: Index,
    con: DuckDBPyConnection,
) -> pd.DataFrame:
    """`static`, repeated per scenario where stochastic, with each attribute's static rows assigned: the wide frame in `_pos` order.

    An attribute addressed by `snapshot` is static only on its NULL-snapshot
    rows; one not addressed by it is static throughout.

    Notes
    -----
    - [flags](https://energy-models.github.io/datarecord/design/use/reading/#flags)
    """
    keys = shape.index_names
    if shape.stochastic and SCENARIO not in static.columns:
        static = scenarios.expand(
            static.project(f"*, NULL AS {SCENARIO}").set_alias("m"), SCENARIO
        )

    picked: dict[str, DuckDBPyRelation] = {}
    over_snapshots: set[str] = set()
    for attr, (long, flags) in attributes.items():
        addressed = SNAPSHOT in (flags.varies | flags.broadcast)
        if addressed and SNAPSHOT not in flags.broadcast:
            continue
        rows = long
        if shape.stochastic and SCENARIO not in rows.columns:
            rows = rows.project(f"*, CAST(NULL AS VARCHAR) AS {SCENARIO}")
            rows = scenarios.expand(rows, SCENARIO)
        picked[attr] = rows
        if addressed:
            over_snapshots.add(attr)

    frame = _left_join_values(static, picked, over_snapshots, keys, con)
    if shape.stochastic:
        frame = frame.sort_values(
            SCENARIO,
            kind="stable",
            key=lambda s: s.map({v: i for i, v in enumerate(scenarios.index)}),
        )
    return frame.set_index(keys)


def _left_join_values(
    static: DuckDBPyRelation,
    values: Mapping[str, DuckDBPyRelation],
    over_snapshots: Container[str],
    keys: Sequence[str],
    con: DuckDBPyConnection,
) -> pd.DataFrame:
    """`static` with each of `values` left-joined on `keys` as its named column, in `_pos` order, `_pos` dropped.

    A value fills its column where `static` has none, and one with no static
    row is dropped. Each relation is held in a table of its own for the joins:
    chaining them over the deep lazy trees is quadratic in the attribute count.

    Parameters
    ----------
    values
        Long rows carrying `keys` and `value`, one per key, by the column they fill.
    over_snapshots
        Names in `values` of whose rows only the NULL-snapshot ones are static.
    """
    tag = uuid4().hex
    tables = [f"_pypsa_static_{tag}"]
    static.create(tables[0])
    try:
        joined = con.table(tables[0]).set_alias("s")
        exprs = {}
        for i, (name, long) in enumerate(values.items()):
            rows = (
                long.filter(col(SNAPSHOT).isnull()) if name in over_snapshots else long
            )
            tables.append(f"_pypsa_value_{tag}_{i}")
            rows.project(*(col(k) for k in keys), col("value")).create(tables[-1])
            alias = f"a{i}"
            picked = con.table(tables[-1]).set_alias(alias)
            joined = joined.join(
                picked, ex_all(col("s", k) == col(alias, k) for k in keys), how="left"
            )
            value = col(alias, "value")
            exprs[name] = (
                coalesce(value, col("s", name)) if name in static.columns else value
            ).alias(name)
        return (
            joined.project(
                *(col("s", c) for c in static.columns if c not in exprs),
                *exprs.values(),
            )
            .order("_pos")
            .project(star(exclude=["_pos"]))
            .df()
        )
    finally:
        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS {table}")


def _series_frame(
    rows: DuckDBPyRelation,
    shape: NetworkShape,
    snapshots: Index,
    static_index: pd.Index,
    con: DuckDBPyConnection,
) -> pd.DataFrame:
    """Pivot long series rows to the `n.snapshots x static_index` frame.

    Only the columns the rows pivot, in `static_index` order: PyPSA takes
    every column given as a series, so completing them would move a static
    component into `dynamic`. The pivot is on a column's position, so a name
    never becomes an SQL identifier.
    """
    group_by = [SNAPSHOT]
    keys = shape.index_names
    columns = Index.of(static_index.set_names(keys), con).rel.set_alias("k")
    rows = rows.set_alias("r")
    positioned = rows.join(  # noqa: F841 - bound by name for the query below
        columns, ex_all(col("r", k) == col("k", k) for k in keys)
    ).project(
        *(col("r", g) for g in group_by),
        col("k", "_pos").alias("_col"),
        col("r", "value"),
    )
    piv = con.sql(
        f"PIVOT positioned ON _col USING first(value) GROUP BY {', '.join(group_by)}"
    )

    present = sorted(int(c) for c in set(piv.columns) - set(group_by))
    if not present:
        return pd.DataFrame(index=snapshots.index, columns=static_index[:0])

    wide = (
        snapshots.left_join(piv.set_alias("piv"), group_by)
        .project(*(col(str(p)) for p in present))
        .df()
    )
    wide.index = snapshots.index
    wide.columns = static_index[[p - 1 for p in present]]
    return wide


# -- build -------------------------------------------------------------------


def _own_static(c: pypsa.Components) -> pd.DataFrame:
    """`c.static` less the rows named as in PyPSA's standard type library, edited or not.

    Notes
    -----
    - [standard types](https://energy-models.github.io/datarecord/design/tools/pypsa/#standard-types)
    """
    library = c.standard_types
    if library is None:
        return c.static
    level = "name" if isinstance(c.static.index, pd.MultiIndex) else None
    return c.static.drop(library.index, level=level, errors="ignore")


def _exported(c) -> bool:
    """Whether `c`'s members belong in a record at all: those it holds beyond PyPSA's library (`_own_static`)."""
    return not _own_static(c).empty


def _as_entities(c) -> bool:
    """Whether `c`'s members are written as entities, not as labels of their own dim."""
    return _exported(c) and c.name not in _AXIS_TYPES


def _names(index: pd.Index) -> pd.Index:
    """The `name` level of a static index, `(scenario, name)` on a stochastic network."""
    return index.get_level_values("name") if isinstance(index, pd.MultiIndex) else index


def _unscoped(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """`frame` less `scenario`, one row per `keys`: a member or connection is not scoped per scenario."""
    if SCENARIO not in frame.columns:
        return frame
    return frame.drop(columns=[SCENARIO]).drop_duplicates(subset=keys)


def _union_all(rels: Sequence[DuckDBPyRelation]) -> DuckDBPyRelation | None:
    """`rels` as one positional `UNION ALL`, `None` where there are none."""
    return reduce(DuckDBPyRelation.union, rels) if rels else None


def _dim_of(ctype: str) -> str:
    """The dim `ctype`'s members are labels of: `entity`, or their own (`_AXIS_TYPES`)."""
    return _AXIS_TYPES.get(ctype, ENTITY)


def _keyed_as_entity(rel: DuckDBPyRelation, dim: str) -> DuckDBPyRelation:
    """`rel` with its `dim` column named `entity`, the key PyPSA's frames are built on."""
    if dim == ENTITY:
        return rel
    return rel.project(
        *(col(c).alias(ENTITY) if c == dim else col(c) for c in rel.columns)
    )


def _none_for_empty(frame: pd.DataFrame) -> pd.DataFrame:
    """`frame` with an empty `carrier`, PyPSA's "none", as NULL: it names no label."""
    if CARRIER not in frame:
        return frame
    return frame.assign(**{CARRIER: frame[CARRIER].where(frame[CARRIER] != "", None)})


def _empty_for_none(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """`rel` with a NULL `carrier` as PyPSA's `""`, the inverse of `_none_for_empty`."""
    if CARRIER not in rel.columns:
        return rel
    return rel.project(
        *(
            coalesce(col(c), lit("")).alias(c) if c == CARRIER else col(c)
            for c in rel.columns
        )
    )


def _colliding_names(n: pypsa.Network) -> frozenset[str]:
    """Names more than one entity type claims, read off the `name` level of a stochastic index.

    Notes
    -----
    - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
    """
    seen: dict[str, str] = {}
    clashing: set[str] = set()
    for c in n.components:
        if not _as_entities(c):
            continue
        for value in _names(_own_static(c).index):
            name = str(value)
            if seen.setdefault(name, c.name) != c.name:
                clashing.add(name)
    return frozenset(clashing)


def _custom_names(c: pypsa.Components) -> list[str]:
    """`c`'s static columns PyPSA's registry has no entry for, but `_DERIVED`."""
    derived = _DERIVED.get(c.name, frozenset())
    return [
        name
        for name in c.static.columns
        if name not in c.defaults.index and name not in derived
    ]


@dataclass(frozen=True)
class _Custom:
    """A network's custom static columns, as the schema declares them.

    A text column, or one that as an attribute would hold one name over two
    dims, is a property of its type's dim; any other an attribute over it.

    Notes
    -----
    - [custom columns](https://energy-models.github.io/datarecord/design/tools/pypsa/#custom-columns)
    """

    #: Exported type -> its custom columns and their dtypes.
    columns: dict[str, dict[str, str]]
    #: Dim -> its custom properties and their dtypes.
    properties: dict[str, dict[str, str]]
    #: The custom attributes, each over its type's dim.
    attributes: dict[str, AttributeSpec]

    @classmethod
    def of(cls, n: pypsa.Network) -> _Custom:
        """Place `n`'s custom columns.

        Raises
        ------
        ValueError
            If types of one dim carry a custom property with different dtypes.
        """
        columns: dict[str, dict[str, str]] = {}
        for c in n.components:
            if _exported(c) and (names := _custom_names(c)):
                static = _own_static(c)
                columns[c.name] = {name: _column_dtype(static[name]) for name in names}
        declared = _registry().schema.attributes
        dims: dict[str, set[str]] = {}
        for ctype, typed in columns.items():
            for name in typed:
                dims.setdefault(name, set()).add(_dim_of(ctype))
        placed = {
            (_dim_of(ctype), name)
            for ctype, typed in columns.items()
            for name, dtype in typed.items()
            if dtype == "str"
            or len(dims[name]) > 1
            or (name in declared and declared[name].dims != {_dim_of(ctype)})
        }
        carried: dict[tuple[str, str], dict[str, str]] = {}
        attributes: dict[str, AttributeSpec] = {}
        for ctype, typed in columns.items():
            dim = _dim_of(ctype)
            for name, dtype in typed.items():
                if (dim, name) in placed:
                    carried.setdefault((dim, name), {})[ctype] = dtype
                else:
                    attributes.setdefault(
                        name, AttributeSpec(dtype=dtype, dims=frozenset({dim}))
                    )
        properties: dict[str, dict[str, str]] = {}
        for (dim, name), by_type in carried.items():
            if len(set(by_type.values())) > 1:
                msg = (
                    f"The custom column `{name}` is a property of `{dim}` with "
                    f"one dtype, but its types carry it as {by_type}"
                )
                raise ValueError(msg)
            properties.setdefault(dim, {})[name] = next(iter(by_type.values()))
        return cls(columns, properties, attributes)


def _column_dtype(values: pd.Series) -> str:
    """Return the mathspec dtype to declare a custom static column with: `str` unless it is bool, int or float."""
    if pd.api.types.is_bool_dtype(values):
        return "bool"
    if pd.api.types.is_integer_dtype(values):
        return "int"
    if pd.api.types.is_float_dtype(values):
        return "float"
    return "str"


# TODO(pypsa): every function below, up to `_new_network`, ports a method that
# only exists on PyPSA's unreleased data-records branch. Delete the ported copy
# and call the real method once a PyPSA release carries it.


def _apply_snapshots_import(n: pypsa.Network, df: pd.DataFrame) -> None:
    """Set snapshots and snapshot weightings from an imported axis table.

    Ported from `pypsa.Network._apply_snapshots_import` (unreleased).
    """
    snapshot_levels = {"period", "timestep", "snapshot"}.intersection(df.columns)
    if snapshot_levels:
        df = df.set_index(sorted(snapshot_levels))
    n.set_snapshots(df.index)

    cols = ["objective", "stores", "generators"]
    if not df.columns.intersection(cols).empty:
        existing_cols = [c for c in cols if c in df.columns]
        n.snapshot_weightings = df.reindex(index=n.snapshots, columns=existing_cols)
    elif "weightings" in df.columns:
        n.snapshot_weightings = df["weightings"].reindex(n.snapshots)


def _broadcast_standard_types(n: pypsa.Network) -> None:
    """Broadcast standard-type static tables across scenarios after import.

    Ported from `pypsa.Network._broadcast_standard_types` (unreleased).
    """
    for component in n.standard_type_components:
        comp = n.components[component]
        if n.has_scenarios and not isinstance(comp.static.index, pd.MultiIndex):
            comp.static = pd.concat(
                dict.fromkeys(n.scenarios, comp.static), names=["scenario"]
            )


def _collect_network_attributes(n: pypsa.Network) -> dict[str, Any]:
    """The serializable scalar network attributes (incl. `name`, `pypsa_version`).

    Ported from `pypsa.Network._collect_network_attributes` (unreleased),
    less `srid`, which the CRS in the schema's `meta` carries.
    """
    numpy_types = tuple(np.sctypeDict.values())
    allowed_types = (float, int, bool, str)
    skip_attrs = {
        "component_attrs",
        "df",
        "pnl",
        "static",
        "dynamic",
        "iterate_components",
        "_name",
        "_pypsa_version",
        "srid",
    }

    attrs: dict[str, Any] = {}
    for attr in dir(n):
        if attr.startswith("__") or attr in skip_attrs:
            continue
        # Skip read-only properties (except pypsa_version) without invoking
        # their getters, which may emit warnings (e.g. model, objective).
        prop = getattr(type(n), attr, None)
        if isinstance(prop, property) and prop.fset is None and attr != "pypsa_version":
            continue
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r".*component_attrs is deprecated as of 1\.0.*",
                category=DeprecationWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message=r".*the API for how to access components data has changed.*",
                category=DeprecationWarning,
            )
            value = getattr(n, attr)
        if isinstance(value, numpy_types):
            attrs[attr] = value.item()
        elif isinstance(value, allowed_types):
            attrs[attr] = value
    return attrs


def _apply_network_attributes(n: pypsa.Network, attrs: dict[str, Any]) -> None:
    """Apply scalar network attributes read back from a record.

    Ported from `pypsa.Network._apply_network_attributes` (unreleased), less
    its version-compat warnings and PyPI update check.
    """
    attrs = dict(attrs)
    name = attrs.pop("name", None)
    if name is not None:
        n.name = name
    attrs.pop("pypsa_version", None)
    for attr, value in attrs.items():
        if attr in ("model", "objective", "objective_constant"):
            setattr(n, f"_{attr}", value)
        else:
            setattr(n, attr, value)


def _new_network(
    record: RecordLike, shape: NetworkShape, con: DuckDBPyConnection
) -> pypsa.Network:
    """An empty network with the record's axes already established."""
    import pypsa

    schema = record.schema
    n = pypsa.Network()
    _apply_network_attributes(n, schema.meta.get("attributes") or {})
    n.meta = schema.meta.get("meta") or {}
    crs = schema.meta.get("crs")
    if crs is not None:
        n.to_crs(crs)

    _apply_snapshots_import(n, shape.snapshot_levels)
    if shape.multiperiod:
        n.periods = shape.periods[PERIOD]
        n.investment_period_weightings = _weightings(
            record, PERIOD, n.investment_period_weightings, con
        )
    labels = pd.Index(shape.snapshots[SNAPSHOT], name=SNAPSHOT)
    n.snapshot_weightings = _weightings(
        record, SNAPSHOT, n.snapshot_weightings.set_axis(labels), con
    ).set_axis(n.snapshots)
    if shape.stochastic:
        index = pd.Index(shape.scenarios[SCENARIO].astype(str), name=SCENARIO)
        equal = pd.DataFrame({"weight": 1 / len(index)}, index=index)
        # Not `_scenarios_data`: `set_scenarios` gives the empty frames their `scenario` level.
        n.set_scenarios(_weightings(record, SCENARIO, equal, con)["weight"])
    return n


def _weightings(
    record: RecordLike, dim: str, table: pd.DataFrame, con: DuckDBPyConnection
) -> pd.DataFrame:
    """`table`, PyPSA's weightings of `dim` by label, with each one `record` stores in place.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    restored = table.copy()
    labels = {dim: Index.of(table.index.rename(dim), con).rel}
    overstated = Flags(frozenset({dim}), frozenset({dim}))
    for attribute, weighting in _WEIGHTINGS.items():
        frame = record.attributes.get(attribute)
        if weighting.dim != dim or frame is None:
            continue
        rows = settle(frame, labels, overstated, filter=False)
        rows = rows.project(col(dim), col("value")).df()
        values = rows.set_index(dim)["value"].reindex(table.index)
        restored[weighting.column] = values.fillna(table[weighting.column])
    return restored


def _add_component_type(
    n: pypsa.Network,
    ctype: str,
    static: DuckDBPyRelation,
    attributes: dict[str, tuple[DuckDBPyRelation, Flags]],
    shape: NetworkShape,
    con: DuckDBPyConnection,
) -> None:
    """Assign one type's static frame, then its series."""
    scenarios = Index.of(n.scenarios, con)
    static_df = _assign_static(static, attributes, shape, scenarios, con)
    for column in _geometries(ctype).intersection(static_df.columns):
        static_df[column] = _from_wkt(static_df[column], n.crs)
    n._import_components_from_df(static_df, ctype)

    snapshots = Index.of(n.snapshots, con, shape.snapshots[[SNAPSHOT]])
    static_index = n.c[ctype].static.index
    for attr, (long, flags) in attributes.items():
        if SNAPSHOT not in flags.varies:
            continue
        ts_rows = long.filter("snapshot IS NOT NULL AND value IS NOT NULL")
        wide = _series_frame(ts_rows, shape, snapshots, static_index, con)
        n._import_series_from_df(wide, ctype, attr)


# -- results -----------------------------------------------------------------


def _attach_sub_networks(n: pypsa.Network) -> None:
    """Give each SubNetwork row its `obj`, the `_DERIVED` column PyPSA's topology code reads on every row."""
    import pypsa

    static = n.c.SubNetwork.static
    static["obj"] = [pypsa.SubNetwork(n, name) for name in static.index]


def _output_attributes(c: pypsa.Components) -> frozenset[str]:
    """`c`'s result attributes by `defaults["status"]`, read off `c` for a network's extra ports (`p2`)."""
    defaults = c.defaults
    is_output = defaults["status"].astype(str).str.startswith("Output")
    return frozenset(defaults.index[is_output])


_DERIVED: dict[str, frozenset[str]] = {
    "Bus": frozenset({"p", "q", "control", "generator", "sub_network"}),
    "Line": frozenset({"v_nom"}),
    "ShuntImpedance": frozenset({"v_nom"}),
    "SubNetwork": frozenset({"obj"}),
}
"""Columns not written, by type: PyPSA derives each from what is written."""

_INTERNAL_RESULTS: dict[str, str] = {"p": "p_internal"}
"""A result at no port, renamed off the per-port result whose name it shares (`Link.p`)."""

_INTERNAL_COLUMNS: dict[str, str] = {
    name: attr for attr, name in _INTERNAL_RESULTS.items()
}
"""`_INTERNAL_RESULTS` back to PyPSA's names."""


def _result_attributes(c: pypsa.Components, inputs: Container[str]) -> dict[str, str]:
    """`c`'s PyPSA outputs -> the result each lands in, the rule schema and frames share.

    A per-port output lands in its stem, any other keeps PyPSA's name but for
    `_INTERNAL_RESULTS`. Dropped: `name`, `_DERIVED`, and any named in
    `inputs`, since PyPSA registers some names as an input on another type.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
    """
    per_port = _port_stems(c)
    derived = _DERIVED.get(c.name, frozenset())
    results = {}
    for attr in _output_attributes(c):
        if attr == "name" or attr in derived:
            continue
        name = per_port.get(attr) or _INTERNAL_RESULTS.get(attr, attr)
        if name not in inputs:
            results[attr] = name
    return results


def _collection_members(
    model: pypsa.NetworkCollection, dim: str, base: RecordLike | None
) -> dict[Any, pypsa.Network]:
    """`model`'s members by their label of `dim`, each checked against what `dim` asks of it.

    Raises
    ------
    ValueError
        If `model` is indexed by more than one level, `dim` names a dim of the
        PyPSA schema other than `period` and `scenario`, a member of a `period`
        or `scenario` collection is stochastic, one of a `period` collection
        holds another investment period than its label, a `scenario`
        collection is written over a `base` holding scenarios, a `period` one
        over a `base` with a dim of its own (`_own_dims`), or one over a dim
        of its own over a `base` with another.

    Notes
    -----
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    """
    if model.index.nlevels != 1:
        msg = (
            "A `NetworkCollection` is written with one index level, whose labels "
            f"are those of `{dim}`, not with {list(model.index.names)}"
        )
        raise ValueError(msg)
    if dim in _registry().schema.dimensions.keys() - {PERIOD, SCENARIO}:
        msg = (
            f"`collection_dim={dim!r}` names a dim the PyPSA schema declares: a "
            "collection's index is `period`, `scenario` or a dim of its own"
        )
        raise ValueError(msg)
    members = dict(zip(model.index, model.networks, strict=True))
    own = [] if base is None else _own_dims(base.schema)
    if dim == PERIOD and own:
        msg = (
            f"A collection over `period` is written over a `base` with a dim of "
            f"its own, {own}: `base` builds to a `NetworkCollection`"
        )
        raise ValueError(msg)
    if dim not in (PERIOD, SCENARIO):
        if set(own) - {dim}:
            msg = (
                f"A collection over `{dim}` is written over a `base` with a dim "
                f"of its own, {own}: a record has one, since collections do not nest"
            )
            raise ValueError(msg)
        return members
    for label, m in members.items():
        if m.has_scenarios:
            msg = f"The network of {dim} {label!r} is stochastic"
            raise ValueError(msg)
        if dim == PERIOD and list(m.investment_periods) != [label]:
            msg = (
                f"The network of period {label!r} has investment periods "
                f"{list(m.investment_periods)}, not [{label!r}]"
            )
            raise ValueError(msg)
    if dim == SCENARIO and base is not None and SCENARIO in base.dims:
        msg = (
            "A collection over `scenario` is written over a `base` without "
            "scenarios: its members are the scenarios"
        )
        raise ValueError(msg)
    return members


def _fitting(members: Mapping[Any, pypsa.Network], merged: pypsa.Network) -> None:
    """Refuse a member of a `period` collection holding a snapshot or component `merged` lacks."""
    for period, m in members.items():
        extra = m.snapshots.difference(merged.snapshots)
        if len(extra) > 0:
            msg = f"The network of period {period!r} has snapshots `base` lacks: {list(extra)}"
            raise ValueError(msg)
        for c in m.components:
            if not _exported(c) or c.name == "SubNetwork":
                continue
            unknown = c.static.index.difference(merged.c[c.name].static.index)
            if len(unknown) > 0:
                msg = f"The network of period {period!r} has {c.name} `base` lacks: {list(unknown)}"
                raise ValueError(msg)


def _alike(members: Mapping[Any, pypsa.Network], by: str) -> None:
    """Refuse a member holding other components than the first: it would read their defaults."""
    (first, reference), *rest = members.items()
    for label, m in rest:
        for c in reference.components:
            if c.name == "SubNetwork":
                continue
            apart = _own_static(c).index.symmetric_difference(
                _own_static(m.c[c.name]).index
            )
            if len(apart) > 0:
                msg = (
                    f"The network of {by} {label!r} holds other {c.name} than "
                    f"that of {by} {first!r}: {list(apart)}"
                )
                raise ValueError(msg)


def _clashing(long: pd.Series, keys: list[int]) -> pd.Series:
    """Per row of `long`, whether the members' values at its label clash.

    Floats agree within `_RTOL` and `_ATOL` of their largest magnitude, an
    infinity only with one of its sign, any other dtype where equal; missing
    clashes with held. A missing key is a label like any other.
    """
    grouped = long.groupby(level=keys, sort=False, dropna=False)
    held = long.notna().groupby(level=keys, sort=False, dropna=False)
    mixed = held.transform("any") & ~held.transform("all")
    unequal = grouped.transform("nunique") > 1
    if not pd.api.types.is_float_dtype(long):
        return mixed | unequal
    spread = grouped.transform("max") - grouped.transform("min")
    scale = long.abs().groupby(level=keys, sort=False, dropna=False).transform("max")
    infinite = np.isinf(long).groupby(level=keys, sort=False, dropna=False)
    return (
        mixed | (spread > _ATOL + _RTOL * scale) | (infinite.transform("any") & unequal)
    )


def _agreed(
    what: str, attr: str, values: Mapping[Any, pd.Series], by: str
) -> pd.Series:
    """One value per label, from each member's `values` that holds it, in the order first held.

    Parameters
    ----------
    values
        Per member label of `by`, its values by label, on one or more index levels.

    Raises
    ------
    ValueError
        If two members hold values for one label that do not agree (`_clashing`).
    """
    long = pd.concat(values, names=[by])
    keys = list(range(1, long.index.nlevels))
    apart = _clashing(long, keys)
    if apart.any():
        label, held = _first_apart(long, apart)
        name = label[0] if len(label) == 1 else label
        msg = (
            f"{what} {name!r} has `{attr}` {held} by {by}: a value that does not "
            f"vary over `{by}` is one across the members holding it"
        )
        raise ValueError(msg)
    return long.groupby(level=keys, sort=False).first()


def _first_apart(long: pd.Series, apart: pd.Series) -> tuple[tuple, dict[Any, Any]]:
    """The label of `long`'s first row `apart` marks, and each member's value there."""
    keys = list(range(1, long.index.nlevels))
    group = long.groupby(level=keys, sort=False, dropna=False).ngroup().to_numpy()
    values = long[group == group[apart.to_numpy()][0]]
    return values.index[0][1:], {key[0]: value for key, value in values.items()}


def _ordered_union(what: str, indexes: Mapping[Any, pd.Index], by: str) -> pd.Index:
    """Every member's labels in one order: the first member's, each later one's new labels after.

    Raises
    ------
    ValueError
        If a member orders the labels it shares with the members before it
        otherwise than they do.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    """
    union = next(iter(indexes.values()))[:0]
    for label, index in indexes.items():
        shared = index[index.isin(union)]
        before = union[union.isin(index)]
        if not shared.equals(before):
            msg = (
                f"The network of {by} {label!r} orders `{what}` as "
                f"{list(shared)}, the members before it as {list(before)}"
            )
            raise ValueError(msg)
        union = union.append(index[~index.isin(union)])
    return union


def _merge(
    merged: pypsa.Network,
    members: Mapping[Any, pypsa.Network],
    names: Callable[[pypsa.Components], Iterable[str]],
) -> None:
    """Replace each of `names` on `merged` with the `members`' values, then number its SubNetworks.

    A series is each member's at its snapshots, a static value the agreed one
    of the members holding it, anything no member holds PyPSA's default.
    `sub_network` is `merged`'s own topology.

    Notes
    -----
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    """
    for c in merged.components:
        if not _exported(c) or c.name == "SubNetwork":
            continue
        parts = {p: m.c[c.name] for p, m in members.items()}
        for attr in set(names(c)) - {SUB_NETWORK}:
            default = c.defaults["default"].get(attr)
            frames = {
                p: part.dynamic[attr]
                for p, part in parts.items()
                if attr in part.dynamic
            }
            named = pd.Index([]).append([f.columns for f in frames.values()])
            series = c.static.index[c.static.index.isin(named)]
            if attr in c.static.columns:
                held = {
                    p: part.static[attr].drop(series, errors="ignore")
                    for p, part in parts.items()
                    if attr in part.static.columns
                }
                values = _agreed(c.name, attr, held, PERIOD)
                c.static[attr] = values.reindex(c.static.index, fill_value=default)
            if len(series) > 0:
                wide = [
                    _dense(part, attr, frames[p], series)
                    for p, part in parts.items()
                    if p in frames
                ]
                c.dynamic[attr] = pd.concat(wide).reindex(
                    merged.snapshots, fill_value=default
                )
    if any(not m.sub_networks.empty for m in members.values()):
        merged.determine_network_topology(skip_isolated_buses=True)


def _dense(
    part: pypsa.Components, attr: str, frame: pd.DataFrame, columns: pd.Index
) -> pd.DataFrame:
    """`frame`, one member's series of `attr`, over `columns`: an entity it holds no series of at its static value, one it lacks at NaN."""
    held = columns[columns.isin(part.static.index)]
    wide = frame.reindex(columns=held)
    if attr in part.static.columns:
        wide = wide.fillna(part.static[attr].reindex(held))
    return wide.reindex(columns=columns)


def _over_periods(members: Mapping[Any, pypsa.Network]) -> pypsa.Network:
    """The multi-period network holding every member's period, its axes and components the union of theirs (`_merge`).

    Its network attributes, `meta` and CRS are the first member's.

    Notes
    -----
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    """
    import pypsa

    first, *rest = members.values()
    merged = pypsa.Network()
    _apply_network_attributes(merged, _collect_network_attributes(first))
    merged.meta = dict(first.meta)
    merged.to_crs(first.crs)
    merged.set_snapshots(
        first.snapshots.append([m.snapshots for m in rest]).set_names(
            [PERIOD, TIMESTEP]
        )
    )
    merged.snapshot_weightings = pd.concat(
        [m.snapshot_weightings for m in members.values()]
    ).set_axis(merged.snapshots)
    merged.investment_period_weightings = pd.concat(
        [m.investment_period_weightings for m in members.values()]
    )
    held: dict[str, dict[Any, pd.DataFrame]] = {}
    for p, m in members.items():
        for c in m.components:
            if _exported(c) and c.name != "SubNetwork":
                held.setdefault(c.name, {})[p] = c.static
    for ctype, statics in held.items():
        names = _ordered_union(
            ctype, {p: static.index for p, static in statics.items()}, PERIOD
        )
        static = pd.concat(statics.values())
        merged.c[ctype].static = static[~static.index.duplicated()].reindex(names)
    _merge(
        merged,
        members,
        lambda c: {
            attr
            for m in members.values()
            for attr in [*m.c[c.name].static.columns, *m.c[c.name].dynamic]
        },
    )
    return merged


# -- network -> layer ---------------------------------------------------------

#: The `role` of a port: `0` is the input end, every other an output, a one-port type's attached.
_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT = "input", "output", "attached"
_ROLES = (_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT)

_PORT_SUFFIX = re.compile(r"(.+?)(\d+)")


@cache
def _per_port_stems() -> frozenset[str]:
    """Every attribute stem some PyPSA type carries once per port, inputs and results alike.

    A name ending in one of its type's port indices (`bus0`, `p1`), and the
    stems a multi-port type leaves unsuffixed at port `1` (`efficiency`).

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    """
    import pypsa

    n = pypsa.Network()
    stems: set[str] = set()
    for ctype in n.all_components:
        c = n.components[ctype]
        ports = set(c.ports) - {""}
        stems |= set(getattr(c, "_unsuffixed_attrs", ()))
        stems |= {
            match.group(1)
            for attr in c.defaults.index
            if (match := _PORT_SUFFIX.fullmatch(attr)) and match.group(2) in ports
        }
    return frozenset(stems)


def _port_role(port: str) -> str:
    """The `role` for a port index."""
    if port == "":
        return _SINGLE_PORT
    return _INPUT_PORT if port == "0" else _OUTPUT_PORT


def _port_attribute(stem: str, port: str, registered: Container[str]) -> str:
    """PyPSA's name for `stem` at `port`: `bus0`, `p1`, `efficiency`, `efficiency2`, ...

    A stem PyPSA registers at port `0` is suffixed from `0`; one it does not
    leaves port `1` unsuffixed. A one-port type's port `""` is the bare stem.

    Parameters
    ----------
    registered
        The type's attribute names, which say whether `stem` starts at port `0`.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    """
    if port == "1" and f"{stem}0" not in registered:
        return stem
    return f"{stem}{port}"


def _carries(stem: str, port: str, registered: Container[str]) -> bool:
    """Whether PyPSA has a `stem` column at `port`: a registered one, or any at an additional port."""
    return (
        _port_attribute(stem, port, registered) in registered
        or _port_attribute(BUS, port, registered) not in registered
    )


@cache
def _registered(ctype: str) -> frozenset[str]:
    """`ctype`'s attribute names in PyPSA's bare registry, for `_port_attribute`."""
    from pypsa.components.types import get as get_component_type

    return frozenset(get_component_type(ctype).defaults.index)


def _connection_rows(c: pypsa.Components) -> pd.DataFrame:
    """One type's connections as `(entity, bus, role)` rows, one per port of `c.ports` with a bus.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    """
    static = c.static
    frames = []
    for port in c.ports:
        column = _port_attribute("bus", port, c.defaults.index)
        if column not in static.columns:
            continue
        attached = static[static[column].astype(str) != ""]
        if attached.empty:
            continue
        rows = attached[[column]].rename(columns={column: "bus"})
        rows = rows.reset_index().rename(columns={"name": "entity"})
        rows["role"] = _port_role(port)
        frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["entity", "bus", "role"])
    return pd.concat(frames, ignore_index=True)


# TODO(pypsa): `_is_output`, `_drop_default_rows` and `_scalar_rows` below port
# helpers that only exist on PyPSA's unreleased data-records branch.
# Delete the ported copy and call the real one once a PyPSA release carries it.


def _is_output(defaults: pd.DataFrame, attr: str) -> bool:
    """Whether `attr` is an output (custom attrs with no status are inputs).

    Ported from `pypsa.common._is_output`.
    """
    if attr not in defaults.index:
        return False
    return str(defaults.at[attr, "status"]).startswith("Output")


@cache
def _geometries(ctype: str) -> frozenset[str]:
    """`ctype`'s geometry columns in PyPSA's bare registry.

    Notes
    -----
    - [geometry](https://energy-models.github.io/datarecord/design/tools/pypsa/#geometry)
    """
    from pypsa.components.types import get as get_component_type

    defaults = get_component_type(ctype).defaults
    return frozenset(defaults.index[defaults["typ"].astype(str) == GEOMETRY])


def _from_wkt(values: pd.Series, crs: Any) -> pd.Series:
    """A WKT column as the GeoSeries PyPSA keeps, in `crs`."""
    import geopandas as gpd

    return gpd.GeoSeries.from_wkt(values, crs=crs)


def _drop_default_rows(rows: pd.DataFrame, default: Any) -> pd.DataFrame:
    """Remove scalar rows that just repeat the attribute's default value.

    Ported from `pypsa.components.array._drop_default_rows`.
    """
    if pd.isnull(default):
        return rows[rows["value"].notna()]
    return rows[rows["value"] != default]


def _scalar_rows(c: pypsa.Components, attr: str, exclude: pd.Index) -> pd.DataFrame:
    """Build long rows from the static scalars of `attr`, less their NULL `snapshot`.

    Ported from `pypsa.components.array.Components._scalar_rows`, built in one
    constructor: per-call pandas overhead dominated an export.
    """
    static = _own_static(c)[attr]
    if attr in _geometries(c.name):
        static = static.to_wkt()
    if len(exclude):
        static = static[~static.index.isin(exclude)]
    index = static.index
    stochastic = isinstance(index, pd.MultiIndex)
    return pd.DataFrame(
        {
            "entity": _names(index),
            "scenario": index.get_level_values(SCENARIO) if stochastic else np.nan,
            "value": static.array,
        }
    )


def _shared_across_scenarios(rows: pd.DataFrame) -> pd.DataFrame:
    """`rows` with an entity's per-scenario repeats of one value folded to a NULL scenario.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    if rows["scenario"].isna().all():
        return rows
    shared = rows.groupby("entity")["value"].transform(
        lambda values: values.nunique(dropna=False) == 1
    )
    folded = rows[shared.astype(bool)].drop_duplicates("entity").assign(scenario=None)
    return pd.concat([folded, rows[~shared.astype(bool)]], ignore_index=True)


# -- the tool ---------------------------------------------------------------


class PyPSATool(Tool):
    """Build a `pypsa.Network` from a record and read its results back."""

    name = "pypsa"
    schema = Schema()

    def component_types(self) -> frozenset[str]:
        """The component types PyPSA's type registry knows."""
        from pypsa.components.types import all_components

        return frozenset(c.name for c in all_components.values())

    def requires(self, record: RecordLike) -> Requirements:
        """PyPSA's axes, the record's component types PyPSA knows, and their required attributes by source."""
        known = self.component_types()
        ctypes = {ct for ct in _components(record) if ct in known}
        return Requirements(
            dims=REQUIRED_DIMS,
            categories=frozenset(ctypes),
            attributes=frozenset(
                (ct, src)
                for ct in ctypes
                for attr in _required_attributes(ct)
                for src in self.schema.sources(ct, attr)
            ),
        )

    def verify(
        self, record: RecordLike, *, select: Mapping[str, Any] | None = None
    ) -> Requirements:
        """What this record, at `select` as `build` takes it, fails to supply for a PyPSA build; falsy if it is usable.

        Reports PyPSA's axes the schema lacks, keys and dims of its own this
        tool cannot honour (and nothing else where it finds any), component
        types PyPSA does not know, required attributes (by source) with no
        row, property or declared default, a port's bus where a member has no
        connection there, curves, and in `ignored` stored attributes a type
        does not take, which `build` leaves out.

        Raises
        ------
        ValueError
            If `select` is one `build` refuses.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        return self._verify(_select(record, select or {}))[0]

    def _verify(
        self, record: RecordLike
    ) -> tuple[Requirements, dict[str, _TypeMembers]]:
        """`verify`, with the members it read for each known type, for `build` to reuse; empty where a key check failed."""
        dims = REQUIRED_DIMS - set(record.schema.dims)
        unsupported_keys = self._unsupported_keys(record)
        unsupported_dims = self._unsupported_dims(record)
        if unsupported_keys or unsupported_dims:
            return Requirements(
                dims=frozenset(dims),
                unsupported_keys=frozenset(unsupported_keys),
                unsupported_dims=frozenset(unsupported_dims),
            ), {}

        categories: set[str] = set()
        attributes: set[tuple[str, str]] = set()
        unsupported_values: set[tuple[str, str]] = set()
        ignored: set[tuple[str, str]] = set()

        known = self.component_types()
        declared = record.schema
        read: dict[str, _TypeMembers] = {}
        ctypes = _components(record)
        connection_frame = _connection_frame(record) if known & set(ctypes) else None
        for ctype in ctypes:
            if ctype not in known:
                categories.add(ctype)
                continue
            resolved = record.flags(**{CTYPE: ctype})
            taken = _taken(declared, ctype, resolved)
            ignored |= {(ctype, attr) for attr in resolved if attr not in taken}
            unsupported_values |= {
                (ctype, attr) for attr in taken if resolved[attr].breakpoints
            }
            read[ctype] = _TypeMembers.of(record, ctype, connection_frame)
            static_cols = read[ctype].columns
            from_connections = read[ctype].port_attributes(ctype)
            for attr in _required_attributes(ctype):
                for src in self.schema.sources(ctype, attr):
                    if src in resolved or src in static_cols or src in from_connections:
                        continue
                    if (
                        src in declared.attributes
                        and declared.default_for(src, **{CTYPE: ctype}) is not None
                    ):
                        continue
                    attributes.add((ctype, src))
        return Requirements(
            dims=frozenset(dims),
            categories=frozenset(categories),
            attributes=frozenset(attributes),
            unsupported_values=frozenset(unsupported_values),
            ignored=frozenset(ignored),
        ), read

    def _unsupported_keys(self, record: RecordLike) -> set[tuple[str, str]]:
        """`(key, dim)` pairs this tool cannot honour: `snapshot`, as any key.

        A component's whole series must come from one layer for PyPSA's static/series split.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
        """
        defs = record.schema
        return {("owner_map_key", SNAPSHOT)} if SNAPSHOT in defs.partial_dims else set()

    def _unsupported_dims(self, record: RecordLike) -> set[str]:
        """The record's dims of its own (`_own_dims`), where it declares more than one: a collection has one index.

        Notes
        -----
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        own = _own_dims(record.schema)
        return set(own) if len(own) > 1 else set()

    def build(
        self, record: RecordLike, *, select: Mapping[str, Any] | None = None
    ) -> pypsa.Network | pypsa.NetworkCollection:
        """The resolved network, or the collection of them along the record's dim of its own.

        A record declaring a dim PyPSA's schema does not, such as
        `weather_year`, builds a `NetworkCollection` indexed by it in axis
        order, each member the network of the rows at its label and those at
        every label. Any other builds a `Network`.

        Parameters
        ----------
        select
            Labels to build from, by dim, as xarray's `sel` takes them. One
            label of `period`, `scenario` or the record's dim of its own
            squeezes the dim, a single-period or deterministic network, or a
            `Network` rather than a collection; a list keeps it at those
            labels, as does a partial date string naming several.
            `snapshot`, `bus`, `entity` and `carrier` take a label or a list
            and are never squeezed, a snapshot named by its timestamp in
            every period. A slice keeps `snapshot`, `period` or
            a dim of its own of numbers or datetimes at the labels it covers,
            as pandas' `.loc` slices. A label referring to one the selection
            drops goes too: a snapshot of another period, a period of no
            snapshot, a bus or entity of another carrier, an entity attached
            to a dropped bus and, over one period, an entity inactive in it.

        Raises
        ------
        ValueError
            If `select` names another dim, no label or one the axis lacks,
            slices with a step, along an axis of text or over no label,
            leaves no snapshot, or keeps scenarios weighing nothing in all.
        UnsupportedRecordError
            If `verify` reports anything missing, of the record or of a member.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        - [selecting before a build](https://energy-models.github.io/datarecord/design/tools/pypsa/#selecting-before-a-build)
        """
        import pypsa

        view = _select(record, select or {})
        own = _own_dims(view.schema)
        if len(own) != 1:
            return self._network(view)
        (dim,) = own
        labels = _axis_labels(view, dim)
        members = [
            self._network(replace(view, squeeze={**view.squeeze, dim: label}))
            for label in labels
        ]
        return pypsa.NetworkCollection(
            pd.Series(members, index=pd.Index(labels, name=dim))
        )

    def _network(self, record: RecordLike) -> pypsa.Network:
        """`build`'s network of a record with no dim of its own."""
        missing, read = self._verify(record)
        if missing:
            raise UnsupportedRecordError(self.name, missing)

        con = _connection(record)
        schema = record.schema
        shape = NetworkShape(record.dims, record.properties)
        n = _new_network(record, shape, con)
        flagged = record.flags()

        over_entity = [a for a in schema.attributes if schema.addresses(a, ENTITY)]
        scenarios = {SCENARIO: record.dims[SCENARIO]} if shape.stochastic else {}
        tuples = record.dims.get(CONNECTION)
        for ctype in _AXIS_TYPES:
            self._add_labels(n, record, ctype, flagged, shape, scenarios, con)
        _attach_sub_networks(n)
        for ctype, members in read.items():
            static = members.members
            if static is None:
                continue
            labels = {**scenarios, ENTITY: static}
            if tuples is not None:
                labels[CONNECTION] = _tuples_of(tuples, static)
            if not shape.stochastic and SCENARIO in static.columns:
                static = static.project(star(exclude=[SCENARIO]))
            static = static.project("*, row_number() OVER () AS _pos")
            connections = members.connections
            static = _collapse_connections(static, connections, ctype, con)
            attributes = {}
            carried = _taken(schema, ctype, over_entity)
            for attr, flags in flagged.items():
                if attr not in carried:
                    continue
                long = _unpack_connection(
                    self.schema.resolve(
                        record, ctype, attr, labels, flagged, filter=False
                    )
                )
                if CONNECTION in schema.attributes[attr].dims:
                    for column, rows in _port_columns(
                        long, attr, connections, ctype, con
                    ).items():
                        attributes[column] = (rows, flags)
                else:
                    attributes[_INTERNAL_COLUMNS.get(attr, attr)] = (long, flags)

            _add_component_type(n, ctype, static, attributes, shape, con)

        _broadcast_standard_types(n)
        return n

    def _add_labels(
        self,
        n: pypsa.Network,
        record: RecordLike,
        ctype: str,
        flagged: dict[str, Flags],
        shape: NetworkShape,
        scenarios: Mapping[str, DuckDBPyRelation],
        con: DuckDBPyConnection,
    ) -> None:
        """Add an `_AXIS_TYPES` type's members: its dim's labels with their properties, defaults applied, and the attributes over it."""
        dim = _AXIS_TYPES[ctype]
        frame = record.properties.get(dim)
        if frame is None:
            return
        schema = record.schema
        labels = with_defaults(schema, dim, frame, list(schema.properties[dim]), con)
        static = _keyed_as_entity(_empty_for_none(labels), dim).project(
            "*, row_number() OVER () AS _pos"
        )
        over = {**scenarios, dim: labels}
        attributes = {
            attr: (
                _keyed_as_entity(
                    self.schema.resolve(
                        record, ctype, attr, over, flagged, filter=False
                    ),
                    dim,
                ),
                flags,
            )
            for attr, flags in flagged.items()
            if attr in schema.attributes and dim in schema.attributes[attr].dims
        }
        _add_component_type(n, ctype, static, attributes, shape, con)

    def to_datarecord(
        self,
        model: pypsa.Network | pypsa.NetworkCollection,
        con: DuckDBPyConnection,
        *,
        results_only: bool = False,
        base: RecordLike | None = None,
        collection_dim: str | None = None,
    ) -> RecordLike:
        """Present a `Network`, or a `NetworkCollection` along `collection_dim`, as the `Record` `write_layer` persists.

        The inverse of `build`: static and series values become long rows on
        `con`, port columns connection rows carrying a `role`, buses and
        carriers the `bus` and `carrier` axes in PyPSA's order, `""` NULL.
        Outputs are attributes like any other. A static value is left out
        where it repeats what the record resolves to without it: over `base`,
        `base`'s value, else the default; over nothing, the default.

        With `results_only` the record is a thin layer over `base`: the
        outputs, at their default too, `base`'s output rows it does not write
        again restated as NULL, and this solve's SubNetworks as the
        `sub_network` axis.

        A `NetworkCollection`'s labels are those of `collection_dim`, and it
        is checked whole before this returns, so the record builds back. Over
        `period` it is the one multi-period network, over `scenario` one
        stochastic record weighing them equally, over any other a dim of the
        record's own; an attribute whose values differ across the members is
        over that dim, one that agrees written once.

        Raises
        ------
        ValueError
            If `base` is missing with `results_only`, or given without it
            for a `NetworkCollection`; `collection_dim` without a
            `NetworkCollection`, or missing with one; if the collection is not one the dim takes (more than one
            index level, `collection_dim` another dim of PyPSA's, stochastic
            members, a member of `period`
            holding another period or what `base` lacks, a `base` with a dim
            the collection cannot sit over); if members differ where they must
            agree (schemas, axis order, properties, components, a value PyPSA
            holds once across `scenario` or `period`); or if types of one dim
            carry a custom property with different dtypes.
        UnsupportedRecordError
            If two entity types share a name.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
        - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
        """
        import pypsa

        if results_only and base is None:
            msg = (
                "`to_datarecord` takes `base` with `results_only=True`: a results "
                "layer is written over the record the network was built from"
            )
            raise ValueError(msg)
        msg = (
            "`to_datarecord` takes `collection_dim` exactly when `model` is a "
            "`NetworkCollection`: it names the dim the collection's index labels"
        )
        if not isinstance(model, pypsa.NetworkCollection):
            if collection_dim is not None:
                raise ValueError(msg)
            return self._source(model, con, base, results_only)
        if collection_dim is None:
            raise ValueError(msg)
        if base is not None and not results_only:
            msg = "A `NetworkCollection` is exported whole over nothing: it takes `base` with `results_only=True` only"
            raise ValueError(msg)
        members = _collection_members(model, collection_dim, base)
        if collection_dim != PERIOD:
            _alike(members, collection_dim)
            sources = {
                label: self._source(m, con, base, results_only)
                for label, m in members.items()
            }
            return _StackedSource.of(sources, collection_dim, con)
        if base is None:
            return self._source(_over_periods(members), con, None, results_only)
        merged = self._network(base)
        _fitting(members, merged)
        inputs = _registry().inputs
        _merge(merged, members, lambda c: _result_attributes(c, inputs))
        return _ResultsSource(merged, con, base)

    def _source(
        self,
        n: pypsa.Network,
        con: DuckDBPyConnection,
        base: RecordLike | None,
        results_only: bool,
    ) -> _NetworkSource:
        """`n` as a full export, over `base` where given, or as results over `base`, once it is checked writable."""
        clashing = _colliding_names(n)
        if clashing:
            raise UnsupportedRecordError(self.name, Requirements(names=clashing))
        source = (
            _ResultsSource(n, con, base)
            if results_only and base is not None
            else _NetworkSource(n, con, base)
        )
        source.custom  # placing the custom columns is the check
        return source


def _port_names(c: pypsa.Components) -> dict[str, tuple[str, str]]:
    """Per-port attribute name -> `(stem, port)`, for this type's actual ports."""
    registered = c.defaults.index
    return {
        name: (stem, port)
        for stem in sorted(_per_port_stems())
        for port in c.ports
        if (name := _port_attribute(stem, port, registered)) in registered
    }


def _port_stems(c: pypsa.Components) -> dict[str, str]:
    """Per-port attribute name -> its stem, for this type's actual ports."""
    return {name: stem for name, (stem, _) in _port_names(c).items()}


def _attribute_spec(row: pd.Series, *, over: str) -> AttributeSpec:
    """One `c.defaults` row as the spec a record declares over `over`, a varying one over `snapshot` and `scenario` too."""
    dims = {SNAPSHOT, SCENARIO} if row["varying"] else set()
    dims.add(over)
    return AttributeSpec(
        dtype=_DTYPES.get(row["typ"], "str"),
        dims=frozenset(dims),
        default=_default(row["default"]),
        unit=_text(row.get("unit")),
        description=_text(row.get("description")),
    )


def _port_spec(
    defaults: pd.DataFrame, stem: str, per_port: Mapping[str, str]
) -> AttributeSpec:
    """A per-port stem's spec over `connection`, from its first port's row, with no default where its ports' differ (`rate0`, `rate1`)."""
    columns = [name for name, of in per_port.items() if of == stem]
    spec = _attribute_spec(defaults.loc[columns[0]], over=CONNECTION)
    if len({_default(defaults.at[name, "default"]) for name in columns}) > 1:
        return spec.model_copy(update={"default": None})
    return spec


def _with_default_by(
    attributes: dict[str, AttributeSpec], by_type: dict[str, dict[str, Any]]
) -> dict[str, AttributeSpec]:
    """`attributes`, each given `default_by` over `ctype` where a type's default differs.

    Parameters
    ----------
    by_type
        Attribute name -> component type -> that type's registry default.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)
    """
    widened = dict(attributes)
    for name, defaults_of in by_type.items():
        spec = attributes[name]
        differing = {
            ct: v for ct, v in defaults_of.items() if not _same(v, spec.default)
        }
        if differing:
            widened[name] = spec.model_copy(
                update={"default_by": {BY_CTYPE: differing}}
            )
    return widened


def _property(name: str, spec: AttributeSpec) -> PropertySpec:
    """The property spec of registry input `name`; a `carrier` names a label of `carrier`, `""` none."""
    fields = spec.model_dump(exclude={"dims", "breakpoints"})
    if name == CARRIER:
        fields |= {
            "dtype": None,
            "one_of": CARRIER,
            "default": fields["default"] or None,
        }
    return PropertySpec(**fields)


class _Registry(NamedTuple):
    """What PyPSA's whole component registry declares."""

    #: The record schema every network exports against.
    schema: RecordSchema
    #: Per entity type, the names it declares, which `_taken` keeps its values to.
    carries: dict[str, frozenset[str]]
    #: Every input, attribute or property of any subject, which no result may be named.
    inputs: frozenset[str]


@cache
def _registry() -> _Registry:
    """The record schema PyPSA's whole component registry declares, what each entity type takes, and its inputs.

    Every network exports against this one schema. One spec per name
    record-wide: the first type declaring it gives the spec, a default
    differing per type is `default_by`, and a per-port column collapses to
    its stem over `connection`. `Bus`, `Carrier` and `SubNetwork` members
    are labels of their own dim (`_AXIS_TYPES`).

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    - [results on connections](https://energy-models.github.io/datarecord/design/tools/pypsa/#results-on-connections)
    """
    import pypsa

    n = pypsa.Network()
    attributes: dict[str, AttributeSpec] = {
        name: AttributeSpec(
            dtype="float",
            dims=frozenset({weighting.dim}),
            description=weighting.description,
        )
        for name, weighting in _WEIGHTINGS.items()
    }
    carries: dict[str, frozenset[str]] = {}
    by_type: dict[str, dict[str, Any]] = {}
    exported = sorted(n.all_components)
    ctypes = [ctype for ctype in exported if ctype not in _AXIS_TYPES]
    for ctype in ctypes:
        c = n.components[ctype]
        defaults = c.defaults
        per_port = _port_stems(c)
        is_output = _output_attributes(c)
        carried: set[str] = set()
        for attr in defaults.index:
            if attr == "name" or attr in is_output:
                continue
            stem = per_port.get(attr, attr)
            if stem in carried or stem == BUS:
                continue
            carried.add(stem)
            if attr not in per_port:
                by_type.setdefault(stem, {})[c.name] = _default(
                    defaults.at[attr, "default"]
                )
            attributes.setdefault(
                stem,
                _port_spec(defaults, stem, per_port)
                if attr in per_port
                else _attribute_spec(defaults.loc[attr], over=ENTITY),
            )
        if carried:
            carries[c.name] = frozenset(carried)
    attributes = _with_default_by(attributes, by_type)
    properties = {
        ENTITY: {
            CTYPE: PropertySpec(
                one_of=CTYPE, description="What kind of entity an entity is."
            ),
            **{
                name: _property(name, attributes.pop(name))
                for name in _PROPERTIES[ENTITY]
            },
        }
    }
    for ctype, dim in _AXIS_TYPES.items():
        c = n.components[ctype]
        specs = {
            attr: _attribute_spec(c.defaults.loc[attr], over=dim)
            for attr in c.defaults.index
            if attr != "name" and attr not in _output_attributes(c)
        }
        properties[dim] = (
            {CARRIER: PropertySpec(one_of=CARRIER, description=_SUB_NETWORK_CARRIER)}
            if dim == SUB_NETWORK
            else {name: _property(name, specs.pop(name)) for name in _PROPERTIES[dim]}
        )
        attributes |= specs
    inputs = frozenset(
        {*attributes, *(name for named in properties.values() for name in named)}
    )
    results: dict[str, AttributeSpec] = {}
    for ctype in exported:
        c = n.components[ctype]
        per_port = _port_stems(c)
        named = _result_attributes(c, inputs)
        for attr, name in sorted(named.items()):
            over = CONNECTION if attr in per_port else _dim_of(ctype)
            results.setdefault(name, _attribute_spec(c.defaults.loc[attr], over=over))
        if ctype not in _AXIS_TYPES and named:
            carries[ctype] = carries.get(ctype, frozenset()) | set(named.values())
    properties[CONNECTION] = {
        ROLE: PropertySpec(
            one_of=ROLE,
            description="Which end of the component this attachment is.",
        )
    }
    schema = RecordSchema(
        dimensions={
            SNAPSHOT: Dimension(
                dtype="datetime",
                description="A point in the operational time series.",
            ),
            TIMESTEP: Dimension(
                dtype="datetime",
                description="A timestep of a multi-period network, within a period.",
            ),
            PERIOD: Dimension(
                dtype="int",
                unit="year",
                description="An investment period, labelled by its year.",
            ),
            SCENARIO: Dimension(
                dtype="str",
                description="One realisation of a stochastic problem.",
            ),
            ENTITY: Dimension(dtype="str", description="An entity."),
            CTYPE: Dimension(one_of=ctypes, description="A kind of entity."),
            BUS: Dimension(dtype="str", description="A node of the network."),
            CARRIER: Dimension(
                dtype="str", description="An energy carrier or technology."
            ),
            SUB_NETWORK: Dimension(
                dtype="str",
                description="A connected part of the network, as PyPSA numbers them.",
            ),
            ROLE: Dimension(
                one_of=_ROLES,
                description="An end of a component an attachment can be.",
            ),
            CONNECTION: Dimension(
                over={"entity": ENTITY, "bus": BUS},
                description="An entity's attachment to one bus.",
            ),
        },
        properties=properties,
        attributes=attributes | results,
        partial=frozenset({ENTITY, BUS, SCENARIO, CONNECTION}),
    )
    return _Registry(schema, carries, inputs)


def _taken(schema: RecordSchema, ctype: str, names: Iterable[str]) -> list[str]:
    """Those of `names` a `ctype` takes, in order: what no other type claims, or its own.

    A type claims its registry names and the custom columns its network was
    exported with (`meta`); a name no type claims reaches every type.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-value-its-type-does-not-take)
    """
    custom = schema.meta.get(_CUSTOM) or {}
    claimed = {
        t: carried | frozenset(custom.get(t, ()))
        for t, carried in _registry().carries.items()
    }
    everyone = frozenset().union(*claimed.values(), *map(frozenset, custom.values()))
    own = claimed.get(ctype, frozenset(custom.get(ctype, ())))
    return [n for n in names if n in own or n not in everyone]


@dataclass(frozen=True)
class _NetworkSource:
    """A `pypsa.Network` presented as a `Record`, nothing unpivoted until a key is looked up.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
    """

    n: pypsa.Network
    #: The connection the wide->long melt runs on.
    con: DuckDBPyConnection
    #: The record this export is written over, `None` for one over nothing.
    base: RecordLike | None

    #: Whether a result's static rows still at their default are left out.
    _DROP_RESULT_DEFAULTS: ClassVar[bool] = True

    @cached_property
    def custom(self) -> _Custom:
        """The network's custom static columns, placed (`_Custom.of`)."""
        return _Custom.of(self.n)

    @cached_property
    def schema(self) -> RecordSchema:
        """The registry's schema, widened by this network's custom columns, multi-period `snapshot` and `meta`.

        Notes
        -----
        - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
        - [custom columns](https://energy-models.github.io/datarecord/design/tools/pypsa/#custom-columns)
        """
        base = _registry().schema
        dimensions = dict(base.dimensions)
        properties = dict(base.properties)
        if PERIOD in self._snapshots:
            properties[SNAPSHOT] = _snapshot_properties(self._snapshots)
            if TIMESTEP in self._snapshots:
                dimensions[SNAPSHOT] = dimensions[SNAPSHOT].model_copy(
                    update={"dtype": "int"}
                )
        custom = self.custom
        for dim, typed in custom.properties.items():
            properties[dim] = properties[dim] | {
                name: PropertySpec(dtype=dtype)
                for name, dtype in typed.items()
                if name not in properties[dim]
            }
        return _rebuilt(
            base,
            dimensions=dimensions,
            properties=properties,
            attributes=custom.attributes | dict(base.attributes),
            meta={
                "format": "pypsa-parquet",
                "attributes": _collect_network_attributes(self.n),
                _CUSTOM: {
                    ctype: sorted(columns)
                    for ctype, columns in custom.columns.items()
                    if _dim_of(ctype) == ENTITY
                },
                "meta": dict(self.n.meta),
                "crs": self.n.crs.to_string(),
            },
        )

    @cached_property
    def _snapshots(self) -> pd.DataFrame:
        """`n.snapshots` as the `snapshot` axis with its properties (`_snapshot_frame`)."""
        return _snapshot_frame(self.n.snapshots)

    @cached_property
    def _snapshot_labels(self) -> pd.Index:
        """The labels of `_snapshots`, in PyPSA's order."""
        return pd.Index(self._snapshots[SNAPSHOT], name=SNAPSHOT)

    @cached_property
    def _sub_networks(self) -> pd.DataFrame:
        """`n.sub_networks` as the `sub_network` axis, each label with its carrier."""
        return self._labels("SubNetwork")

    @cached_property
    def _axes(self) -> dict[str, pd.DataFrame]:
        """Every labelled axis with its properties, by dim, in PyPSA's order; empty where the network has none.

        A carrier named but not in `n.carriers` is added after those listed,
        its properties NULL.
        """
        snapshots = self._snapshots
        timesteps = (
            snapshots[[TIMESTEP]].drop_duplicates(ignore_index=True)
            if TIMESTEP in snapshots
            else pd.DataFrame(columns=[TIMESTEP])
        )
        buses = self._labels("Bus")
        listed = self._labels("Carrier")
        named = [
            frame[CARRIER]
            for frame in (buses, self._sub_networks, self._entity_frame())
            if CARRIER in frame
        ]
        unlisted = (
            pd.concat(named).dropna().drop_duplicates()
            if named
            else pd.Series([], dtype=object)
        )
        unlisted = unlisted[~unlisted.isin(listed[CARRIER])]
        added = pd.DataFrame({CARRIER: unlisted.to_numpy()})
        return {
            SNAPSHOT: snapshots,
            TIMESTEP: timesteps,
            PERIOD: self.n.investment_periods.to_frame(index=False),
            SCENARIO: self.n.scenario_weightings.index.to_frame(index=False),
            BUS: buses,
            CARRIER: pd.concat([listed, added], ignore_index=True),
            SUB_NETWORK: self._sub_networks,
        }

    @property
    def dims(self) -> LazyRelations:
        """Every axis this network holds as keys, `entity` and the multi-dim `connection` among them."""
        present = tuple(d for d, frame in self._axes.items() if len(frame) > 0)

        def axis(dim: str) -> DuckDBPyRelation:
            if dim == CONNECTION:
                return self._connection_frame().project(col(CONNECTION))
            if dim == ENTITY:
                return self.con.from_df(self._entity_frame()[[ENTITY]])
            return self.con.from_df(self._axes[dim][[dim]])

        return LazyRelations((*present, ENTITY, CONNECTION), axis)

    def _labels(self, ctype: str) -> pd.DataFrame:
        """One `_AXIS_TYPES` type's members as labels of its dim, with their properties."""
        dim = _AXIS_TYPES[ctype]
        static = self.n.c[ctype].static
        columns = [p for p in self.schema.properties[dim] if p in static.columns]
        frame = static[columns].rename_axis(index={"name": dim}).reset_index()
        return _unscoped(_none_for_empty(frame), [dim])

    def _entity_frame(self) -> pd.DataFrame:
        """Every entity type's members and their properties, typed by `ctype`.

        Notes
        -----
        - [a label is unique within its dim](https://energy-models.github.io/datarecord/design/storage/format/#a-label-is-unique-within-its-dim)
        """
        frames = [
            self._static_frame(c.name).assign(**{CTYPE: c.name})
            for c in self.n.components
            if _as_entities(c)
        ]
        if not frames:
            return pd.DataFrame(columns=[ENTITY, CTYPE])
        merged = _none_for_empty(pd.concat(frames, ignore_index=True))
        lead = [ENTITY, CTYPE]
        return merged[[*lead, *(c for c in merged.columns if c not in lead)]]

    def _connection_frame(self) -> DuckDBPyRelation:
        """Every type's connections in one frame, `(connection, role)`.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        types = [c.name for c in self.n.components if _as_entities(c) and c.ports]
        frames = [_connection_rows(self.n.c[ctype]) for ctype in types]
        rows = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=["entity", "bus", "role"])
        )
        flat = self.con.from_df(_unscoped(rows, [ENTITY, BUS]))
        return flat.project(
            _connection_struct(col(ENTITY), col(BUS)), star(exclude=[ENTITY, BUS])
        )

    def _as_long_rel(
        self, c: pypsa.Components, attr: str, *, unchanged: str | None = None
    ) -> DuckDBPyRelation | None:
        """`attr` as long rows `(entity, snapshot, scenario, value)`, `None` where it has none.

        A series is melted, a static scalar a NULL-snapshot row, left out
        where it repeats what record attribute `unchanged` resolves to
        without it (`_changing`).

        Raises
        ------
        AttributeError
            If `attr` is neither varying, an output nor a static column of `c`.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
        """
        varying = attr in c.dynamic or (
            attr in c.defaults.index and bool(c.defaults.at[attr, "varying"])
        )
        if (
            not varying
            and not _is_output(c.defaults, attr)
            and attr not in c.static.columns
        ):
            msg = f"'{attr}' of '{c.name}' components has no long representation."
            raise AttributeError(msg)

        parts: list[DuckDBPyRelation | None] = []
        excluded: pd.Index = pd.Index([])
        if varying:
            parts.append(self._melt_dynamic(c, attr))
            excluded = c.dynamic[attr].columns
        static_backed = attr in c.defaults.index and bool(c.defaults.at[attr, "static"])
        if not varying or static_backed:
            parts.append(self._scalar_rel(c, attr, excluded, unchanged=unchanged))
        return _union_all([p for p in parts if p is not None])

    def _melt_dynamic(self, c: pypsa.Components, attr: str) -> DuckDBPyRelation | None:
        """Melt one varying attribute's wide dynamic frame to long rows, `None` where it has no columns.

        The frame is transposed before the melt, so the rows come out entity by
        entity: a parquet row group then holds a handful of entity names, which
        its dictionary encodes, where snapshot-major rows put every name in
        each row group and leave them unencoded.

        The snapshots are melted by position and looked up in a typed list of
        the labels, so a label never becomes an SQL identifier and keeps its
        dtype.
        """
        con = self.con
        wide = c.dynamic[attr]
        stochastic = isinstance(wide.columns, pd.MultiIndex)
        valcols = (
            [f"{s}\x1f{e}" for s, e in wide.columns]
            if stochastic
            else list(wide.columns)
        )
        if not valcols or wide.empty:
            return None
        positional = [f"s{i}" for i in range(len(wide.index))]
        w = (
            wide.T.set_axis(positional, axis=1)
            .set_axis(pd.Index(valcols, name="colname"))
            .reset_index()
        )
        wide_rel = con.from_df(w)  # noqa: F841 - bound by name for the query below
        inlist = ", ".join(f"{p} AS {lit(str(i))}" for i, p in enumerate(positional))
        # Two steps: the replacement scan does not compose through a subquery.
        melted = con.sql(  # noqa: F841 - bound by name for the query below
            "SELECT colname, CAST(pos AS INTEGER) AS pos, value FROM wide_rel "
            f"UNPIVOT INCLUDE NULLS (value FOR pos IN ({inlist}))"
        )
        if stochastic:
            entity = "split_part(colname, chr(31), 2)"
            scenario = "split_part(colname, chr(31), 1)"
        else:
            entity, scenario = "colname", "CAST(NULL AS VARCHAR)"
        labels = lit(self._snapshot_labels.tolist())
        return con.sql(
            f"SELECT {entity} AS entity, ({labels})[pos + 1] AS {SNAPSHOT}, "
            f"{scenario} AS scenario, value FROM melted"
        )

    def _scalar_rel(
        self,
        c: pypsa.Components,
        attr: str,
        exclude: pd.Index,
        *,
        unchanged: str | None = None,
    ) -> DuckDBPyRelation | None:
        """Untyped-NULL-snapshot long rows from `attr`'s static scalars (`_scalar_rows`), `None` where none remain."""
        static = _shared_across_scenarios(_scalar_rows(c, attr, exclude))
        if unchanged is not None and attr in c.defaults.index:
            default = c.defaults.at[attr, "default"]
            static = self._changing(c, unchanged, static, default)
        if static.empty:
            return None
        return self.con.sql(
            "SELECT entity, NULL AS snapshot, scenario, value FROM static"
        )

    def _changing(
        self, c: pypsa.Components, attribute: str, rows: pd.DataFrame, default: Any
    ) -> pd.DataFrame:
        """`rows` but those repeating what `attribute` resolves to without them.

        An entity `base` holds rows of `attribute` for is compared with each
        of them at the row's scenario or none, at any for a row at none, and
        left out only where each is static and equal, NULL equal to NULL. Any
        other entity is compared with `default`, as without `base`
        (`_drop_default_rows`). The declared default of an entity `base`
        lacks is the registry's, `default`, as this layer's schema says.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        rows = rows.reset_index(drop=True)
        keep = pd.Series(rows.index.isin(_drop_default_rows(rows, default).index))
        key = (attribute, _dim_of(c.name))
        if key not in self._base_statics:
            self._base_statics[key] = self._base_static(*key)
        held = self._base_statics[key]
        if held is None:
            return rows[keep]
        pairs = rows.reset_index().merge(held, on=ENTITY, suffixes=("", "_base"))
        scenario, scenario_base = pairs[SCENARIO], pairs[f"{SCENARIO}_base"]
        at = scenario.isna() | scenario_base.isna() | (scenario == scenario_base)
        pairs = pairs[at]
        value, value_base = pairs["value"], pairs["value_base"]
        equal = (value == value_base) | (value.isna() & value_base.isna())
        repeats = (pairs["static"] & equal).groupby(pairs["index"]).all()
        keep[repeats.index] = ~repeats
        return rows[keep]

    @cached_property
    def _base_statics(self) -> dict[tuple[str, str], pd.DataFrame | None]:
        """`_base_static` by its arguments, read once for the component types sharing an attribute."""
        return {}

    def _base_static(self, attribute: str, dim: str) -> pd.DataFrame | None:
        """`base`'s rows of `attribute` keyed by `dim`, one `(entity, scenario, value, static)` per entity and scenario; `None` where it has none.

        A row is static where no key but `dim` and `scenario` is set, and an
        entity at a scenario where each of its rows is; only then is `value`
        its one row's. Aggregated in DuckDB: a series' rows never reach pandas.
        """
        rel = None if self.base is None else self.base.attributes.get(attribute)
        if rel is None or dim not in rel.columns:
            return None
        others = [
            c for c in rel.columns if c not in (dim, SCENARIO, "attribute", "value")
        ]
        scenario = col(SCENARIO) if SCENARIO in rel.columns else lit(None)
        static = ex_all([lit(True), *(col(c).isnull() for c in others)])
        keyed = rel.filter(col(dim).isnotnull()).project(
            col(dim).cast(sqltypes.VARCHAR).alias(ENTITY),
            scenario.cast(sqltypes.VARCHAR).alias(SCENARIO),
            col("value"),
            static.alias("static"),
        )
        return keyed.aggregate(
            "entity, scenario, any_value(value) AS value, bool_and(static) AS static"
        ).df()

    @property
    def attributes(self) -> LazyRelations:
        """Every input that is no property, the weightings of each axis the network holds among them, and every result with rows."""
        names = self._attribute_names
        results = self._result_rels

        def frame(attr: str) -> DuckDBPyRelation:
            if attr in results:
                return results[attr]
            return self._long_frame(attr, names[attr])

        return LazyRelations((*names, *results), frame)

    @cached_property
    def _attribute_names(self) -> dict[str, list[str]]:
        """Input attribute -> the exported types carrying it."""
        names: dict[str, list[str]] = {}
        for c in self.n.components:
            if not _exported(c):
                continue
            for attr in self._record_attributes(c):
                names.setdefault(attr, []).append(c.name)
        for attribute, weighting in _WEIGHTINGS.items():
            if len(self._weighting_table(weighting.dim)):
                names[attribute] = []
        return names

    def _weighting_table(self, dim: str) -> pd.DataFrame:
        """PyPSA's weighting frame of `dim` by label, empty where the network lacks the axis."""
        if dim == SNAPSHOT:
            return self.n.snapshot_weightings.set_axis(self._snapshot_labels)
        if dim == PERIOD:
            return self.n.investment_period_weightings
        return self.n.scenario_weightings

    def _weighting_frame(self, attribute: str) -> DuckDBPyRelation:
        """One axis weighting as long rows over its axis."""
        weighting = _WEIGHTINGS[attribute]
        table = self._weighting_table(weighting.dim)
        weights = (  # noqa: F841 - bound by name for the query below
            table[weighting.column].rename("value").reset_index()
        )
        return self.con.sql(
            f"SELECT * EXCLUDE (value), {lit(attribute)} AS attribute, "
            "NULL AS breakpoint, CAST(value AS DOUBLE) AS value FROM weights"
        )

    @cached_property
    def _output_names(self) -> dict[str, list[str]]:
        """Result attribute -> the exported types declaring it, as the schema does."""
        names: dict[str, list[str]] = {}
        for ctype, results in self._results.items():
            for attr in sorted(set(results.values())):
                names.setdefault(attr, []).append(ctype)
        return names

    @cached_property
    def _result_rels(self) -> dict[str, DuckDBPyRelation]:
        """Result attribute -> its long rows across types, where any type has rows.

        Notes
        -----
        - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
        """
        return {
            attr: rel
            for attr, ctypes in self._output_names.items()
            if (rel := self._output_rel(attr, ctypes)) is not None
        }

    @cached_property
    def _results(self) -> dict[str, dict[str, str]]:
        """Exported type -> its `_result_attributes`."""
        inputs = _registry().inputs
        return {
            c.name: _result_attributes(c, inputs)
            for c in self.n.components
            if _exported(c)
        }

    @property
    def properties(self) -> LazyRelations:
        """The keys and property columns of the entities, labelled axes, connections and multi-period snapshots."""
        labelled = [d for d in (BUS, CARRIER, SUB_NETWORK) if len(self._axes[d])]
        timed = [SNAPSHOT] if PERIOD in self._snapshots else []
        subjects = (ENTITY, *labelled, CONNECTION, *timed)

        def frame(subject: str) -> DuckDBPyRelation:
            if subject == CONNECTION:
                rel = self._connection_frame()
            elif subject == ENTITY:
                rel = self.con.from_df(self._entity_frame())
            else:
                rel = self.con.from_df(self._axes[subject])
            declared = {subject, *self.schema.properties[subject]}
            return rel.project(*(col(c) for c in rel.columns if c in declared))

        return LazyRelations(subjects, frame)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Never consulted: `write_layer` persists frames, not flags."""
        return {}

    # -- key sets -----------------------------------------------------------

    def _record_attributes(self, c: pypsa.Components) -> list[str]:
        """`c`'s inputs that are no property, per-port ones as their stem, its custom columns placed as attributes among them.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        defaults = c.defaults
        skipped = {"name", *_output_attributes(c), *_PROPERTIES[_dim_of(c.name)]}
        per_port = _port_stems(c)
        stems: list[str] = []
        placed = self.schema.properties[_dim_of(c.name)]
        custom = [name for name in _custom_names(c) if name not in placed]
        for attr in [*defaults.index, *custom]:
            if attr in skipped:
                continue
            if attr in defaults.index and not (
                defaults.at[attr, "varying"] or attr in c.static.columns
            ):
                continue
            stem = per_port.get(attr, attr)
            if stem != BUS and stem not in stems:
                stems.append(stem)
        return stems

    # -- frames -------------------------------------------------------------

    def _static_frame(self, ctype: str) -> pd.DataFrame:
        """Members of one type with the properties of `entity` it has columns for."""
        static = _own_static(self.n.c[ctype])
        columns = [p for p in self.schema.properties[ENTITY] if p in static.columns]
        frame = static[columns].reset_index().rename(columns={"name": "entity"})
        return _unscoped(frame, [ENTITY])

    def _tagged_long_rel(
        self,
        c: pypsa.Components,
        attribute: str,
        source_attr: str,
        *,
        drop_defaults: bool,
    ) -> DuckDBPyRelation | None:
        """PyPSA's `source_attr` as long rows of `attribute`, a scalar, keyed by `c`'s dim; `None` where it has none."""
        unchanged = attribute if drop_defaults else None
        rel = self._as_long_rel(c, source_attr, unchanged=unchanged)
        if rel is None:
            return None
        dim = _dim_of(c.name)
        return rel.select(
            *(
                col(name).alias(dim) if name == ENTITY else col(name)
                for name in rel.columns
            ),
            lit(attribute).alias("attribute"),
            lit(None).alias("breakpoint"),
        )

    def _port_long_rel(
        self,
        c: pypsa.Components,
        attribute: str,
        column: str,
        port: str,
        *,
        drop_defaults: bool,
    ) -> DuckDBPyRelation | None:
        """`column`'s long rows at `port`, tagged `attribute`, keyed by connection; `None` where the port has no bus column or no rows.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        """
        static = c.static
        bus_column = _port_attribute(BUS, port, c.defaults.index)
        if bus_column not in static.columns:
            return None
        buses = static[bus_column]
        if isinstance(buses.index, pd.MultiIndex):
            buses = buses.droplevel(SCENARIO)
            buses = buses[~buses.index.duplicated()]
        buses = buses[buses.astype(str) != ""]
        bus_df = buses.rename("bus").rename_axis("entity").reset_index()  # noqa: F841
        bus_rel = self.con.sql(  # noqa: F841 - bound by name for the join below
            "SELECT CAST(entity AS VARCHAR) entity, bus FROM bus_df"
        )
        v = self._tagged_long_rel(c, attribute, column, drop_defaults=drop_defaults)
        if v is None:
            return None
        return (
            v.set_alias("v")
            .join(bus_rel.set_alias("b"), col("v", ENTITY) == col("b", ENTITY))
            .project(
                *(col("v", name) for name in v.columns if name != ENTITY),
                _connection_struct(col("v", ENTITY), col("b", BUS)),
            )
        )

    def _rows(
        self,
        c: pypsa.Components,
        attribute: str,
        column: str,
        port: str | None,
        *,
        drop_defaults: bool,
    ) -> DuckDBPyRelation | None:
        """PyPSA's `column` as long rows of `attribute`, keyed by its connection where it is at a `port`."""
        if port is None:
            return self._tagged_long_rel(
                c, attribute, column, drop_defaults=drop_defaults
            )
        return self._port_long_rel(
            c, attribute, column, port, drop_defaults=drop_defaults
        )

    def _long_frame(self, attribute: str, ctypes: list[str]) -> DuckDBPyRelation:
        """One input's long rows across `ctypes`, typed as declared where none has rows.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/model/attributes/#wide-and-long-rows)
        """
        if attribute in _WEIGHTINGS:
            return self._weighting_frame(attribute)
        rels = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            ports: dict[str, str | None] = {
                name: port
                for name, (stem, port) in _port_names(c).items()
                if stem == attribute
            }
            if not ports and (
                attribute in c.defaults.index or attribute in c.static.columns
            ):
                ports = {attribute: None}
            rels += [
                self._rows(c, attribute, name, port, drop_defaults=port is None)
                for name, port in ports.items()
            ]
        columns = list(self.schema.long_columns_for(attribute))
        rel = _union_all([r for r in rels if r is not None])
        if rel is not None:
            return rel.select(*map(col, columns))
        schema = _registry().schema
        return empty_relation(
            self.con, **{c: schema.column_type(c) or sqltypes.VARCHAR for c in columns}
        )

    def _output_rel(self, attribute: str, ctypes: list[str]) -> DuckDBPyRelation | None:
        """One result's long rows across `ctypes`, as declared; `None` where none has rows."""
        rels = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            ports = _port_names(c)
            for column, name in sorted(self._results[ctype].items()):
                if name != attribute:
                    continue
                port = ports[column][1] if column in ports else None
                drop = self._DROP_RESULT_DEFAULTS
                try:
                    rels.append(self._rows(c, name, column, port, drop_defaults=drop))
                except (AttributeError, KeyError):
                    # Not every registry output has a container on every network.
                    continue
        rel = _union_all([r for r in rels if r is not None])
        if rel is None:
            return None
        held = _registry().schema.long_columns_for(attribute)
        return rel.select(*map(col, held)).select(
            *self._as_declared(attribute, self.schema.long_columns_for(attribute))
        )

    def _as_declared(self, attribute: str, columns: Iterable[str]) -> list[Expression]:
        """`columns` of `attribute`'s rows, each PyPSA's schema lacks NULL: the network's value at every label."""
        held = _registry().schema.long_columns_for(attribute)

        def everywhere(dim: str) -> Expression:
            declared = self.schema.duck_type(self.schema.dimensions[dim])
            return lit(None).cast(declared).alias(dim)

        return [col(c) if c in held else everywhere(c) for c in columns]


@dataclass(frozen=True)
class _ResultsSource(_NetworkSource):
    """A solved `pypsa.Network`'s results, as a thin layer over `base`.

    Its results, at their default too, `base`'s result rows it does not write
    again as NULL, and the `sub_network` axis restated whole.

    Notes
    -----
    - [writing results](https://energy-models.github.io/datarecord/design/tools/pypsa/#writing-results)
    """

    #: The record the network was built from.
    base: RecordLike

    _DROP_RESULT_DEFAULTS: ClassVar[bool] = False

    @cached_property
    def schema(self) -> RecordSchema:
        """`base`'s schema: a results layer declares nothing the record does not."""
        return self.base.schema

    @property
    def dims(self) -> LazyRelations:
        """The `sub_network` axis: this solve's SubNetworks, as `_restates` says."""
        return LazyRelations(
            self._restates, lambda _: self._sub_network_rel().project(col(SUB_NETWORK))
        )

    @property
    def properties(self) -> LazyRelations:
        """This solve's SubNetworks' properties."""
        return LazyRelations(self._restates, lambda _: self._sub_network_rel())

    @cached_property
    def _restates(self) -> tuple[str, ...]:
        """The axes this layer restates: `sub_network` where the solve or `base` has one, empty if only `base` does."""
        held = len(self._sub_networks) > 0 or SUB_NETWORK in self.base.dims
        return (SUB_NETWORK,) if held else ()

    def _sub_network_rel(self) -> DuckDBPyRelation:
        """`_sub_networks` with its columns typed as the schema declares them, not as an empty frame infers them."""
        rel = self.con.from_df(self._sub_networks)
        return cast_declared(self.schema, rel, SUB_NETWORK)

    def _dead_sub_networks(self) -> DuckDBPyRelation | None:
        """`base`'s SubNetworks this solve does not number, aliased `d`; `None` where `base` has no axis."""
        axis = self.base.dims.get(SUB_NETWORK)
        if axis is None:
            return None
        live = self._sub_network_rel().set_alias("n")
        return (
            axis.set_alias("a")
            .join(live, col("a", SUB_NETWORK) == col("n", SUB_NETWORK), how="anti")
            .set_alias("d")
        )

    @property
    def attributes(self) -> LazyRelations:
        """Every result with rows, `base`'s included."""
        results = self._restated
        return LazyRelations(tuple(results), results.__getitem__)

    @cached_property
    def _restated(self) -> dict[str, DuckDBPyRelation]:
        """This solve's results, then each row of `base`'s it leaves out as NULL, but those of a SubNetwork it drops."""
        written = self._result_rels
        outputs = _registry().schema.attributes.keys() - _registry().inputs
        dead = self._dead_sub_networks()
        restated = dict(written)
        for attr in sorted(outputs & set(self.base.attributes)):
            columns = self.schema.long_columns_for(attr)
            keys = [c for c in columns if c != "value"]
            stale = (
                self.base.attributes[attr]
                .select(*self._as_declared(attr, keys))
                .distinct()
                .set_alias("b")
            )
            if attr in written:
                stale = stale.join(
                    written[attr].set_alias("w"), null_safe("b", "w", keys), how="anti"
                )
            if dead is not None and SUB_NETWORK in keys:
                stale = stale.set_alias("s").join(
                    dead, col("s", SUB_NETWORK) == col("d", SUB_NETWORK), how="anti"
                )
            null = lit(None).cast(self.schema.duck_type(self.schema.attributes[attr]))
            rows = stale.project(*map(col, keys), null.alias("value"))
            restated[attr] = rows if attr not in written else written[attr].union(rows)
        return restated


def _held(relations: Mapping[Any, Relations], key: str) -> dict[Any, DuckDBPyRelation]:
    """`key`'s relation in each member's `relations` that holds it."""
    return {label: rels[key] for label, rels in relations.items() if key in rels}


def _keys(relations: Mapping[Any, Relations]) -> tuple[str, ...]:
    """Every member's keys of `relations`, in the order first held."""
    return tuple(dict.fromkeys(k for rels in relations.values() for k in rels))


def _difference(
    attr: str, held: Mapping[Any, DuckDBPyRelation], labels: Sequence[Any], by: str
) -> str | None:
    """Where `attr`'s rows, `held` by each member holding it, first differ across the members' `labels` of `by`; None where they agree.

    A row held by one of two members differs, one held by both as `_clashing`
    says. Each member is compared with the first alone, to hold two in memory.
    """
    lacking = [label for label in labels if label not in held]
    if lacking:
        return f"`{attr}` is held by {by} {list(held)} and not by {lacking}"
    (first, rel), *rest = held.items()
    reference = _values(rel)
    for label, other in rest:
        long = pd.concat({first: reference, label: _values(other)}, names=[by])
        keys = list(range(1, long.index.nlevels))
        grouped = long.groupby(level=keys, sort=False, dropna=False)
        apart = (grouped.transform("size") < 2) | _clashing(long, keys)
        if apart.any():
            coordinates, held_by = _first_apart(long, apart)
            where = ", ".join(
                f"{name} {value!r}"
                for name, value in zip(long.index.names[1:], coordinates, strict=True)
                if name not in ("attribute", "breakpoint") and not pd.isna(value)
            )
            return f"{where} has `{attr}` {held_by} by {by}"
    return None


def _values(rel: DuckDBPyRelation) -> pd.Series:
    """One attribute's long rows as its values by their key columns."""
    frame = _unpack_connection(rel).df()
    return frame.set_index([c for c in frame.columns if c != "value"])["value"]


def _widened(
    schema: RecordSchema, dim: str, labels: Sequence[Any], differing: Container[str]
) -> RecordSchema:
    """`schema` with each of `differing` over `dim`, and `dim`, if new, declared from `labels` and `partial`."""
    attributes = {
        name: spec.model_copy(update={"dims": spec.dims | {dim}})
        if name in differing
        else spec
        for name, spec in schema.attributes.items()
    }
    dimensions = dict(schema.dimensions)
    partial = schema.partial or frozenset()
    if dim not in dimensions:
        added = Dimension(dtype=_column_dtype(pd.Series(labels)))
        dimensions = {dim: added, **dimensions}
        partial = partial | {dim}
    return _rebuilt(
        schema, dimensions=dimensions, attributes=attributes, partial=partial
    )


def _axis_union(
    schema: RecordSchema,
    subject: str,
    held: Mapping[Any, DuckDBPyRelation],
    by: str,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """The members' `held` relations of one axis or its properties as one, typed as declared.

    Its labels are in one order (`_ordered_union`), each column's values agreed (`_agreed`).
    """
    keys = [ENTITY, BUS] if subject == CONNECTION else [subject]
    keyed = {
        label: _unpack_connection(rel).df().set_index(keys)
        for label, rel in held.items()
    }
    order = _ordered_union(subject, {label: f.index for label, f in keyed.items()}, by)
    columns = next(iter(keyed.values())).columns
    agreed = {
        c: _agreed(subject, c, {label: f[c] for label, f in keyed.items()}, by)
        for c in columns
    }
    frame = pd.DataFrame(agreed, index=order, columns=columns)
    rel = con.from_df(frame.reset_index(names=keys))
    if subject == CONNECTION:
        rel = rel.project(
            _connection_struct(col(ENTITY), col(BUS)), star(exclude=[ENTITY, BUS])
        )
    return cast_declared(schema, rel, subject)


def _labelled(
    schema: RecordSchema, attr: str, held: Mapping[Any, DuckDBPyRelation], dim: str
) -> DuckDBPyRelation:
    """Each member's rows of `attr`, an attribute over `dim`, at its label."""
    columns = schema.long_columns_for(attr)
    label_type = schema.duck_type(schema.dimensions[dim])

    def at(label: Any, rel: DuckDBPyRelation) -> DuckDBPyRelation:
        labelled = lit(label).cast(label_type).alias(dim)
        rows = rel.select(*(labelled if c == dim else col(c) for c in columns))
        return cast_declared(schema, rows, attribute=attr)

    return reduce(DuckDBPyRelation.union, (at(*item) for item in held.items()))


def _equal_weights(
    schema: RecordSchema, labels: Sequence[Any], con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """`scenario_weight`'s rows, each of `labels` weighing equally."""
    weights = pd.DataFrame({SCENARIO: list(labels), "value": 1 / len(labels)})
    rel = con.from_df(weights).select(
        col(SCENARIO),
        lit(SCENARIO_WEIGHT).alias("attribute"),
        lit(None).alias("breakpoint"),
        col("value"),
    )
    columns = schema.long_columns_for(SCENARIO_WEIGHT)
    return cast_declared(
        schema, rel.select(*map(col, columns)), attribute=SCENARIO_WEIGHT
    )


@dataclass(frozen=True)
class _StackedSource:
    """A `NetworkCollection`'s members, each presented on its own, as one record along a dim (`of`).

    Notes
    -----
    - [a collection](https://energy-models.github.io/datarecord/design/tools/pypsa/#a-collection)
    """

    schema: RecordSchema
    dims: Relations
    properties: Relations
    attributes: Relations

    @classmethod
    def of(
        cls,
        members: Mapping[Any, _NetworkSource],
        dim: str,
        con: DuckDBPyConnection,
    ) -> _StackedSource:
        """`members`, each source by its label of `dim`, as one record, checked whole.

        Their axes and properties are one union (`_axis_union`). An attribute
        whose rows differ across the members (`_difference`) is over `dim`,
        each member's rows at its label; any other is held once. A
        `scenario` collection's scenarios weigh equally.

        Raises
        ------
        ValueError
            If two members declare different dims, properties or attributes,
            order the labels they share otherwise (`_ordered_union`), hold
            different properties for one label (`_agreed`), or, `dim` being
            PyPSA's, differ in an attribute it does not declare over `dim`.
        """
        (first, schema), *rest = ((label, m.schema) for label, m in members.items())
        declared = (schema.dimensions, schema.properties, schema.attributes)
        for label, other in rest:
            if (other.dimensions, other.properties, other.attributes) != declared:
                msg = (
                    f"The network of {dim} {label!r} declares other dims, "
                    f"properties or attributes than that of {dim} {first!r}"
                )
                raise ValueError(msg)
        labels = list(members)
        attributes = {label: m.attributes for label, m in members.items()}
        differences = {
            attr: _difference(attr, _held(attributes, attr), labels, dim)
            for attr in _keys(attributes)
        }
        differing = {attr for attr, found in differences.items() if found}
        if dim in _registry().schema.dimensions:
            for attr in sorted(differing):
                if dim not in schema.attributes[attr].dims:
                    msg = (
                        f"{differences[attr]}, which PyPSA does not vary over "
                        f"`{dim}`: name a dim of your own, such as `weather_year`, "
                        "for members whose values of it differ"
                    )
                    raise ValueError(msg)
        schema = _widened(schema, dim, labels, differing)
        held_dims = {label: m.dims for label, m in members.items()}
        dims = {
            d: _axis_union(schema, d, _held(held_dims, d), dim, con)
            for d in _keys(held_dims)
            if d != dim
        }
        axis = con.from_df(pd.DataFrame({dim: labels}))
        dims[dim] = cast_declared(schema, axis, dim)
        held_properties = {label: m.properties for label, m in members.items()}
        properties = {
            s: _axis_union(schema, s, _held(held_properties, s), dim, con)
            for s in _keys(held_properties)
        }
        rows = {}
        for attr in _keys(attributes):
            held = _held(attributes, attr)
            agreed = next(iter(held.values()))
            rows[attr] = (
                _labelled(schema, attr, held, dim) if attr in differing else agreed
            )
        if dim == SCENARIO:
            rows[SCENARIO_WEIGHT] = _equal_weights(schema, labels, con)
        return cls(schema, dims, properties, rows)

    def flags(self, **categories: str | Sequence[str]) -> dict[str, Flags]:
        """Never consulted, as `_NetworkSource.flags` is not."""
        return {}


def _required_attributes(ctype: str) -> frozenset[str]:
    """`ctype`'s mandatory input attributes, from PyPSA's registry, but `name`, which the member list supplies."""
    from pypsa.components.types import get as get_component_type

    defaults = get_component_type(ctype).defaults
    required = defaults.index[defaults["status"].astype(str) == "Input (required)"]
    return frozenset(required) - {"name"}


def _connection_frame(record: RecordLike) -> pd.DataFrame | None:
    """Every type's connections as flat `(entity, bus, ...)` rows, read once for `_ordered_connections` to scope per type."""
    frame = record.properties.get(CONNECTION)
    if frame is None:
        return None
    return _unpack_connection(frame).df()


def _tuples_of(tuples: DuckDBPyRelation, members: DuckDBPyRelation) -> DuckDBPyRelation:
    """The `connection` tuples of `tuples` whose entity is one of `members`."""
    return tuples.set_alias("t").join(
        members.project(col(ENTITY)).set_alias("m"),
        fn.struct_extract(col("t", CONNECTION), lit(ENTITY)) == col("m", ENTITY),
        how="semi",
    )


def _ordered_connections(
    frame: pd.DataFrame | None, members: DuckDBPyRelation | None
) -> pd.DataFrame | None:
    """`members`' connections out of `frame`, numbered per component in row order, inputs first.

    A restatement that appends a connection renumbers none, and `bus0` is the
    input end PyPSA's sign convention expects. A one-port component's port is `""`.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/layers/resolution/#owner-map)
    - [one record over one fold](https://energy-models.github.io/datarecord/design/layers/resolution/#one-record-over-one-fold)
    """
    if frame is None or members is None:
        return None
    mine = {entity for (entity,) in members.project(col(ENTITY)).fetchall()}
    df = frame[frame["entity"].isin(mine)]
    if df.empty:
        return None
    df = df.assign(_role_rank=(df["role"] != _INPUT_PORT).astype(int)).sort_values(
        ["entity", "_role_rank"], kind="stable"
    )
    grouped = df.groupby(["entity"], dropna=False)
    single = grouped["bus"].transform("size").eq(1) & df["role"].eq(_SINGLE_PORT)
    df["port"] = grouped.cumcount().astype(str).where(~single, "")
    return df


@dataclass(frozen=True)
class _TypeMembers:
    """One type's members and numbered connections, read once for `verify` and `build` to share."""

    #: `_members`: the static frame PyPSA's `n.<type>` starts from.
    members: DuckDBPyRelation | None
    #: `_ordered_connections`: the type's connections with their port index.
    connections: pd.DataFrame | None

    @classmethod
    def of(
        cls, record: RecordLike, ctype: str, connections: pd.DataFrame | None
    ) -> _TypeMembers:
        """`ctype`'s reads, scoping `connections` (`_connection_frame`) to it."""
        members = _members(record, ctype)
        return cls(members, _ordered_connections(connections, members))

    @property
    def columns(self) -> frozenset[str]:
        """The properties the members carry, defaults applied."""
        return (
            frozenset(self.members.columns) if self.members is not None else frozenset()
        )

    def port_attributes(self, ctype: str) -> frozenset[str]:
        """The `bus0`/`bus1`/... columns the connections supply for every member, not an entity left attached to no bus.

        Notes
        -----
        - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
        - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
        """
        if self.connections is None or self.members is None:
            return frozenset()
        members = {entity for (entity,) in self.members.project(col(ENTITY)).fetchall()}
        registered = _registered(ctype)
        return frozenset(
            _port_attribute(BUS, port, registered)
            for port, rows in self.connections.groupby("port")
            if members <= set(rows[ENTITY])
        )


def _collapse_connections(
    static: DuckDBPyRelation,
    connections: pd.DataFrame | None,
    ctype: str,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Add `bus0`/`bus1`/... and per-port input columns to a static frame from its connection rows.

    Parameters
    ----------
    connections
        `ctype`'s connections as `_ordered_connections` numbers them, which
        `_port_columns` places per-connection values by.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    """
    if connections is None:
        return static
    registered = _registered(ctype)
    stems = [BUS, *(s for s in sorted(_per_port_stems() - {BUS}) if s in connections)]
    pieces = []
    for stem in stems:
        piece = connections.pivot(index="entity", columns="port", values=stem)
        piece = piece[[p for p in piece if _carries(stem, p, registered)]]
        piece.columns = [_port_attribute(stem, port, registered) for port in piece]
        pieces.append(piece if stem == BUS else piece.dropna(axis=1, how="all"))
    wide = pd.concat(pieces, axis=1).reset_index()  # noqa: F841 - queried by name below
    ports = con.sql("FROM wide").set_alias("ports")
    joined = static.set_alias("s").join(
        ports, col("s", ENTITY) == col("ports", ENTITY), how="left"
    )
    return joined.project(
        *(col("s", c) for c in static.columns),
        *(col("ports", c) for c in ports.columns if c != ENTITY),
    )


def _connection_struct(entity: Expression, bus: Expression) -> Expression:
    """The `connection` column packed from `entity` and `bus`, typed as declared.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
    """
    schema = _registry().schema
    struct = schema.duck_type(schema.dimensions[CONNECTION])
    return struct_of({ENTITY: entity, BUS: bus}).cast(struct).alias(CONNECTION)


def _unpack_connection(long: DuckDBPyRelation) -> DuckDBPyRelation:
    """`long` with its `connection` struct spread to `entity` and `bus`, if it has one."""
    if CONNECTION not in long.columns:
        return long
    return long.project(
        *(col(c) for c in long.columns if c != CONNECTION),
        *(
            fn.struct_extract(col(CONNECTION), lit(field)).alias(field)
            for field in (ENTITY, BUS)
        ),
    )


def _port_columns(
    long: DuckDBPyRelation,
    stem: str,
    connections: pd.DataFrame | None,
    ctype: str,
    con: DuckDBPyConnection,
) -> dict[str, DuckDBPyRelation]:
    """A per-connection attribute's rows split into PyPSA's port columns that carry `stem`.

    A row takes the port `_ordered_connections` numbered its `(entity, bus)`
    with, so it lands beside its bus; one naming no connection is dropped.

    Parameters
    ----------
    long
        The attribute's rows, carrying `entity` and `bus`.
    connections
        `ctype`'s connections as `_ordered_connections` numbers them.

    Notes
    -----
    - [ports](https://energy-models.github.io/datarecord/design/tools/pypsa/#ports)
    """
    if connections is None:
        return {}
    keyed = connections[[ENTITY, BUS, "port"]]  # noqa: F841 - queried by name below
    ports = con.sql("FROM keyed").set_alias("ports")
    placed = (
        long.set_alias("v")
        .join(ports, ex_all(col("v", c) == col("ports", c) for c in (ENTITY, BUS)))
        .project(*(col("v", c) for c in long.columns if c != BUS), col("ports", "port"))
    )
    registered = _registered(ctype)
    columns = {
        _port_attribute(stem, port, registered): port
        for port in connections["port"].unique()
        if _carries(stem, port, registered)
    }
    return {
        column: placed.filter(col("port") == lit(port))
        .project(star(exclude=["port"]))
        .set_alias(column)
        for column, port in columns.items()
    }


def _components(record: RecordLike) -> list[str]:
    """The `ctype` labels `record`'s entity axis has members of, sorted."""
    frame = record.properties.get(ENTITY)
    if frame is None or CTYPE not in frame.columns:
        return []
    labels = frame.project(col(CTYPE)).distinct().fetchall()
    return sorted(str(label) for (label,) in labels if label is not None)


def _members(record: RecordLike, ctype: str) -> DuckDBPyRelation | None:
    """`ctype`'s members in axis order, with the properties it takes (`_taken`), defaults applied, `ctype` left out.

    `None` where the record has no `ctype` property on its entity axis.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    """
    schema = record.schema
    if CTYPE not in schema.categorizing(ENTITY):
        return None
    rel = matching(record.properties[ENTITY], {CTYPE: (ctype,)})
    names = _taken(schema, ctype, schema.properties[ENTITY])
    defaulted = with_defaults(schema, ENTITY, rel, names, _connection(record))
    return _empty_for_none(defaulted.project(star(exclude=[CTYPE])))


#: The tool, a module-level singleton.
PyPSA = PyPSATool()
