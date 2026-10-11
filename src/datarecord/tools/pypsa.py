# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The PyPSA tool: record -> `pypsa.Network` -> results.

The only module that knows PyPSA's network shape - that its axes are
`snapshot`/`period`/`scenario`, that a stochastic network is indexed by
`(scenario, name)`, how its static/series split maps onto the record's
`dims/entity` + `inputs/` split, that a component's type is its
`entity_group`, and that buses, carriers and technology groups are axes rather
than entities.

Notes
-----
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
"""

from __future__ import annotations

import math
import re
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache, cached_property, reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any

import narwhals as nw
import numpy as np
import pandas as pd
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb import StarExpression as star

from datarecord.duck import ex_all, fn
from datarecord.record import EMPTY, Flags, Frames, LazyFrames, RecordLike
from datarecord.schema import AttributeSpec, Dimension, Group
from datarecord.schema import Schema as RecordSchema
from datarecord.tools.base import (
    Requirements,
    Schema,
    Tool,
    UnsupportedRecordError,
    to_relation,
)

if TYPE_CHECKING:
    import pypsa
    from mathspec.spec import Spec

# The axes a PyPSA network is built from; all three must be declared in the
# schema's `dimensions`, since a network shape is built from them (https://energy-models.github.io/datarecord/design/tools/).
# A declared axis with no rows is fine - that is just a deterministic or
# single-period network.
SNAPSHOT, PERIOD, SCENARIO = "snapshot", "period", "scenario"
ENTITY, BUS, CONNECTION = "entity", "bus", "connection"
CARRIER, TECHNOLOGY_GROUP = "carrier", "technology_group"
# An attribute over the `connection` group, not a column the format fixes: the
# port vocabulary below is this tool's, so the record layer never names it.
ROLE = "role"
ENTITY_GROUP = "entity_group"
REQUIRED_DIMS = frozenset({SNAPSHOT, PERIOD, SCENARIO})

# The record's names for PyPSA's axis weightings: `scenario_weightings.weight`,
# `snapshot_weightings.<w>` and `investment_period_weightings.<w>`. Prefixed,
# since the attribute namespace is flat and `weight`/`objective` are taken twice.
SCENARIO_WEIGHT = "scenario_weight"
_SNAPSHOT_WEIGHTINGS = ("objective", "stores", "generators")
_PERIOD_WEIGHTINGS = ("objective", "years")
# The period's year as a value: a dimension label is never a value to a
# mathspec spec, which compares it against `build_year`.
PERIOD_YEAR = "period_year"
EXTENDABLE = "extendable"
ACTIVE_IN_PERIOD = "active_in_period"

# PyPSA component types the record carries as axes rather than entities, and
# what their names become.
_AXIS_TYPES = {"Bus": BUS, "Carrier": CARRIER}
# Component types the record does not carry: shapes become bus region columns
# (`_bus_regions`), and global constraints belong to the math, not the data.
_NOT_EXPORTED = frozenset({"Shape", "GlobalConstraint"})
# Bus regions, from `Shape` rows with `component == "Bus"`, per `type`.
_REGIONS = {"onshore": "region_onshore", "offshore": "region_offshore"}
# `Shape` rows with no component, which pypsa-eur writes for context and the
# record drops (https://energy-models.github.io/datarecord/design/tools/#shapes).
_DROPPED_SHAPES = frozenset({"country", "offshore"})
# Component columns that are a function of the technology group, so they move
# to its axis file, with their declared types.
_TECHNOLOGY_GROUP_COLUMNS = {"resource_class": nw.Int64(), "nice_name": nw.String()}
DEFAULT_CRS = "EPSG:4326"

#: The base mathspec spec for the record `PyPSA.to_datarecord` writes.
MATHSPEC = Path(__file__).with_name("pypsa.mathspec.yaml")


def _classification(key: str, into: str) -> str:
    """A functional group's name: `<key>_<into>`, never the dim it maps into.

    A mathspec relation may not share a dimension's name, and the group is
    bound to one by name.
    """
    return f"{key}_{into}"


_CARRIER_GROUPS = {
    ENTITY: _classification(ENTITY, CARRIER),
    BUS: _classification(BUS, CARRIER),
    TECHNOLOGY_GROUP: _classification(TECHNOLOGY_GROUP, CARRIER),
}
TECHNOLOGY_GROUP_GROUP = _classification(ENTITY, TECHNOLOGY_GROUP)

# PyPSA's `defaults["typ"]` (the attribute's Python type) mapped to the narwhals
# type the long schema stores, `String` for anything unlisted. Keyed on `typ` -
# what a value *is* - not `type`, whose `"static or series"`-style strings
# describe only where it lives and grow a new variant per PyPSA release.
_DTYPES = {
    bool: nw.Boolean(),
    int: nw.Int64(),
    float: nw.Float64(),
    str: nw.String(),
}


def _default(value: Any) -> Any:
    """One `defaults["default"]` cell as JSON-storable, NaN as absent.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _text(value: Any) -> str | None:
    """One `unit`/`description` cell as text, or None where PyPSA has none.

    Its registry leaves both NaN rather than empty, and `None` is the schema's
    "undeclared" - `""` would claim the attribute is documented as blank, or
    dimensionless, neither of which a missing cell says.

    Notes
    -----
    - [unit and description](https://energy-models.github.io/datarecord/design/schema/#unit-and-description)
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def _column_dtype(column: pd.Series) -> nw.dtypes.DType:
    """A custom column's declared type, from its pandas dtype; `String` otherwise.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    if pd.api.types.is_bool_dtype(column):
        return nw.Boolean()
    if pd.api.types.is_integer_dtype(column):
        return nw.Int64()
    if pd.api.types.is_float_dtype(column):
        return nw.Float64()
    return nw.String()


def _as_wkt(frame: pd.DataFrame) -> pd.DataFrame:
    """`frame` with every shapely geometry cell written as WKT.

    The record stores geometry as WKT text, its CRS in `schema.meta["crs"]`.

    Notes
    -----
    - [geometry](https://energy-models.github.io/datarecord/design/tools/#geometry)
    """
    from shapely.geometry.base import BaseGeometry

    def wkt(value: Any) -> Any:
        return value.wkt if isinstance(value, BaseGeometry) else value

    out = frame.copy()
    for column in out.columns:
        if pd.api.types.is_object_dtype(out[column]):
            out[column] = out[column].map(wkt)
    return out


def _functional(frame: pd.DataFrame, key: str, into: str) -> pd.DataFrame:
    """`(key, into)` rows, one per `key`, empty labels left out.

    Raises
    ------
    ValueError
        If a `key` maps to more than one label, e.g. a carrier that differs
        between the scenarios of a stochastic network.

    Notes
    -----
    - [into](https://energy-models.github.io/datarecord/design/schema/#into-a-group-that-classifies)
    """
    rows = frame[[key, into]].dropna()
    rows = rows[rows[into].astype(str) != ""].drop_duplicates()
    clashing = sorted(rows.loc[rows[key].duplicated(), key].astype(str))
    if clashing:
        msg = f"{clashing} map to more than one {into!r}; each has exactly one"
        raise ValueError(msg)
    return rows.reset_index(drop=True)


@dataclass(frozen=True)
class NetworkShape:
    """PyPSA's reading of a record's axis frames.

    The axis names and the index convention live here rather than on the
    record, which stays schema-generic: a record may declare any dims, and it is
    this tool that decides `snapshot` is the time axis and that a non-empty
    `scenario` axis means a stochastic network.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """

    dims: Frames

    def _axis(self, dim: str) -> pd.DataFrame:
        """`dim`'s axis as a DataFrame, empty if the record declares none.

        A dim with no rows anywhere is absent from the mapping rather than
        present-and-empty, and both read the same way here: an empty
        frame, which is a deterministic or single-period network.

        Notes
        -----
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        """
        frame = self.dims.get(dim)
        return (
            frame.to_native().df() if frame is not None else pd.DataFrame(columns=[dim])
        )

    @cached_property
    def snapshots(self) -> pd.DataFrame:
        return self._axis(SNAPSHOT)

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
        """The static frame's index: PyPSA carries `scenario` as a level.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        return [SCENARIO, "entity"] if self.stochastic else ["entity"]


def _connection(record: RecordLike) -> DuckDBPyConnection:
    """The DuckDB connection `record`'s frames belong to.

    Off the concrete backing, since the protocol stays backend-agnostic.
    Needed because a relation exposes no reachable reference to its connection,
    and this tool's `PIVOT` - which narwhals cannot express and
    `relation.query` refuses as a `MULTI` statement - needs one.

    Raises
    ------
    TypeError
        If `record` exposes no connection, i.e. is not DuckDB-backed.

    Notes
    -----
    - [the protocol names no engine](https://energy-models.github.io/datarecord/design/record/#the-protocol-names-no-engine)
    """
    con = getattr(record, "con", None)
    if not isinstance(con, DuckDBPyConnection):
        msg = (
            f"{type(record).__name__} exposes no DuckDB connection; "
            "the PyPSA tool builds with DuckDB SQL"
        )
        raise TypeError(msg)
    return con


# -- long -> wide -----------------------------------------------------------

# Separates pivoted index-level values in a combined PIVOT column name; chosen
# to never collide with a component/scenario name (the broadcast rule).
_KEY_SEP = "\x01"


@dataclass(frozen=True)
class Index:
    """A pandas Index paired with a matching DuckDB relation.

    `rel` carries `_pos`, the index's row position, so a join against it can
    be restored to `index`'s order (not necessarily sorted) by ordering on
    `_pos` rather than by value.
    """

    index: pd.Index
    rel: DuckDBPyRelation

    @classmethod
    def of(
        cls,
        index: pd.Index,
        con: DuckDBPyConnection,
        names: dict[str, str] | None = None,
    ) -> Index:
        """`index` with its levels renamed by `names` in the relation alone."""
        df = index.to_frame(index=False).rename(columns=names or {})  # noqa: F841 - queried by name below
        return cls(index, con.sql("SELECT *, row_number() OVER () AS _pos FROM df"))

    def expand(self, rel: DuckDBPyRelation, column: str) -> DuckDBPyRelation:
        """Broadcast rows with a null `column` across this index, others as-is.

        `rel` must already carry the alias the caller wants to select its
        own columns by - `expand` doesn't impose one.
        """
        if len(self.index) == 0:
            return rel
        return rel.join(
            self.rel.set_alias("_ax"), col(rel.alias, column).isnull(), how="left"
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
) -> pd.DataFrame:
    """Set each varying attribute's static-valued entries, resolved to a wide frame."""
    keys = shape.index_names

    # `static` already carries a `_pos` in member order, assigned before the
    # connection collapse could scramble it (`build`); the wide frame is ordered
    # by it below (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).

    # An entity exists once, whatever the scenario, so the record's member
    # frame carries one row per entity - where PyPSA's static container is
    # `(scenario, name)`-indexed. Repeated here, and any attribute that really
    # differs between scenarios arrived as `inputs/` rows and is assigned over
    # the repeats below.
    if shape.stochastic and SCENARIO not in static.columns:
        static = scenarios.expand(
            static.project(f"*, NULL AS {SCENARIO}").set_alias("m"), SCENARIO
        )

    joined = static
    attr_exprs = {}
    for attr, (long, flags) in attributes.items():
        # PyPSA's `static` container takes the rows that do not vary over
        # `snapshot`. Two ways an attribute has them, and the flags distinguish
        # the two because they are scoped to what it is addressed by (https://energy-models.github.io/datarecord/design/record/#flags):
        # a NULL-snapshot row on an attribute that *may* vary over it puts
        # `snapshot` in `broadcast`, while one not addressed by it at all has
        # `snapshot` in neither set and every row static.
        addressed = SNAPSHOT in (flags.varies | flags.broadcast)
        if addressed and SNAPSHOT not in flags.broadcast:
            continue
        # No column to filter on where the attribute is not addressed by it.
        rows = long.filter("snapshot IS NULL") if addressed else long
        if shape.stochastic:
            rows = scenarios.expand(rows, SCENARIO)
        # First-wins on a duplicate key, same as `values[~values.index.duplicated()]`;
        # any deterministic order is fine since a valid record never actually collides.
        rows = rows.project(
            f"*, row_number() OVER (PARTITION BY {', '.join(keys)}) AS _rn"
        ).filter("_rn = 1")
        # LEFT JOIN from `static`, not `rows`, so a value with no matching static
        # row is dropped rather than added (`common = values.index.intersection(...)`).
        on = ex_all(col(static.alias, k) == col(rows.alias, k) for k in keys)
        joined = joined.join(rows, on, how="left")
        value = col(rows.alias, "value")
        attr_exprs[attr] = (
            coalesce(value, col(static.alias, attr))
            if attr in static.columns
            else value
        ).alias(attr)

    wide = joined.project(
        *(col(static.alias, c) for c in static.columns if c not in attr_exprs),
        *attr_exprs.values(),
    )
    frame = wide.order("_pos").project(star(exclude=["_pos"])).df()
    # Scenario-major: PyPSA's `(scenario, name)` index groups a scenario's
    # components together, and `_pos` gives the order within one - the
    # entity axis carrying one row per entity rather than one per scenario, so
    # it cannot order the outer level itself. A stable sort keeps that order.
    if shape.stochastic:
        frame = frame.sort_values(
            SCENARIO,
            kind="stable",
            key=lambda s: s.map({v: i for i, v in enumerate(scenarios.index)}),
        )
    return frame.set_index(keys)


def _flat_index(index: pd.Index, sep: str = "_") -> pd.Index[str]:
    """Join a MultiIndex's levels into one string-valued Index; other indexes as-is."""
    if not isinstance(index, pd.MultiIndex):
        return index

    return reduce(
        lambda x, y: x + sep + y,
        (index.get_level_values(lvl) for lvl in range(index.nlevels)),
    )


def _series_frame(
    rows: DuckDBPyRelation,
    shape: NetworkShape,
    periods: Index,
    scenarios: Index,
    snapshots: Index,
    static_index: pd.Index,
    con: DuckDBPyConnection,
) -> pd.DataFrame:
    """Pivot long series rows to the `n.snapshots x static_index` frame.

    `snapshots` (the row axis) is known before the pivot, so missing rows are
    filled by joining against it directly rather than reindexing pandas-side
    after. Columns stay restricted to what the data actually pivoted, only
    ordered to match `static_index`: `_import_series_from_df` takes
    `df.columns` as-is without merging against `static`, so a column added
    just to complete `static_index` would wrongly force that component into
    `dynamic` instead of leaving it static.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    if shape.multiperiod:
        rows = periods.expand(rows, PERIOD)
    if shape.stochastic:
        rows = scenarios.expand(rows, SCENARIO)
    group_by = [PERIOD, SNAPSHOT] if shape.multiperiod else [SNAPSHOT]
    pivot_key = f" || '{_KEY_SEP}' || ".join(shape.index_names)
    piv = con.sql(
        f"PIVOT rows ON {pivot_key} USING first(value) GROUP BY {', '.join(group_by)}"
    )

    present = set(piv.columns) - set(group_by)
    pivoted_names = _flat_index(static_index, _KEY_SEP)
    keep = pivoted_names.isin(present)

    wide = (
        snapshots.left_join(piv.set_alias("piv"), group_by)
        .project(*(col("piv", name) for name in pivoted_names[keep]))
        .df()
    )
    wide.index = snapshots.index
    wide.columns = static_index[keep]
    return wide


# -- build (https://energy-models.github.io/datarecord/design/tools/) -------------------------------------------------------------


def _exported(c) -> bool:
    """Whether `c`'s members are entities of the record.

    Empty types have nothing to write. Standard types (`LineType`,
    `TransformerType`) are excluded because PyPSA prepopulates them on every
    fresh `Network` and `_broadcast_standard_types` restores them on build -
    writing them would import each row a second time on the way back. Buses
    and carriers are axes (`_AXIS_TYPES`), and shapes and global constraints
    are not carried (`_NOT_EXPORTED`).

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    return (
        not c.static.empty
        and c.name not in c.n.standard_type_components
        and c.name not in _AXIS_TYPES
        and c.name not in _NOT_EXPORTED
    )


def _names(index: pd.Index) -> pd.Index:
    """A static frame's component names: its `name` level where it has several."""
    return index.get_level_values("name") if index.nlevels > 1 else index


def _colliding_names(n: pypsa.Network) -> frozenset[str]:
    """Names more than one exported component type claims.

    Only exported types count, since only those become member rows. Read off the
    `name` level rather than the index: a stochastic network is keyed
    `(scenario, name)`, and comparing tuples would read one component in two
    scenarios as two names while missing a real collision.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    seen: dict[str, str] = {}
    clashing: set[str] = set()
    for c in n.components:
        if not _exported(c):
            continue
        for value in _names(c.static.index):
            name = str(value)
            if seen.setdefault(name, c.name) != c.name:
                clashing.add(name)
    return frozenset(clashing)


# TODO(pypsa): every function below, up to `_new_network`, ports a method that
# only exists on PyPSA's unreleased data-records branch (https://energy-models.github.io/datarecord/design/tools/). Delete the
# ported copy and call the real method once a PyPSA release carries it.


def _apply_snapshots(n: pypsa.Network, df: pd.DataFrame) -> None:
    """Set snapshots and their weightings from the record's `snapshot` axis.

    A multi-period axis is keyed `(period, snapshot)`, which PyPSA calls
    `(period, timestep)`.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    levels = [c for c in (PERIOD, SNAPSHOT) if c in df.columns]
    frame = df.set_index(levels)
    if len(levels) == 2:
        frame.index = frame.index.set_names([PERIOD, "timestep"])
    n.set_snapshots(frame.index)
    weights = {
        w: frame[f"snapshot_weighting_{w}"].to_numpy()
        for w in _SNAPSHOT_WEIGHTINGS
        if f"snapshot_weighting_{w}" in frame.columns
    }
    if weights:
        n.snapshot_weightings = pd.DataFrame(weights, index=n.snapshots)


def _apply_periods(n: pypsa.Network, df: pd.DataFrame) -> None:
    """Set the investment period weightings from the record's `period` axis."""
    frame = df.set_index(PERIOD)
    n.investment_period_weightings = pd.DataFrame(
        {
            w: frame[f"period_weighting_{w}"]
            for w in _PERIOD_WEIGHTINGS
            if f"period_weighting_{w}" in frame.columns
        },
        index=frame.index,
    ).reindex(n.investment_periods)


def _broadcast_standard_types(n: pypsa.Network) -> None:
    """Broadcast standard-type static tables across scenarios after import.

    Ported from `pypsa.Network._broadcast_standard_types` (unreleased).

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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
    trimmed of nothing - the reflection over `dir(n)` is exactly what decides
    which attributes are safe to round-trip, so there is no smaller version of
    this that still answers the same question.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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

    Ported from `pypsa.Network._apply_network_attributes` (unreleased),
    stripped of the version-compat warnings and PyPI update check the real
    method also does: those are for a human importing a file written by an
    older PyPSA, and have nothing to check here since `to_datarecord` and this
    function always agree on what `attrs` holds.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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


def _new_network(schema: RecordSchema, shape: NetworkShape) -> pypsa.Network:
    """An empty network with the record's axes already established."""
    import pypsa

    n = pypsa.Network()
    _apply_network_attributes(n, schema.meta.get("attributes") or {})
    n.meta = schema.meta.get("meta") or {}

    n.crs = schema.meta.get("crs") or DEFAULT_CRS
    _apply_snapshots(n, shape.snapshots)
    if shape.multiperiod:
        _apply_periods(n, shape.periods)
    if shape.stochastic:
        scen = shape.scenarios.set_index(SCENARIO)
        scen.index = scen.index.astype(str).rename(SCENARIO)
        # `set_scenarios` rather than assigning `_scenarios_data`: it rebuilds
        # each component's empty `static`/`dynamic` frames with the `scenario`
        # level, so later imports union against a correctly named 2-level index
        # instead of the single-level one a fresh component carries.
        n.set_scenarios(scen[SCENARIO_WEIGHT].rename("weight"))
    return n


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
    static_df = _assign_static(static, attributes, shape, scenarios)
    n._import_components_from_df(static_df, ctype)

    periods = Index.of(n.investment_periods, con)
    snapshots = Index.of(n.snapshots, con, {"timestep": SNAPSHOT})
    static_index = n.c[ctype].static.index
    for attr, (long, flags) in attributes.items():
        # ... and `dynamic` the per-snapshot ones.
        if SNAPSHOT not in flags.varies:
            continue
        ts_rows = long.filter("snapshot IS NOT NULL")
        # Partial columns keep non-series components static (https://energy-models.github.io/datarecord/design/tools/).
        wide = _series_frame(
            ts_rows, shape, periods, scenarios, snapshots, static_index, con
        )
        n._import_series_from_df(wide, ctype, attr)


# -- results (https://energy-models.github.io/datarecord/design/read-path/#outputs) ---------------------------------------------------------


def _output_attributes(c: pypsa.Components) -> list[str]:
    """A component type's result attributes, from PyPSA's own registry.

    `defaults["status"]` marks them ("Output"), so a PyPSA upgrade that adds a
    result attribute is picked up rather than needing a list kept here.
    """
    defaults = c.defaults
    is_output = defaults["status"].astype(str).str.startswith("Output")
    return list(defaults.index[is_output])


# -- network -> layer (https://energy-models.github.io/datarecord/design/format/) ------------------------------------------------

# Which end of the component a port is. PyPSA encodes this only by sign
# convention - `p0` flows in at `bus0`, `p1` out at `bus1` - so the mapping to
# a record's `role` lives here, the one place that convention is written down
# (https://energy-models.github.io/datarecord/design/record/#connections). Ports beyond the first two are outputs: a multi-port Link's `bus2`
# onward are additional sinks.
_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT = "input", "output", "attached"

_PORT_SUFFIX = re.compile(r"(.+?)(\d+)")
_INTERNAL = "_internal"


def _port_role(port: str) -> str:
    """The `role` for a port index.

    A single-port component (`c.ports == [""]`, e.g. a Generator's one `bus`)
    is neither an input nor an output end - it has only one attachment - so it
    gets its own role rather than being forced into the two-ended convention.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    if port == "":
        return _SINGLE_PORT
    return _INPUT_PORT if port == "0" else _OUTPUT_PORT


def _port_attribute(
    stem: str, port: str, unsuffixed: frozenset[str] = frozenset()
) -> str:
    """PyPSA's name for `stem` at `port`: `bus0`, `p1`, `efficiency`, `efficiency2`, ...

    Suffixed by the port, except a multi-port type's `_unsuffixed_attrs`
    (a Link's `efficiency`, `delay`, `cyclic_delay`) at port `1`.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    return stem if port == "1" and stem in unsuffixed else f"{stem}{port}"


def _unsuffixed(c: pypsa.Components) -> frozenset[str]:
    """The stems `c` leaves unsuffixed at port `1`, from PyPSA's multi-port class."""
    from pypsa.components._types.mixin.multiports import _Multiport

    return frozenset(c._unsuffixed_attrs) if isinstance(c, _Multiport) else frozenset()


@cache
def _blank_network() -> pypsa.Network:
    """An empty network, whose components answer registry questions per type."""
    import pypsa

    return pypsa.Network()


@cache
def _port_stems() -> frozenset[str]:
    """Every attribute stem some PyPSA type carries once per port, from its registry.

    A registry name ending in one of the type's port digits (`bus0`, `p1`,
    `q0`, `rate0`), and the multi-port types' unsuffixed ones.
    """
    stems: set[str] = set()
    for ctype in _registry():
        c = _blank_network().c[ctype]
        ports = set(c.ports) - {""}
        stems |= _unsuffixed(c)
        stems |= {
            match.group(1)
            for attr in c.defaults.index
            if (match := _PORT_SUFFIX.fullmatch(attr)) and match.group(2) in ports
        }
    return frozenset(stems)


def _port_columns(c: pypsa.Components) -> dict[str, tuple[str, str]]:
    """`c`'s per-port attribute names -> `(stem, port)`, for its actual ports.

    A single-port type's `bus`, `p`, `efficiency` are per-port at port `""`.
    """
    unsuffixed = _unsuffixed(c)
    named = (
        (_port_attribute(stem, port, unsuffixed), stem, port)
        for stem in sorted(_port_stems())
        for port in c.ports
    )
    return {
        name: (stem, port) for name, stem, port in named if name in c.defaults.index
    }


def _record_names(c: pypsa.Components) -> dict[str, str]:
    """`c`'s registry attribute -> its record name.

    A per-port attribute is named by its stem; a per-entity one whose name is
    a port stem elsewhere (a Link's internal `p`) takes `_internal`.
    """
    ports = _port_columns(c)
    names = {}
    for attr in c.defaults.index:
        if attr in ports:
            names[attr] = ports[attr][0]
        elif attr in _port_stems():
            names[attr] = f"{attr}{_INTERNAL}"
        else:
            names[attr] = attr
    return names


def _extendable_attributes(c: pypsa.Components) -> list[str]:
    """`c`'s `<x>_nom_extendable` attribute, which the record calls `extendable`."""
    return [a for a in c.defaults.index if a.endswith("_nom_extendable")]


def _scenario_varying(c: pypsa.Components, columns: Sequence[str]) -> frozenset[str]:
    """Which of `columns` differ between scenarios for some component.

    PyPSA's static frame is `(scenario, name)`-indexed on a stochastic network,
    and its own `consistency_check` permits most attributes to differ across
    scenarios - only an `INVARIANT_ATTRS` set may not. So "static" there means
    "not per snapshot", not "the same everywhere".

    Detected per attribute rather than declared, because whether one differs is
    a property of the data: a network may hold a per-scenario `capital_cost`
    and a shared `carrier`, and only the first varies over `scenario`.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    index = c.static.index
    if not isinstance(index, pd.MultiIndex) or SCENARIO not in (index.names or []):
        return frozenset()
    by_entity = c.static.groupby(level="name")
    return frozenset(
        x for x in columns if (by_entity[x].nunique(dropna=False) > 1).any()
    )


def _connection_rows(c: pypsa.Components) -> pd.DataFrame:
    """One type's connections as `(name, bus, role)` rows, from its port columns.

    Undoes PyPSA's positional encoding: `c.ports` is the authoritative port
    list (`["0", "1"]` plus `additional_ports`), so the port count comes from
    the network rather than a guessed bound.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    static = c.static
    constant = {
        attr: (stem, port)
        for attr, (stem, port) in _port_columns(c).items()
        if stem != "bus"
        and attr in static.columns
        and not c.defaults.at[attr, "varying"]
        and not _is_output(c.defaults, attr)
    }
    frames = []
    for port in c.ports:
        column = _port_attribute("bus", port)
        if column not in static.columns:
            continue
        buses = static[column]
        attached = buses[buses.astype(str) != ""]
        if attached.empty:
            continue
        rows = attached.rename("bus").reset_index()
        rows = rows.rename(columns={"name": "entity"})
        rows["role"] = _port_role(port)
        rows["deleted"] = False
        for attr, (stem, at) in constant.items():
            if at == port:
                rows[stem] = static.loc[attached.index, attr].to_numpy()
        frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["entity", "bus", "role", "deleted"])
    return pd.concat(frames, ignore_index=True)


# TODO(pypsa): every function below, up to `_as_long`, ports a function that
# only exists on PyPSA's unreleased data-records branch (https://energy-models.github.io/datarecord/design/tools/). Delete the
# ported copy and call the real one once a PyPSA release carries it.


def _is_output(defaults: pd.DataFrame, attr: str) -> bool:
    """Whether `attr` is an output (custom attrs with no status are inputs).

    Ported from `pypsa.common._is_output` rather than depended on, since only
    a released PyPSA can be relied on here (no `_as_long` yet).

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    if attr not in defaults.index:
        return False
    return str(defaults.at[attr, "status"]).startswith("Output")


def _drop_default_rows(rows: pd.DataFrame, default: Any) -> pd.DataFrame:
    """Remove scalar rows that just repeat the attribute's default value.

    Ported from `pypsa.components.array._drop_default_rows`.
    """
    if pd.isnull(default):
        return rows[rows["value"].notna()]
    return rows[rows["value"] != default]


def _concat_long(series_rows: pd.DataFrame, scalar_rows: pd.DataFrame) -> pd.DataFrame:
    """Concatenate series rows and null-snapshot scalar rows of one attribute.

    Ported from `pypsa.components.array._concat_long`. The scalar rows carry
    plain NaN in the dimension columns, which pandas would flag as an all-NA
    entry with a conflicting dtype - take the dtypes from the series rows
    instead.
    """
    if scalar_rows.empty:
        return series_rows
    if series_rows.empty:
        return scalar_rows
    scalar_rows = scalar_rows.copy()
    for column in ("snapshot", "period"):
        scalar_rows[column] = series_rows[column].iloc[:0].reindex(scalar_rows.index)
    return pd.concat([series_rows, scalar_rows[series_rows.columns]], ignore_index=True)


def _stack_dynamic(c: pypsa.Components, wide: pd.DataFrame) -> pd.DataFrame:
    """Melt a wide `snapshots x components` frame into long rows.

    Ported from `pypsa.components.array.Components._stack_dynamic`.
    """
    stochastic = isinstance(wide.columns, pd.MultiIndex)
    wide = wide.rename_axis(columns=["scenario", "entity"] if stochastic else "entity")

    stacked = wide.stack(level=list(range(wide.columns.nlevels)), future_stack=True)
    long = stacked.rename("value").reset_index()
    if isinstance(c.snapshots, pd.MultiIndex):
        long = long.rename(columns={"timestep": "snapshot"})
    if "scenario" not in long.columns:
        long["scenario"] = np.nan
    if "period" not in long.columns:
        long["period"] = np.nan
    return long


def _scalar_rows(c: pypsa.Components, attr: str, exclude: pd.Index) -> pd.DataFrame:
    """Build null-snapshot long rows from the static scalars of `attr`.

    Ported from `pypsa.components.array.Components._scalar_rows`.
    """
    static = c.static[attr]
    if len(exclude):
        static = static[~static.index.isin(exclude)]
    out = static.rename("value").reset_index().rename(columns={"name": "entity"})
    if "scenario" not in out.columns:
        out["scenario"] = np.nan
    out["snapshot"] = np.nan
    out["period"] = np.nan
    return out


def _as_long(
    c: pypsa.Components, attr: str, *, drop_defaults: bool = False
) -> pd.DataFrame:
    """`attr` as a long/tidy DataFrame; `drop_defaults` omits default-valued scalars.

    Ported from `pypsa.components.array.Components._as_long` (with its
    helpers above) rather than called on `c`: only an unreleased PyPSA branch
    carries this method, and a tool may not require one. The reshaping
    is otherwise built entirely from public surface - `c.dynamic`, `c.static`,
    `c.defaults`, `c.has_scenarios`, `c.snapshots` - so porting it costs
    nothing PyPSA doesn't already expose.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    varying = attr in c.dynamic or (
        attr in c.defaults.index and bool(c.defaults.at[attr, "varying"])
    )
    # A scenario-varying static attribute has a long representation too: its
    # rows carry the scenario they hold for, which `_scalar_rows` preserves.
    if (
        not varying
        and not _is_output(c.defaults, attr)
        and attr not in c.static.columns
    ):
        msg = f"'{attr}' of '{c.name}' components has no long representation."
        raise AttributeError(msg)

    columns = ["entity", "snapshot", "scenario", "period", "value"]
    if not varying:
        out = _scalar_rows(c, attr, pd.Index([]))[columns]
        if drop_defaults:
            out = _drop_default_rows(out, c.defaults.at[attr, "default"])
        return out
    wide = c.dynamic[attr]
    out = _stack_dynamic(c, wide)[columns]
    # scalar rows import into `c.static[attr]`, so only attrs with a static
    # column get them
    static_backed = attr in c.defaults.index and bool(c.defaults.at[attr, "static"])
    if static_backed:
        scalars = _scalar_rows(c, attr, wide.columns)[columns]
        if drop_defaults:
            scalars = _drop_default_rows(scalars, c.defaults.at[attr, "default"])
        out = _concat_long(out, scalars)
    return out


def _long_rows(
    c: pypsa.Components,
    attribute: str,
    dims: tuple[str, ...],
    *,
    drop_defaults: bool = False,
) -> pd.DataFrame:
    """One attribute's long rows for one component type, in the long schema.

    `_as_long` already emits the dim columns and `value`; this adds the
    columns the record's schema fixes and PyPSA has no notion of - `attribute`,
    and the NULL `breakpoint` that marks the value a scalar. No
    `entity_type`, and no `bus`: that is the `connection` group's
    coordinate, so it belongs to the rows of an attribute addressed by that
    group and `_per_port_long_rows` is what fills it.

    The widest shape any caller needs, from which `_long_frame` selects the
    attribute's own columns - `dims` is every axis a network has, not one
    attribute's.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    long = _as_long(c, attribute, drop_defaults=drop_defaults)
    long = long.assign(attribute=attribute, breakpoint=None)
    for dim in dims:
        if dim not in long.columns:
            long[dim] = None
    return long


def _per_port_long_rows(
    c: pypsa.Components,
    stem: str,
    dims: tuple[str, ...],
    *,
    drop_defaults: bool = False,
) -> pd.DataFrame:
    """A per-connection attribute's long rows, `bus` filled from the port.

    The inverse of the positional collapse: `efficiency`/`efficiency2` become
    one attribute whose rows carry the bus each port attaches to, so the
    overlay owns them per connection rather than per component.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    static = c.static
    frames = []
    ports = {port: attr for attr, (s, port) in _port_columns(c).items() if s == stem}
    for port, column in ports.items():
        bus_column = _port_attribute("bus", port)
        if bus_column not in static.columns:
            continue
        long = _long_rows(c, column, dims, drop_defaults=drop_defaults)
        if long.empty:
            continue
        # `static` is `(scenario, name)`-indexed on a stochastic network, so map
        # the bus by name alone - a component's bus does not vary by scenario, so
        # the name level deduplicates to one bus per component.
        buses = static[bus_column]
        if isinstance(buses.index, pd.MultiIndex):
            buses = buses.droplevel(SCENARIO)
            buses = buses[~buses.index.duplicated()]
        long["bus"] = long["entity"].map(buses)
        long["attribute"] = stem
        frames.append(long[long["bus"].astype(str) != ""])
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _record_rows(
    c: pypsa.Components,
    name: str,
    dims: tuple[str, ...],
    *,
    drop_defaults: bool = False,
) -> pd.DataFrame:
    """The long rows of record attribute `name` for one type, per port where it is per-port."""
    if name in {stem for stem, _ in _port_columns(c).values()}:
        return _per_port_long_rows(c, name, dims, drop_defaults=drop_defaults)
    (attr,) = (a for a, n in _record_names(c).items() if n == name)
    return _long_rows(c, attr, dims, drop_defaults=drop_defaults).assign(attribute=name)


# -- the tool ---------------------------------------------------------------


class PyPSATool(Tool):
    """Build a `pypsa.Network` from a record and read its results back."""

    name = "pypsa"

    # PyPSA defines the record vocabulary today, so every attribute maps to
    # itself and there is nothing to declare. The seam is here for the first
    # tool whose names differ, or whose value is computed from several of the
    # record's (`Attr.compute`).
    schema = Schema()

    def component_types(self) -> frozenset[str]:
        """The component types PyPSA knows, from its type registry.

        Taken from the component *type registry*, not a network instance:
        `pypsa.Network().components` iterates only the components attached to
        that network, which for an empty one is just the standard types.
        """
        from pypsa.components.types import all_components

        return frozenset(c.name for c in all_components.values())

    def requires(self, record: RecordLike) -> Requirements:
        """PyPSA's axes, the record's own component types, and their required attributes.

        The attribute half is record-dependent: only types the record
        actually defines members for are required, and which of their
        attributes are mandatory comes from PyPSA's registry
        (`defaults["status"] == "Input (required)"`), not a list kept here.
        Reported by source attribute (`Schema.sources`), so a renamed or
        computed one names what the record must actually supply.
        """
        known = self.component_types()
        ctypes = {ct for ct in _entity_groups(record) if ct in known}
        return Requirements(
            dims=REQUIRED_DIMS,
            entity_types=frozenset(ctypes),
            attributes=frozenset(
                (ct, src)
                for ct in ctypes
                for attr in _required_attributes(ct)
                for src in self.schema.sources(ct, attr)
            ),
        )

    def verify(self, record: RecordLike) -> Requirements:
        """What this record fails to supply for a PyPSA build; falsy if it is usable.

        Checks what a build needs: the schema declares PyPSA's axes, its key dims are ones this tool can honour, every component
        group it names is one PyPSA knows, and each type's required attributes
        are resolvable - either owned by some layer for one of its members,
        supplied by a column of `dims/entity.parquet` the type has, or
        carrying a declared default. A renamed or computed attribute is checked
        against its sources (`Schema.sources`), which is also how it is
        reported.

        The key checks come first and return early: the owner-map fold keys
        its relations by those dims, so reading the map at all could raise a
        binder error before anything below could report it.

        Notes
        -----
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        dims = REQUIRED_DIMS - set(record.schema.dims)
        unsupported_keys = self._unsupported_keys(record)
        if unsupported_keys:
            return Requirements(
                dims=frozenset(dims), unsupported_keys=frozenset(unsupported_keys)
            )

        entity_types: set[str] = set()
        attributes: set[tuple[str, str]] = set()
        unsupported_values: set[tuple[str, str]] = set()

        known = self.component_types()
        declared = record.schema
        group_flags = _group_flags(record)
        for ctype in _entity_groups(record):
            # A type PyPSA has no registry entry for. Reported rather than
            # raised: the record layer knows no framework's vocabulary, so this
            # reads back fine and it is this tool's business that it cannot be
            # built.
            if ctype not in known:
                entity_types.add(ctype)
                continue
            members = _members(record, ctype)
            resolved = group_flags.get(ctype, {})
            owned = set(resolved)
            # A curve, not a scalar (https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows). PyPSA takes a scalar for every
            # attribute this build assigns, so the record is storing something
            # correct that this translation cannot express - reported here
            # rather than silently pivoting one arbitrary breakpoint.
            unsupported_values |= {
                (ctype, attr) for attr, flags in resolved.items() if flags.breakpoints
            }
            static_cols = frozenset(members.columns)
            # A port attribute the record supplies as connection rows rather
            # than as a column: `bus0`/`bus1` are satisfied by a connection
            # per port, so the collapse in `build` can name them (https://energy-models.github.io/datarecord/design/record/#connections).
            from_connections = _connection_attributes(record, members)
            specs = declared.attributes
            for attr in _required_attributes(ctype):
                for src in self.schema.sources(ctype, attr):
                    if src in owned or src in static_cols or src in from_connections:
                        continue
                    # A declared default makes the attribute resolvable even
                    # with no row anywhere (the broadcast rule).
                    spec = specs.get(src)
                    if spec is not None and spec.default is not None:
                        continue
                    attributes.add((ctype, src))
        return Requirements(
            dims=frozenset(dims),
            entity_types=frozenset(entity_types),
            attributes=frozenset(attributes),
            unsupported_values=frozenset(unsupported_values),
        )

    def _unsupported_keys(self, record: RecordLike) -> set[tuple[str, str]]:
        """`(key, dim)` pairs this tool cannot honour: `snapshot`, as any key.

        PyPSA's static/series split needs a component's whole series to come
        from one layer, so a snapshot-keyed overlay could leave a broadcast row
        and a descendant's per-snapshot row with no single container to land in. A limit of the *representation*, not the format - which is why
        the record layer permits the declaration and this reports it.

        Notes
        -----
        - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        defs = record.schema
        return {("input_key", SNAPSHOT)} if SNAPSHOT in defs.partial_dims else set()

    def build(self, record: RecordLike) -> pypsa.Network:
        """The resolved network, one component type at a time.

        Raises
        ------
        UnsupportedRecordError
            If `verify` reports anything missing - a partial build would
            surface as a confusing PyPSA error several frames later.

        Notes
        -----
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        missing = self.verify(record)
        if missing:
            raise UnsupportedRecordError(self.name, missing)

        con = _connection(record)
        schema = record.schema
        shape = NetworkShape(record.dims)
        n = _new_network(schema, shape)
        _add_buses(n, record, shape, con)
        _add_carriers(n, record, shape, con)

        group_flags = _group_flags(record)
        for ctype in _entity_groups(record):
            members = _members(record, ctype)
            static = members
            if not shape.stochastic and SCENARIO in static.columns:
                static = static.project(star(exclude=[SCENARIO]))
            # `_pos` in member order before any join scrambles it: the member
            # frame arrives in member order (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis),
            # and `_assign_static` sorts the wide frame back by it at the end.
            static = static.project("*, row_number() OVER () AS _pos")
            # Connections back to the positional columns PyPSA expects (https://energy-models.github.io/datarecord/design/tools/).
            static = _collapse_connections(static, record, members, con)
            static = _classify(static, record, ctype, con)
            static = _restore_extendable(static, ctype)

            # Frames are built and released per type, so peak memory is one
            # type's wide frames rather than the whole network (https://energy-models.github.io/datarecord/design/tools/).
            # Through the schema, so a renamed or computed attribute reaches the
            # pivot below as an ordinary long relation. Scoped by a semi-join
            # against this type's members (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
            attributes = {
                attr: (
                    _scoped(self.schema.resolve(record, ctype, attr), members),
                    flags,
                )
                for attr, flags in group_flags.get(ctype, {}).items()
                if attr != ACTIVE_IN_PERIOD
            }
            _add_component_type(n, ctype, static, attributes, shape, con)

        _broadcast_standard_types(n)
        return n

    def results(self, model: pypsa.Network) -> Frames:
        """A solved network's result attributes in the record's long form.

        Keyed by attribute, matching `outputs/<attr>.parquet`: every component
        type's rows for one attribute are concatenated into one frame, exactly as
        `attributes` presents `inputs/`. A bus result is keyed by `bus`, under
        its record name (`bus_p`). Which attributes count as results comes from
        PyPSA's registry, so an upgrade that adds one is picked up.

        Rows still at the attribute's default are dropped, and an attribute with
        no rows left is absent: a static output like `Bus.control` carries its
        default whether or not the network was solved, and an absent output
        means exactly "take the default".

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        source = _NetworkSource(model)
        frames = {
            attr: source._output_rows(attr, ctypes)
            for attr, ctypes in source._output_types.items()
        }
        return {
            attr: nw.from_native(rows).lazy()
            for attr, rows in frames.items()
            if rows is not None
        }

    def to_datarecord(self, model: pypsa.Network) -> RecordLike:
        """Present a `Network` as the `Record` `write_record` persists.

        The inverse of `build`, and the only place PyPSA's shape is undone:
        `c.static`/`c.dynamic` become long rows, `bus0`/`bus1`/`efficiency2`
        become connection rows carrying a `role`, buses and carriers become
        axes, and `carrier`/`technology_group` become groups. Key sets are read
        off the network, so listing unpivots nothing and a lookup only what is
        asked for.

        Raises
        ------
        UnsupportedRecordError
            If two component types share a name: PyPSA scopes names per type, a
            record record-wide, and this reports rather than renames. Likewise
            if a custom column of an axis (bus, carrier, technology group) has
            the record name of a custom entity column or another axis's.
        ValueError
            If a `Shape` is neither a bus region nor a dropped country or
            offshore shape, or a technology group does not have exactly one
            carrier and at most one resource class.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        clashing = _colliding_names(model)
        if clashing:
            raise UnsupportedRecordError(self.name, Requirements(names=clashing))
        source = _NetworkSource(model)
        _ = source._regions, source._technology_groups
        if source._clashing_columns:
            raise UnsupportedRecordError(
                self.name, Requirements(columns=source._clashing_columns)
            )
        return source

    def check_spec(self, spec: Spec, record: RecordLike) -> None:
        """Check every name `spec` binds to is one `record` carries.

        Dimensions must be dims of the record, relations its groups, and
        parameters its attributes. A dim `within` a parent also offers the
        relation `<dim>_<parent>` (`snapshot_period`), whose rows are the
        nested axis file's key.

        A name the record cannot carry because the network has nothing it would
        describe is not missing: an attribute only component types the record
        holds no member of register (`e_nom` with no `Store`), and the
        technology-group axis and its groups where no component has a
        `technology_group`. The spec's rows over them are empty, as they are in
        PyPSA's model. Every other name is required.

        Raises
        ------
        ValueError
            Naming every dimension, relation and parameter the record lacks.

        Notes
        -----
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        schema = record.schema
        nested = {
            _classification(dim, parent)
            for dim, spec_ in schema.dimensions.items()
            for parent in spec_.within
        }
        unheld = _unheld_names(record)
        missing = {
            "dimensions": sorted(
                set(spec.dimensions) - set(schema.dimensions) - unheld
            ),
            "relations": sorted(
                set(spec.relations) - set(schema.groups) - nested - unheld
            ),
            "parameters": sorted(
                (set(spec.parameters) | set(spec.given.parameters))
                - set(schema.attributes)
                - unheld
            ),
        }
        if any(missing.values()):
            detail = "; ".join(f"{k} {v}" for k, v in missing.items() if v)
            msg = f"the record carries no {detail} the spec binds to"
            raise ValueError(msg)


def _unheld_names(record: RecordLike) -> frozenset[str]:
    """Names `record` cannot carry, the network holding nothing for them.

    The record-name stems of attributes that only component types no
    `entity_group` of the record names register, and the technology-group
    axis with its groups where the record declares no `technology_group` dim.

    Notes
    -----
    - [the mathspec spec](https://energy-models.github.io/datarecord/design/tools/#the-mathspec-spec)
    """
    registry = {
        ctype: set(_record_names(_blank_network().c[ctype]).values()) - {"name"}
        for ctype in _registry()
        if ctype not in _AXIS_TYPES and ctype not in _NOT_EXPORTED
    }
    held = set(_entity_groups(record))
    carried = set().union(
        *(attrs for ctype, attrs in registry.items() if ctype in held)
    )
    unheld = set().union(
        *(attrs for ctype, attrs in registry.items() if ctype not in held)
    )
    names = unheld - carried
    if TECHNOLOGY_GROUP not in record.schema.dimensions:
        names |= {
            TECHNOLOGY_GROUP,
            TECHNOLOGY_GROUP_GROUP,
            _classification(TECHNOLOGY_GROUP, CARRIER),
        }
    return frozenset(names)


def _registry() -> dict[str, frozenset[str]]:
    """Every PyPSA component type's attribute names, from its registry."""
    from pypsa.components.types import all_components

    return {
        c.name: frozenset(c.defaults.index) - {"name"} for c in all_components.values()
    }


def _bus_regions(n: pypsa.Network) -> pd.DataFrame:
    """Bus regions as WKT columns `region_onshore`/`region_offshore`, by bus.

    Raises
    ------
    ValueError
        If a `Shape` is neither a bus region nor a country or offshore shape
        with no component, or a bus has two regions of one type.

    Notes
    -----
    - [shapes](https://energy-models.github.io/datarecord/design/tools/#shapes)
    """
    shapes = n.c["Shape"].static
    component = shapes["component"].fillna("").astype(str)
    kind = shapes["type"].fillna("").astype(str)
    region = (component == "Bus") & kind.isin(list(_REGIONS))
    dropped = (component == "") & kind.isin(list(_DROPPED_SHAPES))
    other = shapes.index[~region & ~dropped]
    if len(other):
        msg = (
            f"shapes {sorted(map(str, other))} are neither a bus's onshore or "
            f"offshore region nor a country or offshore shape; the record has "
            f"no place for them"
        )
        raise ValueError(msg)
    rows = pd.DataFrame(
        {
            BUS: shapes.loc[region, "idx"].astype(str).to_numpy(),
            "column": kind[region].map(_REGIONS).to_numpy(),
            "geometry": _as_wkt(shapes.loc[region, ["geometry"]])[
                "geometry"
            ].to_numpy(),
        }
    )
    twice = rows[rows.duplicated([BUS, "column"])]
    if not twice.empty:
        msg = f"buses {sorted(twice[BUS])} have more than one region of one type"
        raise ValueError(msg)
    unknown = sorted(set(rows[BUS]) - set(_names(n.c["Bus"].static.index).astype(str)))
    if unknown:
        msg = f"shapes name buses {unknown} the network does not have"
        raise ValueError(msg)
    return rows.pivot(index=BUS, columns="column", values="geometry")


@dataclass(frozen=True)
class _TechnologyGroups:
    """`entity -> technology_group` and the per-group columns derived from it.

    Attributes
    ----------
    members
        `(entity, technology_group)`, one row per grouped entity.
    axis
        One row per group: `technology_group` and the `_TECHNOLOGY_GROUP_COLUMNS`
        the members carry.
    carriers
        `(technology_group, carrier)`, one row per group.
    moved
        Per component type, the columns moved off it onto the group's axis.
    """

    members: pd.DataFrame
    axis: pd.DataFrame
    carriers: pd.DataFrame
    moved: dict[str, tuple[str, ...]]


def _technology_groups(n: pypsa.Network) -> _TechnologyGroups | None:
    """The network's technology groups, or `None` where no type has the column.

    Raises
    ------
    ValueError
        If a group's members do not share exactly one carrier and at most one
        value of each `_TECHNOLOGY_GROUP_COLUMNS` column, or an entity outside
        every group carries one of those columns.

    Notes
    -----
    - [technology groups](https://energy-models.github.io/datarecord/design/tools/#technology-groups)
    """
    frames = []
    moved: dict[str, tuple[str, ...]] = {}
    for c in n.components:
        if not _exported(c) or TECHNOLOGY_GROUP not in c.static.columns:
            continue
        columns = tuple(x for x in _TECHNOLOGY_GROUP_COLUMNS if x in c.static.columns)
        moved[c.name] = columns
        frame = c.static[[TECHNOLOGY_GROUP, CARRIER, *columns]].reset_index()
        frames.append(
            frame.rename(columns={"name": ENTITY}).drop(
                columns=SCENARIO, errors="ignore"
            )
        )
    if not frames:
        return None
    rows = pd.concat(frames, ignore_index=True).drop_duplicates()
    grouped = rows[TECHNOLOGY_GROUP].notna() & (
        rows[TECHNOLOGY_GROUP].astype(str) != ""
    )
    present = [x for x in _TECHNOLOGY_GROUP_COLUMNS if x in rows.columns]
    stray = rows.loc[~grouped & rows[present].notna().any(axis=1), ENTITY]
    if not stray.empty:
        msg = (
            f"entities {sorted(stray.astype(str))} carry {present} but no "
            f"technology group; those columns are the group's"
        )
        raise ValueError(msg)
    rows = rows[grouped]
    carriers = rows[rows[CARRIER].notna() & (rows[CARRIER].astype(str) != "")]
    count = carriers.groupby(TECHNOLOGY_GROUP)[CARRIER].nunique()
    count = count.reindex(rows[TECHNOLOGY_GROUP].unique(), fill_value=0)
    if (count != 1).any():
        msg = (
            f"technology groups {sorted(count.index[count != 1].astype(str))} do "
            f"not have exactly one carrier among their members"
        )
        raise ValueError(msg)
    for column in present:
        many = rows.groupby(TECHNOLOGY_GROUP)[column].nunique() > 1
        if many.any():
            msg = (
                f"technology groups {sorted(many.index[many].astype(str))} have "
                f"more than one {column!r} among their members"
            )
            raise ValueError(msg)
    axis = rows.groupby(TECHNOLOGY_GROUP, sort=False)[present].first().reset_index()
    if "resource_class" in axis.columns:
        axis["resource_class"] = pd.to_numeric(axis["resource_class"]).astype("Int64")
    return _TechnologyGroups(
        members=_functional(rows, ENTITY, TECHNOLOGY_GROUP),
        axis=axis,
        carriers=_functional(rows, TECHNOLOGY_GROUP, CARRIER),
        moved=moved,
    )


@dataclass(frozen=True)
class _NetworkSource:
    """A `pypsa.Network` presented as a `Record`.

    Every `LazyFrames` here is built from key sets read off the network and
    its registry, so nothing is unpivoted until a key is looked up.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """

    n: pypsa.Network

    # The dims a PyPSA network has axes for; `Coords` stays generic, this tool
    # decides these three are what a network is shaped by (https://energy-models.github.io/datarecord/design/tools/).
    _DIMS = (SNAPSHOT, PERIOD, SCENARIO)

    @cached_property
    def _regions(self) -> pd.DataFrame:
        return _bus_regions(self.n)

    @cached_property
    def _technology_groups(self) -> _TechnologyGroups | None:
        return _technology_groups(self.n)

    @cached_property
    def _entity_ctypes(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.n.components if _exported(c))

    @property
    def _varying_dims(self) -> tuple[str, ...]:
        """What PyPSA's `varying` flag means as coordinates.

        The time axis, its period where the network has investment periods (the
        axis being nested in them), and the scenario axis where the network has
        one: a stochastic network's series differ per scenario.
        """
        return (
            SNAPSHOT,
            *((PERIOD,) if len(self.n.investment_periods) else ()),
            *((SCENARIO,) if self.n.has_scenarios else ()),
        )

    def _moved(self, ctype: str) -> tuple[str, ...]:
        """Columns of `ctype` the record stores as groups or on another axis."""
        tg = self._technology_groups
        moved = tg.moved.get(ctype, ()) if tg is not None else ()
        extendable = _extendable_attributes(self.n.c[ctype])
        return (CARRIER, TECHNOLOGY_GROUP, *moved, *extendable)

    def _entity_columns(self, c: pypsa.Components) -> list[str]:
        """`c`'s static columns that stay per entity (`_moved` and ports aside)."""
        per_port = set(_port_columns(c))
        moved = set(self._moved(c.name))
        return [x for x in c.static.columns if x not in per_port and x not in moved]

    @cached_property
    def _axis_columns(self) -> dict[str, list[str]]:
        """Each axis's PyPSA columns: registry, custom and derived.

        Notes
        -----
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        bus = self.n.c["Bus"]
        tg = self._technology_groups
        return {
            BUS: [
                *(
                    x
                    for x in (*bus.defaults.index, *bus.static.columns)
                    if x not in ("name", CARRIER)
                ),
                *self._regions.columns,
            ],
            CARRIER: list(self.n.c["Carrier"].static.columns),
            TECHNOLOGY_GROUP: [] if tg is None else list(tg.axis.columns[1:]),
        }

    @cached_property
    def _names(self) -> dict[str, dict[str, str]]:
        """Per axis, PyPSA column -> the record's attribute name.

        The attribute namespace is flat, so a column whose name another PyPSA
        type's registry or a dim or group already claims is prefixed `<axis>_`
        (`bus_x`, `bus_p`). Decided by the registry and the structural names
        alone, so one column has one record name in every network and a fixed
        spec binds across records; a clash between custom columns is
        `_clashing_columns`'s to report, not this rule's to rename.

        Notes
        -----
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        registry = _registry()
        own = {dim: ctype for ctype, dim in _AXIS_TYPES.items()}
        structural = {
            *self._DIMS,
            ENTITY,
            BUS,
            CARRIER,
            TECHNOLOGY_GROUP,
            ENTITY_GROUP,
            CONNECTION,
            TECHNOLOGY_GROUP_GROUP,
            *_CARRIER_GROUPS.values(),
            ROLE,
            SCENARIO_WEIGHT,
            PERIOD_YEAR,
            EXTENDABLE,
            ACTIVE_IN_PERIOD,
        }
        names = {}
        for dim, mine in self._axis_columns.items():
            taken = set().union(
                *(attrs for ctype, attrs in registry.items() if ctype != own.get(dim)),
                structural,
            )
            names[dim] = {
                x: f"{dim}_{x}" if x in taken else x for x in dict.fromkeys(mine)
            }
        return names

    @cached_property
    def _clashing_columns(self) -> frozenset[tuple[str, str, str]]:
        """`(column, owner, other)` for a record name two owners would both write.

        Only a custom column can clash once `_names` has prefixed: an axis
        column against a custom entity column, or two axes' columns.

        Notes
        -----
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        registry = set().union(*_registry().values())
        owners: dict[str, set[str]] = {}
        for ctype in self._entity_ctypes:
            for column in self._entity_columns(self.n.c[ctype]):
                if column not in registry:
                    owners.setdefault(column, set()).add(ctype)
        for dim, names in self._names.items():
            for name in names.values():
                owners.setdefault(name, set()).add(dim)
        return frozenset(
            (column, a, b)
            for column, claimed in owners.items()
            for a in claimed
            for b in claimed
            if a < b and (a in self._names or b in self._names)
        )

    @cached_property
    def schema(self) -> RecordSchema:
        """The layer's schema, derived from PyPSA's own registry.

        `c.defaults` already declares what an `AttributeSpec` asks for, so this reads it
        rather than restating it. Every stored attribute, not only the varying
        ones - `dims={"entity"}` is what puts one in `dims/entity.parquet`.
        A custom column the registry lacks is declared from its pandas dtype.
        An entity's component type is `entity_group`, one more such attribute:
        no dim, group or trait, so it reaches a spec as a value to compare and
        never as an axis.

        Results go to `results` rather than `attributes`, read off the same
        registry (`status` starting "Output"), so a PyPSA upgrade adding one is
        still picked up rather than needing a list kept here.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [the schema](https://energy-models.github.io/datarecord/design/schema/)
        - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
        """
        weight = AttributeSpec(dtype=nw.Float64(), default=1.0)
        attributes: dict[str, AttributeSpec] = {
            # Declared over the `connection` group, so it is a column of that
            # group's file rather than a long row (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).
            ROLE: AttributeSpec(
                dtype=nw.String(),
                dims=frozenset({CONNECTION}),
                description="Which end of the component this attachment is.",
            ),
            ENTITY_GROUP: AttributeSpec(
                dtype=nw.String(),
                dims=frozenset({ENTITY}),
                description="PyPSA's component type of an entity.",
            ),
            SCENARIO_WEIGHT: weight.model_copy(
                update={
                    "dims": frozenset({SCENARIO}),
                    "description": "How much this scenario counts in the expectation.",
                }
            ),
            PERIOD_YEAR: AttributeSpec(
                dtype=nw.Int64(),
                dims=frozenset({PERIOD}),
                unit="year",
                description="The year an investment period stands for.",
            ),
            EXTENDABLE: AttributeSpec(
                dtype=nw.Boolean(),
                dims=frozenset({ENTITY}),
                default=False,
                description="Whether the optimisation sizes the entity: `p_nom_extendable`, `s_nom_extendable` or `e_nom_extendable`.",
            ),
            ACTIVE_IN_PERIOD: AttributeSpec(
                dtype=nw.Boolean(),
                dims=frozenset({ENTITY, PERIOD}),
                default=False,
                description="Whether the entity stands in a period: `active` and `build_year <= period < build_year + lifetime`.",
            ),
            **{
                f"snapshot_weighting_{w}": weight.model_copy(
                    update={"dims": frozenset({SNAPSHOT})}
                )
                for w in _SNAPSHOT_WEIGHTINGS
            },
            **{
                f"period_weighting_{w}": weight.model_copy(
                    update={"dims": frozenset({PERIOD})}
                )
                for w in _PERIOD_WEIGHTINGS
            },
        }
        results: dict[str, AttributeSpec] = {}
        for ctype in self._entity_ctypes:
            c = self.n.c[ctype]
            # Per-port columns collapse to their stem, which a record keys by
            # bus rather than by position (https://energy-models.github.io/datarecord/design/record/#connections); `efficiency2` and `efficiency`
            # agree on every declaration, so either row answers.
            per_port = _port_columns(c)
            names = _record_names(c)
            outputs = set(_output_attributes(c))
            moved = set(self._moved(ctype))
            for attr in c.defaults.index:
                if attr == "name" or attr in moved:
                    continue
                stem = names[attr]
                # A per-port attribute names the `connection` group, which
                # expands to `(entity, bus)` (https://energy-models.github.io/datarecord/design/schema/#groups).
                spec = self._registry_spec(
                    c.defaults.loc[attr], CONNECTION if attr in per_port else ENTITY
                )
                # One attribute, one spec, record-wide: the first type to
                # declare one wins, PyPSA's registry agreeing everywhere today.
                if attr in outputs:
                    results.setdefault(stem, spec)
                    continue
                attributes.setdefault(stem, spec)
            for column in self._entity_columns(c):
                if column in c.defaults.index:
                    continue
                attributes.setdefault(
                    column,
                    AttributeSpec(
                        dtype=_column_dtype(c.static[column]),
                        dims=frozenset({ENTITY}),
                    ),
                )
        self._axis_specs(attributes, results)
        # A name PyPSA registers as an output on one type and an input on
        # another would be both here; the input declaration wins.
        results = {a: s for a, s in results.items() if a not in attributes}
        tg = self._technology_groups
        multiperiod = len(self.n.investment_periods) > 0
        dimensions = {
            SNAPSHOT: Dimension(
                dtype=nw.Datetime(),
                within=frozenset({PERIOD}) if multiperiod else frozenset(),
                description="A point in the operational time series.",
            ),
            PERIOD: Dimension(
                dtype=nw.Int64(),
                unit="year",
                description="An investment period, labelled by its year.",
            ),
            SCENARIO: Dimension(
                dtype=nw.String(),
                description="One realisation of a stochastic problem.",
            ),
            ENTITY: Dimension(dtype=nw.String(), description="A component."),
            BUS: Dimension(dtype=nw.String(), description="A node of the network."),
            CARRIER: Dimension(dtype=nw.String(), description="An energy carrier."),
        }
        groups = {
            CONNECTION: Group(
                over={ENTITY: ENTITY, BUS: BUS},
                description="A component's attachment to one bus.",
            ),
            _CARRIER_GROUPS[ENTITY]: Group(
                over={ENTITY: ENTITY},
                into=CARRIER,
                description="The carrier of each component.",
            ),
            _CARRIER_GROUPS[BUS]: Group(
                over={BUS: BUS}, into=CARRIER, description="The carrier of each bus."
            ),
        }
        if tg is not None:
            dimensions[TECHNOLOGY_GROUP] = Dimension(
                dtype=nw.String(),
                description="A technology: components of one carrier and resource class.",
            )
            groups[TECHNOLOGY_GROUP_GROUP] = Group(
                over={ENTITY: ENTITY},
                into=TECHNOLOGY_GROUP,
                description="The technology group of each component.",
            )
            groups[_CARRIER_GROUPS[TECHNOLOGY_GROUP]] = Group(
                over={TECHNOLOGY_GROUP: TECHNOLOGY_GROUP},
                into=CARRIER,
                description="The one carrier of each technology group.",
            )
        return RecordSchema(
            dimensions=dimensions,
            groups=groups,
            attributes=attributes,
            results=results,
            # `partial` names value dims a layer patches per value; membership
            # keys are in the fold key by being membership (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
            partial=frozenset({SCENARIO}),
            meta={
                "format": "pypsa-parquet",
                "attributes": _collect_network_attributes(self.n),
                "meta": dict(self.n.meta),
                "crs": self.n.crs.to_string()
                if self.n.crs is not None
                else DEFAULT_CRS,
                "names": {
                    dim: {record: pypsa for pypsa, record in names.items()}
                    for dim, names in self._names.items()
                },
            },
        )

    def _registry_spec(self, row: pd.Series, address: str) -> AttributeSpec:
        """One registry row's spec, addressed by `address` and, if varying, more."""
        dims = {address, *self._varying_dims} if row["varying"] else {address}
        return AttributeSpec(
            dtype=_DTYPES.get(row["typ"], nw.String()),
            dims=frozenset(dims),
            default=_default(row["default"]),
            unit=_text(row.get("unit")),
            description=_text(row.get("description")),
        )

    def _axis_specs(
        self, attributes: dict[str, AttributeSpec], results: dict[str, AttributeSpec]
    ) -> None:
        """Declare the bus, carrier and technology group axes' attributes.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        frames = {
            BUS: self._bus_frame(),
            CARRIER: self._carrier_frame(),
            TECHNOLOGY_GROUP: self._technology_group_frame(),
        }
        defaults = {
            BUS: self.n.c["Bus"].defaults,
            CARRIER: self.n.c["Carrier"].defaults,
        }
        for dim, names in self._names.items():
            registry = defaults.get(dim)
            for column, name in names.items():
                if registry is not None and column in registry.index:
                    row = registry.loc[column]
                    target = (
                        results
                        if str(row["status"]).startswith("Output")
                        else attributes
                    )
                    target[name] = self._registry_spec(row, dim)
                    continue
                dtype = (
                    _TECHNOLOGY_GROUP_COLUMNS[column]
                    if dim == TECHNOLOGY_GROUP
                    else _column_dtype(frames[dim][name])
                )
                attributes[name] = AttributeSpec(dtype=dtype, dims=frozenset({dim}))

    @property
    def dims(self) -> LazyFrames:
        """Every axis this network holds: PyPSA's, `entity`, and the axis types.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        axes = {
            SNAPSHOT: self._snapshot_frame,
            PERIOD: self._period_frame,
            # With its weights: an axis frame carries the whole row, not just
            # the key column (https://energy-models.github.io/datarecord/design/record/#axis-order).
            SCENARIO: lambda: self.n.scenario_weightings.reset_index().rename(
                columns={"weight": SCENARIO_WEIGHT}
            ),
            ENTITY: self._entity_axis_frame,
            BUS: self._bus_frame,
            CARRIER: self._carrier_frame,
            TECHNOLOGY_GROUP: self._technology_group_frame,
        }
        held = {
            SNAPSHOT: len(self.n.snapshots) > 0,
            PERIOD: len(self.n.investment_periods) > 0,
            SCENARIO: self.n.has_scenarios,
            ENTITY: True,
            BUS: not self.n.c["Bus"].static.empty,
            CARRIER: not self.n.c["Carrier"].static.empty,
            TECHNOLOGY_GROUP: self._technology_groups is not None,
        }
        present = tuple(d for d in axes if held[d])
        return LazyFrames(present, lambda dim: nw.from_native(axes[dim]()).lazy())

    def _snapshot_frame(self) -> pd.DataFrame:
        """`(period,) snapshot` and its weightings, `timestep` named `snapshot`."""
        frame = self.n.snapshot_weightings.reset_index()
        return frame.rename(
            columns={
                "timestep": SNAPSHOT,
                **{w: f"snapshot_weighting_{w}" for w in _SNAPSHOT_WEIGHTINGS},
            }
        )

    def _period_frame(self) -> pd.DataFrame:
        """`period`, its year as a value, and its weightings."""
        frame = self.n.investment_period_weightings.rename_axis(PERIOD).reset_index()
        frame = frame.rename(
            columns={w: f"period_weighting_{w}" for w in _PERIOD_WEIGHTINGS}
        )
        return frame.assign(**{PERIOD_YEAR: frame[PERIOD]})

    def _axis_frame(self, dim: str, frame: pd.DataFrame) -> pd.DataFrame:
        """An axis type's frame, keyed by `dim`, columns renamed and WKT."""
        frame = frame.rename(columns={"name": dim, **self._names[dim]})
        return self._tagged(_as_wkt(frame), dim)

    def _bus_frame(self) -> pd.DataFrame:
        """`dims/bus.parquet`: every non-varying input column, and the regions.

        Notes
        -----
        - [shapes](https://energy-models.github.io/datarecord/design/tools/#shapes)
        """
        c = self.n.c["Bus"]
        outputs = set(_output_attributes(c))
        columns = [
            x
            for x in c.static.columns
            if x != CARRIER
            and x not in outputs
            and not (x in c.defaults.index and c.defaults.loc[x, "varying"])
        ]
        static = c.static[columns]
        if isinstance(static.index, pd.MultiIndex):
            static = static.droplevel(SCENARIO)
            static = static[~static.index.duplicated()]
        frame = static.join(self._regions).rename_axis("name").reset_index()
        return self._axis_frame(BUS, frame)

    def _carrier_frame(self) -> pd.DataFrame:
        """`dims/carrier.parquet`: every Carrier column."""
        static = self.n.c["Carrier"].static
        if isinstance(static.index, pd.MultiIndex):
            static = static.droplevel(SCENARIO)
            static = static[~static.index.duplicated()]
        return self._axis_frame(CARRIER, static.rename_axis("name").reset_index())

    def _technology_group_frame(self) -> pd.DataFrame:
        """`dims/technology_group.parquet`, derived from the members.

        Notes
        -----
        - [technology groups](https://energy-models.github.io/datarecord/design/tools/#technology-groups)
        """
        tg = self._technology_groups
        if tg is None:
            return pd.DataFrame(columns=[TECHNOLOGY_GROUP])
        return self._axis_frame(TECHNOLOGY_GROUP, tg.axis)

    def _entity_axis_frame(self) -> pd.DataFrame:
        """`dims/entity.parquet`: every exported type's members, `entity_group` their type.

        One row per entity and one column per constant attribute of any type,
        so a column is NULL on the rows of a type that does not have it. Which
        columns a type has is PyPSA's registry and the custom columns it holds a
        value in (`_members`).

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        frames = [
            self._members_frame(ctype).assign(**{ENTITY_GROUP: ctype})
            for ctype in self._entity_ctypes
        ]
        if not frames:
            return pd.DataFrame(columns=[ENTITY, ENTITY_GROUP, "deleted"])
        return pd.concat(frames, ignore_index=True)

    @property
    def entity_types(self) -> LazyFrames:
        """None: a component's type is the `entity_group` column of the entity axis."""
        return EMPTY

    @property
    def groups(self) -> LazyFrames:
        """`connection` and the carrier and technology group classifications.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        frames = {
            CONNECTION: self._connection_frame,
            _CARRIER_GROUPS[ENTITY]: self._entity_carrier_frame,
            _CARRIER_GROUPS[BUS]: lambda: _functional(
                self.n.c["Bus"].static.reset_index().rename(columns={"name": BUS}),
                BUS,
                CARRIER,
            ),
        }
        tg = self._technology_groups
        if tg is not None:
            frames[TECHNOLOGY_GROUP_GROUP] = lambda: tg.members
            frames[_CARRIER_GROUPS[TECHNOLOGY_GROUP]] = lambda: tg.carriers
        return LazyFrames(
            tuple(frames),
            lambda name: nw.from_native(self._tagged(frames[name](), None)).lazy(),
        )

    def _entity_carrier_frame(self) -> pd.DataFrame:
        """`(entity, carrier)` over every exported type with a carrier."""
        frames = [
            self.n.c[ctype].static.reset_index().rename(columns={"name": ENTITY})
            for ctype in self._entity_ctypes
            if CARRIER in self.n.c[ctype].static.columns
        ]
        if not frames:
            return pd.DataFrame(columns=[ENTITY, CARRIER])
        return _functional(pd.concat(frames, ignore_index=True), ENTITY, CARRIER)

    def _connection_frame(self) -> pd.DataFrame:
        """Every type's connections in one frame, `(entity, bus, role)`.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        frames = [
            _connection_rows(self.n.c[ctype])
            for ctype in self._entity_ctypes
            if self.n.c[ctype].ports
        ]
        rows = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=[ENTITY, BUS, ROLE])
        )
        # A stochastic network repeats its connections per scenario, which
        # collapse to one row: an attachment exists or it does not.
        if SCENARIO in rows.columns:
            rows = rows.drop(columns=[SCENARIO]).drop_duplicates(subset=[ENTITY, BUS])
        return rows

    @cached_property
    def _input_types(self) -> dict[str, list[str]]:
        """Each `inputs/` attribute -> the component types holding it."""
        names: dict[str, list[str]] = {}
        for ctype in self._entity_ctypes:
            for attr in self._record_attributes(self.n.c[ctype]):
                names.setdefault(attr, []).append(ctype)
        if len(self.n.investment_periods):
            names[ACTIVE_IN_PERIOD] = list(self._entity_ctypes)
        bus = self.n.c["Bus"]
        outputs = set(_output_attributes(bus))
        for attr in bus.defaults.index[bus.defaults["varying"].astype(bool)]:
            if attr not in outputs:
                names.setdefault(self._names[BUS][attr], []).append("Bus")
        return names

    @cached_property
    def _output_types(self) -> dict[str, list[str]]:
        """Each `outputs/` attribute -> the component types holding it."""
        names: dict[str, list[str]] = {}
        for ctype in self._entity_ctypes:
            c = self.n.c[ctype]
            for attr in _output_attributes(c):
                names.setdefault(_record_names(c)[attr], []).append(ctype)
        for attr in _output_attributes(self.n.c["Bus"]):
            names.setdefault(self._names[BUS][attr], []).append("Bus")
        return names

    @property
    def attributes(self) -> LazyFrames:
        names = self._input_types
        return LazyFrames(
            tuple(names), lambda attr: self._long_frame(attr, names[attr])
        )

    @property
    def outputs(self) -> LazyFrames:
        """Result attributes, keyed by name, from PyPSA's own registry.

        A network that was never solved still names them; the frame is then
        empty and `write_record` writes an empty file rather than none, which
        reads back as "take the default" either way.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        names = self._output_types
        return LazyFrames(
            tuple(names), lambda attr: self._output_frame(attr, names[attr])
        )

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Never consulted: `write_record` persists frames, not flags.

        Notes
        -----
        - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
        """
        return {}

    # -- key sets -----------------------------------------------------------

    def _record_attributes(self, c: pypsa.Components) -> list[str]:
        """`c`'s input attributes that go to `inputs/`, in the record's vocabulary.

        Those varying over `snapshot`, which PyPSA's registry flags - and those
        varying over `scenario`, which it does not. Per-port attributes collapse
        to their stem (`efficiency2` -> `efficiency`).

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        defaults = c.defaults
        outputs = set(_output_attributes(c))
        names = _record_names(c)
        diverging = _scenario_varying(c, list(c.static.columns))
        moved = set(self._moved(c.name))
        stems: list[str] = []
        for attr in defaults.index:
            if attr in outputs or attr == "name" or attr in moved:
                continue
            if not defaults.loc[attr, "varying"] and attr not in diverging:
                continue
            stem = names[attr]
            if stem not in stems:
                stems.append(stem)
        return stems

    # -- frames -------------------------------------------------------------

    def _members_frame(self, ctype: str) -> pd.DataFrame:
        """Wide members of one type: the non-varying, non-port static columns.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        """
        c = self.n.c[ctype]
        defaults = c.defaults

        def keep(column: str) -> bool:
            if column not in defaults.index:
                # A custom column the registry has no entry for, non-varying by
                # construction: a varying one needs a registry entry to have a
                # container.
                return True
            return (
                not defaults.loc[column, "varying"]
                and defaults.loc[column, "status"] != "Output"
            )

        columns = [x for x in self._entity_columns(c) if keep(x)]
        # Scenario-varying columns belong in `inputs/`; what is left is the
        # same in every scenario and collapses to one entity row.
        columns = [x for x in columns if x not in _scenario_varying(c, columns)]
        frame = c.static[columns].reset_index().rename(columns={"name": ENTITY})
        extendable = c.static[_extendable_attributes(c)].any(axis=1).to_numpy()
        frame[EXTENDABLE] = extendable
        return self._tagged(_as_wkt(frame), ENTITY)

    @staticmethod
    def _tagged(frame: pd.DataFrame, key: str | None) -> pd.DataFrame:
        """A `dims/` or `groups/` frame with the tombstone column the fold scopes by.

        No `scenario`: a member exists or it does not, so a stochastic
        network's per-scenario repeats collapse to one row per `key`.

        Notes
        -----
        - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
        - [the record format](https://energy-models.github.io/datarecord/design/format/)
        """
        if key is not None and SCENARIO in frame.columns:
            frame = frame.drop(columns=[SCENARIO]).drop_duplicates(subset=[key])
        if "deleted" not in frame.columns:
            frame = frame.assign(deleted=False)
        return frame

    def _long_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """One attribute's long rows, across every type that has it.

        A per-connection attribute contributes rows carrying the bus each port
        attaches to; a bus attribute is keyed by `bus` rather than `entity`.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
        """
        dims = self._DIMS
        bus_names = {record: pypsa for pypsa, record in self._names[BUS].items()}
        columns = list(self.schema.long_columns_for(attribute))
        if attribute == ACTIVE_IN_PERIOD:
            return nw.from_native(self._activity_rows()[columns]).lazy()
        frames = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            if ctype == "Bus":
                rows = _long_rows(c, bus_names[attribute], dims)
                rows = rows.rename(columns={ENTITY: BUS}).assign(attribute=attribute)
            else:
                rows = _record_rows(c, attribute, dims)
            if not rows.empty:
                frames.append(rows)
        if not frames:
            return nw.from_native(pd.DataFrame(columns=columns)).lazy()
        # No dtype fixing here: `write_record` casts every schema column to the
        # record layer's declared type on the way out (https://energy-models.github.io/datarecord/design/writing/).
        return nw.from_native(pd.concat(frames, ignore_index=True)[columns]).lazy()

    def _activity_rows(self) -> pd.DataFrame:
        """`active_in_period`'s rows: one `True` per entity and period it stands in.

        PyPSA's `get_active_assets`: `active` and
        `build_year <= period < build_year + lifetime`.
        """
        frames = []
        for ctype in self._entity_ctypes:
            c = self.n.c[ctype]
            active = pd.concat(
                {p: c.get_active_assets(p) for p in self.n.investment_periods},
                names=[PERIOD],
            )
            rows = active[active].rename("value").reset_index()
            frames.append(rows.rename(columns={"name": ENTITY}))
        rows = pd.concat(frames, ignore_index=True)
        rows = rows.drop(columns=SCENARIO, errors="ignore").drop_duplicates()
        return rows.assign(attribute=ACTIVE_IN_PERIOD, breakpoint=None)

    def _output_keys(self, attribute: str, ctypes: list[str]) -> list[str]:
        """The key columns of a result's rows: `bus`, or `entity` and `bus` where per-port."""
        if ctypes == ["Bus"]:
            return [BUS]
        per_port = any(
            attribute in {stem for stem, _ in _port_columns(self.n.c[ct]).values()}
            for ct in ctypes
        )
        return [ENTITY, BUS] if per_port else [ENTITY]

    def _output_rows(self, attribute: str, ctypes: list[str]) -> pd.DataFrame | None:
        """One result attribute's long rows off their defaults, or `None` if none.

        No port: a result comes from a per-entity container, PyPSA keeping
        `Link.p0` and `p1` as separate results. A bus result is keyed by `bus`.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        bus_names = {record: pypsa for pypsa, record in self._names[BUS].items()}
        keys = self._output_keys(attribute, ctypes)
        frames = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            try:
                if ctype == "Bus":
                    long = _as_long(c, bus_names[attribute], drop_defaults=True)
                    long = long.rename(columns={ENTITY: BUS})
                else:
                    long = _record_rows(c, attribute, (), drop_defaults=True)
            except (AttributeError, KeyError):
                # Not every registry output has a container on every network.
                continue
            if long.empty:
                continue
            frames.append(long.assign(attribute=attribute, breakpoint=None))
        if not frames:
            return None
        columns = [*keys, *self._DIMS, "attribute", "breakpoint", "value"]
        return pd.concat(frames, ignore_index=True)[columns]

    def _output_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """`_output_rows`, empty rather than absent.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        rows = self._output_rows(attribute, ctypes)
        if rows is None:
            rows = pd.DataFrame(
                columns=[
                    *self._output_keys(attribute, ctypes),
                    *self._DIMS,
                    "attribute",
                    "breakpoint",
                    "value",
                ]
            )
        return nw.from_native(rows).lazy()


def _required_attributes(ctype: str) -> frozenset[str]:
    """`ctype`'s mandatory input attributes, from PyPSA's registry.

    `name` is the component's identity, always supplied by the record's member
    list rather than an attribute row, so it never counts as missing.
    """
    from pypsa.components.types import get as get_component_type

    defaults = get_component_type(ctype).defaults
    required = defaults.index[defaults["status"].astype(str) == "Input (required)"]
    return frozenset(required) - {"name"}


def _entity_groups(record: RecordLike) -> list[str]:
    """The component types `record`'s entities are, by their `entity_group`, sorted.

    Raises
    ------
    ValueError
        If an entity has no `entity_group`: it is no component PyPSA can build.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    frame = record.dims.get(ENTITY)
    if frame is None:
        return []
    ungrouped = frame.filter(nw.col(ENTITY_GROUP).is_null()).select(ENTITY).collect()
    if len(ungrouped):
        msg = f"entities {sorted(ungrouped[ENTITY].to_list())} have no {ENTITY_GROUP!r}"
        raise ValueError(msg)
    return sorted(frame.select(ENTITY_GROUP).unique().collect()[ENTITY_GROUP].to_list())


def _members(record: RecordLike, ctype: str) -> DuckDBPyRelation:
    """`ctype`'s rows of the entity axis, in member order, with the columns it has.

    The axis holds every type's constant columns, NULL where a type has none.
    A type has the columns PyPSA's registry declares for it, and any other
    column one of its members holds a value in - a custom column.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    rows = to_relation(record.dims[ENTITY]).filter(col(ENTITY_GROUP) == lit(ctype))
    registry = _registry()[ctype]
    candidates = [c for c in rows.columns if c not in (ENTITY, ENTITY_GROUP, "deleted")]
    counts = (
        rows.aggregate([fn.count(col(c)) for c in candidates]).fetchall()[0]
        if candidates
        else ()
    )
    held = [c for c, k in zip(candidates, counts) if c in registry or k]
    return rows.project(*(col(c) for c in (ENTITY, *held)))


def _flags(long: DuckDBPyRelation, by: str | None = None) -> dict[Any, Flags]:
    """The shape of `long`'s rows over `snapshot`, per value of `by` or as one.

    `snapshot` is the one broadcast dim a build reads the flags of, and an
    attribute not addressed by it has neither set. A value of `by` with no row
    is absent, and so is `None` where `by` is `None` and `long` has no row.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    snapshot = (
        [fn.bool_or(col(SNAPSHOT).isnotnull()), fn.bool_or(col(SNAPSHOT).isnull())]
        if SNAPSHOT in long.columns
        else [lit(False), lit(False)]  # noqa: FBT003
    )
    keys = [] if by is None else [col(by)]
    rows = long.aggregate(
        [
            *keys,
            fn.count(lit(1)),
            fn.bool_or(col("breakpoint").isnotnull()),
            *snapshot,
        ],
        by or "",
    ).fetchall()
    return {
        (key[0] if key else None): Flags(
            varies=frozenset({SNAPSHOT} if varies else ()),
            broadcast=frozenset({SNAPSHOT} if broadcast else ()),
            breakpoints=bool(breakpoints),
        )
        for *key, count, breakpoints, varies, broadcast in rows
        if count
    }


def _group_flags(record: RecordLike) -> dict[str, dict[str, Flags]]:
    """Per component type, each attribute with rows for its members, and their flags.

    The record's own `flags` cannot answer per type, the type being an
    attribute rather than an axis, so this reads each attribute's rows by
    `entity_group` instead. An attribute over an axis alone belongs to the
    record rather than to a component, and is in no type's map.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    frame = record.dims.get(ENTITY)
    if frame is None:
        return {}
    groups = to_relation(frame).project(ENTITY, ENTITY_GROUP).set_alias("g")
    out: dict[str, dict[str, Flags]] = {}
    for attr in record.attributes:
        if not record.schema.addresses_entity(attr):
            continue
        long = to_relation(record.attributes[attr]).set_alias("a")
        rows = long.join(groups, "a.entity = g.entity").project(
            *(col("a", c) for c in long.columns), col("g", ENTITY_GROUP)
        )
        for group, flags in _flags(rows, ENTITY_GROUP).items():
            out.setdefault(group, {})[attr] = flags
    return out


def _scoped(long: DuckDBPyRelation, members: DuckDBPyRelation) -> DuckDBPyRelation:
    """`long`'s rows for `members` alone."""
    return long.set_alias("a").join(
        members.project(ENTITY).set_alias("m"), "a.entity = m.entity", how="semi"
    )


def _ordered_connections(
    record: RecordLike, members: DuckDBPyRelation
) -> pd.DataFrame | None:
    """One type's connections with a port index assigned per component.

    The positional collapse: connections come in member order (an
    overlay's `order_key`: first-introduced) and are numbered within each
    component, so a port index follows the order connections were introduced
    and a patch that adds one appends rather than renumbering. Inputs are
    placed before outputs, so `bus0` is the input end PyPSA's sign convention
    expects.

    Scoped to one type by its `members`, one frame holding every type's
    connections.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    frame = record.groups.get(CONNECTION)
    if frame is None:
        return None
    df = frame.collect(backend="pandas").to_native()
    mine = {entity for (entity,) in members.project(ENTITY).fetchall()}
    df = df[df["entity"].isin(mine)]
    if df.empty:
        return None
    # Per component: ports are numbered within the entity they belong to.
    within = ["entity"]
    if "role" in df.columns:
        df = df.assign(_role_rank=(df["role"] != _INPUT_PORT).astype(int)).sort_values(
            [*within, "_role_rank"], kind="stable"
        )
    # A single-attachment component keeps PyPSA's unsuffixed `bus`, so its port
    # is the empty index rather than `0` (`_port_attribute`).
    # `dropna=False`: a dim left NULL means "every value of it" (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule), which
    # is an ordinary group here, not a row to drop.
    grouped = df.groupby(within, dropna=False)
    single = grouped["bus"].transform("size").eq(1) & df["role"].eq(_SINGLE_PORT)
    df["port"] = grouped.cumcount().astype(str).where(~single, "")
    return df


def _connection_attributes(
    record: RecordLike, members: DuckDBPyRelation
) -> frozenset[str]:
    """Port attribute names this type's connection rows can supply.

    `bus0`/`bus1`/... for the ports that actually exist, so `verify` knows a
    required `bus0` is satisfied by a connection rather than by a column.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    df = _ordered_connections(record, members)
    if df is None:
        return frozenset()
    return frozenset(_port_attribute("bus", port) for port in df["port"].unique())


def _collapse_connections(
    static: DuckDBPyRelation,
    record: RecordLike,
    members: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Add `bus0`/`bus1`/... columns to a static frame from its connection rows.

    PyPSA wants a column per port; the record stores a row per connection. This is the seam between them, and the only direction that needs
    positions at all.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    df = _ordered_connections(record, members)
    if df is None:
        return static
    # Pivoted on the same key the ports were numbered within, so each
    # (component, scenario, ...) keeps its own port columns.
    index = ["entity"]
    # A dim left NULL means "every value of it" (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule), so it is a real key
    # here - but `pivot` drops NaN index levels, so such a dim is folded away
    # instead: every row shares its value, and the join below matches on the
    # dims that actually distinguish rows.
    varying = [d for d in index[1:] if df[d].notna().any()]
    wide = df.pivot(index=["entity", *varying], columns="port", values="bus")
    wide.columns = [_port_attribute("bus", port) for port in wide.columns]
    wide = wide.reset_index()  # noqa: F841 - queried by name below
    ports = con.sql("FROM wide").set_alias("ports")
    on = ["entity", *(d for d in varying if d in static.columns)]
    joined = static.set_alias("s").join(
        ports, ex_all(col("s", c) == col("ports", c) for c in on), how="left"
    )
    return joined.project(
        *(col("s", c) for c in static.columns),
        *(col("ports", c) for c in ports.columns if c not in ("entity", *varying)),
    )


def _restore_extendable(static: DuckDBPyRelation, ctype: str) -> DuckDBPyRelation:
    """`extendable` back under `ctype`'s `<x>_nom_extendable`, or dropped where it has none."""
    if EXTENDABLE not in static.columns:
        return static
    names = _extendable_attributes(_blank_network().c[ctype])
    return static.project(
        *(col(c) for c in static.columns if c != EXTENDABLE),
        *(col(EXTENDABLE).alias(name) for name in names),
    )


def _frame(record: RecordLike, kind: str, key: str) -> pd.DataFrame | None:
    """One of `record`'s `dims` or `groups` frames as pandas, `None` if absent."""
    frames = record.dims if kind == "dims" else record.groups
    frame = frames.get(key)
    return None if frame is None else frame.collect(backend="pandas").to_native()


def _pypsa_columns(frame: pd.DataFrame, record: RecordLike, dim: str) -> pd.DataFrame:
    """An axis frame's attribute columns under their PyPSA names, keyed `name`.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    """
    names: dict[str, str] = record.schema.meta["names"][dim]
    columns = [c for c in frame.columns if c in names]
    return frame.set_index(dim)[columns].rename(columns=names).rename_axis("name")


def _add_buses(
    n: pypsa.Network, record: RecordLike, shape: NetworkShape, con: DuckDBPyConnection
) -> None:
    """Buses from the `bus` axis, their carrier group, series and regions.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    - [shapes](https://energy-models.github.io/datarecord/design/tools/#shapes)
    """
    frame = _frame(record, "dims", BUS)
    if frame is None:
        return
    static = _pypsa_columns(frame, record, BUS)
    carriers = _frame(record, "groups", _CARRIER_GROUPS[BUS])
    if carriers is not None:
        static = static.join(carriers.set_index(BUS)[CARRIER])
    regions = static[[c for c in _REGIONS.values() if c in static.columns]]
    rel = _positioned(static.drop(columns=regions.columns), con)

    schema = record.schema
    attributes = {}
    for name, pypsa_name in schema.meta["names"][BUS].items():
        spec = schema.attributes.get(name)
        if spec is None or not spec.varying or name not in record.attributes:
            continue
        long = to_relation(record.attributes[name]).project(
            star(exclude=[BUS]), col(BUS).alias(ENTITY)
        )
        flags = _flags(long).get(None)
        if flags is not None:
            attributes[pypsa_name] = (long, flags)
    _add_component_type(n, "Bus", rel, attributes, shape, con)
    _add_regions(n, regions)


def _add_regions(n: pypsa.Network, regions: pd.DataFrame) -> None:
    """Bus regions back to `Shape` rows, numbered in bus order.

    Notes
    -----
    - [shapes](https://energy-models.github.io/datarecord/design/tools/#shapes)
    """
    import geopandas as gpd

    kinds = {column: kind for kind, column in _REGIONS.items()}
    rows = (
        regions.rename_axis("idx")
        .reset_index()
        .melt(id_vars="idx", var_name="type", value_name="geometry")
    )
    rows = rows.dropna(subset=["geometry"]).reset_index(drop=True)
    if rows.empty:
        return
    names = rows.index.astype(str)
    n.add(
        "Shape",
        names,
        geometry=gpd.GeoSeries.from_wkt(
            rows["geometry"].to_numpy(), index=names, crs=n.crs
        ),
        idx=pd.Series(rows["idx"].to_numpy(), index=names),
        component="Bus",
        type=pd.Series(rows["type"].map(kinds).to_numpy(), index=names),
    )


def _positioned(static: pd.DataFrame, con: DuckDBPyConnection) -> DuckDBPyRelation:
    """A `name`-indexed static frame as the relation `_add_component_type` takes."""
    df = static.reset_index().rename(columns={"name": ENTITY})  # noqa: F841 - queried by name below
    return con.sql("SELECT *, row_number() OVER () AS _pos FROM df")


def _add_carriers(
    n: pypsa.Network, record: RecordLike, shape: NetworkShape, con: DuckDBPyConnection
) -> None:
    """Carriers from the `carrier` axis."""
    frame = _frame(record, "dims", CARRIER)
    if frame is not None:
        static = _positioned(_pypsa_columns(frame, record, CARRIER), con)
        _add_component_type(n, "Carrier", static, {}, shape, con)


def _classify(
    static: DuckDBPyRelation, record: RecordLike, ctype: str, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Add `carrier`, and `technology_group` with its axis columns, from the groups.

    A component with no carrier row takes the registry's default. The
    technology group columns reach only a type with grouped members, so a type
    that never carried them gains none.

    Notes
    -----
    - [the PyPSA tool](https://energy-models.github.io/datarecord/design/tools/#the-pypsa-tool)
    - [technology groups](https://energy-models.github.io/datarecord/design/tools/#technology-groups)
    """
    from pypsa.components.types import get as get_component_type

    entities = static.project(ENTITY).df()[ENTITY]
    extra = pd.DataFrame(index=pd.Index(entities, name=ENTITY))
    carriers = _frame(record, "groups", _CARRIER_GROUPS[ENTITY])
    default = get_component_type(ctype).defaults.at[CARRIER, "default"]
    extra[CARRIER] = (
        default
        if carriers is None
        else extra.index.map(carriers.set_index(ENTITY)[CARRIER]).fillna(default)
    )
    members = _frame(record, "groups", TECHNOLOGY_GROUP_GROUP)
    if members is not None and members[ENTITY].isin(entities).any():
        groups = members.set_index(ENTITY)[TECHNOLOGY_GROUP]
        extra[TECHNOLOGY_GROUP] = extra.index.map(groups)
        axis = _frame(record, "dims", TECHNOLOGY_GROUP)
        if axis is not None:
            columns = _pypsa_columns(axis, record, TECHNOLOGY_GROUP)
            extra = extra.join(columns, on=TECHNOLOGY_GROUP)
    extra = extra.reset_index()  # noqa: F841 - queried by name below
    joined = con.sql("FROM extra").set_alias("x")
    return (
        static.set_alias("s")
        .join(joined, "s.entity = x.entity", how="left")
        .project(
            *(col("s", c) for c in static.columns),
            *(col("x", c) for c in joined.columns if c != ENTITY),
        )
    )


# The tool is a module-level singleton, imported rather than looked up by
# name: `from datarecord.tools.pypsa import PyPSA` then `PyPSA.build(revision.record)`.
PyPSA = PyPSATool()
