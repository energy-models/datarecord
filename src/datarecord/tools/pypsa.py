# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The PyPSA tool: record -> `pypsa.Network` -> results.

The only module that knows PyPSA's network shape - that its axes are
`snapshot`/`period`/`scenario`, that a stochastic network is indexed by
`(scenario, name)`, and how its static/series split maps onto the record's
`dims/entity_type` + `inputs/` split.

Notes
-----
- [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
"""

from __future__ import annotations

import math
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache, cached_property
from typing import TYPE_CHECKING, Any

import narwhals as nw
import numpy as np
import pandas as pd
from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb import StarExpression as star

from datarecord.duck import ex_all
from datarecord.record import Flags, Frames, LazyFrames, RecordLike
from datarecord.schema import AttributeSpec, Dimension, Group, Trait
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

# The axes a PyPSA network is built from; all three must be declared in the
# schema's `dimensions`, since a network shape is built from them (https://energy-models.github.io/datarecord/design/tools/).
# A declared axis with no rows is fine - that is just a deterministic or
# single-period network.
SNAPSHOT, PERIOD, SCENARIO = "snapshot", "period", "scenario"
# Not PyPSA's own `weight` - that name is a Generator attribute (its clustering
# weight), and a record has one attribute per name record-wide - so the scenario
# weighting is renamed on the way in and back to `weight` on build.
SCENARIO_WEIGHT = "scenario_weight"
ENTITY, BUS, CONNECTION = "entity", "bus", "connection"
ENTITY_TYPE = "entity_type"
# An attribute over the `connection` group, not a column the format fixes: the
# port vocabulary below is this tool's, so the record layer never names it.
ROLE = "role"
REQUIRED_DIMS = frozenset({SNAPSHOT, PERIOD, SCENARIO})

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
    def of(cls, index: pd.Index, con: DuckDBPyConnection) -> Index:
        df = index.to_frame(index=False)  # noqa: F841 - queried by name below
        return cls(index, con.sql("SELECT *, row_number() OVER () AS _pos FROM df"))

    def expand(self, rel: DuckDBPyRelation, column: str) -> DuckDBPyRelation:
        """Broadcast rows with a null `column` across this index, others as-is.

        `rel` must already carry the alias the caller wants to select its
        own columns by - `expand` doesn't impose one.
        """
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

    The pivot is on each column's position in `static_index` rather than its
    name, so a name never becomes an SQL identifier: one equal to a group-by
    column would collide with it, and DuckDB folds identifiers' case.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    if shape.multiperiod:
        rows = periods.expand(rows, PERIOD)
    if shape.stochastic:
        rows = scenarios.expand(rows, SCENARIO)
    group_by = [PERIOD, SNAPSHOT] if shape.multiperiod else [SNAPSHOT]
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
    wide = (
        snapshots.left_join(piv.set_alias("piv"), group_by)
        .project(*(col("piv", str(p)) for p in present))
        .df()
    )
    wide.index = snapshots.index
    wide.columns = static_index[[p - 1 for p in present]]
    return wide


# -- build (https://energy-models.github.io/datarecord/design/tools/) -------------------------------------------------------------


def _exported(c) -> bool:
    """Whether `c`'s members belong in a record at all.

    Empty types have nothing to write. Standard types (`LineType`,
    `TransformerType`) are excluded because PyPSA prepopulates them on every
    fresh `Network` and `_broadcast_standard_types` restores them on build -
    writing them would import each row a second time on the way back.
    """
    return not c.static.empty and c.name not in c.n.standard_type_components


def _colliding_names(n: pypsa.Network) -> frozenset[str]:
    """Names more than one exported component claims, compared case-insensitively.

    Only exported types count, since only those become member rows. Read off the
    `name` level rather than the index: a stochastic network is keyed
    `(scenario, name)`, and comparing tuples would read one component in two
    scenarios as two names while missing a real collision. Case-folded because
    the wide->long melt turns names into DuckDB column identifiers, which are
    case-insensitive: `Gen` and `gen` would silently merge.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    seen: dict[str, tuple[str, str]] = {}
    clashing: set[str] = set()
    for c in n.components:
        if not _exported(c):
            continue
        index = c.static.index
        names = (
            index.get_level_values("name") if "name" in (index.names or []) else index
        )
        for value in names:
            name = str(value)
            first = seen.setdefault(name.casefold(), (name, c.name))
            if first != (name, c.name):
                clashing.update((name, first[0]))
    return frozenset(clashing)


# TODO(pypsa): every function below, up to `_new_network`, ports a method that
# only exists on PyPSA's unreleased data-records branch (https://energy-models.github.io/datarecord/design/tools/). Delete the
# ported copy and call the real method once a PyPSA release carries it.


def _apply_snapshots_import(n: pypsa.Network, df: pd.DataFrame) -> None:
    """Set snapshots and snapshot weightings from an imported axis table.

    Ported from `pypsa.Network._apply_snapshots_import` (unreleased) -
    built entirely from public surface (`set_snapshots`, `snapshot_weightings`).

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
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

    _apply_snapshots_import(n, shape.snapshots)
    if shape.multiperiod:
        ip = shape.periods.set_index(PERIOD)
        n.periods = ip.index
        n._investment_periods_data = ip.reindex(n.investment_periods)
    if shape.stochastic:
        scen = shape.scenarios.set_index(SCENARIO)
        scen.index = scen.index.astype(str).rename(SCENARIO)
        # `set_scenarios` rather than assigning `_scenarios_data`: it rebuilds
        # each component's empty `static`/`dynamic` frames with the `scenario`
        # level, so later imports union against a correctly named 2-level index
        # instead of the single-level one a fresh component carries.
        n.set_scenarios(scen[SCENARIO_WEIGHT])
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
    snapshots = Index.of(n.snapshots, con)
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


def _output_attributes(c: pypsa.Components) -> frozenset[str]:
    """A component type's result attributes, from PyPSA's own registry.

    `defaults["status"]` marks them ("Output"), so a PyPSA upgrade that adds a
    result attribute is picked up rather than needing a list kept here. Read off
    `c` rather than the bare registry: a network's extra ports (`p2` on a Link
    with `bus2`) exist only in its own `defaults`.
    """
    defaults = c.defaults
    is_output = defaults["status"].astype(str).str.startswith("Output")
    return frozenset(defaults.index[is_output])


# -- network -> layer (https://energy-models.github.io/datarecord/design/format/) ------------------------------------------------

# Which end of the component a port is. PyPSA encodes this only by sign
# convention - `p0` flows in at `bus0`, `p1` out at `bus1` - so the mapping to
# a record's `role` lives here, the one place that convention is written down
# (https://energy-models.github.io/datarecord/design/record/#connections). Ports beyond the first two are outputs: a multi-port Link's `bus2`
# onward are additional sinks.
_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT = "input", "output", "attached"
# Every value `_port_role` can return - the closed vocabulary of `role`, so the
# schema declares it as an enum rather than a bare string.
_ROLES = (_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT)

# Attribute stems that exist once per port. A record stores each as one
# bus-keyed attribute (https://energy-models.github.io/datarecord/design/record/#connections), so these are the names whose port suffix is
# undone on write and reapplied on build.
_PORT_STEMS = ("bus", "efficiency", "p")


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


def _port_attribute(stem: str, port: str) -> str:
    """PyPSA's name for `stem` at `port`: `bus0`, `efficiency`, `efficiency2`, ...

    `bus` is suffixed from `0`, every other per-port attribute from `2` - the
    quirk that makes the port vocabulary unguessable from a registry and is why
    a record keys connections by bus instead.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    if stem == "bus":
        return f"bus{port}"
    return stem if port == "1" else f"{stem}{port}"


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
        frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["entity", "bus", "role", "deleted"])
    return pd.concat(frames, ignore_index=True)


# TODO(pypsa): `_is_output`, `_drop_default_rows` and `_scalar_rows` below port
# helpers that only exist on PyPSA's unreleased data-records branch (https://energy-models.github.io/datarecord/design/tools/).
# Delete the ported copy and call the real one once a PyPSA release carries it.


def _is_output(defaults: pd.DataFrame, attr: str) -> bool:
    """Whether `attr` is an output (custom attrs with no status are inputs).

    Ported from `pypsa.common._is_output` rather than depended on, since only
    a released PyPSA can be relied on here.

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
        ctypes = {ct for ct in record.entity_types if ct in known}
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
        type it names is one PyPSA knows, and each type's required attributes
        are resolvable - either owned by some layer (the owner map), supplied
        by the `dims/entity_type` frame, or carrying a declared default. A renamed or computed attribute is checked against its sources
        (`Schema.sources`), which is also how it is reported.

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
        for ctype in sorted(record.entity_types):
            # A type PyPSA has no registry entry for, though the record's own
            # schema declares it. Reported rather than raised: the record layer
            # upholds its schema's vocabulary and knows no framework's, so this
            # reads back fine and it is this tool's business that it cannot be
            # built.
            if ctype not in known:
                entity_types.add(ctype)
                continue
            resolved = record.flags(ctype)
            owned = set(resolved)
            # A curve, not a scalar (https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows). PyPSA takes a scalar for every
            # attribute this build assigns, so the record is storing something
            # correct that this translation cannot express - reported here
            # rather than silently pivoting one arbitrary breakpoint.
            unsupported_values |= {
                (ctype, attr) for attr, flags in resolved.items() if flags.breakpoints
            }
            static_cols = _static_columns(record, ctype)
            # A port attribute the record supplies as connection rows rather
            # than as a column: `bus0`/`bus1` are satisfied by a connection
            # per port, so the collapse in `build` can name them (https://energy-models.github.io/datarecord/design/record/#connections).
            from_connections = _connection_attributes(record, ctype)
            specs = declared.attributes_for(ctype)
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

        for ctype in sorted(record.entity_types):
            static = to_relation(record.entity_types[ctype])
            if not shape.stochastic and SCENARIO in static.columns:
                static = static.project(star(exclude=[SCENARIO]))
            # `_pos` in member order before any join scrambles it: the member
            # frame arrives in member order (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis),
            # and `_assign_static` sorts the wide frame back by it at the end.
            static = static.project("*, row_number() OVER () AS _pos")
            # Connections back to the positional columns PyPSA expects (https://energy-models.github.io/datarecord/design/tools/).
            static = _collapse_connections(static, record, ctype, con)

            # Frames are built and released per type, so peak memory is one
            # type's wide frames rather than the whole network (https://energy-models.github.io/datarecord/design/tools/).
            attributes = {}
            carried = schema.attributes_for(ctype)
            for attr, flags in record.flags(ctype).items():
                if attr not in carried:
                    continue
                # Both sets empty no longer means "no rows": the flags are
                # scoped to what an attribute is addressed by (https://energy-models.github.io/datarecord/design/record/#flags), so an
                # attribute over `entity` alone has no broadcast dim to report
                # and still has values. An attribute with no rows at all is
                # absent from the map entirely, which is what this filtered.
                # Through the schema, so a renamed or computed attribute
                # reaches the pivot below as an ordinary long relation. Scoped
                # by a semi-join against `static`, this type's entity table (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types).
                long = self.schema.resolve(record, ctype, attr)
                # An attribute addressed by `entity` scopes to this type's
                # members; one addressed by an axis alone has no entity column
                # to scope by, and belongs to the record rather than to a
                # component (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
                if ENTITY in long.columns:
                    long = long.set_alias("a").join(
                        static.project("entity").distinct().set_alias("m"),
                        "a.entity = m.entity",
                        how="semi",
                    )
                attributes[attr] = (long, flags)

            _add_component_type(n, ctype, static, attributes, shape, con)

        _broadcast_standard_types(n)
        return n

    def results(self, model: pypsa.Network, con: DuckDBPyConnection) -> Frames:
        """A solved network's result attributes in the record's long form.

        Keyed by attribute, matching `outputs/<attr>.parquet`: every component
        type's rows for one attribute are concatenated into one frame, exactly as
        `attributes` presents `inputs/`. The union needs no
        `entity_type` to tell the arms apart. Which attributes count as
        results comes from PyPSA's registry, so an upgrade that adds one is
        picked up.

        Each frame is a lazy DuckDB relation on `con` over the network's
        containers, melted wide->long by `UNPIVOT`, so nothing is computed until
        collected.

        Rows still at the attribute's default are dropped: a static output like
        `Bus.control` carries its default whether or not the network was solved,
        and an absent output file means exactly "take the default".

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        # The same wide->long melt `write_layer` uses, kept lazy. `outputs`
        # keys every registry result; a result absent on this network unions to
        # `None` and is dropped here rather than surfaced, since a reader takes
        # the default for it (the outputs default rule) - no key, no collect.
        source = _NetworkSource(model, con)
        names = source._output_names()
        columns = ["entity", *source._DIMS, "attribute", "breakpoint", "value"]
        frames = {
            attr: rel
            for attr, ctypes in names.items()
            if (rel := source._union_rel(source._output_rels(attr, ctypes), columns))
            is not None
        }
        return {attr: nw.from_native(rel).lazy() for attr, rel in frames.items()}

    def to_datarecord(
        self, model: pypsa.Network, con: DuckDBPyConnection
    ) -> RecordLike:
        """Present a `Network` as the `Record` `write_layer` persists.

        The inverse of `build`, and the only place PyPSA's shape is undone:
        `c.static`/`c.dynamic` become long rows on `con`, and
        `bus0`/`bus1`/`efficiency2` become connection rows carrying a `role`.
        Key sets are read off the network, so listing unpivots nothing and a
        lookup only what is asked for.

        Raises
        ------
        UnsupportedRecordError
            If two component types share a name: PyPSA scopes names per type, a
            record record-wide, and this reports rather than renames.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        clashing = _colliding_names(model)
        if clashing:
            raise UnsupportedRecordError(self.name, Requirements(names=clashing))
        return _NetworkSource(model, con)


def _port_stems(c: pypsa.Components) -> dict[str, str]:
    """Per-port attribute name -> its stem, for this type's actual ports."""
    return {
        name: stem
        for stem in _PORT_STEMS
        for port in c.ports
        if (name := _port_attribute(stem, port)) in c.defaults.index
    }


@cache
def _registry_schema() -> RecordSchema:
    """The record schema PyPSA's whole component registry declares.

    Every network exports against this one schema: its dims, groups, attributes,
    results and traits describe every PyPSA component type and attribute, not
    only the ones a given network holds. Declaring a type or attribute a network
    lacks writes no file and is rejected by nothing - the enum only rejects an
    *unknown* type - so a network-independent schema is both correct and the
    stabler contract (two networks with different component sets no longer
    produce mutually incompatible schemas). Derived once, cached: the registry
    does not change within a process.

    `c.defaults` already declares what an `AttributeSpec` asks for, so this reads
    it rather than restating it. Results go to `results` rather than
    `attributes`, read off the same registry (`status` starting "Output"), so a
    PyPSA upgrade adding one is picked up rather than needing a list kept here;
    they carry no trait, a trait being the input vocabulary a type is validated
    and split against, which a result is not.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    import pypsa

    n = pypsa.Network()
    # Which end of the component an attachment is, which PyPSA's sign convention
    # reads off the port index (`_port_role`). Declared over the `connection`
    # group, so it is a column of that group's file rather than a long row - and
    # so the record layer needs no rule of its own for a word that is this tool's
    # vocabulary (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).
    attributes: dict[str, AttributeSpec] = {
        ROLE: AttributeSpec(
            dtype=nw.Enum(_ROLES),
            dims=frozenset({CONNECTION}),
            description="Which end of the component this attachment is.",
        ),
        # A probability per scenario, which `dims` addresses by the scenario axis
        # alone - so it is a column of `dims/scenario.parquet`, where
        # `scenario_weightings` already puts it, rather than a long row. Declared
        # because it is data a consumer reads back: `set_scenarios` takes it on
        # import, and an undeclared column would carry no dtype. A non-stochastic
        # network simply writes no scenario axis and no file.
        SCENARIO_WEIGHT: AttributeSpec(
            dtype=nw.Float64(),
            dims=frozenset({SCENARIO}),
            description="How much this scenario counts in the expectation.",
        ),
    }
    results: dict[str, AttributeSpec] = {}
    carries: dict[str, frozenset[str]] = {}
    for ctype in sorted(n.all_components - n.standard_type_components):
        c = n.components[ctype]
        defaults = c.defaults
        # Per-port columns collapse to their stem, which a record keys by bus
        # rather than by position (https://energy-models.github.io/datarecord/design/record/#connections); `efficiency2` and `efficiency`
        # agree on every declaration, so either row answers.
        per_port = _port_stems(c)
        outputs = set(_output_attributes(c))
        carried: set[str] = set()
        for attr in defaults.index:
            if attr == "name":
                continue
            stem = per_port.get(attr, attr)
            row = defaults.loc[attr]
            # `entity` is a dim like any other, so an entity attribute declares
            # it: `dims` is the whole address, and an attribute omitting it would
            # be one no entity owns (https://energy-models.github.io/datarecord/design/schema/#attributespec). A per-port attribute names the
            # `connection` group instead, which expands to `(entity, bus)`
            # (https://energy-models.github.io/datarecord/design/schema/#groups). A varying attribute varies over time and,
            # in a stochastic network, scenario; both are declared always, a
            # network without scenarios writing none.
            dims = {SNAPSHOT, SCENARIO} if row["varying"] else set()
            dims.add(CONNECTION if attr in per_port else ENTITY)
            spec = AttributeSpec(
                dtype=_DTYPES.get(row["typ"], nw.String()),
                dims=frozenset(dims),
                default=_default(row["default"]),
                unit=_text(row.get("unit")),
                description=_text(row.get("description")),
            )
            # One attribute, one spec, record-wide: two types declaring the same
            # name must agree, since one `<kind>/<attr>.parquet` with one `value`
            # column serves both. PyPSA's registry does agree everywhere today,
            # so the first type to declare one wins and a later disagreement is a
            # schema error rather than a silent per-type divergence the storage
            # could not have honoured.
            if attr in outputs:
                # A result is declared but not carried: it belongs to no trait,
                # `attributes_for` being the input vocabulary a type is validated
                # and split against.
                results.setdefault(stem, spec)
                continue
            if stem in carried:
                continue
            carried.add(stem)
            attributes.setdefault(stem, spec)
        if carried:
            carries[c.name] = frozenset(carried)
    # A name PyPSA registers as an output on one type and an input on another
    # would be both here; the input declaration wins, one file holding one
    # `value` column either way.
    results = {a: s for a, s in results.items() if a not in attributes}
    # PyPSA's registry is per type - a `Line` has no `efficiency` - so every
    # attribute is narrowed to the types that declare it, and none is left
    # carried by all. One trait per type is the faithful translation of a
    # registry that ships no trait vocabulary of its own (https://energy-models.github.io/datarecord/design/schema/#traits).
    traits = {
        ctype: Trait(attributes=carried, on={ENTITY_TYPE: frozenset({ctype})})
        for ctype, carried in carries.items()
    }
    return RecordSchema(
        dimensions={
            SNAPSHOT: Dimension(
                dtype=nw.Datetime(),
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
            # The two axes the `connection` group is over. `entity` is the entity
            # axis the format knows by name; `bus` is an ordinary dim, one
            # coordinate of one group.
            ENTITY: Dimension(dtype=nw.String(), description="An entity."),
            BUS: Dimension(dtype=nw.String(), description="A node of the network."),
            # The kinds an entity may be: the labels are PyPSA's registry, so an
            # enum rather than a bare string, and a type outside it is rejected on
            # write.
            ENTITY_TYPE: Dimension(
                dtype=nw.Enum(sorted(carries)),
                description="What kind of entity an entity is.",
            ),
        },
        groups={
            CONNECTION: Group(
                over={"entity": ENTITY, "bus": BUS},
                description="An entity's attachment to one bus.",
            ),
            # `into` over `entity` alone is what makes `entity_type` the
            # entity-type axis (https://energy-models.github.io/datarecord/design/schema/#entity_type-the-axis-of-kinds).
            ENTITY_TYPE: Group(
                over={ENTITY: ENTITY},
                into=ENTITY_TYPE,
                description="What kind of entity each entity is.",
            ),
        },
        attributes=attributes,
        results=results,
        traits=traits,
        # `partial` names value dims a layer patches per value: a layer may set
        # one generator's `p_nom` per scenario without restating the rest.
        # Membership keys - `entity`, the `connection` group's `bus` - are in the
        # fold key by being membership, not by being `partial`
        # (https://energy-models.github.io/datarecord/design/read-path/#one-fold-for-every-axis).
        partial=frozenset({SCENARIO}),
    )


@dataclass(frozen=True)
class _NetworkSource:
    """A `pypsa.Network` presented as a `Record`.

    Every `LazyFrames` here is built from key sets read off the network and
    its registry, so nothing is unpivoted until a key is looked up.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """

    n: pypsa.Network
    #: The connection the wide->long melt runs on.
    con: DuckDBPyConnection

    # The dims a PyPSA network has axes for; `Coords` stays generic, this tool
    # decides these three are what a network is shaped by (https://energy-models.github.io/datarecord/design/tools/).
    _DIMS = (SNAPSHOT, PERIOD, SCENARIO)

    @property
    def schema(self) -> RecordSchema:
        """The layer's schema: the registry-derived schema with this network's meta.

        The dims, groups, attributes, results and traits are the same for every
        network - `_registry_schema` derives them once from PyPSA's whole
        component registry - so all that varies here is `meta`, which carries
        this network's own attributes and `n.meta` for read-back.

        Notes
        -----
        - [the schema](https://energy-models.github.io/datarecord/design/schema/)
        """
        return _registry_schema().model_copy(
            update={
                "meta": {
                    "format": "pypsa-parquet",
                    "attributes": _collect_network_attributes(self.n),
                    "meta": dict(self.n.meta),
                }
            }
        )

    @property
    def dims(self) -> LazyFrames:
        """Every axis this network holds, `entity` among them.

        `entity` is an axis like the others rather than something the writer
        works out: a record supplies its own membership, so nothing downstream
        has to reconstruct it from the per-type files.

        Notes
        -----
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        """
        axes = {
            SNAPSHOT: lambda: self.n.snapshots.to_frame(index=False),
            PERIOD: lambda: self.n.investment_periods.to_frame(index=False),
            # With its weights: an axis frame carries the whole row, not just
            # the key column (https://energy-models.github.io/datarecord/design/record/#axis-order), and `scenarios` alone is a bare Index.
            SCENARIO: lambda: self.n.scenario_weightings.reset_index().rename(
                columns={"weight": SCENARIO_WEIGHT}
            ),
        }
        present = tuple(d for d in self._DIMS if len(axes[d]()) > 0)
        axes[ENTITY] = self._entity_axis_frame
        return LazyFrames(
            (*present, ENTITY), lambda dim: nw.from_native(axes[dim]()).lazy()
        )

    def _entity_axis_frame(self) -> pd.DataFrame:
        """`(entity, entity_type, deleted)` over every exported type.

        The type is a column here and in no member file: one file per type is
        what says a row's type there, and this axis is what carries it for every
        later reader.

        Notes
        -----
        - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
        """
        frames = [
            self._entity_type_frame(ctype)
            .select(ENTITY, "deleted")
            .with_columns(**{ENTITY_TYPE: nw.lit(ctype)})
            .collect(backend="pandas")
            .to_native()
            for ctype in self.entity_types
        ]
        if not frames:
            return pd.DataFrame(columns=[ENTITY, ENTITY_TYPE, "deleted"])
        return pd.concat(frames, ignore_index=True)

    @property
    def entity_types(self) -> LazyFrames:
        types = tuple(c.name for c in self.n.components if _exported(c))
        return LazyFrames(types, self._entity_type_frame)

    @property
    def groups(self) -> LazyFrames:
        """The `connection` group, one frame across every type.

        Every type with any port, single-attachment ones included: a
        Generator's one `bus` is as much a connection as a Link's `bus0`,
        and `c.ports == [""]` makes `_port_attribute` name it correctly.

        The entity-type group is not here, its rows being the entity axis's -
        `dims["entity"]`, which this record supplies like any other axis.

        Notes
        -----
        - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
        """
        return LazyFrames((CONNECTION,), lambda _: self._connection_frame())

    def _connection_frame(self) -> nw.LazyFrame:
        """Every type's connections in one frame, `(entity, bus, role)`.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        types = [c.name for c in self.n.components if _exported(c) and c.ports]
        frames = [_connection_rows(self.n.c[ctype]) for ctype in types]
        rows = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=["entity", "bus", "role"])
        )
        # A stochastic network repeats its connections per scenario, which
        # collapse to one row: an attachment exists or it does not, so nothing
        # scopes it per value of an axis (`_tagged` says the same of members).
        if SCENARIO in rows.columns:
            rows = rows.drop(columns=[SCENARIO]).drop_duplicates(
                subset=["entity", "bus"]
            )
        return nw.from_native(rows).lazy()

    def _as_long_rel(
        self, c: pypsa.Components, attr: str, *, drop_defaults: bool = False
    ) -> DuckDBPyRelation | None:
        """`attr` as a long relation `(entity, snapshot, scenario, period, value)`.

        `None` where the attribute contributes no rows on this network, so a
        caller unions only the types that do rather than empty relations.

        The wide `snapshots x entities` (or
        `snapshots x (scenario, entity)`) dynamic frame is melted by `UNPIVOT`
        rather than `pandas.stack`, so the relation stays a lazy plan
        `write_layer` writes without a copy. Static scalars land as null-snapshot
        rows exactly as `_scalar_rows` builds them; `drop_defaults` omits a scalar
        that only repeats the attribute's default.

        Parameters
        ----------
        drop_defaults
            Omit static scalar rows whose value equals the attribute's default.

        Notes
        -----
        - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
        - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
        """
        con = self.con
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

        parts: list[DuckDBPyRelation] = []
        excluded: pd.Index = pd.Index([])
        if varying:
            melted = self._melt_dynamic(con, c, attr)
            if melted is not None:
                parts.append(melted)
            excluded = c.dynamic[attr].columns
        static_backed = attr in c.defaults.index and bool(c.defaults.at[attr, "static"])
        if not varying or static_backed:
            scalars = self._scalar_rel(
                con, c, attr, excluded, drop_defaults=drop_defaults
            )
            if scalars is not None:
                parts.append(scalars)
        if not parts:
            return None
        rel = parts[0]
        for other in parts[1:]:
            rel = rel.union(other)
        return rel

    def _melt_dynamic(
        self, con: DuckDBPyConnection, c: pypsa.Components, attr: str
    ) -> DuckDBPyRelation | None:
        """Melt one varying attribute's wide dynamic frame to long rows.

        `None` where the frame has no entity columns - a degenerate
        `UNPIVOT ... IN ()` and an empty relation the caller would only union
        away.

        The melted columns are named by position and aliased back to their
        names as string literals, so a name never becomes an SQL identifier:
        a quote in it would end the identifier, a name equal to an id column
        would duplicate it, and DuckDB folds identifiers' case.
        """
        wide = c.dynamic[attr]
        stochastic = isinstance(wide.columns, pd.MultiIndex)
        multi_snapshot = isinstance(c.snapshots, pd.MultiIndex)
        idcols = ["period", "snapshot"] if multi_snapshot else ["snapshot"]
        # One flat value column per melted column: `scenario\x1fentity` where a
        # scenario level exists, split back out after the UNPIVOT. `reset_index`
        # returns a fresh frame, so relabelling its columns never mutates the
        # dynamic frame PyPSA owns - no defensive copy needed.
        valcols = (
            [f"{s}\x1f{e}" for s, e in wide.columns]
            if stochastic
            else list(wide.columns)
        )
        if not valcols:
            return None
        positional = [f"v{i}" for i in range(len(valcols))]
        w = wide.set_axis(positional, axis=1).reset_index()
        w.columns = [*idcols, *positional]
        # `from_df` captures the frame into the relation's own plan, so a later
        # call reusing the frame variable cannot rebind this one (unlike a named
        # registered view).
        wide_rel = con.from_df(w)  # noqa: F841 - bound by name for the query below
        inlist = ", ".join(
            f"{p} AS {lit(str(name))}"
            for p, name in zip(positional, valcols, strict=True)
        )
        idsel = ", ".join(idcols)
        # Two steps rather than one nested query: the local-relation replacement
        # scan does not compose through a subquery, so melt first, then project.
        melted = con.sql(  # noqa: F841 - bound by name for the query below
            f"SELECT {idsel}, colname, value FROM wide_rel "
            f"UNPIVOT INCLUDE NULLS (value FOR colname IN ({inlist}))"
        )
        if stochastic:
            entity = "split_part(colname, chr(31), 2)"
            scenario = "split_part(colname, chr(31), 1)"
        else:
            entity, scenario = "colname", "CAST(NULL AS VARCHAR)"
        period = "period" if multi_snapshot else "CAST(NULL AS BIGINT)"
        return con.sql(
            f"SELECT {entity} AS entity, snapshot, {scenario} AS scenario, "
            f"{period} AS period, value FROM melted"
        )

    def _scalar_rel(
        self,
        con: DuckDBPyConnection,
        c: pypsa.Components,
        attr: str,
        exclude: pd.Index,
        *,
        drop_defaults: bool = False,
    ) -> DuckDBPyRelation | None:
        """Null-snapshot long rows from `attr`'s static scalars (`_scalar_rows`).

        `None` where no scalar rows remain - every entity excluded, an empty
        static frame, or (with `drop_defaults`) every scalar at its default - a
        relation the caller would only union away. The default drop happens here
        in pandas, where the emptiness is decidable without executing a relation.

        The reshape stays in pandas: it is one `reset_index` on a Series, cheaper
        than selecting a single column out of a wide registered relation per
        attribute (measured), and the frame lands in DuckDB the same way the melt
        does.
        """
        static = _scalar_rows(c, attr, exclude)[  # noqa: F841 - bound for the query
            ["entity", "snapshot", "scenario", "period", "value"]
        ]
        if drop_defaults:
            static = _drop_default_rows(static, c.defaults.at[attr, "default"])
        if static.empty:
            return None
        return con.sql("SELECT * FROM static")

    @property
    def attributes(self) -> LazyFrames:
        names: dict[str, list[str]] = {}
        for c in self.n.components:
            if not _exported(c):
                continue
            for attr in self._record_attributes(c):
                names.setdefault(attr, []).append(c.name)
        return LazyFrames(
            tuple(names), lambda attr: self._long_frame(attr, names[attr])
        )

    @property
    def outputs(self) -> LazyFrames:
        """Result attributes, keyed by name, from PyPSA's own registry.

        Keys are named per type by the registry and merged, matching the file
        layout - one `outputs/<attr>.parquet` across types, like `inputs/`.
        A network that was never solved still names them; the frame is then
        empty and `write_layer` writes an empty file rather than none, which
        reads back as "take the default" either way.

        Notes
        -----
        - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        names = self._output_names()
        return LazyFrames(
            tuple(names), lambda attr: self._output_frame(attr, names[attr])
        )

    def _output_names(self) -> dict[str, list[str]]:
        """Result attribute -> the exported types declaring it, PyPSA's registry."""
        names: dict[str, list[str]] = {}
        for c in self.n.components:
            if not _exported(c):
                continue
            for attr in _output_attributes(c):
                names.setdefault(attr, []).append(c.name)
        return names

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Never consulted: `write_layer` persists frames, not flags.

        A network-backed source exists to be written, and the write path reads
        only the frame mappings. Answering properly is easy - `c.static` and
        `c.dynamic` are PyPSA's own split on the snapshot axis, so it needs no
        scan - but an implementation no caller reaches is one no test pins, so
        the honest answer is the empty one.

        Notes
        -----
        - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
        """
        return {}

    # -- key sets -----------------------------------------------------------

    def _record_attributes(self, c: pypsa.Components) -> list[str]:
        """`c`'s input attributes that go to `inputs/`, in the record's vocabulary.

        Those varying over `snapshot`, which PyPSA's registry flags - and those
        varying over `scenario`, which it does not, a stochastic network being
        free to hold a different `capital_cost` per scenario. Either way the
        attribute varies over something, so `inputs/` is where it lives.

        Per-port attributes collapse to their stem (`efficiency2` ->
        `efficiency`), a record keying them by bus rather than position.
        Outputs belong to `outputs/`.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        defaults = c.defaults
        outputs = set(_output_attributes(c))
        per_port = self._port_stems(c)
        diverging = _scenario_varying(c, [x for x in c.static.columns])
        stems: list[str] = []
        for attr in defaults.index:
            if attr in outputs or attr == "name":
                continue
            if not defaults.loc[attr, "varying"] and attr not in diverging:
                continue
            stem = per_port.get(attr, attr)
            if stem not in stems:
                stems.append(stem)
        return stems

    def _port_stems(self, c: pypsa.Components) -> dict[str, str]:
        """Per-port attribute name -> its stem, for this type's actual ports."""
        return _port_stems(c)

    # -- frames -------------------------------------------------------------

    def _entity_type_frame(self, ctype: str) -> nw.LazyFrame:
        """Wide members of one type: the non-varying, non-port static columns.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        """
        c = self.n.c[ctype]
        defaults = c.defaults
        per_port = set(self._port_stems(c))

        def keep(column: str) -> bool:
            if column in per_port:
                return False  # a connection supplies it (https://energy-models.github.io/datarecord/design/record/#connections)
            if column not in defaults.index:
                # A custom column the registry has no entry for (e.g.
                # `ac_dc_meshed`'s `Bus.country`). Non-varying by construction -
                # a varying attribute needs a registry entry to have a
                # container - so it belongs in the static frame, and dropping
                # it would silently lose data the protocol says to keep.
                return True
            return (
                not defaults.loc[column, "varying"]
                and defaults.loc[column, "status"] != "Output"
            )

        columns = [x for x in c.static.columns if keep(x)]
        # A stochastic network repeats its static frame per scenario, and
        # PyPSA permits the repeats to differ - `capital_cost` may be one value
        # in `high` and another in `low`. Such an attribute varies over
        # `scenario`, so it belongs in `inputs/` and is dropped here; what is
        # left is the same in every scenario and collapses to one entity row.
        columns = [x for x in columns if x not in _scenario_varying(c, columns)]
        frame = c.static[columns].reset_index().rename(columns={"name": "entity"})
        return nw.from_native(self._tagged(frame)).lazy()

    @staticmethod
    def _tagged(frame: pd.DataFrame) -> pd.DataFrame:
        """A `dims/` frame with the tombstone column the fold scopes by.

        `deleted` because the fold reads it from this same file. Not the type -
        a per-type member file is the file its rows are in, and the entity axis
        (`_entity_axis_frame`) is what states it.

        No `scenario`: an entity exists or it does not, so nothing scopes
        membership per value of an axis. A stochastic network repeats its
        static frame per scenario, which is a *value* being per-scenario
        rather than the component - so the repeats collapse to one row here
        rather than reaching the record as duplicate entities.

        Notes
        -----
        - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
        - [the record format](https://energy-models.github.io/datarecord/design/format/)
        """
        if SCENARIO in frame.columns:
            frame = frame.drop(columns=[SCENARIO]).drop_duplicates(subset=["entity"])
        if "deleted" not in frame.columns:
            frame["deleted"] = False
        return frame

    def _tagged_long_rel(
        self,
        c: pypsa.Components,
        attribute: str,
        source_attr: str,
        *,
        drop_defaults: bool,
    ) -> DuckDBPyRelation | None:
        """`source_attr`'s long relation, tagged with `attribute` and a NULL breakpoint.

        `attribute` is the record-facing name and `source_attr` PyPSA's - the two
        differ only for a per-port attribute, whose ports (`p`, `p1`, ...) each
        contribute under the one stem. A NULL `breakpoint` marks the value a
        scalar (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
        `None` where `source_attr` contributes no rows.
        """
        rel = self._as_long_rel(c, source_attr, drop_defaults=drop_defaults)
        if rel is None:
            return None
        return rel.select(
            star(),
            lit(attribute).alias("attribute"),
            lit(None).alias("breakpoint"),
        )

    def _long_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """One attribute's long rows, across every type that has it.

        A per-connection attribute contributes rows carrying the bus each port
        attaches to; a component-level one has no `bus` column at all, that
        being the connection group's coordinate rather than a column the format
        fixes. The rows are a DuckDB relation melted by `_as_long_rel`, so the
        frame stays a lazy plan `write_layer` writes without a copy.

        Notes
        -----
        - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
        - [wide and long rows](https://energy-models.github.io/datarecord/design/record/#wide-and-long-rows)
        """
        # This attribute's own columns, from its spec: one file is one attribute,
        # so a component attribute hands over no `bus` and none a dim it is not
        # addressed by (https://energy-models.github.io/datarecord/design/format/#the-long-schema).
        columns = list(_registry_schema().long_columns_for(attribute))
        rels = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            if attribute in self._port_stems(c).values():
                rels.extend(self._per_port_long_rels(c, attribute))
            elif attribute in c.defaults.index:
                rel = self._tagged_long_rel(
                    c, attribute, attribute, drop_defaults=False
                )
                if rel is not None:
                    rels.append(rel)
        return self._union_select(rels, columns)

    def _per_port_long_rels(
        self, c: pypsa.Components, stem: str
    ) -> list[DuckDBPyRelation]:
        """A per-connection attribute's long relations, one per port, `bus` filled.

        The inverse of the positional collapse: `efficiency`/`efficiency2` become
        one attribute whose rows carry the bus each port attaches to. The bus is
        joined from the port's static bus column rather than mapped in pandas, so
        the rows stay a relation.

        Notes
        -----
        - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
        """
        con = self.con
        static = c.static
        rels = []
        for port in c.ports:
            column = _port_attribute(stem, port)
            bus_column = _port_attribute("bus", port)
            if column not in c.defaults.index or bus_column not in static.columns:
                continue
            # `static` is `(scenario, name)`-indexed on a stochastic network; a
            # component's bus does not vary by scenario, so the name level
            # deduplicates to one bus per component.
            buses = static[bus_column]
            if isinstance(buses.index, pd.MultiIndex):
                buses = buses.droplevel(SCENARIO)
                buses = buses[~buses.index.duplicated()]
            buses = buses[buses.astype(str) != ""]
            bus_df = buses.rename("bus").rename_axis("entity").reset_index()  # noqa: F841
            bus_rel = con.sql(  # noqa: F841 - bound by name for the join below
                "SELECT CAST(entity AS VARCHAR) entity, bus FROM bus_df"
            )
            # `v` already carries `attribute` = `stem`, so the join only adds
            # `bus`; an inner join drops a port whose bus is empty, as
            # `_per_port_long_rows` did by filtering non-empty buses.
            v = self._tagged_long_rel(  # noqa: F841 - bound by name for the query
                c, stem, column, drop_defaults=False
            )
            if v is None:
                continue
            rels.append(
                con.sql(
                    "SELECT v.*, b.bus FROM v JOIN bus_rel b ON v.entity = b.entity"
                )
            )
        return rels

    def _output_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """One result attribute's long rows, across every type that has it.

        `drop_defaults` matters here as it does in `results`: a static output
        carries its default whether or not the network was solved, and an
        absent value means exactly "take the default" to a reader.

        Notes
        -----
        - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
        """
        # No `bus`: a result comes from a per-entity container, never a per-port
        # one - PyPSA keeps `Link.p0` and `p1` as separate result attributes
        # where it collapses the *input* `efficiency`/`efficiency2` to one over
        # the connection group. So there is no port to key a result by.
        columns = ["entity", *self._DIMS, "attribute", "breakpoint", "value"]
        return self._union_select(self._output_rels(attribute, ctypes), columns)

    def _output_rels(self, attribute: str, ctypes: list[str]) -> list[DuckDBPyRelation]:
        """One result attribute's per-type long relations, empties dropped."""
        rels = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            try:
                rel = self._tagged_long_rel(c, attribute, attribute, drop_defaults=True)
            except (AttributeError, KeyError):
                # Not every registry output has a container on every network.
                continue
            if rel is not None:
                rels.append(rel)
        return rels

    def _union_rel(
        self, rels: list[DuckDBPyRelation], columns: list[str]
    ) -> DuckDBPyRelation | None:
        """Union `rels` and project `columns`; `None` where none contribute."""
        if not rels:
            return None
        rel = rels[0]
        for other in rels[1:]:
            rel = rel.union(other)
        return rel.select(*(col(name) for name in columns))

    def _union_select(
        self, rels: list[DuckDBPyRelation], columns: list[str]
    ) -> nw.LazyFrame:
        """Union `rels`, project `columns`, as a lazy narwhals frame.

        An empty list is an empty frame with the right columns; `write_layer`
        casts every column to its declared type, so a column pandas would type as
        float from all-NULLs still lands right.
        """
        rel = self._union_rel(rels, columns)
        if rel is None:
            return nw.from_native(pd.DataFrame(columns=columns)).lazy()
        return nw.from_native(rel).lazy()


def _required_attributes(ctype: str) -> frozenset[str]:
    """`ctype`'s mandatory input attributes, from PyPSA's registry.

    `name` is the component's identity, always supplied by the record's member
    list rather than an attribute row, so it never counts as missing.
    """
    from pypsa.components.types import get as get_component_type

    defaults = get_component_type(ctype).defaults
    required = defaults.index[defaults["status"].astype(str) == "Input (required)"]
    return frozenset(required) - {"name"}


def _ordered_connections(record: RecordLike, ctype: str) -> pd.DataFrame | None:
    """One type's connections with a port index assigned per component.

    The positional collapse: connections come in member order (an
    overlay's `order_key`: first-introduced) and are numbered within each
    component, so a port index follows the order connections were introduced
    and a patch that adds one appends rather than renumbering. Inputs are
    placed before outputs, so `bus0` is the input end PyPSA's sign convention
    expects.

    Scoped to `ctype` by that type's entities, one frame holding every type's
    connections.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [one record over one fold](https://energy-models.github.io/datarecord/design/read-path/#one-record-over-one-fold)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    frame = record.groups.get(CONNECTION)
    members = record.entity_types.get(ctype)
    if frame is None or members is None:
        return None
    df = frame.collect(backend="pandas").to_native()
    mine = set(members.select("entity").collect(backend="pandas").to_native()["entity"])
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


def _connection_attributes(record: RecordLike, ctype: str) -> frozenset[str]:
    """Port attribute names this type's connection rows can supply.

    `bus0`/`bus1`/... for the ports that actually exist, so `verify` knows a
    required `bus0` is satisfied by a connection rather than by a column.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    df = _ordered_connections(record, ctype)
    if df is None:
        return frozenset()
    return frozenset(_port_attribute("bus", port) for port in df["port"].unique())


def _collapse_connections(
    static: DuckDBPyRelation, record: RecordLike, ctype: str, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Add `bus0`/`bus1`/... columns to a static frame from its connection rows.

    PyPSA wants a column per port; the record stores a row per connection. This is the seam between them, and the only direction that needs
    positions at all.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    df = _ordered_connections(record, ctype)
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


def _static_columns(record: RecordLike, ctype: str) -> frozenset[str]:
    """Columns `dims/entity_type/<ctype>.parquet` supplies for this record.

    The non-varying half of the static frame: an attribute present here
    needs no `inputs/` row to be resolvable.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    """
    frame = record.entity_types.get(ctype)
    return (
        frozenset(frame.collect_schema().names()) if frame is not None else frozenset()
    )


# The tool is a module-level singleton, imported rather than looked up by
# name: `from datarecord.tools.pypsa import PyPSA` then `PyPSA.build(revision.record)`.
PyPSA = PyPSATool()
