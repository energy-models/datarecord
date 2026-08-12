"""The PyPSA tool: record -> `pypsa.Network` -> results (design doc §12).

The only module that knows PyPSA's network shape - that its axes are
`snapshot`/`period`/`scenario`, that a stochastic network is indexed by
`(scenario, name)`, and how its static/series split maps onto the store's
`dims/components` + `inputs/` split (§3.1).
"""

from __future__ import annotations

import math
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property, reduce
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
from datarecord.schema import AttributeSpec, Dimension
from datarecord.schema import Schema as RecordSchema
from datarecord.record import Flags, Frames, LazyFrames, Record
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
# schema's `dimensions`, since a network shape is built from them (§12).
# A declared axis with no rows is fine - that is just a deterministic or
# single-period network.
SNAPSHOT, PERIOD, SCENARIO = "snapshot", "period", "scenario"
REQUIRED_DIMS = frozenset({SNAPSHOT, PERIOD, SCENARIO})

# PyPSA's `defaults["type"]` vocabulary, mapped to the DuckDB types §3.2
# stores. `series` and `static or series` describe *where* a value lives, not
# what it is - both are floats in a record's `value` column.
_DTYPES = {
    "boolean": "BOOLEAN",
    "float": "DOUBLE",
    "int": "BIGINT",
    "string": "VARCHAR",
    "geometry": "VARCHAR",
    "series": "DOUBLE",
    "static": "DOUBLE",
    "static or series": "DOUBLE",
}


def _default(value: Any) -> Any:
    """One `defaults["default"]` cell as JSON-storable, NaN as absent (§5.2)."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _text(value: Any) -> str | None:
    """One `unit`/`description` cell as text, or None where PyPSA has none (§5.8).

    Its registry leaves both NaN rather than empty, and `None` is the schema's
    "undeclared" - `""` would claim the attribute is documented as blank, or
    dimensionless, neither of which a missing cell says.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class NetworkShape:
    """PyPSA's reading of a store's axis frames (§12).

    The axis names and the index convention live here rather than on the
    store, which stays schema-generic: a store may declare any dims, and it is
    this tool that decides `snapshot` is the time axis and that a non-empty
    `scenario` axis means a stochastic network.
    """

    dims: Frames

    def _axis(self, dim: str) -> pd.DataFrame:
        """`dim`'s axis as a DataFrame, empty if the store declares none.

        A dim with no rows anywhere is absent from the mapping rather than
        present-and-empty (§4.2), and both read the same way here: an empty
        frame, which is a deterministic or single-period network.
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
        """The static frame's index: PyPSA carries `scenario` as a level (§12)."""
        return [SCENARIO, "name"] if self.stochastic else ["name"]


def _connection(store: Record) -> DuckDBPyConnection:
    """The DuckDB connection `store`'s frames belong to.

    Off the concrete backing, since the protocol stays backend-agnostic (§4.4).
    Needed because a relation exposes no reachable reference to its connection,
    and this tool's `PIVOT` - which narwhals cannot express and
    `relation.query` refuses as a `MULTI` statement - needs one.

    Raises
    ------
    TypeError
        If `store` exposes no connection, i.e. is not DuckDB-backed.
    """
    con = getattr(store, "con", None)
    if not isinstance(con, DuckDBPyConnection):
        msg = (
            f"{type(store).__name__} exposes no DuckDB connection; "
            "the PyPSA tool builds with DuckDB SQL"
        )
        raise TypeError(msg)
    return con


# -- long -> wide -----------------------------------------------------------

# Separates pivoted index-level values in a combined PIVOT column name; chosen
# to never collide with a component/scenario name (§3 decode rule).
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

    joined = static
    attr_exprs = {}
    for attr, (long, flags) in attributes.items():
        # PyPSA's `static` container takes the snapshot-broadcast rows (§8.1).
        if SNAPSHOT not in flags.broadcast:
            continue
        rows = long.filter("snapshot IS NULL")
        if shape.stochastic:
            rows = scenarios.expand(rows, SCENARIO)
        # First-wins on a duplicate key, same as `values[~values.index.duplicated()]`;
        # any deterministic order is fine since a valid store never actually collides.
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
    return (
        wide.order("order_key")
        .project(star(exclude=["order_key"]))
        .df()
        .set_index(keys)
    )


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
    `dynamic` instead of leaving it static (§12).
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


# -- build (§12) -------------------------------------------------------------


def _exported(c) -> bool:
    """Whether `c`'s members belong in a store at all.

    Empty types have nothing to write. Standard types (`LineType`,
    `TransformerType`) are excluded because PyPSA prepopulates them on every
    fresh `Network` and `_broadcast_standard_types` restores them on build -
    writing them would import each row a second time on the way back.
    """
    return not c.static.empty and c.name not in c.n.standard_type_components


# TODO(pypsa): every function below, up to `_new_network`, ports a method that
# only exists on PyPSA's unreleased data-records branch (§12). Delete the
# ported copy and call the real method once a PyPSA release carries it.


def _apply_snapshots_import(n: pypsa.Network, df: pd.DataFrame) -> None:
    """Set snapshots and snapshot weightings from an imported axis table.

    Ported from `pypsa.Network._apply_snapshots_import` (unreleased, §12) -
    built entirely from public surface (`set_snapshots`, `snapshot_weightings`).
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

    Ported from `pypsa.Network._broadcast_standard_types` (unreleased, §12).
    """
    for component in n.standard_type_components:
        comp = n.components[component]
        if n.has_scenarios and not isinstance(comp.static.index, pd.MultiIndex):
            comp.static = pd.concat(
                dict.fromkeys(n.scenarios, comp.static), names=["scenario"]
            )


def _collect_network_attributes(n: pypsa.Network) -> dict[str, Any]:
    """The serializable scalar network attributes (incl. `name`, `pypsa_version`).

    Ported from `pypsa.Network._collect_network_attributes` (unreleased, §12),
    trimmed of nothing - the reflection over `dir(n)` is exactly what decides
    which attributes are safe to round-trip, so there is no smaller version of
    this that still answers the same question.
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
    """Apply scalar network attributes read back from a store.

    Ported from `pypsa.Network._apply_network_attributes` (unreleased, §12),
    stripped of the version-compat warnings and PyPI update check the real
    method also does: those are for a human importing a file written by an
    older PyPSA, and have nothing to check here since `to_datarecord` and this
    function always agree on what `attrs` holds.
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
    """An empty network with the store's axes already established."""
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
        n.set_scenarios(scen["weight"])
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
        # Partial columns keep non-series components static (§12).
        wide = _series_frame(
            ts_rows, shape, periods, scenarios, snapshots, static_index, con
        )
        n._import_series_from_df(wide, ctype, attr)


# -- results (§9.4) ---------------------------------------------------------


def _output_attributes(c: pypsa.Components) -> list[str]:
    """A component type's result attributes, from PyPSA's own registry.

    `defaults["status"]` marks them ("Output"), so a PyPSA upgrade that adds a
    result attribute is picked up rather than needing a list kept here.
    """
    defaults = c.defaults
    is_output = defaults["status"].astype(str).str.startswith("Output")
    return list(defaults.index[is_output])


# -- network -> layer (§4) ------------------------------------------------

# Which end of the component a port is. PyPSA encodes this only by sign
# convention - `p0` flows in at `bus0`, `p1` out at `bus1` - so the mapping to
# a record's `role` lives here, the one place that convention is written down
# (§6). Ports beyond the first two are outputs: a multi-port Link's `bus2`
# onward are additional sinks.
_INPUT_PORT, _OUTPUT_PORT, _SINGLE_PORT = "input", "output", "attached"

# Attribute stems that exist once per port. A record stores each as one
# bus-keyed attribute (§6), so these are the names whose port suffix is
# undone on write and reapplied on build.
_PORT_STEMS = ("bus", "efficiency", "p")


def _port_role(port: str) -> str:
    """The `role` for a port index (§6).

    A single-port component (`c.ports == [""]`, e.g. a Generator's one `bus`)
    is neither an input nor an output end - it has only one attachment - so it
    gets its own role rather than being forced into the two-ended convention.
    """
    if port == "":
        return _SINGLE_PORT
    return _INPUT_PORT if port == "0" else _OUTPUT_PORT


def _port_attribute(stem: str, port: str) -> str:
    """PyPSA's name for `stem` at `port`: `bus0`, `efficiency`, `efficiency2`, ...

    `bus` is suffixed from `0`, every other per-port attribute from `2` - the
    quirk that makes the port vocabulary unguessable from a registry and is why
    a record keys connections by bus instead (§6).
    """
    if stem == "bus":
        return f"bus{port}"
    return stem if port == "1" else f"{stem}{port}"


def _connection_rows(c: pypsa.Components) -> pd.DataFrame:
    """One type's connections as `(name, bus, role)` rows, from its port columns (§6).

    Undoes PyPSA's positional encoding: `c.ports` is the authoritative port
    list (`["0", "1"]` plus `additional_ports`), so the port count comes from
    the network rather than a guessed bound.
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
        rows["role"] = _port_role(port)
        rows["deleted"] = False
        frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["name", "bus", "role", "deleted"])
    return pd.concat(frames, ignore_index=True)


# TODO(pypsa): every function below, up to `_as_long`, ports a function that
# only exists on PyPSA's unreleased data-records branch (§12). Delete the
# ported copy and call the real one once a PyPSA release carries it.


def _is_output(defaults: pd.DataFrame, attr: str) -> bool:
    """Whether `attr` is an output (custom attrs with no status are inputs).

    Ported from `pypsa.common._is_output` rather than depended on, since only
    a released PyPSA can be relied on here (no `_as_long` yet, §12).
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
    wide = wide.rename_axis(columns=["scenario", "name"] if stochastic else "name")

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
    out = static.rename("value").reset_index()
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
    carries this method (§12), and a tool may not require one. The reshaping
    is otherwise built entirely from public surface - `c.dynamic`, `c.static`,
    `c.defaults`, `c.has_scenarios`, `c.snapshots` - so porting it costs
    nothing PyPSA doesn't already expose.
    """
    varying = attr in c.dynamic or (
        attr in c.defaults.index and bool(c.defaults.at[attr, "varying"])
    )
    if not varying and not _is_output(c.defaults, attr):
        msg = f"'{attr}' of '{c.name}' components has no long representation."
        raise AttributeError(msg)

    columns = ["name", "snapshot", "scenario", "period", "value"]
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
    c: pypsa.Components, attribute: str, dims: tuple[str, ...]
) -> pd.DataFrame:
    """One attribute's long rows for one component type, in the §3 schema.

    `_as_long` already emits the dim columns and `value`; this adds the
    columns the record's schema fixes and PyPSA has no notion of -
    `component_type`, `attribute`, and the NULL `bus`/`breakpoint` that mark
    an attribute as the component's own and a scalar (§6, §7).
    """
    long = _as_long(c, attribute, drop_defaults=False)
    long = long.assign(
        component_type=c.name, attribute=attribute, bus=None, breakpoint=None
    )
    for dim in dims:
        if dim not in long.columns:
            long[dim] = None
    return long


def _per_port_long_rows(
    c: pypsa.Components, stem: str, dims: tuple[str, ...]
) -> pd.DataFrame:
    """A per-connection attribute's long rows, `bus` filled from the port (§6).

    The inverse of the positional collapse: `efficiency`/`efficiency2` become
    one attribute whose rows carry the bus each port attaches to, so the
    overlay owns them per connection rather than per component.
    """
    static = c.static
    frames = []
    for port in c.ports:
        column = _port_attribute(stem, port)
        bus_column = _port_attribute("bus", port)
        if column not in c.defaults.index or bus_column not in static.columns:
            continue
        long = _long_rows(c, column, dims)
        if long.empty:
            continue
        long["bus"] = long["name"].map(static[bus_column])
        long["attribute"] = stem
        frames.append(long[long["bus"].astype(str) != ""])
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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

    def requires(self, store: Record) -> Requirements:
        """PyPSA's axes, the store's own component types, and their required attributes.

        The attribute half is store-dependent: only types the store
        actually defines members for are required, and which of their
        attributes are mandatory comes from PyPSA's registry
        (`defaults["status"] == "Input (required)"`), not a list kept here.
        Reported by source attribute (`Schema.sources`), so a renamed or
        computed one names what the store must actually supply.
        """
        known = self.component_types()
        ctypes = {ct for ct in store.components if ct in known}
        return Requirements(
            dims=REQUIRED_DIMS,
            component_types=frozenset(ctypes),
            attributes=frozenset(
                (ct, src)
                for ct in ctypes
                for attr in _required_attributes(ct)
                for src in self.schema.sources(ct, attr)
            ),
        )

    def verify(self, store: Record) -> Requirements:
        """What this store fails to supply for a PyPSA build; falsy if it is usable.

        Checks what a build needs: the schema declares PyPSA's axes
        (§12), its key dims are ones this tool can honour, every component
        type it names is one PyPSA knows, and each type's required attributes
        are resolvable - either owned by some layer (the owner map), supplied
        by the `dims/components` frame, or carrying a declared default
        (§5.2). A renamed or computed attribute is checked against its sources
        (`Schema.sources`), which is also how it is reported.

        The key checks come first and return early: the owner-map fold keys
        its relations by those dims, so reading the map at all could raise a
        binder error before anything below could report it.
        """
        dims = REQUIRED_DIMS - set(store.schema.dims)
        unsupported_keys = self._unsupported_keys(store)
        if unsupported_keys:
            return Requirements(
                dims=frozenset(dims), unsupported_keys=frozenset(unsupported_keys)
            )

        component_types: set[str] = set()
        attributes: set[tuple[str, str]] = set()
        unsupported_values: set[tuple[str, str]] = set()

        known = self.component_types()
        declared = store.schema.attributes
        for ctype in sorted(store.components):
            # A type PyPSA has no registry entry for. Reported rather than
            # raised: the record layer stores `component_type` as a plain
            # string, so an unknown type reads back fine and it is this
            # tool's business that it cannot be built.
            if ctype not in known:
                component_types.add(ctype)
                continue
            resolved = store.flags(ctype)
            owned = set(resolved)
            # A curve, not a scalar (§7). PyPSA takes a scalar for every
            # attribute this build assigns, so the record is storing something
            # correct that this translation cannot express - reported here
            # rather than silently pivoting one arbitrary breakpoint.
            unsupported_values |= {
                (ctype, attr) for attr, flags in resolved.items() if flags.breakpoints
            }
            static_cols = _static_columns(store, ctype)
            # A port attribute the record supplies as connection rows rather
            # than as a column: `bus0`/`bus1` are satisfied by a connection
            # per port, so the collapse in `build` can name them (§6).
            from_connections = _connection_attributes(store, ctype)
            specs = declared.get(ctype) or {}
            for attr in _required_attributes(ctype):
                for src in self.schema.sources(ctype, attr):
                    if src in owned or src in static_cols or src in from_connections:
                        continue
                    # A declared default makes the attribute resolvable even
                    # with no row anywhere (§3.3 decode rule).
                    spec = specs.get(src)
                    if spec is not None and spec.default is not None:
                        continue
                    attributes.add((ctype, src))
        return Requirements(
            dims=frozenset(dims),
            component_types=frozenset(component_types),
            attributes=frozenset(attributes),
            unsupported_values=frozenset(unsupported_values),
        )

    def _unsupported_keys(self, store: Record) -> set[tuple[str, str]]:
        """`(key, dim)` pairs this tool cannot honour: `snapshot`, as any key.

        PyPSA's static/series split needs a component's whole series to come
        from one layer, so a snapshot-keyed overlay could leave a broadcast row
        and a descendant's per-snapshot row with no single container to land in
        (§5.5). A limit of the *representation*, not the format - which is why
        the record layer permits the declaration and this reports it (§12).
        """
        defs = store.schema
        kinds = (
            ("input_key", defs.input_dims),
            ("component_key", defs.component_dims),
            ("connection_key", defs.connection_dims),
        )
        return {(key, SNAPSHOT) for key, dims in kinds if SNAPSHOT in dims}

    def build(self, store: Record) -> pypsa.Network:
        """The resolved network, one component type at a time (§12, §12).

        Raises
        ------
        UnsupportedRecordError
            If `verify` reports anything missing - a partial build would
            surface as a confusing PyPSA error several frames later.
        """
        missing = self.verify(store)
        if missing:
            raise UnsupportedRecordError(self.name, missing)

        con = _connection(store)
        schema = store.schema
        shape = NetworkShape(store.dims)
        n = _new_network(schema, shape)

        for ctype in sorted(store.components):
            static = to_relation(store.components[ctype])
            if not shape.stochastic and SCENARIO in static.columns:
                static = static.project(star(exclude=[SCENARIO]))
            # Connections back to the positional columns PyPSA expects (§12).
            static = _collapse_connections(static, store, ctype, con)

            # Frames are built and released per type, so peak memory is one
            # type's wide frames rather than the whole network (§12).
            attributes = {}
            for attr, flags in store.flags(ctype).items():
                if attr not in schema.attributes.get(ctype, {}):
                    continue
                # Neither container would take it: no rows on the snapshot axis
                # either way (an attribute with no rows at all is absent from
                # the map, so this is the both-sets-empty case).
                if not (flags.varies | flags.broadcast):
                    continue
                # Through the schema, so a renamed or computed attribute
                # reaches the pivot below as an ordinary long relation.
                long = self.schema.resolve(store, ctype, attr).filter(
                    col("component_type") == lit(ctype)
                )
                attributes[attr] = (long, flags)

            _add_component_type(n, ctype, static, attributes, shape, con)

        _broadcast_standard_types(n)
        return n

    def results(self, model: pypsa.Network) -> dict[tuple[str, str], nw.DataFrame]:
        """A solved network's result attributes in the store's long form (§9.4).

        Keyed by `(component_type, attribute)`, as narwhals frames so the return
        type names no one dataframe library. Which attributes count as results
        comes from PyPSA's registry, so an upgrade that adds one is picked up.

        Rows still at the attribute's default are dropped: a static output like
        `Bus.control` carries its default whether or not the network was solved,
        and an absent output file means exactly "take the default" (§3.3).
        """
        out: dict[tuple[str, str], nw.DataFrame] = {}
        for c in model.components:
            if not _exported(c):
                continue
            for attr in _output_attributes(c):
                try:
                    long = _as_long(c, attr, drop_defaults=True)
                except (AttributeError, KeyError):
                    # Not every registry output attribute has a container on
                    # every network (e.g. duals absent unless assigned).
                    continue
                if long.empty:
                    continue
                out[c.name, attr] = nw.from_native(long, eager_only=True).with_columns(
                    component_type=nw.lit(c.name), attribute=nw.lit(attr)
                )
        return out

    def to_datarecord(self, model: pypsa.Network) -> Record:
        """Present a `Network` as the `Record` `write_record` persists (§10, §12).

        The inverse of `build`, and the only place PyPSA's shape is undone:
        `c.static`/`c.dynamic` become long rows, and `bus0`/`bus1`/`efficiency2`
        become connection rows carrying a `role` (§6). Key sets are read off the
        network, so listing unpivots nothing and a lookup only what is asked for.
        """
        return _NetworkSource(model)


@dataclass(frozen=True)
class _NetworkSource:
    """A `pypsa.Network` presented as a `Record` (§4).

    Every `LazyFrames` here is built from key sets read off the network and
    its registry, so nothing is unpivoted until a key is looked up.
    """

    n: pypsa.Network

    # The dims a PyPSA network has axes for; `Dims` stays generic, this tool
    # decides these three are what a network is shaped by (§12).
    _DIMS = (SNAPSHOT, PERIOD, SCENARIO)

    @property
    def schema(self) -> RecordSchema:
        """The layer's schema, derived from PyPSA's own registry (§5).

        `c.defaults` already declares what §5.2 asks for, so this reads it
        rather than restating it. Every stored attribute, not only the varying
        ones - `dims=frozenset()` is what puts one in `dims/components/` (§3.1).
        Results are excluded, belonging to `outputs/` (§9.4).
        """
        attributes: dict[str, dict[str, AttributeSpec]] = {}
        for c in self.n.components:
            if not _exported(c):
                continue
            defaults = c.defaults
            # Per-port columns collapse to their stem, which a record keys by
            # bus rather than by position (§6); `efficiency2` and `efficiency`
            # agree on every declaration, so either row answers.
            per_port = self._port_stems(c)
            outputs = set(_output_attributes(c))
            entries = {}
            for attr in defaults.index:
                if attr in outputs or attr == "name":
                    continue
                stem = per_port.get(attr, attr)
                if stem in entries:
                    continue
                row = defaults.loc[attr]
                entries[stem] = AttributeSpec(
                    dtype=_DTYPES.get(row["type"], "VARCHAR"),
                    dims=frozenset({SNAPSHOT}) if row["varying"] else frozenset(),
                    default=_default(row["default"]),
                    bus="connection" if attr in per_port else "component",
                    unit=_text(row.get("unit")),
                    description=_text(row.get("description")),
                )
            if entries:
                attributes[c.name] = entries
        return RecordSchema(
            dimensions={
                SNAPSHOT: Dimension(
                    dtype="TIMESTAMP",
                    description="A point in the operational time series.",
                ),
                PERIOD: Dimension(
                    dtype="BIGINT",
                    unit="year",
                    description="An investment period, labelled by its year.",
                ),
                # `scenario` keys both entity tables: a network's components
                # are the same across scenarios, but a layer may still patch
                # one scenario's values (§5.3).
                SCENARIO: Dimension(
                    dtype="VARCHAR",
                    keys=frozenset({"component", "connection"}),
                    description="One realisation of a stochastic problem.",
                ),
            },
            attributes=attributes,
            partial=frozenset({SCENARIO}),
            meta={
                "format": "pypsa-parquet",
                "attributes": _collect_network_attributes(self.n),
                "meta": dict(self.n.meta),
            },
        )

    @property
    def dims(self) -> LazyFrames:
        axes = {
            SNAPSHOT: lambda: self.n.snapshots.to_frame(index=False),
            PERIOD: lambda: self.n.investment_periods.to_frame(index=False),
            # With its weights: an axis frame carries the whole row, not just
            # the key column (§3.4), and `scenarios` alone is a bare Index.
            SCENARIO: lambda: self.n.scenario_weightings.reset_index(),
        }
        present = tuple(d for d in self._DIMS if len(axes[d]()) > 0)
        return LazyFrames(present, lambda dim: nw.from_native(axes[dim]()).lazy())

    @property
    def components(self) -> LazyFrames:
        types = tuple(c.name for c in self.n.components if _exported(c))
        return LazyFrames(types, self._component_frame)

    @property
    def connections(self) -> LazyFrames:
        # Every type with any port, single-attachment ones included: a
        # Generator's one `bus` is as much a connection as a Link's `bus0`,
        # and `c.ports == [""]` makes `_port_attribute` name it correctly.
        types = tuple(c.name for c in self.n.components if _exported(c) and c.ports)
        return LazyFrames(types, self._connection_frame)

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
        """Result attributes, keyed by name, from PyPSA's own registry (§9.4).

        Keys are named per type by the registry and merged, matching the file
        layout - one `outputs/<attr>.parquet` across types, like `inputs/`.
        A network that was never solved still names them; the frame is then
        empty and `write_record` writes an empty file rather than none, which
        reads back as "take the default" either way (§12).
        """
        names: dict[str, list[str]] = {}
        for c in self.n.components:
            if not _exported(c):
                continue
            for attr in _output_attributes(c):
                names.setdefault(attr, []).append(c.name)
        return LazyFrames(
            tuple(names), lambda attr: self._output_frame(attr, names[attr])
        )

    def flags(self, ctype: str) -> dict[str, Flags]:
        """Never consulted: `write_record` persists frames, not flags (§10).

        A network-backed source exists to be written, and the write path reads
        only the frame mappings. Answering properly is easy - `c.static` and
        `c.dynamic` are PyPSA's own split on the snapshot axis, so it needs no
        scan - but an implementation no caller reaches is one no test pins, so
        the honest answer is the empty one, as `mutable._Written` gives.
        """
        return {}

    # -- key sets -----------------------------------------------------------

    def _record_attributes(self, c: pypsa.Components) -> list[str]:
        """`c`'s *varying* input attributes, in the record's vocabulary (§3.1).

        Only varying attributes go to `inputs/`, and PyPSA agrees: a non-varying
        one has no `_as_long` representation. Per-port attributes collapse to
        their stem (`efficiency2` -> `efficiency`), a record keying them by bus
        rather than position (§6). Outputs belong to `outputs/` (§9.4).
        """
        defaults = c.defaults
        outputs = set(_output_attributes(c))
        per_port = self._port_stems(c)
        stems: list[str] = []
        for attr in defaults.index:
            if attr in outputs or attr == "name" or not defaults.loc[attr, "varying"]:
                continue
            stem = per_port.get(attr, attr)
            if stem not in stems:
                stems.append(stem)
        return stems

    def _port_stems(self, c: pypsa.Components) -> dict[str, str]:
        """Per-port attribute name -> its stem, for this type's actual ports."""
        mapping = {}
        for stem in _PORT_STEMS:
            for port in c.ports:
                name = _port_attribute(stem, port)
                if name in c.defaults.index:
                    mapping[name] = stem
        return mapping

    # -- frames -------------------------------------------------------------

    def _component_frame(self, ctype: str) -> nw.LazyFrame:
        """Wide members of one type: the non-varying, non-port static columns (§3)."""
        c = self.n.c[ctype]
        defaults = c.defaults
        per_port = set(self._port_stems(c))

        def keep(column: str) -> bool:
            if column in per_port:
                return False  # a connection supplies it (§6)
            if column not in defaults.index:
                # A custom column the registry has no entry for (e.g.
                # `ac_dc_meshed`'s `Bus.country`). Non-varying by construction -
                # a varying attribute needs a registry entry to have a
                # container - so it belongs in the static frame, and dropping
                # it would silently lose data §3 says to keep.
                return True
            return (
                not defaults.loc[column, "varying"]
                and defaults.loc[column, "status"] != "Output"
            )

        frame = c.static[[x for x in c.static.columns if keep(x)]].reset_index()
        return nw.from_native(self._tagged(frame, ctype)).lazy()

    def _connection_frame(self, ctype: str) -> nw.LazyFrame:
        """One type's connections, `(name, bus, role)` (§6)."""
        frame = _connection_rows(self.n.c[ctype])
        return nw.from_native(self._tagged(frame, ctype)).lazy()

    @staticmethod
    def _tagged(frame: pd.DataFrame, ctype: str) -> pd.DataFrame:
        """A `dims/` frame with the columns the fold keys and scopes by (§9.1).

        `component_type` because one owner map covers every type; `deleted`
        because the fold reads the tombstone column from the same file (§8.3);
        `scenario` because it is a declared key dim, NULL meaning "every
        scenario" for a deterministic network (§5.5).
        """
        frame = frame.assign(component_type=ctype)
        if "deleted" not in frame.columns:
            frame["deleted"] = False
        if SCENARIO not in frame.columns:
            frame[SCENARIO] = None
        return frame

    def _long_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """One attribute's long rows, across every type that has it (§3).

        A per-connection attribute contributes rows carrying the bus each port
        attaches to; a component-level one contributes rows with `bus` NULL.
        """
        dims = self._DIMS
        frames = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            if attribute in self._port_stems(c).values():
                rows = _per_port_long_rows(c, attribute, dims)
            elif attribute in c.defaults.index:
                rows = _long_rows(c, attribute, dims)
            else:
                continue
            if not rows.empty:
                frames.append(rows)
        columns = [
            "component_type",
            "name",
            "bus",
            *dims,
            "attribute",
            "breakpoint",
            "value",
        ]
        if not frames:
            return nw.from_native(pd.DataFrame(columns=columns)).lazy()
        # No dtype fixing here: `write_record` casts every schema column to the
        # record layer's declared type on the way out (§5), so an all-NULL
        # column pandas typed as float lands as the right type regardless.
        return nw.from_native(pd.concat(frames, ignore_index=True)[columns]).lazy()

    def _output_frame(self, attribute: str, ctypes: list[str]) -> nw.LazyFrame:
        """One result attribute's long rows, across every type that has it (§9.4).

        `drop_defaults` matters here as it does in `results`: a static output
        carries its default whether or not the network was solved, and an
        absent value means exactly "take the default" to a reader.
        """
        dims = self._DIMS
        frames = []
        for ctype in ctypes:
            c = self.n.c[ctype]
            try:
                long = _as_long(c, attribute, drop_defaults=True)
            except (AttributeError, KeyError):
                # Not every registry output has a container on every network.
                continue
            if long.empty:
                continue
            long = long.assign(
                component_type=ctype, attribute=attribute, bus=None, breakpoint=None
            )
            for dim in dims:
                if dim not in long.columns:
                    long[dim] = None
            frames.append(long)
        columns = [
            "component_type",
            "name",
            "bus",
            *dims,
            "attribute",
            "breakpoint",
            "value",
        ]
        if not frames:
            return nw.from_native(pd.DataFrame(columns=columns)).lazy()
        return nw.from_native(pd.concat(frames, ignore_index=True)[columns]).lazy()


def _required_attributes(ctype: str) -> frozenset[str]:
    """`ctype`'s mandatory input attributes, from PyPSA's registry.

    `name` is the component's identity, always supplied by the store's member
    list rather than an attribute row, so it never counts as missing.
    """
    from pypsa.components.types import get as get_component_type

    defaults = get_component_type(ctype).defaults
    required = defaults.index[defaults["status"].astype(str) == "Input (required)"]
    return frozenset(required) - {"name"}


def _ordered_connections(store: Record, ctype: str) -> pd.DataFrame | None:
    """One type's connections with a port index assigned per component (§12).

    The positional collapse: connections come in member order (§9.3, an
    overlay's `order_key`: first-introduced, §9.1) and are numbered within each
    component, so a port index follows the order connections were introduced
    and a patch that adds one appends rather than renumbering. Inputs are
    placed before outputs, so `bus0` is the input end PyPSA's sign convention
    expects.
    """
    frame = store.connections.get(ctype)
    if frame is None:
        return None
    df = frame.to_native().df()
    if df.empty:
        return None
    # Per component *and* per value of every dim the connections map is keyed
    # by (§5.3): a scenario-keyed store holds one row per scenario per port,
    # and numbering across them would give one port as many indices as there
    # are scenarios.
    within = ["name", *(d for d in store.schema.connection_dims if d in df)]
    if "role" in df.columns:
        df = df.assign(_role_rank=(df["role"] != _INPUT_PORT).astype(int)).sort_values(
            [*within, "_role_rank"], kind="stable"
        )
    # A single-attachment component keeps PyPSA's unsuffixed `bus`, so its port
    # is the empty index rather than `0` (`_port_attribute`).
    # `dropna=False`: a dim left NULL means "every value of it" (§3.3), which
    # is an ordinary group here, not a row to drop.
    grouped = df.groupby(within, dropna=False)
    single = grouped["bus"].transform("size").eq(1) & df["role"].eq(_SINGLE_PORT)
    df["port"] = grouped.cumcount().astype(str).where(~single, "")
    return df


def _connection_attributes(store: Record, ctype: str) -> frozenset[str]:
    """Port attribute names this type's connection rows can supply (§6).

    `bus0`/`bus1`/... for the ports that actually exist, so `verify` knows a
    required `bus0` is satisfied by a connection rather than by a column.
    """
    df = _ordered_connections(store, ctype)
    if df is None:
        return frozenset()
    return frozenset(_port_attribute("bus", port) for port in df["port"].unique())


def _collapse_connections(
    static: DuckDBPyRelation, store: Record, ctype: str, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """Add `bus0`/`bus1`/... columns to a static frame from its connection rows.

    PyPSA wants a column per port; the record stores a row per connection
    (§6). This is the seam between them, and the only direction that needs
    positions at all.
    """
    df = _ordered_connections(store, ctype)
    if df is None:
        return static
    # Pivoted on the same key the ports were numbered within, so each
    # (component, scenario, ...) keeps its own port columns.
    index = ["name", *(d for d in store.schema.connection_dims if d in df)]
    # A dim left NULL means "every value of it" (§3.3), so it is a real key
    # here - but `pivot` drops NaN index levels, so such a dim is folded away
    # instead: every row shares its value, and the join below matches on the
    # dims that actually distinguish rows.
    varying = [d for d in index[1:] if df[d].notna().any()]
    wide = df.pivot(index=["name", *varying], columns="port", values="bus")
    wide.columns = [_port_attribute("bus", port) for port in wide.columns]
    wide = wide.reset_index()  # noqa: F841 - queried by name below
    ports = con.sql("FROM wide").set_alias("ports")
    on = ["name", *(d for d in varying if d in static.columns)]
    joined = static.set_alias("s").join(
        ports, ex_all(col("s", c) == col("ports", c) for c in on), how="left"
    )
    return joined.project(
        *(col("s", c) for c in static.columns),
        *(col("ports", c) for c in ports.columns if c not in ("name", *varying)),
    )


def _static_columns(store: Record, ctype: str) -> frozenset[str]:
    """Columns `dims/components/<ctype>.parquet` supplies for this record.

    The non-varying half of the static frame (§3): an attribute present here
    needs no `inputs/` row to be resolvable.
    """
    frame = store.components.get(ctype)
    return (
        frozenset(frame.collect_schema().names()) if frame is not None else frozenset()
    )


# The tool is a module-level singleton, imported rather than looked up by
# name: `from datarecord.tools.pypsa import PyPSA` then `PyPSA.build(record.store)`.
PyPSA = PyPSATool()
