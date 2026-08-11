"""Hand-built patch layers, since the v2 write path does not exist (design doc §12)."""

from pathlib import Path

import pandas as pd

from datarecord.layered.resolve import write_schema as record_write_schema
from datarecord.schema import AttributeSpec, Dimension, Schema

LONG_COLUMNS = [
    "component_type",
    "name",
    "bus",
    "snapshot",
    "scenario",
    "period",
    "attribute",
    "breakpoint",
    "value",
]


def write_input(
    layer: str, attribute: str, rows: list[dict], *, snapshot_dtype="datetime64[ns]"
) -> None:
    """Write `inputs/<attribute>.parquet` in the long schema.

    Each row needs at least `component_type`, `name` and `value`; missing
    dimension columns default to NULL, i.e. "applies to the whole axis".
    `bus` set marks a per-connection attribute (design doc §6), `breakpoint`
    a piecewise-linear one (§7); both NULL is the ordinary component-level
    scalar.
    """
    df = pd.DataFrame(rows)
    df["attribute"] = attribute
    for col in LONG_COLUMNS:
        if col not in df:
            df[col] = None
    df["snapshot"] = pd.Series(df["snapshot"]).astype(snapshot_dtype)
    df["scenario"] = df["scenario"].astype("string")
    df["bus"] = df["bus"].astype("string")
    df["period"] = df["period"].astype("Int64")
    df["breakpoint"] = df["breakpoint"].astype("float64")
    df["value"] = df["value"].astype("float64")

    target = Path(layer, "inputs")
    target.mkdir(parents=True, exist_ok=True)
    df[LONG_COLUMNS].to_parquet(target / f"{attribute}.parquet", index=False)


def write_connections(layer: str, ctype: str, rows: list[dict]) -> None:
    """Write `dims/connections/<ctype>.parquet`, including the `deleted` tombstone (§6).

    Each row needs `name` and `bus`; `role` describes the connection and keys
    nothing, so it is optional here.
    """
    df = pd.DataFrame(rows)
    df["component_type"] = ctype
    for col in ("scenario", "role"):
        if col not in df:
            df[col] = None
        df[col] = df[col].astype("string")
    if "deleted" not in df:
        df["deleted"] = False
    df["deleted"] = df["deleted"].fillna(False).astype(bool)

    lead = ["component_type", "name", "bus", "role", "scenario", "deleted"]
    ordered = lead + [c for c in df.columns if c not in lead]
    target = Path(layer, "dims", "connections")
    target.mkdir(parents=True, exist_ok=True)
    df[ordered].to_parquet(target / f"{ctype}.parquet", index=False)


def tombstone_connection(
    layer: str, ctype: str, pairs: list[tuple[str, str]], scenario=None
) -> None:
    """Mark connections deleted in this layer, by `(name, bus)` (§6)."""
    write_connections(
        layer,
        ctype,
        [
            {"name": name, "bus": bus, "scenario": scenario, "deleted": True}
            for name, bus in pairs
        ],
    )


def write_components(layer: str, ctype: str, rows: list[dict]) -> None:
    """Write `dims/components/<ctype>.parquet`, including the `deleted` tombstone."""
    df = pd.DataFrame(rows)
    df["component_type"] = ctype
    if "scenario" not in df:
        df["scenario"] = None
    df["scenario"] = df["scenario"].astype("string")
    if "deleted" not in df:
        df["deleted"] = False
    df["deleted"] = df["deleted"].fillna(False).astype(bool)

    lead = ["component_type", "name", "scenario", "deleted"]
    ordered = lead + [c for c in df.columns if c not in lead]
    target = Path(layer, "dims", "components")
    target.mkdir(parents=True, exist_ok=True)
    df[ordered].to_parquet(target / f"{ctype}.parquet", index=False)


def tombstone(layer: str, ctype: str, names: list[str], scenario=None) -> None:
    """Mark components deleted in this layer (§8.3)."""
    write_components(
        layer,
        ctype,
        [{"name": n, "scenario": scenario, "deleted": True} for n in names],
    )


def write_scenarios(layer: str, rows: list[dict]) -> None:
    """Write `dims/scenarios.parquet`; each row needs `scenario` and `weight`."""
    df = pd.DataFrame(rows)
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "scenarios.parquet", index=False)


def write_periods(layer: str, rows: list[dict]) -> None:
    """Write `dims/periods.parquet`; each row needs `period`."""
    df = pd.DataFrame(rows)
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "periods.parquet", index=False)


def write_snapshots(layer: str, rows: list[dict]) -> None:
    """Write `dims/snapshots.parquet`; each row needs `snapshot`.

    A `period` column makes it a nested axis (§5.4), keyed by `(period,
    snapshot)` rather than by the timestamp alone.
    """
    df = pd.DataFrame(rows)
    df["snapshot"] = pd.Series(df["snapshot"]).astype("datetime64[ns]")
    if "period" in df:
        df["period"] = df["period"].astype("Int64")
    target = Path(layer, "dims")
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "snapshots.parquet", index=False)


def export_network(n, record, con) -> None:
    """Write `n` as `record`'s layer, through the record layer's own writer.

    Not `n.export_to_parquet`: that emits PyPSA's upstream manifest format,
    which is a different vocabulary from the schema a store declares (§5.6).
    Going through `write_layer` means a test store is written exactly as
    `blocks` writes one.
    """
    from datarecord.layered.write import write_layer
    from datarecord.tools.pypsa import PyPSA

    write_layer(record.id, PyPSA.to_datarecord(n), con)


def write_schema(schema: Schema, base_uri: str | None = None) -> None:
    """Declare the store's one schema, beside the layers (§5.6).

    Not per layer: a layer holds only data, so this writes the store-level
    `manifest.json` that every layer in the tree is read under.
    """
    record_write_schema(schema, base_uri)


def write_directory_schema(directory: str, schema: Schema) -> None:
    """Write `manifest.json` *inside* `directory`, for a standalone store (§5.6)."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    Path(directory, "manifest.json").write_text(schema.model_dump_json())


def schema(
    *,
    partial: set[str] = {"scenario"},
    keys: dict[str, set[str]] = {"scenario": {"component", "connection"}},
    attributes: dict[str, dict[str, AttributeSpec]] | None = None,
    dims: dict[str, str] = {
        "snapshot": "TIMESTAMP",
        "period": "BIGINT",
        "scenario": "VARCHAR",
    },
    within: dict[str, set[str]] | None = None,
) -> Schema:
    """A schema shaped like the PyPSA stores most tests build on (§5).

    Defaults match `PyPSA.to_datarecord`: three declared dims, `scenario`
    alone `partial` and keying both entity tables. Override `partial`/`keys`
    to pin a different layering granularity, `dims` to declare another axis,
    `within` to nest one axis inside another (§5.4).
    """
    nesting = within or {}
    return Schema(
        dimensions={
            d: Dimension(
                dtype=t,
                keys=frozenset(keys.get(d, set())),
                within=frozenset(nesting.get(d, set())),
            )
            for d, t in dims.items()
        },
        attributes=attributes or {},
        partial=frozenset(partial),
    )
