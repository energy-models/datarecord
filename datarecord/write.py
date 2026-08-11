"""Writing a layer from long-format frames (design doc §4).

Blocks writes layers itself rather than through `export_to_parquet`: the store
format is experimental, and the connection rows and `breakpoint` column of
§6/§7 are proposals for it (§2), so this is that proposal's reference
implementation.

Framework-independent, per §13's one-way dependency: a `Store` (`stores.py`)
hands over narwhals frames and this module turns them into parquet. Producing
one from a modelling framework's own object is a tool's job
(`datarecord.tools.pypsa.PyPSA.to_datarecord`).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

import narwhals as nw
from duckdb import DuckDBPyRelation

from datarecord.duck import base_uri_of, layer_dir
from datarecord.node_cache import read_schema, write_schema
from datarecord.schema import Schema
from datarecord.store import Solved, Store

if TYPE_CHECKING:
    import pypsa
    from duckdb import DuckDBPyConnection

_MSG = (
    "Writing a patch layer from two framework objects is not implemented, and "
    "is superseded by `MutableStore` (design doc §11), which captures edits as "
    "they happen rather than deriving them from a diff."
)

# The long schema's fixed columns (§3). `bus`/`breakpoint` are part of it, not
# optional extensions to it: both NULL is the ordinary component-level scalar.
_LONG_FIXED = ("component_type", "name", "bus", "attribute", "breakpoint", "value")


def write_layer(
    record_id: UUID | None,
    source: Store | Solved,
    con: DuckDBPyConnection,
    *,
    uri: str | None = None,
) -> None:
    """Write `source` as `record_id`'s layer, which must not exist yet (§4).

    Creates a new layer: an existing `layer_dir(record_id)` is an error rather
    than an overwrite or a merge, so a whole-layer write can never half-replace
    what a record already holds.

    Every frame goes through the same connection as reads, so remote writes
    reuse one credential path (§13). Keys are looked up one at a time and
    each file written before the next is built, so a source that reads per
    key does one read per file written rather than one per key up front -
    which for a remote source is one round trip each (§4).

    Parameters
    ----------
    record_id
        The record whose layer this is; `layer_dir` derives the path (§13).
        `None` only together with `uri`, for a standalone store that belongs
        to no record (§11.7).
    uri
        Write here instead of at `layer_dir(record_id)` - how a `Directory`
        commit target produces a store outside the layer tree (§11.7).
    source
        The layer's contents. Validated against its own schema before
        anything is written.
    con
        Connection to write through.

    Raises
    ------
    FileExistsError
        If the layer directory already exists.
    ValueError
        If a long frame is missing a §3 column, or the schema declares a key
        dim no frame carries - either would make the fold misresolve the layer.
    """
    if uri is None:
        if record_id is None:
            msg = "write_layer needs a record_id or a uri"
            raise ValueError(msg)
        base = layer_dir(record_id)
    else:
        base = uri if uri.endswith("/") else uri + "/"
    local = "://" not in base
    if local and Path(base).exists():
        msg = f"layer {base} already exists; write_layer creates a new layer (§4)"
        raise FileExistsError(msg)

    schema = source.schema
    if uri is None:
        # One schema for the whole tree (§5.6). The first layer written
        # declares it; every later one is checked against it, so a layer
        # cannot quietly redefine what an attribute means.
        _reconcile_schema(schema, con)

    # Staged, then moved into place: each frame is validated as it is built,
    # since building it twice would defeat the laziness (§4), so a frame the
    # fold could not resolve is only discovered part-way through. Writing aside
    # and renaming means such a failure leaves no layer rather than half of one.
    staging = f"{base.rstrip('/')}.staging/" if local else base
    if local:
        Path(staging).mkdir(parents=True)
    try:
        # A layer holds only data: a layered store's one schema lives beside
        # `layers/`, not inside any of them (§5.6). A standalone directory *is*
        # one store, so there the schema belongs in the directory.
        if local and uri is not None:
            with open(staging + "manifest.json", "w") as fh:
                fh.write(schema.model_dump_json())
        kinds = [
            ("dims", source.dims, "dims"),
            ("components", source.components, "dims/components"),
            ("connections", source.connections, "dims/connections"),
            ("attributes", source.attributes, "inputs"),
        ]
        # `outputs/` only for a source that carries results (§8): a store with
        # none produces a layer without the directory, rather than an empty one.
        if isinstance(source, Solved):
            kinds.append(("outputs", source.outputs, "outputs"))
        for kind, frames, subdir in kinds:
            for key in frames:
                # One key at a time, looked up exactly once: a lazy source
                # does not build `marginal_cost`'s frame to write this one.
                frame = frames[key]
                _validate_frame(frame, kind, key, schema)
                name = f"{key}s" if kind == "dims" else key
                _write_frame(
                    frame, f"{staging}{subdir}/{name}.parquet", con, local, schema
                )
    except BaseException:
        if local:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    if local:
        Path(staging).rename(base.rstrip("/"))


def _reconcile_schema(schema: Schema, con: DuckDBPyConnection) -> None:
    """Declare the store's schema, or check this layer agrees with it (§5.6, §5.7).

    A schema is not layered data, so there is nothing to fold: the first
    writer states it and the rest must be compatible with what is already
    there. `compatible_with` is what "compatible" means - the changes existing
    layers survive, since NULL already means what the new schema needs it to
    (§5.7).

    Read and written beside `con`'s own layers, so writing into one store
    never consults or replaces another's manifest.

    Raises
    ------
    ValueError
        If this layer's schema would make the store's existing layers
        unreadable.
    """
    base = base_uri_of(con)
    existing = read_schema(con)
    if not (existing.dimensions or existing.attributes):
        write_schema(schema, base)
        return
    if schema == existing:
        return
    problems = schema.compatible_with(existing)
    if problems:
        msg = (
            f"this layer's schema is incompatible with the store's: "
            f"{'; '.join(problems)} (§5.7)"
        )
        raise ValueError(msg)
    # Compatible, so it supersedes: a widened schema still reads every layer
    # written under the narrower one.
    write_schema(schema, base)


def _write_frame(
    frame: nw.LazyFrame, uri: str, con: DuckDBPyConnection, local: bool, schema: Schema
) -> None:
    """Persist one narwhals frame as parquet, through `con`.

    Reaches a native representation only here, which is §4's boundary: the
    seam above stays library-agnostic. A frame already backed by DuckDB is
    handed to `to_parquet` unmaterialised; anything else is collected to arrow
    first, which every narwhals backend supports.

    Every column the schema declares a type for is cast to it on the way
    out (§3.2), so a layer's files carry the schema's types and
    a reader can trust them. Without this a source is free to hand over an
    all-NULL column that its dataframe library typed as float - which pandas
    does for `scenario`, `period` and a `snapshot` with no series rows - and
    every reader would have to re-cast defensively instead.
    """
    if local:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    native = frame.to_native()
    if not isinstance(native, DuckDBPyRelation):
        # Not already a DuckDB plan (a pandas frame also has `to_parquet`, so
        # the type is what distinguishes them, not the method).
        arrow = frame.collect(backend="pyarrow").to_native()  # noqa: F841 - by name
        native = con.sql("FROM arrow")
    _typed(schema, native).to_parquet(uri)


def _typed(schema: Schema, rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """`rel` with every column the schema declares a type for cast to it (§3.2).

    Columns it declares no type for pass through as-is: a `dims/components/`
    frame is "a subset of `c.static`" (§3) and its attribute columns belong to
    whatever vocabulary the schema declares, so their types are the writer's
    business, not this layer's.
    """
    cols = ", ".join(
        f'"{c}"::{t} AS "{c}"' if (t := schema.column_type(c)) else f'"{c}"'
        for c in rel.columns
    )
    return rel.project(cols)


def _validate_frame(frame: nw.LazyFrame, kind: str, key: str, schema: Schema) -> None:
    """Check one frame is shaped for the fold to resolve it.

    Structural only: a long frame carries the §3 columns, and a `dims/` frame
    carries every dim the schema declares it keyed by (§5.5, §6). Values
    are not checked - which component types and attribute names are valid
    belongs to whatever vocabulary the schema declares, and the record layer
    knows none (§5).

    Reads the schema rather than the rows, so validating an unmaterialised
    frame costs nothing.
    """
    columns = set(frame.collect_schema().names())

    # `outputs/` uses the same long schema as `inputs/`; it just does not
    # overlay (§9.4), which is a read-path property rather than a shape one.
    if kind in ("attributes", "outputs"):
        subdir = "inputs" if kind == "attributes" else "outputs"
        required = {*_LONG_FIXED, *schema.dims}
        missing = sorted(required - columns)
        if missing:
            msg = (
                f"{subdir}/{key}.parquet is missing long-schema columns {missing}; "
                f"the resolved relation needs {sorted(required)} (§3)"
            )
            raise ValueError(msg)
        return

    if kind == "dims":
        # A nested axis is keyed by `(*parents, dim)` (§5.4), so its file needs
        # a column per parent - without one the fold would key by a column that
        # is not there, and two periods' identically-labelled timesteps would
        # resolve as one row. An undeclared dim has no nesting to check: the
        # schema's vocabulary is what `axis_key` reads, and a source may hand
        # over an axis the schema does not name.
        if key not in schema.dimensions:
            return
        missing = sorted(set(schema.axis_key(key)) - columns)
        if missing:
            msg = (
                f"dims/{key}s.parquet is missing axis key columns {missing}; "
                f"{key!r} is `within` {sorted(schema.dimensions[key].within)} so "
                f"its labels identify a point only within them (§5.4)"
            )
            raise ValueError(msg)
        return

    keyed = {
        "components": schema.component_dims,
        "connections": schema.connection_dims,
    }.get(kind)
    if keyed is None:
        return
    missing = sorted(set(keyed) - columns)
    if missing:
        msg = (
            f"dims/{kind}/{key}.parquet is missing key dims {missing} that the "
            f"schema declares; the fold would key by a column that is not "
            f"there (§5.5)"
        )
        raise ValueError(msg)


def add_patch(
    record: Any, n: pypsa.Network, n_old: pypsa.Network, con: DuckDBPyConnection
) -> Any:
    """Write `n.diff(n_old)` into the open record's layer as a new child (v2, §4)."""
    raise NotImplementedError(_MSG)
