"""Writing a whole store as a layer (design doc §10).

A `Record` hands over narwhals frames and this module turns them into parquet;
producing one from a framework's own object is a tool's job (§12, §13).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import narwhals as nw
from duckdb import DuckDBPyRelation

from datarecord.duck import base_uri_of, layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.record import Record
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

# The long schema's fixed columns (§3). `bus`/`breakpoint` are part of it, not
# optional extensions to it: both NULL is the ordinary component-level scalar.
# No `component_type`: an attribute row is keyed by `name` alone (§3.5).
_LONG_FIXED = ("name", "bus", "attribute", "breakpoint", "value")


def write_record(
    record_id: UUID | None,
    source: Record,
    con: DuckDBPyConnection,
    *,
    uri: str | None = None,
) -> None:
    """Write `source` as `record_id`'s layer, which must not exist yet (§10).

    An existing layer directory is an error rather than an overwrite or a merge,
    so a whole-store write can never half-replace what a record holds. Keys are
    looked up one at a time and each file written before the next is built, so a
    lazily-building source does one read per file rather than one per key up
    front (§10).

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
            msg = "write_record needs a record_id or a uri"
            raise ValueError(msg)
        base = layer_dir(record_id)
    else:
        base = uri if uri.endswith("/") else uri + "/"
    local = "://" not in base
    if local and Path(base).exists():
        msg = f"layer {base} already exists; write_record creates a new layer (§10)"
        raise FileExistsError(msg)

    schema = source.schema
    if uri is None:
        # One schema for the whole tree (§5.6). The first layer written
        # declares it; every later one is checked against it, so a layer
        # cannot quietly redefine what an attribute means.
        _reconcile_schema(schema, con)

    # Staged then renamed, so a frame that fails validation part-way through
    # leaves no layer rather than half of one (§10). Validation happens as each
    # frame is built, since building it twice would defeat the laziness.
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
        # `outputs/` only for a source carrying results, so a store with none
        # produces a layer without the directory rather than an empty one (§10).
        # `getattr` rather than the attribute: `Record` is structural, so a
        # duck-typed source may not define the member at all, which is the same
        # answer as defining it empty.
        outputs = getattr(source, "outputs", None) or {}
        if outputs:
            kinds.append(("outputs", outputs, "outputs"))
        # Each type's names, to check store-wide uniqueness once every component
        # frame has been seen (§3.5). Collected to one backend because a `Record`
        # may hand over a DuckDB frame for one type and a pandas one for another,
        # and `nw.concat` takes a single backend.
        tagged: list[nw.LazyFrame] = []
        for kind, frames, subdir in kinds:
            for key in frames:
                frame = frames[key]  # looked up exactly once (§10)
                _validate_frame(frame, kind, key, schema)
                if kind == "components":
                    tagged.append(
                        frame.select("name")
                        .collect(backend="pyarrow")
                        .lazy()
                        # Cast after collecting: DuckDB lands `name` as arrow
                        # `large_string` where pandas gives `string`, and concat
                        # compares arrow schemas.
                        .select(
                            nw.col("name").cast(nw.String()),
                            component_type=nw.lit(key).cast(nw.String()),
                        )
                    )
                name = f"{key}s" if kind == "dims" else key
                _write_frame(
                    frame, f"{staging}{subdir}/{name}.parquet", con, local, schema
                )
        _require_unique(tagged)
    except BaseException:
        if local:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    if local:
        Path(staging).rename(base.rstrip("/"))


def _reconcile_schema(schema: Schema, con: DuckDBPyConnection) -> None:
    """Declare the store's schema, or check this layer agrees with it (§5.6, §5.7).

    A schema is not layered data, so there is nothing to fold: the first writer
    states it and the rest must be `compatible_with` it. Read and written beside
    `con`'s own layers, so one store never consults another's manifest.

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

    The one place a native representation is reached (§4.2): a DuckDB-backed
    frame goes to `to_parquet` unmaterialised, anything else via arrow. Columns
    are cast to their declared types on the way out, so a reader can trust them
    rather than re-casting an all-NULL column pandas typed as float (§10).
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

    Undeclared columns pass through: a `dims/components/` frame's attribute
    columns belong to the schema's own vocabulary, so their types are the
    writer's business.
    """
    cols = ", ".join(
        f'"{c}"::{t} AS "{c}"' if (t := schema.column_type(c)) else f'"{c}"'
        for c in rel.columns
    )
    return rel.project(cols)


def _require_unique(tagged: list[nw.LazyFrame]) -> None:
    """Reject a store whose component types share a name (§3.5).

    Unlike `_validate_frame`'s checks this reads the rows, uniqueness being a
    property of the data. A tombstone still occupies the name, so `deleted` is
    not filtered out.

    Parameters
    ----------
    tagged
        One frame per component type, each `(name, component_type)`, on a common
        backend so `nw.concat` accepts them.

    Raises
    ------
    ValueError
        Naming each clashing name and the types claiming it.
    """
    if len(tagged) < 2:  # nothing to collide with
        return
    pairs = nw.concat(tagged, how="vertical").unique(["name", "component_type"])
    clashing = pairs.join(
        pairs.group_by("name")
        .agg(nw.col("component_type").n_unique().alias("_types"))
        .filter(nw.col("_types") > 1)
        .select("name"),
        on="name",
        how="inner",
    ).collect()
    if not clashing.is_empty():
        by_name: dict[str, list[str]] = {}
        for name, ctype in zip(
            clashing["name"].to_list(),
            clashing["component_type"].to_list(),
            strict=True,
        ):
            by_name.setdefault(str(name), []).append(str(ctype))
        # Sorted here rather than in the query: the message must be
        # deterministic, and this is a handful of rows.
        detail = "; ".join(
            f"{n!r} is a {' and a '.join(sorted(t))}"
            for n, t in sorted(by_name.items())
        )
        msg = (
            f"component types reuse names: {detail}; a name identifies one "
            f"component across every type (§3.5)"
        )
        raise ValueError(msg)


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
