"""Writing a whole record as a layer.

A `Record` hands over narwhals frames and this module turns them into parquet;
producing one from a framework's own object is a tool's job.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import narwhals as nw
from duckdb import CoalesceOperator as coalesce
from duckdb import DuckDBPyRelation

from datarecord.duck import base_uri_of, col, fn, layer_dir, lit, try_read_parquet
from datarecord.layered.resolve import cast_declared, read_schema, write_schema
from datarecord.record import Record
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


def write_record(
    revision_id: UUID | None,
    source: Record,
    con: DuckDBPyConnection,
    *,
    uri: str | None = None,
) -> None:
    """Write `source` as `revision_id`'s layer, which must not exist yet.

    An existing layer directory is an error rather than an overwrite or a merge,
    so a whole-record write can never half-replace what a record holds. Keys are
    looked up one at a time and each file written before the next is built, so a
    lazily-building source does one read per file rather than one per key up
    front.

    Parameters
    ----------
    revision_id
        The record whose layer this is; `layer_dir` derives the path.
        `None` only together with `uri`, for a standalone record that belongs
        to no record.
    uri
        Write here instead of at `layer_dir(revision_id)` - how a `Directory`
        commit target produces a record outside the layer tree.
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
        If a long frame is missing a long-schema column, or the schema declares a key
        dim no frame carries - either would make the fold misresolve the layer.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    - [committing](https://energy-models.github.io/datarecord/design/working-record/#committing)
    - [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
    """
    if uri is None:
        if revision_id is None:
            msg = "write_record needs a revision_id or a uri"
            raise ValueError(msg)
        base = layer_dir(revision_id)
    else:
        base = uri if uri.endswith("/") else uri + "/"
    local = "://" not in base
    if local and Path(base).exists():
        msg = f"layer {base} already exists; write_record creates a new layer (https://energy-models.github.io/datarecord/design/writing/)"
        raise FileExistsError(msg)

    schema = source.schema
    if uri is None:
        # One schema for the whole tree (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record). The first layer written
        # declares it; every later one is checked against it, so a layer
        # cannot quietly redefine what an attribute means.
        _reconcile_schema(schema, con)

    # Staged then renamed, so a frame that fails validation part-way through
    # leaves no layer rather than half of one (https://energy-models.github.io/datarecord/design/writing/). Validation happens as each
    # frame is built, since building it twice would defeat the laziness.
    staging = f"{base.rstrip('/')}.staging/" if local else base
    if local:
        Path(staging).mkdir(parents=True)
    try:
        # A layer holds only data: a layered record's one schema lives beside
        # `layers/`, not inside any of them (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record). A standalone directory *is*
        # one record, so there the schema belongs in the directory.
        if local and uri is not None:
            with open(staging + "manifest.json", "w") as fh:
                fh.write(schema.model_dump_json())
        kinds = [
            ("dims", source.dims, "dims"),
            ("components", source.components, "dims/components"),
            *(
                (group, frames, f"dims/{group}")
                for group, frames in source.groups.items()
            ),
            ("attributes", source.attributes, "inputs"),
        ]
        # `outputs/` only for a source carrying results, so a record with none
        # produces a layer without the directory rather than an empty one (https://energy-models.github.io/datarecord/design/writing/).
        if source.outputs:
            kinds.append(("outputs", source.outputs, "outputs"))
        # Each type's names, to check record-wide uniqueness once every component
        # frame has been seen (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types). Collected to one backend because a `Record`
        # may hand over a DuckDB frame for one type and a pandas one for another,
        # and `nw.concat` takes a single backend.
        tagged: list[nw.LazyFrame] = []
        for kind, frames, subdir in kinds:
            for key in frames:
                frame = frames[
                    key
                ]  # looked up exactly once (https://energy-models.github.io/datarecord/design/writing/)
                _validate_frame(frame, kind, key, schema)
                if kind == "components":
                    tagged.append(
                        frame.select("entity")
                        .collect(backend="pyarrow")
                        .lazy()
                        # Cast after collecting: DuckDB lands `entity` as arrow
                        # `large_string` where pandas gives `string`, and concat
                        # compares arrow schemas.
                        .select(
                            nw.col("entity").cast(nw.String()),
                            entity_type=nw.lit(key).cast(nw.String()),
                        )
                    )
                _write_frame(
                    frame, f"{staging}{subdir}/{key}.parquet", con, local, schema
                )
        _require_unique(tagged)
        _write_entity_axis(staging, schema, con)
    except BaseException:
        if local:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    if local:
        Path(staging).rename(base.rstrip("/"))


def _reconcile_schema(schema: Schema, con: DuckDBPyConnection) -> None:
    """Declare the record's schema, or check this layer agrees with it.

    A schema is not layered data, so there is nothing to fold: the first writer
    states it and the rest must be `compatible_with` it. Read and written beside
    `con`'s own layers, so one record never consults another's manifest.

    Raises
    ------
    ValueError
        If this layer's schema would make the record's existing layers
        unreadable.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
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
            f"this layer's schema is incompatible with the record's: "
            f"{'; '.join(problems)} (https://energy-models.github.io/datarecord/design/schema/#versioning)"
        )
        raise ValueError(msg)
    # Compatible, so it supersedes: a widened schema still reads every layer
    # written under the narrower one.
    write_schema(schema, base)


def _write_frame(
    frame: nw.LazyFrame, uri: str, con: DuckDBPyConnection, local: bool, schema: Schema
) -> None:
    """Persist one narwhals frame as parquet, through `con`.

    The one place a native representation is reached: a DuckDB-backed
    frame goes to `to_parquet` unmaterialised, anything else via arrow. Columns
    are cast to their declared types on the way out, so a reader can trust them
    rather than re-casting an all-NULL column pandas typed as float.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    if local:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    native = frame.to_native()
    if not isinstance(native, DuckDBPyRelation):
        # Not already a DuckDB plan (a pandas frame also has `to_parquet`, so
        # the type is what distinguishes them, not the method).
        arrow = frame.collect(backend="pyarrow").to_native()  # noqa: F841 - by name
        native = con.sql("FROM arrow")
    cast_declared(schema, native).to_parquet(uri)


def _write_entity_axis(staging: str, schema: Schema, con: DuckDBPyConnection) -> None:
    """Write `dims/entity.parquet`: one row per component, with its type.

    The entity axis is what a component's identity *is* - which entities the
    layer names, what type each is, and which are tombstoned. Derived rather
    than handed over: the per-type frames just written say all three, so a
    `Record` never has to produce it and cannot disagree with itself about it.

    Read back through DuckDB rather than unioned from the source frames,
    because `_write_frame` has already cast every column to what the schema
    declares - where the frames themselves may disagree, an all-NULL `scenario`
    landing as arrow `null` for one type and `string` for another.

    `entity_type` is a column of this axis and of no attribute row, which is
    what makes `attributes_for` reachable from an entity alone. Being a mapping
    it has an axis file of its own too, carrying the attributes addressed by the
    type alone - written from `source.dims` like any axis, not derived here.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    rel = try_read_parquet(
        f"{staging}dims/components/*.parquet",
        con,
        union_by_name=True,
        filename=True,
    )
    if rel is None:
        return
    # The type is the file a row is in, so it comes from the filename rather
    # than a column - which is exactly the glob-and-derive this axis exists to
    # replace for every later reader.
    ctype = fn.regexp_extract(col("filename"), lit(r"([^/]+)\.parquet$"), lit(1))
    deleted = (
        coalesce(col("deleted"), lit(False))  # noqa: FBT003
        if "deleted" in rel.columns
        else lit(False)  # noqa: FBT003
    )
    rel.project(
        col("entity"), ctype.alias("entity_type"), deleted.alias("deleted")
    ).to_parquet(f"{staging}dims/entity.parquet")


def _require_unique(tagged: list[nw.LazyFrame]) -> None:
    """Reject a record whose component types share a name.

    Unlike `_validate_frame`'s checks this reads the rows, uniqueness being a
    property of the data. A tombstone still occupies the name, so `deleted` is
    not filtered out.

    Parameters
    ----------
    tagged
        One frame per component type, each `(name, entity_type)`, on a common
        backend so `nw.concat` accepts them.

    Raises
    ------
    ValueError
        Naming each clashing name and the types claiming it.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    if len(tagged) < 2:  # nothing to collide with
        return
    pairs = nw.concat(tagged, how="vertical").unique(["entity", "entity_type"])
    clashing = (
        pairs.join(
            pairs.group_by("entity")
            .agg(nw.col("entity_type").n_unique().alias("_types"))
            .filter(nw.col("_types") > 1)
            .select("entity"),
            on="entity",
            how="inner",
        )
        .select("entity", "entity_type")  # the order `iter_rows` unpacks
        .collect()
    )
    if not clashing.is_empty():
        by_name: dict[str, list[str]] = {}
        for name, ctype in clashing.iter_rows():
            by_name.setdefault(str(name), []).append(str(ctype))
        # Sorted here rather than in the query: the message must be
        # deterministic, and this is a handful of rows.
        detail = "; ".join(
            f"{n!r} is a {' and a '.join(sorted(t))}"
            for n, t in sorted(by_name.items())
        )
        msg = (
            f"component types reuse names: {detail}; a name identifies one "
            f"component across every type (https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)"
        )
        raise ValueError(msg)


def _validate_frame(frame: nw.LazyFrame, kind: str, key: str, schema: Schema) -> None:
    """Check one frame is shaped for the fold to resolve it.

    Structural only: a long frame carries its own attribute's coordinates, and a
    `dims/` frame carries every dim the schema declares it keyed by. Values
    are not checked - which component types and attribute names are valid
    belongs to whatever vocabulary the schema declares, and the record layer
    knows none.

    An attribute's coordinates are what its `dims` declare, so one file's column
    set is not another's and neither is every declared dim. A result the schema
    does not declare has no coordinates to derive, so it falls back to the fixed
    columns every long row has.

    Reads the schema rather than the rows, so validating an unmaterialised
    frame costs nothing.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    columns = set(frame.collect_schema().names())

    # `outputs/` uses the same long schema as `inputs/`; it just does not
    # overlay (https://energy-models.github.io/datarecord/design/read-path/#outputs), which is a read-path property rather than a shape one.
    if kind in ("attributes", "outputs"):
        subdir = "inputs" if kind == "attributes" else "outputs"
        # An input's shape comes from its spec, so one the schema does not
        # declare has no shape to check it against - and writing it would put a
        # file in `inputs/` that no read path knows the columns of. A *result*
        # is never declared, a tool deriving those from its own registry.
        if kind == "attributes" and key not in schema.attributes:
            msg = (
                f"inputs/{key}.parquet is not a declared attribute; its `dims` "
                f"are what say which columns the file carries (https://energy-models.github.io/datarecord/design/schema/#attributespec)"
            )
            raise ValueError(msg)
        # A result's shape is not the schema's to fix, even where its name
        # matches a declared attribute: `outputs/control.parquet` may vary over
        # axes the *input* `control` does not. So only the fixed columns every
        # long row has are required of one (https://energy-models.github.io/datarecord/design/read-path/#outputs).
        required = (
            set(schema.long_columns_for(key))
            if kind == "attributes"
            else {"attribute", "breakpoint", "value"}
        )
        missing = sorted(required - columns)
        if missing:
            msg = (
                f"{subdir}/{key}.parquet is missing long-schema columns {missing}; "
                f"the resolved relation needs {sorted(required)} (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
            )
            raise ValueError(msg)
        # And an *input* carries nothing else: a coordinate the attribute is
        # not addressed by would be a column the read path never projects,
        # written as a fact about a value that does not have one. Reported
        # rather than dropped, since a source emitting one disagrees with the
        # schema about what the attribute is - the source's bug to fix.
        #
        # A result is exempt for the same reason it need not be declared: its
        # shape is a framework's business, and a name it shares with an input
        # says nothing about which coordinates the *result* varies over.
        extra = sorted(columns - required) if kind == "attributes" else []
        if extra:
            msg = (
                f"{subdir}/{key}.parquet carries columns {extra} the attribute is "
                f"not addressed by; its `dims` say {sorted(required)} (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
            )
            raise ValueError(msg)
        return

    if kind == "dims":
        # A nested axis is keyed by `(*parents, dim)` (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis), so its file needs
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
                f"dims/{key}.parquet is missing axis key columns {missing}; "
                f"{key!r} is `within` {sorted(schema.dimensions[key].within)} so "
                f"its labels identify a point only within them (https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)"
            )
            raise ValueError(msg)
        # A mapping's column lives on the axis it classifies, so that file is
        # where the classification is stored and where its absence shows. Not
        # required: a record may declare `country` before any bus is assigned
        # one, and a NULL is "unclassified". The same goes for an attribute
        # addressed by this axis alone, which is a column here and resolves to
        # its `default` where no layer wrote one (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives).
        #
        # A column no declaration accounts for is rejected, as a long frame's
        # extras are: an axis file's payload is the schema's to state, and one
        # riding along uninvited would be read back as data nothing knows the
        # dtype or meaning of.
        known = (
            set(schema.axis_key(key))
            | set(schema.mappings_on(key))
            | set(schema.attributes_on(key))
            # The structural columns an axis file may carry: a tombstone, and an
            # explicit order key. Not every name in `STRUCTURAL_TYPES` - most of
            # those are a long row's, and `attribute` or `breakpoint` here would
            # be a long frame written to the wrong place.
            | {"deleted", "order_key"}
        )
        extra = sorted(columns - known)
        if extra:
            msg = (
                f"dims/{key}.parquet carries columns {extra} the schema does not "
                f"declare for the {key!r} axis; an axis file holds its key, the "
                f"mappings that classify it, and the attributes addressed by it "
                f"alone (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)"
            )
            raise ValueError(msg)
        return

    if kind not in schema.groups:
        return
    # A group's row is keyed by its coordinates, so a frame lacking one would
    # be keyed by a column that is not there.
    missing = sorted(set(schema.group_coordinates(kind)) - columns)
    if missing:
        msg = (
            f"dims/{kind}/{key}.parquet is missing the {kind!r} group's "
            f"coordinates {missing}; the fold would key by a column that is "
            f"not there (https://energy-models.github.io/datarecord/design/schema/#groups)"
        )
        raise ValueError(msg)
