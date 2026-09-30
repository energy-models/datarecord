# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a whole record as a layer.

A `LayerData` hands over relations and this module turns them into parquet;
`sources.from_sources` produces one from tables keyed by declared names.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
- [consuming a record](https://energy-models.github.io/datarecord/design/sources/)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

from __future__ import annotations

import shutil
from itertools import combinations
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import as_relation, base_uri_of, ex_all, fn, layer_dir
from datarecord.layered.resolve import cast_declared, read_schema, write_schema
from datarecord.record import Frames, LayerData, RecordLike
from datarecord.schema import Schema

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


def write_record(
    revision_id: UUID | None,
    source: LayerData | RecordLike,
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
        Write here instead of at the revision's own layer - how a `Directory`
        commit target produces a record outside the layer tree.
    source
        The layer's contents: a `LayerData` - a `StagedSource` for a `NewChild`
        commit, a `Resolver` for a `Directory` one - or a framework's own
        `RecordLike`, wrapped in a thin adapter reading its `Frames` through the
        same enumerate-and-read pairs. Validated against its own schema before
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
        If a frame carries a column its file does not hold, such as an attribute
        column on a relation frame.
        Also if two rows of one attribute cover one coordinate and neither
        names more of its dims, or both leave the same dims NULL there, which
        no rule orders.

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

    data = (
        source if isinstance(source, LayerData) else _RecordLikeAsLayerData(source, con)
    )
    schema = data.schema
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
            ("dims", data.axes(), data.axis),
            ("relations", data.relations(), data.relation),
            ("attributes", data.attributes(), data.attribute),
        ]
        for kind, keys, read in kinds:
            for key in keys:
                rel = read(
                    key
                )  # looked up exactly once (https://energy-models.github.io/datarecord/design/writing/)
                if rel is None:
                    continue
                _validate_frame(rel, kind, key, schema)
                if kind == "attributes":
                    _refuse_ties(rel, key, schema)
                _write_frame(rel, f"{staging}{kind}/{key}.parquet", schema)
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


DERIVED = ("order_key",)
"""Columns a resolved frame carries that no layer file may.

The fold's answer *about* a frame rather than data in it, so writing one would
both put a column in a file the format does not define and read as stored order
where the fold always re-derives it from file order.

Notes
-----
- [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
"""


def _write_frame(
    rel: DuckDBPyRelation,
    uri: str,
    schema: Schema,
) -> None:
    """Persist one relation as parquet, unmaterialised.

    Columns are cast to their declared types on the way out, so a reader can
    trust them rather than re-casting an all-NULL column pandas typed as float.

    `DERIVED` is dropped from every file here rather than in the callers, so a
    column a source happens to carry cannot reach a file by a path that forgot
    to strip it.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    if "://" not in uri:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    unwritable = [c for c in DERIVED if c in rel.columns]
    if unwritable:
        rel = rel.project(star(exclude=unwritable))
    cast_declared(schema, rel).to_parquet(uri)


def _validate_frame(rel: DuckDBPyRelation, kind: str, key: str, schema: Schema) -> None:
    """Check one frame is shaped for the fold to resolve it.

    Structural only: a long frame carries its own attribute's coordinates, and a
    `dims/` frame carries every dim the schema declares it keyed by. Values
    are not checked - which component types and attribute names are valid
    belongs to whatever vocabulary the schema declares, and the record layer
    knows none.

    An attribute's coordinates are what its `dims` declare, so one file's column
    set is not another's and neither is every declared dim.

    Reads the schema rather than the rows, so validating an unmaterialised
    relation costs nothing.

    Notes
    -----
    - [the Record protocol](https://energy-models.github.io/datarecord/design/record/)
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [the schema](https://energy-models.github.io/datarecord/design/schema/)
    - [partial](https://energy-models.github.io/datarecord/design/schema/#partial-the-granularity-of-an-override)
    """
    columns = set(rel.columns)

    if kind == "attributes":
        # An attribute's shape comes from its spec, so one the schema does not
        # declare has no shape to check it against - and writing it would put a
        # file in `attributes/` that no read path knows the columns of.
        if key not in schema.attributes:
            msg = (
                f"attributes/{key}.parquet is not a declared attribute; its `dims` "
                f"are what say which columns the file carries (https://energy-models.github.io/datarecord/design/schema/#attributespec)"
            )
            raise ValueError(msg)
        required = set(schema.long_columns_for(key))
        missing = sorted(required - columns)
        if missing:
            msg = (
                f"attributes/{key}.parquet is missing long-schema columns {missing}; "
                f"the resolved relation needs {sorted(required)} (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
            )
            raise ValueError(msg)
        # And an attribute carries nothing else: a coordinate it is not
        # addressed by would be a column the read path never projects, written
        # as a fact about a value that does not have one. Reported rather than
        # dropped, since a source emitting one disagrees with the schema about
        # what the attribute is - the source's bug to fix.
        extra = sorted(columns - required)
        if extra:
            msg = (
                f"attributes/{key}.parquet carries columns {extra} the attribute is "
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
        # A column no declaration accounts for is rejected, as a long frame's
        # extras are: one riding along uninvited would be read back as data
        # nothing knows the dtype or meaning of.
        known = (
            set(schema.axis_key(key))
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
                f"declare for the {key!r} axis; an axis file holds its labels, and "
                f"an attribute over {key!r} is `attributes/` rows (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)"
            )
            raise ValueError(msg)
        return

    if kind != "relations" or key not in schema.relations:
        return
    # A relation's row is keyed by its columns, `values` among them, so a frame
    # lacking one would be keyed by a column that is not there.
    own = set(schema.relation_columns(key))
    missing = sorted(own - columns)
    if missing:
        msg = (
            f"relations/{key}.parquet is missing the relation's columns "
            f"{missing}; the fold would key by a column that is not there (https://energy-models.github.io/datarecord/design/schema/#relations)"
        )
        raise ValueError(msg)
    # An attribute is over dims only, so a column beyond the relation's own and
    # its tombstone is data no reader knows the meaning of.
    extra = sorted(columns - own - {"deleted", *DERIVED})
    if extra:
        msg = (
            f"relations/{key}.parquet carries columns {extra} the schema does not "
            f"declare for the {key!r} relation; a relation file holds its columns "
            f"{sorted(own)} and its tombstone (https://energy-models.github.io/datarecord/design/schema/#relations)"
        )
        raise ValueError(msg)


def _refuse_ties(rel: DuckDBPyRelation, attribute: str, schema: Schema) -> None:
    """Refuse two rows that cover one coordinate with neither naming more dims.

    Duplicates first: two rows that name the same dims name none more.

    The read keeps the row that names more of the attribute's dims
    (`resolve._named_most`), which orders two rows only where one names a
    superset of the other's dims. Only pairs of distinct NULL patterns are
    checked, which are few, and a pair overlaps where it agrees on the dims
    both name. Reads the rows, unlike `_validate_frame`.

    Raises
    ------
    ValueError
        If two such rows overlap, naming their NULL patterns and a coordinate
        they share.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    dims = schema.broadcasts_over(attribute)
    _refuse_duplicates(rel, attribute, dims)
    if len(dims) < 2:
        return
    patterns = [
        frozenset(d for d, unnamed in zip(dims, row, strict=True) if not unnamed)
        for row in rel.project(*(col(d).isnull().alias(d) for d in dims))
        .distinct()
        .fetchall()
    ]
    for a, b in combinations(patterns, 2):
        if a <= b or b <= a:
            continue
        overlap = _overlap(rel, dims, a, b)
        if overlap is None:
            continue
        coordinate = ", ".join(
            f"{d}={v!r}" for d, v in zip(sorted(a | b), overlap, strict=True)
        )
        msg = (
            f"attributes/{attribute}.parquet has a row leaving "
            f"{sorted(set(dims) - a)} NULL and one leaving {sorted(set(dims) - b)} "
            f"NULL that both cover {coordinate}; neither names more of "
            f"{list(dims)}, so no rule picks one. State the value at that "
            f"coordinate in a row of its own (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)"
        )
        raise ValueError(msg)


def _refuse_duplicates(
    rel: DuckDBPyRelation, attribute: str, dims: tuple[str, ...]
) -> None:
    """Refuse two rows at one coordinate that leave the same dims NULL.

    A curve is many rows at one coordinate, one per distinct breakpoint, so
    those are allowed; a repeated breakpoint, a second scalar, or a scalar
    beside a curve is not. A NULL groups with a NULL, so two rows leaving a dim
    NULL are at one coordinate along it.
    """
    breakpoint = col("breakpoint")
    row = (
        rel.aggregate(
            [
                *(col(d) for d in dims),
                fn.count_if(breakpoint.isnull()).alias("_scalars"),
                fn.count(breakpoint).alias("_points"),
                sql(f"count(DISTINCT {breakpoint})").alias("_distinct"),
            ]
        )
        .filter(
            (col("_scalars") > lit(1))
            | (col("_points") > col("_distinct"))
            | ((col("_scalars") > lit(0)) & (col("_points") > lit(0)))
        )
        .limit(1)
        .fetchone()
    )
    if row is None:
        return
    values = dict(zip(dims, row, strict=False))
    coordinate = (
        ", ".join(f"{d}={v!r}" for d, v in values.items() if v is not None)
        or "every coordinate"
    )
    scalars, points = row[len(dims)], row[len(dims) + 1]
    if scalars and points:
        shape, keep = "a scalar row and a curve", "either the scalar or the curve"
    elif scalars:
        shape, keep = "two rows", "one row"
    else:
        shape, keep = "a curve repeating a breakpoint", "one row per breakpoint"
    msg = (
        f"attributes/{attribute}.parquet has {shape} at {coordinate}, each "
        f"leaving {[d for d, v in values.items() if v is None]} NULL, so no rule "
        f"picks one. Keep {keep} at that coordinate (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)"
    )
    raise ValueError(msg)


def _overlap(
    rel: DuckDBPyRelation,
    dims: tuple[str, ...],
    a: frozenset[str],
    b: frozenset[str],
) -> tuple | None:
    """A coordinate of `a | b` that a row naming `a` and one naming `b` share.

    `None` where they share none. The dims neither names are left out: both
    rows cover every label there.
    """

    def named(alias: str, names: frozenset[str]) -> DuckDBPyRelation:
        return rel.filter(
            ex_all(col(d).isnotnull() if d in names else col(d).isnull() for d in dims)
        ).set_alias(alias)

    both = sorted(a & b)
    joined = named("a", a).join(
        named("b", b),
        ex_all(col("a", d) == col("b", d) for d in both) if both else lit(True),  # noqa: FBT003
    )
    row = joined.project(
        *(col("a" if d in a else "b", d) for d in sorted(a | b))
    ).fetchone()
    return None if row is None else tuple(row)


class _RecordLikeAsLayerData:
    """A `RecordLike` read through `LayerData`'s enumerate-and-read pairs.

    The adapter that lets `write_record` stay one code path over raw
    relations: `sources.from_sources` hands over narwhals `Frames`,
    one lookup per key exactly as `write_record` already does, so this wraps
    each mapping rather than eagerly converting it. `con` is needed only to
    land a non-DuckDB frame as a relation (`as_relation`).

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/record/#layerdata)
    """

    def __init__(self, source: RecordLike, con: DuckDBPyConnection) -> None:
        self._source = source
        self._con = con

    @property
    def schema(self) -> Schema:
        return self._source.schema

    @property
    def frozen(self) -> bool:
        # A framework object is read once to produce a layer, never folded
        # under a reader, so there is nothing for staleness to mean here.
        return True

    def _read(self, frames: Frames, key: str) -> DuckDBPyRelation | None:
        if key not in frames:
            return None
        return as_relation(frames[key], self._con)

    def axes(self) -> set[str]:
        return set(self._source.dims)

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        return self._read(self._source.dims, dim)

    def relations(self) -> set[str]:
        return set(self._source.relations)

    def relation(self, name: str) -> DuckDBPyRelation | None:
        return self._read(self._source.relations, name)

    def attributes(self) -> set[str]:
        return set(self._source.attributes)

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self._read(self._source.attributes, name)
