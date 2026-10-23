# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a whole record as a layer.

A `LayerData` hands over relations and this module turns them into parquet;
producing one from a framework's own object is a tool's job.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

from __future__ import annotations

import copy
import random
import shutil
from itertools import combinations
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING
from uuid import uuid4

from duckdb import CoalesceOperator as coalesce
from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import as_relation, ex_all, fn, null_safe
from datarecord.files import Absent, PreconditionFailedError, Version
from datarecord.layered.resolve import PinnedResolver, cast_declared, resolve_coords
from datarecord.layered.sources import DirectorySource
from datarecord.record import LayerData, Record, RecordLike, Relations
from datarecord.schema import DERIVED, Schema

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from datarecord.layered.sources import LayerSource
    from datarecord.store import Store, Workspace


def layer_data(source: LayerData | RecordLike, con: DuckDBPyConnection) -> LayerData:
    """`source` as the `LayerData` the writer reads, planned on `con`.

    A framework's `RecordLike` is wrapped in a thin adapter reading its
    `Relations` through the same enumerate-and-read pairs.

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/record/#layerdata)
    """
    if isinstance(source, LayerData):
        return source
    return _RecordLikeAsLayerData(_pinned(source), con)


def _pinned(source: RecordLike) -> RecordLike:
    """`source`, or where it is a `Record` past a staging area, a copy over a `PinnedResolver`.

    The copy keeps every member of `source`'s class and swaps only the resolver, so it reads what
    `source` reads with the fold done once. Held only for one write, during
    which nothing stages.
    """
    if not isinstance(source, Record) or source.resolver.frozen:
        return source
    pinned = copy.copy(source)
    object.__setattr__(pinned, "resolver", PinnedResolver.of(source.resolver))
    return pinned


def write_layer(
    data: LayerData,
    base: str,
    workspace: Workspace,
    *,
    manifest: str | None = None,
    parents: Sequence[LayerSource] = (),
) -> None:
    """Write `data` as the layer directory `base`, which must not exist yet.

    An existing layer directory is an error rather than an overwrite or a merge,
    so a whole-record write can never half-replace what a record holds. Keys are
    looked up one at a time and each file written before the next is built, so a
    lazily-building source does one read per file rather than one per key up
    front. Locally the layer is staged into a sibling and renamed, so a failed
    write leaves nothing; remotely it can leave part of a layer, which nothing
    reaches without the file written after it.

    Parameters
    ----------
    data
        The layer's contents, validated against its own schema as each frame is
        built.
    manifest
        A standalone record's `manifest.json` text, written into the
        directory last; a tree layer holds only data.
    parents
        The sources the layer folds over, root first; none for a root or a
        standalone record.

    Raises
    ------
    FileExistsError
        If the layer directory already exists locally.
    ValueError
        If a long frame is missing a long-schema column, or the schema declares a key
        dim no frame carries - either would make the fold misresolve the layer.
        If a group tuple or a property `{"one_of": <dim>}` alive after the layer names
        a label dead after it.
        If a column's type does not fit its dtype, naming the attribute,
        property or dim that declares the column, and the dtype.
        If two rows of one attribute cover one coordinate and each names a dim
        the other leaves NULL, naming the attribute and the coordinate.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    - [a revision exists only by its file](https://energy-models.github.io/datarecord/design/writing/#a-revision-exists-only-by-its-file)
    """
    local = "://" not in base
    if local and Path(base).exists():
        msg = f"layer {base} already exists; a write creates a new layer (https://energy-models.github.io/datarecord/design/writing/)"
        raise FileExistsError(msg)
    schema = data.schema

    # Staged then renamed, so a frame that fails validation part-way through
    # leaves no layer rather than half of one (https://energy-models.github.io/datarecord/design/writing/). Validation happens as each
    # frame is built, since building it twice would defeat the laziness.
    staging = f"{base.rstrip('/')}.staging/" if local else base
    if local:
        Path(staging).mkdir(parents=True)
    try:
        kinds = [
            ("dims", data.axes(), data.axis),
            ("groups", data.groups(), data.group),
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
                uri = _write_frame(rel, staging, kind, key, schema)
                if kind == "attributes":
                    _refuse_incomparable(workspace.con.read_parquet(uri), key, schema)
        if data.groups() or data.axes():
            layer = DirectorySource(staging, schema, workspace, uuid4())
            _require_living_references(parents, layer, workspace.con, schema)
        # A standalone directory *is* one record, so its schema is in it, and
        # written last: a directory without one reads as no record.
        if manifest is not None:
            workspace.files(staging).write_text(
                staging + "manifest.json", manifest, expect=Absent()
            )
    except BaseException:
        if local:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        if isinstance(data, _RecordLikeAsLayerData):
            data.release()
    if local:
        Path(staging).rename(base.rstrip("/"))


def reconcile_schema(schema: Schema, store: Store) -> None:
    """Declare the record's schema, or check this layer agrees with it.

    A schema is not layered data, so there is nothing to fold: the first writer
    states it and the rest must be `compatible_with` it. Read and written beside
    `store`'s own layers, so one record never consults another's manifest.

    A compare-and-swap: the manifest is written only if unchanged since it was
    read, and a writer that lost re-reads and checks again, so two writers
    widening at once end with both widenings or an incompatibility reported,
    never one silently lost. A lost write backs off with jitter and is retried
    up to `_ATTEMPTS` times.

    Raises
    ------
    ValueError
        If this layer's schema would make the record's existing layers
        unreadable.
    RuntimeError
        If every attempt lost: the manifest kept changing, or it cannot be read
        and a 403 was taken for a missing key.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    - [the manifest's compare-and-swap](https://energy-models.github.io/datarecord/design/writing/#the-manifests-compare-and-swap)
    """
    schema = schema.model_copy(update={"id": None})
    files = store.files
    for attempt in range(_ATTEMPTS):
        found = files.read_text(store.manifest_uri)
        expect: Version | Absent = Absent() if found is None else found[1]
        existing = Schema() if found is None else Schema.model_validate_json(found[0])
        if existing.dimensions or existing.attributes:
            if schema == existing:
                return
            problems = schema.compatible_with(existing)
            if problems:
                msg = (
                    f"this layer's schema is incompatible with the record's: "
                    f"{'; '.join(problems)} (https://energy-models.github.io/datarecord/design/schema/#versioning)"
                )
                raise ValueError(msg)
        try:
            files.write_text(
                store.manifest_uri, schema.model_dump_json(), expect=expect
            )
        except PreconditionFailedError:
            sleep(random.uniform(0, 0.05 * 2**attempt))
            continue
        return
    msg = (
        f"{store.manifest_uri} changed under each of {_ATTEMPTS} writes, or cannot "
        f"be read: a 403 on read counts as missing, so check s3:GetObject "
        f"(https://energy-models.github.io/datarecord/design/writing/#the-manifests-compare-and-swap)"
    )
    raise RuntimeError(msg)


_ATTEMPTS = 10
"""How many times `reconcile_schema` writes the manifest before giving up."""


def _write_frame(
    rel: DuckDBPyRelation, staging: str, kind: str, key: str, schema: Schema
) -> str:
    """Write `rel` as `{kind}/{key}.parquet` under `staging`, unmaterialised; return its URI.

    Columns are cast to their declared types on the way out, so a reader can
    trust them. Before that, a column whose type does not fit its declared
    type (`takes`) is refused, judged by the relation's types, not its rows.

    `DERIVED` is dropped here rather than in the callers, so a column a source
    happens to carry cannot reach a file by a path that forgot to strip it.

    Raises
    ------
    ValueError
        As `Schema.require_takes` does.

    Notes
    -----
    - [relations](https://energy-models.github.io/datarecord/design/record/#relations)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    file = f"{kind}/{key}.parquet"
    uri = f"{staging}{file}"
    if "://" not in uri:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    unwritable = [c for c in DERIVED if c in rel.columns]
    if unwritable:
        rel = rel.project(star(exclude=unwritable))
    subject = key if kind in ("dims", "groups") else None
    attribute = key if kind == "attributes" else None
    rel = schema.require_takes(rel, file, subject, attribute)
    cast_declared(schema, rel, subject, attribute).to_parquet(uri)
    return uri


def _refuse_incomparable(rel: DuckDBPyRelation, attribute: str, schema: Schema) -> None:
    """Refuse two rows of `attribute` covering one coordinate where neither is more specific.

    Two rows cover one coordinate where they agree on every dim both name. A
    row is more specific than another where it names every dim the other
    names, and more. Of two rows of one layer covering a coordinate, a read
    keeps the more specific one, so where each names a dim the other leaves
    NULL, it has none to keep.

    Only the distinct NULL patterns are paired, and they are few. Each pair is
    joined on the distinct coordinates its rows name, so one series is not
    multiplied against another. `rel` is the file as written, so the source
    is built only once, and only its dims are read.

    Raises
    ------
    ValueError
        Naming the attribute and one coordinate both rows cover.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    dims = schema.coordinates_of(attribute)
    if len(dims) < 2:
        return
    patterns = [
        tuple(d for d, named in zip(dims, row, strict=True) if named)
        for row in rel.project(*(col(d).isnotnull() for d in dims))
        .distinct()
        .fetchall()
    ]
    for a, b in combinations(patterns, 2):
        if set(a) <= set(b) or set(b) <= set(a):
            continue
        rows_a, rows_b = _named_rows(rel, dims, a, "a"), _named_rows(rel, dims, b, "b")
        both = [d for d in a if d in b]
        joined = (
            rows_a.join(rows_b, ex_all(col("a", d) == col("b", d) for d in both))
            if both
            else rows_a.cross(rows_b)
        )
        union = [d for d in dims if d in a or d in b]
        row = (
            joined.project(*(col("a" if d in a else "b", d) for d in union))
            .limit(1)
            .fetchone()
        )
        if row is None:
            continue
        coordinate = ", ".join(f"{d}={v}" for d, v in zip(union, row, strict=True))
        msg = (
            f"attributes/{attribute}.parquet holds a row naming {list(a)} and one "
            f"naming {list(b)} that both cover {coordinate}; neither names every "
            f"dim the other does, so neither is the more specific there. Name the "
            f"labels of one of them row by row, leaving that coordinate out (https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)"
        )
        raise ValueError(msg)


def _named_rows(
    rel: DuckDBPyRelation, dims: Sequence[str], named: Sequence[str], alias: str
) -> DuckDBPyRelation:
    """Return the distinct values of `named` in the rows of `rel` that name exactly those of `dims`."""
    exact = ex_all(col(d).isnotnull() if d in named else col(d).isnull() for d in dims)
    return (
        rel.filter(exact).project(*(col(d) for d in named)).distinct().set_alias(alias)
    )


def _require_living_references(
    parents: Sequence[LayerSource],
    layer: LayerSource,
    con: DuckDBPyConnection,
    schema: Schema,
) -> None:
    """Reject a layer storing a reference to a dead label, or killing a label one names.

    A reference is a group tuple's field over a dim, or a property
    `{"one_of": <dim>}`.
    Removing a label tombstones the tuples naming it in the same layer, which
    `WorkingRecord.remove` writes, and a property naming it must be cleared
    first; a layer written otherwise carries both itself. Only what `layer`
    does is checked - the rows it writes, and the rows still naming a label it
    tombstones - so a reference an ancestor left dangling fails no later write.
    A layer writing no referring row and killing no referenced label resolves
    nothing.

    Raises
    ------
    ValueError
        Naming up to three such rows per reference.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/#deletion)
    """
    references = [
        *(
            (g, f, fn.struct_extract(col("t", g), lit(f)), d)
            for g in schema.group_columns
            for f, d in schema.groups[g].over.items()
        ),
        *((s, p, col("t", p), d) for (s, p), d in schema.one_of_properties().items()),
    ]
    referenced = {dim for *_, dim in references}
    killed = {
        d: tomb
        for d in layer.axes()
        if d in referenced
        and (rel := layer.axis(d)) is not None
        and "deleted" in rel.columns
        and (tomb := rel.filter(col("deleted"))).limit(1).fetchone() is not None
    }
    written = {
        s: rel
        for s in {subject for subject, *_ in references}
        if (rel := _living(_rows_of(layer, s, schema))) is not None
        and rel.limit(1).fetchone() is not None
    }
    if not killed and not written:
        return
    coords = resolve_coords(schema, [*parents, layer], con)
    problems = []
    for subject, column, label, dim in references:
        dead = coords.dead.get(dim)
        if dead is None:
            continue
        stored = subject if subject in schema.groups else column
        suspects = (
            [(written[subject], dead)]
            if subject in written and stored in written[subject].columns
            else []
        )
        resolved = (coords.groups if subject in schema.groups else coords.axes).get(
            subject
        )
        if dim in killed and resolved is not None:
            mine = dead.set_alias("d").join(
                killed[dim].set_alias("k"),
                col("d", dim) == col("k", dim),
                how="semi",
            )
            suspects.append((resolved, mine))
        hits = [
            h
            for rows, labels in suspects
            for (h,) in rows.set_alias("t")
            .join(labels.set_alias("x"), label == col("x", dim), how="semi")
            .project(col(subject))
            .limit(3)
            .fetchall()
        ]
        if hits:
            problems.append(f"{subject} {hits[:3]} name a removed {dim} in {column!r}")
    if problems:
        msg = (
            f"{'; '.join(problems)}; removing a label removes the tuples naming "
            f"it and leaves no property naming it, so tombstone those tuples and "
            f"clear those properties in the same layer (https://energy-models.github.io/datarecord/design/layers/#deletion)"
        )
        raise ValueError(msg)


def _rows_of(
    source: LayerSource, subject: str, schema: Schema
) -> DuckDBPyRelation | None:
    """`source`'s rows of `subject`, a group or a dim."""
    return source.group(subject) if subject in schema.groups else source.axis(subject)


def _living(rel: DuckDBPyRelation | None) -> DuckDBPyRelation | None:
    """`rel`'s rows that are no tombstone."""
    if rel is None or "deleted" not in rel.columns:
        return rel
    return rel.filter(~coalesce(col("deleted"), lit(False)))  # noqa: FBT003


def _validate_frame(rel: DuckDBPyRelation, kind: str, key: str, schema: Schema) -> None:
    """Check one frame is shaped for the fold to resolve it.

    Structural only: a long frame carries its own attribute's coordinates, and a
    `dims/` frame carries every dim the schema declares it keyed by. Values
    are not checked - which labels and attribute names are valid
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
        # Nor anything else: a coordinate the attribute is not addressed by
        # would be a column the read path never projects, written as a fact
        # about a value that does not have one. Reported rather than dropped,
        # since a source emitting one disagrees with the schema about what the
        # attribute is - the source's bug to fix.
        extra = sorted(columns - required)
        if extra:
            msg = (
                f"attributes/{key}.parquet carries columns {extra} the attribute is "
                f"not addressed by; its `dims` say {sorted(required)} (https://energy-models.github.io/datarecord/design/format/#the-long-schema)"
            )
            raise ValueError(msg)
        schema.check_group_columns(
            schema.groups_of(key), rel, carrier=f"attributes/{key}.parquet"
        )
        return

    if kind == "dims":
        # A source may hand over an axis the schema does not name.
        if key not in schema.dimensions:
            return
        if key not in columns:
            msg = f"dims/{key}.parquet is missing its key column {key!r} (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)"
            raise ValueError(msg)
        _refuse_extra(
            columns,
            {key, "deleted", "order_key"},
            schema,
            key,
            f"dims/{key}.parquet",
        )
        return

    if kind != "groups" or key not in schema.groups:
        return
    if key not in columns:
        msg = (
            f"groups/{key}.parquet is missing {[key]}; a group file is keyed "
            f"by one struct column named for the group (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)"
        )
        raise ValueError(msg)
    _refuse_extra(
        columns, {key, "deleted", "order_key"}, schema, key, f"groups/{key}.parquet"
    )
    schema.check_group_columns([key], rel, carrier=f"groups/{key}.parquet")


def _refuse_extra(
    columns: set[str], structural: set[str], schema: Schema, subject: str, file: str
) -> None:
    """Refuse a column of an axis or group file beyond `structural` and `subject`'s properties.

    `structural` is the key, the tombstone and an explicit order key.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    known = structural | set(schema.properties.get(subject, {}))
    extra = sorted(columns - known)
    if not extra:
        return
    attributes = [c for c in extra if c in schema.attributes]
    named = "they" if attributes == extra else str(attributes)
    hint = (
        f"; {named} are attributes, whose values are long rows of "
        f"attributes/<attr>.parquet whatever their `dims`"
        if attributes
        else ""
    )
    msg = (
        f"{file} carries columns {extra} the schema does not declare as "
        f"properties of {subject!r}; the file holds only its key, tombstones "
        f"and those properties, {sorted(known)}{hint} (https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)"
    )
    raise ValueError(msg)


class _RecordLikeAsLayerData:
    """A `RecordLike` read through `LayerData`'s enumerate-and-read pairs.

    The adapter that lets `write_layer` stay one code path over raw
    relations: a framework's `to_datarecord()` hands over `Relations`, one
    lookup per key exactly as `write_layer` already does, so this wraps each
    mapping rather than eagerly converting it. `con` is what each relation is
    checked to be planned on (`as_relation`).

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/record/#layerdata)
    """

    def __init__(self, source: RecordLike, con: DuckDBPyConnection) -> None:
        self._source = source
        self._con = con
        self._mappings: dict[str, Relations] = {}

    @property
    def schema(self) -> Schema:
        return self._source.schema

    @property
    def frozen(self) -> bool:
        # A framework object is read once to produce a layer, never folded
        # under a reader, so there is nothing for staleness to mean here.
        return True

    def release(self) -> None:
        """Drop what a pinned source holds for this write."""
        if isinstance(self._source, Record) and isinstance(
            self._source.resolver, PinnedResolver
        ):
            self._source.resolver.release()

    def _frames(self, kind: str) -> Relations:
        """Return the `Relations` of the source named `kind`, enumerated once per adapter.

        A `RecordLike`'s mappings may be properties that re-enumerate the whole
        source on each access, which per key would be quadratic in the keys.
        """
        if kind not in self._mappings:
            self._mappings[kind] = getattr(self._source, kind)
        return self._mappings[kind]

    def _read(self, frames: Relations, key: str) -> DuckDBPyRelation | None:
        if key not in frames:
            return None
        return as_relation(frames[key], self._con)

    def axes(self) -> set[str]:
        return set(self._frames("dims"))

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """`dims[dim]` with `properties[dim]` joined on the label, in `dims[dim]`'s row order.

        The row is numbered on `dims[dim]` alone, as `fold_axis` numbers a
        layer's, and the `ORDER BY` on it is the outermost clause, since a
        join defines no order.
        """
        axis = self._read(self._frames("dims"), dim)
        if axis is None or dim not in self._frames("properties"):
            return axis
        props = self._read(self._frames("properties"), dim)
        if props is None:
            return axis
        values = [c for c in props.columns if c not in axis.columns]
        if not values:
            return axis
        joined = (
            axis.project(star(), sql("row_number() OVER ()").alias("_row"))
            .set_alias("a")
            .join(
                props.set_alias("p"),
                null_safe("a", "p", (dim,)),
                how="left",
            )
            .project(
                col("a", "_row"),
                *(col("a", c) for c in axis.columns),
                *(col("p", c) for c in values),
            )
        )
        return joined.query("j", "SELECT * EXCLUDE (_row) FROM j ORDER BY _row")

    def groups(self) -> set[str]:
        return set(self._frames("groups"))

    def group(self, name: str) -> DuckDBPyRelation | None:
        return self._read(self._frames("groups"), name)

    def attributes(self) -> set[str]:
        return set(self._frames("attributes"))

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self._read(self._frames("attributes"), name)
