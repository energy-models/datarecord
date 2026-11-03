# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a whole record as a layer.

A `LayerData` hands over relations and this module turns them into parquet;
producing one from a framework's own object is a tool's job.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
- [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
- [module layout](https://energy-models.github.io/datarecord/design/package/)
"""

from __future__ import annotations

from functools import cached_property, partial
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from duckdb import ColumnExpression as col
from duckdb import ConstantExpression as lit
from duckdb import SQLExpression as sql
from duckdb import StarExpression as star

from datarecord.duck import as_relation, fn, null_safe, write_parquet
from datarecord.protocols import LayerData, RecordLike, Relations
from datarecord.resolve.coords import (
    Coords,
    one_of_axis,
    refuse_one_of_file,
    refuse_twice,
    refuse_unheld,
    resolve_coords,
)
from datarecord.resolve.owner_map import cast_declared, unsettled
from datarecord.resolve.record import PinnedRecord, Record
from datarecord.resolve.sources import DirectorySource, refuse_foreign_columns
from datarecord.schema.schema import DERIVED, Schema
from datarecord.store.manifest import LayerFiles, Manifest

if TYPE_CHECKING:
    from collections.abc import Callable

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from datarecord.resolve.sources import LayerSource
    from datarecord.store.store import Workspace


def layer_data(source: LayerData | RecordLike, con: DuckDBPyConnection) -> LayerData:
    """Return `source` as the `LayerData` the writer reads, planned on `con`.

    A `RecordLike` is wrapped in a thin adapter reading its `Relations`
    through the same enumerate-and-read pairs. A `Record` that is not frozen
    is read as a `PinnedRecord` over the same four fields, which folds its
    staged edits once for the write, during which nothing stages.

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/use/writing/#layerdata)
    """
    if isinstance(source, LayerData):
        return source
    if isinstance(source, Record) and not source.frozen:
        source = PinnedRecord(source.revision_id, source.sources, source.workspace)
    return _RecordLikeAsLayerData(source, con)


def write_layer(
    data: LayerData,
    base: str,
    workspace: Workspace,
    *,
    parent: Record | None = None,
) -> LayerFiles:
    """Write `data`'s files into the layer directory `base`, and return what it wrote.

    Only the data: the manifest listing the files is the caller's to write
    after, and until it is there no read reaches them. Keys are looked up one
    at a time and each file written before the next is built, so a
    lazily-building source does one read per file rather than one per key up
    front. A failed write leaves the files it wrote, which no manifest lists.

    Parameters
    ----------
    data
        The layer's contents, validated against its own schema as each frame is
        built.
    parent
        The revision's record the layer folds over; none for a root or a
        standalone record.

    Raises
    ------
    ValueError
        If a long frame is missing a long-schema column, or the schema
        declares a key dim no frame carries - either would make the fold
        misresolve the layer.
        If a multi-dim's tuple or a property that refers to a dim names
        a label dead after the layer, in a subject the layer restates or whose
        rows name a label the layer removes.
        If `data` hands over an attribute or an axis the schema does not
        declare, or the axis of a dim with `one_of`, whose axis is the
        schema's list.
        If a column's type does not fit its dtype, naming the attribute,
        property or dim that declares the column, and the dtype.
        If two rows of one attribute cover one coordinate and both name the
        same dims, or each names a dim the other leaves NULL, naming the
        attribute and the coordinate.
        If an axis file holds one label twice, naming it, or orders
        its parent's labels otherwise than the parent, naming the first label
        out of place.
        If a multi-dim's tuple names a label that the layer's final axis of
        that dim does not hold, naming the multi-dim, the coordinate and up to
        three such labels.
        If an attribute row names a label or a tuple that the layer's final
        axis of that dim does not hold, naming the dim and up to three such
        labels.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
    - [the store](https://energy-models.github.io/datarecord/design/storage/store/#the-store)
    """
    schema = data.schema
    con = workspace.con
    written: dict[str, list[str]] = {"dims": [], "attributes": []}

    def _written(
        read: Callable[[str], DuckDBPyRelation | None], kind: str, key: str
    ) -> DuckDBPyRelation | None:
        """`read(key)`, called once, validated, written and read back; `None` where it is."""
        rel = read(key)
        if rel is None:
            return None
        _validate_frame(rel, kind, key, schema)
        return con.read_parquet(_write_frame(rel, base, kind, key, schema, con))

    try:
        refuse_one_of_file(schema, data.axes(), "the layer")
        for key in sorted(data.axes(), key=schema.multi_dims.__contains__):
            frame = _written(data.axis, "dims", key)
            if frame is None:
                continue
            written["dims"].append(key)
            file = f"dims/{key}.parquet"
            refuse_twice(frame, [key], file)
            before = _parent_axis(parent, schema, key, con)
            if before is not None:
                _refuse_reordered(frame, before, key, file)
        held: Callable[[str], DuckDBPyRelation | None]
        held = partial(_parent_axis, parent, schema, con=con)
        if data.axes():
            dims = LayerFiles(dims=tuple(written["dims"]), attributes=())
            manifest = Manifest(id=uuid4(), parent=None, schema=schema, files=dims)
            layer = DirectorySource(base, manifest, workspace, manifest.id)
            parents = [] if parent is None else parent.sources
            coords = resolve_coords([*parents, layer])
            held = coords.axes.get
            for key in written["dims"]:
                if key not in schema.multi_dims:
                    continue
                file = f"dims/{key}.parquet"
                refuse_unheld(
                    coords.axes[key],
                    schema,
                    [key],
                    held,
                    file,
                    staged=False,
                    subject=key,
                )
        for key in data.attributes():
            frame = _written(data.attribute, "attributes", key)
            if frame is None:
                continue
            written["attributes"].append(key)
            file = f"attributes/{key}.parquet"
            _refuse_unordered(frame, key, schema)
            coordinates = schema.coordinates_of(key)
            refuse_unheld(frame, schema, coordinates, held, file, staged=False)
        files = LayerFiles(
            dims=tuple(written["dims"]), attributes=tuple(written["attributes"])
        )
        if data.axes():
            _require_living_references(parent, layer, coords)
    finally:
        if isinstance(data, _RecordLikeAsLayerData):
            data.release()
    return files


def _write_frame(
    rel: DuckDBPyRelation,
    base: str,
    kind: str,
    key: str,
    schema: Schema,
    con: DuckDBPyConnection,
) -> str:
    """Write `rel` as `{kind}/{key}.parquet` under `base`, unmaterialised; return its URI.

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
    - [relations](https://energy-models.github.io/datarecord/design/use/reading/#relations)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/use/writing/)
    """
    file = f"{kind}/{key}.parquet"
    uri = f"{base}{file}"
    if "://" not in uri:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
    unwritable = [c for c in DERIVED if c in rel.columns]
    if unwritable:
        rel = rel.project(star(exclude=unwritable))
    subject = key if kind == "dims" else None
    attribute = key if kind == "attributes" else None
    rel = schema.require_takes(rel, file, subject, attribute)
    rel = cast_declared(schema, rel, subject, attribute)
    write_parquet(rel, uri, con, ordered=kind == "dims")
    return uri


def _refuse_unordered(rel: DuckDBPyRelation, attribute: str, schema: Schema) -> None:
    """Refuse two rows of `attribute` covering one coordinate where neither is more specific.

    Two rows cover one coordinate where they agree on every dim both name. A
    row is more specific than another where it names every dim the other
    names, and more. Of the rows covering a coordinate, a read keeps the one
    naming every dim the others name, so where two name the same dims, or each
    names a dim the other leaves NULL and no row there names both, it has none
    to keep. A record holding rows of several layers, flattened into one,
    holds such a row wherever two of them cross. `breakpoint` is part of a
    row, so the rows of one curve are no such pair.

    Rows naming the same dims are found by `refuse_twice` over the coordinates,
    crossing ones by `unsettled`. `rel` is the file as written, so the source
    is built only once, and only its dims and `breakpoint` are read.

    Raises
    ------
    ValueError
        Naming the attribute and one coordinate both rows cover.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)
    """
    dims = schema.coordinates_of(attribute)
    refuse_twice(
        rel,
        [*dims, "breakpoint"],
        f"attributes/{attribute}.parquet",
        "they name the same dims, so neither is the more specific there. Keep "
        "one of them (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)",
    )
    for a, b, meets in unsettled(rel, dims):
        rows = meets.limit(1).fetchall()
        if not rows:
            continue
        coordinate = ", ".join(
            f"{d}={v}" for d, v in zip(dims, rows[0], strict=True) if d in a | b
        )
        msg = (
            f"attributes/{attribute}.parquet holds a row naming {[d for d in dims if d in a]} "
            f"and one naming {[d for d in dims if d in b]} that both cover {coordinate}; "
            f"neither names every dim the other does and no row there names both, "
            f"so neither is the more specific there. Name the labels of one of "
            f"them row by row, leaving that coordinate out, or add a row naming "
            f"both (https://energy-models.github.io/datarecord/design/model/attributes/#the-broadcast-rule)"
        )
        raise ValueError(msg)


def _refuse_reordered(
    rel: DuckDBPyRelation, parent: DuckDBPyRelation, key: str, file: str
) -> None:
    """Refuse a restatement ordering the labels it shares with `parent` otherwise.

    Only the relative order of the labels both hold is compared, so a new
    label may sit anywhere. The labels are ranked by each side's row order,
    and the first label, in the restatement's order, whose two ranks differ
    is named.

    Raises
    ------
    ValueError
        Naming the file and that label.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/design/model/dims/#axis-order)
    """
    mine = rel.project(col(key), sql("row_number() OVER ()").alias("_mine"))
    theirs = parent.project(col(key), sql("row_number() OVER ()").alias("_theirs"))
    shared = (
        mine.set_alias("m")
        .join(theirs.set_alias("t"), col("m", key) == col("t", key))
        .project(col("m", key).alias(key), col("_mine"), col("_theirs"))
    )
    ranked = shared.project(
        col(key),
        col("_mine"),
        sql("row_number() OVER (ORDER BY _mine)").alias("_a"),
        sql("row_number() OVER (ORDER BY _theirs)").alias("_b"),
    )
    first = (
        ranked.filter(col("_a") != col("_b"))
        .order("_mine")
        .project(col(key))
        .limit(1)
        .fetchall()
    )
    if first:
        msg = (
            f"{file} puts {key} {first[0][0]!r} out of its parent's order; a "
            f"restatement keeps the order of the labels its parent holds and "
            f"places a new one anywhere (https://energy-models.github.io/datarecord/design/model/dims/#axis-order)"
        )
        raise ValueError(msg)


def _parent_axis(
    parent: Record | None, schema: Schema, name: str, con: DuckDBPyConnection
) -> DuckDBPyRelation | None:
    """`parent`'s axis `name`, `None` where there is none; a `one_of` dim's is `schema`'s list.

    The axis every read of `parent` takes, its labels alone; a declared dim
    without rows is an empty one, which holds nothing as `None` does.
    """
    if name in schema.one_of_dims:
        return one_of_axis(schema, name, con)
    if parent is None or name not in parent.dims:
        return None
    return parent.dims[name]


def _require_living_references(
    parent: Record | None, layer: LayerSource, coords: Coords
) -> None:
    """Reject a layer leaving a reference to a dead label in a subject it touches.

    A reference is a multi-dim tuple's coordinate over a dim, or a property
    that refers to a dim. A property's subject the layer restates is
    checked whole, in its final state, against every dead label; any other
    reference only for the labels the layer removes, its parent's axis minus
    its restatement. A restated multi-dim's tuples `refuse_unheld`
    already holds to the final axes. Removing a label restates every
    multi-dim naming it without its tuples, which `WorkingRecord.remove` writes, and a
    property holding it must be cleared first; a layer written otherwise
    carries both itself. A property an ancestor left dangling thus fails the
    next layer restating its subject, which can drop or repair it.

    Raises
    ------
    ValueError
        Naming up to three such rows per reference.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/design/layers/removal/)
    """
    schema = coords.schema
    touched = layer.axes()
    references = [
        *(
            (m, f, fn.struct_extract(col("t", m), lit(f)), d, False)
            for m in schema.multi_dims
            for f, d in schema.over(m).items()
        ),
        *(
            (s, p, col("t", p), d, s in touched)
            for (s, p), d in schema.one_of_properties().items()
        ),
    ]
    referenced = {r[3] for r in references}
    whole = any(r[4] for r in references)
    killed = {}
    for dim in layer.axes() & referenced:
        before = _parent_axis(parent, schema, dim, layer.con)
        now = layer.axis(dim)
        if before is None or now is None:
            continue
        gone = (
            before.project(col(dim))
            .set_alias("b")
            .join(
                now.project(col(dim)).set_alias("n"),
                col("b", dim) == col("n", dim),
                how="anti",
            )
        )
        if gone.limit(1).fetchall():
            killed[dim] = gone
    if not whole and not killed:
        return
    problems = []
    for subject, column, label, dim, restated in references:
        dead = coords.dead.get(dim)
        resolved = coords.axes.get(subject)
        stored = subject if subject in schema.multi_dims else column
        if dead is None or resolved is None or stored not in resolved.columns:
            continue
        labels = dead if restated else killed.get(dim)
        if labels is None:
            continue
        hits = [
            h
            for (h,) in resolved.set_alias("t")
            .join(labels.set_alias("x"), label == col("x", dim), how="semi")
            .project(col(subject))
            .limit(3)
            .fetchall()
        ]
        if hits:
            problems.append(f"{subject} {hits} name a removed {dim} in {column!r}")
    if problems:
        msg = (
            f"{'; '.join(problems)}; removing a label removes the tuples naming "
            f"it and leaves no property holding it, so restate those multi-dims "
            f"without the tuples and clear those properties (https://energy-models.github.io/datarecord/design/layers/removal/)"
        )
        raise ValueError(msg)


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
    - [the Record protocol](https://energy-models.github.io/datarecord/design/use/reading/)
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/design/model/multi-dims/#attributes-over-a-multi-dim)
    - [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
    - [partial](https://energy-models.github.io/datarecord/design/layers/tree/#partial-the-granularity-of-an-override)
    """
    columns = set(rel.columns)

    if kind == "attributes":
        # An attribute's shape comes from its spec, so one the schema does not
        # declare has no shape to check it against - and writing it would put a
        # file in `attributes/` that no read path knows the columns of.
        if key not in schema.attributes:
            msg = (
                f"attributes/{key}.parquet is not a declared attribute; its `dims` "
                f"are what say which columns the file carries (https://energy-models.github.io/datarecord/design/model/attributes/#attributespec)"
            )
            raise ValueError(msg)
        required = set(schema.long_columns_for(key))
        missing = sorted(required - columns)
        if missing:
            msg = (
                f"attributes/{key}.parquet is missing long-schema columns {missing}; "
                f"the resolved relation needs {sorted(required)} (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)"
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
                f"not addressed by; its `dims` say {sorted(required)} (https://energy-models.github.io/datarecord/design/storage/format/#the-long-schema)"
            )
            raise ValueError(msg)
        schema.check_multi_dim_columns(
            schema.multi_dims_of(key), rel, carrier=f"attributes/{key}.parquet"
        )
        return

    if kind == "dims":
        if key not in schema.dimensions:
            msg = (
                f"dims/{key}.parquet is not a declared dim; a descendant "
                f"declaring it would read these labels (https://energy-models.github.io/datarecord/design/use/writing/#shape)"
            )
            raise ValueError(msg)
        if key not in columns:
            msg = f"dims/{key}.parquet is missing its key column {key!r} (https://energy-models.github.io/datarecord/design/storage/format/#where-a-value-lives)"
            raise ValueError(msg)
        refuse_foreign_columns(
            columns, {key, "order_key"}, schema, key, f"dims/{key}.parquet"
        )
        if key in schema.multi_dims:
            schema.check_multi_dim_columns([key], rel, carrier=f"dims/{key}.parquet")


class _RecordLikeAsLayerData:
    """A `RecordLike` read through `LayerData`'s enumerate-and-read pairs.

    The adapter that lets `write_layer` stay one code path over raw
    relations: a framework's `to_datarecord()` hands over `Relations`, one
    lookup per key exactly as `write_layer` already does, so this wraps each
    mapping rather than eagerly converting it. `con` is what each relation is
    checked to be planned on (`as_relation`).

    Notes
    -----
    - [LayerData](https://energy-models.github.io/datarecord/design/use/writing/#layerdata)
    """

    def __init__(self, source: RecordLike, con: DuckDBPyConnection) -> None:
        self._source = source
        self._con = con

    @property
    def schema(self) -> Schema:
        return self._source.schema

    def release(self) -> None:
        """Drop what a pinned source holds for this write."""
        if isinstance(self._source, PinnedRecord):
            self._source.release()

    @cached_property
    def _dims(self) -> Relations:
        """Return the source's `dims`, enumerated once per adapter.

        A `RecordLike`'s mappings may be properties that re-enumerate the whole
        source on each access, which per key would be quadratic in the keys, so
        `_properties` and `_attributes` are kept the same way.
        """
        return self._source.dims

    @cached_property
    def _properties(self) -> Relations:
        return self._source.properties

    @cached_property
    def _attributes(self) -> Relations:
        return self._source.attributes

    def _read(self, frames: Relations, key: str) -> DuckDBPyRelation | None:
        if key not in frames:
            return None
        return as_relation(frames[key], self._con)

    def axes(self) -> set[str]:
        """The source's dims but those with `one_of`, whose axis a `RecordLike` answers from the schema.

        Raises
        ------
        ValueError
            As `_require_one_of_axis` does.
        """
        return self._axes

    @cached_property
    def _axes(self) -> set[str]:
        one_of = set(self._dims) & set(self.schema.one_of_dims)
        for dim in sorted(one_of):
            self._require_one_of_axis(dim)
        return set(self._dims) - one_of

    def _require_one_of_axis(self, dim: str) -> None:
        """Refuse the source's axis of `dim`, a dim with `one_of`, unless it is the list.

        Dropped rather than written, it may differ from the list only by
        nothing: the same labels in the same order.

        Raises
        ------
        ValueError
            Naming `dim` and whether the axis holds NULL, the labels it
            repeats, lacks or adds, or else the order it holds them in.

        Notes
        -----
        - [a dim with one_of](https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)
        """
        rel = as_relation(self._dims[dim], self._con)
        given = [
            None if label is None else str(label)
            for (label,) in rel.project(col(dim)).fetchall()
        ]
        expected = list(self.schema.dimensions[dim].one_of or ())
        if given == expected:
            return
        labels = [label for label in given if label is not None]
        repeated = [label for label in dict.fromkeys(labels) if labels.count(label) > 1]
        missing = [label for label in expected if label not in labels]
        extra = [label for label in dict.fromkeys(labels) if label not in expected]
        problems = {
            "holds NULL": None in given,
            f"repeats {repeated}": repeated,
            f"lacks {missing}": missing,
            f"adds {extra}": extra,
        }
        found = [problem for problem, hit in problems.items() if hit]
        difference = " and ".join(found) or f"holds the labels in the order {given}"
        msg = (
            f"the source's axis of {dim!r} {difference}, but {dim!r} lists its "
            f"labels with `one_of`, {expected}, so its axis is the schema's "
            f"list and is written only as it stands "
            f"(https://energy-models.github.io/datarecord/design/model/dims/#a-dim-with-one_of)"
        )
        raise ValueError(msg)

    def axis(self, dim: str) -> DuckDBPyRelation | None:
        """`dims[dim]` with `properties[dim]` joined on the label, in `dims[dim]`'s row order.

        The row is numbered on `dims[dim]` alone, and the `ORDER BY` on it is
        the outermost clause, since a join defines no order.
        """
        axis = self._read(self._dims, dim)
        if axis is None or dim not in self._properties:
            return axis
        props = self._read(self._properties, dim)
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

    def attributes(self) -> set[str]:
        return set(self._attributes)

    def attribute(self, name: str) -> DuckDBPyRelation | None:
        return self._read(self._attributes, name)
