# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a layer from long-format frames.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
"""

import json
import re
import shutil
from itertools import permutations
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from pydantic import ValidationError

from datarecord import Workspace
from datarecord.mutable import WorkingRecord
from datarecord.protocols import EMPTY, LazyRelations
from datarecord.resolve.sources import ParquetLayer
from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec
from datarecord.store.manifest import LayerFiles, Manifest
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    broadcast,
    empty_child,
    empty_root,
    export_network,
    relation,
    schema,
    unpacked,
    write_axis,
)


class _Source:
    """A minimal `Record` over ready-made frames, counting each build.

    Without `connections`, each multi-dim holds the tuples the attributes
    name, and without `dims`, each dim's axis holds the labels the attributes
    and multi-dims name, so a write meets no label or tuple it lacks.
    `connections` are the multi-dims' frames, handed over among `dims`.
    """

    def __init__(
        self,
        schema,
        attributes=None,
        connections=None,
        dims=None,
        properties=None,
    ):
        self._schema = schema
        self._con = duckdb.connect()
        self.built: list[str] = []
        self._attributes = attributes or {}
        connections = (
            _named_multi_dims(schema, self._attributes)
            if connections is None
            else connections
        )
        named = {**self._attributes, **connections}
        axes = _named_axes(schema, named) if dims is None else dims
        self._dims = {**axes, **connections}
        self._properties = properties or {}

    @property
    def schema(self):
        return self._schema

    @property
    def con(self):
        return self._con

    def _frames(self, mapping, tag):
        def build(key):
            self.built.append(f"{tag}:{key}")
            return mapping[key]

        return LazyRelations(tuple(mapping), build)

    @property
    def dimensions(self):
        return self._frames(self._dims, "dimensions") if self._dims else EMPTY

    @property
    def properties(self):
        return self._frames(self._properties, "properties")

    @property
    def attributes(self):
        return self._frames(self._attributes, "attributes")

    def flags(self, **categories):
        return {}


def _named_multi_dims(schema, attributes) -> dict[str, pd.DataFrame]:
    """Return each multi-dim's tuples that the `attributes` frames name, as its file."""
    named: dict[str, dict] = {}
    for frame in attributes.values():
        for column in frame.columns:
            if column in schema.multi_dimensions:
                tuples = named.setdefault(column, {})
                tuples.update((tuple(t.items()), t) for t in frame[column].dropna())
    return {g: pd.DataFrame({g: list(t.values())}) for g, t in named.items()}


def _named_axes(schema, frames) -> dict[str, pd.DataFrame]:
    """Return each dim's labels that the `frames` name, as its axis."""
    named: dict[str, list] = {}
    for frame in frames.values():
        for column in frame.columns:
            if column in schema.multi_dimensions:
                for field, dim in schema.over(column).items():
                    tuples = frame[column].dropna()
                    named.setdefault(dim, []).extend(t.get(field) for t in tuples)
            elif column in schema.dimensions:
                named.setdefault(column, []).extend(frame[column])
    axes = {
        d: pd.Series(labels).dropna().drop_duplicates() for d, labels in named.items()
    }
    return {d: pd.DataFrame({d: labels}) for d, labels in axes.items() if len(labels)}


_SCHEMA = schema()
_CTYPES = list(_SCHEMA.dimensions["ctype"].one_of or ())


def _long(**overrides) -> pd.DataFrame:
    """One long row carrying a column per dim of its attribute.

    Shaped from the spec rather than spelled: a source handing over a column
    the attribute is not addressed by is what the writer rejects, so a
    helper that spelled every declared dim would be testing against a record no
    reader would accept.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    row = {
        "connection": None,
        "entity": "steel_dri",
        "bus": None,
        "snapshot": None,
        "scenario": None,
        "period": None,
        "ctype": None,
        "attribute": "p_nom",
        "breakpoint": None,
        "value": 1.0,
    }
    row.update(overrides)
    columns = _SCHEMA.long_columns_for(str(row["attribute"]))
    return pd.DataFrame([{c: row[c] for c in columns}])


# -- the lazy mapping (https://energy-models.github.io/datarecord/reference/format/) -------------------------------------------------


def test_a_write_builds_each_key_once(store):
    """The writer looks up every key exactly once, and only what it writes."""
    source = _Source(
        _SCHEMA,
        attributes={"p_nom": _long(), "e_nom": _long(attribute="e_nom")},
        dims={"entity": pd.DataFrame({"entity": ["steel_dri"]})},
        properties={
            "entity": pd.DataFrame({"entity": ["steel_dri"], "ctype": ["Process"]})
        },
    )
    store.commit(source, parent=None)

    assert sorted(source.built) == [
        "attributes:e_nom",
        "attributes:p_nom",
        "dimensions:entity",
        "properties:entity",
    ]


# -- creating a layer -------------------------------------------------------


def test_a_write_creates_a_new_layer(store):
    """Files land where `layer_dir` says, beside the claim and the manifest listing them.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/reference/format/)
    """
    revision = store.commit(
        _Source(_SCHEMA, attributes={"p_nom": _long()}), parent=None, meta={"run": 1}
    )

    base = Path(store.layer_dir(revision.id))
    assert (base / "attributes" / "p_nom.parquet").exists()
    assert (base / "claim.json").exists()
    manifest = Manifest.read(store.files, str(base / "manifest.json"), {})
    assert manifest == Manifest(
        id=revision.id,
        parent=None,
        schema=_SCHEMA,
        files=LayerFiles(dimensions=("entity",), attributes=("p_nom",)),
        meta={"run": 1},
    ), "the full schema and every file written, by name"
    assert revision.manifest == manifest
    assert [r.id for r in store.revisions()] == [revision.id], (
        "the manifest makes it real"
    )


def test_a_write_rejects_a_category_its_dim_does_not_list(store):
    entities = pd.DataFrame({"entity": ["a"], "ctype": ["Nope"]})
    with pytest.raises(Exception, match="Nope"):
        store.commit(
            _Source(
                _SCHEMA,
                dims={"entity": entities[["entity"]]},
                properties={"entity": entities},
            ),
            parent=None,
        )
    _nothing_reachable(store)


def _with_ctype_axis(labels) -> _Source:
    """A record answering `labels` as its axis of `ctype`, a dim with `one_of`."""
    return _Source(
        _SCHEMA,
        attributes={"p_nom": _long()},
        dims={
            "entity": pd.DataFrame({"entity": ["steel_dri"]}),
            "ctype": pd.DataFrame({"ctype": labels}),
        },
    )


def test_a_dim_with_one_of_has_no_layer_file(store, tmp_path):
    """Its axis is the schema's list: no write, layer or resolved files hold a file of it.

    A `RecordLike` answers every dim's axis, the list of a dim with `one_of`
    included, and the writer takes it without writing it.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
    """
    revision = store.commit(_with_ctype_axis(_CTYPES), parent=None)
    assert revision.record.dimensions["ctype"].df()["ctype"].tolist() == _CTYPES
    revision.materialise()
    out = tmp_path / "flat"
    store.workspace.write(revision.record, str(out))
    written = [
        Path(store.layer_dir(revision.id)),
        Path(store.resolved_dir(revision.id)),
        out,
    ]
    assert not [d for d in written if (d / "dimensions" / "ctype.parquet").exists()], (
        "neither the layer, its resolved files nor a record written whole holds the list"
    )

    layer = empty_root(store, _SCHEMA)
    write_axis(layer, "ctype", [{"ctype": "Process"}])
    forged = layer.commit()
    with pytest.raises(ValueError, match=r"holds an axis file of \['ctype'\]"):
        store.commit(ParquetLayer(forged.manifest, store), parent=None)
    with pytest.raises(ValueError, match=r"holds an axis file of \['ctype'\]"):
        forged.record.dimensions["ctype"]


@pytest.mark.parametrize(
    ("labels", "match"),
    [
        ([*reversed(_CTYPES)], r"'ctype' holds the labels in the order"),
        (
            [None, *_CTYPES[1:], "Nope", "Nope"],
            rf"'ctype' holds NULL and repeats \['Nope'\] and lacks "
            rf"\['{_CTYPES[0]}'\] and adds \['Nope'\], but",
        ),
    ],
    ids=["reordered", "several"],
)
def test_a_record_handing_over_another_one_of_axis_is_refused(store, labels, match):
    """Dropping it would lose what it says; the list is the axis, so it is refused.

    Notes
    -----
    - [a dim with one_of](https://energy-models.github.io/datarecord/reference/data-model/#a-dim-with-one_of)
    """
    with pytest.raises(ValueError, match=match):
        store.commit(_with_ctype_axis(labels), parent=None)
    _nothing_reachable(store)


def test_a_directory_target_carries_its_own_schema(workspace, tmp_path):
    """A standalone record *is* one record, so its schema goes in the directory.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/reference/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    record = workspace.write(source, out)

    assert (Path(out) / "manifest.json").exists()
    assert record.schema == _SCHEMA
    assert workspace.at(out).schema == record.schema

    # And it is that file answering: a standalone record is one whole record,
    # so it reads the same through a workspace that knows nothing about it.
    elsewhere = Workspace()
    try:
        again = elsewhere.at(out)
        assert again.schema == record.schema
        assert again.revision_id == record.revision_id, (
            "the manifest's id names one layer to every reader"
        )
    finally:
        elsewhere.close()


@pytest.mark.parametrize(
    ("spoil", "match"),
    [
        pytest.param(
            lambda raw: raw["schema"].update(misspelt=1),
            "misspelt",
            id="a-misspelt-schema-key",
        ),
        pytest.param(lambda raw: raw.pop("id"), "id\n  Field required", id="no-id"),
    ],
)
def test_a_cold_read_refuses_a_spoilt_manifest(workspace, tmp_path, spoil, match):
    """A manifest without an id leaves the record's folds nothing to be keyed by.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/reference/format/)
    """
    out = tmp_path / "standalone"
    workspace.write(_p_nom(1.0), str(out))
    raw = json.loads((out / "manifest.json").read_text())
    spoil(raw)
    (out / "manifest.json").write_text(json.dumps(raw))
    with pytest.raises(ValidationError, match=match):
        workspace.at(str(out))


def test_a_manifest_carries_its_layers_id(store, tmp_path):
    """The id is the manifest's, in the file and on the record read from it.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/reference/format/)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    out = tmp_path / "standalone"
    record = store.workspace.write(source, str(out), meta={"run": "a"})

    written = Manifest.read(store.files, str(out / "manifest.json"), {})
    assert written is not None
    assert (written.id, written.parent, written.meta) == (
        record.revision_id,
        None,
        {"run": "a"},
    )

    copy = store.workspace.write(record, str(tmp_path / "copy"))
    assert copy.revision_id != record.revision_id, "a copy mints its own id"

    revision = store.commit(record, parent=None)
    assert revision.manifest.id == revision.id != record.revision_id


def test_a_layer_of_a_tree_is_no_standalone_record(store, tmp_path):
    """`Workspace.at` refuses a layer with a parent: that is read through its store.

    A directory with no manifest holds no record at all.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/reference/format/)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    root = store.commit(source, parent=None)
    child = store.commit(source, parent=root)
    with pytest.raises(ValueError, match=r"`layer\(id\)`"):
        store.workspace.at(store.layer_dir(child.id))
    assert store.layer(child.id).schema == _SCHEMA
    with pytest.raises(FileNotFoundError, match="no manifest"):
        store.workspace.at(str(tmp_path / "layers"))


def test_a_standalone_write_refuses_an_existing_directory(workspace, tmp_path):
    """Refused up front, empty or holding a record, so nothing is half-replaced.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/reference/store/#a-standalone-directory)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, str(tmp_path))
    assert source.built == [], "refused before any frame was built"
    assert list(tmp_path.iterdir()) == [], "refused before anything was written"

    out = str(tmp_path / "new")
    record = workspace.write(source, out)
    assert record.schema == _SCHEMA
    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, out)


def test_a_failed_standalone_write_leaves_no_record(workspace, tmp_path):
    """The manifest is written last, so a directory whose write failed is none.

    Its data files stay, and the URI takes a record once the caller deletes them.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/reference/store/#a-standalone-directory)
    """
    out = tmp_path / "standalone"
    short = _long().drop(columns=["breakpoint"])
    with pytest.raises(ValueError, match="missing long-schema columns"):
        workspace.write(_Source(_SCHEMA, attributes={"p_nom": short}), str(out))
    assert not (out / "manifest.json").exists()
    with pytest.raises(FileNotFoundError):
        workspace.at(str(out))
    with pytest.raises(FileExistsError):
        workspace.write(_p_nom(1.0), str(out))
    shutil.rmtree(out)
    assert _read_p_nom(workspace.write(_p_nom(1.0), str(out))) == [1.0]


def _p_nom(value: float) -> _Source:
    return _Source(_SCHEMA, attributes={"p_nom": _long(value=value)})


def _read_p_nom(record) -> list[float]:
    return record.attributes["p_nom"].df()["value"].tolist()


def test_a_record_written_again_at_one_uri_reads_as_itself(workspace, tmp_path):
    """The workspace shares fold tables by layer id, so each write mints its own.

    An id derived from the URI would key the second record's folds as the
    first's, and serve the deleted record's values.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/reference/store/#a-standalone-directory)
    """
    out = tmp_path / "standalone"
    assert _read_p_nom(workspace.write(_p_nom(1.0), str(out))) == [1.0]
    shutil.rmtree(out)
    assert _read_p_nom(workspace.write(_p_nom(2.0), str(out))) == [2.0], (
        "the new record, not the fold of the one deleted"
    )


def test_a_relative_uri_is_read_where_it_was_resolved(workspace, tmp_path, monkeypatch):
    """One relative path names two records from two working directories.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/reference/store/#a-standalone-directory)
    """
    for name, value in [("a", 1.0), ("b", 2.0)]:
        (tmp_path / name).mkdir()
        monkeypatch.chdir(tmp_path / name)
        assert _read_p_nom(workspace.write(_p_nom(value), "out")) == [value], (
            f"the record written under {name}/"
        )
    assert _read_p_nom(workspace.at("out")) == [2.0], "read from the cwd, b/"
    monkeypatch.chdir(tmp_path / "a")
    assert _read_p_nom(workspace.at("out")) == [1.0], "read from the cwd, a/"


def _nothing_reachable(store) -> None:
    """A failed tree write leaves its layer pending: claimed, with no manifest."""
    assert store.revisions() == [], "no manifest, so nothing reaches a layer"
    assert [p.name for p in Path(store.uri, "layers").glob("*/*.json")] == [
        "claim.json"
    ]


def test_a_file_carries_only_its_own_attributes_dims(store):
    """One attribute is one file, so one column set - not every declared dim.

    A component attribute has no `bus` column, a connection attribute does, and
    neither carries a dim it is not addressed by. The uniform prefix this
    replaces put an all-NULL `bus` on every file and a `period` column on
    attributes that never vary over one.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    """
    connection = {"entity": "steel_dri", "bus": "h2_north"}
    revision = store.commit(
        _Source(
            _SCHEMA,
            attributes={
                "p_max_pu": _long(
                    attribute="p_max_pu", snapshot=pd.Timestamp("2030-01-01")
                ),
                "efficiency": _long(attribute="efficiency", connection=connection),
                "weight": _long(attribute="weight", scenario="s1"),
            },
        ),
        parent=None,
    )
    inputs = Path(store.layer_dir(revision.id), "attributes")

    def columns(attribute: str) -> set[str]:
        return set(
            store.workspace.con.read_parquet(
                str(inputs / f"{attribute}.parquet")
            ).columns
        )

    assert columns("p_max_pu") == {
        "entity",
        "snapshot",
        "scenario",
        "period",
        "attribute",
        "breakpoint",
        "value",
    }, "a component attribute carries `entity`, not the connection multi-dim's `bus`"

    efficiency = columns("efficiency")
    assert "connection" in efficiency, "a connection attribute carries the multi-dim"
    assert "bus" not in efficiency, "as one struct column, not its fields"
    assert columns("weight") & {"entity", "period"} == set(), (
        "and no dim it is not addressed by"
    )
    axis = store.workspace.con.read_parquet(
        str(Path(store.layer_dir(revision.id), "dimensions", "entity.parquet"))
    )
    assert "p_max_pu" not in axis.columns, "a single-dim attribute is no axis column"


# -- validation -------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "match"),
    [
        (_Source(_SCHEMA, attributes={"not_declared": _long()}), "attribute"),
        (
            _Source(_SCHEMA, dims={"region": pd.DataFrame({"region": ["north"]})}),
            "dim",
        ),
    ],
    ids=["attribute", "dim"],
)
def test_a_write_rejects_an_undeclared_attribute_or_dim(store, source, match):
    """A file the schema does not declare is refused rather than listed.

    An attribute with no spec has no shape to write it in, and an axis no dim
    declares would be read by a descendant declaring that dim.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/reference/schema/#attributes)
    - [shape](https://energy-models.github.io/datarecord/reference/layers/#what-a-layer-may-contain)
    """
    with pytest.raises(ValueError, match=f"not a declared {match}"):
        store.commit(source, parent=None)


def test_a_write_rejects_a_dim_the_attribute_lacks(store):
    """A column the attribute is not addressed by is a disagreement, not a spare.

    The read path projects a column per dim of an attribute, so a `bus` on a
    component attribute would be written and never read - and a source emitting
    one means something different by the attribute than the schema does.
    Reported rather than dropped, since silently narrowing would hide that.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/reference/format/#attributesattrparquet)
    """
    wide = _long().assign(bus=None)
    source = _Source(_SCHEMA, attributes={"p_nom": wide})

    with pytest.raises(
        ValueError, match=r"carries columns \['bus'\].*not addressed by"
    ):
        store.commit(source, parent=None)


def test_a_write_rejects_a_multi_dim_column_not_of_its_fields(store):
    """A multi-dim's column is a struct of exactly its fields.

    DuckDB casts a struct by field name, so a missing field would be written as
    NULL rather than refused, relabelling every row.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
    """
    row = {"attribute": "efficiency", "breakpoint": None, "value": 1.0}
    row |= {"snapshot": None, "scenario": None, "period": None}
    short = pd.DataFrame([{"connection": {"entity": "steel_dri"}, **row}])
    source = _Source(_SCHEMA, attributes={"efficiency": short})

    with pytest.raises(ValueError, match=r"'connection' as .*\['entity', 'bus'\]"):
        store.commit(source, parent=None)


_WHOLE = {"entity": "steel_dri", "bus": "h2_north"}
_PARTIAL = {"entity": "steel_dri", "bus": None}


def _connection_source(tuples: list[dict], attributes=None) -> _Source:
    """A `_Source` whose `connection` axis holds `tuples`, its dims what they name."""
    connections = {"connection": pd.DataFrame({"connection": tuples})}
    dims = _named_axes(_SCHEMA, connections)
    return _Source(_SCHEMA, attributes, connections, dims)


@pytest.mark.parametrize(
    ("source", "file"),
    [
        (lambda: _connection_source([_PARTIAL]), "dimensions/connection"),
        (
            lambda: _connection_source(
                [_WHOLE],
                {"efficiency": _long(attribute="efficiency", connection=_PARTIAL)},
            ),
            "attributes/efficiency",
        ),
    ],
    ids=["dimensions", "attributes"],
)
def test_a_write_rejects_a_tuple_with_some_fields_null(store, source, file):
    """A multi-dim value is a whole tuple or NULL, checked on the file the layer wrote.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    with pytest.raises(
        ValueError,
        match=rf"{file}\.parquet carries a 'connection' with some fields NULL",
    ):
        store.commit(source(), parent=None)


def test_a_write_reads_its_source_once(store):
    """A connection attribute is scanned by the write alone; its checks read the file.

    A `RecordBatchReader` can be scanned once, so a second scan of it raises.
    """
    tuples = [{"entity": e, "bus": "h2"} for e in ("a", "b", "c")]
    frame = pa.Table.from_pandas(
        pd.concat(_long(attribute="efficiency", connection=t) for t in tuples),
        preserve_index=False,
    )
    reader = pa.RecordBatchReader.from_batches(frame.schema, frame.to_batches())
    rel = store.workspace.con.from_arrow(reader)

    revision = store.commit(
        _connection_source(tuples, {"efficiency": rel}), parent=None
    )

    efficiency = revision.record.attributes["efficiency"]
    assert len(efficiency.fetchall()) == len(tuples), "the write landed every row"


def test_a_write_rejects_a_missing_long_column(store):
    """A frame the fold could not resolve is refused before anything is written."""
    short = _long().drop(columns=["breakpoint"])
    source = _Source(_SCHEMA, attributes={"p_nom": short})

    with pytest.raises(ValueError, match="missing long-schema columns.*breakpoint"):
        store.commit(source, parent=None)
    _nothing_reachable(store)


def test_a_write_rejects_a_multi_dim_frame_without_its_struct_column(store):
    """A multi-dim's axis file has the struct of its fields as its column, named for it.

    Notes
    -----
    - [multi-dims](https://energy-models.github.io/datarecord/reference/data-model/#multi-dims)
    """
    frame = pd.DataFrame({"entity": ["steel_dri"], "bus": ["h2"]})
    source = _Source(_SCHEMA, connections={"connection": frame})

    with pytest.raises(ValueError, match="missing its column 'connection'"):
        store.commit(source, parent=None)


def _add_junk(store, axes):  # noqa: ARG001
    staged = _rows_staged(axes, [], "p_nom")
    staged.add("entity", ["c"], junk=1)


def _write_junk(store, axes):  # noqa: ARG001
    frame = pd.DataFrame({"scenario": ["high"], "junk": [1]})
    try:
        store.commit(_Source(_SCHEMA, dims={"scenario": frame}), parent=None)
    finally:
        _nothing_reachable(store)


def _read_junk(store, axes):  # noqa: ARG001
    root = empty_root(store, _SCHEMA)
    write_axis(root, "scenario", [{"scenario": "high", "junk": 1}])
    root.commit().record.dimensions["scenario"].fetchall()


@pytest.mark.parametrize(
    ("write", "accepted"),
    [
        pytest.param(_add_junk, "['ctype', 'entity']", id="add"),
        pytest.param(_write_junk, "['order_key', 'scenario']", id="write"),
        pytest.param(_read_junk, "['scenario']", id="read"),
    ],
)
def test_an_undeclared_column_is_refused(store, axes, write, accepted):
    """A column no declaration accounts for has no dtype and no meaning, so it is refused, not dropped.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
    """
    with pytest.raises(ValueError, match=r"\['junk'\]") as raised:
        write(store, axes)
    assert accepted in str(raised.value), "the error names the columns it accepts"


def _written_axis(store, frame):
    store.commit(_Source(_SCHEMA, dims={"scenario": frame}), parent=None)


def _read_axis(store, frame):
    root = empty_root(store, _SCHEMA)
    target = root.dir / "dimensions"
    target.mkdir(parents=True)
    frame.to_parquet(target / "scenario.parquet", index=False)
    root.commit().record.dimensions["scenario"].fetchall()


@pytest.fixture
def split_passes(request, monkeypatch):
    """Find a row held twice in one pass over the file, or across one pass per row."""
    if request.param:
        monkeypatch.setattr("datarecord.resolve.axes._ROWS_PER_PASS", 1)


@pytest.mark.parametrize(
    ("split_passes", "by"),
    [
        pytest.param(False, _written_axis, id="one-pass-write"),
        pytest.param(True, _read_axis, id="a-pass-per-row-read"),
    ],
    indirect=["split_passes"],
)
@pytest.mark.usefixtures("split_passes")
def test_an_axis_file_holding_a_label_twice_is_refused(store, by):
    """A label held twice has no one row.

    Notes
    -----
    - [deletion](https://energy-models.github.io/datarecord/reference/layers/#removing-labels)
    """
    with pytest.raises(ValueError, match=re.escape("holds 2 rows at scenario=high;")):
        by(store, pd.DataFrame({"scenario": ["high", "low", "high"]}))


def _scenarios(record) -> list[str]:
    return record.dimensions["scenario"].df()["scenario"].tolist()


def _xyz(store):
    """A root whose `scenario` axis is `x, y, z`."""
    frame = pd.DataFrame({"scenario": ["x", "y", "z"]})
    return store.commit(_Source(_SCHEMA, dims={"scenario": frame}), parent=None)


@pytest.mark.parametrize(
    "order",
    [["x", "new", "y", "z"], ["z"]],
)
def test_a_restatement_in_its_parents_order_is_written(store, order):
    """Only the parent's labels' relative order is checked, so a new label sits anywhere.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/reference/data-model/#dims-and-labels)
    """
    restated = _Source(_SCHEMA, dims={"scenario": pd.DataFrame({"scenario": order})})
    root = _xyz(store)
    assert _scenarios(store.commit(restated, parent=root).record) == order


@pytest.mark.parametrize(
    ("order", "first"),
    [(["y", "new", "x"], "y"), (["x", "z", "y"], "z")],
)
def test_a_restatement_reordering_its_parents_labels_is_refused(store, order, first):
    """Sorting it into the parent's order would bring a per-label fold back at write.

    Notes
    -----
    - [axis order](https://energy-models.github.io/datarecord/reference/data-model/#dims-and-labels)
    """
    root = _xyz(store)
    restated = _Source(_SCHEMA, dims={"scenario": pd.DataFrame({"scenario": order})})
    with pytest.raises(ValueError, match=f"scenario '{first}' out of its parent's"):
        root.store.commit(restated, parent=root)


def test_an_attribute_column_in_a_dims_frame_is_refused(store):
    """`weight` over `scenario` alone is an attribute, so long rows, never an axis column.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/reference/format/#where-a-value-lives)
    """
    source = _Source(
        schema(),
        dims={"scenario": pd.DataFrame({"scenario": ["high"], "weight": [0.4]})},
    )

    with pytest.raises(ValueError, match="they are attributes, whose values are long"):
        store.commit(source, parent=None)
    _nothing_reachable(store)


_ENTITIES, _SCENARIOS, _T = ["a", "b"], ["x", "y"], pd.Timestamp("2030-01-01")
_AB, _BB = {"entity": "a", "bus": "b0"}, {"entity": "b", "bus": "b0"}
_BUSES = ["b0", "b1"]


def _axes(store, tuples=(_AB, _BB)):
    """Return a written base holding `_ENTITIES`, `_BUSES`, `_SCENARIOS`, `_T` and `tuples`."""
    base = empty_root(store, _SCHEMA).commit()
    staged = WorkingRecord(base.record)
    staged.add("entity", _ENTITIES, ctype="Generator")
    staged.add("bus", _BUSES)
    staged.add("scenario", _SCENARIOS)
    staged.add("snapshot", [_T])
    staged.add("connection", list(tuples))
    return base.store.commit(staged)


@pytest.fixture(scope="module")
def axes(tmp_path_factory):
    """`_axes` once for the module; a test only ever commits beside or below it."""
    workspace = Workspace()
    yield _axes(workspace.store(str(tmp_path_factory.mktemp("axes"))))
    workspace.close()


def _rows_staged(base, rows: list[dict], attribute: str):
    """Return a staging area over `base` holding `rows` of `attribute`.

    A dim a row leaves out is NULL there; a `connection` is set by its fields.
    """
    staged = WorkingRecord(base.record)
    for row in rows:
        keywords = {c: v for c, v in row.items() if c not in ("value", "connection")}
        staged.set(attribute, row["value"], **keywords, **row.get("connection", {}))
    return staged


def _rows_layer(
    axes, rows: list[dict], via: str, attribute: str = "p_nom", tuples=(_AB, _BB)
):
    """Return a revision whose top layer holds `rows` of `attribute`, written `via` `set` or a source.

    A dim a row leaves out is NULL there. Written in `axes`' store, over
    `axes` itself where `tuples` are its own.
    """
    store = axes.store
    if via == "set":
        base = axes if tuples == (_AB, _BB) else _axes(store, tuples)
        return store.commit(_rows_staged(base, rows, attribute))
    nulls = dict.fromkeys(("entity", "connection", "scenario", "snapshot"))
    frame = pd.concat(
        [_long(**{**nulls, "attribute": attribute, **row}) for row in rows],
        ignore_index=True,
    )
    return store.commit(
        _Source(
            _SCHEMA,
            attributes={attribute: frame},
            connections={"connection": pd.DataFrame({"connection": list(tuples)})},
            dims={
                "entity": pd.DataFrame({"entity": _ENTITIES}),
                "bus": pd.DataFrame({"bus": _BUSES}),
                "scenario": pd.DataFrame({"scenario": _SCENARIOS}),
                "snapshot": pd.DataFrame({"snapshot": [_T]}),
            },
            properties={
                "entity": pd.DataFrame(
                    {"entity": _ENTITIES, "ctype": ["Generator"] * 2}
                )
            },
        ),
        parent=None,
    )


def _read(revision, attribute: str = "p_nom") -> dict[tuple, float]:
    """Return `attribute` resolved per `(entity, scenario)`, a connection read by its entity."""
    first = "connection" if attribute == "efficiency" else "entity"
    resolved = broadcast(revision.record, attribute, first, "scenario")
    return {
        (c[0] if isinstance(c, tuple) else c, s): v for (c, s), v in resolved.items()
    }


@pytest.mark.parametrize(
    ("via", "attribute", "rows", "read"),
    [
        pytest.param(
            "set",
            "p_nom",
            [{"value": 2.0}, {"entity": "a", "value": 1.0}],
            {("a", "x"): 1.0, ("a", "y"): 1.0, ("b", "x"): 2.0, ("b", "y"): 2.0},
            id="broadcast-and-specific-set",
        ),
        pytest.param(
            "write_layer",
            "p_nom",
            [
                {"value": 3.0},
                {"entity": "a", "value": 2.0},
                {"entity": "a", "scenario": "x", "value": 1.0},
            ],
            {("a", "x"): 1.0, ("a", "y"): 2.0, ("b", "x"): 3.0, ("b", "y"): 3.0},
            id="three-levels-write_layer",
        ),
        pytest.param(
            "write_layer",
            "p_nom",
            [
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"scenario": "y", "snapshot": _T, "value": 2.0},
            ],
            {("a", "x"): 1.0, ("a", "y"): 2.0, ("b", "y"): 2.0},
            id="neither-but-disjoint-write_layer",
        ),
        pytest.param(
            "write_layer",
            "efficiency",
            [{"value": 0.5}, {"connection": _AB, "value": 0.9}],
            {("a", "x"): 0.9, ("a", "y"): 0.9, ("b", "x"): 0.5, ("b", "y"): 0.5},
            id="connection-write_layer",
        ),
    ],
)
def test_a_layer_reads_its_more_specific_row(axes, via, attribute, rows, read):
    """Within one layer, a row naming a label beats one broadcasting over its dim.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    got = _read(_rows_layer(axes, rows, via, attribute), attribute)
    assert got == read, "one value per coordinate, the most specific row's"


_ALL_TWO = {("a", "x"): 2.0, ("a", "y"): 2.0, ("b", "x"): 2.0, ("b", "y"): 2.0}


@pytest.mark.parametrize(
    ("attribute", "rows", "read"),
    [
        ("p_nom", [{"entity": "a", "value": 1.0}, {"value": 2.0}], _ALL_TWO),
        (
            "p_nom",
            [
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"ctype": "Generator", "value": 2.0},
            ],
            _ALL_TWO,
        ),
        ("efficiency", [{"connection": _AB, "value": 1.0}, {"value": 2.0}], _ALL_TWO),
        (
            "p_nom",
            [
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"scenario": "y", "value": 2.0},
            ],
            {("a", "x"): 1.0, ("a", "y"): 2.0, ("b", "y"): 2.0},
        ),
    ],
    ids=["broadcast-after-specific", "category-keyword", "multi-dim", "not-covering"],
)
def test_a_later_set_replaces_what_it_covers(axes, attribute, rows, read):
    """A `set` drops the earlier staged rows its own rows cover, and keeps the rest.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/reference/editing/#set)
    """
    got = _read(_rows_layer(axes, rows, "set", attribute), attribute)
    assert got == read, "the later set wins where it covers"


_T2 = pd.Timestamp("2031-01-01")


def _child(axes, attribute: str = "p_nom", connections=None, **row):
    """Write one `attribute` row, or `connections` alone, as a child layer holding no axis, over `_rows_layer`'s."""
    base = _rows_layer(axes, [{"entity": "a", "value": 1.0}], "write_layer")
    if connections is not None:
        frame = pd.DataFrame({"connection": connections})
        return base.store.commit(
            _Source(_SCHEMA, connections={"connection": frame}, dims={}), parent=base
        )
    nulls = dict.fromkeys(("entity", "connection", "scenario", "snapshot"))
    frame = _long(**{**nulls, "attribute": attribute, **row})
    return base.store.commit(
        _Source(_SCHEMA, attributes={attribute: frame}, connections={}, dims={}),
        parent=base,
    )


_OUTSIDE_PARTIAL = {"entity": "a", "snapshot": _T2, "value": 1.0}


@pytest.mark.parametrize(
    ("via", "row", "dim"),
    [
        pytest.param("set", {"entity": "c", "value": 1.0}, "entity", id="partial-set"),
        pytest.param(
            "child", {"entity": "c", "value": 1.0}, "entity", id="partial-child"
        ),
        pytest.param("set", _OUTSIDE_PARTIAL, "snapshot", id="outside-partial-set"),
        pytest.param(
            "write_layer",
            _OUTSIDE_PARTIAL,
            "snapshot",
            id="outside-partial-write_layer",
        ),
    ],
)
def test_a_value_at_a_label_its_axis_lacks_is_refused(axes, via, row, dim):
    """`set` judges the axis as staged, the writer the layer's final axis, its parent's where it writes none.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    with pytest.raises((KeyError, ValueError), match=rf"names '{dim}' labels \["):
        if via == "child":
            _child(axes, **row)
        else:
            _rows_layer(axes, [row], via)


_A1 = {"entity": "a", "bus": "b1"}


@pytest.mark.parametrize("via", ["set", "child"])
def test_a_value_at_a_tuple_its_multi_dim_lacks_is_refused(axes, via):
    """`a` and `b1` are labels of their axes, but no `connection` tuple pairs them.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    row = {"connection": _A1, "value": 1.0}
    with pytest.raises((KeyError, ValueError), match=r"names 'connection' tuples \["):
        if via == "child":
            _child(axes, "efficiency", **row)
        else:
            _rows_layer(axes, [row], via, "efficiency")


def test_set_at_a_tuple_naming_an_unknown_label_names_the_label(axes):
    """The label is checked before the tuple, so the error names the bus, not the tuple.

    Notes
    -----
    - [validation](https://energy-models.github.io/datarecord/reference/editing/#validation)
    """
    row = {"connection": {"entity": "a", "bus": "zz"}, "value": 1.0}
    with pytest.raises(
        KeyError, match=r"names 'bus' labels \['zz'\] in 'connection.bus'"
    ):
        _rows_layer(axes, [row], "set", "efficiency")


@pytest.mark.parametrize("via", ["set", "child"])
def test_a_tuple_naming_a_label_no_axis_holds_is_refused(axes, via):
    """`add` judges the axis as staged, the writer the layer's final axis, its parent's where it writes none.

    Notes
    -----
    - [add / remove](https://energy-models.github.io/datarecord/reference/editing/#add-and-remove)
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    tuples = [_AB, _BB, {"entity": "a", "bus": "zz"}]
    with pytest.raises(
        (KeyError, ValueError),
        match=r"names 'bus' labels \['zz'\] in 'connection.bus'",
    ):
        if via == "child":
            _child(axes, connections=tuples)
        else:
            _rows_layer(axes, [{"entity": "a", "value": 1.0}], via, tuples=tuples)


def test_a_label_removed_after_its_set_takes_the_value_with_it(axes):
    """The label is in no restatement, so no fold marks it dead and the staging area drops the row.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """
    staged = _rows_staged(axes, [{"entity": "b", "value": 1.0}], "p_nom")
    staged.add("snapshot", [_T2])
    staged.set("p_nom", 5.0, entity="a", snapshot=_T2)
    staged.remove("snapshot", [_T2])

    def values(record):
        rows = record.attributes["p_nom"].df()
        return set(zip(rows["entity"], rows["value"], strict=True))

    reads = [staged, axes.store.commit(staged).record]
    assert [values(r) for r in reads] == [{("b", 1.0)}] * 2, (
        "staged and committed, the removed label's value is gone"
    )


def test_a_later_layer_overrides_a_layer_holding_both_rows(axes):
    """The lower layer still picks its more specific row where it keeps the map row.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    rows: list[dict] = [{"value": 2.0}, {"entity": "a", "value": 1.0}]
    lower = _rows_layer(axes, rows, "write_layer")
    staged = WorkingRecord(lower.record)
    staged.set("p_nom", 5.0, scenario="x")
    assert _read(lower.store.commit(staged)) == {
        ("a", "x"): 5.0,
        ("a", "y"): 1.0,
        ("b", "x"): 5.0,
        ("b", "y"): 2.0,
    }


_ENTITY_SCENARIO = [{"entity": "a", "value": 1.0}, {"scenario": "x", "value": 2.0}]


@pytest.mark.parametrize(
    ("rows", "coordinate"),
    [
        (_ENTITY_SCENARIO, "entity=a, scenario=x"),
        (
            [
                {"entity": "a", "value": 1.0},
                {"scenario": "x", "value": 2.0},
                {"scenario": "y", "value": 2.0},
                {"entity": "a", "scenario": "x", "value": 3.0},
            ],
            "entity=a, scenario=y",
        ),
    ],
    ids=["entity-scenario", "settled-elsewhere"],
)
def test_a_layer_with_two_rows_neither_more_specific_is_refused(axes, rows, coordinate):
    """Each row names a dim the other leaves NULL, so no rule picks one where both meet.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    with pytest.raises(ValueError, match=rf"p_nom\.parquet.*{coordinate}"):
        _rows_layer(axes, rows, "write_layer")


@pytest.mark.parametrize(
    ("rows", "dims", "meet"),
    [
        (_ENTITY_SCENARIO, ("entity", "scenario"), ("a", "x")),
        (
            [{"entity": "a", "value": 1.0}, {"snapshot": _T, "value": 2.0}],
            ("entity", "snapshot"),
            ("a", _T),
        ),
    ],
    ids=["entity-scenario", "entity-snapshot"],
)
def test_a_set_crossing_a_staged_row_wins_where_the_two_meet(axes, rows, dims, meet):
    """Two `set`s whose rows cross: the later stages the row naming both, with its value.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """
    record = _rows_layer(axes, rows, "set").record
    assert broadcast(record, "p_nom", *dims)[meet] == 2.0, "the later edit wins"


def test_a_set_crossing_a_later_staged_row_keeps_its_value_elsewhere(axes):
    """Whichever of two crossing `set`s comes last wins where they meet, and only there.

    Notes
    -----
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """
    rows = [{"scenario": "x", "value": 2.0}, {"entity": "a", "value": 1.0}]
    assert _read(_rows_layer(axes, rows, "set")) == {
        ("a", "x"): 1.0,
        ("a", "y"): 1.0,
        ("b", "x"): 2.0,
    }, "the entity row, staged last, wins at (a, x)"


_THREE_DIMS = {
    "entity": {"entity": "a", "value": 1.0},
    "scenario": {"scenario": "x", "value": 2.0},
    "snapshot": {"snapshot": _T, "value": 3.0},
}


@pytest.mark.parametrize(
    "order",
    list(permutations(_THREE_DIMS)),
    ids=lambda order: "-".join(order),
)
def test_the_last_of_three_crossing_sets_wins_where_they_meet(axes, order):
    """Three `set`s, each naming one `partial` dim: the last covering a coordinate wins there.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    - [edits](https://energy-models.github.io/datarecord/reference/editing/#edits)
    """
    rows = [_THREE_DIMS[d] for d in order]
    dims = tuple(_THREE_DIMS)
    expected = {
        (e, s, _T): next(
            r["value"]
            for r in reversed(rows)
            if all(r.get(d, c) == c for d, c in zip(dims, (e, s, _T), strict=True))
        )
        for e in _ENTITIES
        for s in _SCENARIOS
    }
    staged = _rows_staged(axes, rows, "p_nom")
    assert broadcast(staged, "p_nom", *dims) == expected, "staged, the last set wins"
    committed = axes.store.commit(staged).record
    assert broadcast(committed, "p_nom", *dims) == expected, "committed, it still does"


def test_two_rows_crossing_where_a_row_names_both_are_written(axes):
    """A record flattened from several layers holds a row naming both wherever two cross.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    rows = [*_ENTITY_SCENARIO, {"entity": "a", "scenario": "x", "value": 3.0}]
    record = _rows_layer(axes, rows, "write_layer").record
    assert broadcast(record, "p_nom", "entity", "scenario")[("a", "x")] == 3.0, (
        "the row naming both wins where the two cross"
    )


def test_a_frame_staging_two_rows_neither_more_specific_is_refused(axes):
    """One `set` staging two crossing rows, with none naming both, has no later edit to win.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    staged = _rows_staged(axes, [], "p_nom")
    frame = pd.DataFrame(
        {"entity": ["a", None], "scenario": [None, "x"], "value": [1.0, 2.0]}
    )
    with pytest.raises(
        ValueError, match=r"'p_nom'.*\{'entity': 'a', 'scenario': 'x'\}"
    ):
        staged.set("p_nom", frame)
    assert staged.attributes["p_nom"].fetchall() == [], (
        "the refused edit stages nothing"
    )


_ONE_LABEL = [{"entity": "a", "value": 1.0}, {"entity": "a", "value": 2.0}]


@pytest.mark.parametrize(
    ("split_passes", "rows", "coordinate"),
    [
        pytest.param(
            False,
            [{"value": 1.0}, {"value": 2.0}],
            "every coordinate",
            id="one-pass-broadcast",
        ),
        pytest.param(False, _ONE_LABEL, "entity=a", id="one-pass-one-label"),
        pytest.param(
            False,
            [
                {"entity": "a", "breakpoint": 0.5, "value": 1.0},
                {"entity": "a", "breakpoint": 0.5, "value": 2.0},
            ],
            "entity=a, breakpoint=0.5",
            id="one-pass-one-breakpoint",
        ),
        pytest.param(True, _ONE_LABEL, "entity=a", id="a-pass-per-row-one-label"),
    ],
    indirect=["split_passes"],
)
@pytest.mark.usefixtures("split_passes")
def test_a_layer_with_two_rows_naming_the_same_dims_is_refused(axes, rows, coordinate):
    """Two rows naming the same labels are equally specific, so no rule picks one.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    with pytest.raises(
        ValueError, match=rf"p_nom\.parquet holds 2 rows at {coordinate};"
    ):
        _rows_layer(axes, rows, "write_layer")


def test_a_curve_is_no_pair_of_rows(axes):
    """The rows of one curve differ in `breakpoint`, so each is its own row."""
    rows: list[dict] = [{"entity": "a", "breakpoint": b, "value": b} for b in (0, 1)]
    df = _rows_layer(axes, rows, "write_layer").record.attributes["p_nom"].df()
    assert sorted(df["breakpoint"]) == [0, 1], "both breakpoints held as written"


def test_a_stored_pair_naming_the_same_dims_fails_the_read(axes):
    """A layer not written by `write_layer` is held to the rule when it is read.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/reference/data-model/#the-broadcast-rule)
    """
    rows: list[dict] = [{"value": 2.0}, {"entity": "a", "value": 1.0}]
    revision = _rows_layer(axes, rows, "write_layer")
    file = Path(axes.store.layer_dir(revision.id), "attributes", "p_nom.parquet")
    twin = duckdb.read_parquet(str(file)).filter("entity = 'a'")
    duckdb.read_parquet(str(file)).union(twin).to_parquet(str(file) + ".twin")
    Path(str(file) + ".twin").replace(file)
    with pytest.raises(duckdb.Error, match="'p_nom' covering entity=a, .*same dims"):
        revision.record.attributes["p_nom"].df()


_TYPED = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "snapshot": Dimension(dtype="datetime"),
    },
    properties={"entity": {"tag": PropertySpec(dtype="str")}},
    attributes={"p_nom": AttributeSpec(dtype="float", dims=frozenset({"entity"}))},
    partial=frozenset({"entity"}),
)


def _typed(file: str, column: str, values: list) -> _Source:
    """A `_TYPED` source writing `values` as `column` of `file`, one label each."""
    kind, key = file.removesuffix(".parquet").split("/")
    labels = [f"e{i}" for i in range(len(values))]
    if kind == "attributes":
        frame = pd.DataFrame(
            {"entity": labels, "attribute": key, "breakpoint": None, column: values}
        )
        return _Source(_TYPED, attributes={key: frame})
    if key == "entity":
        frame = pd.DataFrame({"entity": labels, column: values})
        return _Source(
            _TYPED, dims={"entity": frame[["entity"]]}, properties={"entity": frame}
        )
    return _Source(_TYPED, dims={key: pd.DataFrame({column: values})})


@pytest.mark.parametrize(
    ("file", "column", "values", "stored"),
    [
        ("attributes/p_nom.parquet", "value", [1, 2], "DOUBLE"),
        ("dimensions/entity.parquet", "tag", [None], "VARCHAR"),
        (
            "dimensions/snapshot.parquet",
            "snapshot",
            pd.to_datetime(["2030-01-01"]),
            "TIMESTAMP",
        ),
    ],
    ids=["attribute", "property-null", "axis"],
)
def test_a_write_stores_the_declared_type_widening_a_column(
    store, file, column, values, stored
):
    """A column of a narrower type of its kind is cast to the declared one.

    Notes
    -----
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    revision = store.commit(_typed(file, column, values), parent=None)
    path = Path(store.layer_dir(revision.id), file)
    on_disk = store.workspace.con.read_parquet(str(path)).project(column)
    assert str(on_disk.types[0]) == stored, f"{column!r} of {file} as declared"


@pytest.mark.parametrize(
    ("file", "column", "values", "match"),
    [
        (
            "attributes/p_nom.parquet",
            "value",
            ["high"],
            "does not fit 'p_nom' of dtype float \\(DOUBLE\\)",
        ),
        (
            "dimensions/snapshot.parquet",
            "snapshot",
            ["2030-01-01"],
            "does not fit 'snapshot' of dtype datetime \\(TIMESTAMP\\)",
        ),
    ],
    ids=["attribute", "axis"],
)
def test_a_write_refuses_a_column_that_does_not_fit_its_dtype(
    store, file, column, values, match
):
    """A column of another kind is refused, naming what declares it and its dtype.

    A string is not parsed into a `datetime` label, nor a float truncated to
    an `int`.

    Notes
    -----
    - [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
    """
    with pytest.raises(
        ValueError, match=f"{file}: column '{column}' has type .*{match}"
    ):
        store.commit(_typed(file, column, values), parent=None)
    _nothing_reachable(store)


def _plant(p_nom: frozenset[str]) -> Schema:
    """Entities and scenarios, `p_nom` over `p_nom`."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype="str"),
            "scenario": Dimension(dtype="str"),
        },
        attributes={"p_nom": AttributeSpec(dtype="float", dims=p_nom)},
        partial=frozenset({"entity", "scenario"}),
    )


def _plant_layer(store, schema: Schema):
    """A root holding entities `g1` and `g2` with their `p_nom`, under `schema`."""
    root = empty_root(store, schema).commit()
    staged = WorkingRecord(root.record)
    staged.add("entity", ["g1", "g2"])
    staged.set("p_nom", {"g1": 1.0, "g2": 2.0})
    return root.store.commit(staged)


def test_widening_a_single_dim_attribute_reads_old_rows_in_every_scenario(store):
    """An old row has no `scenario` column, read NULL, so it broadcasts.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/reference/schema/#compatibility)
    """
    narrow = _plant(frozenset({"entity"}))
    layer = _plant_layer(store, narrow)

    wide = _plant(frozenset({"entity", "scenario"}))
    assert wide.compatible_with(narrow) == []
    widened = empty_child(layer, wide).commit()
    staged = WorkingRecord(widened.record)
    staged.add("scenario", ["low", "high"])
    child = store.commit(staged)

    assert broadcast(child.record, "p_nom", "entity", "scenario") == {
        ("g1", "low"): 1.0,
        ("g1", "high"): 1.0,
        ("g2", "low"): 2.0,
        ("g2", "high"): 2.0,
    }, "the old layer's value in every scenario"


_AWKWARD = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "select": Dimension(dtype="str"),
        "Horizon": Dimension(dtype="str"),
    },
    attributes={
        "Order_Size": AttributeSpec(
            dtype="float", dims={"entity", "select", "Horizon"}
        ),
    },
    properties={"select": {"Group": PropertySpec(dtype="float")}},
    partial={"select", "Horizon"},
)


def _awkward(values: dict[tuple[str, str], float]) -> _Source:
    long = pd.DataFrame(
        [
            {
                "entity": "plant",
                "select": s,
                "Horizon": h,
                "attribute": "Order_Size",
                "breakpoint": None,
                "value": v,
            }
            for (s, h), v in values.items()
        ]
    )
    return _Source(
        _AWKWARD,
        attributes={"Order_Size": long},
        dims={
            "entity": pd.DataFrame({"entity": ["plant"]}),
            "select": pd.DataFrame({"select": ["a", "b"]}),
            "Horizon": pd.DataFrame({"Horizon": ["near", "far"]}),
        },
        properties={
            "select": pd.DataFrame({"select": ["a", "b"], "Group": [1.0, 2.0]})
        },
    )


def test_names_needing_quotes_round_trip_through_layers(store, workspace, tmp_path):
    """Mixed case and an SQL keyword are names a declared name may be.

    The child patches one value along both `partial` dims, so the owner map's
    flag structs, keyed by those names, are built and folded.

    Notes
    -----
    - [names](https://energy-models.github.io/datarecord/reference/schema/#names)
    """
    root = store.commit(_awkward({("a", "near"): 1.0, ("b", "far"): 2.0}), parent=None)
    child = root.store.commit(_awkward({("b", "far"): 5.0}), parent=root)

    assert child.record.schema == _AWKWARD, "the manifest reads back unchanged"
    rows = relation(child, "Order_Size").df()
    got = dict(zip(zip(rows["select"], rows["Horizon"]), rows["value"]))
    assert got == {("a", "near"): 1.0, ("b", "far"): 5.0}, (
        "the child's value wins at its coordinate and the root's elsewhere"
    )
    axis = child.record.properties["select"].df()
    assert dict(zip(axis["select"], axis["Group"])) == {"a": 1.0, "b": 2.0}

    out = str(tmp_path / "standalone")
    workspace.write(child.record, out)
    again = workspace.at(out)
    assert again.schema == _AWKWARD
    flat = again.attributes["Order_Size"].df()
    assert dict(zip(zip(flat["select"], flat["Horizon"]), flat["value"])) == got, (
        "a flattened standalone copy reads the same"
    )


# -- the PyPSA source (https://energy-models.github.io/datarecord/reference/format/) ------------------------------------------------


def test_to_datarecord_lists_without_unpivoting(store, ac_dc):
    """Key sets come off the network and its registry, so listing is cheap."""
    source = PyPSA.to_datarecord(ac_dc, store.workspace.con)
    assert "entity" in source.dimensions
    assert "connection" in source.dimensions
    assert "p_max_pu" in source.attributes
    assert "v_nom" in source.attributes, "a static attribute is long rows too"
    assert "carrier" not in source.attributes, "a property is no attribute"
    # A port attribute is one bus-keyed attribute, not one per port (https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column).
    assert "efficiency" in source.attributes
    assert "efficiency2" not in source.attributes


def test_ports_round_trip_through_connections_with_a_role_per_type(store):
    """Each port is a connection row with a role, and comes back as its own column.

    Notes
    -----
    - [attributes over a multi-dim](https://energy-models.github.io/datarecord/reference/data-model/#a-multi-dims-column)
    - [consuming a record](https://energy-models.github.io/datarecord/reference/tools/)
    """
    import pypsa

    n = pypsa.Network()
    n.set_snapshots(pd.date_range("2030-01-01", periods=1, freq="h"))
    n.add("Bus", ["b0", "b1"])
    n.add("Generator", "g", bus="b0")
    n.add("Link", "l", bus0="b0", bus1="b1")
    revision = export_network(n, store)
    rows = unpacked(
        store.workspace.con.read_parquet(
            store.layer_dir(revision.id) + "dimensions/connection.parquet"
        )
    )
    back = PyPSA.build(revision.record)

    for ctype, roles, ports in (
        ("Link", {"input", "output"}, ("bus0", "bus1")),
        ("Generator", {"attached"}, ("bus",)),
    ):
        original = n.c[ctype].static
        mine = rows[rows["entity"].isin(original.index)]
        assert set(mine["role"]) == roles, f"{ctype} roles follow PyPSA's sign"
        for port in ports:
            assert set(mine["bus"]) >= set(original[port]), f"{ctype}.{port} stored"
            assert list(back.c[ctype].static[port]) == list(original[port]), (
                f"{ctype}.{port} comes back under its own name"
            )
