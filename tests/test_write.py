# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a layer from long-format frames.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
"""

import json
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from datarecord import Workspace
from datarecord.layered.resolve import read_schema
from datarecord.mutable import WorkingRecord
from datarecord.record import EMPTY, LazyRelations
from datarecord.schema import AttributeSpec, Dimension, PropertySpec, Schema
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import (
    empty_root,
    export_network,
    relation,
    schema,
    unpacked,
    write_schema,
)


class _Source:
    """A minimal `Record` over ready-made frames, counting each build."""

    def __init__(
        self,
        schema,
        attributes=None,
        connections=None,
        dims=None,
        properties=None,
    ):
        self._schema = schema
        self.built: list[str] = []
        self._attributes = attributes or {}
        self._connections = connections or {}
        self._dims = dims or {}
        self._properties = properties or {}

    @property
    def schema(self):
        return self._schema

    def _frames(self, mapping, tag):
        def build(key):
            self.built.append(f"{tag}:{key}")
            return mapping[key]

        return LazyRelations(tuple(mapping), build)

    @property
    def dims(self):
        return self._frames(self._dims, "dims") if self._dims else EMPTY

    @property
    def properties(self):
        return self._frames(self._properties, "properties")

    @property
    def groups(self):
        """Keyed by group, one frame each."""
        return self._frames(self._connections, "groups")

    @property
    def attributes(self):
        return self._frames(self._attributes, "attributes")

    def flags(self, **categories):
        return {}


_SCHEMA = schema()


def _long(**overrides) -> pd.DataFrame:
    """One long row carrying its attribute's own coordinates, and no others.

    Shaped from the spec rather than spelled: a source handing over a column
    the attribute is not addressed by is what the writer now rejects, so a
    helper that spelled every declared dim would be testing against a record no
    reader would accept.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
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


# -- the lazy mapping (https://energy-models.github.io/datarecord/design/format/) -------------------------------------------------


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
    store.root(source)

    assert sorted(source.built) == [
        "attributes:e_nom",
        "attributes:p_nom",
        "dims:entity",
        "properties:entity",
    ]


# -- creating a layer -------------------------------------------------------


def test_a_write_creates_a_new_layer(store):
    """Files land where `layer_dir` says - data only, no schema.

    The record's one schema goes beside `layers/`, so a layer directory holds
    nothing but data. That is what keeps it a plain parquet directory a reader
    knowing nothing about layering can open.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    revision = store.root(_Source(_SCHEMA, attributes={"p_nom": _long()}))

    base = Path(store.layer_dir(revision.id))
    assert (base / "attributes" / "p_nom.parquet").exists()
    assert not (base / "manifest.json").exists()
    # Written once for the whole tree, and it is what the layer is read under.
    assert read_schema(store) == _SCHEMA
    assert store.revisions() == [revision], "the revision file makes it real"


def test_no_layer_file_carries_order_key(store, tmp_path):
    """`order_key` is the fold's answer about a frame, never a column of one.

    A source handing over *resolved* frames carries it - which is what writing
    a `WorkingRecord` as a standalone directory does, the record itself being
    what is written. Writing it would put a struct column in files the
    format promises a foreign reader can open, and would look like stored order
    where the fold always re-derives it from file order.

    Notes
    -----
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    entities = ["steel_dri", "h2_north"]
    connection = {"entity": "steel_dri", "bus": "h2_north"}
    revision = store.root(
        _Source(
            _SCHEMA,
            attributes={"p_nom": _long()},
            dims={"entity": pd.DataFrame({"entity": entities})},
            properties={
                "entity": pd.DataFrame(
                    {"entity": entities, "ctype": ["Process", "Bus"]}
                )
            },
            connections={"connection": pd.DataFrame({"connection": [connection]})},
        )
    )

    staged = WorkingRecord(revision.record)
    staged.set("p_nom", 150.0, entity=["steel_dri"])
    out = str(tmp_path / "flat")
    store.workspace.write(staged, out)

    written = sorted(Path(out).rglob("*.parquet"))
    assert {"dims/entity.parquet", "groups/connection.parquet"} <= {
        path.relative_to(out).as_posix() for path in written
    }, "the frames the fold orders must be among those written"
    for path in written:
        columns = store.workspace.con.sql(
            f"SELECT * FROM read_parquet('{path}')"
        ).columns
        assert "order_key" not in columns, f"{path.relative_to(out)} carries order_key"


def test_a_write_rejects_a_category_the_enum_does_not_declare(store):
    entities = pd.DataFrame({"entity": ["a"], "ctype": ["Nope"]})
    with pytest.raises(Exception, match="Nope"):
        store.root(
            _Source(
                _SCHEMA,
                dims={"entity": entities[["entity"]]},
                properties={"entity": entities},
            )
        )
    _nothing_reachable(store)


def test_a_directory_target_carries_its_own_schema(workspace, tmp_path):
    """A standalone record *is* one record, so its schema goes in the directory.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = str(tmp_path / "standalone")
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    record = workspace.write(source, out)

    assert (Path(out) / "manifest.json").exists()
    assert record.schema == _SCHEMA.model_copy(update={"id": record.schema.id})
    assert workspace.at(out).schema == record.schema

    # And it is that file answering: a standalone record is one whole record,
    # so it reads the same through a workspace that knows nothing about it.
    elsewhere = Workspace()
    try:
        again = elsewhere.at(out)
        assert again.schema == record.schema
        assert again.resolver.revision_id == record.resolver.revision_id, (
            "the manifest's id names one layer to every reader"
        )
    finally:
        elsewhere.close()


def test_only_a_standalone_manifest_carries_an_id(store, tmp_path):
    """The id is the schema's field, in the file and on the record read from it.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    out = tmp_path / "standalone"
    record = store.workspace.write(source, str(out))

    written = Schema.model_validate_json((out / "manifest.json").read_text())
    assert written.id == record.resolver.revision_id
    assert record.schema.id == record.resolver.revision_id

    copy = store.workspace.write(record, str(tmp_path / "copy"))
    assert copy.schema.id not in {None, record.schema.id}, "a copy mints its own id"

    store.root(record)
    tree = store.files.read_text(store.manifest_uri)
    assert tree is not None
    assert "id" not in json.loads(tree[0]), "a tree's manifest carries no id"


def test_a_directory_without_a_manifest_is_no_standalone_record(store, tmp_path):
    """`Workspace.at` refuses a layer directory: that is read through its store.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    revision = store.root(_Source(_SCHEMA, attributes={"p_nom": _long()}))
    with pytest.raises(FileNotFoundError, match="no manifest"):
        store.workspace.at(store.layer_dir(revision.id))
    assert store.layer(revision.id).schema == _SCHEMA


def test_a_standalone_write_refuses_an_existing_directory(workspace, tmp_path):
    """Refused up front, empty or holding a record, so nothing is half-replaced.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/design/writing/#a-standalone-directory)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, str(tmp_path))
    assert source.built == [], "refused before any frame was built"
    assert list(tmp_path.iterdir()) == [], "refused before anything was written"

    out = str(tmp_path / "new")
    record = workspace.write(source, out)
    assert record.schema == _SCHEMA.model_copy(update={"id": record.schema.id})
    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, out)


def test_a_failed_standalone_write_leaves_no_record(workspace, tmp_path):
    """The manifest is written last, so a directory whose write failed is none.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/design/writing/#a-standalone-directory)
    """
    out = tmp_path / "standalone"
    short = _long().drop(columns=["breakpoint"])
    with pytest.raises(ValueError, match="missing long-schema columns"):
        workspace.write(_Source(_SCHEMA, attributes={"p_nom": short}), str(out))
    assert not out.exists()
    with pytest.raises(FileNotFoundError):
        workspace.at(str(out))


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
    - [a standalone directory](https://energy-models.github.io/datarecord/design/writing/#a-standalone-directory)
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
    - [a standalone directory](https://energy-models.github.io/datarecord/design/writing/#a-standalone-directory)
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


def test_a_manifest_without_an_id_is_refused(workspace, tmp_path):
    """Read without one, the record's folds would have nothing to be keyed by.

    Notes
    -----
    - [one schema per record](https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record)
    """
    out = tmp_path / "standalone"
    workspace.write(_p_nom(1.0), str(out))
    (out / "manifest.json").write_text(_SCHEMA.model_dump_json())
    with pytest.raises(ValueError, match="no record id"):
        workspace.at(str(out))


def _nothing_reachable(store) -> None:
    """A failed tree write leaves no revision, no layer and no manifest."""
    assert store.revisions() == [], "no revision file, so nothing reaches a layer"
    layers = Path(store.uri, "layers")
    assert not (layers.exists() and any(layers.iterdir())), "staged and removed"
    assert not Path(store.manifest_uri).exists(), "never widened for no layer"


def test_a_file_carries_only_its_own_attributes_coordinates(store):
    """One attribute is one file, so one column set - not every declared dim.

    A component attribute has no `bus` column, a connection attribute does, and
    neither carries a dim it is not addressed by. The uniform prefix this
    replaces put an all-NULL `bus` on every file and a `period` column on
    attributes that never vary over one.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    connection = {"entity": "steel_dri", "bus": "h2_north"}
    revision = store.root(
        _Source(
            _SCHEMA,
            attributes={
                "p_max_pu": _long(
                    attribute="p_max_pu", snapshot=pd.Timestamp("2030-01-01")
                ),
                "efficiency": _long(attribute="efficiency", connection=connection),
                "weight": _long(attribute="weight", scenario="s1"),
            },
        )
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
    }, "a component attribute carries `entity`, not the connection group's `bus`"

    efficiency = columns("efficiency")
    assert "connection" in efficiency, "a connection attribute carries the group"
    assert "bus" not in efficiency, "as one struct column, not its coordinates"
    assert columns("weight") & {"entity", "period"} == set(), (
        "and no dim it is not addressed by"
    )


# -- validation -------------------------------------------------------------


def test_a_write_rejects_an_undeclared_attribute(store):
    """An attribute with no spec has no shape, so there is nothing to write it as.

    Its `dims` are what say which columns the file carries, so writing one the
    schema does not declare would put a file in `attributes/` whose column set no
    reader could derive.

    Notes
    -----
    - [AttributeSpec](https://energy-models.github.io/datarecord/design/schema/#attributespec)
    """
    source = _Source(_SCHEMA, attributes={"not_declared": _long(attribute="nope")})

    with pytest.raises(ValueError, match="not a declared attribute"):
        store.root(source)


def test_a_write_rejects_a_coordinate_the_attribute_lacks(store):
    """A column the attribute is not addressed by is a disagreement, not a spare.

    The read path projects an attribute's own coordinates, so a `bus` on a
    component attribute would be written and never read - and a source emitting
    one means something different by the attribute than the schema does.
    Reported rather than dropped, since silently narrowing would hide that.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    wide = _long().assign(bus=None)
    source = _Source(_SCHEMA, attributes={"p_nom": wide})

    with pytest.raises(
        ValueError, match=r"carries columns \['bus'\].*not addressed by"
    ):
        store.root(source)


def test_a_write_rejects_a_group_column_that_is_not_its_key(store):
    """A group's column is a struct of exactly its key coordinates.

    DuckDB casts a struct by field name, so a missing field would be written as
    NULL rather than refused, re-keying every row.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    row = {"attribute": "efficiency", "breakpoint": None, "value": 1.0}
    row |= {"snapshot": None, "scenario": None, "period": None}
    short = pd.DataFrame([{"connection": {"entity": "steel_dri"}, **row}])
    source = _Source(_SCHEMA, attributes={"efficiency": short})

    with pytest.raises(ValueError, match=r"'connection' as .*\['entity', 'bus'\]"):
        store.root(source)


def test_a_group_column_round_trips_as_a_struct(store):
    """What a source hands over as the group's struct reads back as one."""
    row = {"attribute": "efficiency", "breakpoint": None, "value": 1.0}
    row |= {"snapshot": None, "scenario": None, "period": None}
    connection = {"entity": "steel_dri", "bus": "h2_north"}
    frame = pd.DataFrame([{"connection": connection, **row}])
    revision = store.root(_Source(_SCHEMA, attributes={"efficiency": frame}))

    rows = relation(revision, "efficiency").project("connection, value").fetchall()
    assert rows == [(connection, 1.0)]


def test_a_write_rejects_a_missing_long_column(store):
    """A frame the fold could not resolve is refused before anything is written."""
    short = _long().drop(columns=["breakpoint"])
    source = _Source(_SCHEMA, attributes={"p_nom": short})

    with pytest.raises(ValueError, match="missing long-schema columns.*breakpoint"):
        store.root(source)
    _nothing_reachable(store)


def test_a_write_rejects_a_flat_group_frame(store):
    """A group file is keyed by the struct column named for it, not its coordinates.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    flat = pd.DataFrame({"entity": ["steel_dri"], "bus": ["h2"]})
    source = _Source(_SCHEMA, connections={"connection": flat})

    with pytest.raises(ValueError, match=r"missing \['connection'\]"):
        store.root(source)


def test_a_write_rejects_a_group_struct_not_of_its_key(store):
    """A struct missing a key field would re-key every row once cast.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    partial = pd.DataFrame({"connection": [{"entity": "steel_dri"}]})
    source = _Source(_SCHEMA, connections={"connection": partial})

    with pytest.raises(ValueError, match="one struct column"):
        store.root(source)


def test_a_write_rejects_an_axis_without_its_key_column(store):
    """An axis file is keyed by its dim's column; one without it labels nothing.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    source = _Source(
        schema(), dims={"snapshot": pd.DataFrame({"period": pd.array([2030])})}
    )

    with pytest.raises(ValueError, match="missing its key column 'snapshot'"):
        store.root(source)
    _nothing_reachable(store)


def _set_junk(store):
    _, staged = _rows_staged(store, [], "p_nom")
    staged.set("p_nom", pd.DataFrame({"entity": ["a"], "value": [1.0], "junk": [1]}))


def _add_junk(store):
    _, staged = _rows_staged(store, [], "p_nom")
    staged.add("entity", ["c"], junk=1)


def _add_group_junk(store):
    _, staged = _rows_staged(store, [], "p_nom")
    staged.add_group("connection", pd.DataFrame({"connection": [_AB], "junk": [1]}))


def _write_junk(store):
    frame = pd.DataFrame({"scenario": ["high"], "junk": [1]})
    try:
        store.root(_Source(_SCHEMA, dims={"scenario": frame}))
    finally:
        _nothing_reachable(store)


@pytest.mark.parametrize(
    ("write", "accepted"),
    [
        pytest.param(
            _set_junk,
            "['entity', 'period', 'scenario', 'snapshot', 'value']",
            id="set",
        ),
        pytest.param(_add_junk, "['ctype', 'deleted', 'entity']", id="add"),
        pytest.param(_add_group_junk, "['connection', 'deleted']", id="add_group"),
        pytest.param(_write_junk, "['deleted', 'order_key', 'scenario']", id="write"),
    ],
)
def test_an_undeclared_column_is_refused(store, write, accepted):
    """A column no declaration accounts for has no dtype and no meaning, so it is refused, not dropped.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    with pytest.raises(ValueError, match=r"\['junk'\]") as raised:
        write(store)
    assert accepted in str(raised.value), "the error names the columns it accepts"


def test_an_attribute_column_in_a_dims_frame_is_refused(store):
    """`weight` over `scenario` alone is an attribute, so long rows, never an axis column.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    source = _Source(
        schema(),
        dims={"scenario": pd.DataFrame({"scenario": ["high"], "weight": [0.4]})},
    )

    with pytest.raises(ValueError, match="they are attributes, whose values are long"):
        store.root(source)
    _nothing_reachable(store)


_ENTITIES, _SCENARIOS, _T = ["a", "b"], ["x", "y"], pd.Timestamp("2030-01-01")
_AB, _BB = {"entity": "a", "bus": "b0"}, {"entity": "b", "bus": "b0"}


def _rows_staged(store, rows: list[dict], attribute: str):
    """Return a written base, and a staging area over it holding `rows` of `attribute`.

    A coordinate a row leaves out is NULL there; a `connection` is set by its fields.
    """
    write_schema(_SCHEMA, store)
    base = empty_root(store)
    staged = WorkingRecord(base.record)
    staged.add("ctype", ["Generator"])
    staged.add("entity", _ENTITIES, ctype="Generator")
    staged.add("bus", ["b0"])
    staged.add("scenario", _SCENARIOS)
    staged.add("snapshot", [_T])
    staged.add_group("connection", pd.DataFrame({"connection": [_AB, _BB]}))
    base = base.child(staged.edits)
    staged = WorkingRecord(base.record)
    for row in rows:
        coords = {c: v for c, v in row.items() if c not in ("value", "connection")}
        staged.set(attribute, row["value"], **coords, **row.get("connection", {}))
    return base, staged


def _rows_layer(store, rows: list[dict], via: str, attribute: str = "p_nom"):
    """Return a revision whose top layer holds `rows` of `attribute`, written `via` `set` or a source.

    A coordinate a row leaves out is NULL there.
    """
    if via == "set":
        base, staged = _rows_staged(store, rows, attribute)
        return base.child(staged.edits)
    nulls = dict.fromkeys(("entity", "connection", "scenario", "snapshot"))
    frame = pd.concat(
        [_long(**{**nulls, "attribute": attribute, **row}) for row in rows],
        ignore_index=True,
    )
    return store.root(
        _Source(
            _SCHEMA,
            attributes={attribute: frame},
            connections={"connection": pd.DataFrame({"connection": [_AB, _BB]})},
            dims={
                "entity": pd.DataFrame({"entity": _ENTITIES}),
                "bus": pd.DataFrame({"bus": ["b0"]}),
                "scenario": pd.DataFrame({"scenario": _SCENARIOS}),
                "snapshot": pd.DataFrame({"snapshot": [_T]}),
            },
            properties={
                "entity": pd.DataFrame(
                    {"entity": _ENTITIES, "ctype": ["Generator"] * 2}
                )
            },
        )
    )


def _read(revision, attribute: str = "p_nom") -> list:
    """Return `attribute` as sorted `((first coordinate, scenario), value)` pairs."""
    df = revision.record.attributes[attribute].df()
    first = df["connection" if attribute == "efficiency" else "entity"]
    keys = zip(
        (v["entity"] if isinstance(v, dict) else v for v in first),
        df["scenario"],
        strict=True,
    )
    return sorted(zip(keys, df["value"], strict=True))


@pytest.mark.parametrize("via", ["set", "write_layer"])
@pytest.mark.parametrize(
    ("attribute", "rows", "read"),
    [
        (
            "p_nom",
            [{"value": 2.0}, {"entity": "a", "value": 1.0}],
            {("a", "x"): 1.0, ("a", "y"): 1.0, ("b", "x"): 2.0, ("b", "y"): 2.0},
        ),
        (
            "p_nom",
            [
                {"scenario": "x", "value": 2.0},
                {"entity": "a", "scenario": "x", "value": 1.0},
            ],
            {("a", "x"): 1.0, ("b", "x"): 2.0},
        ),
        (
            "p_nom",
            [
                {"value": 3.0},
                {"entity": "a", "value": 2.0},
                {"entity": "a", "scenario": "x", "value": 1.0},
            ],
            {("a", "x"): 1.0, ("a", "y"): 2.0, ("b", "x"): 3.0, ("b", "y"): 3.0},
        ),
        (
            "p_nom",
            [
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"scenario": "y", "snapshot": _T, "value": 2.0},
            ],
            {("a", "x"): 1.0, ("a", "y"): 2.0, ("b", "y"): 2.0},
        ),
        (
            "efficiency",
            [{"value": 0.5}, {"connection": _AB, "value": 0.9}],
            {("a", "x"): 0.9, ("a", "y"): 0.9, ("b", "x"): 0.5, ("b", "y"): 0.5},
        ),
    ],
    ids=[
        "broadcast-and-specific",
        "two-dims-over-one",
        "three-levels",
        "neither-but-disjoint",
        "connection",
    ],
)
def test_a_layer_reads_its_more_specific_row(store, via, attribute, rows, read):
    """Within one layer, a row naming a label beats one broadcasting over its dim.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    got = _read(_rows_layer(store, rows, via, attribute), attribute)
    assert got == sorted(read.items()), (
        "one value per coordinate, the most specific row's"
    )


_ALL_TWO = {("a", "x"): 2.0, ("a", "y"): 2.0, ("b", "x"): 2.0, ("b", "y"): 2.0}


@pytest.mark.parametrize(
    ("attribute", "rows", "read"),
    [
        ("p_nom", [{"entity": "a", "value": 1.0}, {"value": 2.0}], _ALL_TWO),
        (
            "p_nom",
            [{"entity": "a", "value": 1.0}, {"entity": "a", "value": 2.0}],
            {("a", "x"): 2.0, ("a", "y"): 2.0},
        ),
        (
            "p_nom",
            [
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"entity": "a", "value": 2.0},
            ],
            {("a", "x"): 2.0, ("a", "y"): 2.0},
        ),
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
        (
            "efficiency",
            [{"connection": _AB, "value": 1.0}, {"connection": _BB, "value": 2.0}],
            {("a", "x"): 1.0, ("a", "y"): 1.0, ("b", "x"): 2.0, ("b", "y"): 2.0},
        ),
    ],
    ids=[
        "broadcast-after-specific",
        "same-coordinates",
        "broadcast-over-one-dim",
        "category-keyword",
        "group",
        "not-covering",
        "other-tuple",
    ],
)
def test_a_later_set_replaces_what_it_covers(store, attribute, rows, read):
    """A `set` drops the earlier staged rows its own rows cover, and keeps the rest.

    Notes
    -----
    - [set](https://energy-models.github.io/datarecord/design/working-record/#set)
    """
    got = _read(_rows_layer(store, rows, "set", attribute), attribute)
    assert got == sorted(read.items()), "the later set wins where it covers"


@pytest.mark.parametrize("via", ["set", "write_layer"])
@pytest.mark.parametrize(
    ("override", "read"),
    [
        (
            {"entity": "a", "scenario": "x"},
            {("a", "x"): 5.0, ("a", "y"): 1.0, ("b", "x"): 2.0, ("b", "y"): 2.0},
        ),
        (
            {"scenario": "x"},
            {("a", "x"): 5.0, ("a", "y"): 1.0, ("b", "x"): 5.0, ("b", "y"): 2.0},
        ),
    ],
    ids=["one-key", "a-broadcast-key"],
)
def test_a_later_layer_overrides_a_layer_holding_both_rows(store, via, override, read):
    """The lower layer still picks its more specific row where it keeps the key.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    rows: list[dict] = [{"value": 2.0}, {"entity": "a", "value": 1.0}]
    lower = _rows_layer(store, rows, via)
    staged = WorkingRecord(lower.record)
    staged.set("p_nom", 5.0, **override)
    assert _read(lower.child(staged.edits)) == sorted(read.items())


@pytest.mark.parametrize("via", ["set", "write_layer"])
@pytest.mark.parametrize(
    ("rows", "coordinate"),
    [
        (
            [{"entity": "a", "value": 1.0}, {"scenario": "x", "value": 2.0}],
            "entity=a, scenario=x",
        ),
        (
            [{"entity": "a", "value": 1.0}, {"snapshot": _T, "value": 2.0}],
            "entity=a, snapshot=2030-01-01 00:00:00",
        ),
        (
            [
                {"value": 3.0},
                {"entity": "b", "value": 1.0},
                {"scenario": "y", "value": 2.0},
            ],
            "entity=b, scenario=y",
        ),
    ],
    ids=["entity-scenario", "entity-snapshot", "beside-a-default"],
)
def test_a_layer_with_two_rows_neither_more_specific_is_refused(
    store, via, rows, coordinate
):
    """Each row names a dim the other leaves NULL, so no rule picks one where both meet.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    with pytest.raises(ValueError, match=rf"p_nom\.parquet.*{coordinate}"):
        _rows_layer(store, rows, via)


def test_a_staged_pair_neither_more_specific_fails_the_read(store):
    """A layer not written by `write_layer` is held to the rule when it is read.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    rows = [{"entity": "a", "value": 1.0}, {"scenario": "x", "value": 2.0}]
    _, staged = _rows_staged(store, rows, "p_nom")
    with pytest.raises(duckdb.Error, match="'p_nom' covering entity=a, .*scenario=x"):
        staged.attributes["p_nom"].df()


@pytest.mark.parametrize(
    ("rows", "coordinate"),
    [
        ([{"value": 1.0}, {"value": 2.0}], "every coordinate"),
        ([{"entity": "a", "value": 1.0}, {"entity": "a", "value": 2.0}], "entity=a"),
        (
            [
                {"value": 3.0},
                {"entity": "a", "scenario": "x", "value": 1.0},
                {"entity": "a", "scenario": "x", "value": 2.0},
            ],
            "entity=a, scenario=x",
        ),
        (
            [
                {"entity": "a", "breakpoint": 0.5, "value": 1.0},
                {"entity": "a", "breakpoint": 0.5, "value": 2.0},
            ],
            "entity=a, breakpoint=0.5",
        ),
    ],
    ids=["broadcast", "one-label", "beside-a-default", "one-breakpoint"],
)
def test_a_layer_with_two_rows_naming_the_same_dims_is_refused(store, rows, coordinate):
    """Two rows naming the same labels are equally specific, so no rule picks one.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    with pytest.raises(
        ValueError, match=rf"p_nom\.parquet holds 2 rows at {coordinate} "
    ):
        _rows_layer(store, rows, "write_layer")


def test_a_curve_is_no_pair_of_rows(store):
    """The rows of one curve differ in `breakpoint`, so each is its own row."""
    rows: list[dict] = [{"entity": "a", "breakpoint": b, "value": b} for b in (0, 1)]
    df = _rows_layer(store, rows, "write_layer").record.attributes["p_nom"].df()
    assert sorted(df["breakpoint"]) == [0, 0, 1, 1], "each point in each scenario"


def test_a_stored_pair_naming_the_same_dims_fails_the_read(store, tmp_path):
    """A layer not written by `write_layer` is held to the rule when it is read.

    Notes
    -----
    - [the broadcast rule](https://energy-models.github.io/datarecord/design/record/#the-broadcast-rule)
    """
    rows: list[dict] = [{"value": 2.0}, {"entity": "a", "value": 1.0}]
    revision = _rows_layer(store, rows, "write_layer")
    (file,) = tmp_path.rglob("attributes/p_nom.parquet")
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
    properties={
        "entity": {"x": PropertySpec(dtype="float"), "tag": PropertySpec(dtype="str")}
    },
    attributes={
        "p_nom": AttributeSpec(dtype="float", dims=frozenset({"entity"})),
        "units": AttributeSpec(dtype="int", dims=frozenset({"entity"})),
    },
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
        ("attributes/units.parquet", "value", pd.array([1], dtype="int32"), "BIGINT"),
        ("dims/entity.parquet", "x", [1, 2], "DOUBLE"),
        ("dims/entity.parquet", "tag", [None], "VARCHAR"),
        (
            "dims/snapshot.parquet",
            "snapshot",
            pd.to_datetime(["2030-01-01"]),
            "TIMESTAMP",
        ),
    ],
    ids=["attribute", "attribute-int", "property", "property-null", "axis"],
)
def test_a_write_stores_the_declared_type_widening_a_column(
    store, file, column, values, stored
):
    """A column of a narrower type of its kind is cast to the declared one.

    Notes
    -----
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    revision = store.root(_typed(file, column, values))
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
            "attributes/units.parquet",
            "value",
            [1.5],
            "does not fit 'units' of dtype int \\(BIGINT\\)",
        ),
        (
            "dims/entity.parquet",
            "x",
            ["high"],
            "does not fit 'entity.x' of dtype float \\(DOUBLE\\)",
        ),
        (
            "dims/snapshot.parquet",
            "snapshot",
            ["2030-01-01"],
            "does not fit 'snapshot' of dtype datetime \\(TIMESTAMP\\)",
        ),
    ],
    ids=["attribute", "attribute-int", "property", "axis"],
)
def test_a_write_refuses_a_column_that_does_not_fit_its_dtype(
    store, file, column, values, match
):
    """A column of another kind is refused, naming what declares it and its dtype.

    A string is not parsed into a `datetime` label, nor a float truncated to
    an `int`.

    Notes
    -----
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    with pytest.raises(
        ValueError, match=f"{file}: column '{column}' has type .*{match}"
    ):
        store.root(_typed(file, column, values))
    _nothing_reachable(store)


def _plant(p_nom: frozenset[str], *, categorized: bool = False) -> Schema:
    """Entities and scenarios, `p_nom` over `p_nom`, `ctype` if `categorized`."""
    return Schema(
        dimensions={
            "entity": Dimension(dtype="str"),
            "scenario": Dimension(dtype="str"),
            "ctype": Dimension(dtype={"one_of": ["Gen", "Load"]}),
        },
        properties=(
            {"entity": {"ctype": PropertySpec(dtype={"one_of": "ctype"})}}
            if categorized
            else {}
        ),
        attributes={"p_nom": AttributeSpec(dtype="float", dims=p_nom)},
        partial=frozenset({"entity", "scenario"}),
    )


def _plant_layer(store, schema: Schema, **columns):
    """A root holding `ctype` `Gen` and entities `g1` and `g2` with their `p_nom`, under `schema`."""
    write_schema(schema, store)
    root = empty_root(store)
    staged = WorkingRecord(root.record)
    staged.add("ctype", ["Gen"])
    staged.add("entity", ["g1", "g2"], **columns)
    staged.set("p_nom", {"g1": 1.0, "g2": 2.0})
    return root.child(staged.edits)


def test_a_single_dim_attribute_is_long_rows_of_attributes(store):
    """`p_nom` over `entity` alone is one `attributes/` row per label, not an axis column.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    layer = _plant_layer(store, _plant(frozenset({"entity"})))

    assert "p_nom" in layer.record.attributes
    on_disk = pd.read_parquet(
        Path(store.layer_dir(layer.id), "attributes", "p_nom.parquet")
    )
    assert list(on_disk.columns) == ["entity", "attribute", "breakpoint", "value"]
    assert dict(zip(on_disk["entity"], on_disk["value"])) == {"g1": 1.0, "g2": 2.0}, (
        "one row per label"
    )
    axis = pd.read_parquet(Path(store.layer_dir(layer.id), "dims", "entity.parquet"))
    assert "p_nom" not in axis.columns


def test_widening_a_single_dim_attribute_reads_old_rows_in_every_scenario(store):
    """An old row has no `scenario` column, read NULL, so it broadcasts.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    """
    narrow = _plant(frozenset({"entity"}))
    layer = _plant_layer(store, narrow)

    wide = _plant(frozenset({"entity", "scenario"}))
    assert wide.compatible_with(narrow) == []
    write_schema(wide, store)
    staged = WorkingRecord(store.revision(layer.id).record)
    staged.add("scenario", ["low", "high"])
    child = store.revision(layer.id).child(staged.edits)

    rows = child.record.attributes["p_nom"].df()
    got = dict(zip(zip(rows["entity"], rows["scenario"]), rows["value"]))
    assert got == {
        ("g1", "low"): 1.0,
        ("g1", "high"): 1.0,
        ("g2", "low"): 2.0,
        ("g2", "high"): 2.0,
    }, "the old layer's value in every scenario"


def test_categories_add_one_column_to_the_entity_axis_and_nothing_else(tmp_path):
    """Same files with and without a categorizing property; `dims/entity.parquet` differs by it.

    Notes
    -----
    - [the entity axis](https://energy-models.github.io/datarecord/design/format/#the-entity-axis)
    """
    layouts = {}
    for categorized in (False, True):
        ws = Workspace()
        try:
            store = ws.store(str(tmp_path / str(categorized)))
            columns = {"ctype": "Gen"} if categorized else {}
            layer = _plant_layer(
                store, _plant(frozenset({"entity"}), categorized=categorized), **columns
            )
        finally:
            ws.close()
        base = Path(store.layer_dir(layer.id))
        files = sorted(str(f.relative_to(base)) for f in base.rglob("*.parquet"))
        layouts[categorized] = (
            files,
            pd.read_parquet(base / "dims" / "entity.parquet"),
        )

    (plain_files, plain_axis), (typed_files, typed_axis) = layouts[False], layouts[True]
    assert plain_files == typed_files, "the same files"
    assert set(typed_axis.columns) - set(plain_axis.columns) == {"ctype"}
    assert set(plain_axis.columns) <= set(typed_axis.columns), (
        "differing by exactly one column"
    )
    assert dict(zip(typed_axis["entity"], typed_axis["ctype"].astype(str))) == {
        "g1": "Gen",
        "g2": "Gen",
    }, "the category is read back from the axis file"


_AWKWARD = Schema(
    dimensions={
        "entity": Dimension(dtype="str"),
        "select": Dimension(dtype="str"),
        "Horizon": Dimension(dtype="str"),
    },
    attributes={
        "Order Size": AttributeSpec(
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
                "attribute": "Order Size",
                "breakpoint": None,
                "value": v,
            }
            for (s, h), v in values.items()
        ]
    )
    return _Source(
        _AWKWARD,
        attributes={"Order Size": long},
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
    """A space, mixed case and an SQL keyword are all a declared name may hold.

    The child patches one value along both `partial` dims, so the owner map's
    flag structs, keyed by those names, are built and folded.

    Notes
    -----
    - [names](https://energy-models.github.io/datarecord/design/schema/#names)
    """
    root = store.root(_awkward({("a", "near"): 1.0, ("b", "far"): 2.0}))
    child = root.child(_awkward({("b", "far"): 5.0}))

    assert child.record.schema == _AWKWARD, "the manifest reads back unchanged"
    rows = relation(child, "Order Size").df()
    got = dict(zip(zip(rows["select"], rows["Horizon"]), rows["value"]))
    assert got == {("a", "near"): 1.0, ("b", "far"): 5.0}, (
        "the child's value wins at its coordinate and the root's elsewhere"
    )
    axis = child.resolver.dims.axes["select"].df()
    assert dict(zip(axis["select"], axis["Group"])) == {"a": 1.0, "b": 2.0}

    out = str(tmp_path / "standalone")
    workspace.write(child.record, out)
    again = workspace.at(out)
    assert again.schema == _AWKWARD.model_copy(update={"id": again.schema.id})
    flat = again.attributes["Order Size"].df()
    assert dict(zip(zip(flat["select"], flat["Horizon"]), flat["value"])) == got, (
        "a flattened standalone copy reads the same"
    )


# -- the PyPSA source (https://energy-models.github.io/datarecord/design/format/) ------------------------------------------------


def test_to_datarecord_lists_without_unpivoting(store, ac_dc):
    """Key sets come off the network and its registry, so listing is cheap."""
    source = PyPSA.to_datarecord(ac_dc, store.workspace.con)
    assert "entity" in source.dims
    assert "connection" in source.groups
    assert "p_max_pu" in source.attributes
    assert "v_nom" in source.attributes, "a static attribute is long rows too"
    assert "carrier" not in source.attributes, "a property is no attribute"
    # A port attribute is one bus-keyed attribute, not one per port (https://energy-models.github.io/datarecord/design/record/#connections).
    assert "efficiency" in source.attributes
    assert "efficiency2" not in source.attributes


def test_ports_round_trip_through_connections_with_a_role_per_type(store, ac_dc):
    """Each port is a connection row with a role, and comes back as its own column.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = export_network(ac_dc, store)
    rows = unpacked(
        store.workspace.con.read_parquet(
            store.layer_dir(revision.id) + "groups/connection.parquet"
        )
    )
    back = PyPSA.build(revision.record)

    for ctype, roles, ports in (
        ("Link", {"input", "output"}, ("bus0", "bus1")),
        ("Generator", {"attached"}, ("bus",)),
    ):
        original = ac_dc.c[ctype].static
        mine = rows[rows["entity"].isin(original.index)]
        assert set(mine["role"]) == roles, f"{ctype} roles follow PyPSA's sign"
        for port in ports:
            assert set(mine["bus"]) >= set(original[port]), f"{ctype}.{port} stored"
            assert list(back.c[ctype].static[port]) == list(original[port]), (
                f"{ctype}.{port} comes back under its own name"
            )
