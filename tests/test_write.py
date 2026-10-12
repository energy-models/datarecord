# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing a layer from long-format frames.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
"""

import shutil
from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Workspace
from datarecord.layered.resolve import read_schema
from datarecord.record import EMPTY, LazyFrames, RecordLike
from datarecord.schema import AttributeSpec, Dimension, Schema
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import empty_child, export_network, relation, schema


class _Source:
    """A minimal `Record` over ready-made frames, counting each build."""

    def __init__(
        self,
        schema,
        attributes=None,
        entity_types=None,
        connections=None,
        outputs=None,
        dims=None,
    ):
        self._schema = schema
        self.built: list[str] = []
        self._attributes = attributes or {}
        self._entity_types = entity_types or {}
        self._connections = connections or {}
        self._outputs = outputs or {}
        self._dims = dims or {}

    @property
    def schema(self):
        return self._schema

    def _frames(self, mapping, tag):
        def build(key):
            self.built.append(f"{tag}:{key}")
            return nw.from_native(mapping[key]).lazy()

        return LazyFrames(tuple(mapping), build)

    @property
    def dims(self):
        return self._frames(self._dims, "dims") if self._dims else EMPTY

    @property
    def entity_types(self):
        return self._frames(self._entity_types, "entity_types")

    @property
    def groups(self):
        """Keyed by group, one frame each - the type is no coordinate of a group."""
        return self._frames(self._connections, "groups")

    @property
    def attributes(self):
        return self._frames(self._attributes, "attributes")

    @property
    def outputs(self):
        return self._frames(self._outputs, "outputs")

    def flags(self, ctype):
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
        "attribute": "p_nom",
        "breakpoint": None,
        "value": 1.0,
    }
    row.update(overrides)
    columns = _SCHEMA.long_columns_for(str(row["attribute"]))
    return pd.DataFrame([{c: row[c] for c in columns}])


# -- the lazy mapping (https://energy-models.github.io/datarecord/design/format/) -------------------------------------------------


def test_source_is_explorable_without_building(store):
    """Keys list, `in` answers, iteration repeats - none of it builds a frame."""
    source = _Source(_SCHEMA, attributes={"p_nom": _long(), "e_nom": _long()})

    assert list(source.attributes) == ["p_nom", "e_nom"]
    assert "p_nom" in source.attributes
    assert "nope" not in source.attributes
    assert len(source.attributes) == 2
    # Re-iterating works, unlike a generator, and still nothing is built.
    assert list(source.attributes) == ["p_nom", "e_nom"]
    assert source.built == []

    source.attributes["p_nom"]
    assert source.built == ["attributes:p_nom"]

    with pytest.raises(KeyError):
        source.attributes["nope"]


def test_a_write_builds_each_key_once(store):
    """The writer looks up every key exactly once, and only what it writes."""
    source = _Source(
        _SCHEMA,
        attributes={"p_nom": _long(), "e_nom": _long(attribute="e_nom")},
        entity_types={
            "Process": pd.DataFrame({"entity": ["steel_dri"], "scenario": [None]})
        },
    )
    store.root(source)

    assert sorted(source.built) == [
        "attributes:e_nom",
        "attributes:p_nom",
        "entity_types:Process",
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
    assert (base / "inputs" / "p_nom.parquet").exists()
    assert not (base / "manifest.json").exists()
    # Written once for the whole tree, and it is what the layer is read under.
    assert read_schema(store) == _SCHEMA
    assert store.revisions() == [revision], "the revision file makes it real"


def test_no_layer_file_carries_order_key(store, ac_dc, tmp_path):
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
    from datarecord.mutable import WorkingRecord

    revision = export_network(ac_dc, store)

    staged = WorkingRecord(revision.record)
    staged.set("p_nom", 150.0, entity=["Manchester Wind"])
    out = str(tmp_path / "flat")
    store.workspace.write(staged, out)

    written = sorted(Path(out).rglob("*.parquet"))
    assert written, "the write must have written something to check"
    for path in written:
        columns = store.workspace.con.sql(
            f"SELECT * FROM read_parquet('{path}')"
        ).columns
        assert "order_key" not in columns, f"{path.relative_to(out)} carries order_key"


def test_a_per_type_member_file_does_not_repeat_its_type(store, ac_dc):
    """`dims/entity_type/<T>.parquet` is indexed by `entity`, one column per attribute.

    The type is the file the rows are in, and it reaches a reader already:
    `dims/entity.parquet` carries `entity -> entity_type` so nobody has to
    glob. A column repeating it here would be a second copy, and the one that
    can disagree.

    The *axis* file keeps it, and that is a different file: there `entity_type`
    is the key, not a restatement of the path.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    - [the entity axis](https://energy-models.github.io/datarecord/design/format/#the-entity-axis)
    """
    revision = export_network(ac_dc, store)
    layer = Path(store.layer_dir(revision.id))

    members = layer / "dims" / "entity_type" / "Generator.parquet"
    columns = store.workspace.con.sql(
        f"SELECT * FROM read_parquet('{members}')"
    ).columns
    assert "entity" in columns, "still indexed by entity"
    assert "entity_type" not in columns, "the filename already says which type"

    # Derived from those files all the same, so the type still reaches a reader.
    axis = store.workspace.con.sql(
        f"SELECT * FROM read_parquet('{layer / 'dims' / 'entity.parquet'}')"
    )
    assert "entity_type" in axis.columns
    types = {t for (t,) in axis.project("entity_type").distinct().fetchall()}
    assert "Generator" in types, "the type survives being off the member file"


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
    assert record.schema == _SCHEMA
    assert workspace.at(out).schema == _SCHEMA

    # And it is that file answering: a standalone record is one whole record,
    # so it reads the same through a workspace that knows nothing about it.
    elsewhere = Workspace()
    try:
        again = elsewhere.at(out)
        assert again.schema == _SCHEMA
        assert again.resolver.revision_id == record.resolver.revision_id, (
            "the manifest's id names one layer to every reader"
        )
    finally:
        elsewhere.close()


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
    """A whole-record write never half-replaces what a record already holds.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    """
    out = str(tmp_path / "standalone")
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    workspace.write(source, out)

    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, out)


def test_a_standalone_write_refuses_an_existing_empty_directory(workspace, tmp_path):
    """Refused up front, where the write itself would fail on the path existing.

    Notes
    -----
    - [a standalone directory](https://energy-models.github.io/datarecord/design/writing/#a-standalone-directory)
    """
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    with pytest.raises(FileExistsError, match="does not exist yet"):
        workspace.write(source, str(tmp_path))
    assert source.built == [], "refused before any frame was built"
    assert list(tmp_path.iterdir()) == [], "refused before anything was written"

    assert workspace.write(source, str(tmp_path / "new")).schema == _SCHEMA


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
    return record.attributes["p_nom"].collect()["value"].to_list()


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


def test_a_file_carries_only_its_own_attributes_coordinates(store, ac_dc):
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
    revision = export_network(ac_dc, store)
    inputs = Path(store.layer_dir(revision.id), "inputs")

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
        "attribute",
        "breakpoint",
        "value",
    }, "a component attribute carries `entity`, not the connection group's `bus`"

    efficiency = columns("efficiency")
    assert "connection" in efficiency, "a connection attribute carries the group"
    assert "bus" not in efficiency, "as one struct column, not its coordinates"
    assert "period" not in efficiency, "and no dim it is not addressed by"


# -- validation -------------------------------------------------------------


def test_a_write_rejects_an_undeclared_attribute(store):
    """An attribute with no spec has no shape, so there is nothing to write it as.

    Its `dims` are what say which columns the file carries, so writing one the
    schema does not declare would put a file in `inputs/` whose column set no
    reader could derive. A *result* is exempt - a tool derives those from its
    own registry, never from the schema.

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


def test_a_write_rejects_a_group_frame_missing_a_coordinate(store):
    """A group's row is keyed by its coordinates, so one lacking them misresolves.

    Notes
    -----
    - [groups](https://energy-models.github.io/datarecord/design/schema/#groups)
    """
    source = _Source(
        _SCHEMA,
        connections={"connection": pd.DataFrame({"entity": ["steel_dri"]})},  # no `bus`
    )

    with pytest.raises(ValueError, match="coordinates.*bus"):
        store.root(source)


def test_a_write_rejects_a_name_two_types_share(store):
    """Names are unique across every type, checked before anything is written.

    The attribute rows record no type, so two components sharing a name would
    silently share every attribute key - which is why this is enforced rather
    than assumed.

    Notes
    -----
    - [entity is unique across types](https://energy-models.github.io/datarecord/design/format/#entity-is-unique-across-types)
    """
    source = _Source(
        _SCHEMA,
        entity_types={
            "Process": pd.DataFrame({"entity": ["shared"], "scenario": [None]}),
            "Widget": pd.DataFrame({"entity": ["shared"], "scenario": [None]}),
        },
    )

    # The detail names the name and the types claiming it, in that order - the
    # message is what a caller acts on, so a transposed pair is a defect.
    with pytest.raises(ValueError, match=r"'shared' is a Process and a Widget"):
        store.root(source)
    _nothing_reachable(store)


def test_a_write_accepts_one_name_per_type(store):
    """The negative half: the same two types with distinct names write fine."""
    source = _Source(
        _SCHEMA,
        entity_types={
            "Process": pd.DataFrame({"entity": ["a"], "scenario": [None]}),
            "Widget": pd.DataFrame({"entity": ["b"], "scenario": [None]}),
        },
    )

    revision = store.root(source)
    assert Path(store.layer_dir(revision.id)).exists()


def test_the_uniqueness_check_spans_backends(store):
    """A `Record` may hand over one type as DuckDB and another as pandas.

    `WorkingRecord` does exactly this, mixing base frames with staged ones, so
    the check must not assume the component frames share a backend - `nw.concat`
    refuses a mixed list outright.

    Notes
    -----
    - [the long schema](https://energy-models.github.io/datarecord/design/format/#the-long-schema)
    """
    source = _Source(
        _SCHEMA,
        entity_types={
            # DuckDB-backed, and pandas-backed, colliding on `shared`.
            "Process": store.workspace.con.sql(
                "SELECT 'shared' AS entity, NULL AS scenario"
            ),
            "Widget": pd.DataFrame({"entity": ["shared"], "scenario": [None]}),
        },
    )

    with pytest.raises(ValueError, match="entity types reuse names"):
        store.root(source)


def test_a_write_rejects_a_nested_axis_without_its_parent(store):
    """A `within` dim's file needs a column per parent, or the fold miskeys it.

    `snapshot within period` makes the axis key `(period, snapshot)`, so a
    `snapshot.parquet` carrying only timestamps would fold two periods'
    identically labelled hours into one row.

    Notes
    -----
    - [within](https://energy-models.github.io/datarecord/design/schema/#within-an-axis-inside-an-axis)
    """
    nested = schema(within={"snapshot": {"period"}})
    source = _Source(
        nested,
        dims={"snapshot": pd.DataFrame({"snapshot": pd.to_datetime(["2020-01-01"])})},
    )

    with pytest.raises(ValueError, match="axis key columns.*period"):
        store.root(source)
    _nothing_reachable(store)


def test_a_write_rejects_an_undeclared_axis_column(store):
    """An axis file's payload is the schema's to state, like a long frame's.

    A column no declaration accounts for would be read back with no dtype and
    no meaning, so it is refused rather than carried along.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    source = _Source(
        schema(),
        dims={"scenario": pd.DataFrame({"scenario": ["high"], "nonsense": [1.0]})},
    )

    with pytest.raises(ValueError, match="does not declare for the 'scenario' axis"):
        store.root(source)
    _nothing_reachable(store)


def test_an_axis_carries_the_attributes_addressed_by_it_alone(store):
    """`weight` over `scenario` alone is a column of that axis's file.

    Declared, so it round-trips with a dtype - which is what distinguishes it
    from the undeclared column above.

    Notes
    -----
    - [where a value lives](https://energy-models.github.io/datarecord/design/format/#where-a-value-lives)
    """
    declared = schema()
    assert "weight" in declared.attributes_on("scenario")

    source = _Source(
        declared,
        dims={"scenario": pd.DataFrame({"scenario": ["high"], "weight": [0.4]})},
    )
    revision = store.root(source)

    axis = revision.resolver.dims.axes["scenario"].df()
    assert dict(zip(axis["scenario"], axis["weight"])) == {"high": 0.4}


_AWKWARD = Schema(
    dimensions={
        "entity": Dimension(dtype=nw.String()),
        "select": Dimension(dtype=nw.String()),
        "Horizon": Dimension(dtype=nw.String()),
    },
    attributes={
        "Order Size": AttributeSpec(
            dtype=nw.Float64(), dims={"entity", "select", "Horizon"}
        ),
        "Group": AttributeSpec(dtype=nw.Float64(), dims={"select"}),
    },
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
            "select": pd.DataFrame({"select": ["a", "b"], "Group": [1.0, 2.0]}),
            "Horizon": pd.DataFrame({"Horizon": ["near", "far"]}),
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
    assert again.schema == _AWKWARD
    flat = again.attributes["Order Size"].collect().to_pandas()
    assert dict(zip(zip(flat["select"], flat["Horizon"]), flat["value"])) == got, (
        "a flattened standalone copy reads the same"
    )


# -- the PyPSA source (https://energy-models.github.io/datarecord/design/format/) ------------------------------------------------


def test_to_datarecord_lists_without_unpivoting(store, ac_dc):
    """Key sets come off the network and its registry, so listing is cheap."""
    source = PyPSA.to_datarecord(ac_dc, store.workspace.con)

    assert isinstance(source, RecordLike)
    assert "Generator" in source.entity_types
    assert "connection" in source.groups
    assert "p_max_pu" in source.attributes
    # Non-varying attributes belong to `dims/entity_type/`, not `inputs/` (https://energy-models.github.io/datarecord/design/record/).
    assert "v_nom" not in source.attributes
    # A port attribute is one bus-keyed attribute, not one per port (https://energy-models.github.io/datarecord/design/record/#connections).
    assert "efficiency" in source.attributes
    assert "efficiency2" not in source.attributes


def test_write_then_build_round_trips(store, ac_dc):
    """A network written by blocks and read back through `build` is unchanged.

    Distinct from `test_roundtrip.py`, which reads an `export_to_parquet`
    record: this exercises the writer and the connection collapse in one pass.

    Notes
    -----
    - [the record format](https://energy-models.github.io/datarecord/design/format/)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = export_network(ac_dc, store)

    assert not PyPSA.verify(revision.record)
    back = PyPSA.build(revision.record)

    for ctype in ("Bus", "Generator", "Link", "Line", "Load"):
        original, rebuilt = ac_dc.c[ctype].static, back.c[ctype].static
        assert list(rebuilt.index) == list(original.index), ctype
        # Every column survives, custom ones included (`Bus.country` has no
        # registry entry and must not be silently dropped).
        assert set(rebuilt.columns) == set(original.columns), ctype
        for column in original.columns:
            assert rebuilt[column].astype(str).equals(original[column].astype(str)), (
                ctype,
                column,
            )


def test_multi_port_links_round_trip_through_connections(store, ac_dc):
    """`bus0`/`bus1` become connection rows and come back as columns.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = export_network(ac_dc, store)

    # Stored bus-keyed, with a role from PyPSA's sign convention. One file for
    # every type, so the Links are reached by their entities.
    rows = store.workspace.con.read_parquet(
        store.layer_dir(revision.id) + "groups/connection.parquet"
    ).df()
    links = rows[rows["entity"].isin(ac_dc.c["Link"].static.index)]
    assert set(links["role"]) == {"input", "output"}
    assert set(links["bus"]) >= set(ac_dc.c["Link"].static["bus0"])

    back = PyPSA.build(revision.record)
    assert list(back.c["Link"].static["bus0"]) == list(ac_dc.c["Link"].static["bus0"])
    assert list(back.c["Link"].static["bus1"]) == list(ac_dc.c["Link"].static["bus1"])


def test_single_port_components_keep_their_unsuffixed_bus(store, ac_dc):
    """A Generator's one `bus` is a connection too, and stays `bus`.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = export_network(ac_dc, store)

    rows = store.workspace.con.read_parquet(
        store.layer_dir(revision.id) + "groups/connection.parquet"
    ).df()
    mine = rows[rows["entity"].isin(ac_dc.c["Generator"].static.index)]
    assert set(mine["role"]) == {"attached"}

    back = PyPSA.build(revision.record)
    assert list(back.c["Generator"].static["bus"]) == list(
        ac_dc.c["Generator"].static["bus"]
    )


def test_static_series_split_survives_the_writer(store, ac_dc):
    """Only the components with a series get a `dynamic` column.

    Notes
    -----
    - [consuming a record](https://energy-models.github.io/datarecord/design/tools/)
    """
    revision = export_network(ac_dc, store)
    back = PyPSA.build(revision.record)

    assert sorted(back.c["Generator"].dynamic["p_max_pu"].columns) == sorted(
        ac_dc.c["Generator"].dynamic["p_max_pu"].columns
    )


def test_written_layer_overlays(store, ac_dc):
    """A written layer is an ordinary layer: a child patches it as any other."""
    from tests.fixtures import write_input

    root = export_network(ac_dc, store)
    root.materialise()

    child = empty_child(root)
    write_input(
        store.layer_dir(child.id),
        "p_nom",
        [{"entity": "Manchester Wind", "value": 999.0}],
    )

    resolved = relation(child, "p_nom").filter("entity = 'Manchester Wind'").df()
    assert list(resolved["value"]) == [999.0]
