"""Writing a layer from long-format frames (design doc §4)."""

from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

from datarecord import DataRecord
from datarecord.duck import layer_dir
from datarecord.node_cache import read_schema
from datarecord.store import EMPTY, DirectoryStore, LazyFrames, Store
from datarecord.tools.pypsa import PyPSA
from datarecord.write import add_patch, write_layer
from tests.fixtures import schema


class _Source:
    """A minimal `Store` over ready-made frames, counting each build."""

    def __init__(
        self,
        schema,
        attributes=None,
        components=None,
        connections=None,
        outputs=None,
        dims=None,
    ):
        self._schema = schema
        self.built: list[str] = []
        self._attributes = attributes or {}
        self._components = components or {}
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
    def components(self):
        return self._frames(self._components, "components")

    @property
    def connections(self):
        return self._frames(self._connections, "connections")

    @property
    def attributes(self):
        return self._frames(self._attributes, "attributes")

    @property
    def outputs(self):
        return self._frames(self._outputs, "outputs")

    def flags(self, ctype):
        return {}


def _long(**overrides) -> pd.DataFrame:
    """One long-schema row, with every §3 column present."""
    row = {
        "component_type": "Process",
        "name": "steel_dri",
        "bus": None,
        "snapshot": None,
        "scenario": None,
        "period": None,
        "attribute": "p_nom",
        "breakpoint": None,
        "value": 1.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


_SCHEMA = schema()


# -- the lazy mapping (§4) -------------------------------------------------


def test_source_is_explorable_without_building(con, base_uri):
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


def test_write_layer_builds_each_key_once(con, base_uri):
    """The writer looks up every key exactly once, and only what it writes."""
    record = DataRecord.create(con)
    source = _Source(
        _SCHEMA,
        attributes={"p_nom": _long(), "e_nom": _long(attribute="e_nom")},
        components={
            "Process": pd.DataFrame({"name": ["steel_dri"], "scenario": [None]})
        },
    )
    write_layer(record.id, source, con)

    assert sorted(source.built) == [
        "attributes:e_nom",
        "attributes:p_nom",
        "components:Process",
    ]


# -- creating a layer -------------------------------------------------------


def test_write_layer_creates_a_new_layer(con, base_uri):
    """Files land where `layer_dir` says - data only, no schema (§5.6).

    The store's one schema goes beside `layers/`, so a layer directory holds
    nothing but data. That is what keeps it a plain parquet store a reader
    knowing nothing about layering can open.
    """
    record = DataRecord.create(con)
    write_layer(record.id, _Source(_SCHEMA, attributes={"p_nom": _long()}), con)

    base = Path(layer_dir(record.id))
    assert (base / "inputs" / "p_nom.parquet").exists()
    assert not (base / "manifest.json").exists()
    # Written once for the whole tree, and it is what the layer is read under.
    assert read_schema() == _SCHEMA


def test_a_directory_target_carries_its_own_schema(con, base_uri, tmp_path):
    """A standalone store *is* one store, so its schema goes in the directory (§5.6)."""
    out = str(tmp_path / "standalone")
    write_layer(None, _Source(_SCHEMA, attributes={"p_nom": _long()}), con, uri=out)

    assert (Path(out) / "manifest.json").exists()
    assert DirectoryStore(out, con).schema == _SCHEMA


def test_write_layer_refuses_an_existing_layer(con, base_uri):
    """A whole-layer write never half-replaces what a record already holds (§4)."""
    record = DataRecord.create(con)
    source = _Source(_SCHEMA, attributes={"p_nom": _long()})
    write_layer(record.id, source, con)

    with pytest.raises(FileExistsError, match="already exists"):
        write_layer(record.id, source, con)


# -- validation -------------------------------------------------------------


def test_write_layer_rejects_a_missing_long_column(con, base_uri):
    """A frame the fold could not resolve is refused before anything is written."""
    record = DataRecord.create(con)
    short = _long().drop(columns=["breakpoint"])
    source = _Source(_SCHEMA, attributes={"p_nom": short})

    with pytest.raises(ValueError, match="missing long-schema columns.*breakpoint"):
        write_layer(record.id, source, con)
    assert not Path(layer_dir(record.id)).exists()


def test_write_layer_rejects_an_unbacked_key_dim(con, base_uri):
    """A schema keying by a dim the frames lack would misresolve (§5.5)."""
    record = DataRecord.create(con)
    source = _Source(
        _SCHEMA,
        components={"Process": pd.DataFrame({"name": ["steel_dri"]})},  # no `scenario`
    )

    with pytest.raises(ValueError, match="missing key dims.*scenario"):
        write_layer(record.id, source, con)


def test_write_layer_rejects_a_nested_axis_without_its_parent(con, base_uri):
    """A `within` dim's file needs a column per parent, or the fold miskeys it (§5.4).

    `snapshot within period` makes the axis key `(period, snapshot)`, so a
    `snapshots.parquet` carrying only timestamps would fold two periods'
    identically labelled hours into one row.
    """
    record = DataRecord.create(con)
    nested = schema(within={"snapshot": {"period"}})
    source = _Source(
        nested,
        dims={"snapshot": pd.DataFrame({"snapshot": pd.to_datetime(["2020-01-01"])})},
    )

    with pytest.raises(ValueError, match="axis key columns.*period"):
        write_layer(record.id, source, con)
    assert not Path(layer_dir(record.id)).exists()


# -- the PyPSA source (§4) ------------------------------------------------


def test_to_datarecord_lists_without_unpivoting(con, base_uri, ac_dc):
    """Key sets come off the network and its registry, so listing is cheap."""
    source = PyPSA.to_datarecord(ac_dc)

    assert isinstance(source, Store)
    assert "Generator" in source.components
    assert "Link" in source.connections
    assert "p_max_pu" in source.attributes
    # Non-varying attributes belong to `dims/components/`, not `inputs/` (§3).
    assert "v_nom" not in source.attributes
    # A port attribute is one bus-keyed attribute, not one per port (§6).
    assert "efficiency" in source.attributes
    assert "efficiency2" not in source.attributes


def test_write_then_build_round_trips(con, base_uri, ac_dc):
    """A network written by blocks and read back through `build` is unchanged.

    Distinct from `test_roundtrip.py`, which reads an `export_to_parquet`
    store: this exercises the writer of §4 and the connection collapse of
    §12 in one pass.
    """
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)

    assert not PyPSA.verify(record)
    back = PyPSA.build(record)

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


def test_multi_port_links_round_trip_through_connections(con, base_uri, ac_dc):
    """`bus0`/`bus1` become connection rows and come back as columns (§6, §12)."""
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)

    # Stored bus-keyed, with a role from PyPSA's sign convention.
    rows = con.read_parquet(layer_dir(record.id) + "dims/connections/Link.parquet").df()
    assert set(rows["role"]) == {"input", "output"}
    assert set(rows["bus"]) >= set(ac_dc.c["Link"].static["bus0"])

    back = PyPSA.build(record)
    assert list(back.c["Link"].static["bus0"]) == list(ac_dc.c["Link"].static["bus0"])
    assert list(back.c["Link"].static["bus1"]) == list(ac_dc.c["Link"].static["bus1"])


def test_single_port_components_keep_their_unsuffixed_bus(con, base_uri, ac_dc):
    """A Generator's one `bus` is a connection too, and stays `bus` (§6)."""
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)

    rows = con.read_parquet(
        layer_dir(record.id) + "dims/connections/Generator.parquet"
    ).df()
    assert set(rows["role"]) == {"attached"}

    back = PyPSA.build(record)
    assert list(back.c["Generator"].static["bus"]) == list(
        ac_dc.c["Generator"].static["bus"]
    )


def test_static_series_split_survives_the_writer(con, base_uri, ac_dc):
    """Only the components with a series get a `dynamic` column (§12)."""
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)
    back = PyPSA.build(record)

    assert sorted(back.c["Generator"].dynamic["p_max_pu"].columns) == sorted(
        ac_dc.c["Generator"].dynamic["p_max_pu"].columns
    )


def test_written_layer_overlays(con, base_uri, ac_dc):
    """A written layer is an ordinary layer: a child patches it as any other."""
    from tests.fixtures import write_input

    root = DataRecord.create(con)
    write_layer(root.id, PyPSA.to_datarecord(ac_dc), con)
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "p_nom",
        [{"component_type": "Generator", "name": "Manchester Wind", "value": 999.0}],
    )

    resolved = child.relation("p_nom").filter("name = 'Manchester Wind'").df()
    assert list(resolved["value"]) == [999.0]


# -- still out of scope (§4) ----------------------------------------------


def test_add_patch_is_not_implemented(con, base_uri):
    """Superseded by `MutableStore`, which needs no diff at all (§11)."""
    with pytest.raises(NotImplementedError, match="MutableStore"):
        add_patch(None, None, None, con)
