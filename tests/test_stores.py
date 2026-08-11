"""`Store` over an overlay and over a directory (design doc §9.3)."""

from dataclasses import dataclass

import narwhals as nw
import pytest

from datarecord import DataRecord
from datarecord.directory import DirectoryStore
from datarecord.duck import layer_dir, node_dir
from datarecord.layered.record import LayeredStore
from datarecord.layered.write import write_layer
from datarecord.schema import Schema
from datarecord.store import Flags, Frames, Solved, Store
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import schema, write_components, write_input, write_schema

MEMBERS = ("dims", "components", "connections", "attributes")


@pytest.fixture
def written(con, base_uri, ac_dc):
    """A record whose layer blocks wrote, so both backings can read the same store."""
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)
    return record


@pytest.fixture
def both(written, con):
    return (
        LayeredStore(written.node_cache),
        DirectoryStore(layer_dir(written.id), con),
    )


# -- the protocol ------------------------------------------------------------


def test_both_backings_satisfy_the_protocol(both):
    """Structural conformance, so a consumer cannot tell which it holds (§9.3)."""
    for store in both:
        assert isinstance(store, Store)


def test_a_network_source_is_a_store(ac_dc):
    """`to_datarecord` returns one too, which is what puts read and write on one seam."""
    assert isinstance(PyPSA.to_datarecord(ac_dc), Store)


def test_a_plain_dict_backed_store_satisfies_the_protocol(con):
    """`Frames` is the `Mapping` ABC, so eager `dict` values are a valid backing.

    The protocol asks for named lazy frames, not for a mapping that defers
    building them - so whether an implementation is lazy like `LazyFrames` or
    eager like this stays its own business (§4).
    """
    members = nw.from_native(
        con.sql(
            "SELECT 'Generator' AS component_type, 'wind' AS name,"
            " NULL::VARCHAR AS scenario"
        )
    )
    long = nw.from_native(
        con.sql(
            "SELECT 'Generator' AS component_type, 'wind' AS name,"
            " NULL::VARCHAR AS bus, 'p_nom' AS attribute,"
            " NULL::DOUBLE AS breakpoint, 100.0 AS value,"
            " NULL::VARCHAR AS snapshot, NULL::VARCHAR AS scenario,"
            " NULL::BIGINT AS period"
        )
    )

    @dataclass(frozen=True)
    class DictStore:
        schema: Schema
        dims: Frames
        components: Frames
        connections: Frames
        attributes: Frames

        def flags(self, ctype: str) -> dict[str, Flags]:
            return {}

    store = DictStore(schema(), {}, {"Generator": members}, {}, {"p_nom": long})
    assert isinstance(store, Store)
    # No results, so not `Solved` - and a `Store` need not carry them (§8).
    assert not isinstance(store, Solved)
    assert store.attributes["p_nom"].implementation == nw.Implementation.DUCKDB

    # And `write_layer` consumes it, which is the point of widening the type:
    # it iterates and looks up, both of which a `dict` answers.
    record = DataRecord.create(con)
    write_layer(record.id, store, con)
    assert "p_nom" in DirectoryStore(layer_dir(record.id), con).attributes


def test_record_exposes_its_store(written):
    """`record.store` is the entry point; `node_cache` stays the DuckDB view."""
    store = written.store
    assert isinstance(store, Store)
    assert isinstance(store, LayeredStore)
    assert store.node_cache is written.node_cache


def test_backings_agree_on_every_key_set(both):
    """One store, two ways of reading it: the keys must not depend on which."""
    node, directory = both
    for member in MEMBERS:
        assert list(getattr(node, member)) == list(getattr(directory, member)), member


def test_backings_agree_on_flags(both):
    """Owner map and file aggregate answer the same question (§9.3)."""
    node, directory = both
    for ctype in sorted(node.components):
        assert node.flags(ctype) == directory.flags(ctype), ctype


def test_backings_agree_on_rows(both):
    """A single-layer store resolves to the same rows either way."""
    node, directory = both
    for attribute in node.attributes:
        left = node.attributes[attribute].collect().to_native()
        right = directory.attributes[attribute].collect().to_native()
        assert len(left) == len(right), attribute


# -- laziness (§4, §9.3) ---------------------------------------------------


def test_frames_stay_unmaterialised(both):
    """An overlay-backed frame is a DuckDB plan, not a materialised table."""
    node, _ = both
    frame = node.attributes["p_max_pu"]
    assert isinstance(frame, nw.LazyFrame)
    assert frame.implementation == nw.Implementation.DUCKDB
    # A narwhals operation pushes into the plan rather than executing it.
    filtered = frame.filter(~nw.col("snapshot").is_null())
    assert "FILTER" in filtered.to_native().explain().upper()


def test_keys_list_without_building(both):
    """Listing a store is cheap; only a lookup does work (§4)."""
    for store in both:
        assert "p_max_pu" in store.attributes
        assert "nope" not in store.attributes
        assert len(list(store.attributes)) == len(store.attributes)


# -- flags, the one non-frame member (§9.3) ----------------------------------


def test_flags_are_per_component_type(con, base_uri):
    """One file, two types, different shapes - OR-ing across them would lose both."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"name": "wind"}])
    write_components(layer, "Link", [{"name": "dc"}])
    write_input(
        layer,
        "p_max_pu",
        [
            # Generator: series only. Link: static only.
            {"component_type": "Generator", "name": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"component_type": "Link", "name": "dc", "value": 1.0}],
    )

    for store in (LayeredStore(record.node_cache), DirectoryStore(layer, con)):
        generator = store.flags("Generator")["p_max_pu"]
        link = store.flags("Link")["p_max_pu"]
        # The Generator's rows set `snapshot`; the Link's leaves it NULL. Naming
        # the dim is what makes these two answers distinguishable at all.
        assert "snapshot" in generator.varies
        assert "snapshot" not in generator.broadcast
        assert "snapshot" in link.broadcast
        assert "snapshot" not in link.varies


def test_a_materialised_map_survives_a_dim_being_declared(con, base_uri):
    """Adding a dim is compatible (§5.7), and the persisted owner map too (§9.1).

    The flags live in two structs rather than a `varies_<dim>` column each, so
    a map written before the dim existed differs from the current schema by a
    struct *field*, which `UNION ALL BY NAME` fills with NULL - where a missing
    *column* would have to be reconciled. The new dim reads as unset, which is
    what it is: no row mentions it.
    """
    narrow = {"snapshot": "TIMESTAMP", "period": "BIGINT"}
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema(dims=narrow, keys={}, partial=set()))
    write_components(layer, "Generator", [{"name": "wind"}])
    write_input(
        layer,
        "p_max_pu",
        [
            {"component_type": "Generator", "name": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ],
    )
    record.materialise()
    # The map on disk knows nothing of `scenario` - without that this test
    # would pass whatever the flags' layout.
    uri = f"{node_dir(record.id)}owner_map/inputs.parquet"
    persisted = con.sql(f"SELECT varies FROM read_parquet('{uri}')")
    assert "scenario" not in str(persisted.types[0])

    # The dim arrives after the map is on disk.
    write_schema(schema(dims={**narrow, "scenario": "VARCHAR"}, keys={}, partial=set()))
    child = record.child()
    flags = LayeredStore(child.node_cache).flags("Generator")["p_max_pu"]
    assert "snapshot" in flags.varies
    assert "scenario" not in flags.varies
    assert "scenario" not in flags.broadcast


def test_flags_report_both_sets_where_components_disagree(con, base_uri):
    """A mixed type puts `snapshot` in both sets, which is the split (§4.3).

    Two generators, one with a series and one with a single value. Both
    containers are needed, and both sets holding `snapshot` is what says so -
    the constant pass takes the NULL-snapshot rows, the series pass the rest.
    """
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"name": "wind"}, {"name": "gas"}])
    write_input(
        layer,
        "p_max_pu",
        [
            {"component_type": "Generator", "name": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"component_type": "Generator", "name": "gas", "value": 1.0}],
    )

    for store in (LayeredStore(record.node_cache), DirectoryStore(layer, con)):
        combined = store.flags("Generator")["p_max_pu"]
        assert "snapshot" in combined.varies
        assert "snapshot" in combined.broadcast


def test_flags_report_a_curve(con, base_uri):
    """`breakpoints` distinguishes a curve from a scalar, from either backing (§2)."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, "Process", [{"name": "steel"}])
    write_input(
        layer,
        "marginal_cost",
        [
            {"component_type": "Process", "name": "steel", "breakpoint": x, "value": v}
            for x, v in ((0.0, 20.0), (50.0, 35.0))
        ],
    )

    for store in (LayeredStore(record.node_cache), DirectoryStore(layer, con)):
        assert store.flags("Process")["marginal_cost"].breakpoints


# -- what only the overlay can do -------------------------------------------


def test_node_store_resolves_the_overlay(con, base_uri, ac_dc):
    """The point of two backings: one reads a layer, the other the resolution."""
    root = DataRecord.create(con)
    write_layer(root.id, PyPSA.to_datarecord(ac_dc), con)
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"component_type": "Generator", "name": "Manchester Gas", "value": 0.1}],
    )

    overlay = LayeredStore(child.node_cache)
    layer_only = DirectoryStore(layer_dir(child.id), con)

    # The child's own layer holds one row; the resolution holds the root's too.
    assert len(layer_only.attributes["p_max_pu"].collect().to_native()) == 1
    resolved = overlay.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert len(resolved) > 1
    patched = resolved[resolved["name"] == "Manchester Gas"]["value"].tolist()
    assert patched == [0.1]


def test_node_store_orders_members(con, base_uri, ac_dc):
    """A `Store` promises member order; for an overlay that means `order_key` (§9.3)."""
    root = DataRecord.create(con)
    write_layer(root.id, PyPSA.to_datarecord(ac_dc), con)
    root.materialise()

    child = root.child()
    write_components(layer_dir(child.id), "Generator", [{"name": "New Solar"}])

    names = list(
        LayeredStore(child.node_cache)
        .components["Generator"]
        .collect()
        .to_native()
        .to_pandas()["name"]
    )
    # First-introduced order: the root's members, then the child's addition.
    assert names == [*ac_dc.c["Generator"].static.index, "New Solar"]


# -- the directory backing on its own ----------------------------------------


def test_directory_store_reads_a_plain_store(con, base_uri, ac_dc, tmp_path):
    """No record, no overlay: any parquet directory blocks wrote is a `Store`."""
    record = DataRecord.create(con)
    write_layer(record.id, PyPSA.to_datarecord(ac_dc), con)

    store = DirectoryStore(layer_dir(record.id), con)
    assert isinstance(store, Store)
    assert "Generator" in store.components
    assert "p_max_pu" in store.attributes
    assert store.schema.attributes


def test_directory_store_has_no_connections_when_none_were_written(
    con, base_uri, tmp_path
):
    """A store with no `dims/connections/` reads as having none, not as an error (§6)."""
    record = DataRecord.create(con)
    layer = layer_dir(record.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"name": "wind"}])

    assert list(DirectoryStore(layer, con).connections) == []


def test_directory_store_reads_connections_blocks_wrote(written, con):
    """A store blocks wrote has them, with the roles the collapse assigned (§6)."""
    store = DirectoryStore(layer_dir(written.id), con)
    assert "Link" in store.connections

    rows = store.connections["Link"].collect().to_native().to_pandas()
    assert set(rows["role"]) == {"input", "output"}


def test_missing_key_raises(both):
    """A key the store does not hold is a `KeyError`, not an empty frame."""
    for store in both:
        with pytest.raises(KeyError):
            store.attributes["not_an_attribute"]


def test_outputs_are_empty_until_solved(both):
    """An unsolved network's results are absent rather than defaults (§9.4)."""
    for store in both:
        assert list(store.outputs) == []


def test_outputs_is_a_separate_protocol(both, con, base_uri):
    """A `Store` need not carry results; a `Solved` one does (§8)."""
    node, directory = both
    # Both backings happen to implement `outputs`, so both are `Solved` - what
    # the split buys is that a consumer must ask rather than assume.
    for store in (node, directory):
        assert isinstance(store, Store)
        assert isinstance(store, Solved)


def test_write_layer_omits_outputs_for_an_unsolved_source(con, base_uri, ac_dc):
    """A source with no results produces a layer with no `outputs/` (§13)."""
    from datarecord.duck import try_read_parquet

    solved = PyPSA.to_datarecord(ac_dc)
    assert isinstance(solved, Solved)

    class Unsolved:
        """The same store, with the results member removed."""

        schema = solved.schema
        dims = solved.dims
        components = solved.components
        connections = solved.connections
        attributes = solved.attributes
        flags = solved.flags

    source = Unsolved()
    assert isinstance(source, Store)
    assert not isinstance(source, Solved)

    record = DataRecord.create(con)
    write_layer(record.id, source, con)
    layer = layer_dir(record.id)
    assert try_read_parquet(layer + "outputs/*.parquet", con) is None
    assert "p_max_pu" in DirectoryStore(layer, con).attributes


# -- one schema per store root (§5.6) ----------------------------------------


def test_two_roots_in_one_process_read_their_own_schema(tmp_path):
    """A connection's schema comes from *its* root, not the process default.

    `connect(base_uri=...)` already scopes a connection to one store - its
    `layer_dir`/`node_dir` macros derive from that root - so the manifest
    beside those layers is a property of the connection too. Two records on
    two roots therefore disagree about their dims without either being wrong.
    """
    from datarecord import duck
    from datarecord.layered.resolve import write_schema as write_manifest

    roots = {}
    for name, dims in (("a", {"scenario": "VARCHAR"}), ("b", {"vintage": "BIGINT"})):
        root = str(tmp_path / name)
        con = duck.connect(base_uri=root)
        write_manifest(schema(dims=dims, partial=set(dims), keys={}), root)
        roots[name] = (root, con, DataRecord.create(con))

    (_, _, record_a), (root_b, con_b, record_b) = roots["a"], roots["b"]
    assert record_a.store.schema.dims == ("scenario",)
    assert record_b.store.schema.dims == ("vintage",)

    # A layer read directly needs no schema supplied either: its own directory
    # carries none (§5.6), so the connection's root answers - which is what
    # `DirectoryStore` used to take a `declared` argument for.
    layer = DirectoryStore(layer_dir(record_b.id, root_b), con_b)
    assert layer.schema.dims == ("vintage",)

    for _, con, _ in roots.values():
        con.close()
