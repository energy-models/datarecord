"""`Record` over an overlay and over a directory.

Notes
-----
- [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
"""

from dataclasses import dataclass

import narwhals as nw
import pytest

from datarecord import Revision
from datarecord.directory import DirectoryRecord
from datarecord.duck import layer_dir, resolved_dir
from datarecord.layered.revision import LayeredRecord
from datarecord.layered.write import write_record
from datarecord.record import EMPTY, Flags, Frames, Record
from datarecord.schema import Schema
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import schema, write_components, write_input, write_schema

MEMBERS = ("dims", "components", "connections", "attributes")


@pytest.fixture
def written(con, base_uri, ac_dc):
    """A record whose layer blocks wrote, so both backings can read the same record."""
    revision = Revision.create(con)
    write_record(revision.id, PyPSA.to_datarecord(ac_dc), con)
    return revision


@pytest.fixture
def both(written, con):
    return (
        LayeredRecord(written.node_cache),
        DirectoryRecord(layer_dir(written.id), con),
    )


# -- the protocol ------------------------------------------------------------


def test_both_backings_satisfy_the_protocol(both):
    """Structural conformance, so a consumer cannot tell which it holds.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """
    for record in both:
        assert isinstance(record, Record)


def test_a_network_source_is_a_record(ac_dc):
    """`to_datarecord` returns one too, which is what puts read and write on one seam."""
    assert isinstance(PyPSA.to_datarecord(ac_dc), Record)


def test_a_plain_dict_backed_record_satisfies_the_protocol(con):
    """`Frames` is the `Mapping` ABC, so eager `dict` values are a valid backing.

    The protocol asks for named lazy frames, not for a mapping that defers
    building them - so whether an implementation is lazy like `LazyFrames` or
    eager like this stays its own business.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    """
    members = nw.from_native(
        con.sql(
            "SELECT 'Generator' AS component_type, 'wind' AS entity,"
            " NULL::VARCHAR AS scenario"
        )
    )
    long = nw.from_native(
        con.sql(
            "SELECT 'Generator' AS component_type, 'wind' AS entity,"
            " NULL::VARCHAR AS bus, 'p_nom' AS attribute,"
            " NULL::DOUBLE AS breakpoint, 100.0 AS value,"
            " NULL::VARCHAR AS snapshot, NULL::VARCHAR AS scenario,"
            " NULL::BIGINT AS period"
        )
    )

    @dataclass(frozen=True)
    class DictRecord:
        schema: Schema
        dims: Frames
        components: Frames
        connections: Frames
        attributes: Frames
        outputs: Frames

        def flags(self, ctype: str) -> dict[str, Flags]:
            return {}

    record = DictRecord(
        schema(), {}, {"Generator": members}, {}, {"p_nom": long}, EMPTY
    )
    assert isinstance(record, Record)
    # Results absent, spelled as an empty mapping rather than a protocol a
    # consumer has to test for (https://energy-models.github.io/datarecord/design/record/#frames).
    assert list(record.outputs) == []
    assert record.attributes["p_nom"].implementation == nw.Implementation.DUCKDB

    # And `write_record` consumes it, which is the point of widening the type:
    # it iterates and looks up, both of which a `dict` answers.
    revision = Revision.create(con)
    write_record(revision.id, record, con)
    assert "p_nom" in DirectoryRecord(layer_dir(revision.id), con).attributes


def test_revision_exposes_its_record(written):
    """`revision.record` is the entry point; `node_cache` stays the DuckDB view."""
    record = written.record
    assert isinstance(record, Record)
    assert isinstance(record, LayeredRecord)
    assert record.node_cache is written.node_cache


def test_backings_agree_on_every_key_set(both):
    """One record, two ways of reading it: the keys must not depend on which."""
    node, directory = both
    for member in MEMBERS:
        assert list(getattr(node, member)) == list(getattr(directory, member)), member


def test_backings_agree_on_flags(both):
    """Owner map and file aggregate answer the same question.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """
    node, directory = both
    for ctype in sorted(node.components):
        assert node.flags(ctype) == directory.flags(ctype), ctype


def test_backings_agree_on_rows(both):
    """A single-layer record resolves to the same rows either way."""
    node, directory = both
    for attribute in node.attributes:
        left = node.attributes[attribute].collect().to_native()
        right = directory.attributes[attribute].collect().to_native()
        assert len(left) == len(right), attribute


# -- laziness (https://energy-models.github.io/datarecord/design/record/#frames, https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations) ---------------------------------------------------


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
    """Listing a record is cheap; only a lookup does work.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    """
    for record in both:
        assert "p_max_pu" in record.attributes
        assert "nope" not in record.attributes
        assert len(list(record.attributes)) == len(record.attributes)


# -- flags, the one non-frame member (https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations) ----------------------------------


def test_flags_are_per_component_type(con, base_uri):
    """One file, two types, different shapes - OR-ing across them would lose both."""
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"entity": "wind"}])
    write_components(layer, "Link", [{"entity": "dc"}])
    write_input(
        layer,
        "p_max_pu",
        [
            # Generator: series only. Link: static only.
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"entity": "dc", "value": 1.0}],
    )

    for record in (LayeredRecord(revision.node_cache), DirectoryRecord(layer, con)):
        generator = record.flags("Generator")["p_max_pu"]
        link = record.flags("Link")["p_max_pu"]
        # The Generator's rows set `snapshot`; the Link's leaves it NULL. Naming
        # the dim is what makes these two answers distinguishable at all.
        assert "snapshot" in generator.varies
        assert "snapshot" not in generator.broadcast
        assert "snapshot" in link.broadcast
        assert "snapshot" not in link.varies


def test_a_materialised_map_survives_a_dim_being_declared(con, base_uri):
    """Adding a dim is compatible, and the persisted owner map too.

    The flags live in two structs rather than a `varies_<dim>` column each, so
    a map written before the dim existed differs from the current schema by a
    struct *field*, which `UNION ALL BY NAME` fills with NULL - where a missing
    *column* would have to be reconciled. The new dim reads as unset, which is
    what it is: no row mentions it.

    Notes
    -----
    - [versioning](https://energy-models.github.io/datarecord/design/schema/#versioning)
    - [the owner map](https://energy-models.github.io/datarecord/design/read-path/#owner-map)
    """
    narrow = {"snapshot": "TIMESTAMP", "period": "BIGINT"}
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema(dims=narrow, keys={}, partial=set()))
    write_components(layer, "Generator", [{"entity": "wind"}])
    write_input(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ],
    )
    revision.materialise()
    # The map on disk knows nothing of `scenario` - without that this test
    # would pass whatever the flags' layout.
    uri = f"{resolved_dir(revision.id)}owner_map/inputs.parquet"
    persisted = con.sql(f"SELECT varies FROM read_parquet('{uri}')")
    assert "scenario" not in str(persisted.types[0])

    # The dim arrives after the map is on disk.
    write_schema(schema(dims={**narrow, "scenario": "VARCHAR"}, keys={}, partial=set()))
    child = revision.child()
    flags = LayeredRecord(child.node_cache).flags("Generator")["p_max_pu"]
    assert "snapshot" in flags.varies
    assert "scenario" not in flags.varies
    assert "scenario" not in flags.broadcast


def test_flags_report_both_sets_where_components_disagree(con, base_uri):
    """A mixed type puts `snapshot` in both sets, which is the split.

    Two generators, one with a series and one with a single value. Both
    containers are needed, and both sets holding `snapshot` is what says so -
    the constant pass takes the NULL-snapshot rows, the series pass the rest.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"entity": "wind"}, {"entity": "gas"}])
    write_input(
        layer,
        "p_max_pu",
        [
            {"entity": "wind", "snapshot": s, "value": v}
            for s, v in (("2030-01-01", 0.4), ("2030-01-02", 0.6))
        ]
        + [{"entity": "gas", "value": 1.0}],
    )

    for record in (LayeredRecord(revision.node_cache), DirectoryRecord(layer, con)):
        combined = record.flags("Generator")["p_max_pu"]
        assert "snapshot" in combined.varies
        assert "snapshot" in combined.broadcast


def test_flags_report_a_curve(con, base_uri):
    """`breakpoints` distinguishes a curve from a scalar, from either backing.

    Notes
    -----
    - [Flags](https://energy-models.github.io/datarecord/design/record/#flags)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, "Process", [{"entity": "steel"}])
    write_input(
        layer,
        "marginal_cost",
        [
            {"entity": "steel", "breakpoint": x, "value": v}
            for x, v in ((0.0, 20.0), (50.0, 35.0))
        ],
    )

    for record in (LayeredRecord(revision.node_cache), DirectoryRecord(layer, con)):
        assert record.flags("Process")["marginal_cost"].breakpoints


# -- what only the overlay can do -------------------------------------------


def test_node_record_resolves_the_overlay(con, base_uri, ac_dc):
    """The point of two backings: one reads a layer, the other the resolution."""
    root = Revision.create(con)
    write_record(root.id, PyPSA.to_datarecord(ac_dc), con)
    root.materialise()

    child = root.child()
    write_input(
        layer_dir(child.id),
        "p_max_pu",
        [{"entity": "Manchester Gas", "value": 0.1}],
    )

    overlay = LayeredRecord(child.node_cache)
    layer_only = DirectoryRecord(layer_dir(child.id), con)

    # The child's own layer holds one row; the resolution holds the root's too.
    assert len(layer_only.attributes["p_max_pu"].collect().to_native()) == 1
    resolved = overlay.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert len(resolved) > 1
    patched = resolved[resolved["entity"] == "Manchester Gas"]["value"].tolist()
    assert patched == [0.1]


def test_node_record_orders_members(con, base_uri, ac_dc):
    """A `Record` promises member order; for an overlay that means `order_key`.

    Notes
    -----
    - [what differs between the implementations](https://energy-models.github.io/datarecord/design/read-path/#what-differs-between-the-implementations)
    """
    root = Revision.create(con)
    write_record(root.id, PyPSA.to_datarecord(ac_dc), con)
    root.materialise()

    child = root.child()
    write_components(layer_dir(child.id), "Generator", [{"entity": "New Solar"}])

    names = list(
        LayeredRecord(child.node_cache)
        .components["Generator"]
        .collect()
        .to_native()
        .to_pandas()["entity"]
    )
    # First-introduced order: the root's members, then the child's addition.
    assert names == [*ac_dc.c["Generator"].static.index, "New Solar"]


# -- the directory backing on its own ----------------------------------------


def test_directory_record_reads_a_plain_record(con, base_uri, ac_dc, tmp_path):
    """No record, no overlay: any parquet directory blocks wrote is a `Record`."""
    revision = Revision.create(con)
    write_record(revision.id, PyPSA.to_datarecord(ac_dc), con)

    record = DirectoryRecord(layer_dir(revision.id), con)
    assert isinstance(record, Record)
    assert "Generator" in record.components
    assert "p_max_pu" in record.attributes
    assert record.schema.attributes


def test_directory_record_has_no_connections_when_none_were_written(
    con, base_uri, tmp_path
):
    """A record with no `dims/connections/` reads as having none, not as an error.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    revision = Revision.create(con)
    layer = layer_dir(revision.id)
    write_schema(schema())
    write_components(layer, "Generator", [{"entity": "wind"}])

    assert list(DirectoryRecord(layer, con).connections) == []


def test_directory_record_reads_connections_blocks_wrote(written, con):
    """A record blocks wrote has them, with the roles the collapse assigned.

    Notes
    -----
    - [connections](https://energy-models.github.io/datarecord/design/record/#connections)
    """
    record = DirectoryRecord(layer_dir(written.id), con)
    assert "Link" in record.connections

    rows = record.connections["Link"].collect().to_native().to_pandas()
    assert set(rows["role"]) == {"input", "output"}


def test_missing_key_raises(both):
    """A key the record does not hold is a `KeyError`, not an empty frame."""
    for record in both:
        with pytest.raises(KeyError):
            record.attributes["not_an_attribute"]


def test_outputs_are_empty_until_solved(both):
    """An unsolved network's results are absent rather than defaults.

    Notes
    -----
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    for record in both:
        assert list(record.outputs) == []


def test_outputs_is_an_ordinary_record_member(both, con, base_uri):
    """`outputs` is on `Record`; emptiness is the existence answer.

    Notes
    -----
    - [Frames](https://energy-models.github.io/datarecord/design/record/#frames)
    - [outputs](https://energy-models.github.io/datarecord/design/read-path/#outputs)
    """
    node, directory = both
    # No separate protocol to satisfy: an unsolved record answers with an empty
    # mapping, the same way every other member answers for what it lacks.
    for record in (node, directory):
        assert isinstance(record, Record)
        assert list(record.outputs) == []


def test_write_record_omits_outputs_for_an_unsolved_source(con, base_uri, ac_dc):
    """A source with no results produces a layer with no `outputs/`.

    Notes
    -----
    - [writing a whole record](https://energy-models.github.io/datarecord/design/writing/)
    """
    from datarecord.duck import try_read_parquet

    solved = PyPSA.to_datarecord(ac_dc)

    class Unsolved:
        """The same record, with no results: `outputs` answers empty."""

        schema = solved.schema
        dims = solved.dims
        components = solved.components
        connections = solved.connections
        attributes = solved.attributes
        outputs = EMPTY
        flags = solved.flags

    source = Unsolved()
    assert isinstance(source, Record)

    revision = Revision.create(con)
    # An empty `outputs` writes no `outputs/` at all, rather than an empty
    # directory (https://energy-models.github.io/datarecord/design/writing/).
    write_record(revision.id, source, con)
    layer = layer_dir(revision.id)
    assert try_read_parquet(layer + "outputs/*.parquet", con) is None
    assert "p_max_pu" in DirectoryRecord(layer, con).attributes


# -- one schema per record root (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record) ----------------------------------------


def test_two_roots_in_one_process_read_their_own_schema(tmp_path):
    """A connection's schema comes from *its* root, not the process default.

    `connect(base_uri=...)` already scopes a connection to one record - its
    `layer_dir` macro derives from that root - so the manifest
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
        roots[name] = (root, con, Revision.create(con))

    (_, _, revision_a), (root_b, con_b, revision_b) = roots["a"], roots["b"]
    assert revision_a.record.schema.dims == ("scenario",)
    assert revision_b.record.schema.dims == ("vintage",)

    # A layer read directly needs no schema supplied either: its own directory
    # carries none (https://energy-models.github.io/datarecord/design/schema/#one-schema-per-record), so the connection's root answers - which is what
    # `DirectoryRecord` used to take a `declared` argument for.
    layer = DirectoryRecord(layer_dir(revision_b.id, root_b), con_b)
    assert layer.schema.dims == ("vintage",)

    for _, con, _ in roots.values():
        con.close()
