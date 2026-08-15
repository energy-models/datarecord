"""`WorkingRecord`: staging, the edit operations, commit (design doc §9)."""

import narwhals as nw
import pandas as pd
import pytest

from datarecord import Revision
from datarecord.directory import DirectoryRecord
from datarecord.duck import layer_dir
from datarecord.layered.resolve import read_schema, write_schema
from datarecord.mutable import Directory, NewChild, WorkingRecord, normalise_value
from datarecord.record import Record
from datarecord.schema import AttributeSpec
from datarecord.tools.pypsa import PyPSA
from tests.fixtures import export_network

GEN = "Generator"


@pytest.fixture
def root(con, base_uri, ac_dc):
    """A materialised record to branch edits from."""
    revision = Revision.create(con)
    export_network(ac_dc, revision, con)
    revision.materialise()
    return revision


@pytest.fixture
def staged(root, con):
    return WorkingRecord(root.record, con)


def _static(revision, attribute, ctype=GEN):
    """One attribute as the built network sees it, per component name.

    Through the build rather than `relation()`: a non-varying attribute like
    `p_nom` lives in `dims/components/` (§4.1), so `inputs/` alone would not
    show what the record resolves to.
    """
    return PyPSA.build(revision.record).c[ctype].static[attribute].to_dict()


# -- the protocol (§9.1) ----------------------------------------------------


def test_a_mutable_record_reads_as_a_record(staged):
    """Editable *and* readable: the pending edits are a layer, so reads compose.

    The load-bearing half of §9: a `WorkingRecord` satisfies `Record`, so what
    it reads is the data with its pending edits applied and it can be handed
    to anything that only knows how to read.
    """
    assert isinstance(staged, Record)


def test_nothing_is_pending_before_an_edit(staged):
    p = staged.pending
    assert (p.attributes, p.components, p.connections, p.tombstones) == ({}, {}, {}, {})


# -- value forms (§9.2) -----------------------------------------------------


def test_scalar_applies_to_every_name():
    names, values, dims = normalise_value(150.0, ["wind1", "wind2"], {})
    assert (names, values, dims) == (["wind1", "wind2"], [150.0, 150.0], {})


def test_a_sequence_is_positional():
    names, values, _ = normalise_value([150.0, 80.0], ["wind1", "wind2"], {})
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_mapping_supplies_its_own_names():
    names, values, _ = normalise_value({"wind1": 150.0, "wind2": 80.0}, None, {})
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 150.0, "wind2": 80.0}


def test_a_series_indexed_by_names_is_per_name():
    series = pd.Series({"wind1": 1.0, "wind2": 2.0})
    names, values, dims = normalise_value(series, ["wind1", "wind2"], {})
    assert names is not None
    assert dict(zip(names, values, strict=True)) == {"wind1": 1.0, "wind2": 2.0}
    assert dims == {}


def test_a_series_indexed_by_an_axis_is_per_coordinate():
    """The same type, read as a dim series - which axis labels it carries decides."""
    series = pd.Series({"2030-01-01": 0.4, "2030-01-02": 0.6})
    names, values, dims = normalise_value(
        series, None, {"snapshot": ["2030-01-01", "2030-01-02"]}
    )
    assert names is None
    assert values == [0.4, 0.6]
    assert list(dims) == ["snapshot"]


def test_a_sequence_of_the_wrong_length_is_rejected():
    with pytest.raises(ValueError, match="2 names"):
        normalise_value([1.0, 2.0, 3.0], ["wind1", "wind2"], {})


def test_an_ambiguous_index_is_rejected():
    """Matching both names and an axis has no single reading, so it is an error."""
    series = pd.Series({"wind1": 1.0})
    with pytest.raises(ValueError, match="matches both"):
        normalise_value(series, ["wind1"], {"scenario": ["wind1"]})


# -- set (§9.2) -------------------------------------------------------------


def test_set_stages_without_writing(staged, root):
    """Staging is not a layer: the record reads the edit, the record does not."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])

    assert staged.pending.attributes == {"p_nom": 1}
    # The record itself is untouched until commit.
    assert _static(root, "p_nom")["Manchester Wind"] != 150.0


def test_a_staged_edit_is_visible_through_the_record(staged):
    """§9.10: a set of pending edits is a layer, so the read resolves over it."""
    staged.set("p_max_pu", 0.42, names=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = set(rows[rows["name"] == "Manchester Wind"]["value"])
    assert got == {0.42}
    # Every other name still reads the base record's rows.
    assert set(rows["name"]) > {"Manchester Wind"}


def test_two_lazy_reads_stay_bound_to_their_own_relations(staged, con):
    """§3.5: a `Record` hands over unmaterialised frames, so two must not alias.

    The read path composes relations by replacement scan, which binds each one
    at build time. Registering them under a fixed catalog name instead would
    rebind on the second read and both frames would collapse onto the last
    one - the frames are lazy, so nothing forces the first before that happens.
    """
    staged.set("p_max_pu", 0.42, names=["Manchester Wind"])
    first = staged.attributes["p_max_pu"]

    staged.set("p_min_pu", 0.11, names=["Manchester Wind"])
    second = staged.attributes["p_min_pu"]

    # Collected only now, after the second frame was built.
    got_first = first.collect().to_native().to_pandas()
    got_second = second.collect().to_native().to_pandas()
    assert set(got_first["attribute"]) == {"p_max_pu"}
    assert 0.42 in set(got_first["value"])
    assert set(got_second["attribute"]) == {"p_min_pu"}

    # And nothing was left behind in the catalog to leak into the next read.
    views = {v for (v,) in con.sql("SELECT view_name FROM duckdb_views()").fetchall()}
    assert not {v for v in views if v.startswith("_")}


def test_last_write_wins_within_the_staging_area(staged, root):
    """Two edits to one key collapse to the later one, by `_seq` (§9.7)."""
    staged.set("p_nom", 100.0, names=["Manchester Wind"])
    staged.set("p_nom", 150.0, names=["Manchester Wind"])

    # Both are staged - `pending` counts rows, and the collapse is applied at
    # commit rather than on every edit (§9.6).
    assert staged.pending.attributes == {"p_nom": 2}
    child = staged.commit(NewChild(root))
    assert _static(child, "p_nom")["Manchester Wind"] == 150.0


def test_set_over_several_names(staged, root):
    staged.set("p_nom", 150.0, names=["Manchester Wind", "Norway Wind"])
    child = staged.commit(NewChild(root))

    got = _static(child, "p_nom")
    assert got["Manchester Wind"] == got["Norway Wind"] == 150.0


def test_set_rejects_an_unknown_name(staged):
    """A value for a name no layer declares would resolve to nothing (§9.5)."""
    with pytest.raises(KeyError, match="Nope"):
        staged.set("p_nom", 1.0, names=["Nope"])


def test_set_accepts_a_name_staged_by_add(staged, root):
    """`add` makes the name exist, so a value for it is no longer unknown (§9.8)."""
    staged.add(GEN, pd.DataFrame([{"name": "NewSolar", "carrier": "solar"}]))
    staged.set("p_nom", 7.0, names=["NewSolar"])

    child = staged.commit(NewChild(root))
    assert _static(child, "p_nom")["NewSolar"] == 7.0


# -- the overlay does not overlap (§3.3, §9.10) -----------------------------


def test_a_broadcast_edit_displaces_the_whole_series(staged):
    """A staged NULL dim means "all values of that dim", so it replaces them (§3.3).

    Rows never overlap within a record, so a broadcast edit and the base's
    per-snapshot rows cannot both survive - the edit covers every snapshot.
    """
    staged.set("p_max_pu", 0.42, names=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = rows[rows["name"] == "Manchester Wind"]
    assert set(mine["value"]) == {0.42}
    assert mine["snapshot"].isna().all()


def test_a_pointwise_edit_keeps_the_rest_of_the_series(staged):
    """An edit naming a coordinate displaces that one only (§3.3, §9.10).

    Keying the overlay on the input key alone would drop the whole series here,
    since it excludes the dims an attribute is not owned per (§5.5).
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["name"] == "Manchester Wind"].sort_values("snapshot")
    one = mine.iloc[[0]][["name", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, names=["Manchester Wind"])

    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = rows[rows["name"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_a_long_frame_naming_an_unknown_component_is_rejected(staged):
    """§9.8 applies to the frame form too: a typo is caught where it is typed."""
    with pytest.raises(KeyError, match="Nope"):
        staged.set(
            "p_max_pu",
            pd.DataFrame([{"name": "Nope", "value": 1.0}]),
        )


def test_an_expression_value_stages_the_whole_series(staged, root):
    """A derived edit covers every coordinate it read, not just one (§9.3)."""
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["name"] == "Manchester Wind"].sort_values("snapshot")

    staged.set("p_max_pu", nw.col("value") * 2, names=["Manchester Wind"])
    assert staged.pending.attributes == {"p_max_pu": len(mine)}

    child = staged.commit(NewChild(root))
    got = child.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = got[got["name"] == "Manchester Wind"].sort_values("snapshot")
    assert got["value"].tolist() == (mine["value"] * 2).tolist()


def test_flags_report_a_dim_a_staged_edit_introduces(staged, ac_dc):
    """A staged row's dims join the flags, unioned with the base answer (§9.10).

    `flags` is the one non-`Frames` member of `Record`, so the promise that a
    read reflects pending edits has to hold for it too - and it decides which
    container a consumer puts a value in (`PyPSA.build` splits static from
    series on exactly this). `marginal_cost` starts broadcast over `snapshot`
    and varying over nothing; a per-snapshot edit must add `snapshot` to
    `varies` while leaving `broadcast` alone, since the base's NULL-snapshot
    rows are still there.
    """
    before = staged.flags(GEN)["marginal_cost"]
    assert "snapshot" not in before.varies
    assert "snapshot" in before.broadcast

    staged.set(
        "marginal_cost",
        pd.DataFrame(
            [{"name": "Manchester Wind", "snapshot": ac_dc.snapshots[0], "value": 7.5}]
        ),
        names=["Manchester Wind"],
    )

    after = staged.flags(GEN)["marginal_cost"]
    assert "snapshot" in after.varies
    assert after.broadcast == before.broadcast


# -- value dtypes (§3.6, §5.2) -----------------------------------------------


def test_a_non_float_attribute_stages_and_commits(staged, root):
    """`value` carries the attribute's declared dtype, not always `DOUBLE` (§3.6).

    One staging table holds every attribute's values, so it stages `value` as
    text and casts to the declared dtype where the attribute is known - which
    is the point at which `inputs/<attr>.parquet` is per-attribute.
    """
    amended = read_schema()
    amended.attributes[GEN]["carrier"] = AttributeSpec(
        dtype="VARCHAR", dims={"scenario"}
    )
    write_schema(amended)

    staged.set("carrier", "solar", names=["Manchester Wind"])
    rows = staged.attributes["carrier"].collect().to_native().to_pandas()
    assert rows[rows["name"] == "Manchester Wind"]["value"].tolist() == ["solar"]

    child = staged.commit(NewChild(root))
    got = child.record.attributes["carrier"].collect().to_native().to_pandas()
    assert got[got["name"] == "Manchester Wind"]["value"].tolist() == ["solar"]


def test_a_float_attribute_stays_numeric(staged):
    """The cast is per attribute, so a `DOUBLE` one is not turned into text."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])

    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    assert rows["value"].dtype.kind == "f"
    assert 150.0 in set(rows["value"])


# -- ownership granularity (§5.5, §9.7) -------------------------------------


def test_a_non_partial_axis_is_restated_whole(staged, root):
    """Touching one snapshot makes the layer own the whole series (§5.5, §9.7).

    `snapshot` is declared but not `partial`, so a layer cannot patch one hour
    and leave the rest to the parent: the coordinates the edit did not name
    would resolve to nothing rather than to the parent's value. So the commit
    reads the resolved series and writes it out complete - the one commit-time
    read of parent data.
    """
    assert "snapshot" not in (staged.schema.partial or frozenset())
    assert staged.schema.owned_per(GEN, "p_max_pu") == frozenset()

    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["name"] == "Manchester Wind"].sort_values("snapshot")
    assert len(mine) > 1
    one = mine.iloc[[0]][["name", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, names=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    got = child.record.attributes["p_max_pu"].collect().to_native().to_pandas()
    got = got[got["name"] == "Manchester Wind"].sort_values("snapshot")
    assert len(got) == len(mine)
    # The edit applied, and every other hour kept the parent's value.
    assert got.iloc[0]["value"] == 0.123
    assert got.iloc[1:]["value"].tolist() == mine.iloc[1:]["value"].tolist()


def test_the_restated_series_is_in_the_layer_itself(staged, root, con):
    """The layer carries the whole extent, not a parent lookup at read time.

    A patch layer holds only edits (§9.7) - except along an axis owned whole,
    where the completed series must be in the layer, since that is what makes
    this layer its owner.
    """
    base = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    mine = base[base["name"] == "Manchester Wind"]
    one = mine.iloc[[0]][["name", "snapshot"]].assign(value=0.123)

    staged.set("p_max_pu", one, names=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert len(rows[rows["name"] == "Manchester Wind"]) == len(mine)
    # Only the touched component: an axis owned whole obliges the layer to
    # carry that key's extent, not every key's.
    assert set(rows["name"]) == {"Manchester Wind"}


def test_a_partial_axis_stays_a_patch(staged, root, con):
    """`scenario` *is* partial, so one value may be patched alone (§5.5)."""
    assert staged.schema.owned_per(GEN, "p_nom") == frozenset()
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_nom"].collect().to_native().to_pandas()
    # `p_nom` varies over nothing, so there is no extent to restate: one row.
    assert len(rows) == 1


# -- add and remove (§9.5) --------------------------------------------------


def test_add_then_commit_makes_a_component_exist(staged, root):
    staged.add(
        GEN,
        pd.DataFrame(
            [
                {
                    "name": "NewSolar",
                    "bus": "Manchester",
                    "role": "attached",
                    "carrier": "solar",
                    "p_nom": 42.0,
                }
            ]
        ),
    )
    assert staged.pending.components == {GEN: 1}

    child = staged.commit(NewChild(root))
    assert "NewSolar" in set(child.node_cache.components.df()["name"])

    static = PyPSA.build(child.record).c[GEN].static
    assert static.loc["NewSolar", "p_nom"] == 42.0
    assert static.loc["NewSolar", "carrier"] == "solar"


def test_add_rejects_a_name_another_type_already_holds(staged):
    """Names are unique across types, enforced at the edit (§4.3, §9.5).

    Not left to be discovered: the attribute rows record no type, so two
    components sharing a name would silently share every attribute key.
    """
    with pytest.raises(ValueError, match=r"'Manchester' is already a Bus"):
        staged.add(GEN, pd.DataFrame([{"name": "Manchester", "carrier": "solar"}]))


def test_add_accepts_a_name_of_its_own_type(staged, root):
    """Re-adding a name of the same type is an edit to that member, not a clash."""
    staged.add(GEN, pd.DataFrame([{"name": "Manchester Wind", "p_nom": 5.0}]))
    child = staged.commit(NewChild(root))
    assert (
        PyPSA.build(child.record).c[GEN].static.loc["Manchester Wind", "p_nom"] == 5.0
    )


def test_a_name_lookup_stays_a_query_until_it_is_filtered(staged):
    """The type lookup stays lazy, so an edit collects only the names it asks for.

    `set` resolves each named key to its type (§4.3), so a lookup that pulled
    every name into Python would price one edit at a full resolution.
    """
    names = ["Manchester Wind", "Norway Wind"]
    total = sum(len(staged._resolved_names(ct)) for ct in staged.components)
    assert total > len(names), "fixture too small for the assertion to mean anything"

    assert isinstance(staged._name_types(), nw.LazyFrame)
    assert staged._resolve_types(names) == {
        "Manchester Wind": GEN,
        "Norway Wind": GEN,
    }


def test_resolve_types_rejects_a_name_no_layer_declares(staged):
    """A value keyed to a name with no member row is caught, not dropped (§9.8)."""
    with pytest.raises(KeyError, match="no member row"):
        staged._resolve_types(["Manchester Wind", "Nowhere"])


def test_a_freed_name_may_be_reclaimed_by_another_type(staged, root):
    """`remove` then `add` under another type collapses to the later op (§4.3, §9.7).

    The staged entity rows are keyed without `component_type`, so one name has
    one answer. Partitioning on the type as well would keep both the Generator
    tombstone and the Bus member row, and commit would write a record whose two
    types share a name - the collision `write_record` rejects.
    """
    staged.remove(GEN, ["Manchester Wind"])
    staged.add("Bus", pd.DataFrame([{"name": "Manchester Wind"}]))

    rows = [
        r
        for r in staged._collapsed_entities("components").fetchall()
        if r[1] == "Manchester Wind"
    ]
    assert len(rows) == 1
    assert rows[0][0] == "Bus"

    # And it commits: a record with two rows for the name would be rejected.
    child = staged.commit(NewChild(root))
    assert "Manchester Wind" not in PyPSA.build(child.record).c[GEN].static.index


def test_add_routes_a_port_attribute_to_the_connections(staged, root):
    """`bus` keys a connection rather than being a member column (§3.2).

    Putting it in `dims/components/` would introduce a column the ancestors'
    files lack, which then reads as NULL for their rows - so every existing
    component would lose its bus.
    """
    staged.add(
        GEN,
        pd.DataFrame([{"name": "NewSolar", "bus": "Manchester", "role": "attached"}]),
    )
    child = staged.commit(NewChild(root))

    buses = PyPSA.build(child.record).c[GEN].static["bus"]
    assert buses["NewSolar"] == "Manchester"
    # The point of the routing: the inherited components keep theirs.
    assert buses["Manchester Wind"] == "Manchester"


def test_remove_tombstones_without_enumerating_attributes(staged, root):
    staged.remove(GEN, ["Norway Gas"])
    assert staged.pending.tombstones == {GEN: 1}

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in set(child.node_cache.components.df()["name"])


def test_remove_rejects_a_dim_that_keys_nothing(staged):
    with pytest.raises(KeyError, match="period"):
        staged.remove(GEN, ["Norway Gas"], period=2030)


def test_add_after_remove_leaves_the_component_alive(staged, root):
    """The collapse is per key by `_seq`, so the later `add` wins (§9.7)."""
    staged.remove(GEN, ["Norway Gas"])
    staged.add(GEN, pd.DataFrame([{"name": "Norway Gas", "carrier": "gas"}]))

    child = staged.commit(NewChild(root))
    assert "Norway Gas" in set(child.node_cache.components.df()["name"])


def test_a_tombstone_drops_that_components_staged_attributes(staged, root):
    """Removing a component discards values staged for it, via the anti-join (§9.7)."""
    staged.set("p_nom", 99.0, names=["Norway Gas"])
    staged.remove(GEN, ["Norway Gas"])

    child = staged.commit(NewChild(root))
    assert "Norway Gas" not in _static(child, "p_nom")


# -- connect and disconnect (§9.5, §3.2) --------------------------------------


def test_connect_stages_a_new_connection(staged, root):
    """A connection is a row keyed by `(name, bus)`, not a positional column (§3.2)."""
    staged.connect(
        "Generator",
        pd.DataFrame(
            [{"name": "Manchester Wind", "bus": "Norway", "role": "attached"}]
        ),
    )
    assert staged.pending.connections == {"Generator": 1}

    child = staged.commit(NewChild(root))
    rows = child.node_cache.connection_frame("Generator").df()
    got = set(rows[rows["name"] == "Manchester Wind"]["bus"])
    assert "Norway" in got


def test_disconnect_stages_a_tombstone(staged, root):
    """One `deleted` row per `(name, bus)`, scoped by the connection key dims (§3.2)."""
    staged.disconnect("Link", [("Norwich Converter", "Norwich")])
    # A disconnect is a deletion, so it counts as a tombstone rather than as a
    # connection staged to exist (§9.6).
    assert staged.pending.tombstones == {"Link": 1}
    assert staged.pending.connections == {}

    child = staged.commit(NewChild(root))
    rows = child.node_cache.connection_frame("Link").df()
    left = set(rows[rows["name"] == "Norwich Converter"]["bus"])
    assert "Norwich" not in left
    # The component's other port survives: deletion is per connection, not per
    # component (§3.2).
    assert "Norwich DC" in left


def test_disconnect_rejects_a_dim_that_keys_nothing(staged):
    with pytest.raises(KeyError, match="period"):
        staged.disconnect("Link", [("Norwich Converter", "Norwich")], period=2030)


def test_connect_needs_a_bus(staged):
    with pytest.raises(ValueError, match="'bus'"):
        staged.connect("Generator", pd.DataFrame([{"name": "Manchester Wind"}]))


# -- rollback (§9.6) --------------------------------------------------------


def test_rollback_discards_everything_staged(staged, root):
    staged.set("p_max_pu", 0.42, names=["Manchester Wind"])
    staged.remove(GEN, ["Norway Gas"])
    staged.rollback()

    assert staged.pending.attributes == {}
    assert staged.pending.tombstones == {}
    # And the record reads the base rows again.
    rows = staged.attributes["p_max_pu"].collect().to_native().to_pandas()
    assert 0.42 not in set(rows["value"])


def test_commit_clears_the_staging_area(staged, root):
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    staged.commit(NewChild(root))

    assert staged.pending.attributes == {}


# -- commit targets (§9.7) --------------------------------------------------


def test_a_child_layer_holds_only_the_edits(staged, root, con):
    """A patch layer is the edits alone; the fold resolves the rest (§9.7)."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    rows = layer.attributes["p_nom"].collect().to_native().to_pandas()
    assert list(rows["name"]) == ["Manchester Wind"]
    # Yet the resolved record reads every generator's value.
    assert len(_static(child, "p_nom")) > 1


def test_a_directory_target_writes_a_flattened_record(staged, root, con, tmp_path):
    """No parent to resolve against, so the whole record is written (§9.7)."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    out = str(tmp_path / "flat")
    assert staged.commit(Directory(out)) is None

    record = DirectoryRecord(out, con)
    rows = record.attributes["p_nom"].collect().to_native().to_pandas()
    got = dict(zip(rows["name"], rows["value"], strict=True))
    assert got["Manchester Wind"] == 150.0
    # Flattened: every component is present, not left to a parent to supply.
    members = record.components[GEN].collect().to_native().to_pandas()
    assert len(members) == 6


def test_a_committed_child_builds_a_network(staged, root):
    """The whole point: an edited record is still a buildable model (§10)."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    child = staged.commit(NewChild(root))

    assert (
        PyPSA.build(child.record).c[GEN].static.loc["Manchester Wind", "p_nom"] == 150.0
    )


# -- the `Expr` value form's raise rule (§9.3) -------------------------------


def test_an_expression_over_a_named_target_with_no_rows_raises(staged):
    """A named target that resolves to nothing is a failed change, not a no-op.

    The caller asked for these rows to take a new value and there is nothing to
    derive one from, so it fails loudly rather than staging zero rows (§9.3).
    """
    with pytest.raises(KeyError, match="no current value to derive from"):
        staged.set(
            "p_max_pu",
            nw.col("value") * 2,
            names=["Manchester Wind"],
            snapshot="1999-01-01",
        )


def test_an_unscoped_expression_over_an_absent_attribute_stages_nothing(staged):
    """`names=None` and no scope means "whatever resolves", so empty is an answer."""
    absent = next(
        a for a in sorted(staged.schema.attributes[GEN]) if a not in staged.attributes
    )
    staged.set(absent, nw.col("value") * 2)
    assert absent not in staged.pending.attributes


# -- results through `kind="outputs"` (§9.2, §7.4) ---------------------------


def test_results_stage_and_read_back_without_committing(staged):
    """A tool can attach what it solved and the record reads it (§9.2)."""
    assert list(staged.outputs) == []
    staged.set("p", 42.0, names=["Manchester Wind"], kind="outputs")

    rows = staged.outputs["p"].collect().to_native().to_pandas()
    assert dict(zip(rows["name"], rows["value"], strict=True)) == {
        "Manchester Wind": 42.0
    }
    # Staged as a result, so it is not an input.
    assert "p" not in staged.pending.attributes


def test_results_survive_a_commit_into_the_new_layer(staged, root, con):
    """Staged results land in the child's `outputs/`, alongside its inputs (§7.4)."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    staged.set("p", 42.0, names=["Manchester Wind"], kind="outputs")
    child = staged.commit(NewChild(root))

    layer = DirectoryRecord(layer_dir(child.id), con)
    assert "p" in layer.outputs
    rows = layer.outputs["p"].collect().to_native().to_pandas()
    assert rows["value"].tolist() == [42.0]
    # And the inputs went where inputs go.
    assert "p_nom" in layer.attributes


def test_results_accept_a_component_type_the_record_never_declared(staged):
    """A solve may derive a component the record has no member row for (§9.3.1).

    PyPSA's `SubNetwork` is the real case: it exists only after a solve, so
    requiring a declared member row - which an *input* value must have (§9.8) -
    would refuse a legitimate result.
    """
    staged.set("carrier", "AC", names=["1"], kind="outputs")
    rows = staged.outputs["carrier"].collect().to_native().to_pandas()
    assert rows["name"].tolist() == ["1"]

    # The same name as an input is still rejected: membership governs inputs.
    with pytest.raises(KeyError, match="member row"):
        staged.set("p_nom", 1.0, names=["NoSuchGenerator"])


def test_a_multi_type_results_frame_stages_by_name_alone(staged, root, con):
    """One frame spanning types is one call, keyed by name alone (§4.3, §9.3.1).

    `Tool.results` hands over one frame per attribute carrying every type's rows
    (§10); with names unique there is no type to stamp, so the frame needs no
    `component_type` and nothing can be silently relabelled.
    """
    frame = pd.DataFrame(
        [
            {"name": "Manchester Wind", "value": 1.0},
            {"name": "0", "value": 2.0},
        ]
    )
    staged.set("p", frame, kind="outputs")

    rows = staged.outputs["p"].collect().to_native().to_pandas()
    assert dict(zip(rows["name"], rows["value"], strict=True)) == {
        "Manchester Wind": 1.0,
        "0": 2.0,
    }

    # And it survives the commit into the layer's own `outputs/`.
    child = staged.commit(NewChild(root))
    got = (
        DirectoryRecord(layer_dir(child.id), con)
        .outputs["p"]
        .collect()
        .to_native()
        .to_pandas()
    )
    assert set(got["name"]) == {"Manchester Wind", "0"}
    assert "component_type" not in got.columns


def test_a_frame_carrying_component_type_is_rejected(staged):
    """The column says the writer thinks the type keys the row; it does not (§9.2)."""
    frame = pd.DataFrame(
        [{"component_type": GEN, "name": "Manchester Wind", "value": 1.0}]
    )
    with pytest.raises(ValueError, match="component_type"):
        staged.set("p_max_pu", frame)


def test_a_scalar_derives_the_type_from_the_name(staged):
    """No type keyword: the name determines it, so a bare `set` is enough (§9.2)."""
    staged.set("p_nom", 150.0, names=["Manchester Wind"])
    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    assert set(rows[rows["name"] == "Manchester Wind"]["value"]) == {150.0}


def test_one_call_spans_component_types(staged):
    """Names decide the type, so one edit may cross types (§9.2).

    `p_nom` is declared for both `Generator` and `Link`, and each name is
    validated against its own type's spec - one call, two types, no keyword.
    """
    staged.set("p_nom", {"Manchester Wind": 150.0, "DC link": 80.0})
    rows = staged.attributes["p_nom"].collect().to_native().to_pandas()
    got = dict(zip(rows["name"], rows["value"], strict=True))
    assert got["Manchester Wind"] == 150.0
    assert got["DC link"] == 80.0


def test_an_attribute_the_names_type_does_not_declare_is_rejected(staged):
    """The spec checked is the name's *own* type's, and the error names the name.

    `p_max_pu` is a Generator attribute and `London Load` is a Load, so the
    failure is reported against the name that caused it rather than against a
    type the caller never mentioned (§9.8).
    """
    with pytest.raises(KeyError, match="London Load"):
        staged.set("p_max_pu", {"Manchester Wind": 0.5, "London Load": 0.5})
